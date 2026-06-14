"""
Natural-language query engine for the Schema Registry cache.

Flow:
  1. User asks a question in plain English
  2. LLM converts it to a SQLite SELECT query
  3. Query runs against the local sr_schemas cache
  4. If the question mentions "sample messages", also fetch live Kafka samples

Handles two intents in one endpoint:
  - "search"        — schema/topic discovery (SQL-backed)
  - "sample"        — fetch live Kafka messages for a named topic
  - "search+sample" — both (find the topic, then sample it)
"""
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# ── Intent detection ──────────────────────────────────────────────────────────

_SAMPLE_PATTERNS = [
    r"\bsample\b", r"\bmessages?\b", r"\bpeek\b", r"\bpreview\b",
    r"\bconsume\b", r"\bread\b.{0,20}\bfrom\b", r"\bshow\b.{0,20}\bdata\b",
    r"\bfetch\b.{0,20}\bmessages?\b",
]

_TOPIC_FROM_PATTERNS = [
    r"(?:from|on|in|for|topic)\s+['\"]?([a-zA-Z][\w.\-]*)['\"]?",
    r"['\"]([a-zA-Z][\w.\-]+)['\"]",
]


def _wants_samples(question: str) -> bool:
    q = question.lower()
    return any(re.search(p, q) for p in _SAMPLE_PATTERNS)


def _extract_topic(question: str, sql_results: Optional[List[dict]] = None) -> Optional[str]:
    """Extract an explicit topic name from the question or fall back to first SQL result."""
    for p in _TOPIC_FROM_PATTERNS:
        m = re.search(p, question, re.IGNORECASE)
        if m and len(m.group(1)) > 2:
            candidate = m.group(1).strip("\"'")
            # Skip common stop words
            if candidate.lower() not in {"the", "a", "an", "all", "any", "some"}:
                return candidate
    if sql_results:
        row = sql_results[0]
        return row.get("topic_name") or row.get("schema_name")
    return None


# ── LLM → SQL ─────────────────────────────────────────────────────────────────

def _llm_to_sql(question: str, ddl: str) -> str:
    from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY
    from langchain_openai import ChatOpenAI
    from langchain_core.messages import SystemMessage, HumanMessage

    system = (
        "You are a SQLite expert. Convert the user's question to a single SQLite SELECT query.\n\n"
        f"Table schema:\n{ddl}\n\n"
        "Rules:\n"
        "1. Return ONLY the SQL query — no explanation, no markdown, no trailing semicolon\n"
        "2. Use LOWER() with LIKE for case-insensitive text search: "
        "LOWER(col) LIKE LOWER('%value%')\n"
        "3. To find a specific field name in fields_json, use: "
        "LOWER(fields_json) LIKE LOWER('%\"name\": \"fieldname\"%') "
        "or more broadly LOWER(fields_json) LIKE LOWER('%fieldname%')\n"
        "4. Default LIMIT 100 unless user specifies otherwise\n"
        "5. Always SELECT at minimum: schema_name, topic_name, field_count\n"
        "6. If the question is about sample messages / live data (not schema structure), "
        "return: SELECT schema_name, topic_name, field_count, namespace_str "
        "FROM sr_schemas LIMIT 10\n"
    )

    llm = ChatOpenAI(
        base_url=LLM_BASE_URL, model=LLM_MODEL, api_key=LLM_API_KEY, temperature=0.2
    )
    resp = llm.invoke([
        SystemMessage(content=system),
        HumanMessage(content=question),
    ])
    sql = resp.content.strip()
    # Strip accidental code fences
    if "```" in sql:
        sql = sql.split("```")[1].lstrip("sql").strip()
    return sql.rstrip(";").strip()


# ── Public API ────────────────────────────────────────────────────────────────

def query_registry_nl(question: str) -> dict[str, Any]:
    """
    NL query against the indexed Schema Registry cache.

    Returns:
    {
        "intent":          "search" | "sample" | "search+sample" | "error",
        "sql":             "SELECT ...",
        "results":         [{schema_name, topic_name, ...}, ...],
        "count":           N,
        "sample_topic":    "orders",           # only when sample intent
        "sample_messages": [{...}, ...],       # only when sample intent
        "error":           "...",              # only on error
    }
    """
    from tools.kafka.schema_registry_cache import get_table_ddl, search_sql, get_stats, init_db

    init_db()
    stats = get_stats()

    if stats["total_schemas"] == 0:
        return {
            "intent": "error",
            "sql": "",
            "results": [],
            "count": 0,
            "error": (
                "Schema Registry index is empty. "
                "Call POST /api/registry/index to build the index first."
            ),
        }

    wants_sample = _wants_samples(question)
    ddl = get_table_ddl()

    # --- Generate SQL via LLM ---
    sql = ""
    results: List[dict] = []
    try:
        sql = _llm_to_sql(question, ddl)
        logger.info(f"[nl_query] generated SQL: {sql}")
        if not sql.strip().upper().startswith("SELECT"):
            raise ValueError(f"LLM returned non-SELECT query: {sql[:100]}")
        results = search_sql(sql)

    except Exception as exc:
        logger.warning(f"[nl_query] LLM/SQL failed ({exc}), falling back to LIKE search")
        # Safe fallback: plain LIKE search across topic name, fields, namespace
        safe_q = question[:60].replace("'", "''")
        sql = (
            "SELECT schema_name, topic_name, schema_group, field_count, "
            "namespace_str, fields_json "
            "FROM sr_schemas "
            f"WHERE LOWER(topic_name)    LIKE LOWER('%{safe_q}%') "
            f"   OR LOWER(fields_json)   LIKE LOWER('%{safe_q}%') "
            f"   OR LOWER(namespace_str) LIKE LOWER('%{safe_q}%') "
            f"   OR LOWER(description)   LIKE LOWER('%{safe_q}%') "
            "LIMIT 50"
        )
        try:
            results = search_sql(sql)
        except Exception as exc2:
            return {
                "intent": "error", "sql": sql,
                "results": [], "count": 0, "error": str(exc2),
            }

    out: dict[str, Any] = {
        "intent": "search",
        "sql": sql,
        "results": results,
        "count": len(results),
    }

    # --- Optional: fetch sample messages ---
    if wants_sample:
        topic = _extract_topic(question, results)
        if topic:
            try:
                from tools.kafka.kafka_tools import sample_kafka_messages
                messages = sample_kafka_messages(topic, num_messages=5)
                out["intent"] = "search+sample" if results else "sample"
                out["sample_topic"] = topic
                out["sample_messages"] = messages
            except Exception as exc:
                out["sample_error"] = str(exc)

    return out

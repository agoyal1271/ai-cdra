import json
import logging
import re as _re
from typing import AsyncGenerator, Optional, Tuple

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from tools.iceberg.iceberg_tools import list_iceberg_tables

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/nl-to-code", tags=["nl-to-code"])


class NLRequest(BaseModel):
    question: str
    model: Optional[str] = None  # if None, falls back to LLM_MODEL from config


def _sse(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"


# LLM only does the understanding — small, fast JSON, no code generation
NL_SYSTEM_PROMPT = """\
You are a data query intent analyzer for Apache Iceberg on Cloudera.

Respond ONLY with compact JSON (no markdown, no explanation):
{"understanding":"one clear sentence in your own words","intent":"SELECT|COUNT|FILTER|AGGREGATE|CATALOG_QUERY","entities":["key","words"],"table_used":"exact.table.name or empty if catalog-level query","columns_used":["col1","col2"],"filter_logic":"PLAIN ENGLISH only — e.g. 'tables created in the last 1 hour' or 'name is not null' — absolutely NO SQL syntax","sort":"column name or none","limit":100}

Rules:
- understanding: rephrase in your own words — never just copy the question
- intent CATALOG_QUERY: use this when user asks about tables, schemas, namespaces, or metadata (e.g. "show all tables", "which tables were created recently")
- intent CATALOG_QUERY: set table_used to empty string ""
- columns_used: if user says "all", "everything", or "show all", list ALL columns from the schema
- filter_logic: plain English ONLY — no SQL keywords (no now(), interval, WHERE, >=, etc.)
- Example good filter_logic: "tables created in the last 1 hour"
- Example bad filter_logic: "created_at > now() - interval 1 hour" ← never do this"""


def _build_catalog_context(tables: list[dict]) -> str:
    lines = []
    for t in tables:
        fields = t.get("fields", [])
        schema = ", ".join(f"{f['name']}:{f['type']}" for f in fields)
        lines.append(f"{t['name']} ({schema})")
    return " | ".join(lines) if lines else "demo.test_iceberg (id:long, name:string)"


def _time_interval_expr(filter_logic: str) -> str:
    """Returns a Spark SQL INTERVAL string from plain English filter."""
    m = _re.search(r'last\s+(\d+)\s+hour', filter_logic, _re.I)
    if m: return f"'{m.group(1)} HOURS'"
    m = _re.search(r'last\s+(\d+)\s+day', filter_logic, _re.I)
    if m: return f"'{m.group(1)} DAYS'"
    m = _re.search(r'last\s+(\d+)\s+minute', filter_logic, _re.I)
    if m: return f"'{m.group(1)} MINUTES'"
    if _re.search(r'today', filter_logic, _re.I): return "'1 DAYS'"
    if _re.search(r'last\s+week', filter_logic, _re.I): return "'7 DAYS'"
    if _re.search(r'last\s+month', filter_logic, _re.I): return "'30 DAYS'"
    return "'1 HOURS'"


def _generate_catalog_code(understanding: dict, tables: list[dict]) -> dict:
    """Generate catalog-level metadata query code (CATALOG_QUERY intent)."""
    filter_logic = understanding.get("filter_logic", "none")
    _, py_expr, time_desc = _resolve_time(filter_logic)
    has_time_filter = py_expr is not None

    # Detect namespace from known tables vs what the user mentioned
    search_text = (
        understanding.get("understanding", "") + " " +
        " ".join(understanding.get("entities", []))
    ).lower()
    known_namespaces = list({t["name"].split(".")[0] for t in tables if "." in t["name"]})
    namespace = next((ns for ns in known_namespaces if ns.lower() in search_text), "")

    # ── PyIceberg ──────────────────────────────────────────────────────────
    ns_arg = f"('{namespace}',)" if namespace else ""
    if has_time_filter:
        py_preamble, _ = py_expr
        pyiceberg = (
            "from pyiceberg.catalog import load_catalog\n"
            "from datetime import datetime, timedelta, timezone\n\n"
            "catalog = load_catalog('default')\n"
            f"# {understanding.get('understanding', '')}\n"
            f"{py_preamble}\n"
            "cutoff_ms = int(_cutoff.timestamp() * 1000)\n\n"
            "results = []\n"
            f"namespaces = catalog.list_namespaces({ns_arg})\n"
            "for ns in namespaces:\n"
            "    ns_name = '.'.join(ns)\n"
            "    for tbl_id in catalog.list_tables(ns):\n"
            "        try:\n"
            "            tbl = catalog.load_table(tbl_id)\n"
            "            snaps = tbl.metadata.snapshots or []\n"
            "            if snaps:\n"
            "                first_snap_ms = min(s.timestamp_ms for s in snaps)\n"
            "                if first_snap_ms >= cutoff_ms:\n"
            "                    results.append({\n"
            "                        'table': f'{ns_name}.{tbl_id[1]}',\n"
            "                        'created_ms': first_snap_ms,\n"
            "                    })\n"
            "        except Exception:\n"
            "            pass\n\n"
            "for r in results:\n"
            "    print(r)"
        )
    else:
        pyiceberg = (
            "from pyiceberg.catalog import load_catalog\n\n"
            "catalog = load_catalog('default')\n"
            f"# {understanding.get('understanding', '')}\n"
            f"namespaces = catalog.list_namespaces({ns_arg})\n"
            "for ns in namespaces:\n"
            "    ns_name = '.'.join(ns)\n"
            "    for tbl_id in catalog.list_tables(ns):\n"
            "        print(f'{ns_name}.{tbl_id[1]}')"
        )

    # ── Spark SQL ──────────────────────────────────────────────────────────
    ns_clause = f"IN {namespace}" if namespace else ""
    target_ns = namespace or "demo"
    if has_time_filter:
        interval = _time_interval_expr(filter_logic)
        spark_sql = (
            f"-- {understanding.get('understanding', '')}\n\n"
            f"-- Step 1: list all tables in the namespace\n"
            f"SHOW TABLES {ns_clause};\n\n"
            f"-- Step 2: check each table's first snapshot (= creation time)\n"
            f"-- Replace <table_name> with each result from above:\n"
            f"SELECT '{target_ns}.<table_name>' AS table_name,\n"
            f"       to_timestamp(min(committed_at) / 1000) AS created_at\n"
            f"FROM   {target_ns}.<table_name>.snapshots\n"
            f"HAVING min(committed_at) >= unix_millis(current_timestamp()) - unix_millis(current_timestamp() - INTERVAL {interval});"
        )
    else:
        spark_sql = (
            f"-- {understanding.get('understanding', '')}\n"
            f"SHOW TABLES {ns_clause};"
        )

    # ── Flink SQL ──────────────────────────────────────────────────────────
    if namespace:
        flink_sql = (
            f"-- {understanding.get('understanding', '')}\n"
            f"USE CATALOG default;\n"
            f"USE {namespace};\n"
            f"SHOW TABLES;"
        )
    else:
        flink_sql = (
            f"-- {understanding.get('understanding', '')}\n"
            f"USE CATALOG default;\n"
            f"SHOW TABLES;"
        )

    return {"pyiceberg": pyiceberg, "spark_sql": spark_sql, "flink_sql": flink_sql}


def _generate_code(understanding: dict, tables: list[dict]) -> dict:
    """Template-generate all three code formats from LLM understanding."""
    tbl_name = understanding.get("table_used", "")
    tbl = next((t for t in tables if t["name"] == tbl_name), tables[0] if tables else None)
    fields = tbl.get("fields", []) if tbl else []
    tbl_name = tbl["name"] if tbl else tbl_name
    all_cols = [f["name"] for f in fields]

    # If Ollama only returned filter column(s) but user asked for "all", use full schema
    ollama_cols = understanding.get("columns_used") or []
    intent = understanding.get("intent", "SELECT")
    filter_logic = understanding.get("filter_logic", "none")

    # CATALOG_QUERY: user is asking about tables/metadata, not records within a table
    if intent == "CATALOG_QUERY" or not understanding.get("table_used"):
        return _generate_catalog_code(understanding, tables)

    # Use all columns if user said "all/everything", or if Ollama only returned filter column(s)
    use_all = any(w in (understanding.get("understanding","") + filter_logic).lower() for w in ["all", "every", "everything", "show all"])
    cols = all_cols if (use_all or len(ollama_cols) <= 1) else ollama_cols
    sort_col = understanding.get("sort", "none")
    limit = understanding.get("limit", 100)

    has_filter = filter_logic and filter_logic.lower() not in ("none", "no filter", "n/a", "")

    # Build filter expression for PyIceberg (simplified)
    py_filter = _infer_py_filter(filter_logic, cols, fields) if has_filter else None
    sql_where = _infer_sql_where(filter_logic, cols) if has_filter else None

    # SELECT / COUNT handling
    if intent == "COUNT":
        select_expr = "COUNT(*) AS total_count"
        py_select = None  # scan all for count
    else:
        select_expr = ", ".join(cols) if cols else "*"
        py_select = tuple(cols) if cols else None

    ns = tbl_name.split(".")
    spark_table = tbl_name  # Spark uses namespace.table directly with Iceberg catalog

    py_scan_args = []
    if py_select and intent != "COUNT":
        py_scan_args.append(f"    selected_fields={py_select!r}")
    if py_filter:
        py_scan_args.append(f"    row_filter=\"{py_filter}\"")
    if limit and intent != "COUNT":
        py_scan_args.append(f"    limit={limit}")
    py_scan_block = "table.scan(\n" + ",\n".join(py_scan_args) + "\n)" if py_scan_args else "table.scan()"

    pyiceberg = (
        "from pyiceberg.catalog import load_catalog\n\n"
        "catalog = load_catalog('default')\n"
        f"table = catalog.load_table('{tbl_name}')\n\n"
        + (f"# {understanding.get('understanding', '')}\n" if understanding.get("understanding") else "")
        + f"df = {py_scan_block}.to_pandas()\n"
        + ("print(f'Total rows: {len(df)}')" if intent == "COUNT" else "print(df.head(10))")
    )

    sql_parts = [f"SELECT {select_expr}", f"FROM {spark_table}"]
    if sql_where:
        sql_parts.append(f"WHERE {sql_where}")
    if sort_col and sort_col.lower() not in ("none", "n/a", ""):
        sql_parts.append(f"ORDER BY {sort_col} DESC")
    if limit and intent != "COUNT":
        sql_parts.append(f"LIMIT {limit}")
    spark_sql = "-- " + understanding.get("understanding", "") + "\n" + "\n".join(sql_parts) + ";"

    flink_table_alias = tbl_name.replace(".", "_")
    flink_col_defs = "\n".join(f"    {f['name']} {f['type'].upper()}," for f in fields).rstrip(",")
    flink_select = "COUNT(*)" if intent == "COUNT" else select_expr
    flink_where = f"\nWHERE {sql_where}" if sql_where else ""
    flink_sql = (
        f"-- {understanding.get('understanding', '')}\n"
        f"CREATE TABLE IF NOT EXISTS {flink_table_alias} (\n{flink_col_defs}\n) WITH (\n"
        f"    'connector' = 'iceberg',\n"
        f"    'catalog-name' = 'default',\n"
        f"    'catalog-database' = '{ns[0] if len(ns) > 1 else 'default'}',\n"
        f"    'catalog-table' = '{ns[-1]}'\n"
        f");\n\n"
        f"SELECT {flink_select}\nFROM {flink_table_alias}{flink_where};"
    )

    return {"pyiceberg": pyiceberg, "spark_sql": spark_sql, "flink_sql": flink_sql}


# Time expression patterns: (regex, sql_fn, py_fn, description_fn)
# py_fn returns a (preamble, row_filter) tuple — preamble is extra Python lines before scan()
_TIME_PATTERNS = [
    (_re.compile(r'last\s+(\d+)\s+hour', _re.I),
     lambda m: f"created_at >= NOW() - INTERVAL '{m.group(1)} HOURS'",
     lambda m: (f"_cutoff = datetime.now(timezone.utc) - timedelta(hours={m.group(1)})", f"created_at >= _cutoff.isoformat()"),
     lambda m: f"last {m.group(1)} hour(s) → now() - {m.group(1)}h"),
    (_re.compile(r'last\s+(\d+)\s+day', _re.I),
     lambda m: f"created_at >= NOW() - INTERVAL '{m.group(1)} DAYS'",
     lambda m: (f"_cutoff = datetime.now(timezone.utc) - timedelta(days={m.group(1)})", f"created_at >= _cutoff.isoformat()"),
     lambda m: f"last {m.group(1)} day(s) → now() - {m.group(1)}d"),
    (_re.compile(r'last\s+(\d+)\s+minute', _re.I),
     lambda m: f"created_at >= NOW() - INTERVAL '{m.group(1)} MINUTES'",
     lambda m: (f"_cutoff = datetime.now(timezone.utc) - timedelta(minutes={m.group(1)})", f"created_at >= _cutoff.isoformat()"),
     lambda m: f"last {m.group(1)} minute(s) → now() - {m.group(1)}m"),
    (_re.compile(r'today', _re.I),
     lambda m: "created_at >= CURRENT_DATE",
     lambda m: ("_cutoff = datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0)", "created_at >= _cutoff.isoformat()"),
     lambda m: "today → start of current date"),
    (_re.compile(r'last\s+week', _re.I),
     lambda m: "created_at >= NOW() - INTERVAL '7 DAYS'",
     lambda m: ("_cutoff = datetime.now(timezone.utc) - timedelta(days=7)", "created_at >= _cutoff.isoformat()"),
     lambda m: "last week → now() - 7d"),
    (_re.compile(r'last\s+month', _re.I),
     lambda m: "created_at >= NOW() - INTERVAL '30 DAYS'",
     lambda m: ("_cutoff = datetime.now(timezone.utc) - timedelta(days=30)", "created_at >= _cutoff.isoformat()"),
     lambda m: "last month → now() - 30d"),
    (_re.compile(r'this\s+year', _re.I),
     lambda m: "created_at >= DATE_TRUNC('year', CURRENT_DATE)",
     lambda m: ("_cutoff = datetime.now(timezone.utc).replace(month=1,day=1,hour=0,minute=0,second=0)", "created_at >= _cutoff.isoformat()"),
     lambda m: "this year → start of current year"),
]


def _resolve_time(filter_logic: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Returns (sql_expr, py_expr, description) if a time pattern matches, else (None, None, None)."""
    for pat, sql_fn, py_fn, desc_fn in _TIME_PATTERNS:
        m = pat.search(filter_logic)
        if m:
            return sql_fn(m), py_fn(m), desc_fn(m)
    return None, None, None


def _infer_py_filter(filter_logic: str, cols: list, fields: list) -> str:
    sql, py, _ = _resolve_time(filter_logic)
    if py:
        return py
    fl = filter_logic.lower()
    for col in cols:
        if col.lower() in fl:
            if "not null" in fl or "is not null" in fl:
                return f"{col} is not null"
            if "null" in fl:
                return f"{col} is null"
            m = _re.search(r'>\s*(\d+)', fl)
            if m:
                return f"{col} > {m.group(1)}"
            m = _re.search(r'<\s*(\d+)', fl)
            if m:
                return f"{col} < {m.group(1)}"
    return filter_logic


def _infer_sql_where(filter_logic: str, cols: list) -> str:
    sql, _, _ = _resolve_time(filter_logic)
    if sql:
        return sql
    fl = filter_logic.lower()
    for col in cols:
        if col.lower() in fl:
            if "not null" in fl or "is not null" in fl:
                return f"{col} IS NOT NULL"
            if "null" in fl:
                return f"{col} IS NULL"
            m = _re.search(r'>\s*(\d+)', fl)
            if m:
                return f"{col} > {m.group(1)}"
            m = _re.search(r'<\s*(\d+)', fl)
            if m:
                return f"{col} < {m.group(1)}"
    return filter_logic


def _build_translation_chain(question: str, understanding: dict, tables: list) -> list[dict]:
    """Builds a step-by-step chain showing how Ollama translated English → code constructs."""
    filter_logic = understanding.get("filter_logic", "none")
    sql_expr, py_expr, time_desc = _resolve_time(filter_logic)
    cols = understanding.get("columns_used", [])

    intent = understanding.get("intent", "SELECT")
    table_label = understanding.get("table_used", "") or ("(catalog-level query)" if intent == "CATALOG_QUERY" else "")
    steps = [
        {"label": "Natural language", "value": question, "type": "input"},
        {"label": "Ollama understood", "value": understanding.get("understanding", ""), "type": "understanding"},
        {"label": "Intent classified as", "value": intent, "type": "intent"},
        {"label": "Table identified", "value": table_label, "type": "table"},
    ]
    if cols:
        steps.append({"label": "Columns referenced", "value": ", ".join(cols), "type": "columns"})
    if filter_logic and filter_logic.lower() not in ("none", "no filter", "n/a", ""):
        steps.append({"label": "Filter condition (from Ollama)", "value": filter_logic, "type": "filter_nl"})
        if time_desc:
            steps.append({"label": "Time expression resolved", "value": time_desc, "type": "time_resolve"})
        if sql_expr:
            steps.append({"label": "→ SQL expression", "value": sql_expr, "type": "sql_expr"})
        if py_expr:
            py_display = py_expr[0] if isinstance(py_expr, tuple) else py_expr
            steps.append({"label": "→ PyIceberg cutoff", "value": py_display, "type": "py_expr"})
        elif cols:
            fl = filter_logic.lower()
            for col in cols:
                if col.lower() in fl:
                    if "not null" in fl:
                        steps.append({"label": "→ SQL expression", "value": f"{col} IS NOT NULL", "type": "sql_expr"})
    return steps


async def _stream(question: str, model: Optional[str] = None) -> AsyncGenerator[str, None]:
    import time
    from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY

    active_model = model or LLM_MODEL

    yield _sse({"type": "thought", "content": "Loading Iceberg catalog..."})
    tables = list_iceberg_tables()
    catalog_ctx = _build_catalog_context(tables)
    yield _sse({"type": "catalog", "tables": [t["name"] for t in tables]})
    yield _sse({"type": "model", "model": active_model})
    yield _sse({"type": "thought", "content": f"Sending to {active_model} — waiting for first token..."})

    user_msg = f"Tables: {catalog_ctx}\nQuestion: {question}"

    try:
        from langchain_openai import ChatOpenAI
        from langchain_core.messages import HumanMessage, SystemMessage

        llm = ChatOpenAI(
            base_url=LLM_BASE_URL, model=active_model, api_key=LLM_API_KEY,
            temperature=0.2, streaming=True,
        )

        full_text = ""
        token_count = 0
        t_start = time.monotonic()
        t_first: Optional[float] = None

        async for chunk in llm.astream([SystemMessage(content=NL_SYSTEM_PROMPT), HumanMessage(content=user_msg)]):
            token = chunk.content
            if token:
                if t_first is None:
                    t_first = time.monotonic()
                    yield _sse({"type": "thought", "content": f"First token in {t_first - t_start:.1f}s — model is generating..."})
                full_text += token
                token_count += 1
                yield _sse({"type": "token", "text": token})

        t_end = time.monotonic()
        elapsed = round(t_end - t_start, 1)
        tps = round(token_count / max(elapsed, 0.1), 1)
        yield _sse({"type": "llm_done", "model": active_model, "tokens": token_count, "elapsed_s": elapsed, "tokens_per_s": tps})

        understanding = _parse_understanding(full_text)
        translation = _build_translation_chain(question, understanding, tables)
        yield _sse({"type": "understanding", **understanding, "translation": translation})
        yield _sse({"type": "thought", "content": "Generating PyIceberg, Spark SQL, and Flink SQL..."})

        code = _generate_code(understanding, tables)
        yield _sse({"type": "complete", **understanding, **code, "translation": translation, "model": active_model})

    except Exception as e:
        logger.warning(f"LLM failed: {e}, using rule-based fallback")
        understanding = {
            "understanding": f"Retrieve records from the available table matching: {question}",
            "intent": "SELECT", "entities": [], "table_used": tables[0]["name"] if tables else "demo.test_iceberg",
            "columns_used": [f["name"] for f in (tables[0].get("fields", []) if tables else [])],
            "filter_logic": "none", "sort": "none", "limit": 100,
        }
        code = _generate_code(understanding, tables)
        yield _sse({"type": "complete", **understanding, **code, "fallback": True, "model": active_model})


def _parse_understanding(text: str) -> dict:
    clean = text.strip()
    if "```" in clean:
        clean = clean.split("```", 1)[1].split("```", 1)[0].strip()
        clean = clean.lstrip("json").strip()
    s = clean.find("{"); e = clean.rfind("}") + 1
    if s != -1 and e > s:
        clean = clean[s:e]
    try:
        return json.loads(clean)
    except Exception:
        return {
            "understanding": text[:200],
            "intent": "SELECT", "entities": [], "table_used": "",
            "columns_used": [], "filter_logic": "none", "sort": "none", "limit": 100,
        }


@router.post("/generate")
async def generate_code(req: NLRequest):
    return StreamingResponse(
        _stream(req.question, req.model),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


class AskAssetRequest(BaseModel):
    question: str
    asset_name: str
    fields: list[dict]
    asset_type: str = "iceberg_table"
    engine: str = "impala"


@router.post("/ask-asset")
async def ask_asset(req: AskAssetRequest):
    """
    Context-aware SQL generation.
    Schema is pre-loaded — single LLM call, no catalog discovery.
    """
    from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY
    from langchain_openai import ChatOpenAI
    from langchain_core.messages import SystemMessage, HumanMessage

    schema_lines = "\n".join(
        f"  {f['name']}: {f.get('type', 'string')}" for f in req.fields
    )
    prompt = f"""You are a SQL expert for Cloudera CDP.
Given the exact table schema below, write a single {req.engine.upper()} SQL query that answers the question.
Return ONLY the SQL — no explanation, no markdown fences.

Table: {req.asset_name}
Engine: {req.engine}
Schema:
{schema_lines}

Question: {req.question}"""

    try:
        llm = ChatOpenAI(
            base_url=LLM_BASE_URL, model=LLM_MODEL, api_key=LLM_API_KEY,
            temperature=0.1,
        )
        response = await llm.ainvoke([
            SystemMessage(content="You generate precise SQL queries. Return only the SQL statement."),
            HumanMessage(content=prompt),
        ])
        sql = response.content.strip().strip("```sql").strip("```").strip()
        return {"sql": sql, "understanding": req.question, "engine": req.engine}
    except Exception as e:
        logger.warning(f"[ask-asset] LLM failed: {e}")
        cols = ", ".join(f['name'] for f in req.fields[:5])
        return {
            "sql": f"SELECT {cols} FROM {req.asset_name} LIMIT 100;",
            "understanding": req.question,
            "engine": req.engine,
            "fallback": True,
        }


class RunSQLRequest(BaseModel):
    sql: str
    engine: str = "impala"   # dialect label; execution always runs on the live Impala engine


def _is_read_only(sql: str) -> bool:
    """Allow only single read-only statements. Blocks writes/DDL."""
    s = sql.strip().rstrip(";").lstrip()
    if ";" in s:                       # no multi-statement
        return False
    first = s.split(None, 1)[0].lower() if s else ""
    return first in ("select", "with", "show", "describe", "desc")


@router.post("/run-asset")
async def run_asset(req: RunSQLRequest):
    """
    Execute a read-only SQL query against the live Cloudera Impala engine (via Knox)
    and return the result rows. Mirrors the Data Quality tab: SQL is shown per dialect
    (Impala/Trino/Spark) but actually executed on Impala.
    """
    import asyncio
    import os

    if not _is_read_only(req.sql):
        return {"error": "Only read-only queries (SELECT/WITH/SHOW/DESCRIBE) can be run.",
                "rows": [], "columns": []}

    knox_host = os.getenv("KNOX_HOST") or "cdp-utility.cdp.local"
    knox_user = os.getenv("KNOX_USERNAME", "")
    knox_pass = os.getenv("KNOX_PASSWORD", "")

    def _run():
        from impala.dbapi import connect as impala_connect
        conn = impala_connect(
            host=knox_host, port=8443, use_http_transport=True,
            http_path="gateway/cdp-proxy-api/impala/",
            auth_mechanism="LDAP", user=knox_user, password=knox_pass,
        )
        cur = conn.cursor()
        sql = req.sql.strip().rstrip(";")
        cur.execute(sql)
        columns = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(200)        # cap result set
        conn.close()
        # Coerce to JSON-safe values
        out = [[(str(v) if v is not None else None) for v in row] for row in rows]
        return columns, out

    try:
        columns, rows = await asyncio.wait_for(asyncio.to_thread(_run), timeout=60)
        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "executed_on": "impala",
            "dialect": req.engine,
        }
    except asyncio.TimeoutError:
        return {"error": "Query timed out after 60s.", "rows": [], "columns": []}
    except Exception as e:
        logger.warning(f"[run-asset] execution failed: {e}")
        return {"error": str(e), "rows": [], "columns": []}

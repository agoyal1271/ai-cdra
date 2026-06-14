"""
Schema Registry API routes.

POST /api/registry/index   — index all schemas into local SQLite cache
GET  /api/registry/status  — indexing status + cache stats
POST /api/registry/query   — NL query: LLM → SQL → results (+ optional sample messages)
POST /api/registry/sample  — fetch live sample messages from a Kafka topic
GET  /api/registry/search  — quick text search (autocomplete)
"""
import asyncio
import logging

from fastapi import APIRouter, BackgroundTasks, Query
from pydantic import BaseModel

router = APIRouter(prefix="/api/registry", tags=["registry"])
logger = logging.getLogger(__name__)


class QueryRequest(BaseModel):
    question: str


class SampleRequest(BaseModel):
    topic: str
    n: int = 5


# ── Index ─────────────────────────────────────────────────────────────────────

@router.post("/index")
async def index_registry(background_tasks: BackgroundTasks, background: bool = False):
    """
    Index the Schema Registry into the local SQLite cache.

    - Cloudera SR: one aggregated API call — fast even at 10k+ topics.
    - Confluent SR: fetches all subjects sequentially.

    Pass ?background=true to run the indexer as a background task and
    return immediately. Poll /status for progress.
    """
    from tools.kafka.schema_registry_indexer import run_index, get_indexing_status

    status = get_indexing_status()
    if status.get("status") == "indexing":
        return {"status": "already_indexing", **status}

    if background:
        background_tasks.add_task(run_index)
        return {"status": "indexing_started", "message": "Indexing running in background — poll /api/registry/status"}

    result = await asyncio.to_thread(run_index)
    return {"status": "done", **result}


# ── Status ────────────────────────────────────────────────────────────────────

@router.get("/status")
async def registry_status():
    """Returns indexing state, total schema count, and per-group breakdown."""
    from tools.kafka.schema_registry_indexer import get_indexing_status
    return get_indexing_status()


# ── NL Query ──────────────────────────────────────────────────────────────────

@router.post("/query")
async def query_registry(request: QueryRequest):
    """
    Natural-language query against the indexed Schema Registry.

    The LLM converts your question to SQL, executes it against the local cache,
    and returns matching schemas. If the question mentions sample messages,
    live Kafka samples are also fetched and included.

    Example questions:
      - "find all topics with a customer_id field"
      - "show me topics in the payments namespace"
      - "which topics have more than 10 fields?"
      - "find topics related to orders and show me 5 sample messages"
      - "what topics contain PII-related fields like email or ssn?"
    """
    from tools.kafka.nl_query import query_registry_nl
    result = await asyncio.to_thread(query_registry_nl, request.question)
    return result


# ── Sample Messages ───────────────────────────────────────────────────────────

@router.post("/sample")
async def sample_messages(request: SampleRequest):
    """
    Fetch sample Kafka messages from a topic.
    Messages are decoded using the Schema Registry schema if Avro-encoded.
    Schema metadata is served from the local cache (no SR API call needed).
    """
    from tools.kafka.kafka_tools import sample_kafka_messages
    from tools.kafka.schema_registry_cache import search_sql, init_db

    init_db()

    # Fetch messages from Kafka
    messages = []
    msg_error = None
    try:
        messages = await asyncio.to_thread(sample_kafka_messages, request.topic, request.n)
    except Exception as exc:
        msg_error = str(exc)
        logger.warning(f"[registry] sample_messages failed for {request.topic!r}: {exc}")

    # Get schema from local cache (avoids SR API call)
    schema = {}
    try:
        rows = search_sql(
            "SELECT schema_text, fields_json, schema_type, version, schema_id "
            "FROM sr_schemas WHERE topic_name = ? OR schema_name = ? LIMIT 1",
            (request.topic, request.topic),
        )
        if rows:
            schema = rows[0]
    except Exception:
        pass

    return {
        "topic": request.topic,
        "messages": messages,
        "schema": schema,
        "count": len(messages),
        **({"error": msg_error} if msg_error else {}),
    }


# ── Search / Autocomplete ─────────────────────────────────────────────────────

@router.get("/search")
async def search_registry(
    q: str = Query(default="", description="Search term (topic name, field name, namespace)"),
    limit: int = Query(default=20, le=500),
):
    """
    Quick text search across topic names, field names, and namespaces.
    Returns lightweight rows suitable for autocomplete dropdowns.
    """
    from tools.kafka.schema_registry_cache import search_sql, init_db

    init_db()

    if not q:
        results = search_sql(
            "SELECT schema_name, topic_name, schema_group, field_count, namespace_str "
            "FROM sr_schemas ORDER BY schema_name LIMIT ?",
            (limit,),
        )
    else:
        safe = q.replace("'", "''")
        results = search_sql(
            "SELECT schema_name, topic_name, schema_group, field_count, namespace_str, fields_json "
            "FROM sr_schemas "
            f"WHERE LOWER(topic_name)    LIKE LOWER('%{safe}%') "
            f"   OR LOWER(fields_json)   LIKE LOWER('%{safe}%') "
            f"   OR LOWER(namespace_str) LIKE LOWER('%{safe}%') "
            f"   OR LOWER(description)   LIKE LOWER('%{safe}%') "
            "ORDER BY topic_name LIMIT ?",
            (limit,),
        )

    return {"results": results, "count": len(results), "query": q}

"""
OpenMetadata router.

Endpoints:
  GET  /api/openmetadata/health          — is OM reachable?
  GET  /api/openmetadata/lineage         — fetch lineage for an asset by name
  POST /api/openmetadata/setup           — register Kafka topic + Iceberg table + lineage edge
"""

import logging
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/openmetadata", tags=["openmetadata"])


class SetupRequest(BaseModel):
    topic_name: str          # e.g. "demo.payment_transactions"
    table_name: str          # e.g. "demo.payment_transactions"
    topic_description: str = ""
    table_description: str = ""
    pipeline_label: str = "Kafka → NiFi → Iceberg"


@router.get("/health")
async def om_health():
    from tools.openmetadata.client import health_check
    ok = health_check()
    return {"status": "ok" if ok else "unreachable", "url": "http://localhost:8585"}


@router.get("/lineage")
async def get_lineage(asset: str, asset_type: str = "table"):
    """
    Fetch lineage for a named asset from OpenMetadata.
    asset      — table name like 'demo.payment_transactions' or short name 'payment_transactions'
    asset_type — 'table' | 'topic'
    """
    from tools.openmetadata.client import get_lineage_by_name
    result = get_lineage_by_name(asset, asset_type)
    if not result:
        return {"found": False, "asset": asset, "upstream": [], "downstream": [], "edges": [], "edge_count": 0}
    return {
        "found":      True,
        "entity":     result["entity"],
        "upstream":   result["upstream"],
        "downstream": result["downstream"],
        "edge_count": result.get("edge_count", len(result.get("edges", []))),
    }


@router.post("/setup")
async def setup_lineage(req: SetupRequest):
    """
    Register the Kafka topic + Iceberg table in OM and create the lineage edge between them.
    Run this once after OM is up to establish the demo pipeline story.
    """
    from tools.openmetadata.client import (
        ensure_messaging_service, ensure_database_service,
        register_topic, register_table, create_lineage_edge,
    )
    import requests, json
    from config import SCHEMA_REGISTRY_URL, KNOX_USERNAME, KNOX_PASSWORD

    SR_URL  = SCHEMA_REGISTRY_URL
    SR_AUTH = (KNOX_USERNAME, KNOX_PASSWORD)

    results = {}

    # ── 1. Ensure services exist ──────────────────────────────────────────
    results["kafka_service"]  = ensure_messaging_service("cdp_kafka")
    results["hive_service"]   = ensure_database_service("cdp_hive")

    # ── 2. Fetch Avro schema from Schema Registry ─────────────────────────
    subject = f"{req.topic_name}-value"
    sr_resp = requests.get(f"{SR_URL}/subjects/{subject}/versions/latest", auth=SR_AUTH, timeout=10)
    schema_fields = []
    if sr_resp.ok:
        schema = json.loads(sr_resp.json()["schema"])
        schema_fields = schema.get("fields", [])

    # ── 3. Register Kafka topic ───────────────────────────────────────────
    topic_desc = req.topic_description or (
        f"Payment transactions streamed from CDP Kafka. "
        f"Schema registered under subject '{subject}' in Schema Registry."
    )
    topic_result = register_topic(req.topic_name, schema_fields, description=topic_desc)
    results["topic"] = topic_result.get("fullyQualifiedName") if topic_result else "already exists or failed"

    # ── 4. Register Iceberg table via Impala schema ───────────────────────
    try:
        from impala.dbapi import connect as impala_connect
        import os as _os
        _knox_host = _os.getenv("KNOX_HOST", "cdp-utility.cdp.local")
        conn = impala_connect(
            host=_knox_host, port=8443,
            use_http_transport=True, http_path="gateway/cdp-proxy-api/impala/",
            auth_mechanism="LDAP", user=KNOX_USERNAME, password=KNOX_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE {req.table_name}")
        iceberg_fields = [{"name": r[0], "type": r[1]} for r in cursor.fetchall()]
        conn.close()
    except Exception as e:
        logger.warning(f"Could not describe table via Impala: {e}")
        iceberg_fields = schema_fields   # fall back to Avro schema

    table_desc = req.table_description or (
        f"Iceberg table receiving payment transactions from Kafka topic '{req.topic_name}' "
        f"via NiFi. Partitioned by date. Queryable via Impala on Cloudera CDP."
    )
    table_result = register_table(req.table_name, iceberg_fields, description=table_desc)
    results["table"] = table_result.get("fullyQualifiedName") if table_result else "already exists or failed"

    # ── 5. Create lineage edge: topic → table ─────────────────────────────
    topic_fqn = f"cdp_kafka.{req.topic_name}"
    table_fqn = f"cdp_hive.{req.table_name}.default.{req.table_name.split('.')[-1]}"
    edge = create_lineage_edge(
        from_fqn=topic_fqn,  from_type="topic",
        to_fqn=table_fqn,    to_type="table",
        pipeline_name=req.pipeline_label,
    )
    results["lineage_edge"] = "created" if edge else "failed"

    return results

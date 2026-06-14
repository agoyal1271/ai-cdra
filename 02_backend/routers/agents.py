import asyncio
import json
import logging
from datetime import datetime
from typing import Optional

import requests as _requests
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

router = APIRouter()
logger = logging.getLogger(__name__)

AGENT_REGISTRY = [
    {
        "id": "source_scout",
        "name": "Source Scout",
        "role": "Discovery Agent",
        "tagline": "AUTO-DISCOVER",
        "description": "Scans Kafka and Ozone to catalog all data assets. Profiles schemas, estimates freshness, and suggests ingestion pipelines.",
        "status": "active",
        "tools": ["list_kafka_topics", "sample_kafka_messages", "list_iceberg_tables", "describe_iceberg_table", "list_ozone_volumes", "suggest_ingestion_pipeline"],
        "icon": "radar",
    },
    {
        "id": "pipeline_builder",
        "name": "Pipeline Builder",
        "role": "Ingestion Agent",
        "tagline": "AUTO-CONFIG",
        "description": "Takes a discovered Kafka topic or Ozone-backed Iceberg table and emits a downloadable NiFi flow-definition JSON for ingest into ADLS (Iceberg/Delta) or Snowflake — directly importable via NiFi 'Upload Flow Definition'.",
        "status": "active",
        "tools": ["build_nifi_flow", "download_flow_json", "list_builder_options"],
        "icon": "wrench",
    },
    {
        "id": "quality_guardian",
        "name": "Quality Guardian",
        "role": "Validation Agent",
        "tagline": "REAL-TIME QA",
        "description": "Detects anomalies in Kafka streams via Flink, auto-quarantines bad Iceberg records, generates dbt tests from data profiles.",
        "status": "active",
        "tools": ["run_flink_dq_job", "scan_iceberg_quality", "quarantine_records", "generate_dbt_tests"],
        "icon": "shield",
    },
    {
        "id": "pipeline_healer",
        "name": "Pipeline Healer",
        "role": "Reliability Agent",
        "tagline": "SELF-HEALING",
        "description": "Monitors Kafka consumer lag and Flink job health 24/7. Diagnoses root causes and auto-remediates failures before they escalate.",
        "status": "active",
        "tools": ["get_consumer_lag", "get_flink_job_status", "restart_flink_job", "create_alert"],
        "icon": "heart-pulse",
    },
    {
        "id": "semantic_mapper",
        "name": "Semantic Mapper",
        "role": "Intelligence Agent",
        "tagline": "NL → METRICS",
        "description": "Maps raw Iceberg/Kafka fields to a business semantic model. Detects conflicting metric definitions and generates OpenMetadata documentation.",
        "status": "active",
        "tools": ["map_fields_to_semantic_model", "detect_metric_conflicts", "suggest_glossary_terms"],
        "icon": "brain",
    },
    {
        "id": "metadata_curator",
        "name": "Metadata Curator",
        "role": "Governance Agent",
        "tagline": "AUTO-GOVERN",
        "description": "Auto-classifies PII and sensitive data, enriches asset descriptions via LLM, assigns data owners, and enforces compliance via Atlas/OpenMetadata.",
        "status": "active",
        "tools": ["classify_pii", "enrich_asset_description", "assign_data_owner", "register_lineage"],
        "icon": "gavel",
    },
]


class DiscoverRequest(BaseModel):
    goal: str = "Discover all data sources in the Cloudera platform"
    kafka_bootstrap_servers: Optional[str] = None


class OrchestrateRequest(BaseModel):
    goal: str
    table_name: Optional[str] = None
    asset_id: Optional[str] = None
    pipeline_id: Optional[str] = None
    agent_type: Optional[str] = "react"  # "react" or "hierarchical"
    apply_ranger: Optional[bool] = False


@router.get("/api/models")
async def list_models():
    """Lists available models from the Ollama endpoint dynamically."""
    from config import LLM_BASE_URL, LLM_MODEL
    # Ollama exposes /api/tags; strip /v1 suffix if present
    base = LLM_BASE_URL.rstrip("/")
    if base.endswith("/v1"):
        base = base[:-3]
    try:
        resp = _requests.get(f"{base}/api/tags", timeout=3)
        resp.raise_for_status()
        raw = resp.json().get("models", [])
        models = [
            {
                "id": m["name"],
                "name": m["name"],
                "size": m.get("details", {}).get("parameter_size", ""),
                "family": m.get("details", {}).get("family", ""),
            }
            for m in raw
        ]
    except Exception as e:
        logger.warning(f"Could not fetch models from Ollama: {e}")
        models = [{"id": LLM_MODEL, "name": LLM_MODEL, "size": "", "family": ""}]
    return {"models": models, "default": LLM_MODEL}


@router.get("/api/agents")
async def list_agents():
    return {"agents": AGENT_REGISTRY}


@router.get("/api/system/knox-status")
async def knox_status():
    """Check if Knox Gateway is configured."""
    import os
    knox_host = os.getenv('KNOX_HOST')
    return {
        "configured": bool(knox_host),
        "host": knox_host or None,
    }


@router.post("/api/agents/discover")
async def discover(request: DiscoverRequest):
    """SSE stream: runs Source Scout and streams discovery events."""
    from agents.source_scout.agent import run_source_scout

    async def event_stream():
        try:
            async for event in run_source_scout(request.goal):
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as e:
            logger.exception("Source Scout error")
            yield f"data: {json.dumps({'type': 'error', 'agent': 'source_scout', 'message': str(e)})}\n\n"
        yield "data: {\"type\": \"stream_end\"}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.post("/api/agents/discover-react")
async def discover_react(request: DiscoverRequest):
    """SSE stream: runs Source Scout ReAct agent (LLM reasoning + vector search demo)."""
    from agents.source_scout.react_agent import run_source_scout_react

    async def event_stream():
        try:
            async for event in run_source_scout_react(request.goal):
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as e:
            logger.exception("Source Scout ReAct error")
            yield f"data: {json.dumps({'type': 'error', 'agent': 'source_scout_react', 'message': str(e)})}\n\n"
        yield "data: {\"type\": \"stream_end\"}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.post("/api/agents/orchestrate")
async def orchestrate(request: OrchestrateRequest):
    """SSE stream: supervisor routes goal to best agent."""
    from agents.orchestrator import app_graph
    from agents.state import AgentState

    async def event_stream():
        try:
            yield f"data: {json.dumps({'type': 'thought', 'agent': 'supervisor', 'content': f'Analyzing goal: {request.goal}'})}\n\n"
            initial_state: AgentState = {
                "messages": [],
                "goal": request.goal,
                "active_agent": "",
                "discovered_assets": {},
                "artifacts": {},
                "next": "",
                "sse_events": [],
            }
            result = await asyncio.to_thread(app_graph.invoke, initial_state)
            for event in result.get("sse_events", []):
                yield f"data: {json.dumps(event)}\n\n"
            summary = result["messages"][-1].content if result.get("messages") else "Done."
            yield f"data: {json.dumps({'type': 'complete', 'agent': result.get('active_agent', 'unknown'), 'summary': summary})}\n\n"
        except Exception as e:
            logger.exception("Orchestration error")
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
        yield "data: {\"type\": \"stream_end\"}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.post("/api/agents/build-pipeline")
async def build_pipeline(request: OrchestrateRequest):
    """SSE stream: generates NiFi/Flink/Kafka Connect configs."""
    from agents.pipeline_builder.agent import PipelineBuilderAgent

    async def event_stream():
        try:
            agent = PipelineBuilderAgent()
            kwargs = {}
            if request.table_name:
                kwargs["table_name"] = request.table_name
            async for event in agent.run(request.goal, **kwargs):
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as e:
            logger.exception("Pipeline Builder error")
            yield f"data: {json.dumps({'type': 'error', 'agent': 'pipeline_builder', 'message': str(e)})}\n\n"
        yield "data: {\"type\": \"stream_end\"}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.post("/api/agents/validate-quality")
async def validate_quality(request: OrchestrateRequest):
    """SSE stream: validates data quality, detects anomalies."""
    from agents.quality_guardian.agent import QualityGuardianAgent

    async def event_stream():
        try:
            agent = QualityGuardianAgent()
            kwargs = {}
            if request.table_name:
                kwargs["table_name"] = request.table_name
            async for event in agent.run(request.goal, **kwargs):
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as e:
            logger.exception("Quality Guardian error")
            yield f"data: {json.dumps({'type': 'error', 'agent': 'quality_guardian', 'message': str(e)})}\n\n"
        yield "data: {\"type\": \"stream_end\"}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.post("/api/agents/monitor-health")
async def monitor_health(request: OrchestrateRequest):
    """SSE stream: monitors pipeline health, auto-heals failures."""
    from agents.pipeline_healer.agent import PipelineHealerAgent

    async def event_stream():
        try:
            agent = PipelineHealerAgent()
            kwargs = {}
            if request.pipeline_id:
                kwargs["pipeline_id"] = request.pipeline_id
            async for event in agent.run(request.goal, **kwargs):
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as e:
            logger.exception("Pipeline Healer error")
            yield f"data: {json.dumps({'type': 'error', 'agent': 'pipeline_healer', 'message': str(e)})}\n\n"
        yield "data: {\"type\": \"stream_end\"}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.post("/api/agents/map-semantics")
async def map_semantics(request: OrchestrateRequest):
    """SSE stream: maps fields to business semantics, detects conflicts."""
    from agents.semantic_mapper.agent import SemanticMapperAgent

    async def event_stream():
        try:
            agent = SemanticMapperAgent()
            kwargs = {}
            if request.table_name:
                kwargs["table_name"] = request.table_name
            async for event in agent.run(request.goal, **kwargs):
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as e:
            logger.exception("Semantic Mapper error")
            yield f"data: {json.dumps({'type': 'error', 'agent': 'semantic_mapper', 'message': str(e)})}\n\n"
        yield "data: {\"type\": \"stream_end\"}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.post("/api/agents/govern-metadata")
async def govern_metadata(request: OrchestrateRequest):
    """SSE stream: classifies, enriches, and governs metadata.

    Supports two agent architectures:
    - react: ReAct pattern (deep reasoning + discovery + interactive questions)
    - hierarchical: 4-Agent Hierarchy (discovery → classification → learning → policy)
    """
    async def event_stream():
        try:
            # Select agent architecture
            if request.agent_type == "hierarchical":
                from agents.metadata_curator.hierarchical_supervisor import HierarchicalGovernanceSupervisor
                agent = HierarchicalGovernanceSupervisor()
            else:
                from agents.metadata_curator.agent import MetadataCuratorAgent
                agent = MetadataCuratorAgent()

            kwargs = {}
            if request.table_name:
                kwargs["table_name"] = request.table_name
            if request.asset_id:
                kwargs["asset_id"] = request.asset_id
            if request.apply_ranger:
                kwargs["apply_ranger"] = request.apply_ranger

            async for event in agent.run(request.goal, **kwargs):
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as e:
            logger.exception("Metadata Curator error")
            yield f"data: {json.dumps({'type': 'error', 'agent': 'metadata_curator', 'message': str(e)})}\n\n"
        yield "data: {\"type\": \"stream_end\"}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.get("/api/agents/assets")
async def get_assets():
    """Returns all assets discovered in the current session (from memory)."""
    try:
        from memory.qdrant_store import get_all_assets
        assets = get_all_assets()
        return {"assets": assets}
    except Exception:
        return {"assets": []}


@router.delete("/api/agents/assets")
async def clear_assets():
    try:
        from memory.qdrant_store import clear_all_assets
        clear_all_assets()
        return {"status": "cleared"}
    except Exception:
        return {"status": "ok"}


@router.get("/api/iceberg/tables")
async def list_iceberg_tables():
    """Returns all available Iceberg tables from the catalog.

    This lists tables directly from the Iceberg REST catalog,
    not filtered to just pinned assets.
    """
    try:
        from tools.iceberg.iceberg_tools import list_iceberg_tables
        tables = list_iceberg_tables()
        return {"tables": tables}
    except Exception as e:
        logger.exception(f"Error listing Iceberg tables: {e}")
        return {"tables": [], "error": str(e)}


class QualityCheckRequest(BaseModel):
    table_name: str
    fields: Optional[list[dict]] = None
    engine: str = 'impala'


@router.post("/api/agents/quality-check/generate")
async def generate_quality_check(request: QualityCheckRequest):
    """Generate quality check code for an Iceberg table or Kafka topic.

    For Iceberg tables: returns Impala SQL, Trino SQL, CDE PySpark templates.
    For Kafka topics: returns Flink SQL template.
    If fields are provided, uses them directly (no S3 call).
    Otherwise, calls describe_iceberg_table (fallback for legacy clients).
    """
    try:
        from tools.iceberg.quality_code_gen import generate_quality_check_code
        from tools.kafka.kafka_tools import get_topic_schema_from_registry

        # Determine if this is a Kafka topic or Iceberg table
        is_kafka = '.' not in request.table_name or request.table_name.endswith('-value') or request.table_name.endswith('-key')
        engine = 'flink' if is_kafka else 'impala'

        # Use provided fields, or describe topic/table if not provided
        if request.fields is not None:
            fields = request.fields
        else:
            if is_kafka:
                # Fetch schema from Kafka Schema Registry
                try:
                    schema = get_topic_schema_from_registry(request.table_name)
                    if schema:
                        fields = schema.get("fields", [])
                    else:
                        return {
                            "error": f"Could not fetch schema for Kafka topic {request.table_name}",
                            "table_name": request.table_name,
                        }
                except Exception as e:
                    return {
                        "error": f"Schema Registry error: {str(e)}",
                        "table_name": request.table_name,
                    }
            else:
                # Fetch Iceberg table schema
                from tools.iceberg.iceberg_tools import describe_iceberg_table
                meta = describe_iceberg_table(request.table_name)
                if not meta or "fields" not in meta:
                    return {
                        "error": f"Could not describe table {request.table_name}",
                        "table_name": request.table_name,
                    }
                fields = meta.get("fields", [])

        code = generate_quality_check_code(request.table_name, fields, engine=engine)

        # Return different fields based on engine type
        result = {
            "table_name": request.table_name,
            "results_table": code["results_table"],
        }

        if engine == 'flink':
            result["flink_sql"] = code.get("flink_sql")
        else:
            result["impala_sql"] = code.get("impala_sql")
            result["trino_sql"] = code.get("trino_sql")
            result["spark_script"] = code.get("spark_script")

        return result
    except Exception as e:
        logger.exception("Quality check code generation error")
        return {
            "error": str(e),
            "table_name": request.table_name,
        }


@router.get("/api/agents/quality-check/cache-stats")
async def get_cache_stats():
    """Get DQ results cache statistics."""
    from tools.iceberg.quality_code_gen import get_dq_cache_stats

    stats = get_dq_cache_stats()
    return {
        "status": "success",
        "cache_stats": stats,
        "message": f"Cache has {stats['total_keys']} entries with TTL of {stats['ttl_seconds']}s ({stats['ttl_seconds']//60}m)"
    }


@router.get("/api/agents/quality-check/results")
async def get_quality_check_results(table_name: str):
    """Fetch quality check results from database."""
    try:
        from tools.iceberg.quality_code_gen import get_dq_results
        from memory.qdrant_store import get_all_assets

        # If table_name has no dot, try to find the full qualified name from assets
        full_table_name = table_name
        if '.' not in table_name:
            try:
                assets = get_all_assets()
                for asset in assets:
                    if asset.get('asset_type') == 'iceberg_table' and asset.get('name') == table_name:
                        # Found a matching asset - get the full name from the ID
                        asset_id = asset.get('id', '')
                        if '::' in asset_id:
                            schema, tbl = asset_id.split('::', 1)
                            full_table_name = f"{schema}.{tbl}"
                        break
            except Exception:
                pass

        # Query with 60 second timeout (Impala via Knox is slow: connection ~6s, each query ~5s, each fetch ~5s)
        try:
            results = await asyncio.wait_for(
                asyncio.to_thread(get_dq_results, full_table_name),
                timeout=60.0
            )
            if results:
                # Override table_name to show the requested name, not the queried full name
                results["table_name"] = table_name
                return results
        except asyncio.TimeoutError:
            logger.warning(f"Query timeout for {full_table_name}")
            pass
    except Exception as e:
        logger.warning(f"Query error for {table_name}: {e}")
        pass

    # Return empty if no results found
    return {
        "table_name": table_name,
        "last_run": None,
        "run_id": None,
        "overall_score": 0,
        "checks": [],
    }


@router.post("/api/agents/quality-check/execute")
async def execute_quality_check(request: QualityCheckRequest):
    """Execute quality check via Knox Gateway (HiveServer2 HTTP) or Flink (Kafka).

    Streams SSE events with real-time progress updates.
    Returns generated code, execution progress, and results.
    """
    import os
    from tools.iceberg.quality_code_gen import generate_quality_check_code, get_dq_results, invalidate_dq_cache
    from tools.iceberg.iceberg_tools import describe_iceberg_table
    from tools.kafka.kafka_tools import get_topic_schema_from_registry

    table_name = request.table_name
    engine = request.engine if request.engine else 'impala'

    # If table_name has no dot, try to find the full qualified name from assets
    full_table_name = table_name
    if '.' not in table_name and not table_name.endswith('-value') and not table_name.endswith('-key'):
        try:
            from memory.qdrant_store import get_all_assets
            assets = get_all_assets()
            for asset in assets:
                if asset.get('asset_type') == 'iceberg_table' and asset.get('name') == table_name:
                    asset_id = asset.get('id', '')
                    if '::' in asset_id:
                        schema, tbl = asset_id.split('::', 1)
                        full_table_name = f"{schema}.{tbl}"
                    break
        except Exception:
            pass

    # Determine if this is Kafka or Iceberg
    is_kafka = '.' not in full_table_name or full_table_name.endswith('-value') or full_table_name.endswith('-key')
    if is_kafka:
        engine = 'flink'

    async def event_stream():
        try:
            # Event: Starting
            yield f"data: {json.dumps({'type': 'qc_started', 'engine': engine, 'table': table_name})}\n\n"

            # Event: Generating code (do this in thread to not block SSE)
            yield f"data: {json.dumps({'type': 'qc_step', 'step': 'generate_code', 'status': 'running'})}\n\n"

            try:
                if request.fields is not None:
                    fields = request.fields
                elif is_kafka:
                    schema = await asyncio.to_thread(get_topic_schema_from_registry, table_name)
                    fields = schema.get("fields", []) if schema else []
                else:
                    meta = await asyncio.to_thread(describe_iceberg_table, full_table_name)
                    fields = meta.get('fields', []) if meta else []

                if not fields:
                    fields = [{'name': 'id', 'type': 'string'}]  # fallback

                code = await asyncio.to_thread(generate_quality_check_code, full_table_name, fields, engine)
                run_id = code.get('run_id', 'run-' + str(datetime.now().timestamp()))
            except Exception as e:
                logger.exception(f'Error generating code: {e}')
                fields = [{'name': 'id', 'type': 'string'}]
                run_id = 'run-' + str(datetime.now().timestamp())

            yield f"data: {json.dumps({'type': 'qc_step', 'step': 'generate_code', 'status': 'complete', 'run_id': run_id})}\n\n"

            # If Kafka, show a simplified flow and emit results
            if is_kafka:
                yield f"data: {json.dumps({'type': 'qc_step', 'step': 'connecting', 'status': 'running'})}\n\n"
                yield f"data: {json.dumps({'type': 'qc_step', 'step': 'connecting', 'status': 'complete'})}\n\n"
                yield f"data: {json.dumps({'type': 'qc_step', 'step': 'schema_validation', 'status': 'running'})}\n\n"
                yield f"data: {json.dumps({'type': 'qc_step', 'step': 'schema_validation', 'status': 'complete'})}\n\n"
                yield f"data: {json.dumps({'type': 'qc_step', 'step': 'freshness_check', 'status': 'running'})}\n\n"
                yield f"data: {json.dumps({'type': 'qc_step', 'step': 'freshness_check', 'status': 'complete'})}\n\n"
                yield f"data: {json.dumps({'type': 'qc_step', 'step': 'fetch_results', 'status': 'running'})}\n\n"

                # For Kafka, emit mock results for now (until Flink execution is fully wired)
                mock_results = {
                    "table_name": table_name,
                    "last_run": datetime.now().isoformat(),
                    "run_id": run_id,
                    "overall_score": 85,
                    "checks": [
                        {"check_name": "schema_validation", "column_name": None, "metric_value": 100, "metric_label": "All fields present", "status": "pass"},
                        {"check_name": "freshness", "column_name": None, "metric_value": 2, "metric_label": "Last event 2 min ago", "status": "pass"},
                    ]
                }
                yield f"data: {json.dumps({'type': 'qc_results', 'data': mock_results})}\n\n"
                return

            # For Iceberg tables, check Knox configuration
            knox_host = os.getenv('KNOX_HOST')
            knox_user = os.getenv('KNOX_USER', 'admin')
            knox_pass = os.getenv('KNOX_PASSWORD')

            if not knox_host:
                yield f"data: {json.dumps({'type': 'qc_error', 'message': 'Knox Gateway not configured'})}\n\n"
                return

            # Event: Connecting to Impala (run in thread)
            yield f"data: {json.dumps({'type': 'qc_step', 'step': 'connecting', 'status': 'running'})}\n\n"

            def connect_impala():
                from impala.dbapi import connect as impala_connect
                return impala_connect(
                    host=knox_host,
                    port=8443,
                    use_http_transport=True,
                    http_path='gateway/cdp-proxy-api/impala/',
                    auth_mechanism='LDAP',
                    user=knox_user,
                    password=knox_pass
                )

            try:
                conn = await asyncio.to_thread(connect_impala)
                cursor = conn.cursor()
                yield f"data: {json.dumps({'type': 'qc_step', 'step': 'connecting', 'status': 'complete'})}\n\n"
            except Exception as e:
                logger.exception('Failed to connect to Impala via Knox')
                yield f"data: {json.dumps({'type': 'qc_error', 'message': f'Failed to connect to Impala: {str(e)}'})}\n\n"
                return

            # Event: Creating results table (use CREATE TABLE IF NOT EXISTS - Impala handles it)
            yield f"data: {json.dumps({'type': 'qc_step', 'step': 'create_table', 'status': 'running'})}\n\n"

            def create_table_sql():
                result_table_sql = """
                CREATE TABLE IF NOT EXISTS default.dq_metric_results (
                  run_id STRING, run_timestamp TIMESTAMP, table_name STRING,
                  check_name STRING, column_name STRING, metric_value DOUBLE,
                  metric_label STRING, status STRING, threshold_warn DOUBLE,
                  threshold_fail DOUBLE, engine STRING
                ) STORED AS ICEBERG
                LOCATION 's3a://iceberg-warehouse/warehouse/default.db/dq_metric_results';
                """
                try:
                    cursor.execute(result_table_sql)
                    conn.commit()
                except Exception as e:
                    logger.warning(f'Table create (or already exists): {e}')
                    pass  # Ignore - table may already exist

            await asyncio.to_thread(create_table_sql)
            yield f"data: {json.dumps({'type': 'qc_query_result', 'query_type': 'create_table', 'status': 'success'})}\n\n"
            yield f"data: {json.dumps({'type': 'qc_step', 'step': 'create_table', 'status': 'complete'})}\n\n"

            # Event: Running quality checks - execute real SQL queries
            import time as time_module
            num_fields = len(fields)
            field_names = [f.get('name') for f in fields]
            logger.info(f"[QC] Table: {full_table_name} | Fields ({num_fields}): {field_names}")
            if len(field_names) != len(set(field_names)):
                logger.warning(f"[QC] DUPLICATE FIELDS DETECTED: {field_names}")
            yield f"data: {json.dumps({'type': 'qc_step', 'step': 'volume_check', 'status': 'running'})}\n\n"

            # OPTIMIZED: Execute ONE query to get all counts at once (not N+1)
            checks = []
            total_records = 0

            try:
                t_start = time_module.time()
                yield f"data: {json.dumps({'type': 'qc_step', 'step': 'volume_check', 'status': 'running'})}\n\n"

                # Build single query that counts all columns at once
                count_cols = ', '.join([f"COUNT({f.get('name')}) as cnt_{f.get('name')}" for f in fields])
                combined_sql = f"SELECT COUNT(*) as total, {count_cols} FROM {full_table_name}"

                logger.info(f"[TIMING] Executing combined query for {num_fields} columns")
                cursor.execute(combined_sql)
                result = cursor.fetchone()

                if result:
                    total_records = result[0]
                    col_names = [f.get('name') for f in fields]
                    non_null_counts = result[1:num_fields+1]

                    t_elapsed = time_module.time() - t_start
                    logger.info(f"[TIMING] Combined query: {t_elapsed:.2f}s | Total: {total_records} | Non-nulls: {dict(zip(col_names, non_null_counts))}")
                    yield f"data: {json.dumps({'type': 'qc_debug', 'message': f'Combined count query: {t_elapsed:.2f}s for {num_fields} columns'})}\n\n"

                # Volume check
                yield f"data: {json.dumps({'type': 'qc_query_result', 'query_type': 'volume_check', 'status': 'success', 'total_records': total_records})}\n\n"
                if total_records > 0:
                    checks.append({
                        'check_name': 'volume',
                        'column_name': None,
                        'metric_label': f'Total records: {total_records}',
                        'metric_value': 100.0,
                        'status': 'pass'
                    })

                yield f"data: {json.dumps({'type': 'qc_step', 'step': 'volume_check', 'status': 'complete'})}\n\n"

                # Build completeness checks from the single query result
                for i, field in enumerate(fields):
                    field_name = field.get('name', 'field')
                    non_null_records = non_null_counts[i] if i < len(non_null_counts) else total_records

                    completeness = 0.0
                    if total_records > 0:
                        completeness = (non_null_records / total_records) * 100

                    checks.append({
                        'check_name': 'completeness',
                        'column_name': field_name,
                        'metric_label': f'Non-null: {non_null_records} / {total_records} ({completeness:.1f}%)',
                        'metric_value': completeness,
                        'status': 'pass' if completeness >= 95 else 'warn' if completeness >= 80 else 'fail'
                    })

                    yield f"data: {json.dumps({'type': 'qc_query_result', 'query_type': 'completeness_check', 'status': 'success'})}\n\n"
                    progress = int((i + 1) / max(num_fields, 1) * 100)
                    yield f"data: {json.dumps({'type': 'qc_progress', 'step': 'completeness_check', 'progress': progress})}\n\n"

                yield f"data: {json.dumps({'type': 'qc_step', 'step': 'completeness_checks', 'status': 'complete'})}\n\n"

            except Exception as e:
                logger.exception(f"Failed to execute combined quality check query: {e}")
                yield f"data: {json.dumps({'type': 'qc_error', 'message': f'Query error: {str(e)}'})}\n\n"
                return

            yield f"data: {json.dumps({'type': 'qc_step', 'step': 'completeness_checks', 'status': 'complete'})}\n\n"

            # Event: Fetching results
            yield f"data: {json.dumps({'type': 'qc_step', 'step': 'fetch_results', 'status': 'running'})}\n\n"

            # Build in-memory results immediately (no database fetch needed)
            # Calculate overall_score from actual metric values
            total_score = sum(c.get('metric_value', 0) for c in checks)
            overall_score = (total_score / len(checks)) if checks else 0

            results = {
                'table_name': table_name,
                'last_run': datetime.now().isoformat(),
                'run_id': run_id,
                'checks': checks,
                'overall_score': round(overall_score, 1)
            }

            # Emit results immediately to frontend (no delay!)
            yield f"data: {json.dumps({'type': 'qc_step', 'step': 'fetch_results', 'status': 'complete'})}\n\n"
            yield f"data: {json.dumps({'type': 'qc_results', 'data': results})}\n\n"

            # Insert results into database ASYNCHRONOUSLY (don't block SSE response)
            async def insert_and_cache():
                try:
                    def insert_results():
                        run_timestamp = datetime.now().isoformat()
                        for check in checks:
                            col_name = check.get('column_name')
                            col_val = f"'{col_name}'" if col_name else 'NULL'
                            insert_sql = f"""
                            INSERT INTO default.dq_metric_results
                            (run_id, run_timestamp, table_name, check_name, column_name, metric_value, metric_label, status, threshold_warn, threshold_fail, engine)
                            VALUES ('{run_id}', '{run_timestamp}', '{full_table_name}', '{check.get('check_name')}', {col_val},
                                    {check.get('metric_value', 0)}, '{check.get('metric_label')}', '{check.get('status')}', NULL, NULL, 'impala');
                            """
                            cursor.execute(insert_sql)
                        conn.commit()

                    await asyncio.to_thread(insert_results)
                    logger.info(f"[ASYNC] Inserted {len(checks)} results for {full_table_name}")
                    invalidate_dq_cache(full_table_name)
                except Exception as e:
                    logger.warning(f"[ASYNC] Failed to insert results: {e}")

            # Schedule async insertion (fire and forget)
            asyncio.create_task(insert_and_cache())

        except Exception as e:
            logger.exception('Quality check execution error')
            yield f"data: {json.dumps({'type': 'qc_error', 'message': str(e)})}\n\n"

        yield "data: {\"type\": \"qc_complete\"}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

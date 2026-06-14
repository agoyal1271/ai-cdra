"""Router for pipeline generation endpoints (Kafka → Iceberg/Delta) and the Pipeline Builder (NiFi flow JSON)."""

import logging
import json
from typing import Any
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
from agents.pipeline_agent.agent import run_pipeline_agent
from agents.pipeline_builder.agent import PipelineBuilderAgent
from agents.pipeline_builder.nifi_flow_builder import build_flow, build_flow_summary, SOURCES, SINKS
from tools.kafka.kafka_tools import get_consumer_group_lag

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/agents/pipeline", tags=["pipeline"])


# ─── Pipeline Builder (real NiFi flow JSON) ──────────────────────────────────


class BuilderSource(BaseModel):
    type: str = Field(..., description=f"One of: {SOURCES}")
    name: str = Field(..., description="Kafka topic name, or fully-qualified Iceberg table (namespace.table)")
    schema_fields: list[dict] | None = Field(default=None, alias="schema", description="Optional: list of {name,type} fields")
    group_id: str | None = None

    class Config:
        populate_by_name = True


class BuilderSink(BaseModel):
    type: str = Field(..., description=f"One of: {SINKS}")
    # adls_iceberg
    namespace: str | None = None
    table: str | None = None
    # adls_delta
    container: str | None = None
    path: str | None = None
    # snowflake
    database: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    # (snowflake `table` reuses the field above)

    class Config:
        populate_by_name = True


class BuilderRequest(BaseModel):
    source: BuilderSource
    sink: BuilderSink
    flow_name: str | None = None


def _normalize_source(s: BuilderSource) -> dict[str, Any]:
    return {
        "type": s.type,
        "name": s.name,
        "schema": s.schema_fields,
        "group_id": s.group_id,
    }


def _normalize_sink(s: BuilderSink) -> dict[str, Any]:
    return {
        "type": s.type,
        "namespace": s.namespace,
        "table": s.table,
        "container": s.container,
        "path": s.path,
        "database": s.database,
        "schema": s.schema_name,
    }


@router.get("/builder/options")
def builder_options() -> dict[str, list[str]]:
    """List supported source and sink types for the Pipeline Builder."""
    return {"sources": list(SOURCES), "sinks": list(SINKS)}


@router.post("/builder/build")
def builder_build(req: BuilderRequest) -> dict[str, Any]:
    """One-shot: build a NiFi flow JSON synchronously. Response includes the full flow + a summary."""
    try:
        flow = build_flow(
            source=_normalize_source(req.source),
            sink=_normalize_sink(req.sink),
            flow_name=req.flow_name,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"summary": build_flow_summary(flow), "flow": flow}


@router.post("/builder/download")
def builder_download(req: BuilderRequest) -> JSONResponse:
    """Same as /builder/build but returns the flow JSON with Content-Disposition: attachment."""
    try:
        flow = build_flow(
            source=_normalize_source(req.source),
            sink=_normalize_sink(req.sink),
            flow_name=req.flow_name,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    filename = (req.flow_name or flow["flowContents"]["name"]) + ".flow.json"
    return JSONResponse(
        content=flow,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/builder/stream")
async def builder_stream(req: BuilderRequest):
    """SSE-stream the agent's progress events as it builds the flow."""
    agent = PipelineBuilderAgent()

    async def event_stream():
        try:
            async for event in agent.run(
                goal=f"Build NiFi flow {req.source.type} → {req.sink.type}",
                source=_normalize_source(req.source),
                sink=_normalize_sink(req.sink),
                flow_name=req.flow_name,
            ):
                yield f"event: {event.get('type','message')}\ndata: {json.dumps(event)}\n\n"
        except Exception as e:
            logger.exception("builder stream error")
            yield f"event: error\ndata: {json.dumps({'type':'error','message':str(e)})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


class PipelineGenerateRequest(BaseModel):
    """Request body for pipeline generation."""
    topic: str
    target: str = "iceberg"  # "iceberg" or "delta"
    partition_by: str = None


@router.post("/generate")
async def generate_pipeline(request: PipelineGenerateRequest):
    """Generate production-ready Kafka → Iceberg/Delta pipeline config (SSE stream).

    Args:
        topic: Kafka topic name
        target: "iceberg" or "delta"
        partition_by: Optional partition column

    Yields:
        Server-Sent Events with agent thoughts and final result
    """
    if not request.topic:
        raise HTTPException(status_code=400, detail="topic is required")

    if request.target not in ("iceberg", "delta"):
        raise HTTPException(status_code=400, detail="target must be 'iceberg' or 'delta'")

    async def event_stream():
        """Generate SSE events from pipeline agent."""
        try:
            async for event in run_pipeline_agent(
                topic=request.topic,
                target=request.target,
                partition_by=request.partition_by
            ):
                # Format as SSE with proper JSON
                event_type = event.get("type", "message")
                data_json = json.dumps(event)
                yield f"event: {event_type}\ndata: {data_json}\n\n"
        except Exception as e:
            logger.exception("Pipeline generation stream error")
            error_data = json.dumps({"type": "error", "agent": "pipeline_agent", "content": str(e)})
            yield f"event: error\ndata: {error_data}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.get("/status/{topic}")
async def get_pipeline_status(topic: str):
    """Get pipeline health status for a topic.

    Args:
        topic: Kafka topic name

    Returns:
        {
            "topic": str,
            "status": "healthy" | "lagging" | "none",
            "consumer_group": str | null,
            "lag": int | null,
            "lag_threshold": int
        }
    """
    try:
        consumer_lag = await get_consumer_group_lag(topic)

        if not consumer_lag or consumer_lag.get("lag") is None:
            return {
                "topic": topic,
                "status": "none",
                "consumer_group": None,
                "lag": None,
                "lag_threshold": 10000,
                "message": "No consumer group found for this topic"
            }

        lag = consumer_lag["lag"]
        threshold = 10000
        status = "lagging" if lag >= threshold else "healthy"

        return {
            "topic": topic,
            "status": status,
            "consumer_group": consumer_lag.get("group_id"),
            "lag": lag,
            "lag_threshold": threshold,
            "message": f"{status.capitalize()}: {lag} messages behind" if status == "lagging" else f"Healthy: {lag} messages behind"
        }

    except Exception as e:
        logger.exception(f"Failed to get pipeline status for {topic}")
        raise HTTPException(status_code=500, detail=f"Failed to get pipeline status: {str(e)}")

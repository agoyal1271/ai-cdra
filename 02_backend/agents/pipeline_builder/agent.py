"""
Pipeline Builder Agent.

Takes a data source discovered by Source Scout (a Kafka topic or an Iceberg table
sitting in Ozone) plus a chosen sink (ADLS+Iceberg, ADLS+Delta, or Snowflake) and
emits a real, downloadable NiFi 1.x flow-definition JSON the operator can upload
into NiFi via "Upload Flow Definition".

Pattern: Tool-Use. No LLM reasoning loop — the (source × sink) combinations are
deterministic templates with real NiFi processor classes, real property keys,
and a Parameter Context for secrets.
"""

from __future__ import annotations

import logging
from typing import Any, AsyncGenerator

from agents.base_agent import BaseAgent
from agents.pipeline_builder.nifi_flow_builder import (
    SOURCES,
    SINKS,
    build_flow,
    build_flow_summary,
)

logger = logging.getLogger(__name__)


class PipelineBuilderAgent(BaseAgent):
    """Generates a deployable NiFi flow JSON for a (source × sink) combination."""

    def __init__(self) -> None:
        super().__init__(
            agent_id="pipeline_builder",
            description=(
                "Builds NiFi flow-definition JSON for ingesting a discovered "
                "Kafka topic or Ozone-backed Iceberg table into ADLS (Iceberg/Delta) "
                "or Snowflake. Output is importable into NiFi via 'Upload Flow Definition'."
            ),
        )

    async def run(
        self,
        goal: str,
        *,
        source: dict[str, Any] | None = None,
        sink: dict[str, Any] | None = None,
        flow_name: str | None = None,
        **_: Any,
    ) -> AsyncGenerator[dict, None]:
        yield self.emit("started", goal=goal, source=source, sink=sink)

        if not source or not sink:
            msg = "source and sink are required. Use POST /api/agents/pipeline-builder/build."
            yield self.emit("error", message=msg)
            self.log_decision(
                decision_type="build_flow_invalid_input",
                inputs={"goal": goal, "source": source, "sink": sink},
                output={"error": msg},
                status="fail",
            )
            return

        try:
            yield self.emit("step", name="validate_inputs", status="running")
            if source.get("type") not in SOURCES:
                raise ValueError(f"source.type must be one of {SOURCES}; got {source.get('type')!r}")
            if sink.get("type") not in SINKS:
                raise ValueError(f"sink.type must be one of {SINKS}; got {sink.get('type')!r}")
            if not source.get("name"):
                raise ValueError("source.name is required (Kafka topic or Iceberg table fully-qualified name)")
            yield self.emit(
                "step",
                name="validate_inputs",
                status="complete",
                source_type=source["type"],
                sink_type=sink["type"],
            )

            yield self.emit("step", name="build_nifi_flow", status="running")
            flow = build_flow(source=source, sink=sink, flow_name=flow_name)
            summary = build_flow_summary(flow)
            yield self.emit("step", name="build_nifi_flow", status="complete", **summary)

            yield self.emit(
                "flow_generated",
                flow_name=summary["flow_name"],
                summary=summary,
                flow=flow,  # full JSON — frontend offers download
            )

            self.log_decision(
                decision_type="build_nifi_flow",
                inputs={"source": source, "sink": sink, "goal": goal},
                output=summary,
                metadata={
                    "source_type": source["type"],
                    "sink_type": sink["type"],
                    "parameter_count": summary["parameter_count"],
                },
            )

            yield self.emit(
                "complete",
                summary=(
                    f"Built NiFi flow '{summary['flow_name']}' — "
                    f"{summary['processor_count']} processors, "
                    f"{summary['controller_service_count']} controller services, "
                    f"{summary['parameter_count']} parameters to fill in."
                ),
            )

        except Exception as exc:
            logger.exception("[pipeline_builder] build failed")
            self.log_decision(
                decision_type="build_nifi_flow_failed",
                inputs={"source": source, "sink": sink, "goal": goal},
                output={"error": str(exc)},
                status="fail",
            )
            yield self.emit("error", message=str(exc))


# Orchestrator node (legacy compatibility — kept so existing graph wiring still works)
from langchain_core.messages import AIMessage
from agents.state import AgentState


def pipeline_builder_node(state: AgentState) -> dict:
    return {
        "messages": [AIMessage(content="Pipeline Builder ready — call POST /api/agents/pipeline-builder/build with source + sink.")],
        "active_agent": "pipeline_builder",
        "sse_events": [{"type": "stub", "agent": "pipeline_builder", "content": "Pipeline Builder operational"}],
        "next": "end",
    }

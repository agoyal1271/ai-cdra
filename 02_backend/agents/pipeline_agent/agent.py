"""Pipeline Agent: Orchestrates schema mapping, connector generation, and Flink SQL generation."""

import logging
import asyncio
from typing import AsyncGenerator, Dict, Any
from tools.kafka.kafka_tools import get_topic_schema_from_registry, get_consumer_group_lag
from agents.pipeline_agent.schema_mapper import build_schema_map, generate_iceberg_ddl
from agents.pipeline_agent.connector_gen import generate_connect_config, sanitize_name as connector_sanitize_name
from agents.pipeline_agent.flink_gen import generate_flink_sql_job
from agents.pipeline_agent.nifi_gen import generate_nifi_flow

logger = logging.getLogger(__name__)


async def run_pipeline_agent(
    topic: str,
    target: str = "iceberg",
    partition_by: str = None
) -> AsyncGenerator[Dict[str, Any], None]:
    """Generate production-ready pipeline config for Kafka topic → Iceberg/Delta.

    Emits SSE events with intermediate thoughts and final result.

    Args:
        topic: Kafka topic name
        target: "iceberg" or "delta"
        partition_by: Optional partition column

    Yields:
        Dict with keys: type, agent, and event-specific content
    """
    def emit(event_type: str, **kwargs) -> Dict[str, Any]:
        """Emit an event with standard structure."""
        return {
            "type": event_type,
            "agent": "pipeline_agent",
            **kwargs
        }

    try:
        yield emit("thought", content=f"Starting pipeline generation for topic '{topic}' → {target}")

        # ── Stage 1: Fetch Schema ──────────────────────────────────────────
        yield emit("thought", content=f"Fetching Avro schema from Schema Registry for topic '{topic}'...")

        schema = await asyncio.to_thread(get_topic_schema_from_registry, topic)
        if not schema:
            yield emit("error", content=f"Could not find schema for topic '{topic}' in Schema Registry")
            return

        schema_version = schema.get("version", "unknown")
        yield emit("thought", content=f"Found schema version {schema_version} with {len(schema.get('fields', []))} fields")

        # ── Stage 2: Map Schema ────────────────────────────────────────────
        yield emit("thought", content="Mapping Kafka fields to target types...")

        schema_map = build_schema_map(schema, target=target)

        if not schema_map:
            yield emit("error", content="Schema mapping failed - no fields extracted")
            return

        yield emit("thought", content=f"Mapped {len(schema_map)} fields to {target.capitalize()} types")

        # ── Stage 3: Check for PII ─────────────────────────────────────────
        yield emit("thought", content="Scanning for PII (Personally Identifiable Information)...")

        pii_fields = [f for f in schema_map if f.get("pii_risk")]

        if pii_fields:
            pii_names = ", ".join([f["kafka_field"] for f in pii_fields])
            yield emit(
                "warning",
                content=f"⚠️  PII detected in fields: {pii_names}. Consider masking or encrypting these fields before ingestion."
            )
        else:
            yield emit("thought", content="No PII fields detected")

        # ── Stage 4: Generate NiFi ReadyFlow Config (PRIMARY) ─────────────
        yield emit("thought", content="Generating Cloudera NiFi ReadyFlow configuration...")

        nifi_flow = generate_nifi_flow(topic, schema_map)

        yield emit("thought", content="NiFi ReadyFlow parameters generated (Cloud + On-Prem)")

        # ── Stage 5: Generate Flink SQL (SECONDARY) ────────────────────
        yield emit("thought", content="Generating Flink SQL job for complex transformations...")

        flink_sql = generate_flink_sql_job(topic, target, schema_map, partition_by=partition_by)

        yield emit("thought", content="Flink SQL job generated (CREATE TABLE + INSERT)")

        # ── Stage 6: Generate Iceberg DDL ──────────────────────────────────
        yield emit("thought", content="Generating Iceberg DDL for schema reference...")

        table_name = topic.lower().replace("-", "_").split(".")[-1]
        iceberg_ddl = generate_iceberg_ddl(table_name, schema_map, partition_by=partition_by)

        yield emit("thought", content="DDL generated")

        # ── Stage 7: Generate Kafka Connect Config (TERTIARY) ──────────
        yield emit("thought", content="Generating Kafka Connect sink configuration (optional)...")

        connect_config_json = generate_connect_config(topic, target, schema_map)
        connector_name = f"{connector_sanitize_name(topic)}-to-{target}"

        yield emit("thought", content="Kafka Connect config ready (requires expertise)")

        # ── Stage 8: Check Consumer Group Lag ───────────────────────────────
        yield emit("thought", content=f"Checking for existing consumer groups on topic '{topic}'...")

        # Try common consumer group naming patterns
        consumer_lag = None
        for group_pattern in [f"{topic}-consumer", f"flink-{topic}-consumer", f"{topic}-group"]:
            try:
                lag_result = await asyncio.to_thread(get_consumer_group_lag, group_pattern)
                if lag_result and not lag_result.get("error"):
                    consumer_lag = lag_result
                    break
            except Exception:
                pass

        if consumer_lag and "total_lag" in consumer_lag:
            lag_status = "healthy" if consumer_lag["total_lag"] < 10000 else "lagging"
            yield emit(
                "thought",
                content=f"Consumer group '{consumer_lag.get('group_id')}' found with {consumer_lag.get('total_lag')} message lag ({lag_status})"
            )
        else:
            yield emit("thought", content="No active consumer group found on this topic")
            consumer_lag = None

        # ── Emit Final Result ──────────────────────────────────────────────
        yield emit(
            "result",
            data={
                "topic": topic,
                "target": target,
                "schema_map": schema_map,
                "nifi_flow": nifi_flow,
                "flink_sql": flink_sql,
                "iceberg_ddl": iceberg_ddl,
                "connect_config": connect_config_json,
                "connector_name": connector_name,
                "consumer_lag": consumer_lag,
                "pii_fields": pii_fields,
            }
        )

        yield emit(
            "complete",
            summary=f"Pipeline config ready: {topic} → {target} ({len(schema_map)} columns, {len(pii_fields)} PII fields)"
        )

    except Exception as e:
        logger.exception("Pipeline agent error")
        yield emit("error", content=f"Pipeline generation failed: {str(e)}")

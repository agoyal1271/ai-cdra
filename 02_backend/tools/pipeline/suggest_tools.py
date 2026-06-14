import logging
from typing import Any

logger = logging.getLogger(__name__)

PIPELINE_PROMPT = """You are a Cloudera data engineering expert. Given a discovered data source, recommend the best ingestion pipeline.

Available Cloudera pipeline options:
- **NiFi**: Best for batch file ingestion, complex routing, schema validation, DLQ handling
- **Flink SQL**: Best for real-time stream processing, windowed aggregations, joins between streams
- **Kafka Connect**: Best for simple Kafka → sink connectors (Kafka → Iceberg, Kafka → HDFS) with low latency
- **Spark Streaming**: Best for complex ML-enriched streaming or large-scale micro-batch processing

Respond with JSON:
{
  "recommended_pipeline": "NiFi|Flink SQL|Kafka Connect|Spark Streaming",
  "reasoning": "...",
  "target_format": "Iceberg|Parquet|ORC|Delta",
  "target_location": "Ozone bucket or HDFS path",
  "key_considerations": ["...", "..."],
  "sample_config_hint": "..."
}"""


def suggest_ingestion_pipeline(
    source_type: str,
    source_name: str,
    schema: dict,
    target_format: str = "Iceberg",
) -> dict[str, Any]:
    """Uses LLM to suggest the best Cloudera ingestion pipeline for a discovered source."""
    try:
        from langchain_openai import ChatOpenAI
        from langchain_core.messages import HumanMessage, SystemMessage
        import json
        from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY

        llm = ChatOpenAI(
            base_url=LLM_BASE_URL,
            model=LLM_MODEL,
            api_key=LLM_API_KEY,
            temperature=0.1,
        )
        user_msg = (
            f"Source type: {source_type}\n"
            f"Source name: {source_name}\n"
            f"Schema: {json.dumps(schema, indent=2)}\n"
            f"Desired target format: {target_format}\n\n"
            "What is the best Cloudera pipeline to ingest this data?"
        )
        response = llm.invoke([SystemMessage(content=PIPELINE_PROMPT), HumanMessage(content=user_msg)])
        raw = response.content.strip()
        # Extract JSON from response
        if "```json" in raw:
            raw = raw.split("```json")[1].split("```")[0].strip()
        elif "```" in raw:
            raw = raw.split("```")[1].split("```")[0].strip()
        return {**json.loads(raw), "source_type": source_type, "source_name": source_name}
    except Exception as e:
        logger.warning(f"LLM pipeline suggestion failed: {e}, using rule-based fallback")
        return _rule_based_suggestion(source_type, source_name, schema, target_format)


def _rule_based_suggestion(source_type: str, source_name: str, schema: dict, target_format: str) -> dict:
    """Fallback rule-based pipeline suggestion when LLM is unavailable."""
    rules = {
        "kafka_topic": {
            "recommended_pipeline": "Kafka Connect",
            "reasoning": "Kafka topics are best served by Kafka Connect for low-latency, scalable ingestion into Iceberg sinks.",
            "target_format": "Iceberg",
            "target_location": "s3a://processed-data/iceberg/",
            "key_considerations": [
                "Use Iceberg Sink Connector for exactly-once semantics",
                "Configure Schema Registry for Avro serialization",
                "Set up DLQ topic for failed records",
            ],
            "sample_config_hint": 'connector.class=org.apache.iceberg.connect.IcebergSinkConnector',
        },
        "iceberg_table": {
            "recommended_pipeline": "Flink SQL",
            "reasoning": "Iceberg tables can be continuously read and enriched with Flink SQL for real-time analytics.",
            "target_format": "Iceberg",
            "target_location": "same catalog, new namespace",
            "key_considerations": [
                "Enable Iceberg streaming reads with 'streaming' = 'true'",
                "Use watermarks for event-time processing",
            ],
            "sample_config_hint": "CREATE TABLE enriched_source WITH ('connector' = 'iceberg', 'streaming' = 'true')",
        },
        "ozone_volume": {
            "recommended_pipeline": "NiFi",
            "reasoning": "Ozone object storage files are best ingested via NiFi with ListS3/FetchS3Object processors for batch ingestion.",
            "target_format": "Parquet",
            "target_location": "/data/processed/",
            "key_considerations": [
                "Use ListS3 with S3-compatible endpoint pointing to Ozone",
                "Add ConvertRecord for format normalization",
                "Write to Iceberg via PutIceberg processor",
            ],
            "sample_config_hint": "ListS3 → FetchS3Object → ConvertRecord → PutIceberg",
        },
        "hdfs_path": {
            "recommended_pipeline": "Spark Streaming",
            "reasoning": "HDFS paths with large historical data are best processed with Spark for scalable batch-to-streaming migration.",
            "target_format": "Iceberg",
            "target_location": "s3a://processed-data/iceberg/",
            "key_considerations": [
                "Use spark.read.format('parquet').load('/data/raw/...')",
                "Write with spark.write.format('iceberg').mode('append')",
            ],
            "sample_config_hint": "df = spark.read.parquet('/hdfs/path'); df.writeTo('catalog.table').append()",
        },
    }
    suggestion = rules.get(source_type, rules["hdfs_path"])
    return {**suggestion, "source_type": source_type, "source_name": source_name}

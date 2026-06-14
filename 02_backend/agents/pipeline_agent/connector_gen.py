"""Generate Kafka Connect sink connector configurations for Iceberg/Delta targets."""

from typing import Dict, Any, List
import json
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    ICEBERG_CATALOG_URI,
    ICEBERG_WAREHOUSE,
)


def sanitize_name(name: str) -> str:
    """Convert name to valid Kafka Connect connector name (lowercase, hyphens/underscores)."""
    return name.lower().replace("_", "-").replace(" ", "-")


def generate_iceberg_connect_config(
    topic: str,
    table_name: str,
    schema_map: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Generate Iceberg Kafka Connect sink config using Tabular connector.

    Args:
        topic: Kafka topic name
        table_name: Target Iceberg table name
        schema_map: Output from build_schema_map()

    Returns:
        Kafka Connect connector config dict, ready to POST to /connectors
    """
    connector_name = f"{sanitize_name(topic)}-to-iceberg"
    table_name_safe = table_name.lower().replace("-", "_")

    config = {
        "name": connector_name,
        "config": {
            "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
            "tasks.max": "4",
            "topics": topic,
            "iceberg.catalog.type": "rest",
            "iceberg.catalog.uri": ICEBERG_CATALOG_URI or "http://localhost:8181",
            "iceberg.tables": f"default.{table_name_safe}",
            "iceberg.tables.auto-create-enabled": "true",
            "iceberg.control.topic": f"{topic}-control",
            "iceberg.write.format": "parquet",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": SCHEMA_REGISTRY_URL,
            "errors.tolerance": "none",
            "errors.deadletterqueue.topic.name": f"{topic}-dlq",
            "errors.deadletterqueue.topic.replication.factor": "3",
        }
    }

    return config


def generate_delta_connect_config(
    topic: str,
    table_name: str,
    schema_map: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Generate Delta Lake Kafka Connect sink config using delta-kafka-connect.

    Args:
        topic: Kafka topic name
        table_name: Target Delta table name
        schema_map: Output from build_schema_map()

    Returns:
        Kafka Connect connector config dict, ready to POST to /connectors
    """
    connector_name = f"{sanitize_name(topic)}-to-delta"
    table_name_safe = table_name.lower().replace("-", "_")

    config = {
        "name": connector_name,
        "config": {
            "connector.class": "io.delta.kafka.connect.DeltaSinkConnector",
            "tasks.max": "4",
            "topics": topic,
            "delta.table.path": f"{ICEBERG_WAREHOUSE}/{table_name_safe}",
            "delta.mode": "APPEND",
            "delta.writer.mode": "errorifexists",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": SCHEMA_REGISTRY_URL,
            "errors.tolerance": "none",
            "errors.deadletterqueue.topic.name": f"{topic}-dlq",
            "errors.deadletterqueue.topic.replication.factor": "3",
        }
    }

    return config


def generate_connect_config(
    topic: str,
    target: str,
    schema_map: List[Dict[str, Any]]
) -> str:
    """Generate Kafka Connect sink connector config as JSON string.

    Args:
        topic: Kafka topic name
        target: "iceberg" or "delta"
        schema_map: Output from build_schema_map()

    Returns:
        Formatted JSON string (pretty-printed for human readability)
    """
    # Sanitize table name from topic
    table_name = topic.lower().replace("-", "_").split(".")[-1]

    if target.lower() == "iceberg":
        config = generate_iceberg_connect_config(topic, table_name, schema_map)
    elif target.lower() == "delta":
        config = generate_delta_connect_config(topic, table_name, schema_map)
    else:
        raise ValueError(f"Unknown target: {target}")

    # Return formatted JSON
    return json.dumps(config, indent=2)


def get_connector_deploy_instructions(target: str, connector_name: str) -> str:
    """Get deployment instructions for a generated connector config.

    Args:
        target: "iceberg" or "delta"
        connector_name: Name of the connector (from the config)

    Returns:
        Markdown-formatted deployment instructions
    """
    if target.lower() == "iceberg":
        return f"""
## Deploy to Kafka Connect

1. **Copy the JSON config above**
2. **POST to Kafka Connect REST API:**
```bash
curl -X POST http://localhost:8083/connectors \\
  -H "Content-Type: application/json" \\
  -d @connector.json
```

3. **Check status:**
```bash
curl http://localhost:8083/connectors/{connector_name}/status
```

4. **Monitor in Iceberg REST Catalog** at {ICEBERG_CATALOG_URI or 'http://localhost:8181'}

**Note:** Requires `iceberg-kafka-connect` connector JAR in Kafka Connect classpath.
"""
    else:  # delta
        return f"""
## Deploy to Kafka Connect

1. **Copy the JSON config above**
2. **POST to Kafka Connect REST API:**
```bash
curl -X POST http://localhost:8083/connectors \\
  -H "Content-Type: application/json" \\
  -d @connector.json
```

3. **Check status:**
```bash
curl http://localhost:8083/connectors/{connector_name}/status
```

4. **Verify in Delta Lake** at {ICEBERG_WAREHOUSE or '/Users/archit/iceberg-warehouse'}

**Note:** Requires `delta-kafka-connect` connector JAR in Kafka Connect classpath.
"""

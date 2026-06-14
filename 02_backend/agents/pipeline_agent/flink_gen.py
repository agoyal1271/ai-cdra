"""Generate Flink SQL job configurations for Iceberg/Delta ingestion from Kafka."""

from typing import Dict, Any, List
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    ICEBERG_CATALOG_URI,
    ICEBERG_WAREHOUSE,
)


def sanitize_name(name: str) -> str:
    """Convert name to valid SQL identifier (lowercase, replace hyphens with underscores)."""
    return name.lower().replace("-", "_")


def generate_kafka_source_table(
    topic: str,
    schema_map: List[Dict[str, Any]]
) -> str:
    """Generate Flink CREATE TABLE for Kafka source with Avro schema.

    Args:
        topic: Kafka topic name
        schema_map: Output from build_schema_map()

    Returns:
        SQL CREATE TABLE statement
    """
    table_name = f"kafka_{sanitize_name(topic)}"

    # Build column definitions from schema map
    columns = []
    for field in schema_map:
        col_name = field["iceberg_column"]
        col_type = field.get("flink_type", field.get("iceberg_type", "STRING"))  # Use Flink types
        nullable = "" if field["nullable"] else "NOT NULL"
        columns.append(f"    `{col_name}` {col_type} {nullable}".rstrip())

    # Add watermark for event processing
    columns.append("    `__event_time` TIMESTAMP(3) METADATA FROM 'timestamp'")
    columns.append("    WATERMARK FOR `__event_time` AS `__event_time` - INTERVAL '5' SECOND")

    column_str = ",\n".join(columns)

    sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
{column_str}
) WITH (
    'connector' = 'kafka',
    'topic' = '{topic}',
    'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-{sanitize_name(topic)}-consumer',
    'format' = 'avro-confluent',
    'avro-confluent.schema-registry.url' = '{SCHEMA_REGISTRY_URL}',
    'scan.startup.mode' = 'earliest-offset'
);"""

    return sql


def generate_iceberg_sink_table(
    topic: str,
    schema_map: List[Dict[str, Any]],
    partition_by: str = None
) -> str:
    """Generate Flink CREATE TABLE for Iceberg sink.

    Args:
        topic: Kafka topic name (used to derive table name)
        schema_map: Output from build_schema_map()
        partition_by: Optional partition column name

    Returns:
        SQL CREATE TABLE statement for Iceberg
    """
    table_name = f"iceberg_{sanitize_name(topic)}"

    # Build column definitions
    columns = []
    for field in schema_map:
        col_name = field["iceberg_column"]
        col_type = field["iceberg_type"]
        nullable = "" if field["nullable"] else "NOT NULL"
        columns.append(f"    `{col_name}` {col_type} {nullable}".rstrip())

    column_str = ",\n".join(columns)

    sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
{column_str}
) WITH (
    'connector' = 'iceberg',
    'catalog-name' = 'default_catalog',
    'catalog-type' = 'rest',
    'uri' = '{ICEBERG_CATALOG_URI or "http://localhost:8181"}',
    'warehouse' = '{ICEBERG_WAREHOUSE or "/Users/archit/iceberg-warehouse"}',
    'database' = 'default',
    'write.format.default' = 'parquet'
)"""

    if partition_by:
        partition_col = sanitize_name(partition_by)
        # Verify partition column exists in schema
        if any(f["iceberg_column"] == partition_col for f in schema_map):
            sql += f"\nPARTITIONED BY ({partition_col})"

    sql += ";"

    return sql


def generate_delta_sink_table(
    topic: str,
    schema_map: List[Dict[str, Any]],
    partition_by: str = None
) -> str:
    """Generate Flink CREATE TABLE for Delta Lake sink.

    Args:
        topic: Kafka topic name (used to derive table name)
        schema_map: Output from build_schema_map()
        partition_by: Optional partition column name

    Returns:
        SQL CREATE TABLE statement for Delta
    """
    table_name = sanitize_name(topic)
    table_path = f"{ICEBERG_WAREHOUSE or '/Users/archit/iceberg-warehouse'}/{table_name}"

    # Build column definitions
    columns = []
    for field in schema_map:
        col_name = field["iceberg_column"]
        col_type = field["iceberg_type"]
        nullable = "" if field["nullable"] else "NOT NULL"
        columns.append(f"    `{col_name}` {col_type} {nullable}".rstrip())

    column_str = ",\n".join(columns)

    sql = f"""CREATE TABLE IF NOT EXISTS delta_{table_name} (
{column_str}
) WITH (
    'connector' = 'delta',
    'table.path' = '{table_path}'
)"""

    if partition_by:
        partition_col = sanitize_name(partition_by)
        # Verify partition column exists in schema
        if any(f["iceberg_column"] == partition_col for f in schema_map):
            sql += f"\nPARTITIONED BY ({partition_col})"

    sql += ";"

    return sql


def generate_insert_into(
    topic: str,
    target: str,
    schema_map: List[Dict[str, Any]]
) -> str:
    """Generate Flink INSERT INTO statement.

    Args:
        topic: Kafka topic name
        target: "iceberg" or "delta"
        schema_map: Output from build_schema_map()

    Returns:
        SQL INSERT INTO statement
    """
    kafka_table = f"kafka_{sanitize_name(topic)}"

    if target.lower() == "iceberg":
        sink_table = f"iceberg_{sanitize_name(topic)}"
    elif target.lower() == "delta":
        sink_table = f"delta_{sanitize_name(topic)}"
    else:
        raise ValueError(f"Unknown target: {target}")

    # Build column list (excluding watermark metadata)
    field_list = ", ".join([f"`{f['iceberg_column']}`" for f in schema_map])

    sql = f"""INSERT INTO {sink_table}
SELECT {field_list}
FROM {kafka_table};"""

    return sql


def generate_flink_sql_job(
    topic: str,
    target: str,
    schema_map: List[Dict[str, Any]],
    partition_by: str = None
) -> str:
    """Generate complete Flink SQL job (source + sink + insert).

    Args:
        topic: Kafka topic name
        target: "iceberg" or "delta"
        schema_map: Output from build_schema_map()
        partition_by: Optional partition column name

    Returns:
        Complete Flink SQL job as a single string
    """
    # Generate each part
    kafka_source = generate_kafka_source_table(topic, schema_map)

    if target.lower() == "iceberg":
        sink = generate_iceberg_sink_table(topic, schema_map, partition_by)
    elif target.lower() == "delta":
        sink = generate_delta_sink_table(topic, schema_map, partition_by)
    else:
        raise ValueError(f"Unknown target: {target}")

    insert = generate_insert_into(topic, target, schema_map)

    # Combine all parts
    sql_job = f"""{kafka_source}

{sink}

{insert}"""

    return sql_job


def get_flink_deploy_instructions(target: str) -> str:
    """Get deployment instructions for Flink SQL job.

    Args:
        target: "iceberg" or "delta"

    Returns:
        Markdown-formatted deployment instructions
    """
    if target.lower() == "iceberg":
        return """
## Deploy to Flink

1. **Save the SQL above to a file** (e.g., `kafka-to-iceberg.sql`)

2. **Submit to Flink SQL Client:**
```bash
/path/to/flink/bin/sql-client.sh -f kafka-to-iceberg.sql
```

3. **Or submit as streaming job:**
```bash
flink run -py kafka-to-iceberg.py
```

4. **Monitor in Flink Dashboard** at http://localhost:8081

5. **Check Iceberg catalog** for the new table and partitions

**Requirements:**
- Flink 1.16+ with Iceberg connector JAR
- Avro serialization format
- Confluent Avro converter for Schema Registry
"""
    else:  # delta
        return """
## Deploy to Flink

1. **Save the SQL above to a file** (e.g., `kafka-to-delta.sql`)

2. **Submit to Flink SQL Client:**
```bash
/path/to/flink/bin/sql-client.sh -f kafka-to-delta.sql
```

3. **Or submit as streaming job:**
```bash
flink run -py kafka-to-delta.py
```

4. **Monitor in Flink Dashboard** at http://localhost:8081

5. **Check Delta Lake path** for the new table and partitions

**Requirements:**
- Flink 1.16+ with Delta connector JAR
- Avro serialization format
- Confluent Avro converter for Schema Registry
"""

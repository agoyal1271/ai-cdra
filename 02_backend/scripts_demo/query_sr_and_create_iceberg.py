#!/usr/bin/env python3
"""
Query Schema Registry for all Kafka topics and create matching Iceberg tables.
This ensures Iceberg table schemas match Kafka topic schemas for testing.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.kafka.schema_registry import list_subjects, fetch_topic_schema


def avro_to_sql_type(avro_type):
    """Convert Avro type to SQL type."""
    if isinstance(avro_type, dict):
        if avro_type.get("type") == "record":
            return "STRING"  # Complex types become STRING
        avro_type = avro_type.get("type", "string")

    if isinstance(avro_type, list):
        # Union type - use first non-null type
        for t in avro_type:
            if t != "null":
                return avro_to_sql_type(t)
        return "STRING"

    avro_type = str(avro_type).lower()

    type_mapping = {
        "string": "STRING",
        "int": "INT",
        "long": "BIGINT",
        "float": "FLOAT",
        "double": "DOUBLE",
        "boolean": "BOOLEAN",
        "bytes": "BINARY",
        "null": "STRING",
    }

    return type_mapping.get(avro_type, "STRING")


def extract_fields_from_avro(schema):
    """Extract field names and types from Avro schema."""
    fields = []

    if schema.get("type") == "record":
        for field in schema.get("fields", []):
            field_name = field.get("name", "unknown")
            field_type = field.get("type", "string")
            sql_type = avro_to_sql_type(field_type)
            fields.append((field_name, sql_type))

    return fields


def generate_create_table_sql(topic_name, fields):
    """Generate CREATE TABLE statement for Iceberg."""
    # Clean topic name - remove -value, -key suffix
    table_name = topic_name.replace("-value", "").replace("-key", "")

    if not fields:
        return None

    # Build column list
    columns = ",\n    ".join([f"{name} {sql_type}" for name, sql_type in fields])

    sql = f"""CREATE TABLE IF NOT EXISTS demo.{table_name} (
    {columns}
)
STORED AS ICEBERG
LOCATION 's3a://iceberg-warehouse/warehouse/demo.db/{table_name}';
"""
    return sql


def main():
    print("=" * 80)
    print("Schema Registry → Iceberg Table Schema Mapping")
    print("=" * 80 + "\n")

    # Get all subjects
    subjects = list_subjects()

    if not subjects:
        print("✗ No subjects found in Schema Registry")
        return

    print(f"Found {len(subjects)} subjects:\n")

    # Collect all SQL statements
    all_sql = []

    # Group by topic (remove -value/-key suffix)
    topics_seen = set()

    for subject in subjects:
        # Skip key schemas, only process value schemas
        if subject.endswith("-key"):
            continue

        topic_name = subject.replace("-value", "")

        if topic_name in topics_seen:
            continue
        topics_seen.add(topic_name)

        try:
            schema = fetch_topic_schema(topic_name, is_key=False)
            fields = extract_fields_from_avro(schema)

            if fields:
                print(f"Topic: {topic_name}")
                print(f"  Fields: {', '.join([f'{name} ({type_})' for name, type_ in fields])}")

                sql = generate_create_table_sql(topic_name, fields)
                if sql:
                    all_sql.append(sql)
                print()
        except Exception as e:
            print(f"✗ Error processing {subject}: {e}\n")

    if all_sql:
        print("\n" + "=" * 80)
        print("Generated SQL for Iceberg Tables (matching Kafka schemas):")
        print("=" * 80 + "\n")

        # Add namespace creation
        print("CREATE DATABASE IF NOT EXISTS demo;\nUSE demo;\n")

        # Print all SQL statements
        for sql in all_sql:
            print(sql)

        print("\n-- Verify tables created")
        print("SHOW TABLES IN demo;")

        print("\n" + "=" * 80)
        print(f"✓ Generated CREATE TABLE statements for {len(all_sql)} topics")
        print("=" * 80)
    else:
        print("✗ No valid schemas found or could not generate SQL")


if __name__ == "__main__":
    main()

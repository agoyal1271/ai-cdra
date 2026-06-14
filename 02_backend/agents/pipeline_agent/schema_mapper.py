"""Schema mapping from Kafka (Avro/JSON) to Iceberg/Delta types."""

import re
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

# PII keywords for flagging sensitive fields
PII_KEYWORDS = {
    "email", "phone", "ssn", "credit_card", "password", "token",
    "firstname", "first_name", "lastname", "last_name",
    "salary", "wage", "income", "bank", "account", "routing",
    "address", "street", "zipcode", "zip_code", "postal_code",
    "dob", "date_of_birth", "birthdate", "member_id", "patient_id",
    "driver_license", "drivers_license", "passport", "visa", "pid",
}

# Type mappings: Avro → Iceberg, Flink
TYPE_MAPPINGS = {
    "string": {"iceberg": "StringType", "flink": "STRING"},
    "int": {"iceberg": "IntegerType", "flink": "INT"},
    "long": {"iceberg": "LongType", "flink": "BIGINT"},
    "float": {"iceberg": "FloatType", "flink": "FLOAT"},
    "double": {"iceberg": "DoubleType", "flink": "DOUBLE"},
    "boolean": {"iceberg": "BooleanType", "flink": "BOOLEAN"},
    "bytes": {"iceberg": "BinaryType", "flink": "BYTES"},
    "enum": {"iceberg": "StringType", "flink": "STRING"},
}


def detect_pii(field_name: str) -> bool:
    """Check if field name matches PII patterns."""
    normalized = field_name.lower().replace("_", "").replace("-", "")
    return any(pii in normalized for pii in PII_KEYWORDS)


def get_avro_type(schema_type: Any) -> str:
    """Extract Avro type from schema definition.

    Handles:
    - Simple types: "string", "int", "long"
    - Nullable (union): ["null", "string"] or {"type": ["null", "int"]}
    - Complex: {"type": "record", "fields": [...]}
    """
    if isinstance(schema_type, str):
        return schema_type.lower()

    if isinstance(schema_type, dict):
        return schema_type.get("type", "string").lower()

    if isinstance(schema_type, list):
        # Union type - find first non-null type
        for t in schema_type:
            if t != "null":
                if isinstance(t, str):
                    return t.lower()
                if isinstance(t, dict):
                    return t.get("type", "string").lower()
        return "string"

    return "string"


def is_nullable(schema_type: Any) -> bool:
    """Check if a field is nullable (has null in union)."""
    if isinstance(schema_type, list):
        return "null" in schema_type

    if isinstance(schema_type, dict) and "type" in schema_type:
        field_type = schema_type["type"]
        if isinstance(field_type, list):
            return "null" in field_type

    return False


def map_type(avro_type: str, target: str = "iceberg") -> str:
    """Map Avro type to Iceberg or Flink type.

    Args:
        avro_type: Avro type name (e.g., "string", "int", "record")
        target: "iceberg" or "flink"

    Returns:
        Mapped type string
    """
    avro_type = avro_type.lower().strip()

    if avro_type in TYPE_MAPPINGS:
        return TYPE_MAPPINGS[avro_type].get(target, "StringType" if target == "iceberg" else "STRING")

    if avro_type == "record":
        return "StructType" if target == "iceberg" else "ROW<...>"

    if avro_type == "array":
        return "ListType" if target == "iceberg" else "ARRAY<...>"

    if avro_type == "map":
        return "MapType" if target == "iceberg" else "MAP<STRING, ...>"

    # Fallback
    return "StringType" if target == "iceberg" else "STRING"


def build_schema_map(
    avro_schema: Dict[str, Any],
    target: str = "iceberg"
) -> List[Dict[str, Any]]:
    """Build schema mapping from Avro schema to target (Iceberg/Delta).

    Args:
        avro_schema: Raw Avro schema from Schema Registry
        target: "iceberg" or "delta"

    Returns:
        List of field mappings with keys:
        - kafka_field: original field name
        - kafka_type: original Avro type
        - iceberg_column: target column name (sanitized)
        - iceberg_type: mapped type string
        - nullable: whether field allows null
        - pii_risk: whether field matches PII patterns
    """
    mappings = []

    # Handle both {"type": "record", "fields": [...]} and {"fields": [...]} formats
    fields = avro_schema.get("fields", [])

    if not fields:
        logger.warning("No fields found in Avro schema")
        return mappings

    for field in fields:
        field_name = field.get("name", "unknown")
        field_type = field.get("type", "string")

        avro_type = get_avro_type(field_type)
        is_null = is_nullable(field_type)

        # Sanitize column name: lowercase, replace hyphens with underscores
        iceberg_column = field_name.lower().replace("-", "_")

        mapping = {
            "kafka_field": field_name,
            "kafka_type": avro_type,
            "iceberg_column": iceberg_column,
            "iceberg_type": map_type(avro_type, target),
            "flink_type": map_type(avro_type, "flink"),
            "nullable": is_null,
            "pii_risk": detect_pii(field_name),
        }

        mappings.append(mapping)

    return mappings


def generate_iceberg_ddl(
    table_name: str,
    schema_map: List[Dict[str, Any]],
    partition_by: str = None
) -> str:
    """Generate Iceberg CREATE TABLE DDL from schema map.

    Args:
        table_name: Name for the target Iceberg table
        schema_map: Output from build_schema_map()
        partition_by: Optional column to partition by (default: none)

    Returns:
        CREATE TABLE DDL string
    """
    # Sanitize table name
    table_name = table_name.lower().replace("-", "_").split(".")[-1]  # Last part if contains dot

    # Build column list
    columns = []
    for field in schema_map:
        col_name = field["iceberg_column"]
        col_type = field["iceberg_type"]
        nullable = "NULL" if field["nullable"] else "NOT NULL"
        columns.append(f"  `{col_name}` {col_type} {nullable}")

    column_str = ",\n".join(columns)

    ddl = f"CREATE TABLE IF NOT EXISTS default.{table_name} (\n{column_str}\n)"

    # Add partition clause if specified
    if partition_by:
        partition_col = partition_by.lower().replace("-", "_")
        # Check if column exists in schema
        if any(f["iceberg_column"] == partition_col for f in schema_map):
            ddl += f"\nPARTITIONED BY ({partition_col})"

    return ddl

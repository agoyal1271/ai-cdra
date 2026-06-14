"""
Intent extraction helpers for metadata-aware semantic search and LLM classification.

These functions build descriptions and context that include metadata properties
(storage, format, catalog, location) so that both Qdrant embeddings and LLM
intent classifiers can understand and match metadata queries.
"""
import json
import logging
from typing import Any, Dict
import re

logger = logging.getLogger(__name__)


def normalize_asset_metadata(metadata: Dict[str, Any], asset_type: str = "") -> Dict[str, Any]:
    """
    Normalize metadata to extract storage, format, catalog, location properties.

    Handles different asset types that have metadata in different shapes:
    - Iceberg: has 'location', 'format_version', catalog info
    - Kafka: has 'schema_type', 'namespace'
    - Ozone: has 's3_bucket', 'path'

    Args:
        metadata: raw metadata dict from asset
        asset_type: type of asset (iceberg_table, kafka_topic, ozone_volume)

    Returns:
        Normalized metadata dict with standard keys: storage, format, catalog, location
    """
    normalized = dict(metadata)  # Copy original

    # Extract storage from location path
    location = metadata.get("location", "")
    if location and not metadata.get("storage"):
        # Explicit URI scheme detection
        # Order matters: check specific schemes first, then general categories
        if location.startswith("ofs://") or location.startswith("o3fs://"):
            # Ozone native URIs (not S3-compatible layer)
            normalized["storage"] = "ozone"
        elif location.startswith("s3://"):
            # AWS S3 (specific)
            normalized["storage"] = "s3"
        elif location.startswith("s3a://") or location.startswith("s3n://"):
            # S3-compatible layer: MinIO, DigitalOcean, Wasabi, Ozone S3-gateway, LocalStack, etc.
            # All use s3a://, can't distinguish without additional context
            normalized["storage"] = "s3a"
        elif location.startswith("adl://") or location.startswith("adls://"):
            # Azure Data Lake
            normalized["storage"] = "azure"
        elif location.startswith("abfs://") or location.startswith("wasb://"):
            # Azure Blob Storage
            normalized["storage"] = "azure"
        elif location.startswith("gs://") or location.startswith("gcs://"):
            # Google Cloud Storage
            normalized["storage"] = "gcs"
        elif location.startswith("hdfs://"):
            # Hadoop Distributed File System
            normalized["storage"] = "hdfs"
        elif location.startswith("/"):
            # Local absolute path
            normalized["storage"] = "local"

    # Extract format from format_version or file format indicators
    if "format_version" in metadata and not metadata.get("format"):
        # Iceberg format
        normalized["format"] = "iceberg"
    elif "schema_type" in metadata and not metadata.get("format"):
        # Kafka schema type
        normalized["format"] = metadata.get("schema_type", "avro").lower()

    # Extract catalog info
    if asset_type == "iceberg_table":
        if not metadata.get("catalog"):
            # Default to HMS (Hive Metastore) for Iceberg
            normalized["catalog"] = "hms"
        if not metadata.get("namespace"):
            # Extract from table name if available
            normalized["namespace"] = "default"

    return normalized


def build_asset_description_for_embedding(asset: Dict[str, Any]) -> str:
    """
    Build a searchable description for Qdrant embedding that includes metadata.

    This ensures queries like "stored in ozone", "parquet format", "rest catalog"
    can be matched by semantic search, not just field-level searches.

    Args:
        asset: discovered asset dict with name, type, description, metadata

    Returns:
        A text description optimized for embedding and semantic search.

    Example:
        asset = {
            "name": "customers",
            "asset_type": "iceberg_table",
            "description": "Customer master data",
            "metadata": {"location": "/volume/bucket/path", "format_version": 2}
        }

        Returns: "iceberg_table customers Customer master data storage ozone
                  format iceberg catalog hms"
    """
    metadata = asset.get("metadata", {})
    asset_type = asset.get("asset_type", "")
    name = asset.get("name", "")
    description = asset.get("description", "")

    # Normalize metadata to extract storage, format, catalog
    normalized_metadata = normalize_asset_metadata(metadata, asset_type)

    # Start with basic properties
    parts = []
    if asset_type:
        parts.append(asset_type)
    if name:
        parts.append(name)
    if description:
        parts.append(description)

    # Add normalized metadata properties with context keywords for better embedding
    if normalized_metadata.get("storage"):
        parts.append(f"storage {normalized_metadata['storage']}")

    if normalized_metadata.get("format"):
        parts.append(f"format {normalized_metadata['format']}")

    if normalized_metadata.get("catalog"):
        parts.append(f"catalog {normalized_metadata['catalog']}")

    if normalized_metadata.get("location"):
        parts.append(f"location {normalized_metadata['location']}")

    if normalized_metadata.get("namespace"):
        parts.append(f"namespace {normalized_metadata['namespace']}")

    # PII risk is important metadata
    if asset.get("pii_risk"):
        parts.append("pii_risk sensitive")

    # Join all parts with spaces
    final_description = " ".join(filter(None, parts))

    logger.debug(f"[intent_extractor] built description for {name}: {final_description[:100]}...")
    return final_description


def build_asset_context_for_llm(asset_name: str, schema_info: Dict[str, Any]) -> str:
    """
    Build structured context for LLM intent classifier that includes metadata.

    The LLM needs to see metadata properties (storage, format, catalog) explicitly
    to understand and match metadata-based queries, not just field-level queries.

    Args:
        asset_name: name of the asset
        schema_info: dict with asset_type, metadata, fields, location, format_version, etc.

    Returns:
        Formatted context string for the LLM prompt.

    Example:
        schema_info = {
            "asset_type": "iceberg_table",
            "location": "/volume/bucket/path",
            "format_version": 2,
            "fields": [{"name": "id", "type": "long"}, {"name": "email", "type": "string"}]
        }

        Returns: "Asset Information:\n- Name: customers\n- Type: iceberg_table\n\n
                 Metadata Properties:\n- Storage: ozone\n- Format: iceberg\n..."
    """
    asset_type = schema_info.get("asset_type", "unknown")

    # Merge nested metadata with top-level keys (for Iceberg: location, format_version are top-level)
    raw_metadata = {**schema_info.get("metadata", {})}
    if schema_info.get("location"):
        raw_metadata["location"] = schema_info["location"]
    if schema_info.get("format_version"):
        raw_metadata["format_version"] = schema_info["format_version"]

    # Normalize metadata to extract storage, format, catalog
    metadata = normalize_asset_metadata(raw_metadata, asset_type)

    # Extract field names from schema
    raw_fields = schema_info.get("fields", [])
    field_names = []
    for f in raw_fields:
        if isinstance(f, dict) and f.get("name"):
            field_names.append(f.get("name"))
    field_list = ", ".join(field_names) if field_names else "(no fields)"

    # Build structured context
    context = f"""
=== Asset Information ===
Name: {asset_name}
Type: {asset_type}

=== Metadata Properties ===
Storage: {metadata.get('storage', 'unknown')}
Format: {metadata.get('format', 'unknown')}
Catalog: {metadata.get('catalog', 'unknown')}
Location: {metadata.get('location', 'unknown')}
Namespace: {metadata.get('namespace', 'unknown')}
PII Risk: {'YES' if metadata.get('pii_risk') else 'NO'}

=== Schema Fields ===
Field names: {field_list}

=== Full Schema ===
{json.dumps(schema_info, default=str, indent=2)}
"""
    return context


def extract_metadata_filters_from_goal(goal: str) -> Dict[str, str]:
    """
    Extract metadata filter expectations from the user's goal.

    This helps the LLM understand what metadata properties the user cares about.
    Supports negation: queries with "NOT", "excluding", etc. encode negated values as "!value".

    Args:
        goal: user's query/goal

    Returns:
        Dict with keys like "storage", "format", "catalog" and their expected values.
        Negated filters are prefixed with "!" (e.g. "!ozone" means NOT ozone).
        Empty dict if no metadata filters found.

    Example:
        goal = "tables stored in ozone with parquet format"
        Returns: {"storage": "ozone", "format": "parquet"}

        goal = "find tables NOT in ozone which has geolocation"
        Returns: {"storage": "!ozone"}
    """
    filters = {}
    goal_lower = goal.lower()

    NEGATION_TERMS = {"not", "no", "except", "excluding", "without", "neither", "nor", "other"}

    # Look for explicit metadata patterns
    patterns = {
        "storage": [
            # Matches: "backend as ozone", "stored in s3a", "storage=hdfs", etc.
            r"(?:backend|stored|storage)\s+(?:in|as|=)?\s*(\w+)",
            # Matches: "in s3a storage", "on cloud backend"
            r"(?:in|on)\s+(\w+)\s+(?:storage|location|backend)",
            # Matches: "s3a storage", "cloud backend", "hdfs backend"
            r"(\w+)\s+(?:storage|location|backend)",
            # Matches: "in ozone", "on s3a", "NOT in ozone" (without requiring "storage" keyword)
            # Requires "tables"/"assets" before to avoid false positives
            r"(?:tables|assets)(?:\s+\w+)*?\s+(?:in|on)\s+(\w+)(?:\s|$)",
        ],
        "format": [
            r"(?:format|type)(?:\s+)?(?:is|=)?\s*(\w+)",
            r"(\w+)\s+format",
        ],
        "catalog": [
            r"(?:catalog)\s+(?:is|=)?\s*(\w+)",
        ],
        "location": [
            r"(?:location)\s+(?:in|is|=)?\s*(\w+)",
        ],
    }

    for prop_name, pattern_list in patterns.items():
        for pattern in pattern_list:
            match = re.search(pattern, goal_lower)
            if match:
                value = match.group(1).lower()
                # Skip common stop words that might be captured
                if value not in {"has", "which", "that", "with", "and", "or", "the", "a", "an"}:
                    # Check for negation terms: look in text before match, and within match itself
                    # Text before: "NOT in ozone" where pre_text has "not"
                    # Text within: "tables NOT in ozone" where matched text has "not"
                    pre_text = goal_lower[max(0, match.start() - 30):match.start()]
                    matched_text = match.group(0)

                    pre_words = pre_text.split()
                    matched_words = matched_text.split()

                    is_negated = (
                        any(neg in pre_words for neg in NEGATION_TERMS) or
                        any(neg in matched_words for neg in NEGATION_TERMS)
                    )

                    filters[prop_name] = f"!{value}" if is_negated else value
                    break  # Take first match for this property

    return filters


def build_intent_context_for_llm(goal: str, asset_name: str, schema_info: Dict[str, Any]) -> str:
    """
    Build complete context for LLM intent classifier, including metadata expectations.

    Combines asset information with what the user is asking for, so the LLM can
    match metadata properties and schema fields holistically.

    Args:
        goal: user's query
        asset_name: name of the asset being evaluated
        schema_info: dict with asset_type, metadata, fields, etc.

    Returns:
        Complete formatted context for the LLM prompt.
    """
    asset_context = build_asset_context_for_llm(asset_name, schema_info)
    metadata_filters = extract_metadata_filters_from_goal(goal)

    filters_text = ""
    if metadata_filters:
        filters_text = "\n=== User's Metadata Expectations ===\n"
        for key, value in metadata_filters.items():
            filters_text += f"{key}: {value}\n"

    return f"""User Goal: {goal}

{asset_context}
{filters_text}"""

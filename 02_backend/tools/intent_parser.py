"""
Semantic intent parser with caching.

Converts free-text user goals into structured intent objects.
Uses Qdrant to cache similar past goals → avoid re-parsing with LLM.

Flow:
  1. Embed goal text → dense vector
  2. Search intent cache for similar past goals (similarity > 0.92)
  3. Cache hit: return stored intent (no LLM call)
  4. Cache miss: call LLM to parse goal → store in cache
"""
import json
import logging
from typing import Any, Optional
import uuid

logger = logging.getLogger(__name__)

# Structured intent schema
INTENT_SCHEMA = {
    "type": "object",
    "properties": {
        "asset_types": {
            "type": "array",
            "items": {"enum": ["iceberg_table", "kafka_topic", "ozone_volume"]},
            "description": "Asset types to search for",
        },
        "storage": {
            "oneOf": [
                {"type": "null"},
                {
                    "type": "object",
                    "properties": {
                        "value": {"enum": ["ozone", "s3", "s3a", "hdfs", "azure", "gcs", "local"]},
                        "negate": {"type": "boolean"},
                    },
                },
            ],
            "description": "Storage location filter (may be negated)",
        },
        "format": {
            "oneOf": [
                {"type": "null"},
                {
                    "type": "object",
                    "properties": {
                        "value": {"enum": ["parquet", "avro", "iceberg", "json", "csv"]},
                        "negate": {"type": "boolean"},
                    },
                },
            ],
        },
        "required_fields": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Schema fields that must exist in the asset",
        },
        "pii_only": {
            "type": "boolean",
            "description": "Only return assets with PII risk",
        },
        "time_filter": {
            "oneOf": [
                {"type": "null"},
                {
                    "type": "object",
                    "properties": {
                        "direction": {"enum": ["after", "before"]},
                        "days": {"type": "integer", "minimum": 0},
                    },
                },
            ],
            "description": "Created/modified filter (after/before N days)",
        },
    },
}

INTENT_PROMPT_TEMPLATE = """
Extract the user's search intent as structured JSON. Be precise about negation.

Schema:
- asset_types: ["iceberg_table" | "kafka_topic" | "ozone_volume"]
- storage: {value, negate} or null (e.g., "NOT in ozone" → {value: "ozone", negate: true})
- format: {value, negate} or null (e.g., "excluding parquet" → {value: "parquet", negate: true})
- required_fields: ["field1", "field2"] or []
- pii_only: true/false
- time_filter: {direction: "after"|"before", days: N} or null (e.g., "created after 30 days" → {direction: "after", days: 30})

Rules:
1. "NOT in ozone" → storage: {{value: "ozone", negate: true}}
2. "excluding s3a" → storage: {{value: "s3a", negate: true}}
3. "with geolocation field" → required_fields: ["geolocation"]
4. "recently created" → time_filter: {{direction: "after", days: 7}}
5. If uncertain, set to null

User goal: "{goal}"

Return ONLY valid JSON, no other text.
"""


async def parse_intent_with_cache(goal: str) -> dict[str, Any]:
    """
    Parse user goal into structured intent, using semantic cache when available.

    Returns:
        {"asset_types": [...], "storage": {...}, "format": {...}, ...}
    """
    from memory.qdrant_store import _init_client, _get_embeddings, _ensure_collection

    # Step 1: Embed the goal
    try:
        embeddings = _get_embeddings([goal])
        goal_vector = embeddings[0]
    except Exception as e:
        logger.warning(f"[intent_parser] embedding failed: {e}, falling back to LLM without cache")
        return await _parse_intent_with_llm(goal)

    # Step 2: Search intent cache for similar goals
    client = _init_client()
    if not client or not _ensure_collection("intent_cache", vector_size=384):
        logger.debug("[intent_parser] Qdrant unavailable, parsing with LLM only")
        return await _parse_intent_with_llm(goal)

    try:
        results = client.search(
            collection_name="intent_cache",
            query_vector=goal_vector,
            limit=1,
            score_threshold=0.92,  # Similarity threshold
        )

        if results and len(results) > 0:
            # Cache HIT
            match = results[0]
            cached_goal = match.payload.get("goal", "")
            cached_intent = match.payload.get("intent", {})
            similarity = match.score
            logger.info(
                f"[intent_parser] cache hit: '{goal}' ~ '{cached_goal}' ({similarity:.2f})"
            )
            return cached_intent
    except Exception as e:
        logger.warning(f"[intent_parser] cache search failed: {e}, calling LLM")

    # Step 3: Cache MISS — call LLM
    intent = await _parse_intent_with_llm(goal)

    # Step 4: Store in cache for future similar queries
    try:
        from qdrant_client.models import PointStruct

        point_id = int(uuid.uuid4().int % (2**63))  # Ensure positive int
        point = PointStruct(
            id=point_id,
            vector=goal_vector,
            payload={
                "goal": goal,
                "intent": intent,
            },
        )
        client.upsert(collection_name="intent_cache", points=[point])
        logger.debug(f"[intent_parser] stored intent in cache: {point_id}")
    except Exception as e:
        logger.warning(f"[intent_parser] failed to cache intent: {e}")

    return intent


async def _parse_intent_with_llm(goal: str) -> dict[str, Any]:
    """Call LLM to parse goal into structured intent."""
    try:
        from config import LLM_BASE_URL, LLM_MODEL, LLM_API_KEY
        from langchain_openai import ChatOpenAI
        from langchain_core.messages import HumanMessage

        llm = ChatOpenAI(
            base_url=LLM_BASE_URL,
            model=LLM_MODEL,
            api_key=LLM_API_KEY,
            temperature=0.0,  # Deterministic
        )

        prompt = INTENT_PROMPT_TEMPLATE.format(goal=goal)
        response = await llm.ainvoke([HumanMessage(content=prompt)])

        intent = json.loads(response.content)

        # Normalize/validate intent
        intent = _normalize_intent(intent)
        logger.debug(f"[intent_parser] parsed intent: {intent}")
        return intent
    except Exception as e:
        logger.error(f"[intent_parser] LLM parsing failed: {e}")
        return _default_intent()


def _normalize_intent(intent: dict[str, Any]) -> dict[str, Any]:
    """Ensure intent has all required fields with correct types."""
    normalized = {
        "asset_types": intent.get("asset_types", ["iceberg_table", "kafka_topic", "ozone_volume"]),
        "storage": intent.get("storage"),
        "format": intent.get("format"),
        "required_fields": intent.get("required_fields", []),
        "pii_only": intent.get("pii_only", False),
        "time_filter": intent.get("time_filter"),
    }

    # Validate asset_types
    valid_types = {"iceberg_table", "kafka_topic", "ozone_volume"}
    if normalized["asset_types"]:
        normalized["asset_types"] = [
            t for t in normalized["asset_types"] if t in valid_types
        ]
    if not normalized["asset_types"]:
        normalized["asset_types"] = list(valid_types)

    # Validate storage/format
    for key in ["storage", "format"]:
        if normalized[key] and isinstance(normalized[key], dict):
            if "value" not in normalized[key]:
                normalized[key] = None
            if "negate" not in normalized[key]:
                normalized[key]["negate"] = False

    # Validate required_fields
    if not isinstance(normalized["required_fields"], list):
        normalized["required_fields"] = []

    # Validate time_filter
    if normalized["time_filter"] and isinstance(normalized["time_filter"], dict):
        if "direction" not in normalized["time_filter"] or "days" not in normalized["time_filter"]:
            normalized["time_filter"] = None

    return normalized


def _default_intent() -> dict[str, Any]:
    """Default intent when parsing fails (search all assets)."""
    return {
        "asset_types": ["iceberg_table", "kafka_topic", "ozone_volume"],
        "storage": None,
        "format": None,
        "required_fields": [],
        "pii_only": False,
        "time_filter": None,
    }


def intent_to_metadata_filters(intent: dict[str, Any]) -> dict[str, str]:
    """
    Convert structured intent to metadata filters (compatible with existing code).

    Example:
        intent = {
            "storage": {"value": "ozone", "negate": true},
            "format": {"value": "parquet", "negate": false}
        }
        returns: {"storage": "!ozone", "format": "parquet"}
    """
    filters = {}

    if intent.get("storage"):
        storage = intent["storage"]
        value = storage.get("value", "")
        negate = storage.get("negate", False)
        if value:
            filters["storage"] = f"!{value}" if negate else value

    if intent.get("format"):
        fmt = intent["format"]
        value = fmt.get("value", "")
        negate = fmt.get("negate", False)
        if value:
            filters["format"] = f"!{value}" if negate else value

    return filters

"""
Unified semantic catalog — Qdrant + Ollama embeddings (replaces Chroma).

One collection (data_catalog) covers all asset types:
  kafka_topic, iceberg_table, ozone_volume

Populated at index time by:
  - schema_registry_indexer.run_index()
  - iceberg_tools.list_iceberg_tables()

Queried at run time by agent._catalog_search() to narrow scans.

Requires: ollama pull nomic-embed-text
"""
import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

EMBED_MODEL = os.getenv("CATALOG_EMBED_MODEL", "nomic-embed-text")


# ── Document builder ──────────────────────────────────────────────────────────

def _build_doc(asset_type: str, name: str, fields: list[dict],
               description: str = "", namespace: str = "", extra: str = "") -> str:
    """Single text document per asset — embedded into the vector space."""
    field_text = ", ".join(
        f"{f.get('name', '')} {f.get('type', '')}" for f in fields if f.get("name")
    )
    parts = [f"{asset_type} {name}"]
    if field_text:
        parts.append(f"fields: {field_text}")
    if description:
        parts.append(f"description: {description}")
    if namespace:
        parts.append(f"namespace: {namespace}")
    if extra:
        parts.append(extra)
    return " | ".join(parts)


def _field_csv(fields: list[dict]) -> str:
    return ",".join(f.get("name", "") for f in fields if f.get("name"))


# ── Kafka indexing ────────────────────────────────────────────────────────────

def index_schemas_bulk(schemas: list[dict]) -> int:
    """
    Push SR schemas into Qdrant after PostgreSQL write.
    Each dict is the same shape as schema_registry_indexer produces.
    """
    from memory.qdrant_store import index_asset

    count = 0
    for s in schemas:
        name = s.get("name") or s.get("schema_name", "")
        if not name:
            continue

        # Resolve fields — prefer pre-parsed list, fall back to schema_text
        fields: list[dict] = s.get("fields") or []
        if not fields:
            schema_text = s.get("schema", "") or s.get("schema_text", "")
            if schema_text:
                try:
                    raw = json.loads(schema_text)
                    fields = [
                        {"name": f.get("name"), "type": str(f.get("type", "?"))}
                        for f in raw.get("fields", [])
                    ]
                except Exception:
                    pass

        doc = _build_doc(
            "kafka_topic", name, fields,
            description=s.get("description", ""),
            namespace=s.get("namespace", s.get("namespace_str", "")),
        )

        asset = {
            "id": f"kafka::{name}",
            "asset_type": "kafka_topic",
            "name": name,
            "description": doc,
            "fields": fields,
            "field_names": _field_csv(fields),
            "field_count": len(fields),
            "schema_type": s.get("type", s.get("schema_type", "avro")),
            "namespace": s.get("namespace", s.get("namespace_str", "")) or "",
            "indexed_at": time.time(),
        }

        if index_asset(asset):
            count += 1

    if count > 0:
        logger.info(f"[catalog] indexed {count} Kafka schemas")

    return count


# ── Iceberg indexing ──────────────────────────────────────────────────────────

def index_iceberg_tables_bulk(tables: list[dict]) -> int:
    """
    Push Iceberg tables into Qdrant after list_iceberg_tables() completes.
    Each dict is the shape returned by list_iceberg_tables().
    """
    from memory.qdrant_store import index_asset

    count = 0
    for t in tables:
        name = t.get("name", "")
        if not name or t.get("error"):
            continue

        fields = t.get("fields", [])
        partition_spec = t.get("partition_spec", "")
        extra = f"partition: {partition_spec}" if partition_spec else ""
        namespace = name.split(".")[0] if "." in name else ""

        doc = _build_doc("iceberg_table", name, fields, extra=extra, namespace=namespace)

        asset = {
            "id": f"iceberg::{name}",
            "asset_type": "iceberg_table",
            "name": name,
            "description": doc,
            "fields": fields,
            "field_names": _field_csv(fields),
            "field_count": len(fields),
            "namespace": namespace,
            "partition_spec": partition_spec or "",
            "snapshots": t.get("snapshots", 0),
            "location": t.get("location", "") or "",
            "indexed_at": time.time(),
        }

        if index_asset(asset):
            count += 1

    if count > 0:
        logger.info(f"[catalog] indexed {count} Iceberg tables")

    return count


# ── Ozone indexing ────────────────────────────────────────────────────────────

def index_ozone_volumes(volumes: list[dict]) -> int:
    """Push Ozone volume list into Qdrant."""
    from memory.qdrant_store import index_asset

    count = 0
    for v in volumes:
        name = v.get("name", "")
        if not name:
            continue

        doc = f"ozone_volume {name} | object storage bucket | {v.get('description', '')}"

        asset = {
            "id": f"ozone::{name}",
            "asset_type": "ozone_volume",
            "name": name,
            "description": doc,
            "fields": [],
            "field_names": "",
            "field_count": 0,
            "indexed_at": time.time(),
        }

        if index_asset(asset):
            count += 1

    if count > 0:
        logger.info(f"[catalog] indexed {count} Ozone volumes")

    return count


# ── Search ────────────────────────────────────────────────────────────────────

def search(query: str, asset_types: list[str] = None, top_k: int = 30) -> list[dict]:
    """
    Semantic search across all indexed assets.
    Returns list of dicts: metadata + similarity score (0–1, higher = more relevant).
    """
    from memory.qdrant_store import search_catalog

    results = search_catalog(query, limit=top_k, asset_type=asset_types[0] if asset_types and len(asset_types) == 1 else None)

    output = []
    for result in results:
        output.append({
            "asset_type": result.get("asset_type", ""),
            "name": result.get("name", ""),
            "description": result.get("description", ""),
            "namespace": result.get("namespace", ""),
            "field_names": result.get("fields", []),
            "field_count": len(result.get("fields", [])),
            "similarity": result.get("score", 0.0),
            "id": result.get("asset_id", ""),
        })

    return output


# ── Stats / admin ─────────────────────────────────────────────────────────────

def get_stats() -> dict[str, Any]:
    """Get catalog statistics."""
    from memory.qdrant_store import get_catalog_stats

    try:
        stats = get_catalog_stats()
        return {
            "total": stats.get("docs", 0),
            "available": stats.get("docs", 0) > 0,
            "embed_model": EMBED_MODEL,
        }
    except Exception as e:
        logger.warning(f"[catalog] get_stats failed: {e}")
        return {"total": 0, "available": False, "error": str(e)}


def clear() -> None:
    """Clear all catalog data."""
    from memory.qdrant_store import clear_catalog

    try:
        clear_catalog()
        logger.info("[catalog] cleared all documents")
    except Exception as e:
        logger.warning(f"[catalog] clear failed: {e}")

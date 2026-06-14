"""
Qdrant-backed vector store for discovered assets and semantic catalog.
Replaces Chroma for faster, more scalable vector search.

Collections:
  - discovered_assets: individual assets from discovery runs
  - data_catalog: semantic catalog with embeddings (Iceberg, Kafka, Ozone)
"""
import json
import logging
import os
from typing import Any, Optional, List

logger = logging.getLogger(__name__)

_client = None
_collection_assets = None
_collection_catalog = None
_embed_fn = None


def _init_client():
    """Initialize Qdrant client."""
    global _client
    if _client is None:
        try:
            from qdrant_client import QdrantClient
            from config import QDRANT_URL, QDRANT_API_KEY

            _client = QdrantClient(
                url=QDRANT_URL,
                api_key=QDRANT_API_KEY if QDRANT_API_KEY else None,
                timeout=30.0,
                check_compatibility=False,
            )
            logger.info(f"[qdrant] connected to {QDRANT_URL}")
        except Exception as e:
            logger.warning(f"[qdrant] connection failed: {e}")
            _client = None

    return _client


def _get_embeddings(texts: List[str]) -> List[List[float]]:
    """Generate embeddings using Ollama."""
    global _embed_fn
    if _embed_fn is None:
        try:
            from config import LLM_BASE_URL
            import requests

            _embed_fn = {
                "base_url": LLM_BASE_URL.rstrip("/").replace("/v1", ""),
                "model": os.getenv("CATALOG_EMBED_MODEL", "nomic-embed-text"),
            }
        except Exception as e:
            logger.warning(f"[embeddings] config failed: {e}")
            return [[0.0] * 384] * len(texts)  # fallback embeddings

    try:
        import requests
        url = f"{_embed_fn['base_url']}/api/embeddings"
        embeddings = []

        for text in texts:
            resp = requests.post(
                url,
                json={"model": _embed_fn["model"], "prompt": text},
                timeout=10,
            )
            if resp.status_code == 200:
                embeddings.append(resp.json()["embedding"])
            else:
                logger.warning(f"[embeddings] request failed: {resp.status_code}")
                embeddings.append([0.0] * 384)

        return embeddings
    except Exception as e:
        logger.warning(f"[embeddings] generation failed: {e}")
        return [[0.0] * 384] * len(texts)


def _ensure_collection(name: str, vector_size: int = 384):
    """Create collection if it doesn't exist."""
    client = _init_client()
    if not client:
        return False

    try:
        client.get_collection(name)
        return True
    except:
        try:
            from qdrant_client.models import Distance, VectorParams

            client.create_collection(
                collection_name=name,
                vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
            )
            logger.info(f"[qdrant] created collection: {name}")
            return True
        except Exception as e:
            logger.warning(f"[qdrant] create collection failed: {e}")
            return False


# === Discovered Assets Store ===

def store_asset(asset: dict[str, Any]) -> bool:
    """Store a discovered asset."""
    client = _init_client()
    if not client:
        return False

    if not _ensure_collection("discovered_assets"):
        return False

    try:
        from qdrant_client.models import PointStruct

        asset_id = asset.get("id", "")
        if not asset_id:
            return False

        description = f"{asset.get('asset_type', '')} {asset.get('name', '')} {asset.get('description', '')}"
        embeddings = _get_embeddings([description])

        point = PointStruct(
            id=hash(asset_id) % (2**63),  # Ensure positive int
            vector=embeddings[0],
            payload={
                "asset_id": asset_id,
                "asset_type": asset.get("asset_type", ""),
                "name": asset.get("name", ""),
                "pii_risk": bool(asset.get("pii_risk", False)),
                "metadata": json.dumps(asset.get("metadata", {})),
                "timestamp": asset.get("timestamp", 0),
            },
        )

        client.upsert(collection_name="discovered_assets", points=[point])
        return True
    except Exception as e:
        logger.warning(f"[qdrant] store_asset failed: {e}")
        return False


def get_all_assets(asset_type: Optional[str] = None) -> list[dict]:
    """Get all stored assets, optionally filtered by type."""
    client = _init_client()
    if not client:
        return []

    if not _ensure_collection("discovered_assets"):
        return []

    try:
        # Scroll all points, filter in Python
        scroll_result = client.scroll(collection_name="discovered_assets", limit=1000)
        points = scroll_result[0] if isinstance(scroll_result, tuple) else scroll_result
        assets = []

        for point in points:
            payload = point.payload if hasattr(point, 'payload') else point.get('payload', {})
            point_asset_type = payload.get("asset_type", "")

            # Filter by asset_type if specified
            if asset_type and point_asset_type != asset_type:
                continue

            assets.append(
                {
                    "id": payload.get("asset_id", ""),
                    "name": payload.get("name", ""),
                    "asset_type": point_asset_type,
                    "pii_risk": payload.get("pii_risk", False),
                    "metadata": json.loads(payload.get("metadata", "{}")),
                }
            )

        return assets
    except Exception as e:
        logger.warning(f"[qdrant] get_all_assets failed: {e}")
        return []


def clear_all_assets() -> None:
    """Clear all discovered assets."""
    client = _init_client()
    if not client:
        return

    try:
        client.delete_collection("discovered_assets")
        logger.info("[qdrant] cleared discovered_assets")
    except Exception as e:
        logger.warning(f"[qdrant] clear_all_assets failed: {e}")


# === Semantic Catalog ===

def index_asset(asset: dict[str, Any]) -> bool:
    """Index an asset in the semantic catalog with PII detection."""
    client = _init_client()
    if not client:
        return False

    if not _ensure_collection("data_catalog"):
        return False

    try:
        from qdrant_client.models import PointStruct
        from tools.intent_extractor import build_asset_description_for_embedding
        from tools.pii_detector import detect_pii_fields

        asset_id = asset.get("id", "")
        if not asset_id:
            return False

        # Use metadata-aware description builder for better semantic search
        description = build_asset_description_for_embedding(asset)
        embeddings = _get_embeddings([description])

        fields = asset.get("fields", [])

        # PII detection disabled for now - was causing column duplication
        # TODO: Fix and re-enable when storage format is verified
        # pii_detection = detect_pii_fields(fields, threshold=0.65)

        point = PointStruct(
            id=hash(asset_id) % (2**63),
            vector=embeddings[0],
            payload={
                "asset_id": asset_id,
                "asset_type": asset.get("asset_type", ""),
                "name": asset.get("name", ""),
                "namespace": asset.get("namespace", ""),
                "fields": json.dumps(fields),
                "description": description,
                "indexed_at": asset.get("indexed_at", 0),
            },
        )

        client.upsert(collection_name="data_catalog", points=[point])
        return True
    except Exception as e:
        logger.warning(f"[qdrant] index_asset failed: {e}")
        return False


def search_catalog(query: str, limit: int = 10, asset_type: Optional[str] = None) -> list[dict]:
    """Search the semantic catalog by query."""
    client = _init_client()
    if not client:
        return []

    if not _ensure_collection("data_catalog"):
        return []

    try:
        from qdrant_client.models import Filter, FieldCondition, MatchValue

        embeddings = _get_embeddings([query])
        if not embeddings or not embeddings[0]:
            return []

        filter_condition = None
        if asset_type:
            filter_condition = Filter(
                must=[
                    FieldCondition(
                        key="asset_type",
                        match=MatchValue(value=asset_type),
                    )
                ]
            )

        # Use search_points with correct Qdrant API
        results = client.search(
            collection_name="data_catalog",
            query_vector=embeddings[0],
            limit=limit,
            query_filter=filter_condition if filter_condition else None,
        )

        assets = []
        for result in results:
            payload = result.payload if hasattr(result, 'payload') else result.get('payload', {})
            assets.append(
                {
                    "asset_id": payload.get("asset_id", ""),
                    "name": payload.get("name", ""),
                    "asset_type": payload.get("asset_type", ""),
                    "namespace": payload.get("namespace", ""),
                    "description": payload.get("description", ""),
                    "fields": json.loads(payload.get("fields", "[]")),
                    "score": result.score if hasattr(result, 'score') else result.get('score', 0),
                }
            )

        return assets
    except Exception as e:
        logger.warning(f"[qdrant] search_catalog failed: {e}")
        return []


def clear_catalog() -> None:
    """Clear the semantic catalog."""
    client = _init_client()
    if not client:
        return

    try:
        client.delete_collection("data_catalog")
        logger.info("[qdrant] cleared data_catalog")
    except Exception as e:
        logger.warning(f"[qdrant] clear_catalog failed: {e}")


def get_catalog_stats() -> dict:
    """Get catalog collection stats."""
    client = _init_client()
    if not client:
        return {}

    try:
        info = client.get_collection("data_catalog")
        return {
            "docs": info.points_count,
            "embed_model": os.getenv("CATALOG_EMBED_MODEL", "nomic-embed-text"),
        }
    except Exception as e:
        logger.warning(f"[qdrant] get_catalog_stats failed: {e}")
        return {}

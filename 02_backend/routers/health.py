import logging
from fastapi import APIRouter

router = APIRouter()
logger = logging.getLogger(__name__)


def _check_kafka() -> dict:
    """Checks SR cache — SR is the Kafka data path, no broker connection needed."""
    try:
        from tools.kafka.schema_registry_cache import get_stats
        stats = get_stats()
        count = stats.get("total_schemas", 0)
        if count > 0:
            return {"status": "ok", "topics": count, "source": "schema_registry_cache"}
        from config import SCHEMA_REGISTRY_URL
        if SCHEMA_REGISTRY_URL:
            return {"status": "degraded", "note": "SR cache empty — trigger /api/kafka/index to populate"}
        return {"status": "unconfigured"}
    except Exception as e:
        return {"status": "unavailable", "note": str(e)[:120]}


def _check_iceberg() -> dict:
    try:
        from tools.iceberg.iceberg_tools import _load_catalog
        _load_catalog()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "unavailable", "note": str(e)[:80]}


def _check_ozone() -> dict:
    try:
        from tools.ozone.ozone_tools import _s3_client
        s3 = _s3_client()
        s3.list_buckets()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "unavailable", "note": str(e)[:80]}




@router.post("/api/embeddings")
async def get_embeddings(request: dict):
    """Get embeddings for text using configured LLM provider.

    Used for semantic PII detection, intent matching, etc.
    Supports: Ollama (local), OpenAI, Anthropic, Azure OpenAI
    """
    from config.settings import get_llm_config, validate_llm_config
    from config.llm_provider import get_llm_provider

    text = request.get("text", "")
    if not text:
        return {"error": "text is required", "embedding": []}

    try:
        if not validate_llm_config():
            return {"error": "LLM configuration is invalid", "embedding": []}

        llm_config = get_llm_config()
        provider = llm_config.get("provider", "ollama")

        provider_instance = get_llm_provider(
            provider,
            **{k: v for k, v in llm_config.items() if k != "provider" and k != "model"}
        )

        if not provider_instance:
            return {"error": f"Failed to initialize {provider} provider", "embedding": []}

        embedding = await provider_instance.generate_embeddings(text)

        if not embedding:
            logger.warning(f"[Embeddings] {provider} returned empty embedding for: {text[:50]}")
            return {"error": f"{provider} returned empty embedding", "embedding": []}

        logger.debug(f"[Embeddings] Generated {len(embedding)}-dim embedding using {provider} for: {text[:50]}")
        return {"embedding": embedding, "text": text, "provider": provider}

    except Exception as e:
        logger.warning(f"[Embeddings] Error: {e}")
        return {"error": str(e), "embedding": []}


@router.get("/api/health")
async def health():
    import asyncio
    try:
        kafka, iceberg, ozone = await asyncio.wait_for(
            asyncio.gather(
                asyncio.to_thread(_check_kafka),
                asyncio.to_thread(_check_iceberg),
                asyncio.to_thread(_check_ozone),
            ),
            timeout=5.0
        )
        services = {
            "kafka": kafka,
            "iceberg": iceberg,
            "ozone": ozone,
        }
        overall = "ok" if all(s["status"] == "ok" for s in services.values()) else "degraded"
        return {"status": overall, "services": services}
    except asyncio.TimeoutError:
        return {"status": "timeout", "message": "health checks exceeded 5s timeout"}


# ── Settings Management ──────────────────────────────────────────

@router.get("/api/settings")
async def get_all_settings():
    """Get all current settings (DB > ENV > DEFAULTS)."""
    from config.settings import get_settings

    try:
        settings = get_settings()
        # Don't expose API keys in the response - return placeholders
        safe_settings = {}
        for key, value in settings.items():
            if "key" in key or "password" in key or "token" in key:
                safe_settings[key] = "***" if value else ""
            else:
                safe_settings[key] = value
        return {"status": "ok", "settings": safe_settings}
    except Exception as e:
        logger.error(f"Failed to get settings: {e}")
        return {"status": "error", "message": str(e)}


@router.get("/api/settings/{key}")
async def get_setting(key: str):
    """Get a single setting value."""
    from config.settings import get_setting

    try:
        value = get_setting(key)
        if value is None:
            return {"status": "not_found", "key": key}

        # Don't expose secrets
        if "key" in key or "password" in key or "token" in key:
            return {"status": "ok", "key": key, "value": "***" if value else ""}

        return {"status": "ok", "key": key, "value": value}
    except Exception as e:
        logger.error(f"Failed to get setting {key}: {e}")
        return {"status": "error", "message": str(e)}


@router.post("/api/settings/{key}")
async def update_setting(key: str, request: dict):
    """Update a setting value."""
    from config.settings import update_setting, validate_llm_config

    try:
        value = request.get("value")
        success = update_setting(key, value)

        if not success:
            return {"status": "error", "message": f"Unknown setting: {key}"}

        # If updating LLM config, validate it
        if "llm" in key:
            if not validate_llm_config():
                logger.warning(f"LLM config validation failed after updating {key}")
                return {"status": "warning", "message": "Setting updated but LLM validation failed"}

        logger.info(f"Updated setting: {key}")
        return {"status": "ok", "key": key}
    except Exception as e:
        logger.error(f"Failed to update setting {key}: {e}")
        return {"status": "error", "message": str(e)}


# ── LLM Management ──────────────────────────────────────────────

@router.get("/api/llm/available-models")
async def get_available_models():
    """Get available models for current LLM provider."""
    import asyncio
    from config.settings import get_llm_config
    from config.llm_provider import get_llm_provider

    try:
        llm_config = get_llm_config()
        provider = llm_config.get("provider", "ollama")

        provider_instance = get_llm_provider(
            provider,
            **{k: v for k, v in llm_config.items() if k != "provider" and k != "model"}
        )

        if not provider_instance:
            return {"status": "error", "message": f"Failed to initialize provider: {provider}"}

        try:
            # Add timeout to prevent hanging (3 seconds)
            models = await asyncio.wait_for(
                provider_instance.get_available_models(),
                timeout=3.0
            )
        except asyncio.TimeoutError:
            logger.warning(f"Timeout fetching models from {provider}")
            models = []

        return {
            "status": "ok",
            "provider": provider,
            "models": models,
            "current_model": llm_config.get("model", "")
        }
    except Exception as e:
        logger.error(f"Failed to get available models: {e}")
        return {"status": "error", "message": str(e), "models": []}


@router.post("/api/llm/test")
async def test_llm_connection():
    """Test current LLM configuration and connectivity."""
    from config.settings import get_llm_config, validate_llm_config
    from config.llm_provider import get_llm_provider

    try:
        if not validate_llm_config():
            return {"status": "error", "message": "LLM configuration is invalid or incomplete"}

        llm_config = get_llm_config()
        provider = llm_config.get("provider", "ollama")

        provider_instance = get_llm_provider(
            provider,
            **{k: v for k, v in llm_config.items() if k != "provider" and k != "model"}
        )

        if not provider_instance:
            return {"status": "error", "message": f"Failed to initialize provider: {provider}"}

        # Test with a simple embedding request
        test_text = "test"
        embeddings = await provider_instance.generate_embeddings(test_text)

        if not embeddings or len(embeddings) == 0:
            return {
                "status": "error",
                "message": f"{provider} returned empty embeddings"
            }

        return {
            "status": "ok",
            "provider": provider,
            "model": llm_config.get("model"),
            "embedding_dim": len(embeddings),
            "message": f"Connected to {provider} successfully"
        }
    except Exception as e:
        logger.error(f"LLM test failed: {e}")
        return {"status": "error", "message": str(e)}

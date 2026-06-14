"""Settings management - hybrid approach using .env + database.

Priority:
1. Database (runtime overrides)
2. Environment variables (initial setup)
3. Defaults
"""

import os
import json
import logging
from typing import Optional, Any, Dict
from datetime import datetime

logger = logging.getLogger(__name__)

# Default settings
DEFAULTS = {
    "llm_provider": "ollama",
    "llm_model": "nomic-embed-text",
    "ollama_url": "http://localhost:11434",
    "openai_api_key": "",
    "anthropic_api_key": "",
    "azure_openai_key": "",
    "azure_openai_endpoint": "",
    "azure_openai_deployment": "text-embedding-ada-002",
    "knox_host": "",
    "knox_user": "admin",
    "knox_password": "",
    "schema_registry_url": "",
}

# In-memory cache of settings
_settings_cache: Dict[str, Any] = {}
_cache_timestamp: float = 0
_CACHE_TTL = 300  # 5 minutes


def load_from_env() -> Dict[str, Any]:
    """Load settings from environment variables."""
    return {
        "llm_provider": os.getenv("LLM_PROVIDER", DEFAULTS["llm_provider"]),
        "llm_model": os.getenv("LLM_MODEL", DEFAULTS["llm_model"]),
        "ollama_url": os.getenv("OLLAMA_URL", DEFAULTS["ollama_url"]),
        "openai_api_key": os.getenv("OPENAI_API_KEY", DEFAULTS["openai_api_key"]),
        "anthropic_api_key": os.getenv("ANTHROPIC_API_KEY", DEFAULTS["anthropic_api_key"]),
        "azure_openai_key": os.getenv("AZURE_OPENAI_KEY", DEFAULTS["azure_openai_key"]),
        "azure_openai_endpoint": os.getenv("AZURE_OPENAI_ENDPOINT", DEFAULTS["azure_openai_endpoint"]),
        "azure_openai_deployment": os.getenv(
            "AZURE_OPENAI_DEPLOYMENT", DEFAULTS["azure_openai_deployment"]
        ),
        "knox_host": os.getenv("KNOX_HOST", DEFAULTS["knox_host"]),
        "knox_user": os.getenv("KNOX_USER", DEFAULTS["knox_user"]),
        "knox_password": os.getenv("KNOX_PASSWORD", DEFAULTS["knox_password"]),
        "schema_registry_url": os.getenv("SCHEMA_REGISTRY_URL", DEFAULTS["schema_registry_url"]),
    }


def load_from_db() -> Dict[str, Any]:
    """Load settings from database (runtime overrides).

    TODO: Implement database storage when ready.
    For now, returns empty dict.
    """
    try:
        # Placeholder for database implementation
        # When ready, query settings table
        return {}
    except Exception as e:
        logger.warning(f"Failed to load settings from DB: {e}")
        return {}


def get_settings() -> Dict[str, Any]:
    """Get merged settings with priority: DB > ENV > DEFAULTS."""
    global _settings_cache, _cache_timestamp
    import time

    # Use cache if fresh
    current_time = time.time()
    if _settings_cache and (current_time - _cache_timestamp) < _CACHE_TTL:
        return _settings_cache

    # Load and merge
    settings = DEFAULTS.copy()
    settings.update(load_from_env())
    settings.update(load_from_db())

    _settings_cache = settings
    _cache_timestamp = current_time

    return settings


def get_setting(key: str, default: Any = None) -> Any:
    """Get a single setting value."""
    settings = get_settings()
    return settings.get(key, default)


def update_setting(key: str, value: Any) -> bool:
    """Update a setting (saves to database when implemented).

    For now, updates only the in-memory cache.
    """
    global _settings_cache

    settings = get_settings()
    if key not in settings:
        logger.warning(f"Unknown setting: {key}")
        return False

    settings[key] = value
    _settings_cache = settings

    # TODO: Save to database
    logger.info(f"Updated setting {key} (not persisted yet)")

    return True


def validate_llm_config() -> bool:
    """Validate that current LLM configuration is valid."""
    settings = get_settings()
    provider = settings.get("llm_provider", "ollama")

    if provider == "ollama":
        url = settings.get("ollama_url")
        if not url:
            logger.error("Ollama provider selected but OLLAMA_URL not set")
            return False
        return True

    elif provider == "openai":
        api_key = settings.get("openai_api_key")
        if not api_key:
            logger.error("OpenAI provider selected but OPENAI_API_KEY not set")
            return False
        return True

    elif provider == "anthropic":
        api_key = settings.get("anthropic_api_key")
        if not api_key:
            logger.error("Anthropic provider selected but ANTHROPIC_API_KEY not set")
            return False
        return True

    elif provider == "azure_openai":
        key = settings.get("azure_openai_key")
        endpoint = settings.get("azure_openai_endpoint")
        if not key or not endpoint:
            logger.error("Azure OpenAI selected but credentials not set")
            return False
        return True

    logger.error(f"Unknown LLM provider: {provider}")
    return False


def get_llm_config() -> Dict[str, Any]:
    """Get LLM-specific configuration."""
    settings = get_settings()
    provider = settings.get("llm_provider", "ollama")

    config = {
        "provider": provider,
        "model": settings.get("llm_model"),
    }

    if provider == "ollama":
        config["host"] = settings.get("ollama_url")
    elif provider == "openai":
        config["api_key"] = settings.get("openai_api_key")
    elif provider == "anthropic":
        config["api_key"] = settings.get("anthropic_api_key")
    elif provider == "azure_openai":
        config["api_key"] = settings.get("azure_openai_key")
        config["endpoint"] = settings.get("azure_openai_endpoint")
        config["deployment"] = settings.get("azure_openai_deployment")

    return config

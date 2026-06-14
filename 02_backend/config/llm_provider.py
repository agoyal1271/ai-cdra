"""LLM Provider abstraction - support multiple LLM backends.

Supported providers:
- ollama: Local Ollama instance
- openai: OpenAI API (GPT-4, 3.5-turbo)
- anthropic: Anthropic Claude API
- azure_openai: Azure OpenAI
"""

import os
import logging
from typing import Optional, Any
from abc import ABC, abstractmethod
import httpx

logger = logging.getLogger(__name__)


class LLMProvider(ABC):
    """Abstract base class for LLM providers."""

    @abstractmethod
    async def generate_embeddings(self, text: str) -> list[float]:
        """Generate embeddings for text."""
        pass

    @abstractmethod
    async def get_available_models(self) -> list[str]:
        """Get list of available models for this provider."""
        pass

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Return provider name."""
        pass


class OllamaProvider(LLMProvider):
    """Ollama - local LLM running locally."""

    def __init__(self, host: str = "http://localhost:11434", model: str = "nomic-embed-text"):
        self.host = host
        self.model = model

    @property
    def provider_name(self) -> str:
        return "ollama"

    async def generate_embeddings(self, text: str) -> list[float]:
        """Generate embeddings using Ollama."""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{self.host}/api/embeddings",
                    json={"model": self.model, "prompt": text},
                )
                if response.status_code == 200:
                    return response.json().get("embedding", [])
                logger.warning(f"Ollama error: {response.status_code}")
                return []
        except Exception as e:
            logger.warning(f"Ollama embedding error: {e}")
            return []

    async def get_available_models(self) -> list[str]:
        """Get available Ollama models."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(f"{self.host}/api/tags")
                if response.status_code == 200:
                    models = response.json().get("models", [])
                    return [m.get("name", "") for m in models if m.get("name")]
                return []
        except Exception as e:
            logger.warning(f"Failed to fetch Ollama models: {e}")
            return []


class OpenAIProvider(LLMProvider):
    """OpenAI - ChatGPT, GPT-4."""

    def __init__(self, api_key: str, model: str = "text-embedding-3-small"):
        self.api_key = api_key
        self.model = model

    @property
    def provider_name(self) -> str:
        return "openai"

    async def generate_embeddings(self, text: str) -> list[float]:
        """Generate embeddings using OpenAI."""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    "https://api.openai.com/v1/embeddings",
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    json={"model": self.model, "input": text},
                )
                if response.status_code == 200:
                    return response.json()["data"][0]["embedding"]
                logger.warning(f"OpenAI error: {response.status_code}")
                return []
        except Exception as e:
            logger.warning(f"OpenAI embedding error: {e}")
            return []

    async def get_available_models(self) -> list[str]:
        """Get available OpenAI models."""
        return [
            "text-embedding-3-small",
            "text-embedding-3-large",
            "text-embedding-ada-002",
        ]


class AnthropicProvider(LLMProvider):
    """Anthropic - Claude API."""

    def __init__(self, api_key: str, model: str = "claude-opus-4-1"):
        self.api_key = api_key
        self.model = model

    @property
    def provider_name(self) -> str:
        return "anthropic"

    async def generate_embeddings(self, text: str) -> list[float]:
        """Generate embeddings using Anthropic."""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": self.api_key,
                        "anthropic-version": "2023-06-01",
                    },
                    json={
                        "model": self.model,
                        "max_tokens": 1024,
                        "messages": [{"role": "user", "content": f"Generate embedding vector: {text}"}],
                    },
                )
                if response.status_code == 200:
                    # Note: Anthropic doesn't have native embeddings, this is a fallback
                    # For production, use Anthropic's embedding service or another provider
                    logger.warning("Anthropic embedding not fully implemented, use OpenAI or Ollama")
                    return []
                return []
        except Exception as e:
            logger.warning(f"Anthropic embedding error: {e}")
            return []

    async def get_available_models(self) -> list[str]:
        """Get available Anthropic models."""
        return ["claude-opus-4-1", "claude-sonnet-4-1", "claude-haiku-3"]


class AzureOpenAIProvider(LLMProvider):
    """Azure OpenAI - OpenAI API hosted on Azure."""

    def __init__(self, api_key: str, endpoint: str, deployment: str = "text-embedding-ada-002"):
        self.api_key = api_key
        self.endpoint = endpoint
        self.deployment = deployment

    @property
    def provider_name(self) -> str:
        return "azure_openai"

    async def generate_embeddings(self, text: str) -> list[float]:
        """Generate embeddings using Azure OpenAI."""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{self.endpoint}/openai/deployments/{self.deployment}/embeddings?api-version=2023-05-15",
                    headers={"api-key": self.api_key},
                    json={"input": text},
                )
                if response.status_code == 200:
                    return response.json()["data"][0]["embedding"]
                logger.warning(f"Azure OpenAI error: {response.status_code}")
                return []
        except Exception as e:
            logger.warning(f"Azure OpenAI embedding error: {e}")
            return []

    async def get_available_models(self) -> list[str]:
        """Get available Azure OpenAI models."""
        return ["text-embedding-ada-002", "text-embedding-3-small"]


def get_llm_provider(
    provider: str,
    **kwargs
) -> Optional[LLMProvider]:
    """Factory function to get LLM provider instance.

    Args:
        provider: Provider name (ollama, openai, anthropic, azure_openai)
        **kwargs: Provider-specific parameters

    Returns:
        LLMProvider instance or None if provider not found
    """
    if provider == "ollama":
        return OllamaProvider(
            host=kwargs.get("host", os.getenv("OLLAMA_URL", "http://localhost:11434")),
            model=kwargs.get("model", "nomic-embed-text"),
        )
    elif provider == "openai":
        return OpenAIProvider(
            api_key=kwargs.get("api_key", os.getenv("OPENAI_API_KEY", "")),
            model=kwargs.get("model", "text-embedding-3-small"),
        )
    elif provider == "anthropic":
        return AnthropicProvider(
            api_key=kwargs.get("api_key", os.getenv("ANTHROPIC_API_KEY", "")),
            model=kwargs.get("model", "claude-opus-4-1"),
        )
    elif provider == "azure_openai":
        return AzureOpenAIProvider(
            api_key=kwargs.get("api_key", os.getenv("AZURE_OPENAI_KEY", "")),
            endpoint=kwargs.get("endpoint", os.getenv("AZURE_OPENAI_ENDPOINT", "")),
            deployment=kwargs.get("deployment", "text-embedding-ada-002"),
        )
    else:
        logger.error(f"Unknown LLM provider: {provider}")
        return None

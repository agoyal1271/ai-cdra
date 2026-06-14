"""Backend PII detection using embeddings.

Detects PII fields during asset discovery and stores results for reuse.
"""

import logging
import httpx
from typing import Any

logger = logging.getLogger(__name__)

# Known PII patterns
PII_PATTERNS = [
    "social security number",
    "ssn",
    "passport",
    "national id",
    "driver license",
    "identification number",
    "email address",
    "email",
    "phone number",
    "phone",
    "telephone",
    "mobile",
    "cell phone",
    "contact number",
    "credit card",
    "bank account",
    "routing number",
    "swift code",
    "iban",
    "card number",
    "date of birth",
    "dob",
    "birth date",
    "name",
    "first name",
    "last name",
    "full name",
    "address",
    "home address",
    "street address",
    "zip code",
    "postal code",
    "health insurance",
    "medical record",
    "patient id",
    "diagnosis",
    "prescription",
    "employee id",
    "member id",
    "employee number",
    "payroll",
    "salary",
]

# Cache for embeddings
_embedding_cache = {}


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Calculate cosine similarity between two vectors."""
    if not a or not b:
        return 0.0
    dot_product = sum(x * y for x, y in zip(a, b))
    magnitude_a = sum(x * x for x in a) ** 0.5
    magnitude_b = sum(y * y for y in b) ** 0.5
    if magnitude_a == 0 or magnitude_b == 0:
        return 0.0
    return dot_product / (magnitude_a * magnitude_b)


def _get_embedding(text: str, ollama_url: str = "http://localhost:11434") -> list[float]:
    """Get embedding from Ollama."""
    cache_key = text.lower()
    if cache_key in _embedding_cache:
        return _embedding_cache[cache_key]

    try:
        response = httpx.post(
            f"{ollama_url}/api/embeddings",
            json={"model": "nomic-embed-text", "prompt": text},
            timeout=30.0,
        )
        if response.status_code != 200:
            logger.warning(f"Embedding failed for '{text}': {response.status_code}")
            return []

        data = response.json()
        embedding = data.get("embedding", [])
        _embedding_cache[cache_key] = embedding
        return embedding
    except Exception as e:
        logger.warning(f"Embedding error for '{text}': {e}")
        return []


def detect_pii_fields(
    fields: list[dict[str, Any]], threshold: float = 0.65, ollama_url: str = "http://localhost:11434"
) -> dict[str, Any]:
    """Detect PII fields and return results.

    Args:
        fields: List of field dicts with 'name', 'type' keys
        threshold: Similarity threshold (0-1, default 0.65)
        ollama_url: URL to Ollama API

    Returns:
        Dict with:
        - pii_fields: List of field names that are PII
        - pii_details: Dict mapping field name to {isPii, confidence, match}
    """
    pii_details = {}

    for field in fields:
        field_name = field.get("name", "").lower()
        if not field_name:
            continue

        field_embedding = _get_embedding(field_name, ollama_url)
        if not field_embedding:
            pii_details[field_name] = {"isPii": False, "confidence": 0, "match": ""}
            continue

        max_similarity = 0.0
        best_match = ""

        for pattern in PII_PATTERNS:
            pattern_embedding = _get_embedding(pattern, ollama_url)
            if not pattern_embedding:
                continue

            similarity = _cosine_similarity(field_embedding, pattern_embedding)
            if similarity > max_similarity:
                max_similarity = similarity
                best_match = pattern

        is_pii = max_similarity >= threshold
        pii_details[field_name] = {
            "isPii": is_pii,
            "confidence": round(max_similarity, 2),
            "match": best_match,
        }

    pii_fields = [name for name, details in pii_details.items() if details["isPii"]]

    return {
        "pii_fields": pii_fields,
        "pii_details": pii_details,
    }

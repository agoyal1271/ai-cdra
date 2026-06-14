"""
JWT identity middleware — decodes Bearer token, enriches request.state.identity.

Does NOT enforce auth (no 401 on missing/invalid tokens) — it only enriches.
In production, wire this to Cloudera SSO/Ranger for actual enforcement.

Claims extracted from JWT payload:
  sub                 — username (string)
  allowed_namespaces  — list[str] of Iceberg namespaces visible to this user
  allowed_topics      — list[str] of Kafka topic prefixes visible to this user
  allowed_ozone_vols  — list[str] of Ozone volume names visible to this user

Usage in a FastAPI route:
  from middleware.jwt_identity import get_identity
  identity = Depends(get_identity)
"""
import base64
import json
import logging
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


def _decode_payload(token: str) -> dict:
    """Base64-decodes the JWT payload WITHOUT verifying signature (internal trust model)."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return {}
        # JWT uses base64url without padding
        payload_b64 = parts[1] + "=" * (4 - len(parts[1]) % 4)
        return json.loads(base64.urlsafe_b64decode(payload_b64))
    except Exception:
        return {}


class JWTIdentityMiddleware(BaseHTTPMiddleware):
    """
    Starlette middleware — attaches identity dict to request.state.identity.
    When no token is present, identity.sub = "anonymous" with empty allow-lists
    (which means all assets are visible, for backward compatibility).
    """

    async def dispatch(self, request: Request, call_next):
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            payload = _decode_payload(auth_header[7:])
        else:
            payload = {}

        request.state.identity = {
            "sub": payload.get("sub", "anonymous"),
            "allowed_namespaces": payload.get("allowed_namespaces", []),
            "allowed_topics": payload.get("allowed_topics", []),
            "allowed_ozone_vols": payload.get("allowed_ozone_vols", []),
            "raw": payload,
        }
        logger.debug(f"[jwt] sub={request.state.identity['sub']!r}")
        return await call_next(request)


def get_identity(request: Request) -> dict:
    """FastAPI dependency — returns identity dict attached by JWTIdentityMiddleware."""
    return getattr(request.state, "identity", {
        "sub": "anonymous",
        "allowed_namespaces": [],
        "allowed_topics": [],
        "allowed_ozone_vols": [],
        "raw": {},
    })

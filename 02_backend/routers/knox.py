"""
Knox JWT management endpoints.

GET  /api/knox/status   — token expiry, seconds remaining, refresh config
POST /api/knox/refresh  — force-refresh the token right now
"""
import logging
import time

from fastapi import APIRouter

router = APIRouter(prefix="/api/knox", tags=["knox"])
logger = logging.getLogger(__name__)


@router.get("/status")
async def token_status():
    """
    Returns current Knox JWT status: expiry time, seconds remaining, and
    whether the automatic refresh loop is configured.
    """
    import config
    from agents.source_scout.sidecar import _jwt_exp

    token = config.KNOX_JWT
    exp = _jwt_exp(token) if token else None
    now = time.time()

    if exp is None:
        remaining = None
        expires_at = None
        state = "unknown"
    else:
        remaining = max(0, exp - now)
        expires_at = exp
        if remaining == 0:
            state = "expired"
        elif remaining < config.KNOX_TOKEN_REFRESH_BUFFER_SECS:
            state = "expiring_soon"
        else:
            state = "valid"

    return {
        "state": state,                                      # valid | expiring_soon | expired | unknown
        "expires_at": expires_at,                           # Unix timestamp
        "seconds_remaining": round(remaining) if remaining is not None else None,
        "refresh_buffer_secs": config.KNOX_TOKEN_REFRESH_BUFFER_SECS,
        "auto_refresh_configured": bool(config.KNOX_LOGIN_URL and config.KNOX_USERNAME),
        "token_present": bool(token),
    }


@router.post("/refresh")
async def force_refresh():
    """
    Force-refresh the Knox JWT right now, regardless of expiry.
    Requires KNOX_LOGIN_URL, KNOX_USERNAME, and KNOX_PASSWORD to be set.
    """
    import asyncio
    import config
    from agents.source_scout.sidecar import _jwt_exp

    if not config.KNOX_LOGIN_URL or not config.KNOX_USERNAME:
        return {
            "success": False,
            "error": "KNOX_LOGIN_URL and KNOX_USERNAME must be configured for auto-refresh",
        }

    # Expire the current token in memory so get_valid_knox_token() is forced to fetch a new one
    old_token = config.KNOX_JWT
    config.KNOX_JWT = ""   # blank it so the refresh logic always fetches

    import requests as _requests
    try:
        endpoint = f"{config.KNOX_LOGIN_URL.rstrip('/')}/knoxtoken/api/v1/token"
        resp = await asyncio.to_thread(
            lambda: _requests.get(
                endpoint,
                auth=(config.KNOX_USERNAME, config.KNOX_PASSWORD),
                timeout=10,
                verify=True,
            )
        )
        resp.raise_for_status()
        data = resp.json()
        new_token = data.get("access_token") or data.get("token")
        if not new_token:
            raise ValueError(f"Unexpected Knox response: {list(data.keys())}")
        config.KNOX_JWT = new_token
        exp = _jwt_exp(new_token)
        return {
            "success": True,
            "expires_at": exp,
            "seconds_until_expiry": round(exp - time.time()) if exp else None,
        }
    except Exception as exc:
        config.KNOX_JWT = old_token   # restore old token on failure
        logger.warning(f"[knox] force-refresh failed: {exc}")
        return {"success": False, "error": str(exc)}

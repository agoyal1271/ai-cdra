import logging
import os
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

sys.path.insert(0, os.path.dirname(__file__))

from routers import agents, health, nl_to_code, registry, knox, pipeline, openmetadata
from middleware.jwt_identity import JWTIdentityMiddleware

DEBUG_LOG = "/tmp/cloudera_agents_debug.log"


def _attach_file_logger():
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s")
    fh = logging.FileHandler(DEBUG_LOG, mode="a")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logging.root.setLevel(logging.DEBUG)
    logging.root.addHandler(fh)
    for noisy in ("uvicorn", "uvicorn.access", "httpx", "httpcore", "asyncio", "multipart"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    logging.getLogger(__name__).info(f"[startup] debug log → {DEBUG_LOG}")


async def _knox_refresh_loop():
    """
    Background task: checks Knox JWT expiry every 60 s.
    get_valid_knox_token() is a no-op when the token is still fresh, so this is cheap.
    When the token is within KNOX_TOKEN_REFRESH_BUFFER_SECS of expiry it fetches a new one.
    """
    import asyncio
    from agents.source_scout.sidecar import get_valid_knox_token

    _log = logging.getLogger(__name__)
    while True:
        await asyncio.sleep(60)
        try:
            await asyncio.to_thread(get_valid_knox_token)
        except Exception as exc:
            _log.warning(f"[knox_loop] refresh check failed: {exc}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio
    _attach_file_logger()
    _log = logging.getLogger(__name__)

    # ── Knox JWT background refresh ──────────────────────────────────────────
    from config import KNOX_LOGIN_URL
    knox_task = None
    if KNOX_LOGIN_URL:
        knox_task = asyncio.create_task(_knox_refresh_loop())
        _log.info("[startup] Knox JWT auto-refresh loop started (checks every 60 s)")

    # ── Schema Registry auto-index ───────────────────────────────────────────
    from config import SCHEMA_REGISTRY_URL
    if SCHEMA_REGISTRY_URL:
        try:
            from tools.kafka.schema_registry_cache import init_db, is_stale, get_stats
            init_db()

            # Warm cache synchronously if empty (with timeout)
            stats = get_stats()
            if stats.get("count", 0) == 0:
                _log.info("[startup] Schema Registry cache empty, warm-indexing (timeout: 30s)...")
                from tools.kafka.schema_registry_indexer import run_index
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(run_index),
                        timeout=30.0
                    )
                    _log.info("[startup] Schema Registry warm-indexed successfully")
                except asyncio.TimeoutError:
                    # Timeout: continue with empty cache, background refresh will populate it
                    asyncio.get_event_loop().run_in_executor(None, run_index)
                    _log.warning("[startup] SR warm-index timed out, retrying in background")
            elif is_stale():
                # Cache exists but is stale: refresh in background
                asyncio.get_event_loop().run_in_executor(None, run_index)
                _log.info("[startup] Schema Registry cache stale, background refresh triggered")
        except Exception as exc:
            _log.warning(f"[startup] SR auto-index skipped: {exc}")

    yield

    if knox_task:
        knox_task.cancel()


app = FastAPI(title="Cloudera AI Agents", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(JWTIdentityMiddleware)

app.include_router(agents.router)
app.include_router(health.router)
app.include_router(nl_to_code.router)
app.include_router(registry.router)
app.include_router(knox.router)
app.include_router(pipeline.router)
app.include_router(openmetadata.router)

# Serve frontend build if it exists
FRONTEND_DIST = os.path.join(os.path.dirname(__file__), "..", "03_frontend", "dist")
if os.path.isdir(FRONTEND_DIST):
    app.mount("/", StaticFiles(directory=FRONTEND_DIST, html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    from config import APP_PORT
    uvicorn.run("app:app", host="0.0.0.0", port=APP_PORT, reload=True)

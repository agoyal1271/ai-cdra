"""
PostgreSQL-backed Iceberg schema cache (replaces SQLite).

get_cached_schema(table_name) → dict | None   (None if missing or stale)
store_schema(table_name, schema_data)          (upserts; schema_data is the dict from list_iceberg_tables)
warm_cache(tables)                             (bulk store after full scan)
invalidate(table_name)                         (force-refresh a single table)

TTL: ICEBERG_CACHE_TTL_SECONDS (env, default 300 = 5 min)
"""
import logging

logger = logging.getLogger(__name__)


def get_cached_schema(table_name: str):
    from memory.postgres_cache import get_cached_schema as _get

    return _get(table_name)


def store_schema(table_name: str, schema_data: dict) -> None:
    from memory.postgres_cache import store_schema as _store

    _store(table_name, schema_data)
    logger.debug(f"[iceberg_cache] stored: {table_name}")


def invalidate(table_name: str) -> None:
    from memory.postgres_cache import invalidate as _invalidate

    _invalidate(table_name)
    logger.debug(f"[iceberg_cache] invalidated: {table_name}")


def warm_cache(tables: list[dict]) -> None:
    from memory.postgres_cache import warm_cache as _warm

    _warm(tables)
    logger.debug(f"[iceberg_cache] warmed {len(tables)} table(s)")

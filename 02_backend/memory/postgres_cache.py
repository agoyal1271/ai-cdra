"""
PostgreSQL-backed cache for Iceberg schemas and Schema Registry.
Thread-safe; survives server restarts.
Replaces SQLite for better concurrency and performance.
"""
import json
import logging
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)
_lock = threading.Lock()
_pool = None


def get_connection():
    """Get a connection from the pool."""
    global _pool
    if _pool is None:
        try:
            from psycopg2 import pool
            from config import POSTGRES_URL

            _pool = pool.SimpleConnectionPool(1, 20, POSTGRES_URL)
            logger.info(f"[postgres] connected to {POSTGRES_URL.split('@')[1]}")
            _init_tables()
        except Exception as e:
            logger.warning(f"[postgres] connection failed: {e}. Falling back to in-memory cache.")
            return None

    if _pool:
        try:
            return _pool.getconn()
        except Exception as e:
            logger.warning(f"[postgres] getconn failed: {e}")
            return None
    return None


def release_connection(conn):
    """Return a connection to the pool."""
    if conn and _pool:
        _pool.putconn(conn)


def _init_tables():
    """Create tables if they don't exist."""
    conn = None
    try:
        conn = get_connection()
        if not conn:
            return

        cur = conn.cursor()

        # Iceberg schema cache
        cur.execute("""
            CREATE TABLE IF NOT EXISTS iceberg_schema_cache (
                table_name    TEXT PRIMARY KEY,
                namespace     TEXT,
                fields_json   TEXT NOT NULL,
                snapshots     INTEGER DEFAULT 0,
                first_snap_ms INTEGER,
                location      TEXT,
                cached_at     REAL NOT NULL
            );
        """)

        # Schema Registry cache
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sr_schemas (
                schema_name     TEXT PRIMARY KEY,
                topic_name      TEXT,
                schema_group    TEXT DEFAULT 'Kafka',
                schema_type     TEXT DEFAULT 'avro',
                compatibility   TEXT DEFAULT '',
                schema_id       INTEGER,
                version         INTEGER,
                schema_text     TEXT DEFAULT '',
                fields_json     TEXT DEFAULT '[]',
                field_count     INTEGER DEFAULT 0,
                namespace_str   TEXT DEFAULT '',
                description     TEXT DEFAULT '',
                indexed_at      REAL DEFAULT 0
            );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_sr_topic ON sr_schemas(topic_name);
            CREATE INDEX IF NOT EXISTS idx_sr_group ON sr_schemas(schema_group);
        """)

        # SR metadata
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sr_meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
        """)

        conn.commit()
        logger.info("[postgres] tables initialized")
    except Exception as e:
        logger.error(f"[postgres] init failed: {e}")
    finally:
        if conn:
            release_connection(conn)


# === Iceberg Schema Cache ===

def get_cached_schema(table_name: str) -> Optional[dict]:
    """Get cached Iceberg schema if fresh."""
    from config import ICEBERG_CACHE_TTL_SECONDS

    conn = get_connection()
    if not conn:
        return None

    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT namespace, fields_json, snapshots, first_snap_ms, location, cached_at "
            "FROM iceberg_schema_cache WHERE table_name = %s",
            (table_name,)
        )
        row = cur.fetchone()

        if row is None:
            return None

        namespace, fields_json, snapshots, first_snap_ms, location, cached_at = row
        age = time.time() - cached_at

        if age > ICEBERG_CACHE_TTL_SECONDS:
            logger.debug(f"[iceberg_cache] stale ({age:.0f}s): {table_name}")
            return None

        return {
            "name": table_name,
            "namespace": namespace,
            "fields": json.loads(fields_json),
            "snapshots": snapshots,
            "first_snapshot_ms": first_snap_ms,
            "location": location,
            "mock": False,
            "_from_cache": True,
        }
    except Exception as e:
        logger.warning(f"[iceberg_cache] get failed: {e}")
        return None
    finally:
        release_connection(conn)


def store_schema(table_name: str, schema_data: dict) -> None:
    """Store or update Iceberg schema cache."""
    conn = get_connection()
    if not conn:
        return

    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO iceberg_schema_cache
                (table_name, namespace, fields_json, snapshots, first_snap_ms, location, cached_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (table_name) DO UPDATE SET
                namespace     = EXCLUDED.namespace,
                fields_json   = EXCLUDED.fields_json,
                snapshots     = EXCLUDED.snapshots,
                first_snap_ms = EXCLUDED.first_snap_ms,
                location      = EXCLUDED.location,
                cached_at     = EXCLUDED.cached_at
            """,
            (
                table_name,
                schema_data.get("namespace", table_name.split(".")[0] if "." in table_name else None),
                json.dumps(schema_data.get("fields", [])),
                schema_data.get("snapshots", 0),
                schema_data.get("first_snapshot_ms"),
                schema_data.get("location"),
                time.time(),
            )
        )
        conn.commit()
        logger.debug(f"[iceberg_cache] stored: {table_name}")
    except Exception as e:
        logger.warning(f"[iceberg_cache] store failed: {e}")
    finally:
        release_connection(conn)


def warm_cache(tables: list[dict]) -> None:
    """Bulk store schemas."""
    for table in tables:
        store_schema(table.get("name", ""), table)


def invalidate(table_name: str) -> None:
    """Invalidate a single table cache entry."""
    conn = get_connection()
    if not conn:
        return

    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM iceberg_schema_cache WHERE table_name = %s", (table_name,))
        conn.commit()
    except Exception as e:
        logger.warning(f"[iceberg_cache] invalidate failed: {e}")
    finally:
        release_connection(conn)


# === Schema Registry Cache ===

def init_sr_db() -> None:
    """Initialize Schema Registry tables (called from schema_registry.py)."""
    _init_tables()


def sr_get(schema_name: str) -> Optional[dict]:
    """Get cached schema from Schema Registry."""
    conn = get_connection()
    if not conn:
        return None

    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT schema_id, version, schema_text, fields_json, namespace_str, description, indexed_at "
            "FROM sr_schemas WHERE schema_name = %s",
            (schema_name,)
        )
        row = cur.fetchone()

        if row is None:
            return None

        return {
            "schema_id": row[0],
            "version": row[1],
            "schema_text": row[2],
            "fields": json.loads(row[3]),
            "namespace": row[4],
            "description": row[5],
            "indexed_at": row[6],
        }
    except Exception as e:
        logger.warning(f"[sr_cache] get failed: {e}")
        return None
    finally:
        release_connection(conn)


def sr_upsert(schema_name: str, data: dict) -> None:
    """Upsert Schema Registry schema."""
    conn = get_connection()
    if not conn:
        return

    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO sr_schemas
                (schema_name, topic_name, schema_group, schema_type, compatibility, schema_id,
                 version, schema_text, fields_json, field_count, namespace_str, description, indexed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (schema_name) DO UPDATE SET
                topic_name    = EXCLUDED.topic_name,
                schema_group  = EXCLUDED.schema_group,
                schema_type   = EXCLUDED.schema_type,
                compatibility = EXCLUDED.compatibility,
                schema_id     = EXCLUDED.schema_id,
                version       = EXCLUDED.version,
                schema_text   = EXCLUDED.schema_text,
                fields_json   = EXCLUDED.fields_json,
                field_count   = EXCLUDED.field_count,
                namespace_str = EXCLUDED.namespace_str,
                description   = EXCLUDED.description,
                indexed_at    = EXCLUDED.indexed_at
            """,
            (
                schema_name,
                data.get("topic_name", ""),
                data.get("schema_group", "Kafka"),
                data.get("schema_type", "avro"),
                data.get("compatibility", ""),
                data.get("schema_id"),
                data.get("version"),
                data.get("schema_text", ""),
                json.dumps(data.get("fields", [])),
                len(data.get("fields", [])),
                data.get("namespace", ""),
                data.get("description", ""),
                time.time(),
            )
        )
        conn.commit()
    except Exception as e:
        logger.warning(f"[sr_cache] upsert failed: {e}")
    finally:
        release_connection(conn)


def sr_get_by_topic(topic_name: str) -> list[dict]:
    """Get all schemas for a topic."""
    conn = get_connection()
    if not conn:
        return []

    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT schema_name, schema_text, fields_json FROM sr_schemas WHERE topic_name = %s",
            (topic_name,)
        )
        return [
            {
                "name": row[0],
                "text": row[1],
                "fields": json.loads(row[2]),
            }
            for row in cur.fetchall()
        ]
    except Exception as e:
        logger.warning(f"[sr_cache] get_by_topic failed: {e}")
        return []
    finally:
        release_connection(conn)


def sr_meta_get(key: str, default: str = "") -> str:
    """Get metadata value."""
    conn = get_connection()
    if not conn:
        return default

    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM sr_meta WHERE key = %s", (key,))
        row = cur.fetchone()
        return row[0] if row else default
    except Exception as e:
        logger.warning(f"[sr_cache] meta_get failed: {e}")
        return default
    finally:
        release_connection(conn)


def sr_meta_set(key: str, value: str) -> None:
    """Set metadata value."""
    conn = get_connection()
    if not conn:
        return

    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO sr_meta (key, value) VALUES (%s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            (key, value)
        )
        conn.commit()
    except Exception as e:
        logger.warning(f"[sr_cache] meta_set failed: {e}")
    finally:
        release_connection(conn)


def sr_clear() -> None:
    """Clear all cached schemas."""
    conn = get_connection()
    if not conn:
        return

    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM sr_schemas")
        conn.commit()
    except Exception as e:
        logger.warning(f"[sr_cache] clear failed: {e}")
    finally:
        release_connection(conn)

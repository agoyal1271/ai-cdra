"""
PostgreSQL-backed cache for Schema Registry schemas (replaces SQLite).
Supports Cloudera SR (aggregated endpoint) and Confluent SR.
Thread-safe; survives server restarts.
"""
import json
import logging
import os
import time

logger = logging.getLogger(__name__)

SR_CACHE_TTL = int(os.getenv("SCHEMA_REGISTRY_CACHE_TTL_SECONDS", "3600"))


def init_db() -> None:
    """Initialize database tables (handled by postgres_cache module)."""
    from memory.postgres_cache import init_sr_db
    init_sr_db()


def _extract_fields(schema_text: str) -> tuple[list[dict], str]:
    """Parse Avro/JSON schema, return (fields, namespace)."""
    if not schema_text:
        return [], ""
    try:
        raw = json.loads(schema_text) if isinstance(schema_text, str) else schema_text
        if isinstance(raw, dict):
            fields = [
                {"name": f.get("name"), "type": str(f.get("type", "?"))}
                for f in raw.get("fields", [])
            ]
            return fields, raw.get("namespace", "")
    except Exception:
        pass
    return [], ""


def store_schemas_bulk(schemas: list[dict]) -> int:
    """Bulk-upsert schemas using PostgreSQL."""
    from memory.postgres_cache import sr_upsert, sr_meta_set

    now = time.time()
    count = 0

    for s in schemas:
        name = s.get("name") or s.get("schema_name", "")
        if not name:
            continue

        topic = s.get("topic_name") or name
        schema_text = s.get("schema", "") or s.get("schema_text", "") or ""
        fields, namespace = _extract_fields(schema_text)

        sr_upsert(name, {
            "topic_name": topic,
            "schema_group": s.get("schemaGroup", s.get("schema_group", "Kafka")),
            "schema_type": s.get("type", s.get("schema_type", "avro")),
            "compatibility": s.get("compatibility", ""),
            "schema_id": s.get("id", s.get("schema_id")),
            "version": s.get("version"),
            "schema_text": schema_text,
            "fields": fields,
            "namespace": namespace,
            "description": s.get("description", ""),
            "indexed_at": now,
        })
        count += 1

    if count > 0:
        sr_meta_set("last_indexed", str(now))

    return count


def get_stats() -> dict:
    """Get cache statistics."""
    from memory.postgres_cache import sr_meta_get, get_connection

    conn = get_connection()
    if not conn:
        return {"total_schemas": 0, "by_group": {}, "last_indexed_at": None, "is_stale": True}

    try:
        cur = conn.cursor()

        # Total schemas
        cur.execute("SELECT COUNT(*) FROM sr_schemas")
        total = cur.fetchone()[0]

        # By group
        cur.execute("SELECT schema_group, COUNT(*) AS cnt FROM sr_schemas GROUP BY schema_group")
        groups = {row[0]: row[1] for row in cur.fetchall()}

        # Last indexed
        last_indexed_str = sr_meta_get("last_indexed")
        last_indexed = float(last_indexed_str) if last_indexed_str else None

        from memory.postgres_cache import release_connection
        release_connection(conn)

        return {
            "total_schemas": total,
            "by_group": groups,
            "last_indexed_at": last_indexed,
            "is_stale": last_indexed is None or (time.time() - last_indexed > SR_CACHE_TTL),
        }
    except Exception as e:
        logger.warning(f"[sr_cache] stats failed: {e}")
        return {"total_schemas": 0, "by_group": {}, "last_indexed_at": None, "is_stale": True}


def is_stale() -> bool:
    """Check if schema cache is stale."""
    return get_stats()["is_stale"]


def search_sql(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SELECT against the schema cache."""
    import sqlite3
    import pathlib

    from memory.postgres_cache import get_connection, release_connection

    conn = get_connection()
    if not conn:
        # Fallback to SQLite if PostgreSQL is unavailable
        sqlite_paths = [
            pathlib.Path("/Users/archit/cloudera-ai-agents/02_backend/sr_schemas.db"),
            pathlib.Path("sr_schemas.db"),
            pathlib.Path("./sr_schemas.db"),
        ]
        sqlite_db = None
        for p in sqlite_paths:
            if p.exists():
                sqlite_db = p
                break

        if sqlite_db:
            try:
                sqlite_conn = sqlite3.connect(sqlite_db)
                sqlite_conn.row_factory = sqlite3.Row
                cur = sqlite_conn.cursor()
                cur.execute(sql, params)
                cols = [desc[0] for desc in cur.description] if cur.description else []
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                sqlite_conn.close()
                return rows
            except Exception as e:
                logger.debug(f"SQLite fallback query failed: {e}")
                return []
        raise ValueError("PostgreSQL connection unavailable and SQLite fallback not found")

    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [desc[0] for desc in cur.description] if cur.description else []
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        return rows
    except Exception as e:
        raise ValueError(f"SQL error: {e}") from e
    finally:
        release_connection(conn)


def get_table_ddl() -> str:
    """DDL description injected into LLM prompts for NL-to-SQL generation."""
    return """CREATE TABLE sr_schemas (
    schema_name     TEXT PRIMARY KEY,  -- topic/schema name (e.g. 'orders', 'payments')
    topic_name      TEXT,              -- same as schema_name for Cloudera SR
    schema_group    TEXT,              -- 'Kafka' | 'Hadoop' | other group label
    schema_type     TEXT,              -- 'avro' | 'json' | 'protobuf'
    compatibility   TEXT,              -- 'BACKWARD' | 'FORWARD' | 'FULL' | 'NONE'
    schema_id       INTEGER,           -- numeric registry ID
    version         INTEGER,           -- schema version number
    schema_text     TEXT,              -- full Avro/JSON schema string
    fields_json     TEXT,              -- JSON array: [{"name":"id","type":"string"}, ...]
    field_count     INTEGER,           -- number of fields in the schema
    namespace_str   TEXT,              -- Avro namespace (e.g. 'com.company.payments')
    description     TEXT,              -- human description from the registry
    indexed_at      REAL               -- Unix timestamp of last index
);

-- Useful query patterns:
-- Search by field name:     LOWER(fields_json) LIKE LOWER('%"name": "fieldname"%')
--                       OR  LOWER(fields_json) LIKE LOWER('%fieldname%')
-- Filter by topic name:     LOWER(topic_name) LIKE LOWER('%keyword%')
-- Filter by namespace:      LOWER(namespace_str) LIKE LOWER('%com.example%')
-- Count fields comparison:  field_count > 5
-- Filter by group:          schema_group = 'Kafka'
"""

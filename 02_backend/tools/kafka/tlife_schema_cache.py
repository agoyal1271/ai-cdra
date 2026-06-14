"""
Offline Schema Registry cache for T-Life topics.
Used when the live SR backend is unavailable (error_code 50001).
Populates ChromaDB with T-Life schemas for Source Scout discovery.
"""
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Optional, Dict, List

from tools.kafka.generate_tlife_topics import TLIFE_SCHEMAS, list_all_topics

logger = logging.getLogger(__name__)

CACHE_DB = "tlife_schema_cache.db"

def init_tlife_cache() -> None:
    """Initialize SQLite cache with all T-Life schemas."""
    db_path = Path(CACHE_DB)

    # Create or connect
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tlife_schemas (
            topic_name TEXT PRIMARY KEY,
            namespace TEXT,
            schema_json TEXT,
            field_count INTEGER,
            fields_json TEXT,
            description TEXT,
            indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Insert all T-Life schemas
    for topic_name in list_all_topics():
        schema = TLIFE_SCHEMAS[topic_name]
        fields = schema.get("fields", [])
        field_names = [f["name"] for f in fields]
        field_count = len(fields)

        # Build description from fields
        field_desc = ", ".join(field_names[:5])
        if len(field_names) > 5:
            field_desc += f", ... ({field_count} total)"

        cursor.execute("""
            INSERT OR REPLACE INTO tlife_schemas
            (topic_name, namespace, schema_json, field_count, fields_json, description)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            topic_name,
            schema.get("namespace", ""),
            json.dumps(schema),
            field_count,
            json.dumps(field_names),
            field_desc,
        ))

    conn.commit()
    conn.close()

    logger.info(f"✓ T-Life schema cache initialized: {db_path} ({len(list_all_topics())} topics)")

def get_tlife_schema(topic_name: str) -> Optional[Dict[str, Any]]:
    """Retrieve a schema from the cache."""
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()

    cursor.execute("SELECT schema_json FROM tlife_schemas WHERE topic_name = ?", (topic_name,))
    row = cursor.fetchone()
    conn.close()

    if row:
        return json.loads(row[0])
    return None

def list_tlife_schemas() -> List[str]:
    """List all cached T-Life topic names."""
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()

    cursor.execute("SELECT topic_name FROM tlife_schemas ORDER BY topic_name")
    topics = [row[0] for row in cursor.fetchall()]
    conn.close()

    return topics

def get_tlife_schema_details(topic_name: str) -> Optional[Dict[str, Any]]:
    """Get full schema details including fields."""
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT topic_name, namespace, field_count, fields_json, description
        FROM tlife_schemas WHERE topic_name = ?
    """, (topic_name,))
    row = cursor.fetchone()
    conn.close()

    if row:
        return {
            "topic_name": row[0],
            "namespace": row[1],
            "field_count": row[2],
            "fields": json.loads(row[3]),
            "description": row[4],
        }
    return None

def export_tlife_as_json(output_file: str = "tlife_schemas_export.json") -> None:
    """Export all T-Life schemas to JSON for documentation/import."""
    schemas = {}
    for topic_name in list_tlife_schemas():
        schema = get_tlife_schema(topic_name)
        schemas[topic_name] = schema

    with open(output_file, "w") as f:
        json.dump(schemas, f, indent=2)

    logger.info(f"✓ Exported {len(schemas)} schemas to {output_file}")

if __name__ == "__main__":
    pass

    # Initialize cache
    init_tlife_cache()

    # Export for reference
    export_tlife_as_json()

    # Test retrieval
    print(f"\nCached T-Life Topics ({len(list_tlife_schemas())}):")
    for topic in sorted(list_tlife_schemas())[:5]:
        details = get_tlife_schema_details(topic)
        print(f"  • {topic}: {details['field_count']} fields")
    print(f"  ... and {len(list_tlife_schemas()) - 5} more")

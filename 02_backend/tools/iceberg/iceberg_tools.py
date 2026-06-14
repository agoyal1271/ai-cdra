import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

# TTL cache for list_iceberg_tables — REST catalog round-trips cost ~12s.
# Catalog membership/schemas don't change often; 60s is a reasonable freshness window.
_LIST_CACHE: dict[str, Any] = {"result": None, "expires_at": 0.0}
_LIST_CACHE_TTL_SECS = float(os.getenv("ICEBERG_LIST_CACHE_TTL_SECS", "60"))


def invalidate_iceberg_list_cache() -> None:
    """Force the next list_iceberg_tables() call to re-fetch from the catalog."""
    _LIST_CACHE["result"] = None
    _LIST_CACHE["expires_at"] = 0.0


class TokenExpiredError(Exception):
    """Raised when the Knox JWT is expired and cannot be refreshed automatically."""


def _knox_headers() -> dict:
    """Returns Authorization header if KNOX_JWT is set. Always reads from env
    at call time so a refreshed token is picked up without restarting the process."""
    token = os.getenv("KNOX_JWT", "")
    if token:
        return {"Authorization": f"Bearer {token}"}
    return {}


MOCK_TABLES = [
    {
        "name": "demo.users",
        "location": "/Users/archit/iceberg-warehouse/demo/users",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "created_at", "type": "timestamptz"},
            {"name": "plan_code", "type": "string"},
        ],
        "partition_spec": "days(created_at)",
        "snapshots": 3,
        "row_count": 125_000,
        "file_count": 4,
        "size_bytes": 2_450_000,
        "mock": False,
    },
    {
        "name": "demo.customers",
        "location": "/Users/archit/iceberg-warehouse/demo/customers",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "customer_name", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "phone", "type": "string"},
            {"name": "created_at", "type": "timestamptz"},
        ],
        "partition_spec": "days(created_at)",
        "snapshots": 2,
        "row_count": 50_000,
        "file_count": 2,
        "size_bytes": 1_200_000,
        "mock": False,
    },
]


def _load_catalog():
    from pyiceberg.catalog import load_catalog
    from config import ICEBERG_CATALOG_TYPE, ICEBERG_CATALOG_URI, ICEBERG_WAREHOUSE

    # Refresh token if configured, then read the current value from env
    try:
        from agents.source_scout.sidecar import get_valid_knox_token
        knox_jwt = get_valid_knox_token()
    except ImportError:
        knox_jwt = os.getenv("KNOX_JWT", "")

    props = {"type": ICEBERG_CATALOG_TYPE}
    if ICEBERG_CATALOG_URI:
        props["uri"] = ICEBERG_CATALOG_URI
    if ICEBERG_WAREHOUSE:
        props["warehouse"] = ICEBERG_WAREHOUSE
    if knox_jwt and ICEBERG_CATALOG_TYPE == "rest":
        props["token"] = knox_jwt
    return load_catalog("default", **props)


def list_iceberg_tables(force_refresh: bool = False) -> list[dict[str, Any]]:
    """Lists all Iceberg tables in the catalog with basic metadata.

    Cached for ICEBERG_LIST_CACHE_TTL_SECS (default 60s) — REST-catalog round-trips
    cost ~12s for a small catalog. Pass force_refresh=True to bypass the cache.
    """
    now = time.monotonic()
    if not force_refresh and _LIST_CACHE["result"] is not None and now < _LIST_CACHE["expires_at"]:
        logger.debug(f"[iceberg] returning cached table list ({len(_LIST_CACHE['result'])} tables)")
        return _LIST_CACHE["result"]

    try:
        catalog = _load_catalog()
        tables = []
        for ns in catalog.list_namespaces():
            ns_name = ".".join(ns)
            for tbl_id in catalog.list_tables(ns):
                tbl_name = f"{ns_name}.{tbl_id[1]}"
                try:
                    tbl = catalog.load_table(tbl_id)
                    fields = [{"name": f.name, "type": str(f.field_type)} for f in tbl.schema().fields]
                    snaps = tbl.metadata.snapshots if hasattr(tbl, "metadata") and tbl.metadata.snapshots else []
                    snapshots = len(snaps)
                    first_snapshot_ms = min((s.timestamp_ms for s in snaps), default=None) if snaps else None
                    logger.debug(
                        f"[iceberg] table={tbl_name} snapshots={snapshots} "
                        f"first_snapshot_ms={first_snapshot_ms} "
                        f"fields={[f['name'] for f in fields]}"
                    )
                    tables.append({
                        "name": tbl_name,
                        "location": tbl.location(),
                        "fields": fields,
                        "partition_spec": str(tbl.spec()),
                        "snapshots": snapshots,
                        "first_snapshot_ms": first_snapshot_ms,
                        "mock": False,
                    })
                except Exception as e:
                    tables.append({"name": tbl_name, "error": str(e), "mock": False})
        result = tables if tables else MOCK_TABLES

        # Push to unified ChromaDB catalog for semantic search
        try:
            from tools.catalog import catalog_store
            catalog_store.index_iceberg_tables_bulk(result)
        except Exception as _ce:
            logger.debug(f"[iceberg] catalog push skipped: {_ce}")

        # Populate TTL cache so the next ~60s of callers skip the round-trips
        _LIST_CACHE["result"] = result
        _LIST_CACHE["expires_at"] = time.monotonic() + _LIST_CACHE_TTL_SECS
        return result
    except Exception as e:
        logger.warning(f"Iceberg catalog unavailable ({e}), returning mock tables")
        return MOCK_TABLES


def describe_iceberg_table(table_name: str) -> dict[str, Any]:
    """Returns schema, partition spec, and snapshot history for a table."""
    try:
        catalog = _load_catalog()
        parts = table_name.split(".")
        tbl_id = (parts[0], parts[1]) if len(parts) == 2 else (parts[0], parts[0])
        tbl = catalog.load_table(tbl_id)
        schema = tbl.schema()
        fields = [{"name": f.name, "type": str(f.field_type), "required": f.required} for f in schema.fields]

        # Try to get snapshot history, but don't fail if it errors
        history = []
        try:
            for snap in tbl.history():
                try:
                    op = snap.summary.get("operation") if hasattr(snap, "summary") and snap.summary else None
                except:
                    op = None
                history.append({
                    "snapshot_id": snap.snapshot_id,
                    "timestamp_ms": snap.timestamp_ms,
                    "operation": op
                })
        except Exception as snap_err:
            logger.debug(f"Could not get snapshot history for {table_name}: {snap_err}")
            history = []

        return {
            "name": table_name,
            "location": tbl.location(),
            "format_version": tbl.format_version,
            "fields": fields,
            "partition_spec": str(tbl.spec()),
            "sort_order": str(tbl.sort_order()),
            "snapshots": history,
            "properties": dict(tbl.properties),
            "mock": False,
        }
    except Exception as e:
        logger.warning(f"Could not describe table {table_name}: {e}")
        mock = next((t for t in MOCK_TABLES if t["name"] == table_name), MOCK_TABLES[0])
        return {**mock, "mock": True, "note": str(e)}


def get_iceberg_table_stats(table_name: str) -> dict[str, Any]:
    """Estimates row count, file count, and size from Iceberg metadata."""
    try:
        catalog = _load_catalog()
        parts = table_name.split(".")
        tbl_id = (parts[0], parts[1]) if len(parts) == 2 else (parts[0], parts[0])
        tbl = catalog.load_table(tbl_id)
        scan = tbl.scan()
        tasks = list(scan.plan_files())
        total_rows = sum(t.file.record_count for t in tasks if t.file.record_count)
        total_size = sum(t.file.file_size_in_bytes for t in tasks if t.file.file_size_in_bytes)
        return {
            "table": table_name,
            "file_count": len(tasks),
            "row_count": total_rows,
            "size_bytes": total_size,
            "size_mb": round(total_size / 1024 / 1024, 2),
            "mock": False,
        }
    except Exception as e:
        logger.warning(f"Could not get stats for {table_name}: {e}")
        mock = next((t for t in MOCK_TABLES if t["name"] == table_name), MOCK_TABLES[0])
        return {
            "table": table_name,
            "file_count": mock.get("file_count", 4),
            "row_count": mock.get("row_count", 125_000),
            "size_bytes": mock.get("size_bytes", 2_450_000),
            "size_mb": round(mock.get("size_bytes", 2_450_000) / 1024 / 1024, 2),
            "mock": True,
        }

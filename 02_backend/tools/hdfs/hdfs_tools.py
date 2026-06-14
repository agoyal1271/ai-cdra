import logging
import requests
from typing import Any

logger = logging.getLogger(__name__)

MOCK_PATHS = {
    "/": [
        {"path": "/data", "type": "DIRECTORY", "size": 0, "replication": 0},
        {"path": "/user", "type": "DIRECTORY", "size": 0, "replication": 0},
        {"path": "/tmp", "type": "DIRECTORY", "size": 0, "replication": 0},
    ],
    "/data": [
        {"path": "/data/raw", "type": "DIRECTORY", "size": 0, "replication": 0},
        {"path": "/data/processed", "type": "DIRECTORY", "size": 0, "replication": 0},
        {"path": "/data/archive", "type": "DIRECTORY", "size": 0, "replication": 0},
    ],
    "/data/raw": [
        {"path": "/data/raw/customer_events", "type": "DIRECTORY", "size": 0},
        {"path": "/data/raw/iot_telemetry", "type": "DIRECTORY", "size": 0},
        {"path": "/data/raw/member_eligibility.csv", "type": "FILE", "size": 85_000_000, "replication": 3, "block_size": 134217728},
    ],
    "/data/processed": [
        {"path": "/data/processed/customer_events_parquet", "type": "DIRECTORY", "size": 0},
        {"path": "/data/processed/member_eligibility_parquet", "type": "DIRECTORY", "size": 0},
    ],
}


def list_hdfs_path(path: str = "/") -> list[dict[str, Any]]:
    """Lists files and directories at an HDFS path via WebHDFS REST API."""
    from config import HDFS_WEBHDFS_URL, HDFS_USER
    try:
        url = f"{HDFS_WEBHDFS_URL}/webhdfs/v1{path}?op=LISTSTATUS&user.name={HDFS_USER}"
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        statuses = resp.json().get("FileStatuses", {}).get("FileStatus", [])
        return [
            {
                "path": f"{path.rstrip('/')}/{s['pathSuffix']}",
                "type": s["type"],
                "size": s.get("length", 0),
                "replication": s.get("replication", 0),
                "block_size": s.get("blockSize"),
                "owner": s.get("owner"),
                "mock": False,
            }
            for s in statuses
        ]
    except Exception as e:
        logger.warning(f"HDFS unavailable ({e}), returning mock for {path}")
        return MOCK_PATHS.get(path, [{"path": path, "type": "DIRECTORY", "size": 0, "mock": True}])


def get_hdfs_file_info(path: str) -> dict[str, Any]:
    """Gets detailed status for a specific HDFS file or directory."""
    from config import HDFS_WEBHDFS_URL, HDFS_USER
    try:
        url = f"{HDFS_WEBHDFS_URL}/webhdfs/v1{path}?op=GETFILESTATUS&user.name={HDFS_USER}"
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        s = resp.json().get("FileStatus", {})
        return {
            "path": path,
            "type": s.get("type"),
            "size": s.get("length", 0),
            "replication": s.get("replication", 0),
            "block_size": s.get("blockSize"),
            "owner": s.get("owner"),
            "group": s.get("group"),
            "permissions": s.get("permission"),
            "modification_time": s.get("modificationTime"),
            "mock": False,
        }
    except Exception as e:
        logger.warning(f"Could not get HDFS info for {path}: {e}")
        return {"path": path, "type": "DIRECTORY", "size": 0, "mock": True, "note": str(e)}

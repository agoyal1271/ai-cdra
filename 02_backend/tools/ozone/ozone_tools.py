import logging
from typing import Any

logger = logging.getLogger(__name__)

MOCK_VOLUMES = [
    {"name": "raw-landing", "quota_gb": 500, "object_count": 1_240, "mock": True},
    {"name": "processed-data", "quota_gb": 1000, "object_count": 8_720, "mock": True},
    {"name": "archive", "quota_gb": 2000, "object_count": 45_300, "mock": True},
]

MOCK_KEYS = {
    "raw-landing": [
        {"key": "customer/2026/05/09/events_001.json", "size_bytes": 4_200_000, "last_modified": "2026-05-09T08:00:00Z"},
        {"key": "customer/2026/05/09/events_002.json", "size_bytes": 3_900_000, "last_modified": "2026-05-09T08:30:00Z"},
        {"key": "iot/2026/05/09/telemetry_batch_001.parquet", "size_bytes": 18_500_000, "last_modified": "2026-05-09T09:00:00Z"},
    ],
    "processed-data": [
        {"key": "iceberg/customer_events/data/part-00001.parquet", "size_bytes": 52_000_000, "last_modified": "2026-05-09T09:10:00Z"},
        {"key": "iceberg/member_eligibility/data/part-00001.parquet", "size_bytes": 12_000_000, "last_modified": "2026-05-09T07:00:00Z"},
    ],
    "archive": [
        {"key": "2026/04/customer_events_archive.tar.gz", "size_bytes": 2_100_000_000, "last_modified": "2026-05-01T00:00:00Z"},
    ],
}


def _s3_client():
    import boto3
    from botocore.config import Config
    from config import OZONE_ENDPOINT, OZONE_ACCESS_KEY, OZONE_SECRET_KEY
    return boto3.client(
        "s3",
        endpoint_url=OZONE_ENDPOINT,
        aws_access_key_id=OZONE_ACCESS_KEY or "anonymous",
        aws_secret_access_key=OZONE_SECRET_KEY or "anonymous",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def list_ozone_volumes() -> list[dict[str, Any]]:
    """Lists all Ozone volumes (S3 buckets) with quota and object count."""
    try:
        s3 = _s3_client()
        buckets = s3.list_buckets().get("Buckets", [])
        result = []
        for b in buckets:
            result.append({"name": b["Name"], "created": str(b.get("CreationDate", "")), "mock": False})
        return result if result else MOCK_VOLUMES
    except Exception as e:
        logger.warning(f"Ozone unavailable ({e}), returning mock volumes")
        return MOCK_VOLUMES


def list_ozone_keys(volume: str, prefix: str = "") -> list[dict[str, Any]]:
    """Lists objects in an Ozone volume with size and last modified."""
    try:
        s3 = _s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=volume, Prefix=prefix, PaginationConfig={"MaxItems": 50}):
            for obj in page.get("Contents", []):
                keys.append({
                    "key": obj["Key"],
                    "size_bytes": obj["Size"],
                    "last_modified": str(obj["LastModified"]),
                    "mock": False,
                })
        return keys if keys else MOCK_KEYS.get(volume, [])
    except Exception as e:
        logger.warning(f"Could not list Ozone keys in {volume}: {e}")
        return MOCK_KEYS.get(volume, [])


def get_ozone_object_metadata(volume: str, key: str) -> dict[str, Any]:
    """Gets metadata for a specific object in Ozone."""
    try:
        s3 = _s3_client()
        resp = s3.head_object(Bucket=volume, Key=key)
        return {
            "volume": volume,
            "key": key,
            "size_bytes": resp.get("ContentLength"),
            "content_type": resp.get("ContentType"),
            "last_modified": str(resp.get("LastModified")),
            "metadata": resp.get("Metadata", {}),
            "mock": False,
        }
    except Exception as e:
        logger.warning(f"Could not get metadata for {volume}/{key}: {e}")
        return {"volume": volume, "key": key, "error": str(e), "mock": True}

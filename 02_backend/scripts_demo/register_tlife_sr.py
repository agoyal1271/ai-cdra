#!/usr/bin/env python3
"""
Register all 25 T-Life Kafka topic schemas in Cloudera Schema Registry.
Handles Knox authentication and SSL verification.
"""
import json
import sys
import logging
from pathlib import Path
import argparse
from typing import Optional

# Add parent dirs to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.kafka.generate_tlife_topics import TLIFE_SCHEMAS, list_all_topics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def register_schema(
    sr_url: str,
    topic_name: str,
    schema_dict: dict,
    username: Optional[str] = None,
    password: Optional[str] = None,
    verify_ssl: bool = False,
) -> tuple[bool, str]:
    """
    Register a single schema in the Schema Registry.

    Returns:
        (success: bool, message: str)
    """
    import requests
    from requests.auth import HTTPBasicAuth

    subject = f"{topic_name}-value"
    schema_str = json.dumps(schema_dict)

    # Build URL and headers
    url = f"{sr_url}/subjects/{subject}/versions"
    headers = {
        "Content-Type": "application/vnd.schemaregistry.v1+json",
        "Accept": "application/vnd.schemaregistry.v1+json",
    }
    payload = {"schema": schema_str, "schemaType": "AVRO"}

    # Build auth
    auth = HTTPBasicAuth(username, password) if username and password else None

    try:
        resp = requests.post(
            url,
            json=payload,
            headers=headers,
            auth=auth,
            verify=verify_ssl,
            timeout=10,
        )

        if resp.status_code in (200, 201):
            schema_id = resp.json().get("id", "N/A")
            msg = f"✓ {subject} (schema_id={schema_id})"
            return (True, msg)
        elif resp.status_code == 409:
            # Already exists — that's fine
            msg = f"→ {subject} (already registered)"
            return (True, msg)
        else:
            msg = f"✗ {subject} [HTTP {resp.status_code}]: {resp.text[:150]}"
            return (False, msg)

    except requests.exceptions.SSLError as e:
        msg = f"✗ {subject} [SSL Error]: {str(e)[:100]}"
        return (False, msg)
    except requests.exceptions.Timeout:
        msg = f"✗ {subject} [Timeout after 10s]"
        return (False, msg)
    except Exception as e:
        msg = f"✗ {subject} [{type(e).__name__}]: {str(e)[:100]}"
        return (False, msg)

def main():
    parser = argparse.ArgumentParser(
        description="Register 25 T-Life topics in Cloudera Schema Registry"
    )
    parser.add_argument(
        "--sr-url",
        default="http://cdp-utility.cdp.local:8443/gateway/cdp-proxy-api/schema-registry",
        help="Schema Registry base URL",
    )
    parser.add_argument(
        "--username",
        default="admin",
        help="Schema Registry username (Knox)",
    )
    parser.add_argument(
        "--password",
        default="",
        help="Schema Registry password (Knox)",
    )
    parser.add_argument(
        "--verify-ssl",
        action="store_true",
        help="Verify SSL certificates (default: skip verification for self-signed certs)",
    )

    args = parser.parse_args()

    # If password not provided, try to read from env
    if not args.password:
        import os
        args.password = os.getenv("SCHEMA_REGISTRY_PASSWORD", "")

    sr_url = args.sr_url.rstrip("/")
    topics = sorted(list_all_topics())

    logger.info(f"\n{'='*70}")
    logger.info(f"Registering {len(topics)} T-Life Kafka schemas")
    logger.info(f"Target: {sr_url}")
    logger.info(f"Auth: {args.username}:{'*' * len(args.password)}")
    logger.info(f"{'='*70}\n")

    results = {"success": [], "skipped": [], "failed": []}

    for i, topic_name in enumerate(topics, 1):
        schema = TLIFE_SCHEMAS[topic_name]
        success, msg = register_schema(
            sr_url,
            topic_name,
            schema,
            username=args.username,
            password=args.password,
            verify_ssl=args.verify_ssl,
        )

        logger.info(f"[{i:2d}/{len(topics)}] {msg}")

        if success:
            if "already" in msg.lower():
                results["skipped"].append(topic_name)
            else:
                results["success"].append(topic_name)
        else:
            results["failed"].append(topic_name)

    # Summary
    logger.info(f"\n{'='*70}")
    logger.info(f"SUMMARY:")
    logger.info(f"  Registered: {len(results['success'])}")
    logger.info(f"  Already present: {len(results['skipped'])}")
    logger.info(f"  Failed: {len(results['failed'])}")
    logger.info(f"{'='*70}\n")

    if results["failed"]:
        logger.warning(f"Failed topics: {', '.join(results['failed'])}")
        return 1

    logger.info("✓ All schemas registered successfully!")
    return 0

if __name__ == "__main__":
    sys.exit(main())

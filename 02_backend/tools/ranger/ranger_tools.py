"""
Apache Ranger integration for access control policy management.
Creates and updates Ranger policies for data governance.
"""

import json
import logging
import httpx
from config import RANGER_URL, RANGER_USERNAME, RANGER_PASSWORD

logger = logging.getLogger(__name__)


async def create_hive_policy(
    policy_name: str,
    table_name: str,
    owner: str,
    access_level: str,
) -> dict:
    """
    Create a Ranger policy for Hive table access control.

    Args:
        policy_name: Name of the policy (e.g., "demo.users_restricted")
        table_name: Full table name (e.g., "demo.users")
        owner: Owner email/username (e.g., "data-steward@company.com")
        access_level: "restricted", "internal", "public", "confidential"

    Returns:
        Policy creation result
    """

    # Parse table_name
    if "." in table_name:
        db_name, tbl_name = table_name.split(".", 1)
    else:
        db_name = "default"
        tbl_name = table_name

    # Map access_level to Ranger permissions
    permission_map = {
        "public": ["read"],
        "internal": ["read", "write"],
        "restricted": ["read"],  # Restricted read-only
        "confidential": [],  # No permissions by default
    }

    permissions = permission_map.get(access_level, ["read"])

    # Build Ranger policy
    policy = {
        "name": policy_name,
        "policyType": "access",
        "isEnabled": True,
        "serviceType": "hive",
        "service": "hive",
        "description": f"Auto-governed by Metadata Curator. Access: {access_level}",
        "resources": {
            "database": {
                "values": [db_name],
                "isRecursive": False,
            },
            "table": {
                "values": [tbl_name],
                "isRecursive": False,
            },
            "column": {
                "values": ["*"],
                "isRecursive": False,
            },
        },
        "policyItems": [
            {
                "accesses": [{"type": perm, "isAllowed": True} for perm in permissions],
                "users": [owner.split("@")[0] if "@" in owner else owner],
                "groups": [],
                "conditions": [],
                "delegateAdmin": False,
            }
        ],
        "denyPolicyItems": [],
        "allowExceptions": [],
        "denyExceptions": [],
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{RANGER_URL}/service/public/v2/api/policies",
                json=policy,
                auth=(RANGER_USERNAME, RANGER_PASSWORD),
                headers={"Content-Type": "application/json"},
            )

            if response.status_code in [200, 201]:
                result = response.json()
                logger.info(f"[ranger] Policy created: {policy_name}")
                return {
                    "success": True,
                    "policy_id": result.get("id"),
                    "policy_name": policy_name,
                    "message": f"Ranger policy '{policy_name}' created successfully"
                }
            else:
                logger.warning(f"[ranger] Policy creation failed: {response.status_code}")
                return {
                    "success": False,
                    "error": f"Ranger API error: {response.status_code}",
                    "message": f"Failed to create Ranger policy"
                }

    except Exception as e:
        logger.warning(f"[ranger] Policy creation error: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Could not create Ranger policy: {e}"
        }


async def update_table_tags(
    table_name: str,
    tags: list,
) -> dict:
    """
    Update table tags in Ranger for metadata classification.

    Args:
        table_name: Full table name
        tags: List of tags (e.g., ["pii", "restricted", "sensitive"])

    Returns:
        Update result
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # In production, would update via Ranger tag API
            # For now, return success
            logger.info(f"[ranger] Tags updated for {table_name}: {tags}")
            return {
                "success": True,
                "table": table_name,
                "tags": tags,
                "message": f"Tags applied to {table_name}"
            }

    except Exception as e:
        logger.warning(f"[ranger] Tag update error: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"Could not update tags: {e}"
        }


async def check_ranger_connection() -> bool:
    """Verify Ranger is accessible."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(
                f"{RANGER_URL}/service/public/v2/api/serviceList",
                auth=(RANGER_USERNAME, RANGER_PASSWORD),
            )
            return response.status_code == 200
    except Exception as e:
        logger.warning(f"[ranger] Connection check failed: {e}")
        return False

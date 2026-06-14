#!/usr/bin/env python3
"""Query REST Iceberg catalog to list all tables."""

import sys
import requests
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# REST catalog endpoint
CATALOG_URL = "http://cdp-utility.cdp.local:8443/gateway/cdp-datashare-access/iceberg-rest"
JWT_TOKEN = os.getenv("KNOX_JWT", "")

if not JWT_TOKEN:
    print("⚠ WARNING: KNOX_JWT not set. Run: python ./knoxshare.py --showjwt=True")
    print("Then export KNOX_JWT=<token>")
    exit(1)

headers = {
    "Authorization": f"Bearer {JWT_TOKEN}",
    "Content-Type": "application/json"
}

def list_namespaces():
    """List all namespaces in the catalog."""
    url = f"{CATALOG_URL}/v1/namespaces"
    try:
        resp = requests.get(url, headers=headers, verify=False)
        resp.raise_for_status()
        return resp.json().get("namespaces", [])
    except Exception as e:
        print(f"Error listing namespaces: {e}")
        return []

def list_tables_in_namespace(namespace):
    """List all tables in a namespace."""
    url = f"{CATALOG_URL}/v1/namespaces/{namespace}/tables"
    try:
        resp = requests.get(url, headers=headers, verify=False)
        resp.raise_for_status()
        return resp.json().get("identifiers", [])
    except Exception as e:
        print(f"Error listing tables in {namespace}: {e}")
        return []

def get_table_metadata(namespace, table_name):
    """Get table metadata."""
    url = f"{CATALOG_URL}/v1/namespaces/{namespace}/tables/{table_name}"
    try:
        resp = requests.get(url, headers=headers, verify=False)
        resp.raise_for_status()
        data = resp.json()
        return {
            "name": table_name,
            "schema": data.get("schema", {}),
            "metadata_location": data.get("metadata_location"),
            "row_count": data.get("metadata", {}).get("statistics", {}).get("total_records")
        }
    except Exception as e:
        print(f"Error getting metadata for {namespace}.{table_name}: {e}")
        return None

def main():
    print("=" * 80)
    print("Querying REST Iceberg Catalog")
    print("=" * 80 + "\n")

    namespaces = list_namespaces()
    print(f"Found {len(namespaces)} namespace(s):\n")

    for ns in namespaces:
        print(f"📦 Namespace: {ns}")
        tables = list_tables_in_namespace(ns)

        if not tables:
            print("  (no tables)\n")
            continue

        print(f"  Tables ({len(tables)}):")
        for table_id in tables:
            table_name = table_id.get("name") or table_id
            metadata = get_table_metadata(ns, table_name)

            if metadata:
                schema = metadata.get("schema", {})
                fields = schema.get("fields", [])
                print(f"    🧊 {table_name}")
                print(f"       Fields: {len(fields)}")
                if fields:
                    field_names = [f.get("name") for f in fields[:5]]
                    print(f"       {', '.join(field_names)}" + (" ..." if len(fields) > 5 else ""))
                if metadata.get("row_count"):
                    print(f"       Rows: {metadata['row_count']}")
            else:
                print(f"    🧊 {table_name}")
        print()

    print("=" * 80)

if __name__ == "__main__":
    import sys
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    main()

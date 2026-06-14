#!/usr/bin/env python3
"""
End-to-end test: User query with negation → correct filtered results.

This simulates the exact scenario from the bug report:
  Input: "find tables NOT in ozone which has geolocation"
  Old behavior: Returned ozone tables (wrong)
  New behavior: Returns non-ozone tables with geolocation (correct)
"""
from tools.intent_parser import intent_to_metadata_filters
from tools.intent_extractor import normalize_asset_metadata


def simulate_discovery_with_negation():
    """Simulate the full filter flow from a negated query."""
    print("\n=== E2E Test: 'find tables NOT in ozone which has geolocation' ===\n")

    # Simulate parsed intent from LLM
    # (In real flow, this comes from parse_intent_with_cache)
    structured_intent = {
        "asset_types": ["iceberg_table"],
        "storage": {"value": "ozone", "negate": True},  # ← LLM detects negation!
        "format": None,
        "required_fields": ["geolocation"],
        "pii_only": False,
        "time_filter": None,
    }

    # Convert to metadata filters
    metadata_filters = intent_to_metadata_filters(structured_intent)
    print(f"Metadata filters: {metadata_filters}")
    print(f"(Negation encoded as '!' prefix: {metadata_filters.get('storage')})")

    # Simulate discovered assets
    assets = [
        {
            "name": "orders",
            "asset_type": "iceberg_table",
            "metadata": {
                "location": "ofs://cdp-volume/warehouse/orders",
                "fields": [
                    {"name": "order_id", "type": "long"},
                    {"name": "geolocation", "type": "string"},
                ],
            },
        },
        {
            "name": "customers",
            "asset_type": "iceberg_table",
            "metadata": {
                "location": "s3a://data-lake/customers",
                "fields": [
                    {"name": "customer_id", "type": "long"},
                    {"name": "geolocation", "type": "string"},
                ],
            },
        },
        {
            "name": "products",
            "asset_type": "iceberg_table",
            "metadata": {
                "location": "hdfs:///warehouse/products",
                "fields": [
                    {"name": "product_id", "type": "long"},
                    {"name": "name", "type": "string"},
                    # Missing geolocation!
                ],
            },
        },
        {
            "name": "shipments",
            "asset_type": "iceberg_table",
            "metadata": {
                "location": "ofs://cdp-volume/logistics/shipments",
                "fields": [
                    {"name": "shipment_id", "type": "long"},
                    {"name": "geolocation", "type": "string"},
                ],
            },
        },
    ]

    print(f"\nScanning {len(assets)} discovered assets...\n")

    # Apply metadata filter (from parsed intent)
    metadata_matched = []
    metadata_rejected = []

    for asset in assets:
        metadata = asset.get("metadata", {})
        asset_type = asset.get("asset_type", "")

        # Normalize asset metadata
        normalized = normalize_asset_metadata(metadata, asset_type)

        # Check metadata filters with negation support
        matches = True
        for key, expected_val in metadata_filters.items():
            actual_val = normalized.get(key, "").lower()

            if expected_val.startswith("!"):
                negated_val = expected_val[1:]
                if actual_val == negated_val:  # If it IS the negated value, reject
                    matches = False
                    break
            else:
                if actual_val != expected_val.lower():
                    matches = False
                    break

        if matches:
            metadata_matched.append(asset)
            status = "✓ KEPT"
        else:
            metadata_rejected.append(asset)
            status = "✗ REJECTED"

        storage = normalized.get("storage", "unknown")
        print(
            f"  {asset['name']:15} | storage={storage:8} | metadata_filter='{expected_val}' → {status}"
        )

    print(f"\nMetadata filter results:")
    print(f"  Matched: {len(metadata_matched)}")
    print(f"  Rejected: {len(metadata_rejected)}")

    # Now apply required_fields filter
    print(f"\nApplying required_fields filter: {structured_intent.get('required_fields')}")

    final_results = []
    for asset in metadata_matched:
        fields = asset.get("metadata", {}).get("fields", [])
        field_names = {f.get("name", "").lower() for f in fields}
        required = {f.lower() for f in structured_intent.get("required_fields", [])}

        if required.issubset(field_names):
            final_results.append(asset)
            print(f"  {asset['name']:15} | has fields {required} → ✓ FINAL RESULT")
        else:
            print(f"  {asset['name']:15} | missing {required - field_names} → ✗ FILTERED OUT")

    print(f"\n{'=' * 70}")
    print(f"FINAL RESULTS ({len(final_results)} assets):")
    print(f"{'=' * 70}")
    for asset in final_results:
        storage = normalize_asset_metadata(asset.get("metadata", {}), asset.get("asset_type", "")).get(
            "storage", "unknown"
        )
        print(f"  • {asset['name']:15} | storage={storage}")

    print(f"\nAssertion checks:")
    # Only "customers" and "orders" have geolocation field
    # But "orders" is in ozone, so it's excluded by the negation filter
    # Only "customers" passes both filters
    assert len(final_results) == 1, f"Expected 1 result, got {len(final_results)}"
    print(f"  ✓ Returned {len(final_results)} result(s) (expected 1)")

    result_names = {a["name"] for a in final_results}
    assert result_names == {"customers"}, f"Expected just customers, got {result_names}"
    print(f"  ✓ Results are {result_names}")

    ozone_in_results = any(
        normalize_asset_metadata(a.get("metadata", {}), a.get("asset_type", "")).get("storage")
        == "ozone"
        for a in final_results
    )
    assert not ozone_in_results, "Should NOT have any ozone tables in results!"
    print(f"  ✓ No ozone-stored tables in results (negation worked!)")

    print(f"\n{'=' * 70}")
    print(f"✓ E2E TEST PASSED: Negation correctly excludes ozone storage")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    simulate_discovery_with_negation()

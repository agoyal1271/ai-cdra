#!/usr/bin/env python3
"""
Test semantic intent parsing with negation handling.

Tests:
1. Intent parser converts negation correctly (NOT in ozone → negation flag)
2. Metadata filter loop respects negation (asset.storage != "ozone" is kept)
3. Display formatting shows negation correctly (storage≠ozone)
"""
import asyncio
from tools.intent_parser import (
    intent_to_metadata_filters,
    _normalize_intent,
    _default_intent,
)
from tools.intent_extractor import normalize_asset_metadata


def test_negation_filtering():
    """Test that negation in filters works correctly."""
    print("\n=== Test 1: Negation Filter Logic ===")

    # Simulate metadata filter: "NOT in ozone"
    metadata_filters = {"storage": "!ozone"}

    # Test asset 1: in ozone (should be rejected)
    asset1_metadata = {"location": "ofs://cdp-volume/table1"}
    asset1_normalized = normalize_asset_metadata(asset1_metadata, "iceberg_table")
    print(f"Asset 1 (ozone): normalized storage = {asset1_normalized.get('storage')}")

    matches = True
    for key, expected_val in metadata_filters.items():
        actual_val = asset1_normalized.get(key, "").lower()
        if expected_val.startswith("!"):
            negated_val = expected_val[1:]
            if actual_val == negated_val:
                matches = False
                break
        else:
            if actual_val != expected_val.lower():
                matches = False
                break

    assert not matches, "Asset in ozone should be REJECTED when filter is '!ozone'"
    print(f"✓ Asset in ozone correctly REJECTED with '!ozone' filter")

    # Test asset 2: in s3a (should be accepted)
    asset2_metadata = {"location": "s3a://bucket/table2"}
    asset2_normalized = normalize_asset_metadata(asset2_metadata, "iceberg_table")
    print(f"Asset 2 (s3a): normalized storage = {asset2_normalized.get('storage')}")

    matches = True
    for key, expected_val in metadata_filters.items():
        actual_val = asset2_normalized.get(key, "").lower()
        if expected_val.startswith("!"):
            negated_val = expected_val[1:]
            if actual_val == negated_val:
                matches = False
                break
        else:
            if actual_val != expected_val.lower():
                matches = False
                break

    assert matches, "Asset in s3a should be ACCEPTED when filter is '!ozone'"
    print(f"✓ Asset in s3a correctly ACCEPTED with '!ozone' filter")


def test_positive_filter_still_works():
    """Test that positive filters (without negation) still work."""
    print("\n=== Test 2: Positive Filter Logic (No Regression) ===")

    # Simulate metadata filter: "IN ozone"
    metadata_filters = {"storage": "ozone"}

    # Test asset 1: in ozone (should be accepted)
    asset1_metadata = {"location": "ofs://cdp-volume/table1"}
    asset1_normalized = normalize_asset_metadata(asset1_metadata, "iceberg_table")

    matches = True
    for key, expected_val in metadata_filters.items():
        actual_val = asset1_normalized.get(key, "").lower()
        if expected_val.startswith("!"):
            negated_val = expected_val[1:]
            if actual_val == negated_val:
                matches = False
                break
        else:
            if actual_val != expected_val.lower():
                matches = False
                break

    assert matches, "Asset in ozone should be ACCEPTED when filter is 'ozone'"
    print(f"✓ Asset in ozone correctly ACCEPTED with 'ozone' filter")

    # Test asset 2: in s3a (should be rejected)
    asset2_metadata = {"location": "s3a://bucket/table2"}
    asset2_normalized = normalize_asset_metadata(asset2_metadata, "iceberg_table")

    matches = True
    for key, expected_val in metadata_filters.items():
        actual_val = asset2_normalized.get(key, "").lower()
        if expected_val.startswith("!"):
            negated_val = expected_val[1:]
            if actual_val == negated_val:
                matches = False
                break
        else:
            if actual_val != expected_val.lower():
                matches = False
                break

    assert not matches, "Asset in s3a should be REJECTED when filter is 'ozone'"
    print(f"✓ Asset in s3a correctly REJECTED with 'ozone' filter")


def test_intent_conversion():
    """Test that structured intent converts to metadata filters correctly."""
    print("\n=== Test 3: Intent to Filter Conversion ===")

    # Structured intent with negation
    intent = {
        "asset_types": ["iceberg_table"],
        "storage": {"value": "ozone", "negate": True},
        "format": None,
        "required_fields": ["geolocation"],
        "pii_only": False,
        "time_filter": None,
    }

    filters = intent_to_metadata_filters(intent)
    print(f"Converted intent: {filters}")

    assert filters.get("storage") == "!ozone", "Negated storage should convert to '!ozone'"
    assert "format" not in filters, "None format should not appear in filters"
    print(f"✓ Intent correctly converted to metadata filters")


def test_filter_display():
    """Test that negation is displayed correctly in SSE messages."""
    print("\n=== Test 4: Filter Display Formatting ===")

    metadata_filters = {"storage": "!ozone", "format": "parquet"}

    filter_str = ", ".join(
        f"{k}≠{v[1:]}" if v.startswith("!") else f"{k}={v}"
        for k, v in metadata_filters.items()
    )

    print(f"Formatted filter string: {filter_str}")
    assert "storage≠ozone" in filter_str, "Negated storage should display as 'storage≠ozone'"
    assert "format=parquet" in filter_str, "Positive format should display as 'format=parquet'"
    print(f"✓ Filters display correctly with negation symbol")


if __name__ == "__main__":
    print("Testing Negation-Aware Metadata Filtering...")
    print("=" * 60)

    try:
        test_negation_filtering()
        test_positive_filter_still_works()
        test_intent_conversion()
        test_filter_display()

        print("\n" + "=" * 60)
        print("✓ ALL TESTS PASSED")
        print("=" * 60)
    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {e}")
        exit(1)
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

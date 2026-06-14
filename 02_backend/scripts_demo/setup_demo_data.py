#!/usr/bin/env python3
"""
Unified entry point for demo data setup.

Usage:
  python setup_demo_data.py --help          # Show all available setups
  python setup_demo_data.py --kafka         # Register T-Life Kafka topics
  python setup_demo_data.py --iceberg       # Create test Iceberg tables
  python setup_demo_data.py --all           # Run all setup steps
"""
import argparse
import sys
from pathlib import Path

# Make it easier to import sibling modules
demo_dir = Path(__file__).parent
sys.path.insert(0, str(demo_dir.parent))


def setup_kafka():
    """Register T-Life Kafka topics and schema registry entries."""
    print("\n📨 Setting up Kafka topics...")
    try:
        from register_tlife_topics import main as register_topics
        register_topics()
        print("✓ Kafka topics registered")
    except Exception as e:
        print(f"✗ Kafka setup failed: {e}")
        return False
    return True


def setup_kafka_schema():
    """Register T-Life schemas in Schema Registry."""
    print("\n📋 Setting up Schema Registry...")
    try:
        from register_tlife_sr import main as register_sr
        register_sr()
        print("✓ Schemas registered")
    except Exception as e:
        print(f"✗ Schema Registry setup failed: {e}")
        return False
    return True


def setup_iceberg():
    """Create test Iceberg tables."""
    print("\n🧊 Setting up Iceberg tables...")
    try:
        from create_test_iceberg_tables import main as create_tables
        create_tables()
        print("✓ Iceberg tables created")
    except Exception as e:
        print(f"✗ Iceberg setup failed: {e}")
        return False
    return True


def setup_all():
    """Run all setup steps."""
    print("\n🚀 Starting complete demo data setup...")
    results = {
        "Kafka topics": setup_kafka(),
        "Schema Registry": setup_kafka_schema(),
        "Iceberg tables": setup_iceberg(),
    }

    print("\n" + "=" * 50)
    print("Setup Results:")
    for name, success in results.items():
        status = "✓" if success else "✗"
        print(f"  {status} {name}")

    all_success = all(results.values())
    print("=" * 50)
    return all_success


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Setup demo data for Cloudera AI Agents",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python setup_demo_data.py --kafka         # Setup only Kafka
  python setup_demo_data.py --all           # Setup everything
        """,
    )
    parser.add_argument("--kafka", action="store_true", help="Register Kafka topics")
    parser.add_argument("--schema", action="store_true", help="Register Schema Registry entries")
    parser.add_argument("--iceberg", action="store_true", help="Create Iceberg tables")
    parser.add_argument("--all", action="store_true", help="Run all setup steps (default)")

    args = parser.parse_args()

    # Default to --all if no args
    if not any([args.kafka, args.schema, args.iceberg, args.all]):
        args.all = True

    success = True
    if args.kafka:
        success &= setup_kafka()
    if args.schema:
        success &= setup_kafka_schema()
    if args.iceberg:
        success &= setup_iceberg()
    if args.all:
        success = setup_all()

    sys.exit(0 if success else 1)

#!/usr/bin/env python3
"""
Create 10 test Iceberg tables with dummy data, including some bad records for data quality testing.
Tables created: customers, orders, payments, products, reviews, inventory, shipments, returns, invoices, activity_logs
"""

import sys
import os
from datetime import datetime, timedelta
from random import randint, choice, random
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType
)
import pyarrow as pa


def get_or_create_table(catalog, namespace, table_name, schema):
    """Drop existing table and create a new one."""
    full_name = f"{namespace}.{table_name}"
    try:
        catalog.drop_table(full_name)
        print(f"  Dropped existing {full_name}")
    except:
        pass
    return catalog.create_table(full_name, schema=schema)


def get_catalog():
    """Load Iceberg catalog from config."""
    from config import ICEBERG_CATALOG_TYPE, ICEBERG_CATALOG_URI, ICEBERG_WAREHOUSE

    props = {"type": ICEBERG_CATALOG_TYPE}
    if ICEBERG_CATALOG_URI:
        props["uri"] = ICEBERG_CATALOG_URI
    if ICEBERG_WAREHOUSE:
        props["warehouse"] = ICEBERG_WAREHOUSE

    # Add Knox JWT for REST catalog auth
    try:
        from agents.source_scout.sidecar import get_valid_knox_token
        knox_jwt = get_valid_knox_token()
    except ImportError:
        knox_jwt = os.getenv("KNOX_JWT", "")

    if knox_jwt and ICEBERG_CATALOG_TYPE == "rest":
        props["token"] = knox_jwt

    print(f"Loading catalog: type={ICEBERG_CATALOG_TYPE}, uri={ICEBERG_CATALOG_URI}, warehouse={ICEBERG_WAREHOUSE}")
    if ICEBERG_CATALOG_TYPE == "rest":
        print(f"Using REST catalog with token: {bool(props.get('token'))}")
    return load_catalog("default", **props)


def create_customers_table(catalog):
    """Create customers table with some bad records."""
    namespace = "demo"
    table_name = "customers"

    try:
        catalog.create_namespace(namespace, exist_ok=True)
    except:
        pass

    schema = Schema(
        NestedField(1, "customer_id", LongType(), required=False),
        NestedField(2, "email", StringType(), required=False),
        NestedField(3, "first_name", StringType(), required=False),
        NestedField(4, "last_name", StringType(), required=False),
        NestedField(5, "phone", StringType(), required=False),
        NestedField(6, "created_at", TimestampType(), required=False),
        NestedField(7, "active", BooleanType(), required=False),
    )

    table = get_or_create_table(catalog, namespace, table_name, schema)

    # Generate data with some bad records
    data = []
    for i in range(1, 9):
        if i == 3:  # Bad record: missing email
            data.append({
                "customer_id": i,
                "email": None,  # Missing required field
                "first_name": f"Customer{i}",
                "last_name": f"User{i}",
                "phone": f"555-000{i}",
                "created_at": datetime.now() - timedelta(days=30+i),
                "active": True,
            })
        elif i == 7:  # Bad record: invalid phone
            data.append({
                "customer_id": i,
                "email": f"customer{i}@example.com",
                "first_name": f"Customer{i}",
                "last_name": f"User{i}",
                "phone": "INVALID_PHONE",  # Bad data
                "created_at": datetime.now() - timedelta(days=30+i),
                "active": True,
            })
        else:
            data.append({
                "customer_id": i,
                "email": f"customer{i}@example.com",
                "first_name": f"Customer{i}",
                "last_name": f"User{i}",
                "phone": f"555-000{i}",
                "created_at": datetime.now() - timedelta(days=30+i),
                "active": choice([True, False]),
            })

    table.append(pa.Table.from_pylist(data))
    print(f"✓ Created {namespace}.{table_name} with {len(data)} records")


def create_orders_table(catalog):
    """Create orders table with some bad records."""
    namespace = "demo"
    table_name = "orders"

    try:
        catalog.create_namespace(namespace, exist_ok=True)
    except:
        pass

    schema = Schema(
        NestedField(1, "order_id", LongType(), required=False),
        NestedField(2, "customer_id", IntegerType(), required=False),
        NestedField(3, "order_date", TimestampType(), required=False),
        NestedField(4, "total_amount", DoubleType(), required=False),
        NestedField(5, "status", StringType(), required=False),
        NestedField(6, "shipping_address", StringType(), required=False),
    )

    table = get_or_create_table(catalog, namespace, table_name, schema)

    data = []
    for i in range(1, 11):
        if i == 5:  # Bad record: negative amount
            data.append({
                "order_id": i,
                "customer_id": (i % 8) + 1,
                "order_date": datetime.now() - timedelta(days=10),
                "total_amount": -99.99,  # Bad: negative
                "status": "completed",
                "shipping_address": f"Address {i}",
            })
        elif i == 9:  # Bad record: NULL amount
            data.append({
                "order_id": i,
                "customer_id": (i % 8) + 1,
                "order_date": datetime.now() - timedelta(days=10),
                "total_amount": None,  # Missing amount
                "status": "pending",
                "shipping_address": f"Address {i}",
            })
        else:
            data.append({
                "order_id": i,
                "customer_id": (i % 8) + 1,
                "order_date": datetime.now() - timedelta(days=randint(0, 90)),
                "total_amount": round(random() * 1000, 2),
                "status": choice(["pending", "completed", "shipped", "cancelled"]),
                "shipping_address": f"Address {i}",
            })

    table.append(pa.Table.from_pylist(data))
    print(f"✓ Created {namespace}.{table_name} with {len(data)} records")


def create_payments_table(catalog):
    """Create payments table with bad records."""
    namespace = "demo"
    table_name = "payments"

    try:
        catalog.create_namespace(namespace, exist_ok=True)
    except:
        pass

    schema = Schema(
        NestedField(1, "payment_id", LongType(), required=False),
        NestedField(2, "order_id", LongType(), required=False),
        NestedField(3, "amount", DoubleType(), required=False),
        NestedField(4, "payment_method", StringType(), required=False),
        NestedField(5, "payment_date", TimestampType(), required=False),
        NestedField(6, "status", StringType(), required=False),
    )

    table = get_or_create_table(catalog, namespace, table_name, schema)

    data = []
    for i in range(1, 8):
        if i == 4:  # Bad: huge outlier
            data.append({
                "payment_id": i,
                "order_id": i,
                "amount": 999999.99,  # Outlier
                "payment_method": "credit_card",
                "payment_date": datetime.now() - timedelta(days=5),
                "status": "completed",
            })
        elif i == 6:  # Bad: missing status
            data.append({
                "payment_id": i,
                "order_id": i,
                "amount": round(random() * 500, 2),
                "payment_method": "paypal",
                "payment_date": datetime.now() - timedelta(days=5),
                "status": None,
            })
        else:
            data.append({
                "payment_id": i,
                "order_id": i,
                "amount": round(random() * 500, 2),
                "payment_method": choice(["credit_card", "paypal", "bank_transfer"]),
                "payment_date": datetime.now() - timedelta(days=5),
                "status": choice(["completed", "pending", "failed"]),
            })

    table.append(pa.Table.from_pylist(data))
    print(f"✓ Created {namespace}.{table_name} with {len(data)} records")


def create_products_table(catalog):
    """Create products table."""
    namespace = "demo"
    table_name = "products"

    try:
        catalog.create_namespace(namespace, exist_ok=True)
    except:
        pass

    schema = Schema(
        NestedField(1, "product_id", LongType(), required=False),
        NestedField(2, "product_name", StringType(), required=False),
        NestedField(3, "category", StringType(), required=False),
        NestedField(4, "price", DoubleType(), required=False),
        NestedField(5, "stock_quantity", IntegerType(), required=False),
        NestedField(6, "created_at", TimestampType(), required=False),
    )

    table = get_or_create_table(catalog, namespace, table_name, schema)

    data = []
    for i in range(1, 9):
        if i == 3:  # Bad: negative stock
            data.append({
                "product_id": i,
                "product_name": f"Product {i}",
                "category": choice(["Electronics", "Clothing", "Books"]),
                "price": round(random() * 100, 2),
                "stock_quantity": -5,  # Bad
                "created_at": datetime.now() - timedelta(days=60),
            })
        else:
            data.append({
                "product_id": i,
                "product_name": f"Product {i}",
                "category": choice(["Electronics", "Clothing", "Books"]),
                "price": round(random() * 100, 2),
                "stock_quantity": randint(0, 100),
                "created_at": datetime.now() - timedelta(days=60),
            })

    table.append(pa.Table.from_pylist(data))
    print(f"✓ Created {namespace}.{table_name} with {len(data)} records")


def create_reviews_table(catalog):
    """Create reviews table."""
    namespace = "demo"
    table_name = "reviews"

    try:
        catalog.create_namespace(namespace, exist_ok=True)
    except:
        pass

    schema = Schema(
        NestedField(1, "review_id", LongType(), required=False),
        NestedField(2, "product_id", LongType(), required=False),
        NestedField(3, "customer_id", IntegerType(), required=False),
        NestedField(4, "rating", IntegerType(), required=False),
        NestedField(5, "review_text", StringType(), required=False),
        NestedField(6, "created_at", TimestampType(), required=False),
    )

    table = get_or_create_table(catalog, namespace, table_name, schema)

    data = []
    for i in range(1, 9):
        if i == 2:  # Bad: invalid rating
            data.append({
                "review_id": i,
                "product_id": (i % 8) + 1,
                "customer_id": (i % 8) + 1,
                "rating": 10,  # Out of range (should be 1-5)
                "review_text": "Great product!",
                "created_at": datetime.now() - timedelta(days=7),
            })
        else:
            data.append({
                "review_id": i,
                "product_id": (i % 8) + 1,
                "customer_id": (i % 8) + 1,
                "rating": randint(1, 5),
                "review_text": f"Review {i}",
                "created_at": datetime.now() - timedelta(days=7),
            })

    table.append(pa.Table.from_pylist(data))
    print(f"✓ Created {namespace}.{table_name} with {len(data)} records")


def create_inventory_table(catalog):
    """Create inventory table."""
    namespace = "demo"
    table_name = "inventory"

    try:
        catalog.create_namespace(namespace, exist_ok=True)
    except:
        pass

    schema = Schema(
        NestedField(1, "inventory_id", LongType(), required=False),
        NestedField(2, "product_id", LongType(), required=False),
        NestedField(3, "warehouse_id", StringType(), required=False),
        NestedField(4, "quantity", IntegerType(), required=False),
        NestedField(5, "last_updated", TimestampType(), required=False),
    )

    table = get_or_create_table(catalog, namespace, table_name, schema)

    data = []
    for i in range(1, 8):
        data.append({
            "inventory_id": i,
            "product_id": (i % 8) + 1,
            "warehouse_id": f"WH-{choice(['A', 'B', 'C'])}",
            "quantity": randint(0, 500),
            "last_updated": datetime.now() - timedelta(days=randint(0, 30)),
        })

    table.append(pa.Table.from_pylist(data))
    print(f"✓ Created {namespace}.{table_name} with {len(data)} records")


def create_shipments_table(catalog):
    """Create shipments table with bad records."""
    namespace = "demo"
    table_name = "shipments"

    try:
        catalog.create_namespace(namespace, exist_ok=True)
    except:
        pass

    schema = Schema(
        NestedField(1, "shipment_id", LongType(), required=False),
        NestedField(2, "order_id", LongType(), required=False),
        NestedField(3, "shipped_date", TimestampType(), required=False),
        NestedField(4, "delivery_date", TimestampType(), required=False),
        NestedField(5, "carrier", StringType(), required=False),
        NestedField(6, "tracking_number", StringType(), required=False),
    )

    table = get_or_create_table(catalog, namespace, table_name, schema)

    data = []
    for i in range(1, 9):
        if i == 4:  # Bad: delivery before shipped
            data.append({
                "shipment_id": i,
                "order_id": i,
                "shipped_date": datetime.now() - timedelta(days=5),
                "delivery_date": datetime.now() - timedelta(days=10),  # Before shipped!
                "carrier": "FedEx",
                "tracking_number": f"FDX{i}",
            })
        else:
            shipped = datetime.now() - timedelta(days=randint(1, 20))
            data.append({
                "shipment_id": i,
                "order_id": i,
                "shipped_date": shipped,
                "delivery_date": shipped + timedelta(days=randint(2, 10)),
                "carrier": choice(["FedEx", "UPS", "DHL"]),
                "tracking_number": f"TRK{i}",
            })

    table.append(pa.Table.from_pylist(data))
    print(f"✓ Created {namespace}.{table_name} with {len(data)} records")


def create_returns_table(catalog):
    """Create returns table."""
    namespace = "demo"
    table_name = "returns"

    try:
        catalog.create_namespace(namespace, exist_ok=True)
    except:
        pass

    schema = Schema(
        NestedField(1, "return_id", LongType(), required=False),
        NestedField(2, "order_id", LongType(), required=False),
        NestedField(3, "product_id", LongType(), required=False),
        NestedField(4, "return_date", TimestampType(), required=False),
        NestedField(5, "reason", StringType(), required=False),
        NestedField(6, "refund_amount", DoubleType(), required=False),
    )

    table = get_or_create_table(catalog, namespace, table_name, schema)

    data = []
    for i in range(1, 7):
        data.append({
            "return_id": i,
            "order_id": randint(1, 10),
            "product_id": randint(1, 8),
            "return_date": datetime.now() - timedelta(days=randint(5, 30)),
            "reason": choice(["Defective", "Wrong item", "Changed mind", "Better price elsewhere"]),
            "refund_amount": round(random() * 300, 2),
        })

    table.append(pa.Table.from_pylist(data))
    print(f"✓ Created {namespace}.{table_name} with {len(data)} records")


def create_invoices_table(catalog):
    """Create invoices table with bad records."""
    namespace = "demo"
    table_name = "invoices"

    try:
        catalog.create_namespace(namespace, exist_ok=True)
    except:
        pass

    schema = Schema(
        NestedField(1, "invoice_id", LongType(), required=False),
        NestedField(2, "order_id", LongType(), required=False),
        NestedField(3, "invoice_date", TimestampType(), required=False),
        NestedField(4, "due_date", TimestampType(), required=False),
        NestedField(5, "total_amount", DoubleType(), required=False),
        NestedField(6, "status", StringType(), required=False),
    )

    table = get_or_create_table(catalog, namespace, table_name, schema)

    data = []
    for i in range(1, 8):
        if i == 3:  # Bad: due date before invoice date
            invoice_date = datetime.now() - timedelta(days=10)
            data.append({
                "invoice_id": i,
                "order_id": i,
                "invoice_date": invoice_date,
                "due_date": invoice_date - timedelta(days=5),  # Before invoice date!
                "total_amount": round(random() * 1000, 2),
                "status": "pending",
            })
        else:
            invoice_date = datetime.now() - timedelta(days=10)
            data.append({
                "invoice_id": i,
                "order_id": i,
                "invoice_date": invoice_date,
                "due_date": invoice_date + timedelta(days=30),
                "total_amount": round(random() * 1000, 2),
                "status": choice(["pending", "paid", "overdue"]),
            })

    table.append(pa.Table.from_pylist(data))
    print(f"✓ Created {namespace}.{table_name} with {len(data)} records")


def create_activity_logs_table(catalog):
    """Create activity logs table."""
    namespace = "demo"
    table_name = "activity_logs"

    try:
        catalog.create_namespace(namespace, exist_ok=True)
    except:
        pass

    schema = Schema(
        NestedField(1, "log_id", LongType(), required=True),
        NestedField(2, "user_id", IntegerType(), required=False),
        NestedField(3, "event_type", StringType(), required=False),
        NestedField(4, "event_timestamp", TimestampType(), required=False),
        NestedField(5, "ip_address", StringType(), required=False),
        NestedField(6, "session_id", StringType(), required=False),
    )

    table = get_or_create_table(catalog, namespace, table_name, schema)

    data = []
    for i in range(1, 11):
        data.append({
            "log_id": i,
            "user_id": randint(1, 8),
            "event_type": choice(["login", "logout", "view_product", "add_to_cart", "checkout"]),
            "event_timestamp": datetime.now() - timedelta(minutes=randint(0, 1440)),
            "ip_address": f"192.168.1.{randint(1, 255)}",
            "session_id": f"session-{i}",
        })

    table.append(pa.Table.from_pylist(data))
    print(f"✓ Created {namespace}.{table_name} with {len(data)} records")


def main():
    """Create all test tables."""
    print("=" * 60)
    print("Creating test Iceberg tables with dummy data...")
    print("=" * 60)

    try:
        catalog = get_catalog()
        print("✓ Catalog loaded successfully\n")
    except Exception as e:
        print(f"✗ Failed to load catalog: {e}")
        sys.exit(1)

    try:
        create_customers_table(catalog)
        create_orders_table(catalog)
        create_payments_table(catalog)
        create_products_table(catalog)
        create_reviews_table(catalog)
        create_inventory_table(catalog)
        create_shipments_table(catalog)
        create_returns_table(catalog)
        create_invoices_table(catalog)
        create_activity_logs_table(catalog)

        print("\n" + "=" * 60)
        print("✓ All tables created successfully!")
        print("=" * 60)
        print("\nData quality issues included:")
        print("  • customers: missing email, invalid phone")
        print("  • orders: negative amount, NULL amount")
        print("  • payments: huge outlier, missing status")
        print("  • products: negative stock")
        print("  • reviews: rating out of range (10)")
        print("  • shipments: delivery before shipped")
        print("  • invoices: due date before invoice date")

    except Exception as e:
        print(f"\n✗ Error creating tables: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

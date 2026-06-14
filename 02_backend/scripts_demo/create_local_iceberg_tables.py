#!/usr/bin/env python3
"""
Create 10 test Iceberg tables locally with dummy data.
Uses Hadoop warehouse instead of REST catalog.
"""

import sys
import os
from datetime import datetime, timedelta
from random import randint, choice, random
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DoubleType, TimestampType, BooleanType, LongType
import pyarrow as pa


def get_catalog():
    """Load local Hadoop Iceberg catalog."""
    warehouse = "/Users/archit/iceberg-data"
    os.makedirs(warehouse, exist_ok=True)

    props = {
        "type": "hadoop",
        "warehouse": warehouse
    }
    print(f"Using local Hadoop warehouse: {warehouse}")
    return load_catalog("default", **props)


def create_table(catalog, namespace, table_name, schema, data):
    """Create or replace table with data."""
    full_name = f"{namespace}.{table_name}"
    try:
        catalog.create_namespace(namespace, exist_ok=True)
    except:
        pass

    try:
        catalog.drop_table(full_name, purge=True)
        print(f"  Dropped existing {full_name}")
    except:
        pass

    table = catalog.create_table(full_name, schema=schema)
    table.append(pa.Table.from_pylist(data))
    print(f"✓ {full_name} ({len(data)} records)")
    return table


def main():
    print("=" * 70)
    print("Creating test Iceberg tables with dummy data (local Hadoop warehouse)")
    print("=" * 70 + "\n")

    catalog = get_catalog()
    print("✓ Catalog loaded\n")

    # Customers
    schema = Schema(
        NestedField(1, "customer_id", LongType(), required=False),
        NestedField(2, "email", StringType(), required=False),
        NestedField(3, "first_name", StringType(), required=False),
        NestedField(4, "last_name", StringType(), required=False),
        NestedField(5, "phone", StringType(), required=False),
        NestedField(6, "created_at", TimestampType(), required=False),
        NestedField(7, "active", BooleanType(), required=False),
    )
    data = []
    for i in range(1, 9):
        if i == 3:
            data.append({"customer_id": i, "email": None, "first_name": f"Customer{i}", "last_name": f"User{i}", "phone": f"555-000{i}", "created_at": datetime.now() - timedelta(days=30+i), "active": True})
        elif i == 7:
            data.append({"customer_id": i, "email": f"customer{i}@example.com", "first_name": f"Customer{i}", "last_name": f"User{i}", "phone": "INVALID_PHONE", "created_at": datetime.now() - timedelta(days=30+i), "active": True})
        else:
            data.append({"customer_id": i, "email": f"customer{i}@example.com", "first_name": f"Customer{i}", "last_name": f"User{i}", "phone": f"555-000{i}", "created_at": datetime.now() - timedelta(days=30+i), "active": choice([True, False])})
    create_table(catalog, "demo", "customers", schema, data)

    # Orders
    schema = Schema(
        NestedField(1, "order_id", LongType(), required=False),
        NestedField(2, "customer_id", LongType(), required=False),
        NestedField(3, "order_date", TimestampType(), required=False),
        NestedField(4, "total_amount", DoubleType(), required=False),
        NestedField(5, "status", StringType(), required=False),
        NestedField(6, "shipping_address", StringType(), required=False),
    )
    data = []
    for i in range(1, 11):
        if i == 5:
            data.append({"order_id": i, "customer_id": (i % 8) + 1, "order_date": datetime.now() - timedelta(days=10), "total_amount": -99.99, "status": "completed", "shipping_address": f"Address {i}"})
        elif i == 9:
            data.append({"order_id": i, "customer_id": (i % 8) + 1, "order_date": datetime.now() - timedelta(days=10), "total_amount": None, "status": "pending", "shipping_address": f"Address {i}"})
        else:
            data.append({"order_id": i, "customer_id": (i % 8) + 1, "order_date": datetime.now() - timedelta(days=randint(0, 90)), "total_amount": round(random() * 1000, 2), "status": choice(["pending", "completed", "shipped", "cancelled"]), "shipping_address": f"Address {i}"})
    create_table(catalog, "demo", "orders", schema, data)

    # Payments
    schema = Schema(
        NestedField(1, "payment_id", LongType(), required=False),
        NestedField(2, "order_id", LongType(), required=False),
        NestedField(3, "amount", DoubleType(), required=False),
        NestedField(4, "payment_method", StringType(), required=False),
        NestedField(5, "payment_date", TimestampType(), required=False),
        NestedField(6, "status", StringType(), required=False),
    )
    data = []
    for i in range(1, 8):
        if i == 4:
            data.append({"payment_id": i, "order_id": i, "amount": 999999.99, "payment_method": "credit_card", "payment_date": datetime.now() - timedelta(days=5), "status": "completed"})
        elif i == 6:
            data.append({"payment_id": i, "order_id": i, "amount": round(random() * 500, 2), "payment_method": "paypal", "payment_date": datetime.now() - timedelta(days=5), "status": None})
        else:
            data.append({"payment_id": i, "order_id": i, "amount": round(random() * 500, 2), "payment_method": choice(["credit_card", "paypal", "bank_transfer"]), "payment_date": datetime.now() - timedelta(days=5), "status": choice(["completed", "pending", "failed"])})
    create_table(catalog, "demo", "payments", schema, data)

    # Products
    schema = Schema(
        NestedField(1, "product_id", LongType(), required=False),
        NestedField(2, "product_name", StringType(), required=False),
        NestedField(3, "category", StringType(), required=False),
        NestedField(4, "price", DoubleType(), required=False),
        NestedField(5, "stock_quantity", LongType(), required=False),
        NestedField(6, "created_at", TimestampType(), required=False),
    )
    data = []
    for i in range(1, 9):
        if i == 3:
            data.append({"product_id": i, "product_name": f"Product {i}", "category": choice(["Electronics", "Clothing", "Books"]), "price": round(random() * 100, 2), "stock_quantity": -5, "created_at": datetime.now() - timedelta(days=60)})
        else:
            data.append({"product_id": i, "product_name": f"Product {i}", "category": choice(["Electronics", "Clothing", "Books"]), "price": round(random() * 100, 2), "stock_quantity": randint(0, 100), "created_at": datetime.now() - timedelta(days=60)})
    create_table(catalog, "demo", "products", schema, data)

    # Reviews
    schema = Schema(
        NestedField(1, "review_id", LongType(), required=False),
        NestedField(2, "product_id", LongType(), required=False),
        NestedField(3, "customer_id", LongType(), required=False),
        NestedField(4, "rating", LongType(), required=False),
        NestedField(5, "review_text", StringType(), required=False),
        NestedField(6, "created_at", TimestampType(), required=False),
    )
    data = []
    for i in range(1, 9):
        if i == 2:
            data.append({"review_id": i, "product_id": (i % 8) + 1, "customer_id": (i % 8) + 1, "rating": 10, "review_text": "Great product!", "created_at": datetime.now() - timedelta(days=7)})
        else:
            data.append({"review_id": i, "product_id": (i % 8) + 1, "customer_id": (i % 8) + 1, "rating": randint(1, 5), "review_text": f"Review {i}", "created_at": datetime.now() - timedelta(days=7)})
    create_table(catalog, "demo", "reviews", schema, data)

    # Inventory
    schema = Schema(
        NestedField(1, "inventory_id", LongType(), required=False),
        NestedField(2, "product_id", LongType(), required=False),
        NestedField(3, "warehouse_id", StringType(), required=False),
        NestedField(4, "quantity", LongType(), required=False),
        NestedField(5, "last_updated", TimestampType(), required=False),
    )
    data = []
    for i in range(1, 8):
        data.append({"inventory_id": i, "product_id": (i % 8) + 1, "warehouse_id": f"WH-{choice(['A', 'B', 'C'])}", "quantity": randint(0, 500), "last_updated": datetime.now() - timedelta(days=randint(0, 30))})
    create_table(catalog, "demo", "inventory", schema, data)

    # Shipments
    schema = Schema(
        NestedField(1, "shipment_id", LongType(), required=False),
        NestedField(2, "order_id", LongType(), required=False),
        NestedField(3, "shipped_date", TimestampType(), required=False),
        NestedField(4, "delivery_date", TimestampType(), required=False),
        NestedField(5, "carrier", StringType(), required=False),
        NestedField(6, "tracking_number", StringType(), required=False),
    )
    data = []
    for i in range(1, 9):
        if i == 4:
            data.append({"shipment_id": i, "order_id": i, "shipped_date": datetime.now() - timedelta(days=5), "delivery_date": datetime.now() - timedelta(days=10), "carrier": "FedEx", "tracking_number": f"FDX{i}"})
        else:
            shipped = datetime.now() - timedelta(days=randint(1, 20))
            data.append({"shipment_id": i, "order_id": i, "shipped_date": shipped, "delivery_date": shipped + timedelta(days=randint(2, 10)), "carrier": choice(["FedEx", "UPS", "DHL"]), "tracking_number": f"TRK{i}"})
    create_table(catalog, "demo", "shipments", schema, data)

    # Returns
    schema = Schema(
        NestedField(1, "return_id", LongType(), required=False),
        NestedField(2, "order_id", LongType(), required=False),
        NestedField(3, "product_id", LongType(), required=False),
        NestedField(4, "return_date", TimestampType(), required=False),
        NestedField(5, "reason", StringType(), required=False),
        NestedField(6, "refund_amount", DoubleType(), required=False),
    )
    data = []
    for i in range(1, 7):
        data.append({"return_id": i, "order_id": randint(1, 10), "product_id": randint(1, 8), "return_date": datetime.now() - timedelta(days=randint(5, 30)), "reason": choice(["Defective", "Wrong item", "Changed mind", "Better price elsewhere"]), "refund_amount": round(random() * 300, 2)})
    create_table(catalog, "demo", "returns", schema, data)

    # Invoices
    schema = Schema(
        NestedField(1, "invoice_id", LongType(), required=False),
        NestedField(2, "order_id", LongType(), required=False),
        NestedField(3, "invoice_date", TimestampType(), required=False),
        NestedField(4, "due_date", TimestampType(), required=False),
        NestedField(5, "total_amount", DoubleType(), required=False),
        NestedField(6, "status", StringType(), required=False),
    )
    data = []
    for i in range(1, 8):
        if i == 3:
            invoice_date = datetime.now() - timedelta(days=10)
            data.append({"invoice_id": i, "order_id": i, "invoice_date": invoice_date, "due_date": invoice_date - timedelta(days=5), "total_amount": round(random() * 1000, 2), "status": "pending"})
        else:
            invoice_date = datetime.now() - timedelta(days=10)
            data.append({"invoice_id": i, "order_id": i, "invoice_date": invoice_date, "due_date": invoice_date + timedelta(days=30), "total_amount": round(random() * 1000, 2), "status": choice(["pending", "paid", "overdue"])})
    create_table(catalog, "demo", "invoices", schema, data)

    # Activity Logs
    schema = Schema(
        NestedField(1, "log_id", LongType(), required=False),
        NestedField(2, "user_id", LongType(), required=False),
        NestedField(3, "event_type", StringType(), required=False),
        NestedField(4, "event_timestamp", TimestampType(), required=False),
        NestedField(5, "ip_address", StringType(), required=False),
        NestedField(6, "session_id", StringType(), required=False),
    )
    data = []
    for i in range(1, 11):
        data.append({"log_id": i, "user_id": randint(1, 8), "event_type": choice(["login", "logout", "view_product", "add_to_cart", "checkout"]), "event_timestamp": datetime.now() - timedelta(minutes=randint(0, 1440)), "ip_address": f"192.168.1.{randint(1, 255)}", "session_id": f"session-{i}"})
    create_table(catalog, "demo", "activity_logs", schema, data)

    print("\n" + "=" * 70)
    print("✓ All 10 tables created successfully!")
    print("=" * 70)
    print("\nData quality issues included for testing:")
    print("  • customers: NULL email, invalid phone format")
    print("  • orders: negative amount, NULL amount")
    print("  • payments: huge outlier (999999.99), NULL status")
    print("  • products: negative stock quantity")
    print("  • reviews: rating out of range (10)")
    print("  • shipments: delivery before shipped date")
    print("  • invoices: due date before invoice date")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

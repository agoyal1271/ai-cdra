#!/usr/bin/env python3
"""
Create 10 test Iceberg tables using Spark SQL against REST catalog.
Tables include bad data quality records for testing.
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession

def get_jwt_token():
    """Get Knox JWT token from environment."""
    token = os.getenv("KNOX_JWT", "")
    if not token:
        print("⚠ WARNING: KNOX_JWT not set. Run: python ./knoxshare.py --showjwt=True")
        print("Then export KNOX_JWT=<token>")
    return token

def create_spark_session(jwt_token):
    """Create SparkSession with REST Iceberg catalog."""
    spark = SparkSession.builder \
        .appName("IcebergDataQualityTest") \
        .config("spark.sql.catalog.cdp", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.cdp.type", "rest") \
        .config("spark.sql.catalog.cdp.uri", "http://cdp-utility.cdp.local:8443/gateway/cdp-datashare-access/iceberg-rest") \
        .config("spark.sql.catalog.cdp.token", jwt_token) \
        .config("spark.sql.catalog.cdp.default-namespace", "demo") \
        .config("spark.sql.catalog.cdp.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
        .config("spark.sql.defaultCatalog", "cdp") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://cdp-utility.cdp.local:9878") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("OZONE_ACCESS_KEY", "")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("OZONE_SECRET_KEY", "")) \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    print("=" * 80)
    print("Creating test Iceberg tables using Spark SQL (REST Catalog)")
    print("=" * 80 + "\n")

    jwt_token = get_jwt_token()
    if not jwt_token:
        print("✗ No JWT token available. Cannot proceed.")
        sys.exit(1)

    spark = create_spark_session(jwt_token)
    print("✓ SparkSession created with REST catalog\n")

    try:
        # Create namespace
        spark.sql("CREATE NAMESPACE IF NOT EXISTS cdp.demo")
        print("✓ Created namespace cdp.demo\n")

        # 1. Customers (with bad email and phone records)
        spark.sql("""
            CREATE OR REPLACE TABLE cdp.demo.customers (
                customer_id LONG,
                email STRING,
                first_name STRING,
                last_name STRING,
                phone STRING,
                created_at TIMESTAMP,
                active BOOLEAN
            )
            USING ICEBERG
        """)
        spark.sql("""
            INSERT INTO cdp.demo.customers VALUES
            (1, 'customer1@example.com', 'Alice', 'Smith', '555-0001', current_timestamp(), true),
            (2, 'customer2@example.com', 'Bob', 'Johnson', '555-0002', current_timestamp(), false),
            (3, NULL, 'Charlie', 'Williams', '555-0003', current_timestamp(), true),
            (4, 'customer4@example.com', 'Diana', 'Brown', '555-0004', current_timestamp(), true),
            (5, 'customer5@example.com', 'Eve', 'Jones', '555-0005', current_timestamp(), false),
            (6, 'customer6@example.com', 'Frank', 'Garcia', '555-0006', current_timestamp(), true),
            (7, 'customer7@example.com', 'Grace', 'Miller', 'INVALID_PHONE', current_timestamp(), true),
            (8, 'customer8@example.com', 'Henry', 'Davis', '555-0008', current_timestamp(), false)
        """)
        print("✓ cdp.demo.customers (8 records, bad: NULL email, invalid phone)")

        # 2. Orders (with negative and NULL amounts)
        spark.sql("""
            CREATE OR REPLACE TABLE cdp.demo.orders (
                order_id LONG,
                customer_id LONG,
                order_date TIMESTAMP,
                total_amount DOUBLE,
                status STRING,
                shipping_address STRING
            )
            USING ICEBERG
        """)
        spark.sql("""
            INSERT INTO cdp.demo.orders VALUES
            (1, 1, current_timestamp(), 150.00, 'completed', 'Address 1'),
            (2, 2, current_timestamp(), 200.50, 'shipped', 'Address 2'),
            (3, 3, current_timestamp(), 75.25, 'pending', 'Address 3'),
            (4, 4, current_timestamp(), 300.00, 'completed', 'Address 4'),
            (5, 5, current_timestamp(), -99.99, 'completed', 'Address 5'),
            (6, 6, current_timestamp(), 125.75, 'cancelled', 'Address 6'),
            (7, 7, current_timestamp(), 450.00, 'pending', 'Address 7'),
            (8, 8, current_timestamp(), 89.99, 'shipped', 'Address 8'),
            (9, 1, current_timestamp(), NULL, 'pending', 'Address 9'),
            (10, 2, current_timestamp(), 225.00, 'completed', 'Address 10')
        """)
        print("✓ cdp.demo.orders (10 records, bad: negative amount, NULL amount)")

        # 3. Payments (with huge outlier and NULL status)
        spark.sql("""
            CREATE OR REPLACE TABLE cdp.demo.payments (
                payment_id LONG,
                order_id LONG,
                amount DOUBLE,
                payment_method STRING,
                payment_date TIMESTAMP,
                status STRING
            )
            USING ICEBERG
        """)
        spark.sql("""
            INSERT INTO cdp.demo.payments VALUES
            (1, 1, 150.00, 'credit_card', current_timestamp(), 'completed'),
            (2, 2, 200.50, 'paypal', current_timestamp(), 'completed'),
            (3, 3, 75.25, 'bank_transfer', current_timestamp(), 'pending'),
            (4, 4, 999999.99, 'credit_card', current_timestamp(), 'completed'),
            (5, 5, 125.50, 'paypal', current_timestamp(), 'completed'),
            (6, 6, 250.00, 'bank_transfer', current_timestamp(), NULL),
            (7, 7, 450.00, 'credit_card', current_timestamp(), 'pending')
        """)
        print("✓ cdp.demo.payments (7 records, bad: huge outlier 999999.99, NULL status)")

        # 4. Products (with negative stock)
        spark.sql("""
            CREATE OR REPLACE TABLE cdp.demo.products (
                product_id LONG,
                product_name STRING,
                category STRING,
                price DOUBLE,
                stock_quantity LONG,
                created_at TIMESTAMP
            )
            USING ICEBERG
        """)
        spark.sql("""
            INSERT INTO cdp.demo.products VALUES
            (1, 'Laptop', 'Electronics', 999.99, 50, current_timestamp()),
            (2, 'Phone', 'Electronics', 699.99, 100, current_timestamp()),
            (3, 'T-Shirt', 'Clothing', 29.99, -5, current_timestamp()),
            (4, 'Jeans', 'Clothing', 79.99, 75, current_timestamp()),
            (5, 'Book', 'Books', 19.99, 200, current_timestamp()),
            (6, 'Headphones', 'Electronics', 149.99, 80, current_timestamp()),
            (7, 'Shoes', 'Clothing', 89.99, 120, current_timestamp()),
            (8, 'Novel', 'Books', 14.99, 150, current_timestamp())
        """)
        print("✓ cdp.demo.products (8 records, bad: negative stock -5)")

        # 5. Reviews (with invalid rating)
        spark.sql("""
            CREATE OR REPLACE TABLE cdp.demo.reviews (
                review_id LONG,
                product_id LONG,
                customer_id LONG,
                rating LONG,
                review_text STRING,
                created_at TIMESTAMP
            )
            USING ICEBERG
        """)
        spark.sql("""
            INSERT INTO cdp.demo.reviews VALUES
            (1, 1, 1, 5, 'Excellent laptop!', current_timestamp()),
            (2, 2, 2, 10, 'Amazing phone!', current_timestamp()),
            (3, 3, 3, 4, 'Great shirt', current_timestamp()),
            (4, 4, 4, 5, 'Perfect jeans', current_timestamp()),
            (5, 5, 5, 3, 'Good book', current_timestamp()),
            (6, 6, 6, 5, 'Best headphones ever', current_timestamp()),
            (7, 7, 7, 2, 'Shoes not comfortable', current_timestamp()),
            (8, 8, 8, 4, 'Interesting novel', current_timestamp())
        """)
        print("✓ cdp.demo.reviews (8 records, bad: rating out of range (10))")

        # 6. Inventory
        spark.sql("""
            CREATE OR REPLACE TABLE cdp.demo.inventory (
                inventory_id LONG,
                product_id LONG,
                warehouse_id STRING,
                quantity LONG,
                last_updated TIMESTAMP
            )
            USING ICEBERG
        """)
        spark.sql("""
            INSERT INTO cdp.demo.inventory VALUES
            (1, 1, 'WH-A', 30, current_timestamp()),
            (2, 2, 'WH-B', 50, current_timestamp()),
            (3, 3, 'WH-C', 75, current_timestamp()),
            (4, 4, 'WH-A', 40, current_timestamp()),
            (5, 5, 'WH-B', 120, current_timestamp()),
            (6, 6, 'WH-C', 55, current_timestamp()),
            (7, 7, 'WH-A', 65, current_timestamp())
        """)
        print("✓ cdp.demo.inventory (7 records)")

        # 7. Shipments (with delivery before shipped)
        spark.sql("""
            CREATE OR REPLACE TABLE cdp.demo.shipments (
                shipment_id LONG,
                order_id LONG,
                shipped_date TIMESTAMP,
                delivery_date TIMESTAMP,
                carrier STRING,
                tracking_number STRING
            )
            USING ICEBERG
        """)
        spark.sql("""
            INSERT INTO cdp.demo.shipments VALUES
            (1, 1, current_timestamp(), current_timestamp() + INTERVAL 3 DAY, 'FedEx', 'FDX001'),
            (2, 2, current_timestamp(), current_timestamp() + INTERVAL 5 DAY, 'UPS', 'UPS002'),
            (3, 3, current_timestamp(), current_timestamp() + INTERVAL 2 DAY, 'DHL', 'DHL003'),
            (4, 4, current_timestamp() - INTERVAL 5 DAY, current_timestamp() - INTERVAL 10 DAY, 'FedEx', 'FDX004'),
            (5, 5, current_timestamp(), current_timestamp() + INTERVAL 4 DAY, 'UPS', 'UPS005'),
            (6, 6, current_timestamp(), current_timestamp() + INTERVAL 6 DAY, 'DHL', 'DHL006'),
            (7, 7, current_timestamp(), current_timestamp() + INTERVAL 3 DAY, 'FedEx', 'FDX007'),
            (8, 8, current_timestamp(), current_timestamp() + INTERVAL 2 DAY, 'UPS', 'UPS008')
        """)
        print("✓ cdp.demo.shipments (8 records, bad: delivery before shipped)")

        # 8. Returns
        spark.sql("""
            CREATE OR REPLACE TABLE cdp.demo.returns (
                return_id LONG,
                order_id LONG,
                product_id LONG,
                return_date TIMESTAMP,
                reason STRING,
                refund_amount DOUBLE
            )
            USING ICEBERG
        """)
        spark.sql("""
            INSERT INTO cdp.demo.returns VALUES
            (1, 2, 2, current_timestamp(), 'Defective', 200.50),
            (2, 4, 3, current_timestamp(), 'Wrong item', 29.99),
            (3, 6, 6, current_timestamp(), 'Changed mind', 149.99),
            (4, 8, 7, current_timestamp(), 'Better price elsewhere', 89.99),
            (5, 1, 1, current_timestamp(), 'Not as described', 999.99),
            (6, 5, 5, current_timestamp(), 'Damaged', 19.99)
        """)
        print("✓ cdp.demo.returns (6 records)")

        # 9. Invoices (with due date before invoice date)
        spark.sql("""
            CREATE OR REPLACE TABLE cdp.demo.invoices (
                invoice_id LONG,
                order_id LONG,
                invoice_date TIMESTAMP,
                due_date TIMESTAMP,
                total_amount DOUBLE,
                status STRING
            )
            USING ICEBERG
        """)
        spark.sql("""
            INSERT INTO cdp.demo.invoices VALUES
            (1, 1, current_timestamp(), current_timestamp() + INTERVAL 30 DAY, 150.00, 'pending'),
            (2, 2, current_timestamp(), current_timestamp() + INTERVAL 30 DAY, 200.50, 'paid'),
            (3, 3, current_timestamp(), current_timestamp() - INTERVAL 5 DAY, 75.25, 'pending'),
            (4, 4, current_timestamp(), current_timestamp() + INTERVAL 30 DAY, 300.00, 'overdue'),
            (5, 5, current_timestamp(), current_timestamp() + INTERVAL 30 DAY, 99.99, 'pending'),
            (6, 6, current_timestamp(), current_timestamp() + INTERVAL 30 DAY, 125.75, 'paid'),
            (7, 7, current_timestamp(), current_timestamp() + INTERVAL 30 DAY, 450.00, 'pending')
        """)
        print("✓ cdp.demo.invoices (7 records, bad: due date before invoice date)")

        # 10. Activity Logs
        spark.sql("""
            CREATE OR REPLACE TABLE cdp.demo.activity_logs (
                log_id LONG,
                user_id LONG,
                event_type STRING,
                event_timestamp TIMESTAMP,
                ip_address STRING,
                session_id STRING
            )
            USING ICEBERG
        """)
        spark.sql("""
            INSERT INTO cdp.demo.activity_logs VALUES
            (1, 1, 'login', current_timestamp(), '192.168.1.100', 'session-1'),
            (2, 2, 'view_product', current_timestamp(), '192.168.1.101', 'session-2'),
            (3, 3, 'add_to_cart', current_timestamp(), '192.168.1.102', 'session-3'),
            (4, 4, 'checkout', current_timestamp(), '192.168.1.103', 'session-4'),
            (5, 5, 'login', current_timestamp(), '192.168.1.104', 'session-5'),
            (6, 6, 'logout', current_timestamp(), '192.168.1.105', 'session-6'),
            (7, 7, 'view_product', current_timestamp(), '192.168.1.106', 'session-7'),
            (8, 8, 'add_to_cart', current_timestamp(), '192.168.1.107', 'session-8'),
            (9, 1, 'checkout', current_timestamp(), '192.168.1.108', 'session-9'),
            (10, 2, 'logout', current_timestamp(), '192.168.1.109', 'session-10')
        """)
        print("✓ cdp.demo.activity_logs (10 records)\n")

        # List all tables
        print("=" * 80)
        print("TABLES IN cdp.demo:")
        print("=" * 80)
        spark.sql("SHOW TABLES IN cdp.demo").show(truncate=False)

        print("\n" + "=" * 80)
        print("✓ Successfully created 10 test Iceberg tables with data quality issues!")
        print("=" * 80)
        print("\nData quality issues for testing:")
        print("  • customers: NULL email, invalid phone format")
        print("  • orders: negative amount (-99.99), NULL amount")
        print("  • payments: huge outlier (999999.99), NULL status")
        print("  • products: negative stock (-5)")
        print("  • reviews: rating out of range (10)")
        print("  • shipments: delivery_date before shipped_date")
        print("  • invoices: due_date before invoice_date")
        print("\n" + "=" * 80)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

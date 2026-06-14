#!/usr/bin/env python3
"""
Generate realistic sample messages for T-Life Kafka topics.
Produces JSON serialized versions of sample records for testing.
"""
import json
import random
import time
from datetime import datetime, timedelta

# Sample data generators
def generate_customer_profiles(count: int = 5) -> list[dict]:
    """Generate sample customer profile records."""
    first_names = ["Alice", "Bob", "Charlie", "Diana", "Emma", "Frank"]
    last_names = ["Johnson", "Smith", "Williams", "Brown", "Jones", "Garcia"]
    statuses = ["ACTIVE", "INACTIVE", "SUSPENDED"]

    records = []
    for i in range(count):
        records.append({
            "customer_id": f"CUST-{1000 + i:05d}",
            "email": f"customer{i}@example.com",
            "phone_number": f"+1{random.randint(2000000000, 9999999999)}",
            "first_name": random.choice(first_names),
            "last_name": random.choice(last_names),
            "date_of_birth": None,
            "account_status": random.choice(statuses),
            "creation_date": int((datetime.now() - timedelta(days=random.randint(30, 365))).timestamp() * 1000),
            "last_updated": int(time.time() * 1000),
        })
    return records

def generate_account_details(count: int = 5) -> list[dict]:
    """Generate sample account detail records."""
    plan_names = ["T-Mobile One", "T-Mobile Essentials", "T-Mobile Magenta", "T-Mobile Go"]
    account_types = ["Individual", "Family", "Business"]

    records = []
    for i in range(count):
        records.append({
            "account_id": f"ACCT-{2000 + i:05d}",
            "customer_id": f"CUST-{1000 + i:05d}",
            "account_type": random.choice(account_types),
            "plan_name": random.choice(plan_names),
            "billing_address": f"{random.randint(100, 9999)} Main St, City {chr(65 + i)}, ST 12345",
            "billing_zip": f"{random.randint(10000, 99999)}",
            "account_balance": round(random.uniform(-100, 500), 2),
            "credit_limit": round(random.uniform(1000, 5000), 2) if random.random() > 0.5 else None,
        })
    return records

def generate_billing_cycles(count: int = 5) -> list[dict]:
    """Generate sample billing cycle records."""
    records = []
    for i in range(count):
        start = datetime.now() - timedelta(days=random.randint(1, 30))
        end = start + timedelta(days=30)
        subtotal = round(random.uniform(50, 300), 2)
        tax = round(subtotal * 0.10, 2)
        discount = round(random.uniform(0, 30), 2) if random.random() > 0.7 else 0

        records.append({
            "cycle_id": f"CYCLE-{3000 + i:05d}",
            "account_id": f"ACCT-{2000 + i:05d}",
            "cycle_start_date": int(start.timestamp() * 1000),
            "cycle_end_date": int(end.timestamp() * 1000),
            "subtotal_amount": subtotal,
            "tax_amount": tax,
            "discount_amount": discount if discount > 0 else None,
            "total_amount_due": round(subtotal + tax - discount, 2),
            "due_date": int((end + timedelta(days=14)).timestamp() * 1000),
        })
    return records

def generate_payment_transactions(count: int = 5) -> list[dict]:
    """Generate sample payment transaction records."""
    methods = ["CREDIT_CARD", "DEBIT_CARD", "ACH", "GOOGLE_PAY", "APPLE_PAY"]
    statuses = ["COMPLETED", "PENDING", "FAILED"]

    records = []
    for i in range(count):
        records.append({
            "transaction_id": f"TXN-{4000 + i:05d}",
            "account_id": f"ACCT-{2000 + i:05d}",
            "payment_date": int((datetime.now() - timedelta(days=random.randint(0, 30))).timestamp() * 1000),
            "payment_method": random.choice(methods),
            "amount_paid": round(random.uniform(50, 300), 2),
            "currency": "USD",
            "payment_status": random.choice(statuses),
            "confirmation_number": f"CONF-{random.randint(100000, 999999)}" if random.random() > 0.2 else None,
        })
    return records

def generate_data_usage(count: int = 5) -> list[dict]:
    """Generate sample data usage records."""
    records = []
    for i in range(count):
        data_limit = 100.0
        data_used = round(random.uniform(10, data_limit), 2)

        records.append({
            "usage_id": f"USAGE-{5000 + i:05d}",
            "customer_id": f"CUST-{1000 + i:05d}",
            "date": int((datetime.now() - timedelta(days=random.randint(0, 30))).timestamp() * 1000),
            "data_used_mb": data_used,
            "data_limit_mb": data_limit,
            "usage_percentage": round((data_used / data_limit) * 100, 2),
            "wifi_mb": round(data_used * random.uniform(0.3, 0.7), 2),
            "cellular_mb": round(data_used * random.uniform(0.3, 0.7), 2),
        })
    return records

def generate_device_inventory(count: int = 5) -> list[dict]:
    """Generate sample device inventory records."""
    device_types = ["SMARTPHONE", "TABLET", "SMARTWATCH", "LAPTOP"]
    models = ["iPhone 15", "Galaxy S24", "Pixel 8", "iPad Pro", "Apple Watch"]
    os = ["iOS", "Android", "watchOS"]

    records = []
    for i in range(count):
        records.append({
            "device_id": f"DEV-{6000 + i:05d}",
            "customer_id": f"CUST-{1000 + i:05d}",
            "device_type": random.choice(device_types),
            "model_name": random.choice(models),
            "imei": f"{random.randint(10000000000000, 99999999999999)}",
            "serial_number": f"SN{random.randint(100000, 999999)}",
            "operating_system": random.choice(os),
            "os_version": f"{random.randint(12, 18)}.{random.randint(0, 9)}",
            "purchase_date": int((datetime.now() - timedelta(days=random.randint(30, 730))).timestamp() * 1000),
            "warranty_expiry": int((datetime.now() + timedelta(days=random.randint(0, 365))).timestamp() * 1000),
            "device_status": random.choice(["ACTIVE", "INACTIVE", "SUSPENDED"]),
        })
    return records

def generate_login_events(count: int = 5) -> list[dict]:
    """Generate sample login event records."""
    device_types = ["iOS", "Android", "Web", "Web"]
    records = []
    for i in range(count):
        records.append({
            "event_id": f"LOGIN-{7000 + i:05d}",
            "customer_id": f"CUST-{1000 + i:05d}",
            "login_timestamp": int((datetime.now() - timedelta(hours=random.randint(0, 48))).timestamp() * 1000),
            "device_type": random.choice(device_types),
            "ip_address": f"{random.randint(10, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}",
            "latitude": round(random.uniform(25.0, 50.0), 4),
            "longitude": round(random.uniform(-125.0, -65.0), 4),
            "login_status": random.choice(["SUCCESS", "FAILED", "MFA_REQUIRED"]),
        })
    return records

def generate_support_tickets(count: int = 5) -> list[dict]:
    """Generate sample support ticket records."""
    categories = ["BILLING", "TECHNICAL", "ACCOUNT", "DEVICE", "NETWORK"]
    priorities = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    statuses = ["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"]

    records = []
    for i in range(count):
        created = datetime.now() - timedelta(days=random.randint(0, 30))
        resolved = created + timedelta(hours=random.randint(1, 48)) if random.random() > 0.3 else None

        records.append({
            "ticket_id": f"TICKET-{8000 + i:05d}",
            "customer_id": f"CUST-{1000 + i:05d}",
            "issue_category": random.choice(categories),
            "description": f"Issue description {i}",
            "priority": random.choice(priorities),
            "created_date": int(created.timestamp() * 1000),
            "resolved_date": int(resolved.timestamp() * 1000) if resolved else None,
            "ticket_status": random.choice(statuses),
            "resolution_cost": round(random.uniform(0, 150), 2) if resolved else None,
        })
    return records

def generate_network_quality(count: int = 5) -> list[dict]:
    """Generate sample network quality records."""
    network_types = ["4G", "5G", "LTE", "3G"]
    records = []
    for i in range(count):
        records.append({
            "quality_id": f"QUALITY-{9000 + i:05d}",
            "device_id": f"DEV-{6000 + i:05d}",
            "timestamp": int((datetime.now() - timedelta(minutes=random.randint(0, 60))).timestamp() * 1000),
            "signal_strength_dbm": random.randint(-120, -70),
            "signal_bars": random.randint(1, 5),
            "network_type": random.choice(network_types),
            "download_speed_mbps": round(random.uniform(10, 500), 2),
            "upload_speed_mbps": round(random.uniform(5, 100), 2),
            "latency_ms": random.randint(10, 100),
        })
    return records

def main():
    """Generate and print all sample records grouped by domain."""
    generators = [
        ("Customer Profiles", generate_customer_profiles),
        ("Account Details", generate_account_details),
        ("Billing Cycles", generate_billing_cycles),
        ("Payment Transactions", generate_payment_transactions),
        ("Data Usage", generate_data_usage),
        ("Device Inventory", generate_device_inventory),
        ("Login Events", generate_login_events),
        ("Support Tickets", generate_support_tickets),
        ("Network Quality", generate_network_quality),
    ]

    all_samples = {}
    for domain, gen in generators:
        samples = gen(count=3)
        all_samples[domain] = samples

    # Print as JSON for easy import/use
    print(json.dumps(all_samples, indent=2, default=str))

if __name__ == "__main__":
    main()

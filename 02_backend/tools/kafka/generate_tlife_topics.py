"""
Generate 25 T-Life (T-Mobile app) Kafka topics with Avro schemas.
Covers: customer data, device mgmt, billing, usage, network, support, services.
"""
import json
import logging
from typing import Any, List, Dict

logger = logging.getLogger(__name__)

# Organized by domain
TLIFE_SCHEMAS = {
    # ════════════════════════════════════════════════════════════════════════════════
    # DOMAIN: CUSTOMER & ACCOUNT
    # ════════════════════════════════════════════════════════════════════════════════
    "tlife.customer_profiles": {
        "namespace": "tlife.customer",
        "type": "record",
        "name": "CustomerProfile",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "phone_number", "type": "string"},
            {"name": "first_name", "type": "string"},
            {"name": "last_name", "type": "string"},
            {"name": "date_of_birth", "type": ["null", "string"], "default": None},
            {"name": "account_status", "type": "string"},
            {"name": "creation_date", "type": "long"},
            {"name": "last_updated", "type": "long"},
        ],
    },
    "tlife.account_details": {
        "namespace": "tlife.account",
        "type": "record",
        "name": "AccountDetails",
        "fields": [
            {"name": "account_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "account_type", "type": "string"},
            {"name": "plan_name", "type": "string"},
            {"name": "billing_address", "type": "string"},
            {"name": "billing_zip", "type": "string"},
            {"name": "account_balance", "type": "double"},
            {"name": "credit_limit", "type": ["null", "double"], "default": None},
        ],
    },
    "tlife.login_events": {
        "namespace": "tlife.auth",
        "type": "record",
        "name": "LoginEvent",
        "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "login_timestamp", "type": "long"},
            {"name": "device_type", "type": "string"},
            {"name": "ip_address", "type": "string"},
            {"name": "latitude", "type": ["null", "double"], "default": None},
            {"name": "longitude", "type": ["null", "double"], "default": None},
            {"name": "login_status", "type": "string"},
        ],
    },
    "tlife.address_changes": {
        "namespace": "tlife.customer",
        "type": "record",
        "name": "AddressChange",
        "fields": [
            {"name": "change_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "old_address", "type": "string"},
            {"name": "new_address", "type": "string"},
            {"name": "change_type", "type": "string"},
            {"name": "change_date", "type": "long"},
        ],
    },

    # ════════════════════════════════════════════════════════════════════════════════
    # DOMAIN: DEVICES & HARDWARE
    # ════════════════════════════════════════════════════════════════════════════════
    "tlife.device_inventory": {
        "namespace": "tlife.device",
        "type": "record",
        "name": "DeviceInventory",
        "fields": [
            {"name": "device_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "device_type", "type": "string"},
            {"name": "model_name", "type": "string"},
            {"name": "imei", "type": "string"},
            {"name": "serial_number", "type": "string"},
            {"name": "operating_system", "type": "string"},
            {"name": "os_version", "type": "string"},
            {"name": "purchase_date", "type": ["null", "long"], "default": None},
            {"name": "warranty_expiry", "type": ["null", "long"], "default": None},
            {"name": "device_status", "type": "string"},
        ],
    },
    "tlife.device_metrics": {
        "namespace": "tlife.device",
        "type": "record",
        "name": "DeviceMetrics",
        "fields": [
            {"name": "metric_id", "type": "string"},
            {"name": "device_id", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "battery_level", "type": "int"},
            {"name": "storage_used_gb", "type": "double"},
            {"name": "storage_total_gb", "type": "double"},
            {"name": "ram_used_mb", "type": "int"},
            {"name": "ram_total_mb", "type": "int"},
            {"name": "cpu_usage_percent", "type": "double"},
            {"name": "temperature_celsius", "type": ["null", "double"], "default": None},
        ],
    },
    "tlife.app_installations": {
        "namespace": "tlife.device",
        "type": "record",
        "name": "AppInstallation",
        "fields": [
            {"name": "installation_id", "type": "string"},
            {"name": "device_id", "type": "string"},
            {"name": "app_name", "type": "string"},
            {"name": "package_name", "type": "string"},
            {"name": "version_code", "type": "int"},
            {"name": "install_date", "type": "long"},
            {"name": "update_date", "type": ["null", "long"], "default": None},
            {"name": "app_size_mb", "type": "double"},
        ],
    },

    # ════════════════════════════════════════════════════════════════════════════════
    # DOMAIN: BILLING & PAYMENTS
    # ════════════════════════════════════════════════════════════════════════════════
    "tlife.billing_cycles": {
        "namespace": "tlife.billing",
        "type": "record",
        "name": "BillingCycle",
        "fields": [
            {"name": "cycle_id", "type": "string"},
            {"name": "account_id", "type": "string"},
            {"name": "cycle_start_date", "type": "long"},
            {"name": "cycle_end_date", "type": "long"},
            {"name": "subtotal_amount", "type": "double"},
            {"name": "tax_amount", "type": "double"},
            {"name": "discount_amount", "type": ["null", "double"], "default": None},
            {"name": "total_amount_due", "type": "double"},
            {"name": "due_date", "type": "long"},
        ],
    },
    "tlife.payment_transactions": {
        "namespace": "tlife.billing",
        "type": "record",
        "name": "PaymentTransaction",
        "fields": [
            {"name": "transaction_id", "type": "string"},
            {"name": "account_id", "type": "string"},
            {"name": "payment_date", "type": "long"},
            {"name": "payment_method", "type": "string"},
            {"name": "amount_paid", "type": "double"},
            {"name": "currency", "type": "string"},
            {"name": "payment_status", "type": "string"},
            {"name": "confirmation_number", "type": ["null", "string"], "default": None},
        ],
    },
    "tlife.invoice_items": {
        "namespace": "tlife.billing",
        "type": "record",
        "name": "InvoiceItem",
        "fields": [
            {"name": "item_id", "type": "string"},
            {"name": "cycle_id", "type": "string"},
            {"name": "description", "type": "string"},
            {"name": "quantity", "type": "int"},
            {"name": "unit_price", "type": "double"},
            {"name": "line_total", "type": "double"},
            {"name": "item_category", "type": "string"},
        ],
    },
    "tlife.promotions_applied": {
        "namespace": "tlife.billing",
        "type": "record",
        "name": "PromotionApplied",
        "fields": [
            {"name": "promotion_id", "type": "string"},
            {"name": "account_id", "type": "string"},
            {"name": "promo_code", "type": "string"},
            {"name": "discount_percent", "type": ["null", "double"], "default": None},
            {"name": "discount_amount", "type": ["null", "double"], "default": None},
            {"name": "activation_date", "type": "long"},
            {"name": "expiry_date", "type": ["null", "long"], "default": None},
        ],
    },

    # ════════════════════════════════════════════════════════════════════════════════
    # DOMAIN: NETWORK & USAGE
    # ════════════════════════════════════════════════════════════════════════════════
    "tlife.data_usage": {
        "namespace": "tlife.usage",
        "type": "record",
        "name": "DataUsage",
        "fields": [
            {"name": "usage_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "date", "type": "long"},
            {"name": "data_used_mb", "type": "double"},
            {"name": "data_limit_mb", "type": "double"},
            {"name": "usage_percentage", "type": "double"},
            {"name": "wifi_mb", "type": "double"},
            {"name": "cellular_mb", "type": "double"},
        ],
    },
    "tlife.voice_call_details": {
        "namespace": "tlife.usage",
        "type": "record",
        "name": "VoiceCallDetail",
        "fields": [
            {"name": "call_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "call_start_time", "type": "long"},
            {"name": "call_duration_seconds", "type": "int"},
            {"name": "called_number", "type": ["null", "string"], "default": None},
            {"name": "call_type", "type": "string"},
            {"name": "call_cost", "type": ["null", "double"], "default": None},
        ],
    },
    "tlife.sms_events": {
        "namespace": "tlife.usage",
        "type": "record",
        "name": "SMSEvent",
        "fields": [
            {"name": "sms_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "recipient", "type": ["null", "string"], "default": None},
            {"name": "message_type", "type": "string"},
            {"name": "character_count", "type": "int"},
            {"name": "sms_cost", "type": ["null", "double"], "default": None},
        ],
    },
    "tlife.roaming_usage": {
        "namespace": "tlife.usage",
        "type": "record",
        "name": "RoamingUsage",
        "fields": [
            {"name": "roaming_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "start_date", "type": "long"},
            {"name": "end_date", "type": "long"},
            {"name": "country", "type": "string"},
            {"name": "data_used_mb", "type": "double"},
            {"name": "roaming_charges", "type": "double"},
        ],
    },

    # ════════════════════════════════════════════════════════════════════════════════
    # DOMAIN: NETWORK PERFORMANCE & COVERAGE
    # ════════════════════════════════════════════════════════════════════════════════
    "tlife.network_quality": {
        "namespace": "tlife.network",
        "type": "record",
        "name": "NetworkQuality",
        "fields": [
            {"name": "quality_id", "type": "string"},
            {"name": "device_id", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "signal_strength_dbm", "type": "int"},
            {"name": "signal_bars", "type": "int"},
            {"name": "network_type", "type": "string"},
            {"name": "download_speed_mbps", "type": ["null", "double"], "default": None},
            {"name": "upload_speed_mbps", "type": ["null", "double"], "default": None},
            {"name": "latency_ms", "type": ["null", "int"], "default": None},
        ],
    },
    "tlife.coverage_checks": {
        "namespace": "tlife.network",
        "type": "record",
        "name": "CoverageCheck",
        "fields": [
            {"name": "check_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "latitude", "type": "double"},
            {"name": "longitude", "type": "double"},
            {"name": "timestamp", "type": "long"},
            {"name": "coverage_type", "type": "string"},
            {"name": "signal_strength", "type": "string"},
            {"name": "is_5g_available", "type": "boolean"},
        ],
    },

    # ════════════════════════════════════════════════════════════════════════════════
    # DOMAIN: PLANS & SERVICES
    # ════════════════════════════════════════════════════════════════════════════════
    "tlife.plan_changes": {
        "namespace": "tlife.service",
        "type": "record",
        "name": "PlanChange",
        "fields": [
            {"name": "change_id", "type": "string"},
            {"name": "account_id", "type": "string"},
            {"name": "change_date", "type": "long"},
            {"name": "old_plan_id", "type": "string"},
            {"name": "new_plan_id", "type": "string"},
            {"name": "old_plan_price", "type": "double"},
            {"name": "new_plan_price", "type": "double"},
            {"name": "change_reason", "type": "string"},
        ],
    },
    "tlife.addon_subscriptions": {
        "namespace": "tlife.service",
        "type": "record",
        "name": "AddonSubscription",
        "fields": [
            {"name": "subscription_id", "type": "string"},
            {"name": "account_id", "type": "string"},
            {"name": "addon_name", "type": "string"},
            {"name": "addon_code", "type": "string"},
            {"name": "monthly_cost", "type": "double"},
            {"name": "activation_date", "type": "long"},
            {"name": "cancellation_date", "type": ["null", "long"], "default": None},
            {"name": "auto_renew", "type": "boolean"},
        ],
    },
    "tlife.family_plan_members": {
        "namespace": "tlife.service",
        "type": "record",
        "name": "FamilyPlanMember",
        "fields": [
            {"name": "member_id", "type": "string"},
            {"name": "account_id", "type": "string"},
            {"name": "phone_number", "type": "string"},
            {"name": "member_name", "type": "string"},
            {"name": "relationship", "type": "string"},
            {"name": "join_date", "type": "long"},
            {"name": "data_limit_gb", "type": "double"},
            {"name": "parental_controls_enabled", "type": "boolean"},
        ],
    },

    # ════════════════════════════════════════════════════════════════════════════════
    # DOMAIN: CUSTOMER SUPPORT
    # ════════════════════════════════════════════════════════════════════════════════
    "tlife.support_tickets": {
        "namespace": "tlife.support",
        "type": "record",
        "name": "SupportTicket",
        "fields": [
            {"name": "ticket_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "issue_category", "type": "string"},
            {"name": "description", "type": "string"},
            {"name": "priority", "type": "string"},
            {"name": "created_date", "type": "long"},
            {"name": "resolved_date", "type": ["null", "long"], "default": None},
            {"name": "ticket_status", "type": "string"},
            {"name": "resolution_cost", "type": ["null", "double"], "default": None},
        ],
    },
    "tlife.support_interactions": {
        "namespace": "tlife.support",
        "type": "record",
        "name": "SupportInteraction",
        "fields": [
            {"name": "interaction_id", "type": "string"},
            {"name": "ticket_id", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "interaction_type", "type": "string"},
            {"name": "agent_id", "type": ["null", "string"], "default": None},
            {"name": "duration_seconds", "type": ["null", "int"], "default": None},
            {"name": "resolution_provided", "type": "boolean"},
        ],
    },

    # ════════════════════════════════════════════════════════════════════════════════
    # DOMAIN: PREFERENCES & SETTINGS
    # ════════════════════════════════════════════════════════════════════════════════
    "tlife.notification_preferences": {
        "namespace": "tlife.preferences",
        "type": "record",
        "name": "NotificationPreference",
        "fields": [
            {"name": "preference_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "notification_type", "type": "string"},
            {"name": "channel", "type": "string"},
            {"name": "enabled", "type": "boolean"},
            {"name": "frequency", "type": "string"},
            {"name": "last_updated", "type": "long"},
        ],
    },
    "tlife.privacy_settings": {
        "namespace": "tlife.preferences",
        "type": "record",
        "name": "PrivacySetting",
        "fields": [
            {"name": "setting_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "location_tracking_enabled", "type": "boolean"},
            {"name": "analytics_enabled", "type": "boolean"},
            {"name": "data_sharing_enabled", "type": "boolean"},
            {"name": "marketing_emails_enabled", "type": "boolean"},
            {"name": "last_modified", "type": "long"},
        ],
    },
}

def get_avro_schema(topic_name: str) -> Dict[str, Any]:
    """Returns the Avro schema for a given topic."""
    if topic_name not in TLIFE_SCHEMAS:
        raise ValueError(f"Topic {topic_name} not defined in schemas")
    return TLIFE_SCHEMAS[topic_name]

def list_all_topics() -> List[str]:
    """Returns all defined topic names."""
    return list(TLIFE_SCHEMAS.keys())

def register_topics_in_sr(sr_client: Any) -> Dict[str, bool]:
    """
    Register all TLIFE schemas in the Schema Registry.

    Args:
        sr_client: Schema Registry client (assuming schemaregistry-client)

    Returns:
        {topic_name: success_bool, ...}
    """
    results = {}
    for topic_name, schema_dict in TLIFE_SCHEMAS.items():
        try:
            schema_str = json.dumps(schema_dict)
            sr_client.register_schema(
                subject=f"{topic_name}-value",
                schema_str=schema_str,
                schema_type="AVRO",
            )
            results[topic_name] = True
            logger.info(f"✓ Registered {topic_name}")
        except Exception as e:
            logger.error(f"✗ Failed to register {topic_name}: {e}")
            results[topic_name] = False
    return results

if __name__ == "__main__":
    # For testing: print all topics
    print(f"\n{'='*80}")
    print(f"T-LIFE (T-Mobile) KAFKA TOPICS — {len(TLIFE_SCHEMAS)} schemas")
    print(f"{'='*80}\n")

    for domain, topics in [
        ("CUSTOMER & ACCOUNT", [k for k in TLIFE_SCHEMAS.keys() if "customer" in k or "account" in k or "login" in k or "address" in k]),
        ("DEVICES & HARDWARE", [k for k in TLIFE_SCHEMAS.keys() if "device" in k or "app_" in k]),
        ("BILLING & PAYMENTS", [k for k in TLIFE_SCHEMAS.keys() if "billing" in k or "payment" in k or "invoice" in k or "promotion" in k]),
        ("NETWORK & USAGE", [k for k in TLIFE_SCHEMAS.keys() if "usage" in k or "data_" in k or "voice_" in k or "sms_" in k or "roaming" in k]),
        ("NETWORK PERFORMANCE", [k for k in TLIFE_SCHEMAS.keys() if "network" in k or "coverage" in k]),
        ("PLANS & SERVICES", [k for k in TLIFE_SCHEMAS.keys() if "plan" in k or "addon" in k or "family" in k]),
        ("CUSTOMER SUPPORT", [k for k in TLIFE_SCHEMAS.keys() if "support" in k]),
        ("PREFERENCES & SETTINGS", [k for k in TLIFE_SCHEMAS.keys() if "preference" in k or "privacy" in k]),
    ]:
        if topics:
            print(f"\n{domain}:")
            for topic in sorted(topics):
                schema = TLIFE_SCHEMAS[topic]
                fields = [f["name"] for f in schema["fields"]]
                print(f"  • {topic}")
                print(f"    Fields: {', '.join(fields)}")

    print(f"\n{'='*80}\n")
    print(f"Total: {len(TLIFE_SCHEMAS)} schemas ready for registration\n")

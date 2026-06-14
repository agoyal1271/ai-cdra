"""Generate NiFi ReadyFlow configs for Kafka to Iceberg ingestion."""

from typing import Dict, Any, List
from urllib.parse import urlparse
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SECURITY_PROTOCOL,
    SCHEMA_REGISTRY_URL,
    KNOX_USERNAME,
    KNOX_PASSWORD,
)


def derive_schema_registry_hostname(schema_registry_url: str) -> str:
    """Extract hostname from full Schema Registry URL (no protocol/port)."""
    parsed = urlparse(schema_registry_url)
    return parsed.hostname or schema_registry_url.split("://")[-1].split(":")[0].split("/")[0]


def derive_cdp_environment(schema_registry_url: str) -> str:
    """Derive CDP Environment name from Schema Registry hostname."""
    hostname = derive_schema_registry_hostname(schema_registry_url)
    if not hostname:
        return "default"
    # CDP environment is usually the first part of the FQDN
    # e.g., "cdp-dev.example.com" → "cdp-dev"
    return hostname.split(".")[0]


def generate_nifi_readyflow_params(topic: str, schema_map: List[Dict[str, Any]]) -> Dict[str, str]:
    """Generate pre-filled ReadyFlow parameter context for Cloudera's Kafka → Iceberg ReadyFlow.

    These exact parameter names match Cloudera's official ReadyFlow template.

    Args:
        topic: Kafka topic name
        schema_map: Output from build_schema_map()

    Returns:
        Dict of parameter name → value for ReadyFlow deployment
    """
    table_name = topic.lower().replace("-", "_").split(".")[-1]
    sr_hostname = derive_schema_registry_hostname(SCHEMA_REGISTRY_URL)
    cdp_env = derive_cdp_environment(SCHEMA_REGISTRY_URL)

    return {
        "Kafka Broker Endpoint": KAFKA_BOOTSTRAP_SERVERS,
        "Kafka Source Topic": topic,
        "Kafka Consumer Group ID": f"cdf-{table_name}-consumer",
        "Schema Name": topic,
        "Schema Registry Hostname": sr_hostname,
        "Iceberg Table Name": table_name,
        "Hive Catalog Namespace": "default",
        "CDP Workload User": KNOX_USERNAME or "unknown",
        "CDP Workload User Password": KNOX_PASSWORD or "***",
        "Data Input Format": "AVRO",
        "CDPEnvironment": cdp_env,
    }


def generate_nifi_processor_properties(topic: str, schema_map: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    """Generate ConsumeKafka + PutIceberg processor properties for on-prem CFM deployments.

    For teams running NiFi via Cloudera Flow Management (CFM) on private clusters.

    Args:
        topic: Kafka topic name
        schema_map: Output from build_schema_map()

    Returns:
        Dict with ConsumeKafka and PutIceberg processor properties
    """
    table_name = topic.lower().replace("-", "_").split(".")[-1]

    return {
        "ConsumeKafka": {
            "Kafka Brokers": KAFKA_BOOTSTRAP_SERVERS,
            "Topic Name(s)": topic,
            "Group ID": f"nifi-{table_name}-consumer",
            "Offset Reset": "earliest",
            "Security Protocol": KAFKA_SECURITY_PROTOCOL or "PLAINTEXT",
            "Record Reader": "AvroReader (Schema Registry)",
            "Schema Registry URL": SCHEMA_REGISTRY_URL,
        },
        "PutIceberg": {
            "Catalog Service": "RESTCatalogService",
            "Catalog Namespace": "default",
            "Table Name": table_name,
            "Record Reader": "AvroReader",
            "File Format": "PARQUET",
            "Unmatched Column Behavior": "Ignore Unmatched Columns",
            "Number of Commit Retries": "10",
            "Maximum Commit Wait Time": "2 sec",
            "Maximum Commit Duration": "30 sec",
        }
    }


def generate_nifi_flow(topic: str, schema_map: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate complete NiFi flow config combining ReadyFlow params and processor properties.

    Args:
        topic: Kafka topic name
        schema_map: Output from build_schema_map()

    Returns:
        Dict with nifi_flow config including ReadyFlow params, processor properties, and deploy docs
    """
    readyflow_params = generate_nifi_readyflow_params(topic, schema_map)
    processor_props = generate_nifi_processor_properties(topic, schema_map)

    return {
        "readyflow_name": "Kafka to Iceberg",
        "parameters": readyflow_params,
        "processor_properties": processor_props,
        "deploy_url": "https://docs.cloudera.com/dataflow/cloud/readyflow-overview-kafka-iceberg/topics/cdf-readyflow-kafka-iceberg.html",
        "deployment_options": {
            "cloud_managed": {
                "name": "Cloudera DataFlow (Recommended)",
                "description": "Managed NiFi flow in Cloudera cloud",
                "instructions": "Use CDF UI to import ReadyFlow and provide parameters above"
            },
            "on_prem_cfm": {
                "name": "On-Prem CFM (Private Cluster)",
                "description": "Deploy to Cloudera Flow Management on private cluster",
                "instructions": "Configure ConsumeKafka + PutIceberg processors with properties above"
            }
        }
    }

#!/usr/bin/env python3
"""
Register all T-Life Kafka topics and schemas in the Schema Registry.

Usage:
  python register_tlife_topics.py [--sr-url URL] [--kafka-bootstrap SERVER:PORT] [--partitions N] [--replication-factor N]

Default SR: http://localhost:8081
Default Kafka: localhost:9092
"""
import argparse
import json
import logging
import sys
from pathlib import Path

# Add parent dirs to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.kafka.generate_tlife_topics import TLIFE_SCHEMAS, list_all_topics
from config import SCHEMA_REGISTRY_URL, KAFKA_BOOTSTRAP_SERVERS

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def register_schema_in_sr(sr_url: str, topic_name: str, schema_dict: dict) -> bool:
    """Register a single schema in the Schema Registry via HTTP."""
    import requests

    subject = f"{topic_name}-value"
    schema_str = json.dumps(schema_dict)

    url = f"{sr_url}/subjects/{subject}/versions"
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    payload = {"schema": schema_str, "schemaType": "AVRO"}

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=5)
        if resp.status_code in (200, 201):
            logger.info(f"✓ Registered {subject}")
            return True
        else:
            logger.warning(f"✗ {subject}: HTTP {resp.status_code} — {resp.text[:100]}")
            return False
    except Exception as e:
        logger.error(f"✗ {subject}: {e}")
        return False

def create_kafka_topic(bootstrap_servers: str, topic_name: str, partitions: int = 3, replication_factor: int = 1) -> bool:
    """Create a Kafka topic using AdminClient."""
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        from confluent_kafka import KafkaError

        admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

        # Check if topic exists
        metadata = admin_client.list_topics(timeout=5)
        if topic_name in metadata.topics:
            logger.info(f"→ {topic_name} already exists (skipping)")
            return True

        # Create topic
        new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=replication_factor)
        fs = admin_client.create_topics([new_topic], operation_timeout=5)

        for topic, f in fs.items():
            try:
                f.result(timeout=5)
                logger.info(f"✓ Created topic {topic}")
                return True
            except KafkaError as e:
                logger.error(f"✗ Failed to create {topic}: {e}")
                return False
    except ImportError:
        logger.warning("confluent_kafka not available; skipping topic creation (schemas will still be registered)")
        return False
    except Exception as e:
        logger.error(f"✗ Topic creation failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Register T-Life Kafka topics and schemas")
    parser.add_argument("--sr-url", default=SCHEMA_REGISTRY_URL, help="Schema Registry URL")
    parser.add_argument("--kafka-bootstrap", default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers")
    parser.add_argument("--partitions", type=int, default=3, help="Number of partitions per topic")
    parser.add_argument("--replication-factor", type=int, default=1, help="Replication factor")
    parser.add_argument("--skip-topic-creation", action="store_true", help="Skip Kafka topic creation (only register schemas)")
    args = parser.parse_args()

    topics = list_all_topics()
    logger.info(f"\nProcessing {len(topics)} T-Life topics...\n")

    created = 0
    registered = 0
    failed = 0

    for topic_name in sorted(topics):
        schema = TLIFE_SCHEMAS[topic_name]

        # Create Kafka topic
        if not args.skip_topic_creation:
            if create_kafka_topic(args.kafka_bootstrap, topic_name, args.partitions, args.replication_factor):
                created += 1
            else:
                failed += 1

        # Register schema in SR
        if register_schema_in_sr(args.sr_url, topic_name, schema):
            registered += 1
        else:
            failed += 1

    logger.info(f"\n{'='*60}")
    logger.info(f"Summary:")
    logger.info(f"  Created topics: {created}/{len(topics)}")
    logger.info(f"  Registered schemas: {registered}/{len(topics)}")
    logger.info(f"  Failed: {failed}")
    logger.info(f"{'='*60}\n")

    return 0 if failed == 0 else 1

if __name__ == "__main__":
    sys.exit(main())

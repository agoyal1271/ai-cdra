import json
import logging
from typing import Any, Optional, List, Dict

logger = logging.getLogger(__name__)

MOCK_TOPICS = {
    "customer-events": {
        "partitions": 6,
        "replication_factor": 3,
        "estimated_messages": 1_450_000,
        "last_message_ts": "2026-05-09T08:31:00Z",
        "mock": True,
    },
    "iot-telemetry": {
        "partitions": 12,
        "replication_factor": 3,
        "estimated_messages": 28_700_000,
        "last_message_ts": "2026-05-09T09:14:52Z",
        "mock": True,
    },
    "member-eligibility-updates": {
        "partitions": 3,
        "replication_factor": 2,
        "estimated_messages": 82_500,
        "last_message_ts": "2026-05-09T07:55:10Z",
        "mock": True,
    },
    "payment-transactions": {
        "partitions": 8,
        "replication_factor": 3,
        "estimated_messages": 5_200_000,
        "last_message_ts": "2026-05-09T09:16:01Z",
        "mock": True,
    },
}

MOCK_MESSAGES = {
    "customer-events": [
        {"customer_id": "C-10293", "event_type": "LOGIN", "timestamp": "2026-05-09T08:30:55Z", "channel": "mobile"},
        {"customer_id": "C-87412", "event_type": "PURCHASE", "timestamp": "2026-05-09T08:31:00Z", "amount": 149.99, "currency": "USD"},
    ],
    "iot-telemetry": [
        {"device_id": "DEV-9921", "temperature": 72.3, "humidity": 45.1, "ts": 1746782092},
        {"device_id": "DEV-0043", "temperature": 68.9, "humidity": 52.8, "ts": 1746782093},
    ],
    "member-eligibility-updates": [
        {"member_id": "M-00142", "plan_code": "PPO_GOLD", "effective_date": "2026-06-01", "status": "ACTIVE"},
    ],
    "payment-transactions": [
        {"txn_id": "TXN-88821", "amount": 250.00, "currency": "USD", "merchant": "ACME Corp", "status": "SETTLED"},
    ],
}


def list_kafka_topics(bootstrap_servers: str = None) -> Dict[str, Any]:
    """Lists all Kafka topics with partition count, replication, and message estimates."""
    from config import kafka_client_config
    try:
        from confluent_kafka.admin import AdminClient
        cfg = kafka_client_config()
        if bootstrap_servers:
            cfg["bootstrap.servers"] = bootstrap_servers
        admin = AdminClient(cfg)
        metadata = admin.list_topics(timeout=5)
        result = {}
        for name, topic in metadata.topics.items():
            if name.startswith("__"):
                continue
            result[name] = {
                "partitions": len(topic.partitions),
                "replication_factor": len(next(iter(topic.partitions.values())).replicas) if topic.partitions else 0,
                "error": str(topic.error) if topic.error else None,
                "mock": False,
            }
        return result
    except Exception as e:
        logger.warning(f"Kafka unavailable ({e}), returning mock topics")
        return MOCK_TOPICS


def sample_kafka_messages(topic: str, num_messages: int = 2, bootstrap_servers: str = None) -> List[dict]:
    """Samples up to num_messages from a Kafka topic to help infer schema."""
    if topic in MOCK_MESSAGES:
        msgs = MOCK_MESSAGES[topic][:num_messages]
        for m in msgs:
            m["_mock"] = True
        return msgs
    try:
        from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
        from config import kafka_client_config
        cfg = {**kafka_client_config(), "group.id": "_scout_sampler", "auto.offset.reset": "earliest"}
        if bootstrap_servers:
            cfg["bootstrap.servers"] = bootstrap_servers
        consumer = Consumer(cfg)
        consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])
        messages = []
        for _ in range(num_messages * 3):
            msg = consumer.poll(1.0)
            if msg is None:
                break
            if msg.error():
                continue
            raw = msg.value()
            try:
                from tools.kafka.schema_registry import decode_avro_message, SR_URL
                if raw and len(raw) >= 5 and raw[0] == 0x00 and SR_URL:
                    parsed = decode_avro_message(raw)
                else:
                    parsed = json.loads(raw)
            except Exception:
                parsed = {"_raw": raw.decode("utf-8", errors="replace")}
            messages.append(parsed)
            if len(messages) >= num_messages:
                break
        consumer.close()
        return messages
    except Exception as e:
        logger.warning(f"Could not sample topic {topic}: {e}")
        return []


def get_consumer_group_lag(group_id: str, bootstrap_servers: str = None) -> Dict[str, Any]:
    """Gets consumer group lag per topic-partition."""
    try:
        from confluent_kafka.admin import AdminClient
        from config import kafka_client_config
        cfg = kafka_client_config()
        if bootstrap_servers:
            cfg["bootstrap.servers"] = bootstrap_servers
        admin = AdminClient(cfg)
        groups = admin.list_groups(timeout=8)
        matched = [g for g in groups if g.id == group_id]
        if not matched:
            return {"error": f"Group '{group_id}' not found"}
        return {"group_id": group_id, "state": matched[0].state, "members": len(matched[0].members)}
    except Exception as e:
        logger.warning(f"Consumer group check failed: {e}")
        return {
            "group_id": group_id,
            "state": "Stable",
            "total_lag": 12_430,
            "partitions": {"customer-events-0": 1200, "customer-events-1": 980, "customer-events-2": 1050},
            "mock": True,
        }


def get_all_topics_from_schema_registry() -> dict[str, dict]:
    """
    Returns every topic registered in the SR cache as {topic_name: sr_info_dict}.
    If cache is empty, triggers background indexing (returns empty dict immediately, cache fills in background).
    Used by _scan_kafka() so we can enumerate topics without a broker connection.
    """
    try:
        from tools.kafka.schema_registry_cache import search_sql, get_stats

        rows = search_sql("SELECT * FROM sr_schemas ORDER BY topic_name")

        # If cache is empty, trigger background indexing
        if not rows:
            stats = get_stats()
            if stats.get("count", 0) == 0:
                try:
                    import threading
                    from tools.kafka.schema_registry_indexer import run_index
                    threading.Thread(target=run_index, daemon=True).start()
                    logger.info("[kafka] Cache empty, background indexing started")
                except Exception as idx_err:
                    logger.debug(f"[kafka] Failed to trigger background index: {idx_err}")

        result = {}
        for row in rows:
            topic_name = row.get("topic_name") or row.get("schema_name", "")
            if not topic_name:
                continue
            fields = json.loads(row.get("fields_json", "[]"))
            result[topic_name] = {
                "fields": fields,
                "schema_type": row.get("schema_type", "avro"),
                "compatibility": row.get("compatibility", ""),
                "description": row.get("description", ""),
                "field_count": row.get("field_count", 0),
                "namespace": row.get("namespace_str", ""),
                "schema_id": row.get("schema_id"),
                "version": row.get("version"),
                "source": "schema_registry",
            }
        return result
    except Exception as e:
        logger.debug(f"SR cache bulk lookup failed: {e}")
        return {}


def get_topic_schema_from_registry(topic_name: str) -> Optional[Dict]:
    """
    Looks up the Schema Registry SQLite cache for the given topic.
    Returns a dict with fields, schema_type, compatibility, etc., or None if not cached.
    Prefers schema_registry over message sampling — no broker I/O required.
    """
    try:
        from tools.kafka.schema_registry_cache import search_sql
        rows = search_sql(
            "SELECT * FROM sr_schemas WHERE LOWER(topic_name) = LOWER(?) OR LOWER(schema_name) = LOWER(?)",
            (topic_name, topic_name),
        )
        if not rows:
            return None
        row = rows[0]
        fields = json.loads(row.get("fields_json", "[]"))
        return {
            "fields": fields,
            "schema_type": row.get("schema_type", "avro"),
            "compatibility": row.get("compatibility", ""),
            "description": row.get("description", ""),
            "field_count": row.get("field_count", 0),
            "namespace": row.get("namespace_str", ""),
            "schema_id": row.get("schema_id"),
            "version": row.get("version"),
            "source": "schema_registry",
        }
    except Exception as e:
        logger.debug(f"SR cache lookup failed for {topic_name!r}: {e}")
        return None


def infer_kafka_schema(messages: List[dict]) -> Dict[str, Any]:
    """Infers schema from sampled messages by inspecting field types."""
    if not messages:
        return {"fields": [], "format": "unknown"}
    clean = [m for m in messages if not m.get("_raw") and not m.get("_mock") == True or len(m) > 1]
    if not clean:
        clean = messages
    all_keys: dict[str, set] = {}
    for msg in clean:
        for k, v in msg.items():
            if k.startswith("_"):
                continue
            all_keys.setdefault(k, set()).add(type(v).__name__)
    fields = [{"name": k, "types": list(v)} for k, v in all_keys.items()]
    return {"fields": fields, "format": "json", "sample_count": len(messages)}

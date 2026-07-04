"""Demo service-alert injector.

Publishes a fake ServiceAlert to the service-alerts topic using the shared
Avro schema and Schema Registry, so the downstream pipeline can be exercised
on demand even when the real TTC alerts feed is quiet. Every field of the
alert body can be overridden via environment variables for different demo
scenarios.
"""

import os
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

TOPIC = "service-alerts"
SUBJECT = "service-alerts-value"
SCHEMA_FILE = os.environ.get("SCHEMA_FILE", "/app/schemas/service_alert.avsc")

# Sample Line 1 station stop ids (demo placeholders; override with real GTFS
# stop ids via ALERT_STOPS when accuracy matters).
DEFAULT_STOPS = "14404,14406,14408"


def csv_env(name, default):
    return [item.strip() for item in os.environ.get(name, default).split(",") if item.strip()]


def main():
    broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
    registry_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")

    now = int(time.time())
    alert = {
        "alert_id": os.environ.get("ALERT_ID", f"DEMO-{now}"),
        "effect": os.environ.get("ALERT_EFFECT", "NO_SERVICE"),
        "cause": os.environ.get("ALERT_CAUSE") or None,
        "header_text": os.environ.get(
            "ALERT_HEADER", "Line 1: No service between Bloor-Yonge and St George"
        ),
        "description_text": os.environ.get("ALERT_DESCRIPTION") or None,
        "affected_route_ids": csv_env("ALERT_ROUTES", "1"),
        "affected_stop_ids": csv_env("ALERT_STOPS", DEFAULT_STOPS),
        "active_since": int(os.environ.get("ALERT_ACTIVE_SINCE", now)),
        "timestamp": now,
    }

    with open(SCHEMA_FILE, encoding="utf-8") as f:
        schema_str = f.read()

    registry = SchemaRegistryClient({"url": registry_url})
    serializer = AvroSerializer(registry, schema_str)

    producer = Producer({"bootstrap.servers": broker})
    payload = serializer(alert, SerializationContext(TOPIC, MessageField.VALUE))
    producer.produce(TOPIC, key=alert["alert_id"].encode(), value=payload)
    producer.flush(10)

    print(f"injected demo alert {alert['alert_id']}: "
          f"effect={alert['effect']} routes={alert['affected_route_ids']} "
          f"header={alert['header_text']!r}")


if __name__ == "__main__":
    main()

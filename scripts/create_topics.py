"""
create_topics.py – Create all iiitd_ Kafka topics with the correct settings.

Skips topics that already exist.

Topic configuration (from master plan and tech.md):
  - telemetry_raw:       partitions=12, retention=1h
  - telemetry_clean:     partitions=12, retention=2h
  - telemetry_dlq:       partitions=4,  retention=24h
  - road_events:         partitions=12, retention=2h
  - hazards_state:       partitions=4,  cleanup.policy=compact
  - vehicles_reputation: partitions=4,  cleanup.policy=compact
  - hazards_critical:    partitions=4,  retention=24h
  - hazards_minor:       partitions=4,  retention=24h
  - city_tickets:        partitions=4,  retention=7d
  - ui_events:           partitions=4,  retention=1h
  - sim_control:         partitions=1,  retention=1h

Usage:
  python scripts/create_topics.py

Requires env vars: KAFKA_BOOTSTRAP_SERVERS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD
(same as the pipeline).
"""

from __future__ import annotations

import os
import sys
from typing import Optional

from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
from confluent_kafka import KafkaException


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        print(f"ERROR: env var {name!r} is required", file=sys.stderr)
        sys.exit(1)
    return val


def _get(name: str, default: str) -> str:
    return os.getenv(name, default)


def main() -> None:
    bootstrap = _require("KAFKA_BOOTSTRAP_SERVERS")
    username = _require("KAFKA_SASL_USERNAME")
    password = _require("KAFKA_SASL_PASSWORD")
    protocol = _get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    mechanism = _get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    prefix = _get("RS_TOPIC_PREFIX", "iiitd_")

    admin_conf = {
        "bootstrap.servers": bootstrap,
        "security.protocol": protocol,
        "sasl.mechanism": mechanism,
        "sasl.username": username,
        "sasl.password": password,
    }

    admin = AdminClient(admin_conf)

    # Discover existing topics
    meta = admin.list_topics(timeout=15)
    existing = set(meta.topics.keys())

    _HOUR_MS = str(60 * 60 * 1000)
    _DAY_MS = str(24 * 60 * 60 * 1000)
    _WEEK_MS = str(7 * 24 * 60 * 60 * 1000)

    # (suffix, partitions, config_dict)
    topic_specs: list[tuple[str, int, dict]] = [
        ("telemetry_raw",        12, {"retention.ms": _HOUR_MS}),
        ("telemetry_clean",      12, {"retention.ms": str(2 * 60 * 60 * 1000)}),
        ("telemetry_dlq",         4, {"retention.ms": _DAY_MS}),
        ("road_events",          12, {"retention.ms": str(2 * 60 * 60 * 1000)}),
        ("hazards_state",         4, {"cleanup.policy": "compact",
                                      "min.cleanable.dirty.ratio": "0.1",
                                      "segment.ms": str(60 * 60 * 1000)}),
        ("vehicles_reputation",   4, {"cleanup.policy": "compact",
                                      "min.cleanable.dirty.ratio": "0.1",
                                      "segment.ms": str(60 * 60 * 1000)}),
        ("hazards_critical",      4, {"retention.ms": _DAY_MS}),
        ("hazards_minor",         4, {"retention.ms": _DAY_MS}),
        ("city_tickets",          4, {"retention.ms": _WEEK_MS}),
        ("ui_events",             4, {"retention.ms": _HOUR_MS}),
        ("sim_control",           1, {"retention.ms": _HOUR_MS}),
    ]

    to_create: list[NewTopic] = []
    for suffix, partitions, config in topic_specs:
        full_name = f"{prefix}{suffix}"
        if full_name in existing:
            print(f"  SKIP   {full_name!r} (already exists)")
            continue
        to_create.append(NewTopic(
            full_name,
            num_partitions=partitions,
            replication_factor=1,
            config=config,
        ))

    if not to_create:
        print("All topics already exist.")
        return

    print(f"Creating {len(to_create)} topic(s)…")
    futures = admin.create_topics(to_create)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"  OK     {topic!r}")
        except KafkaException as exc:
            # Topic already exists is OK
            if "TopicExistsException" in str(type(exc)) or "already exists" in str(exc).lower():
                print(f"  SKIP   {topic!r} (already exists)")
            else:
                print(f"  ERROR  {topic!r}: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()

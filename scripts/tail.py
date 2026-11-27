"""
tail.py <topic> [--n N] – Print the last N messages of a Kafka topic.

Usage:
  python scripts/tail.py iiitd_telemetry_raw
  python scripts/tail.py iiitd_hazards_state --n 20

Env vars: KAFKA_BOOTSTRAP_SERVERS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import deque
from typing import Optional

import orjson
from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING, OFFSET_END


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        print(f"ERROR: env var {name!r} is required", file=sys.stderr)
        sys.exit(1)
    return val


def _get(name: str, default: str) -> str:
    return os.getenv(name, default)


def tail_topic(topic: str, n: int = 10) -> None:
    bootstrap = _require("KAFKA_BOOTSTRAP_SERVERS")
    username = _require("KAFKA_SASL_USERNAME")
    password = _require("KAFKA_SASL_PASSWORD")
    protocol = _get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    mechanism = _get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")

    conf = {
        "bootstrap.servers": bootstrap,
        "security.protocol": protocol,
        "sasl.mechanism": mechanism,
        "sasl.username": username,
        "sasl.password": password,
        "group.id": f"iiitd_cg_tail_{int(time.time())}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(conf)

    # Discover partitions
    meta = consumer.list_topics(topic=topic, timeout=10)
    if topic not in meta.topics:
        print(f"ERROR: topic {topic!r} not found", file=sys.stderr)
        consumer.close()
        sys.exit(1)

    partitions = list(meta.topics[topic].partitions.keys())
    tps = [TopicPartition(topic, p) for p in partitions]

    # Get HWM
    hwms: dict[int, int] = {}
    for tp in tps:
        _, hwm = consumer.get_watermark_offsets(tp, timeout=5)
        hwms[tp.partition] = hwm

    # Seek to max(0, hwm - n) per partition
    per_part = max(1, n // max(len(partitions), 1))
    start_tps: list[TopicPartition] = []
    for tp in tps:
        hwm = hwms[tp.partition]
        start = max(0, hwm - per_part)
        start_tps.append(TopicPartition(topic, tp.partition, start))

    consumer.assign(start_tps)

    messages: deque = deque()
    eof_count = 0
    total_partitions = len(partitions)

    print(f"Reading last ~{n} messages from {topic!r}…\n")

    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                eof_count += 1
                if eof_count >= total_partitions:
                    break
            else:
                print(f"ERROR: {msg.error()}", file=sys.stderr)
            continue
        messages.append(msg)
        if len(messages) > n * 2:
            messages.popleft()

    consumer.close()

    # Sort by timestamp and show last n
    sorted_msgs = sorted(messages, key=lambda m: m.timestamp()[1] if m.timestamp()[0] > 0 else 0)
    to_show = list(sorted_msgs)[-n:]

    for i, msg in enumerate(to_show):
        key = msg.key().decode("utf-8", errors="replace") if msg.key() else "(no key)"
        ts = msg.timestamp()
        ts_str = str(ts[1]) if ts[0] > 0 else "?"
        print(f"── [{i+1}/{len(to_show)}] partition={msg.partition()} offset={msg.offset()} "
              f"key={key!r} ts={ts_str}")
        try:
            val = orjson.loads(msg.value())
            print(json.dumps(val, indent=2))
        except Exception:
            print(repr(msg.value()[:500]))
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Tail a Kafka topic")
    parser.add_argument("topic", help="Full topic name (e.g. iiitd_telemetry_raw)")
    parser.add_argument("--n", type=int, default=10, help="Number of messages to show (default 10)")
    args = parser.parse_args()
    tail_topic(args.topic, args.n)


if __name__ == "__main__":
    main()

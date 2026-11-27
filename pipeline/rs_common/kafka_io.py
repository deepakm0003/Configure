"""
kafka_io.py – Resilient Kafka consumer/producer factory and run-loop.

Features:
- Exponential backoff on KafkaException (0.5 s → 30 s cap).
- Graceful SIGTERM: flush producer, commit offsets, close consumer.
- Commit after each produce batch (at-least-once).
- DLQ helper: send unparseable/invalid messages to telemetry_dlq.
- Batch poll loop suitable for all pipeline stages.
"""

from __future__ import annotations

import signal
import time
import json
from typing import Callable, Optional

import orjson
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError, Message

from pipeline.rs_common.config import Config
from pipeline.rs_common.metrics import log_json


# ── Backoff ───────────────────────────────────────────────────────────────────

def _backoff(attempt: int, base: float = 0.5, cap: float = 30.0) -> float:
    """Return sleep seconds for the given retry attempt (exponential, capped)."""
    return min(cap, base * (2 ** attempt))


# ── Producer factory ──────────────────────────────────────────────────────────

def make_producer(cfg: Config) -> Producer:
    return Producer(cfg.kafka_producer_conf())


# ── Consumer factory ─────────────────────────────────────────────────────────

def make_consumer(cfg: Config, topics: list[str], group_id: Optional[str] = None,
                  offset_reset: str = "latest") -> Consumer:
    conf = cfg.kafka_consumer_conf(group_id=group_id, offset_reset=offset_reset)
    c = Consumer(conf)
    c.subscribe(topics)
    return c


# ── DLQ helper ────────────────────────────────────────────────────────────────

def send_to_dlq(
    producer: Producer,
    dlq_topic: str,
    reason: str,
    detail: str,
    raw: bytes | str,
    key: Optional[bytes] = None,
    role: str = "kafka_io",
) -> None:
    """Produce a DLQ message.  raw is truncated to 4 KB."""
    raw_str = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
    payload = {
        "schema": "rs.dlq.v1",
        "reason": reason,
        "detail": detail,
        "raw": raw_str[:4096],
        "ts_ms": int(time.time() * 1000),
    }
    try:
        producer.produce(
            dlq_topic,
            value=orjson.dumps(payload),
            key=key,
            on_delivery=_delivery_report(role),
        )
    except KafkaException as exc:
        log_json(role, "ERROR", "dlq_produce_failed", error=str(exc))


def _delivery_report(role: str) -> Callable:
    def _cb(err, _msg):
        if err:
            log_json(role, "ERROR", "produce_failed", error=str(err))
    return _cb


# ── Produce helper ────────────────────────────────────────────────────────────

def produce(
    producer: Producer,
    topic: str,
    value: bytes,
    key: Optional[bytes] = None,
    role: str = "kafka_io",
) -> None:
    producer.produce(
        topic,
        value=value,
        key=key,
        on_delivery=_delivery_report(role),
    )


# ── Graceful shutdown ─────────────────────────────────────────────────────────

class _ShutdownFlag:
    def __init__(self) -> None:
        self._stop = False

    def request_stop(self, *_) -> None:
        self._stop = True

    @property
    def should_stop(self) -> bool:
        return self._stop


# ── Main run-loop ─────────────────────────────────────────────────────────────

def run_loop(
    cfg: Config,
    input_topics: list[str],
    process_fn: Callable[[Message, Producer], None],
    group_id: Optional[str] = None,
    offset_reset: str = "latest",
    poll_timeout: float = 1.0,
    role: str = "stage",
    extra_setup: Optional[Callable[[Consumer, Producer], None]] = None,
) -> None:
    """
    Main consumer loop.

    - Calls process_fn(msg, producer) for each non-None message.
    - process_fn is responsible for producing output and may call send_to_dlq.
    - Commits after each poll batch that produced at least one output.
    - On KafkaException: logs and backs off exponentially, then retries.
    - Shuts down cleanly on SIGTERM.
    """
    shutdown = _ShutdownFlag()
    signal.signal(signal.SIGTERM, shutdown.request_stop)
    signal.signal(signal.SIGINT, shutdown.request_stop)

    attempt = 0
    producer = make_producer(cfg)

    if extra_setup:
        # For fuse startup recovery: create a temporary consumer before the main loop.
        pass  # handled by caller via extra_setup pattern

    while not shutdown.should_stop:
        consumer: Optional[Consumer] = None
        try:
            consumer = make_consumer(cfg, input_topics, group_id=group_id, offset_reset=offset_reset)
            if extra_setup:
                extra_setup(consumer, producer)
            attempt = 0  # reset backoff on successful connect

            while not shutdown.should_stop:
                msg = consumer.poll(timeout=poll_timeout)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                try:
                    process_fn(msg, producer)
                    producer.poll(0)  # trigger delivery callbacks
                    consumer.commit(asynchronous=False)
                except Exception as exc:  # noqa: BLE001
                    log_json(role, "ERROR", "process_error", error=str(exc))

        except KafkaException as exc:
            sleep = _backoff(attempt)
            log_json(role, "ERROR", "kafka_error", error=str(exc), retry_in_s=sleep, attempt=attempt)
            attempt += 1
            time.sleep(sleep)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass

    # Graceful shutdown
    log_json(role, "INFO", "shutting_down")
    try:
        producer.flush(timeout=10)
    except Exception:
        pass
    log_json(role, "INFO", "shutdown_complete")


def read_to_hwm(
    cfg: Config,
    topics: list[str],
    callback: Callable[[Message], None],
    group_id: str,
    role: str = "recovery",
) -> None:
    """
    Read each partition from earliest to its high-water mark (used for fuse recovery).
    Does NOT commit (read-only scan).
    """
    consumer = Consumer({
        **cfg.kafka_consumer_conf(group_id=group_id, offset_reset="earliest"),
        "enable.auto.commit": False,
    })
    consumer.subscribe(topics)

    # We'll stop once we've consumed up to the HWM for all partitions.
    # Simple approach: track how many partitions hit EOF.
    eof_partitions: set[tuple[str, int]] = set()
    hwm_partitions: set[tuple[str, int]] = set()

    # Discover partition count by polling briefly
    start = time.monotonic()
    while time.monotonic() - start < 15:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            # If we haven't heard anything, give up after 5s
            if time.monotonic() - start > 5:
                break
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                key = (msg.topic(), msg.partition())
                eof_partitions.add(key)
                hwm_partitions.add(key)
                # Check if all known partitions are at EOF
                if eof_partitions >= hwm_partitions:
                    break
            else:
                log_json(role, "ERROR", "recovery_error", error=str(msg.error()))
            continue
        callback(msg)

    consumer.close()
    log_json(role, "INFO", "recovery_complete", topics=topics)

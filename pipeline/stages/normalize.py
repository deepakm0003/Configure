"""
normalize.py – rs-normalize stage.

Input:  telemetry.raw  (key: vehicle_id)
Output: telemetry.clean (key: vehicle_id)
DLQ:    telemetry.dlq   (key: vehicle_id or b"unknown")

Stateless except for a small per-vehicle LRU (last 64 seq numbers + last good fix).

Algorithm (algorithms.md § B):
1. Parse and validate (pydantic).
2. Range checks: lat/lon inside bbox, speed 0–200, |az| ≤ 6 g.
3. Lateness: ts_ms < now − MAX_LATENESS_S*1000 → LATE_EVENT.
   Clock skew:  ts_ms > now + 5000 → CLOCK_SKEW.
4. Duplicate: per-vehicle LRU of last 64 seq → DUPLICATE.
5. Teleport: implied speed > 200 km/h vs last fix → IMPOSSIBLE_JUMP.
6. Enrich: h3_r11, h3_r9, quality.speed_mismatch.
7. Produce to telemetry.clean, stamp trace.normalize_ms.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Optional

import orjson
from confluent_kafka import Message, Producer

from pipeline.rs_common.config import Config
from pipeline.rs_common import geo
from pipeline.rs_common.kafka_io import send_to_dlq, produce, run_loop
from pipeline.rs_common.metrics import StageMetrics, log_json
from pipeline.rs_common.models import TelemetryRaw, TelemetryClean, Quality, Trace

ROLE = "normalize"

# ── Per-vehicle LRU state ─────────────────────────────────────────────────────

@dataclass
class _VehicleState:
    seq_cache: OrderedDict[int, None] = field(default_factory=OrderedDict)  # LRU of last 64 seqs
    last_lat: Optional[float] = None
    last_lon: Optional[float] = None
    last_ts_ms: Optional[int] = None

    def seen_seq(self, seq: int) -> bool:
        return seq in self.seq_cache

    def add_seq(self, seq: int) -> None:
        if seq in self.seq_cache:
            self.seq_cache.move_to_end(seq)
        else:
            self.seq_cache[seq] = None
            if len(self.seq_cache) > 64:
                self.seq_cache.popitem(last=False)

    def update_fix(self, lat: float, lon: float, ts_ms: int) -> None:
        self.last_lat = lat
        self.last_lon = lon
        self.last_ts_ms = ts_ms


# ── Pure normalisation logic ──────────────────────────────────────────────────

def normalise(
    raw_bytes: bytes,
    vehicle_states: dict[str, _VehicleState],
    now_ms: int,
    max_lateness_s: float,
    bbox_lat_min: float,
    bbox_lat_max: float,
    bbox_lon_min: float,
    bbox_lon_max: float,
) -> tuple[Optional[TelemetryClean], Optional[tuple[str, str]]]:
    """
    Pure function: returns (clean_msg, None) on success, or (None, (reason, detail)) on rejection.

    Does NOT touch Kafka.
    """
    # 1. Parse
    try:
        data = orjson.loads(raw_bytes)
        msg = TelemetryRaw.model_validate(data)
    except Exception as exc:
        return None, ("SCHEMA_INVALID", str(exc)[:200])

    vehicle_id = msg.vehicle_id
    state = vehicle_states.setdefault(vehicle_id, _VehicleState())

    lat = msg.gps.lat
    lon = msg.gps.lon
    speed = msg.gps.speed_kmh
    az_max = msg.imu.az_max_g
    az_min = msg.imu.az_min_g

    # 2. Range checks
    if not (bbox_lat_min <= lat <= bbox_lat_max and bbox_lon_min <= lon <= bbox_lon_max):
        return None, ("OUT_OF_RANGE", f"lat={lat} lon={lon} outside bbox")
    if not (0 <= speed <= 200):
        return None, ("OUT_OF_RANGE", f"speed={speed} km/h out of range")
    if abs(az_max) > 6 or abs(az_min) > 6:
        return None, ("OUT_OF_RANGE", f"az_max={az_max} az_min={az_min} out of range")

    # 3. Timestamp checks
    if msg.ts_ms < now_ms - max_lateness_s * 1000:
        return None, ("LATE_EVENT", f"ts_ms={msg.ts_ms} lateness={(now_ms - msg.ts_ms)/1000:.1f}s")
    if msg.ts_ms > now_ms + 5000:
        return None, ("CLOCK_SKEW", f"ts_ms={msg.ts_ms} ahead by {(msg.ts_ms - now_ms)/1000:.1f}s")

    # 4. Duplicate check
    if state.seen_seq(msg.seq):
        return None, ("DUPLICATE", f"seq={msg.seq} already seen")
    state.add_seq(msg.seq)

    # 5. Teleport check (do NOT update last fix on failure)
    if state.last_lat is not None and state.last_ts_ms is not None:
        dt_s = (msg.ts_ms - state.last_ts_ms) / 1000.0
        if dt_s > 0:
            dist_m = geo.haversine(state.last_lat, state.last_lon, lat, lon)
            implied_kmh = (dist_m / dt_s) * 3.6
            if implied_kmh > 200:
                return None, ("IMPOSSIBLE_JUMP", f"implied speed {implied_kmh:.0f} km/h")

    # Accept: update last fix
    state.update_fix(lat, lon, msg.ts_ms)

    # 6. Enrich
    h3_r11 = geo.latlng_to_cell(lat, lon, 11)
    h3_r9 = geo.latlng_to_cell(lat, lon, 9)
    speed_mismatch = abs(speed - msg.can.wheel_speed_kmh) > 25

    # 7. Build clean message
    clean_data = msg.model_dump(by_alias=True)
    clean_data["schema"] = "rs.telemetry.clean.v1"
    clean_data["h3_r11"] = h3_r11
    clean_data["h3_r9"] = h3_r9
    clean_data["quality"] = {"speed_mismatch": speed_mismatch}
    clean_data["trace"]["normalize_ms"] = now_ms

    clean = TelemetryClean.model_validate(clean_data)
    return clean, None


# ── Kafka runner ──────────────────────────────────────────────────────────────

def run(cfg: Config) -> None:
    vehicle_states: dict[str, _VehicleState] = {}
    metrics = StageMetrics(ROLE)

    input_topic = cfg.topics.stage_input(cfg.topics.telemetry_raw)
    output_topic = cfg.topics.stage_output(cfg.topics.telemetry_clean)
    dlq_topic = cfg.topics.telemetry_dlq

    log_json(ROLE, "INFO", "starting", input=input_topic, output=output_topic, dlq=dlq_topic)

    def process(msg: Message, producer: Producer) -> None:
        now_ms = int(time.time() * 1000)
        metrics.record_in()

        raw = msg.value()
        key = msg.key()
        vehicle_key = key if key else b"unknown"

        clean, rejection = normalise(
            raw_bytes=raw,
            vehicle_states=vehicle_states,
            now_ms=now_ms,
            max_lateness_s=cfg.max_lateness_s,
            bbox_lat_min=cfg.bbox_lat_min,
            bbox_lat_max=cfg.bbox_lat_max,
            bbox_lon_min=cfg.bbox_lon_min,
            bbox_lon_max=cfg.bbox_lon_max,
        )

        if rejection is not None:
            reason, detail = rejection
            send_to_dlq(producer, dlq_topic, reason, detail, raw or b"", key=vehicle_key, role=ROLE)
            log_json(ROLE, "DEBUG", "dlq", reason=reason, detail=detail)
            return

        assert clean is not None
        payload = orjson.dumps(clean.model_dump(by_alias=True))
        produce(producer, output_topic, payload, key=clean.vehicle_id.encode(), role=ROLE)
        metrics.record_out()

        emitted = clean.trace.emitted_ms
        stage_ms = clean.trace.normalize_ms
        metrics.record_latency(emitted, stage_ms)
        metrics.maybe_log()

    run_loop(
        cfg=cfg,
        input_topics=[input_topic],
        process_fn=process,
        role=ROLE,
        offset_reset="latest",
    )

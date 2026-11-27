"""
alert.py – rs-alert stage.

Inputs:  hazards.state       (key: hazard_id)
         telemetry.clean     (key: vehicle_id)
Output:  ui.events           (key: vehicle_id or "broadcast")

Algorithm (algorithms.md § E):
  - Keep active hazards (CONFIRMED or DECAYING, not SPEED_BREAKER) indexed by res-9 cell.
  - For each clean telemetry tick with speed ≥ 5 km/h:
      Candidates = hazards in grid_disk(vehicle_r9, 2).
      For each: compute distance + angle; alert if ahead and within RS_ALERT_RADIUS_M.
      Level: d ≤ 200 → warning, else advisory.
      Cooldown per (vehicle, hazard): 300 s, with one escalation.
  - Track alert outcomes: when vehicle comes within 20 m of hazard, emit alert_outcome.
  - Forward hazard_update and reputation_update events to ui.events.
  - Every 1 s emit vehicle_positions; every 5 s emit pipeline_stats.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional
from collections import deque

import orjson
from confluent_kafka import Consumer, Message, Producer, KafkaError, KafkaException

from pipeline.rs_common.config import Config
from pipeline.rs_common import geo
from pipeline.rs_common.kafka_io import make_producer, make_consumer, produce, _backoff
from pipeline.rs_common.metrics import StageMetrics, log_json
from pipeline.rs_common.models import (
    HazardState, TelemetryClean, VehicleReputation, Trace,
    DriverAlert, HazardUpdate, AlertOutcome, ReputationUpdate,
    VehiclePositions, VehiclePosition, PipelineStats, LatencyStats,
)

ROLE = "alert"

_COOLDOWN_S = 300.0
_WARNING_DIST_M = 200.0
_POSITION_EMIT_S = 1.0
_STATS_EMIT_S = 5.0
_POSITION_WINDOW_S = 10.0

_HUMAN_TYPES = {
    "POTHOLE": "Pothole",
    "WATERLOGGING": "Waterlogging",
    "SLIPPERY": "Slippery road",
    "HARSH_BRAKE": "Sudden-braking zone",
}


@dataclass
class _AlertCooldown:
    last_level: str = ""
    last_ts: float = 0.0
    escalated: bool = False


@dataclass
class _PendingOutcome:
    """Track a vehicle that received an alert, waiting to reach the hazard."""
    vehicle_id: str
    hazard_id: str
    speed_at_alert: float
    hazard_lat: float
    hazard_lon: float


@dataclass
class AlertState:
    # active hazards by hazard_id
    hazards: dict[str, HazardState] = field(default_factory=dict)
    # cooldown per (vehicle_id, hazard_id)
    cooldowns: dict[tuple[str, str], _AlertCooldown] = field(default_factory=dict)
    # latest vehicle positions (last 10 s)
    vehicle_positions: dict[str, VehiclePosition] = field(default_factory=dict)
    vehicle_last_seen: dict[str, float] = field(default_factory=dict)
    # pending alert outcomes
    pending_outcomes: list[_PendingOutcome] = field(default_factory=list)
    # latency samples for pipeline_stats
    latency_samples: deque = field(default_factory=lambda: deque(maxlen=10000))


# ── Pure alert logic ──────────────────────────────────────────────────────────

def evaluate_alerts(
    clean: TelemetryClean,
    state: AlertState,
    alert_radius_m: float,
    now_ms: int,
) -> list[DriverAlert]:
    """Evaluate all hazards for a vehicle's current position. Returns alerts to emit."""
    alerts: list[DriverAlert] = []
    speed = clean.gps.speed_kmh
    if speed < 5:
        return alerts

    r9 = clean.h3_r9
    vehicle_id = clean.vehicle_id
    lat = clean.gps.lat
    lon = clean.gps.lon
    heading = clean.gps.heading_deg

    candidates = set()
    disk = geo.cell_disk(r9, 2)
    for hid, hz in list(state.hazards.items()):
        hz_r9 = geo.latlng_to_cell(hz.lat, hz.lon, 9)
        if hz_r9 in disk:
            candidates.add(hid)

    for hid in candidates:
        hz = state.hazards.get(hid)
        if hz is None:
            continue
        if hz.hazard_type == "SPEED_BREAKER":
            continue
        if hz.status not in ("CONFIRMED", "DECAYING"):
            continue

        extra_dist = hz.radius_m if hz.hazard_type == "WATERLOGGING" else 0.0
        ahead, dist_m, _brg = geo.is_ahead(
            lat, lon, heading,
            hz.centroid.lat, hz.centroid.lon,
            max_angle_deg=30.0,
            max_dist_m=alert_radius_m,
            extra_dist_m=extra_dist,
        )
        if not ahead:
            continue

        level = "warning" if dist_m <= _WARNING_DIST_M else "advisory"

        # Cooldown check
        ck = state.cooldowns.get((vehicle_id, hid))
        now_m = time.monotonic()
        if ck:
            elapsed = now_m - ck.last_ts
            if elapsed < _COOLDOWN_S:
                # Allow one escalation: advisory → warning
                if ck.last_level == "advisory" and level == "warning" and not ck.escalated:
                    ck.escalated = True
                else:
                    continue
            # Reset cooldown
            ck.last_level = level
            ck.last_ts = now_m
            ck.escalated = False
        else:
            state.cooldowns[(vehicle_id, hid)] = _AlertCooldown(
                last_level=level, last_ts=now_m, escalated=False
            )

        eta_s = (dist_m / max(speed * 1000 / 3600, 0.1)) if speed > 0 else 0.0
        human_type = _HUMAN_TYPES.get(hz.hazard_type, hz.hazard_type.capitalize())
        adv_speed = hz.advisory_speed_kmh
        msg_text = f"{human_type} in {int(dist_m)} m"
        if adv_speed:
            msg_text += f" - slow to {adv_speed} km/h"

        alert_id = f"{vehicle_id}:{hz.hazard_type}:{hz.h3_cell}:{level}"
        alert = DriverAlert(
            alert_id=alert_id,
            vehicle_id=vehicle_id,
            hazard_id=hid,
            hazard_type=hz.hazard_type,
            severity_label=hz.severity_label,
            level=level,
            distance_m=round(dist_m, 1),
            eta_s=round(eta_s, 1),
            advisory_speed_kmh=adv_speed,
            message=msg_text,
            ts_ms=now_ms,
            trace=Trace(
                emitted_ms=clean.trace.emitted_ms,
                normalize_ms=clean.trace.normalize_ms,
                alert_ms=now_ms,
            ),
        )
        alerts.append(alert)

        # Track for outcome
        state.pending_outcomes.append(_PendingOutcome(
            vehicle_id=vehicle_id,
            hazard_id=hid,
            speed_at_alert=speed,
            hazard_lat=hz.centroid.lat,
            hazard_lon=hz.centroid.lon,
        ))

    return alerts


def check_outcomes(
    clean: TelemetryClean,
    state: AlertState,
) -> list[AlertOutcome]:
    """Check if the vehicle has reached a hazard it was alerted about."""
    outcomes: list[AlertOutcome] = []
    lat = clean.gps.lat
    lon = clean.gps.lon
    vehicle_id = clean.vehicle_id

    remaining = []
    for po in state.pending_outcomes:
        if po.vehicle_id != vehicle_id:
            remaining.append(po)
            continue
        d = geo.haversine(lat, lon, po.hazard_lat, po.hazard_lon)
        if d <= 20.0:
            outcomes.append(AlertOutcome(
                vehicle_id=vehicle_id,
                hazard_id=po.hazard_id,
                speed_at_alert_kmh=po.speed_at_alert,
                speed_at_hazard_kmh=clean.gps.speed_kmh,
            ))
        else:
            remaining.append(po)
    state.pending_outcomes = remaining
    return outcomes


# ── Kafka runner ──────────────────────────────────────────────────────────────

def run(cfg: Config) -> None:
    state = AlertState()
    metrics = StageMetrics(ROLE)

    hazards_topic = cfg.topics.hazards_state
    rep_topic = cfg.topics.vehicles_reputation
    clean_topic = cfg.topics.telemetry_clean
    ui_topic = cfg.topics.stage_output(cfg.topics.ui_events)

    log_json(ROLE, "INFO", "starting", ui_out=ui_topic)

    producer = make_producer(cfg)

    def _ui(payload_dict: dict, key: str = "broadcast") -> None:
        produce(producer, ui_topic, orjson.dumps(payload_dict), key=key.encode(), role=ROLE)
        metrics.record_out()

    last_positions_emit = [time.monotonic()]
    last_stats_emit = [time.monotonic()]

    # Latency samples for pipeline_stats
    latency_buf: deque = deque(maxlen=10000)

    def _percentile(buf: deque, p: float) -> float:
        if not buf:
            return 0.0
        vals = sorted(buf)
        idx = (p / 100) * (len(vals) - 1)
        lo = int(idx)
        hi = min(lo + 1, len(vals) - 1)
        return vals[lo] + (vals[hi] - vals[lo]) * (idx - lo)

    def _maybe_emit_positions(now_ms: int) -> None:
        now_m = time.monotonic()
        if now_m - last_positions_emit[0] < _POSITION_EMIT_S:
            return
        last_positions_emit[0] = now_m
        cutoff_m = now_m - _POSITION_WINDOW_S
        # Prune stale vehicles
        stale = [vid for vid, t in state.vehicle_last_seen.items() if t < cutoff_m]
        for vid in stale:
            state.vehicle_positions.pop(vid, None)
            del state.vehicle_last_seen[vid]
        vehicles = list(state.vehicle_positions.values())
        _ui(VehiclePositions(ts_ms=now_ms, vehicles=vehicles).model_dump(), key="broadcast")

    def _maybe_emit_stats(now_ms: int) -> None:
        now_m = time.monotonic()
        if now_m - last_stats_emit[0] < _STATS_EMIT_S:
            return
        last_stats_emit[0] = now_m
        p50 = _percentile(latency_buf, 50)
        p95 = _percentile(latency_buf, 95)
        stats = PipelineStats(
            ts_ms=now_ms,
            msgs_per_s={"alert_in": 0},
            latency_ms=LatencyStats(
                emit_to_normalize_p50=p50,
                emit_to_normalize_p95=p95,
                emit_to_alert_p50=p50,
                emit_to_alert_p95=p95,
            ),
        )
        _ui(stats.model_dump(), key="broadcast")

    import signal as _signal

    _stop = [False]

    def _sigterm(*_):
        _stop[0] = True

    _signal.signal(_signal.SIGTERM, _sigterm)
    _signal.signal(_signal.SIGINT, _sigterm)

    # Subscribe to two topics using a single consumer
    group_id = cfg.consumer_group()
    conf = cfg.kafka_consumer_conf(group_id=group_id, offset_reset="latest")
    conf["enable.auto.commit"] = False

    attempt = 0
    while not _stop[0]:
        consumer: Optional[Consumer] = None
        try:
            consumer = Consumer(conf)
            consumer.subscribe([hazards_topic, rep_topic, clean_topic])
            attempt = 0

            while not _stop[0]:
                msg = consumer.poll(timeout=0.5)
                now_ms = int(time.time() * 1000)
                now_m = time.monotonic()

                _maybe_emit_positions(now_ms)
                _maybe_emit_stats(now_ms)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                topic = msg.topic()
                raw = msg.value()
                metrics.record_in()

                try:
                    if topic == hazards_topic:
                        if raw is None:
                            # tombstone
                            key = msg.key()
                            if key:
                                hid = key.decode("utf-8", errors="replace")
                                state.hazards.pop(hid, None)
                        else:
                            data = orjson.loads(raw)
                            hz = HazardState.model_validate(data)
                            if hz.status in ("CONFIRMED", "DECAYING"):
                                state.hazards[hz.hazard_id] = hz
                            else:
                                state.hazards.pop(hz.hazard_id, None)
                            _ui(HazardUpdate(hazard=hz).model_dump(), key=hz.hazard_id)

                    elif topic == rep_topic:
                        if raw:
                            data = orjson.loads(raw)
                            rep = VehicleReputation.model_validate(data)
                            _ui(ReputationUpdate(reputation=rep).model_dump(), key=rep.vehicle_id)

                    elif topic == clean_topic:
                        data = orjson.loads(raw)
                        clean = TelemetryClean.model_validate(data)

                        # Update position table
                        state.vehicle_positions[clean.vehicle_id] = VehiclePosition(
                            id=clean.vehicle_id,
                            c=clean.vehicle_class,
                            lat=clean.gps.lat,
                            lon=clean.gps.lon,
                            h=clean.gps.heading_deg,
                            s=clean.gps.speed_kmh,
                        )
                        state.vehicle_last_seen[clean.vehicle_id] = now_m

                        alerts = evaluate_alerts(clean, state, cfg.alert_radius_m, now_ms)
                        for alert in alerts:
                            _ui(alert.model_dump(), key=clean.vehicle_id)

                        outcomes = check_outcomes(clean, state)
                        for outcome in outcomes:
                            _ui(outcome.model_dump(), key=clean.vehicle_id)

                        # Latency
                        em = clean.trace.emitted_ms
                        if em > 0:
                            lat_ms = now_ms - em
                            if 0 <= lat_ms < 60000:
                                latency_buf.append(lat_ms)
                                metrics.record_latency(em, now_ms)

                except Exception as exc:
                    log_json(ROLE, "ERROR", "process_error", error=str(exc))

                producer.poll(0)
                consumer.commit(asynchronous=False)
                metrics.maybe_log()

        except KafkaException as exc:
            sleep = _backoff(attempt)
            log_json(ROLE, "ERROR", "kafka_error", error=str(exc), retry_in_s=sleep)
            attempt += 1
            time.sleep(sleep)
        finally:
            if consumer:
                try:
                    consumer.close()
                except Exception:
                    pass

    log_json(ROLE, "INFO", "shutting_down")
    try:
        producer.flush(10)
    except Exception:
        pass
    log_json(ROLE, "INFO", "shutdown_complete")

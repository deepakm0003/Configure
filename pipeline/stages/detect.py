"""
detect.py – rs-detect stage.

Input:  telemetry.clean  (key: vehicle_id → partition affinity)
Output: road.events      (key: h3 res-9 parent cell)

Per-vehicle state (in memory; fine since input is keyed):
  - EWMA mean μ and variance σ² of peak = max(|az_max|, |az_min|)  (α=0.05)
  - Speed ring buffer for the last 6 s
  - 60 s EWMA cruising speed
  - Current-cell accumulator (h3_r11 → detections + speeds)

Algorithm (algorithms.md § C):
  - Bump detection: z = (peak - μ) / max(σ, 0.01).  z ≥ 4.0 and peak ≥ 0.25 g.
  - POTHOLE vs SPEED_BREAKER: pre_decel, min ax, speed.
  - WATERLOGGING, SLIPPERY, HARSH_BRAKE as defined.
  - Emit one observation per cell traversal (on cell change or every 10 s).
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional
import math

import orjson
from confluent_kafka import Message, Producer

from pipeline.rs_common.config import Config
from pipeline.rs_common import geo
from pipeline.rs_common.kafka_io import produce, run_loop
from pipeline.rs_common.metrics import StageMetrics, log_json
from pipeline.rs_common.models import (
    TelemetryClean, RoadEvent, Detection, DetectionEvidence, DetectionType, Trace
)

ROLE = "detect"

_CLASS_FACTOR = {"car": 1.0, "truck": 0.8, "bus": 0.7, "two_wheeler": 1.6}
_EWMA_ALPHA = 0.05


@dataclass
class _SpeedSample:
    ts_ms: int
    speed_kmh: float
    ax_min_g: float


@dataclass
class _CellAccum:
    h3_r11: str
    h3_r9: str
    enter_ms: int
    last_ms: int
    lat: float
    lon: float
    heading_deg: float
    speeds: list[float] = field(default_factory=list)
    detections: list[Detection] = field(default_factory=list)
    vehicle_class: str = "car"


@dataclass
class _VehicleDetectState:
    # EWMA for peak vertical accel (α = 0.05, updated only without detection)
    ewma_mean: float = 0.0
    ewma_var: float = 0.0025   # start with σ²=(0.05)²
    ewma_initialized: bool = False

    # Speed ring buffer (last 6 s)
    speed_buf: deque = field(default_factory=lambda: deque(maxlen=200))  # 200 × 1 s ticks ≫ 6

    # 60 s EWMA cruising speed (α = 1/60)
    cruising_speed: float = 30.0

    # Current cell accumulator
    cell: Optional[_CellAccum] = None

    # Last emit time for 10 s forced flush
    last_emit_ms: int = 0


# ── Pure detection logic ──────────────────────────────────────────────────────

def _severity_bump(peak_g: float, speed_kmh: float, vehicle_class: str) -> float:
    cf = _CLASS_FACTOR.get(vehicle_class, 1.0)
    denom = (0.4 + 0.012 * speed_kmh) * cf
    return float(max(0.0, min(1.0, peak_g / denom if denom > 0 else 0.0)))


def detect_tick(
    msg: TelemetryClean,
    state: _VehicleDetectState,
    z_threshold: float,
) -> list[Detection]:
    """
    Process one telemetry tick.  Returns a (possibly empty) list of detections.
    Does NOT touch Kafka or state.cell.
    """
    detections: list[Detection] = []
    speed = msg.gps.speed_kmh
    az_max = msg.imu.az_max_g
    az_min = msg.imu.az_min_g
    ax_min = msg.imu.ax_min_g
    brake = msg.can.brake_pressure_bar
    abs_a = msg.can.abs_active
    esc_a = msg.can.esc_active
    wiper = msg.can.wiper_level
    vehicle_class = msg.vehicle_class
    ts_ms = msg.ts_ms

    peak = max(abs(az_max), abs(az_min))

    # Update speed buffer
    state.speed_buf.append(_SpeedSample(ts_ms=ts_ms, speed_kmh=speed, ax_min_g=ax_min))

    # Update 60 s EWMA cruising speed (alpha = 1/60)
    alpha_c = 1 / 60.0
    state.cruising_speed = alpha_c * speed + (1 - alpha_c) * state.cruising_speed

    # ── Bump ─────────────────────────────────────────────────────────────────
    if speed >= 10:
        if not state.ewma_initialized:
            state.ewma_mean = peak
            state.ewma_var = 0.0025
            state.ewma_initialized = True
        sigma = math.sqrt(max(state.ewma_var, 0.0001))
        z = (peak - state.ewma_mean) / max(sigma, 0.01)

        if z >= z_threshold and peak >= 0.25:
            # Classify POTHOLE vs SPEED_BREAKER
            # pre_decel: speed 4 s ago minus current speed
            now_ms = ts_ms
            cutoff_ms = now_ms - 4000
            speeds_4s = [s.speed_kmh for s in state.speed_buf if s.ts_ms >= cutoff_ms]
            ax_4s = [s.ax_min_g for s in state.speed_buf if s.ts_ms >= cutoff_ms]

            pre_decel = speeds_4s[0] - speed if speeds_4s else 0.0
            min_ax = min(ax_4s) if ax_4s else 0.0

            if pre_decel >= 12 and min_ax <= -0.12 and speed <= 25:
                det_type: DetectionType = "SPEED_BREAKER"
            else:
                det_type = "POTHOLE"

            severity = _severity_bump(peak, speed, vehicle_class)
            detections.append(Detection(
                type=det_type,
                severity=severity,
                score=round(z, 2),
                lat=msg.gps.lat,
                lon=msg.gps.lon,
                evidence=DetectionEvidence(
                    peak_g=round(peak, 4),
                    speed_kmh=speed,
                    pre_decel_kmh=round(pre_decel, 2),
                ),
            ))
            # Do NOT update EWMA on ticks with detection
        else:
            # Update EWMA only on non-detection ticks
            state.ewma_mean = _EWMA_ALPHA * peak + (1 - _EWMA_ALPHA) * state.ewma_mean
            new_var = (1 - _EWMA_ALPHA) * (state.ewma_var + _EWMA_ALPHA * (peak - state.ewma_mean) ** 2)
            state.ewma_var = new_var

    # ── WATERLOGGING ─────────────────────────────────────────────────────────
    if wiper >= 2 and speed <= 20 and state.cruising_speed > 0:
        ratio = speed / state.cruising_speed
        if ratio <= 0.6:
            severity = 1.0 - ratio
            detections.append(Detection(
                type="WATERLOGGING",
                severity=round(severity, 3),
                score=0.0,
                lat=msg.gps.lat,
                lon=msg.gps.lon,
            ))

    # ── SLIPPERY ─────────────────────────────────────────────────────────────
    if (abs_a or esc_a) and brake < 20 and ax_min > -0.3:
        detections.append(Detection(
            type="SLIPPERY",
            severity=0.6,
            score=0.0,
            lat=msg.gps.lat,
            lon=msg.gps.lon,
        ))

    # ── HARSH_BRAKE ──────────────────────────────────────────────────────────
    # Need speed 1 s ago ≥ 25.
    speeds_1s = [s.speed_kmh for s in state.speed_buf if s.ts_ms >= ts_ms - 1500]
    prev_speed = speeds_1s[0] if len(speeds_1s) >= 2 else speed
    if ax_min <= -0.45 and prev_speed >= 25:
        severity = float(max(0.0, min(1.0, (-ax_min - 0.45) / 0.4 + 0.4)))
        detections.append(Detection(
            type="HARSH_BRAKE",
            severity=round(severity, 3),
            score=0.0,
            lat=msg.gps.lat,
            lon=msg.gps.lon,
            evidence=DetectionEvidence(ax_min_g_as_peak=ax_min),
        ))

    return detections


def build_observation(
    state: _VehicleDetectState,
    exit_ms: int,
) -> Optional[RoadEvent]:
    """Build and return an observation from the current cell accumulator, then clear it."""
    cell = state.cell
    if cell is None:
        return None
    if not cell.speeds:
        return None

    obs = RoadEvent(
        **{
            "schema": "rs.observation.v1",
            "obs_id": f"{cell.vehicle_class}:{cell.enter_ms}",
            "vehicle_id": "",  # caller sets this
            "vehicle_class": cell.vehicle_class,
            "h3_r11": cell.h3_r11,
            "h3_r9": cell.h3_r9,
            "enter_ms": cell.enter_ms,
            "exit_ms": exit_ms,
            "mean_speed_kmh": round(sum(cell.speeds) / len(cell.speeds), 2),
            "heading_deg": cell.heading_deg,
            "lat": cell.lat,
            "lon": cell.lon,
            "detections": cell.detections,
            "trace": Trace(),
        }
    )
    return obs


# ── Kafka runner ──────────────────────────────────────────────────────────────

def run(cfg: Config) -> None:
    vehicle_states: dict[str, _VehicleDetectState] = {}
    metrics = StageMetrics(ROLE)

    input_topic = cfg.topics.stage_input(cfg.topics.telemetry_clean)
    output_topic = cfg.topics.stage_output(cfg.topics.road_events)

    log_json(ROLE, "INFO", "starting", input=input_topic, output=output_topic)

    def _emit_cell(state: _VehicleDetectState, vehicle_id: str, now_ms: int, producer: Producer) -> None:
        obs = build_observation(state, exit_ms=now_ms)
        if obs is None:
            return
        obs.vehicle_id = vehicle_id
        obs.obs_id = f"{vehicle_id}:{state.cell.enter_ms if state.cell else 0}"
        obs.trace.normalize_ms = state.cell.enter_ms if state.cell else 0  # pass through
        obs.trace.detect_ms = now_ms
        payload = orjson.dumps(obs.model_dump(by_alias=True))
        produce(producer, output_topic, payload, key=obs.h3_r9.encode(), role=ROLE)
        metrics.record_out()
        state.cell = None
        state.last_emit_ms = now_ms

    def process(msg: Message, producer: Producer) -> None:
        now_ms = int(time.time() * 1000)
        metrics.record_in()

        raw = msg.value()
        try:
            data = orjson.loads(raw)
            clean = TelemetryClean.model_validate(data)
        except Exception as exc:
            log_json(ROLE, "ERROR", "parse_error", error=str(exc))
            return

        vehicle_id = clean.vehicle_id
        state = vehicle_states.setdefault(vehicle_id, _VehicleDetectState())

        detections = detect_tick(clean, state, cfg.z_threshold)

        h3_r11 = clean.h3_r11
        h3_r9 = clean.h3_r9
        ts_ms = clean.ts_ms

        # Cell tracking
        if state.cell is None:
            # Start a new accumulator
            state.cell = _CellAccum(
                h3_r11=h3_r11,
                h3_r9=h3_r9,
                enter_ms=ts_ms,
                last_ms=ts_ms,
                lat=clean.gps.lat,
                lon=clean.gps.lon,
                heading_deg=clean.gps.heading_deg,
                vehicle_class=clean.vehicle_class,
            )

        cell = state.cell

        # Check for cell change or 10 s forced flush
        cell_changed = (h3_r11 != cell.h3_r11)
        time_exceeded = (ts_ms - cell.enter_ms) >= 10_000

        if cell_changed or time_exceeded:
            _emit_cell(state, vehicle_id, ts_ms, producer)
            # Start fresh accumulator for new cell
            state.cell = _CellAccum(
                h3_r11=h3_r11,
                h3_r9=h3_r9,
                enter_ms=ts_ms,
                last_ms=ts_ms,
                lat=clean.gps.lat,
                lon=clean.gps.lon,
                heading_deg=clean.gps.heading_deg,
                vehicle_class=clean.vehicle_class,
            )
            cell = state.cell

        # Accumulate
        cell.last_ms = ts_ms
        cell.speeds.append(clean.gps.speed_kmh)
        for d in detections:
            # Keep max severity per type
            existing = next((x for x in cell.detections if x.type == d.type), None)
            if existing is None:
                cell.detections.append(d)
            elif d.severity > existing.severity:
                cell.detections.remove(existing)
                cell.detections.append(d)

        metrics.record_latency(clean.trace.emitted_ms, now_ms)
        metrics.maybe_log()

    run_loop(
        cfg=cfg,
        input_topics=[input_topic],
        process_fn=process,
        role=ROLE,
        offset_reset="latest",
    )

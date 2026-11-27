"""
test_detect.py – Unit tests for the pure detect_tick() logic.

Covers:
  - POTHOLE detection (high z-score, no pre-decel)
  - SPEED_BREAKER detection (high z-score + pre-decel + low speed)
  - Clean drive (below threshold) → no detection
  - WATERLOGGING detection
"""

import time
import pytest

from pipeline.stages.detect import detect_tick, _VehicleDetectState, _SpeedSample
from pipeline.rs_common.models import TelemetryClean, GPS, IMU, CAN, Trace, Quality


def _make_clean(
    speed: float = 40.0,
    az_max: float = 0.05,
    az_min: float = -0.04,
    ax_min: float = -0.02,
    wiper: int = 0,
    abs_active: bool = False,
    esc_active: bool = False,
    brake: float = 0.0,
    vehicle_class: str = "car",
    vehicle_id: str = "SIM-CAR-0001",
    seq: int = 1,
) -> TelemetryClean:
    now = int(time.time() * 1000)
    return TelemetryClean(
        **{
            "schema": "rs.telemetry.clean.v1",
            "vehicle_id": vehicle_id,
            "vehicle_class": vehicle_class,
            "seq": seq,
            "ts_ms": now,
            "gps": {"lat": 12.935, "lon": 77.624, "speed_kmh": speed, "heading_deg": 90.0, "hdop": 1.0},
            "imu": {
                "az_max_g": az_max,
                "az_min_g": az_min,
                "az_std_g": 0.03,
                "ax_min_g": ax_min,
                "ax_max_g": 0.02,
                "peak_offset_ms": 500,
            },
            "can": {
                "wheel_speed_kmh": speed,
                "brake_pressure_bar": brake,
                "abs_active": abs_active,
                "esc_active": esc_active,
                "wiper_level": wiper,
                "fog_lamp": False,
                "throttle_pct": 20.0,
            },
            "h3_r11": "8b618920a2dbfff",
            "h3_r9": "89618920a2fffff",
            "quality": {"speed_mismatch": False},
            "trace": {"emitted_ms": now},
        }
    )


def _warm_ewma(state: _VehicleDetectState, n: int = 50, baseline_peak: float = 0.05) -> None:
    """
    Feed n ticks of quiet driving to initialise the EWMA mean/var before the test tick.
    """
    now = int(time.time() * 1000)
    for i in range(n):
        msg = _make_clean(speed=40.0, az_max=baseline_peak, az_min=-baseline_peak, seq=i)
        msg.ts_ms = now - (n - i) * 1000
        detect_tick(msg, state, z_threshold=4.0)


# ── POTHOLE ───────────────────────────────────────────────────────────────────

def test_pothole_detected():
    """
    After a quiet warm-up, a large vertical spike with no pre-braking should → POTHOLE.
    """
    state = _VehicleDetectState()
    _warm_ewma(state, n=50, baseline_peak=0.05)
    # Spike: peak = 0.9 g, speed 40 km/h, no pre-decel
    msg = _make_clean(speed=40.0, az_max=0.9, az_min=-0.05, ax_min=-0.01)
    dets = detect_tick(msg, state, z_threshold=4.0)
    types = [d.type for d in dets]
    assert "POTHOLE" in types


def test_pothole_severity_range():
    state = _VehicleDetectState()
    _warm_ewma(state, n=50, baseline_peak=0.05)
    msg = _make_clean(speed=40.0, az_max=0.9, az_min=-0.05)
    dets = detect_tick(msg, state, z_threshold=4.0)
    for d in dets:
        if d.type == "POTHOLE":
            assert 0.0 <= d.severity <= 1.0


# ── SPEED_BREAKER ─────────────────────────────────────────────────────────────

def test_speed_breaker_detected():
    """
    Pre-decel ≥ 12 km/h over last 4 s, min ax ≤ -0.12 g, speed ≤ 25 → SPEED_BREAKER.
    """
    state = _VehicleDetectState()
    now = int(time.time() * 1000)

    # Seed the speed ring buffer: speed was 38 km/h 4 s ago
    for i in range(4):
        state.speed_buf.append(_SpeedSample(
            ts_ms=now - (5 - i) * 1000,
            speed_kmh=38.0,
            ax_min_g=-0.18,  # strong braking
        ))

    # Warm the EWMA with quiet ticks
    for i in range(30):
        msg = _make_clean(speed=38.0, az_max=0.05, az_min=-0.05, seq=i)
        msg.ts_ms = now - (30 - i) * 1100
        detect_tick(msg, state, z_threshold=4.0)

    # Detection tick: speed now 20 km/h (pre_decel = 38-20 = 18), big bump
    msg = _make_clean(speed=20.0, az_max=0.85, az_min=-0.05, ax_min=-0.18, seq=100)
    dets = detect_tick(msg, state, z_threshold=4.0)
    types = [d.type for d in dets]
    assert "SPEED_BREAKER" in types


# ── CLEAN DRIVE ───────────────────────────────────────────────────────────────

def test_clean_drive_no_detection():
    """Normal driving at 40 km/h with baseline IMU → no detections."""
    state = _VehicleDetectState()
    _warm_ewma(state, n=60, baseline_peak=0.05)
    msg = _make_clean(speed=40.0, az_max=0.06, az_min=-0.05)
    dets = detect_tick(msg, state, z_threshold=4.0)
    types = [d.type for d in dets]
    assert "POTHOLE" not in types
    assert "SPEED_BREAKER" not in types


# ── WATERLOGGING ──────────────────────────────────────────────────────────────

def test_waterlogging_detected():
    """
    wiper_level ≥ 2, speed ≤ 20, speed ≤ 0.6 × cruising_speed → WATERLOGGING.
    """
    state = _VehicleDetectState()
    # Build up cruising speed ≈ 40 km/h
    for i in range(60):
        msg = _make_clean(speed=40.0, seq=i)
        detect_tick(msg, state, z_threshold=4.0)

    # Waterlogging: speed = 8 km/h (≪ 0.6 × 40 = 24), wiper on high
    msg = _make_clean(speed=8.0, wiper=2, seq=100)
    dets = detect_tick(msg, state, z_threshold=4.0)
    types = [d.type for d in dets]
    assert "WATERLOGGING" in types


def test_waterlogging_not_triggered_at_normal_speed():
    """
    wiper on but speed = 40 km/h → NOT waterlogging (speed > 20 km/h condition fails).
    """
    state = _VehicleDetectState()
    for i in range(30):
        msg = _make_clean(speed=40.0, seq=i)
        detect_tick(msg, state, z_threshold=4.0)
    msg = _make_clean(speed=40.0, wiper=2, seq=100)
    dets = detect_tick(msg, state, z_threshold=4.0)
    types = [d.type for d in dets]
    assert "WATERLOGGING" not in types

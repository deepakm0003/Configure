"""
test_alert.py – Unit tests for the pure alert logic.

Covers:
  - Hazard ahead within 500 m → advisory emitted
  - Hazard ahead within 200 m → warning emitted
  - Hazard behind vehicle → no alert
  - Cooldown: same (vehicle, hazard) pair suppressed within 300 s
"""

import time
import math
import pytest

from pipeline.stages.alert import evaluate_alerts, AlertState
from pipeline.rs_common.models import (
    TelemetryClean, HazardState, HazardCentroid, HazardInternal,
    GPS, IMU, CAN, Trace, Quality,
)
from pipeline.rs_common import geo

_LAT = 12.935
_LON = 77.624


def _make_hazard(
    htype: str = "POTHOLE",
    lat: float = _LAT,
    lon: float = _LON,
    hazard_id: str = "POTHOLE:test_cell",
    status: str = "CONFIRMED",
    severity_label: str = "HIGH",
    advisory_speed: int = 20,
    radius_m: float = 15.0,
) -> HazardState:
    cell = geo.latlng_to_cell(lat, lon, 11)
    r9 = geo.latlng_to_cell(lat, lon, 9)
    return HazardState(
        **{
            "schema": "rs.hazard.v1",
            "hazard_id": hazard_id,
            "hazard_type": htype,
            "status": status,
            "confidence": 0.92,
            "severity_score": 0.7,
            "severity_label": severity_label,
            "advisory_speed_kmh": advisory_speed,
            "h3_cell": cell,
            "h3_res": 11,
            "lat": lat,
            "lon": lon,
            "centroid": {"lat": lat, "lon": lon},
            "radius_m": radius_m,
            "distinct_vehicles": 3,
            "positive_reports": 4,
            "clean_passes": 0,
            "reporters": [],
            "first_seen_ms": 0,
            "confirmed_ms": 0,
            "cleared_ms": None,
            "last_update_ms": 0,
            "version": 1,
            "internal": {"log_odds": 2.5},
            "trace": {},
        }
    )


def _make_clean(
    lat: float = _LAT,
    lon: float = _LON,
    heading: float = 90.0,  # heading East
    speed: float = 40.0,
    vehicle_id: str = "SIM-CAR-TEST",
) -> TelemetryClean:
    now = int(time.time() * 1000)
    r11 = geo.latlng_to_cell(lat, lon, 11)
    r9 = geo.latlng_to_cell(lat, lon, 9)
    return TelemetryClean(
        **{
            "schema": "rs.telemetry.clean.v1",
            "vehicle_id": vehicle_id,
            "vehicle_class": "car",
            "seq": 1,
            "ts_ms": now,
            "gps": {"lat": lat, "lon": lon, "speed_kmh": speed, "heading_deg": heading, "hdop": 1.0},
            "imu": {"az_max_g": 0.05, "az_min_g": -0.04, "az_std_g": 0.03,
                    "ax_min_g": -0.02, "ax_max_g": 0.02, "peak_offset_ms": 500},
            "can": {"wheel_speed_kmh": speed, "brake_pressure_bar": 0.0, "abs_active": False,
                    "esc_active": False, "wiper_level": 0, "fog_lamp": False, "throttle_pct": 20.0},
            "h3_r11": r11,
            "h3_r9": r9,
            "quality": {"speed_mismatch": False},
            "trace": {"emitted_ms": now},
        }
    )


def _offset_latlon(lat: float, lon: float, dist_m: float, heading_deg: float) -> tuple[float, float]:
    """Offset a lat/lon by dist_m in the direction of heading_deg."""
    r = math.radians(heading_deg)
    dlat = (dist_m * math.cos(r)) / 111_111.0
    dlon = (dist_m * math.sin(r)) / (111_111.0 * math.cos(math.radians(lat)))
    return lat + dlat, lon + dlon


# ── Ahead, 350 m → advisory ───────────────────────────────────────────────────

def test_ahead_advisory():
    """Hazard 350 m ahead → advisory."""
    state = AlertState()
    # Vehicle heading East; hazard 350 m to the East
    h_lat, h_lon = _offset_latlon(_LAT, _LON, 350, 90.0)
    hz = _make_hazard(lat=h_lat, lon=h_lon)
    state.hazards[hz.hazard_id] = hz

    clean = _make_clean(lat=_LAT, lon=_LON, heading=90.0)
    now_ms = int(time.time() * 1000)
    alerts = evaluate_alerts(clean, state, alert_radius_m=500.0, now_ms=now_ms)

    assert len(alerts) == 1
    assert alerts[0].level == "advisory"
    assert alerts[0].distance_m < 400


# ── Ahead, 150 m → warning ───────────────────────────────────────────────────

def test_ahead_warning():
    """Hazard 150 m ahead → warning."""
    state = AlertState()
    h_lat, h_lon = _offset_latlon(_LAT, _LON, 150, 90.0)
    hz = _make_hazard(lat=h_lat, lon=h_lon)
    state.hazards[hz.hazard_id] = hz

    clean = _make_clean(lat=_LAT, lon=_LON, heading=90.0)
    now_ms = int(time.time() * 1000)
    alerts = evaluate_alerts(clean, state, alert_radius_m=500.0, now_ms=now_ms)

    assert len(alerts) == 1
    assert alerts[0].level == "warning"


# ── Behind vehicle → no alert ─────────────────────────────────────────────────

def test_behind_no_alert():
    """Hazard 300 m BEHIND the vehicle (180° opposite heading) → no alert."""
    state = AlertState()
    # Vehicle heading East (90°); hazard to the West (270°) = behind
    h_lat, h_lon = _offset_latlon(_LAT, _LON, 300, 270.0)
    hz = _make_hazard(lat=h_lat, lon=h_lon)
    state.hazards[hz.hazard_id] = hz

    clean = _make_clean(lat=_LAT, lon=_LON, heading=90.0)  # heading East
    now_ms = int(time.time() * 1000)
    alerts = evaluate_alerts(clean, state, alert_radius_m=500.0, now_ms=now_ms)

    assert len(alerts) == 0, f"Expected no alert for hazard behind, got: {alerts}"


# ── Cooldown ──────────────────────────────────────────────────────────────────

def test_cooldown_suppresses_repeat():
    """Same (vehicle, hazard) pair within 300 s is suppressed (no second alert)."""
    state = AlertState()
    h_lat, h_lon = _offset_latlon(_LAT, _LON, 350, 90.0)
    hz = _make_hazard(lat=h_lat, lon=h_lon)
    state.hazards[hz.hazard_id] = hz

    clean = _make_clean(lat=_LAT, lon=_LON, heading=90.0)
    now_ms = int(time.time() * 1000)

    alerts1 = evaluate_alerts(clean, state, alert_radius_m=500.0, now_ms=now_ms)
    assert len(alerts1) == 1

    # Immediately again (cooldown not expired)
    alerts2 = evaluate_alerts(clean, state, alert_radius_m=500.0, now_ms=now_ms + 1000)
    assert len(alerts2) == 0, "Second alert within cooldown should be suppressed"


# ── Speed breaker filtered out ────────────────────────────────────────────────

def test_speed_breaker_not_alerted():
    """SPEED_BREAKER hazards must never generate driver alerts."""
    state = AlertState()
    h_lat, h_lon = _offset_latlon(_LAT, _LON, 150, 90.0)
    hz = _make_hazard(htype="SPEED_BREAKER", lat=h_lat, lon=h_lon,
                      hazard_id="SPEED_BREAKER:test_cell")
    state.hazards[hz.hazard_id] = hz

    clean = _make_clean(lat=_LAT, lon=_LON, heading=90.0)
    alerts = evaluate_alerts(clean, state, alert_radius_m=500.0, now_ms=int(time.time() * 1000))
    assert len(alerts) == 0

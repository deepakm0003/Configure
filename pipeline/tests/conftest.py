"""
conftest.py – Shared fixtures for all tests.
"""

import time
import pytest
from pipeline.rs_common.models import TelemetryRaw, TelemetryClean, GPS, IMU, CAN, Trace, Quality


def _base_raw(
    vehicle_id: str = "SIM-CAR-0001",
    seq: int = 1,
    lat: float = 12.935,
    lon: float = 77.624,
    speed: float = 40.0,
    az_max: float = 0.05,
    az_min: float = -0.05,
    ax_min: float = -0.02,
    wiper: int = 0,
    abs_active: bool = False,
    esc_active: bool = False,
    brake: float = 0.0,
    wheel_speed: float = 40.0,
) -> dict:
    now = int(time.time() * 1000)
    return {
        "schema": "rs.telemetry.v1",
        "vehicle_id": vehicle_id,
        "vehicle_class": "car",
        "seq": seq,
        "ts_ms": now,
        "gps": {"lat": lat, "lon": lon, "speed_kmh": speed, "heading_deg": 90.0, "hdop": 1.0},
        "imu": {
            "az_max_g": az_max,
            "az_min_g": az_min,
            "az_std_g": 0.03,
            "ax_min_g": ax_min,
            "ax_max_g": 0.02,
            "peak_offset_ms": 500,
        },
        "can": {
            "wheel_speed_kmh": wheel_speed,
            "brake_pressure_bar": brake,
            "abs_active": abs_active,
            "esc_active": esc_active,
            "wiper_level": wiper,
            "fog_lamp": False,
            "throttle_pct": 20.0,
        },
        "trace": {"emitted_ms": now},
    }


@pytest.fixture
def base_raw():
    return _base_raw


@pytest.fixture
def now_ms():
    return int(time.time() * 1000)

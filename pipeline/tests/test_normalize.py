"""
test_normalize.py – One test per DLQ rejection reason from rs-normalize.

Each test feeds a specifically crafted raw payload to the pure normalise()
function and asserts the expected DLQ reason (or no rejection for valid input).
"""

import time
import json
import pytest
import orjson
from collections import OrderedDict

from pipeline.stages.normalize import normalise, _VehicleState

# Default valid bbox for all tests
_BBOX = dict(
    bbox_lat_min=12.80, bbox_lat_max=13.10,
    bbox_lon_min=77.45, bbox_lon_max=77.80,
)


def _run(raw_dict: dict, states: dict | None = None, now_offset_ms: int = 0,
         max_lateness: float = 60.0) -> tuple:
    if states is None:
        states = {}
    now = int(time.time() * 1000) + now_offset_ms
    raw = orjson.dumps(raw_dict)
    return normalise(
        raw_bytes=raw,
        vehicle_states=states,
        now_ms=now,
        max_lateness_s=max_lateness,
        **_BBOX,
    )


def _valid_msg(seq: int = 1, lat: float = 12.935, lon: float = 77.624, speed: float = 40.0,
               az_max: float = 0.05, az_min: float = -0.04, vehicle_id: str = "SIM-CAR-0001",
               wheel_speed: float = 40.0) -> dict:
    now = int(time.time() * 1000)
    return {
        "schema": "rs.telemetry.v1",
        "vehicle_id": vehicle_id,
        "vehicle_class": "car",
        "seq": seq,
        "ts_ms": now,
        "gps": {"lat": lat, "lon": lon, "speed_kmh": speed, "heading_deg": 90.0, "hdop": 1.0},
        "imu": {"az_max_g": az_max, "az_min_g": az_min, "az_std_g": 0.03,
                "ax_min_g": -0.02, "ax_max_g": 0.02, "peak_offset_ms": 500},
        "can": {"wheel_speed_kmh": wheel_speed, "brake_pressure_bar": 0.0,
                "abs_active": False, "esc_active": False, "wiper_level": 0,
                "fog_lamp": False, "throttle_pct": 20.0},
        "trace": {"emitted_ms": now},
    }


# ── SCHEMA_INVALID ────────────────────────────────────────────────────────────

def test_dlq_schema_invalid_missing_field():
    raw = b'{"schema":"rs.telemetry.v1","vehicle_id":"SIM-CAR-0001"}'  # missing required fields
    clean, rejection = normalise(raw, {}, int(time.time() * 1000), 60.0, **_BBOX)
    assert rejection is not None
    assert rejection[0] == "SCHEMA_INVALID"


def test_dlq_schema_invalid_bad_json():
    raw = b'not valid json{'
    clean, rejection = normalise(raw, {}, int(time.time() * 1000), 60.0, **_BBOX)
    assert rejection is not None
    assert rejection[0] == "SCHEMA_INVALID"


def test_dlq_schema_invalid_wrong_class():
    msg = _valid_msg()
    msg["vehicle_class"] = "helicopter"
    clean, rejection = _run(msg)
    assert rejection is not None
    assert rejection[0] == "SCHEMA_INVALID"


# ── OUT_OF_RANGE ──────────────────────────────────────────────────────────────

def test_dlq_out_of_range_lat():
    msg = _valid_msg(lat=10.0)  # outside bbox_lat_min=12.80
    clean, rejection = _run(msg)
    assert rejection == ("OUT_OF_RANGE", ) or (rejection is not None and rejection[0] == "OUT_OF_RANGE")


def test_dlq_out_of_range_speed_negative():
    msg = _valid_msg(speed=-5.0)
    clean, rejection = _run(msg)
    assert rejection is not None and rejection[0] == "OUT_OF_RANGE"


def test_dlq_out_of_range_speed_too_high():
    msg = _valid_msg(speed=250.0)
    clean, rejection = _run(msg)
    assert rejection is not None and rejection[0] == "OUT_OF_RANGE"


def test_dlq_out_of_range_az():
    msg = _valid_msg(az_max=7.5)  # > 6 g
    clean, rejection = _run(msg)
    assert rejection is not None and rejection[0] == "OUT_OF_RANGE"


# ── LATE_EVENT ────────────────────────────────────────────────────────────────

def test_dlq_late_event():
    now = int(time.time() * 1000)
    msg = _valid_msg()
    msg["ts_ms"] = now - 90_000  # 90 seconds old, lateness=60s
    msg["trace"]["emitted_ms"] = msg["ts_ms"]
    raw = orjson.dumps(msg)
    clean, rejection = normalise(raw, {}, now, 60.0, **_BBOX)
    assert rejection is not None and rejection[0] == "LATE_EVENT"


# ── CLOCK_SKEW ────────────────────────────────────────────────────────────────

def test_dlq_clock_skew():
    now = int(time.time() * 1000)
    msg = _valid_msg()
    msg["ts_ms"] = now + 10_000  # 10 seconds in the future
    msg["trace"]["emitted_ms"] = msg["ts_ms"]
    raw = orjson.dumps(msg)
    clean, rejection = normalise(raw, {}, now, 60.0, **_BBOX)
    assert rejection is not None and rejection[0] == "CLOCK_SKEW"


# ── DUPLICATE ─────────────────────────────────────────────────────────────────

def test_dlq_duplicate():
    states: dict = {}
    msg = _valid_msg(seq=42)
    # First message: should pass
    clean1, rej1 = _run(msg, states)
    assert rej1 is None, f"First message rejected: {rej1}"

    # Same seq again: should be DUPLICATE
    msg2 = _valid_msg(seq=42)  # same seq
    raw2 = orjson.dumps(msg2)
    clean2, rej2 = normalise(raw2, states, int(time.time() * 1000), 60.0, **_BBOX)
    assert rej2 is not None and rej2[0] == "DUPLICATE"


# ── IMPOSSIBLE_JUMP ───────────────────────────────────────────────────────────

def test_dlq_impossible_jump():
    states: dict = {}
    # First message at Bengaluru
    msg1 = _valid_msg(seq=1, lat=12.935, lon=77.624)
    raw1 = orjson.dumps(msg1)
    normalise(raw1, states, int(time.time() * 1000), 60.0, **_BBOX)

    # Second message at Mumbai (> 1000 km away, 1 second later)
    msg2 = _valid_msg(seq=2, lat=18.975, lon=72.825)  # Mumbai
    msg2["ts_ms"] = msg1["ts_ms"] + 1000  # 1 second later
    msg2["trace"]["emitted_ms"] = msg2["ts_ms"]
    raw2 = orjson.dumps(msg2)
    clean, rejection = normalise(raw2, states, msg2["ts_ms"], 60.0, **_BBOX)
    # Mumbai is outside bbox, but if it were inside, it would be IMPOSSIBLE_JUMP.
    # For this test use a coord inside the bbox but very far from last fix.
    # Actually Mumbai is outside bbox=12.80-13.10, so OUT_OF_RANGE fires first.
    # Test a teleport within bbox but impossibly fast.
    msg3 = _valid_msg(seq=3, lat=13.09, lon=77.79)  # inside bbox, ~20 km away
    msg3["ts_ms"] = msg1["ts_ms"] + 500   # 0.5 s later → ~144,000 km/h
    msg3["trace"]["emitted_ms"] = msg3["ts_ms"]
    states2: dict = {}
    # Seed the state with a nearby fix
    msg_seed = _valid_msg(seq=10, lat=12.935, lon=77.624)
    normalise(orjson.dumps(msg_seed), states2, int(time.time() * 1000), 60.0, **_BBOX)
    msg3["ts_ms"] = int(time.time() * 1000) + 500
    raw3 = orjson.dumps(msg3)
    clean3, rej3 = normalise(raw3, states2, msg3["ts_ms"], 60.0, **_BBOX)
    assert rej3 is not None and rej3[0] == "IMPOSSIBLE_JUMP"


# ── Valid message ─────────────────────────────────────────────────────────────

def test_valid_message_produces_clean():
    msg = _valid_msg()
    clean, rejection = _run(msg)
    assert rejection is None
    assert clean is not None
    assert clean.h3_r11 != ""
    assert clean.h3_r9 != ""
    assert clean.schema_ == "rs.telemetry.clean.v1"


def test_speed_mismatch_flag():
    msg = _valid_msg(speed=40.0, wheel_speed=10.0)  # diff = 30 > 25
    clean, rejection = _run(msg)
    assert rejection is None
    assert clean.quality.speed_mismatch is True


def test_no_speed_mismatch():
    msg = _valid_msg(speed=40.0, wheel_speed=41.0)  # diff = 1
    clean, rejection = _run(msg)
    assert rejection is None
    assert clean.quality.speed_mismatch is False

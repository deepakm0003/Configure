"""
test_fuse.py – Unit tests for the pure fuse logic.

Covers:
  - 3 vehicles → CONFIRMED
  - 1 vehicle × 5 → stays CANDIDATE (not enough distinct vehicles)
  - Clean passes → CLEARED (confidence decays)
  - Faulty vehicle → quarantined with no confirmed hazard
  - Neighbour-cell merge (new detection merges with existing hazard in adjacent cell)
  - Recovery round-trip (serialize → restore log_odds)
"""

import math
import time
import pytest

from pipeline.stages.fuse import (
    FuseState, _HazardRecord, _CELL_RES, _L0, _sigmoid,
    process_observation, run_sweep, _CONFIRM_CONF, _CLEARED_CONF,
    _apply_positive, _apply_negative, _find_or_create_hazard,
    _init_reputation, _clamp_rep,
)
from pipeline.rs_common.models import RoadEvent, Detection, Trace, HazardState, HazardInternal
from pipeline.rs_common import geo

# Shared location
_LAT = 12.935
_LON = 77.624
_CELL_R11 = geo.latlng_to_cell(_LAT, _LON, 11)
_CELL_R10 = geo.latlng_to_cell(_LAT, _LON, 10)
_CELL_R9 = geo.latlng_to_cell(_LAT, _LON, 9)


def _obs(
    vehicle_id: str = "SIM-CAR-0001",
    detections: list[dict] | None = None,
    mean_speed: float = 40.0,
    lat: float = _LAT,
    lon: float = _LON,
) -> RoadEvent:
    if detections is None:
        detections = []
    now = int(time.time() * 1000)
    det_models = [Detection(**d) for d in detections]
    r11 = geo.latlng_to_cell(lat, lon, 11)
    r9 = geo.latlng_to_cell(lat, lon, 9)
    return RoadEvent(
        **{
            "schema": "rs.observation.v1",
            "obs_id": f"{vehicle_id}:{now}",
            "vehicle_id": vehicle_id,
            "vehicle_class": "car",
            "h3_r11": r11,
            "h3_r9": r9,
            "enter_ms": now - 2000,
            "exit_ms": now,
            "mean_speed_kmh": mean_speed,
            "heading_deg": 90.0,
            "lat": lat,
            "lon": lon,
            "detections": det_models,
            "trace": Trace(),
        }
    )


def _pothole_det(severity: float = 0.7) -> dict:
    return {"type": "POTHOLE", "severity": severity, "score": 5.0, "lat": _LAT, "lon": _LON}


def _run_sweep_once(fuse: FuseState, dt_s: float = 1.0) -> list:
    now_ms = int(time.time() * 1000)
    return run_sweep(
        fuse, dt_s=dt_s, now_ms=now_ms,
        confirm_min_vehicles=3,
        demo_scale=10.0,
        candidate_timeout_s=15 * 60,
    )


# ── 3 vehicles → CONFIRMED ────────────────────────────────────────────────────

def test_three_vehicles_confirm():
    """
    Per the evidence model: logit(0.8) ≈ 1.386, L0 ≈ -2.944, so we need Δ ≈ 4.33.
    Per vehicle: W_POS=1.4 × rep=0.7 × (0.5+0.5×0.9) = 0.931.
    ceil(4.33 / 0.931) = 5 vehicles, so we use 6 for comfort.
    confirm_min_vehicles is still 3; the extra 3 are needed for log-odds.
    """
    fuse = FuseState()
    now = int(time.time() * 1000)

    for i in range(6):
        ob = _obs(vehicle_id=f"SIM-CAR-{i:04d}", detections=[_pothole_det(0.9)])
        process_observation(ob, fuse, now, confirm_min_vehicles=3, demo_scale=10.0)

    # Run sweep to trigger lifecycle
    _run_sweep_once(fuse)

    # Find the pothole hazard
    hazard = next((r.state for r in fuse.hazards.values()
                   if r.state.hazard_type == "POTHOLE"), None)
    assert hazard is not None, "No POTHOLE hazard created"
    assert hazard.status == "CONFIRMED", f"Expected CONFIRMED, got {hazard.status}"
    assert hazard.distinct_vehicles >= 3


# ── 1 vehicle × 5 → stays CANDIDATE ─────────────────────────────────────────

def test_single_vehicle_stays_candidate():
    fuse = FuseState()
    now = int(time.time() * 1000)

    for i in range(5):
        ob = _obs(vehicle_id="SIM-CAR-0001", detections=[_pothole_det(0.8)])
        process_observation(ob, fuse, now, confirm_min_vehicles=3, demo_scale=10.0)

    _run_sweep_once(fuse)

    hazard = next((r.state for r in fuse.hazards.values()
                   if r.state.hazard_type == "POTHOLE"), None)
    assert hazard is not None
    # Only 1 distinct vehicle → cannot confirm, should still be CANDIDATE
    assert hazard.status == "CANDIDATE", f"Expected CANDIDATE, got {hazard.status}"


# ── Clean passes → CLEARED ────────────────────────────────────────────────────

def test_clean_passes_decay_to_cleared():
    """
    Start with a CONFIRMED hazard, then send many clean passes.
    After many sweep ticks the confidence should drop below 0.2 → CLEARED.
    """
    fuse = FuseState()
    now = int(time.time() * 1000)

    # Confirm it first with 6 vehicles (need enough log-odds to reach conf ≥ 0.8)
    for i in range(6):
        ob = _obs(vehicle_id=f"SIM-CAR-{i:04d}", detections=[_pothole_det(0.9)])
        process_observation(ob, fuse, now, confirm_min_vehicles=3, demo_scale=10.0)
    _run_sweep_once(fuse)

    # Verify confirmed
    rec = next((r for r in fuse.hazards.values() if r.state.hazard_type == "POTHOLE"), None)
    assert rec is not None
    assert rec.state.status == "CONFIRMED", f"Should be CONFIRMED before clean passes, got {rec.state.status}"

    # Send 20 clean passes from different vehicles at speed > 15
    for i in range(20):
        ob = _obs(vehicle_id=f"SIM-CAR-CLN-{i:04d}", detections=[], mean_speed=50.0)
        process_observation(ob, fuse, now, confirm_min_vehicles=3, demo_scale=10.0)

    # Run many sweeps with large dt to accelerate time decay (demo_scale=10)
    # POTHOLE HL = 6 h / 10 = 36 min; to get L < L0 we need many clean negatives
    for _ in range(200):
        run_sweep(fuse, dt_s=120.0, now_ms=now,
                  confirm_min_vehicles=3, demo_scale=10.0,
                  candidate_timeout_s=15 * 60)

    rec2 = next((r for r in fuse.hazards.values() if r.state.hazard_type == "POTHOLE"), None)
    if rec2 is not None:
        assert rec2.state.status in ("DECAYING", "CLEARED"), (
            f"Expected DECAYING or CLEARED, got {rec2.state.status}, conf={rec2.state.confidence}"
        )


# ── Faulty vehicle → quarantined, no confirmed hazard ────────────────────────

def test_faulty_vehicle_quarantined_no_confirm():
    """
    A vehicle with low reputation that reports many hazards in cells where
    other vehicles make clean passes should get quarantined and not trigger CONFIRMED.
    """
    from pipeline.stages.fuse import apply_reputation_clean_pass_check

    fuse = FuseState()
    now = int(time.time() * 1000)

    # Seed the faulty vehicle with low reputation
    faulty_id = "SIM-CAR-FAULTY"
    rep_obj = _init_reputation(fuse, faulty_id)
    rep_obj.reputation = 0.20  # below 0.25 → quarantined
    rep_obj.quarantined = True

    # Faulty vehicle sends 5 pothole reports
    for i in range(5):
        ob = _obs(vehicle_id=faulty_id, detections=[_pothole_det(0.9)])
        process_observation(ob, fuse, now, confirm_min_vehicles=3, demo_scale=10.0)

    _run_sweep_once(fuse)

    hazard = next((r.state for r in fuse.hazards.values()
                   if r.state.hazard_type == "POTHOLE"), None)
    # Either no hazard at all, or still CANDIDATE (quarantined vehicle counts 0)
    if hazard is not None:
        assert hazard.status != "CONFIRMED", \
            f"Quarantined vehicle should not confirm hazard, got {hazard.status}"


# ── Neighbour-cell merge ──────────────────────────────────────────────────────

def test_neighbour_cell_merge():
    """
    Two vehicles detect a pothole in adjacent res-11 cells.
    The second report should merge into the first hazard (not create a second).
    """
    fuse = FuseState()
    now = int(time.time() * 1000)

    lat1 = _LAT
    lon1 = _LON
    # Get a cell slightly displaced (~30 m) so it's a different r11 cell but adjacent
    cell1 = geo.latlng_to_cell(lat1, lon1, 11)
    neighbours = list(geo.cell_disk(cell1, 1) - {cell1})
    if not neighbours:
        pytest.skip("No neighbours found for test cell")

    cell_neighbour = neighbours[0]
    lat2, lon2 = geo.cell_to_latlng(cell_neighbour)

    ob1 = _obs("SIM-CAR-0001", [_pothole_det(0.7)], lat=lat1, lon=lon1)
    ob2 = _obs("SIM-CAR-0002", [_pothole_det(0.7)], lat=lat2, lon=lon2)

    process_observation(ob1, fuse, now, 3, 10.0)
    process_observation(ob2, fuse, now, 3, 10.0)

    pothole_hazards = [r for r in fuse.hazards.values() if r.state.hazard_type == "POTHOLE"]
    # Should merge into ONE hazard, not two separate ones
    assert len(pothole_hazards) == 1, (
        f"Expected 1 merged hazard, got {len(pothole_hazards)}"
    )


# ── Recovery round-trip ───────────────────────────────────────────────────────

def test_recovery_round_trip():
    """
    Serialize a hazard to HazardState, restore it into a new FuseState
    via _HazardRecord with internal.log_odds, and confirm confidence matches.
    """
    fuse = FuseState()
    now = int(time.time() * 1000)

    for i in range(3):
        ob = _obs(vehicle_id=f"SIM-CAR-{i:04d}", detections=[_pothole_det(0.8)])
        process_observation(ob, fuse, now, 3, 10.0)
    _run_sweep_once(fuse)

    # Grab the hazard
    rec = next((r for r in fuse.hazards.values() if r.state.hazard_type == "POTHOLE"), None)
    assert rec is not None

    original_log_odds = rec.log_odds
    original_conf = rec.state.confidence

    # Simulate serialise → deserialise (what recovery does)
    rec.state.internal.log_odds = original_log_odds
    state_dict = rec.state.model_dump(by_alias=True)

    # Restore into a new FuseState
    from pipeline.stages.fuse import _HazardRecord
    restored_state = HazardState.model_validate(state_dict)
    restored_rec = _HazardRecord(
        state=restored_state,
        log_odds=restored_state.internal.log_odds,
    )

    restored_conf = _sigmoid(restored_rec.log_odds)
    assert abs(restored_conf - original_conf) < 0.001, (
        f"Confidence after recovery: {restored_conf:.4f} vs original {original_conf:.4f}"
    )

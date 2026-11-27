"""
fuse.py – rs-fuse stage.

Input:  road.events         (key: h3 res-9 cell)
Output: hazards.state       (key: hazard_id, compacted)
        vehicles.reputation (key: vehicle_id, compacted)

State (in memory + compacted topics for recovery):
  - hazards: dict[hazard_id → HazardState + internal log-odds]
  - reputations: dict[vehicle_id → VehicleReputation]

Algorithm (algorithms.md § D):
  - Log-odds evidence model with per-type weights.
  - Lifecycle: CANDIDATE → CONFIRMED → DECAYING → CLEARED.
  - 1 s sweep: time decay + lifecycle checks.
  - Vehicle reputation: update on confirm/clear, quarantine/release.
  - Emission throttling (at most every 2 s per hazard when Δconfidence ≥ 0.05).
  - Recovery: read hazards.state and vehicles.reputation from earliest on startup.
"""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

import orjson
from confluent_kafka import Message, Producer

from pipeline.rs_common.config import Config
from pipeline.rs_common import geo
from pipeline.rs_common.kafka_io import make_producer, run_loop, read_to_hwm, produce, send_to_dlq
from pipeline.rs_common.metrics import StageMetrics, log_json
from pipeline.rs_common.models import (
    RoadEvent, HazardState, HazardCentroid, HazardInternal, VehicleReputation,
    DetectionType, Trace
)

ROLE = "fuse"

# ── Constants from algorithms.md § D ─────────────────────────────────────────

_PRIOR_P0 = 0.05
_L0 = math.log(_PRIOR_P0 / (1 - _PRIOR_P0))  # logit(0.05) ≈ -2.944

_W_POS: dict[str, float] = {
    "POTHOLE": 1.4, "SPEED_BREAKER": 1.4, "WATERLOGGING": 1.6,
    "SLIPPERY": 1.2, "HARSH_BRAKE": 1.0,
}
_W_NEG: dict[str, float] = {
    "POTHOLE": 0.35, "SPEED_BREAKER": 0.35, "WATERLOGGING": 0.7,
    "SLIPPERY": 0.4, "HARSH_BRAKE": 0.3,
}

# Half-lives in seconds (before demo scale)
_HL_S: dict[str, float] = {
    "POTHOLE": 6 * 3600,
    "SPEED_BREAKER": 7 * 24 * 3600,
    "WATERLOGGING": 20 * 60,
    "SLIPPERY": 3600,
    "HARSH_BRAKE": 2 * 3600,
}

# Cell resolution per hazard type
_CELL_RES: dict[str, int] = {
    "POTHOLE": 11, "SPEED_BREAKER": 11, "SLIPPERY": 11,
    "WATERLOGGING": 10, "HARSH_BRAKE": 10,
}

_MIN_SPEED_NEG = 15.0        # for most types
_MIN_SPEED_NEG_WATER = 30.0  # for WATERLOGGING

_CONFIRM_CONF = 0.8
_DECAYING_CONF = 0.6
_CLEARED_CONF = 0.2
_CANDIDATE_TIMEOUT_S = 15 * 60  # 15 min before demo scale


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def _severity_label(score: float, htype: str) -> tuple[str, Optional[int]]:
    if score < 0.3:
        label, speed = "LOW", None
    elif score < 0.6:
        label, speed = "MEDIUM", 30
    elif score < 0.8:
        label, speed = "HIGH", 20
    else:
        label, speed = "CRITICAL", 15
    if htype == "WATERLOGGING":
        speed = 10
    if label == "LOW":
        speed = None
    return label, speed


# ── Extended hazard record (adds mutable state beyond HazardState) ────────────

@dataclass
class _HazardRecord:
    state: HazardState
    log_odds: float
    # per-vehicle: (first_report_ms, count_in_window)
    vehicle_reports: dict[str, tuple[int, int]] = field(default_factory=dict)
    # per-vehicle max severity contributed
    vehicle_max_sev: dict[str, float] = field(default_factory=dict)
    last_emit_ms: float = 0.0
    last_conf_emitted: float = 0.0


# ── Fuse state container ──────────────────────────────────────────────────────

@dataclass
class FuseState:
    hazards: dict[str, _HazardRecord] = field(default_factory=dict)
    reputations: dict[str, VehicleReputation] = field(default_factory=dict)
    # recent clean passes per cell: {cell: [(ts_ms, vehicle_id), ...]}
    clean_passes: dict[str, list[tuple[int, str]]] = field(default_factory=dict)


# ── Pure fuse logic ───────────────────────────────────────────────────────────

def _get_rep(state: FuseState, vehicle_id: str) -> float:
    rep = state.reputations.get(vehicle_id)
    if rep is None or rep.quarantined:
        return 0.0 if (rep and rep.quarantined) else 0.7
    return rep.reputation


def _init_reputation(state: FuseState, vehicle_id: str) -> VehicleReputation:
    if vehicle_id not in state.reputations:
        state.reputations[vehicle_id] = VehicleReputation(
            **{
                "schema": "rs.reputation.v1",
                "vehicle_id": vehicle_id,
                "reputation": 0.7,
                "quarantined": False,
                "confirmed_reports": 0,
                "rejected_reports": 0,
                "updated_ms": int(time.time() * 1000),
            }
        )
    return state.reputations[vehicle_id]


def _clamp_rep(r: float) -> float:
    return max(0.05, min(0.99, r))


def _apply_positive(
    record: _HazardRecord,
    vehicle_id: str,
    severity: float,
    rep: float,
    now_ms: int,
) -> None:
    """Apply a positive detection to the log-odds."""
    if rep == 0.0:   # quarantined
        return
    htype = record.state.hazard_type
    w = _W_POS.get(htype, 1.0)

    # Only the first report within 10 min counts fully; repeats × 0.2
    first_ms, count = record.vehicle_reports.get(vehicle_id, (now_ms, 0))
    if count == 0:
        multiplier = 1.0
    else:
        # check 10-min window
        if now_ms - first_ms < 10 * 60 * 1000:
            multiplier = 0.2
        else:
            multiplier = 1.0
            first_ms = now_ms
    record.vehicle_reports[vehicle_id] = (first_ms, count + 1)

    delta = w * rep * (0.5 + 0.5 * severity) * multiplier
    record.log_odds += delta
    record.state.positive_reports += 1
    record.state.distinct_vehicles = len(
        {v for v, (_, c) in record.vehicle_reports.items() if c > 0}
    )
    if vehicle_id not in record.state.reporters:
        record.state.reporters.append(vehicle_id)
    record.vehicle_max_sev[vehicle_id] = max(
        record.vehicle_max_sev.get(vehicle_id, 0.0), severity
    )


def _apply_negative(
    record: _HazardRecord,
    vehicle_id: str,
    mean_speed: float,
    rep: float,
    htype: str,
) -> None:
    """Apply a negative (clean pass) observation."""
    if rep == 0.0:
        return
    min_speed = _MIN_SPEED_NEG_WATER if htype == "WATERLOGGING" else _MIN_SPEED_NEG
    if mean_speed < min_speed:
        return
    w = _W_NEG.get(htype, 0.3)
    record.log_odds -= w * rep
    record.state.clean_passes += 1


def _apply_time_decay(record: _HazardRecord, dt_s: float, demo_scale: float) -> None:
    """Decay log-odds toward L0 with half-life HL/demo_scale."""
    htype = record.state.hazard_type
    hl = _HL_S.get(htype, 6 * 3600) / demo_scale
    if hl <= 0:
        return
    # L(t) = L0 + (L - L0) * exp(-ln2 / HL * dt)
    decay = math.exp(-math.log(2) / hl * dt_s)
    record.log_odds = _L0 + (record.log_odds - _L0) * decay


def _median_severity(record: _HazardRecord) -> float:
    sevs = sorted(record.vehicle_max_sev.values())
    if not sevs:
        return 0.0
    mid = len(sevs) // 2
    if len(sevs) % 2 == 1:
        return sevs[mid]
    return (sevs[mid - 1] + sevs[mid]) / 2


def _hazard_cell(htype: str, cell_r11: str, cell_r10: str) -> str:
    """Return the cell for a hazard given its type."""
    res = _CELL_RES.get(htype, 11)
    if res == 11:
        return cell_r11
    return cell_r10


def _find_or_create_hazard(
    fuse: FuseState,
    htype: str,
    cell: str,
    lat: float,
    lon: float,
    now_ms: int,
) -> _HazardRecord:
    """Return existing non-CLEARED hazard within grid_disk(cell,1) or create a new one."""
    for neighbour in geo.cell_disk(cell, 1):
        hid = f"{htype}:{neighbour}"
        rec = fuse.hazards.get(hid)
        if rec and rec.state.status != "CLEARED":
            return rec

    # Create new
    hid = f"{htype}:{cell}"
    res = _CELL_RES.get(htype, 11)
    sev = 0.0
    sev_label, adv_speed = _severity_label(sev, htype)
    new_state = HazardState(
        **{
            "schema": "rs.hazard.v1",
            "hazard_id": hid,
            "hazard_type": htype,
            "status": "CANDIDATE",
            "status_reason": "new detection",
            "confidence": _sigmoid(_L0),
            "severity_score": sev,
            "severity_label": sev_label,
            "advisory_speed_kmh": adv_speed,
            "h3_cell": cell,
            "h3_res": res,
            "lat": lat,
            "lon": lon,
            "centroid": HazardCentroid(lat=lat, lon=lon),
            "radius_m": 15.0,
            "distinct_vehicles": 0,
            "positive_reports": 0,
            "clean_passes": 0,
            "reporters": [],
            "first_seen_ms": now_ms,
            "confirmed_ms": 0,
            "cleared_ms": None,
            "last_update_ms": now_ms,
            "version": 0,
            "internal": HazardInternal(log_odds=_L0),
            "trace": Trace(),
        }
    )
    rec = _HazardRecord(state=new_state, log_odds=_L0)
    fuse.hazards[hid] = rec
    return rec


def process_observation(
    obs: RoadEvent,
    fuse: FuseState,
    now_ms: int,
    confirm_min_vehicles: int,
    demo_scale: float,
) -> list[tuple[str, Optional[str]]]:
    """
    Process one road event observation.

    Returns a list of (hazard_id, change_reason) for hazards whose status or confidence changed.
    """
    vehicle_id = obs.vehicle_id
    rep = _get_rep(fuse, vehicle_id)
    _init_reputation(fuse, vehicle_id)
    changes: list[tuple[str, Optional[str]]] = []

    # Build h3_r10 for WATERLOGGING / HARSH_BRAKE cell resolution
    cell_r10 = geo.latlng_to_cell(obs.lat, obs.lon, 10)
    cell_r11 = obs.h3_r11

    # Positive detections
    detected_types: set[str] = set()
    for det in obs.detections:
        htype = det.type
        cell = _hazard_cell(htype, cell_r11, cell_r10)
        rec = _find_or_create_hazard(fuse, htype, cell, obs.lat, obs.lon, now_ms)
        _apply_positive(rec, vehicle_id, det.severity, rep, now_ms)
        detected_types.add(htype)
        changes.append((rec.state.hazard_id, "positive"))

    # Negative (clean pass): for every type NOT detected, in the observation cell
    for htype in list(_W_NEG.keys()):
        if htype in detected_types:
            continue
        cell = _hazard_cell(htype, cell_r11, cell_r10)
        # Only apply negative if there's already a hazard nearby
        for neighbour in geo.cell_disk(cell, 1):
            hid = f"{htype}:{neighbour}"
            rec = fuse.hazards.get(hid)
            if rec and rec.state.status != "CLEARED":
                _apply_negative(rec, vehicle_id, obs.mean_speed_kmh, rep, htype)
                changes.append((hid, "negative"))

    # Track clean passes per cell for reputation
    cp_key = cell_r11
    fuse.clean_passes.setdefault(cp_key, [])
    if not obs.detections:
        fuse.clean_passes[cp_key].append((now_ms, vehicle_id))
        # Prune older than 10 min
        cutoff = now_ms - 10 * 60 * 1000
        fuse.clean_passes[cp_key] = [(t, v) for t, v in fuse.clean_passes[cp_key] if t >= cutoff]

    return changes


def run_sweep(
    fuse: FuseState,
    dt_s: float,
    now_ms: int,
    confirm_min_vehicles: int,
    demo_scale: float,
    candidate_timeout_s: float,
) -> list[tuple[str, str]]:
    """
    1-second sweep: decay log-odds, run lifecycle transitions.
    Returns list of (hazard_id, reason) for changed hazards.
    """
    changes: list[tuple[str, str]] = []
    to_remove: list[str] = []  # tombstone candidates

    for hid, rec in fuse.hazards.items():
        st = rec.state
        if st.status == "CLEARED":
            # Tombstone after 30 min / scale
            tombstone_s = 30 * 60 / demo_scale
            if st.cleared_ms and (now_ms - st.cleared_ms) / 1000 > tombstone_s:
                to_remove.append(hid)
            continue

        old_conf = _sigmoid(rec.log_odds)
        _apply_time_decay(rec, dt_s, demo_scale)
        new_conf = _sigmoid(rec.log_odds)

        # Update severity
        sev = _median_severity(rec)
        sev_label, adv_speed = _severity_label(sev, st.hazard_type)
        st.severity_score = round(sev, 3)
        st.severity_label = sev_label
        st.advisory_speed_kmh = adv_speed
        st.confidence = round(new_conf, 4)
        st.internal.log_odds = round(rec.log_odds, 4)

        old_status = st.status

        # Lifecycle
        if st.status == "CANDIDATE":
            if new_conf >= _CONFIRM_CONF and st.distinct_vehicles >= confirm_min_vehicles:
                st.status = "CONFIRMED"
                st.confirmed_ms = now_ms
                st.status_reason = f"{st.distinct_vehicles} vehicles, conf {new_conf:.2f}"
                _on_confirmed(fuse, hid, rec, now_ms)
                changes.append((hid, "confirmed"))
            elif (now_ms - st.first_seen_ms) / 1000 > candidate_timeout_s / demo_scale:
                st.status = "CLEARED"
                st.cleared_ms = now_ms
                st.status_reason = "unconfirmed"
                _on_unconfirmed(fuse, hid, rec)
                changes.append((hid, "unconfirmed"))

        elif st.status == "CONFIRMED":
            if new_conf < _DECAYING_CONF:
                st.status = "DECAYING"
                st.status_reason = f"conf dropped to {new_conf:.2f}"
                changes.append((hid, "decaying"))

        elif st.status == "DECAYING":
            if new_conf >= _CONFIRM_CONF:
                st.status = "CONFIRMED"
                st.status_reason = f"conf recovered to {new_conf:.2f}"
                changes.append((hid, "re-confirmed"))
            elif new_conf < _CLEARED_CONF:
                st.status = "CLEARED"
                st.cleared_ms = now_ms
                st.status_reason = "repaired_or_resolved"
                changes.append((hid, "cleared"))

        if old_status != st.status or abs(new_conf - old_conf) >= 0.001:
            st.last_update_ms = now_ms
            st.version += 1

    for hid in to_remove:
        del fuse.hazards[hid]

    return changes


def _on_confirmed(fuse: FuseState, hid: str, rec: _HazardRecord, now_ms: int) -> None:
    """Boost reputation of reporters."""
    for vid in rec.state.reporters:
        rep_obj = _init_reputation(fuse, vid)
        rep_obj.reputation = _clamp_rep(rep_obj.reputation + 0.05 * (1 - rep_obj.reputation))
        rep_obj.confirmed_reports += 1
        rep_obj.updated_ms = now_ms
        _check_quarantine(rep_obj)


def _on_unconfirmed(fuse: FuseState, hid: str, rec: _HazardRecord) -> None:
    """Penalise reporters when candidate is cleared as unconfirmed."""
    for vid in rec.state.reporters:
        rep_obj = _init_reputation(fuse, vid)
        rep_obj.reputation = _clamp_rep(rep_obj.reputation - 0.15 * rep_obj.reputation)
        rep_obj.rejected_reports += 1
        rep_obj.updated_ms = int(time.time() * 1000)
        _check_quarantine(rep_obj)


def _check_quarantine(rep_obj: VehicleReputation) -> None:
    if rep_obj.reputation < 0.25:
        rep_obj.quarantined = True
    elif rep_obj.reputation > 0.4:
        rep_obj.quarantined = False


def apply_reputation_clean_pass_check(
    fuse: FuseState,
    vehicle_id: str,
    htype: str,
    cell: str,
    now_ms: int,
) -> bool:
    """
    If a vehicle reports type T in a cell where ≥ 3 other vehicles made clean passes
    in the last 10 min, penalise its reputation.  Returns True if penalised.
    """
    cp_key = cell
    passes = fuse.clean_passes.get(cp_key, [])
    cutoff = now_ms - 10 * 60 * 1000
    others = [v for t, v in passes if t >= cutoff and v != vehicle_id]
    if len(set(others)) >= 3:
        rep_obj = _init_reputation(fuse, vehicle_id)
        rep_obj.reputation = _clamp_rep(rep_obj.reputation - 0.1 * rep_obj.reputation)
        rep_obj.updated_ms = now_ms
        _check_quarantine(rep_obj)
        return True
    return False


# ── Kafka runner ──────────────────────────────────────────────────────────────

def run(cfg: Config) -> None:
    fuse = FuseState()
    metrics = StageMetrics(ROLE)

    input_topic = cfg.topics.stage_input(cfg.topics.road_events)
    hazards_topic = cfg.topics.hazards_state
    rep_topic = cfg.topics.vehicles_reputation

    log_json(ROLE, "INFO", "starting", input=input_topic)

    # ── Startup recovery ─────────────────────────────────────────────────────
    _recover(cfg, fuse)

    producer = make_producer(cfg)

    # Throttle state: {hazard_id: last_emit_ms}
    emit_times: dict[str, float] = {}
    rep_emit_times: dict[str, float] = {}

    def _should_emit_hazard(hid: str, conf: float, status: str, now: float) -> bool:
        last = emit_times.get(hid, 0.0)
        st_obj = fuse.hazards.get(hid)
        if st_obj is None:
            return False
        last_conf = st_obj.last_conf_emitted
        if abs(conf - last_conf) >= 0.05 or status != fuse.hazards[hid].state.status:
            if now - last >= 2.0:
                return True
        # Always emit on status change
        return False

    def _emit_hazard(hid: str, now_ms: int) -> None:
        rec = fuse.hazards.get(hid)
        if rec is None:
            return
        rec.state.internal.log_odds = round(rec.log_odds, 4)
        payload = orjson.dumps(rec.state.model_dump(by_alias=True))
        produce(producer, hazards_topic, payload, key=hid.encode(), role=ROLE)
        emit_times[hid] = time.monotonic()
        rec.last_conf_emitted = rec.state.confidence
        metrics.record_out()

    def _emit_reputation(vid: str, now_ms: int) -> None:
        rep_obj = fuse.reputations.get(vid)
        if rep_obj is None:
            return
        last = rep_emit_times.get(vid, 0.0)
        if time.monotonic() - last < 5.0:
            return
        rep_obj.updated_ms = now_ms
        payload = orjson.dumps(rep_obj.model_dump(by_alias=True))
        produce(producer, rep_topic, payload, key=vid.encode(), role=ROLE)
        rep_emit_times[vid] = time.monotonic()

    # ── 1 s sweep thread ─────────────────────────────────────────────────────
    _last_sweep = [time.monotonic()]

    def _run_sweep_if_due(now_ms: int) -> None:
        now = time.monotonic()
        dt = now - _last_sweep[0]
        if dt < 1.0:
            return
        _last_sweep[0] = now
        sweep_changes = run_sweep(
            fuse,
            dt_s=dt,
            now_ms=now_ms,
            confirm_min_vehicles=cfg.confirm_min_vehicles,
            demo_scale=cfg.demo_time_scale,
            candidate_timeout_s=_CANDIDATE_TIMEOUT_S,
        )
        for hid, reason in sweep_changes:
            _emit_hazard(hid, now_ms)
            rec = fuse.hazards.get(hid)
            if rec:
                for vid in rec.state.reporters:
                    _emit_reputation(vid, now_ms)

    def process(msg: Message, producer: Producer) -> None:
        now_ms = int(time.time() * 1000)
        metrics.record_in()

        raw = msg.value()
        try:
            data = orjson.loads(raw)
            obs = RoadEvent.model_validate(data)
        except Exception as exc:
            log_json(ROLE, "ERROR", "parse_error", error=str(exc))
            return

        obs.trace.fuse_ms = now_ms

        changes = process_observation(
            obs, fuse, now_ms,
            cfg.confirm_min_vehicles, cfg.demo_time_scale
        )

        for hid, reason in changes:
            rec = fuse.hazards.get(hid)
            if rec is None:
                continue
            conf = rec.state.confidence
            status = rec.state.status
            now_m = time.monotonic()
            last = emit_times.get(hid, 0.0)
            if abs(conf - rec.last_conf_emitted) >= 0.05 and now_m - last >= 2.0:
                _emit_hazard(hid, now_ms)
            for vid in rec.state.reporters:
                _emit_reputation(vid, now_ms)

        # Apply clean-pass reputation check
        if not obs.detections:
            cell_r11 = obs.h3_r11
            for htype in list(_W_NEG.keys()):
                apply_reputation_clean_pass_check(
                    fuse, obs.vehicle_id, htype, cell_r11, now_ms
                )
            _emit_reputation(obs.vehicle_id, now_ms)

        _run_sweep_if_due(now_ms)
        metrics.record_latency(obs.trace.emitted_ms, now_ms)
        metrics.maybe_log()

    run_loop(
        cfg=cfg,
        input_topics=[input_topic],
        process_fn=process,
        role=ROLE,
        offset_reset="latest",
    )


def _recover(cfg: Config, fuse: FuseState) -> None:
    """Read hazards.state and vehicles.reputation from earliest to rebuild in-memory state."""
    log_json(ROLE, "INFO", "recovery_start")

    def _ingest_hazard(msg: Message) -> None:
        try:
            val = msg.value()
            if val is None:
                # Tombstone: remove
                key = msg.key()
                if key:
                    hid = key.decode("utf-8", errors="replace")
                    fuse.hazards.pop(hid, None)
                return
            data = orjson.loads(val)
            st = HazardState.model_validate(data)
            rec = _HazardRecord(
                state=st,
                log_odds=st.internal.log_odds,
            )
            fuse.hazards[st.hazard_id] = rec
        except Exception as exc:
            log_json(ROLE, "ERROR", "recovery_hazard_parse", error=str(exc))

    def _ingest_rep(msg: Message) -> None:
        try:
            val = msg.value()
            if val is None:
                return
            data = orjson.loads(val)
            rep = VehicleReputation.model_validate(data)
            fuse.reputations[rep.vehicle_id] = rep
        except Exception as exc:
            log_json(ROLE, "ERROR", "recovery_rep_parse", error=str(exc))

    read_to_hwm(
        cfg,
        [cfg.topics.hazards_state],
        _ingest_hazard,
        group_id=f"{cfg.topics.prefix}cg_fuse_recovery_hazards",
        role=ROLE,
    )
    read_to_hwm(
        cfg,
        [cfg.topics.vehicles_reputation],
        _ingest_rep,
        group_id=f"{cfg.topics.prefix}cg_fuse_recovery_rep",
        role=ROLE,
    )
    log_json(ROLE, "INFO", "recovery_done",
             hazards=len(fuse.hazards), reputations=len(fuse.reputations))

"""
models.py – Pydantic v2 models for every topic contract.

Follows the data-contracts exactly. All timestamps are epoch milliseconds.
"""

from __future__ import annotations

from typing import Any, Literal, Optional
import time

from pydantic import BaseModel, Field


# ── Shared trace block ────────────────────────────────────────────────────────

class Trace(BaseModel):
    emitted_ms: int = 0
    normalize_ms: int = 0
    detect_ms: int = 0
    fuse_ms: int = 0
    alert_ms: int = 0

    model_config = {"extra": "allow"}


# ── 1. telemetry.raw ──────────────────────────────────────────────────────────

VehicleClass = Literal["car", "bus", "truck", "two_wheeler"]


class GPS(BaseModel):
    lat: float
    lon: float
    speed_kmh: float
    heading_deg: float
    hdop: float = 1.0


class IMU(BaseModel):
    az_max_g: float
    az_min_g: float
    az_std_g: float = 0.0
    ax_min_g: float = 0.0
    ax_max_g: float = 0.0
    peak_offset_ms: int = 500


class CAN(BaseModel):
    wheel_speed_kmh: float = 0.0
    brake_pressure_bar: float = 0.0
    abs_active: bool = False
    esc_active: bool = False
    wiper_level: int = 0
    fog_lamp: bool = False
    throttle_pct: float = 0.0


class TelemetryRaw(BaseModel):
    schema_: str = Field("rs.telemetry.v1", alias="schema")
    vehicle_id: str
    vehicle_class: VehicleClass
    seq: int
    ts_ms: int
    gps: GPS
    imu: IMU
    can: CAN
    trace: Trace = Field(default_factory=Trace)

    model_config = {"populate_by_name": True}


# ── 2. telemetry.clean ────────────────────────────────────────────────────────

class Quality(BaseModel):
    speed_mismatch: bool = False


class TelemetryClean(TelemetryRaw):
    schema_: str = Field("rs.telemetry.clean.v1", alias="schema")
    h3_r11: str = ""
    h3_r9: str = ""
    quality: Quality = Field(default_factory=Quality)

    model_config = {"populate_by_name": True}


# ── 3. telemetry.dlq ─────────────────────────────────────────────────────────

DlqReason = Literal[
    "SCHEMA_INVALID",
    "OUT_OF_RANGE",
    "IMPOSSIBLE_JUMP",
    "LATE_EVENT",
    "CLOCK_SKEW",
    "DUPLICATE",
]


class TelemetryDlq(BaseModel):
    schema_: str = Field("rs.dlq.v1", alias="schema")
    reason: DlqReason
    detail: str = ""
    raw: str = ""  # original payload, truncated to 4 KB
    ts_ms: int = Field(default_factory=lambda: int(time.time() * 1000))

    model_config = {"populate_by_name": True}


# ── 4. road.events ────────────────────────────────────────────────────────────

DetectionType = Literal["POTHOLE", "SPEED_BREAKER", "WATERLOGGING", "SLIPPERY", "HARSH_BRAKE"]


class DetectionEvidence(BaseModel):
    peak_g: float = 0.0
    speed_kmh: float = 0.0
    pre_decel_kmh: float = 0.0


class Detection(BaseModel):
    type: DetectionType
    severity: float
    score: float = 0.0
    lat: float = 0.0
    lon: float = 0.0
    evidence: DetectionEvidence = Field(default_factory=DetectionEvidence)


class RoadEvent(BaseModel):
    schema_: str = Field("rs.observation.v1", alias="schema")
    obs_id: str
    vehicle_id: str
    vehicle_class: VehicleClass
    h3_r11: str
    h3_r9: str
    enter_ms: int
    exit_ms: int
    mean_speed_kmh: float
    heading_deg: float
    lat: float
    lon: float
    detections: list[Detection] = Field(default_factory=list)
    trace: Trace = Field(default_factory=Trace)

    model_config = {"populate_by_name": True}


# ── 5. hazards.state ─────────────────────────────────────────────────────────

HazardStatus = Literal["CANDIDATE", "CONFIRMED", "DECAYING", "CLEARED"]
SeverityLabel = Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"]


class HazardCentroid(BaseModel):
    lat: float
    lon: float


class HazardInternal(BaseModel):
    log_odds: float = 0.0


class HazardState(BaseModel):
    schema_: str = Field("rs.hazard.v1", alias="schema")
    hazard_id: str
    hazard_type: DetectionType
    status: HazardStatus
    status_reason: str = ""
    confidence: float
    severity_score: float
    severity_label: SeverityLabel
    advisory_speed_kmh: Optional[int] = None
    h3_cell: str
    h3_res: int
    lat: float
    lon: float
    centroid: HazardCentroid
    radius_m: float = 15.0
    distinct_vehicles: int = 0
    positive_reports: int = 0
    clean_passes: int = 0
    reporters: list[str] = Field(default_factory=list)
    first_seen_ms: int = 0
    confirmed_ms: int = 0
    cleared_ms: Optional[int] = None
    last_update_ms: int = 0
    version: int = 0
    internal: HazardInternal = Field(default_factory=HazardInternal)
    trace: Trace = Field(default_factory=Trace)

    model_config = {"populate_by_name": True}


# ── 6. vehicles.reputation ────────────────────────────────────────────────────

class VehicleReputation(BaseModel):
    schema_: str = Field("rs.reputation.v1", alias="schema")
    vehicle_id: str
    reputation: float
    quarantined: bool = False
    confirmed_reports: int = 0
    rejected_reports: int = 0
    updated_ms: int = 0

    model_config = {"populate_by_name": True}


# ── 9. ui.events ─────────────────────────────────────────────────────────────

class DriverAlert(BaseModel):
    type: Literal["driver_alert"] = "driver_alert"
    alert_id: str
    vehicle_id: str
    hazard_id: str
    hazard_type: DetectionType
    severity_label: SeverityLabel
    level: Literal["advisory", "warning"]
    distance_m: float
    eta_s: float
    advisory_speed_kmh: Optional[int]
    message: str
    ts_ms: int
    trace: Trace = Field(default_factory=Trace)


class HazardUpdate(BaseModel):
    type: Literal["hazard_update"] = "hazard_update"
    hazard: HazardState


class AlertOutcome(BaseModel):
    type: Literal["alert_outcome"] = "alert_outcome"
    vehicle_id: str
    hazard_id: str
    speed_at_alert_kmh: float
    speed_at_hazard_kmh: float


class ReputationUpdate(BaseModel):
    type: Literal["reputation_update"] = "reputation_update"
    reputation: VehicleReputation


class VehiclePosition(BaseModel):
    id: str
    c: str    # vehicle_class
    lat: float
    lon: float
    h: float  # heading_deg
    s: float  # speed_kmh


class VehiclePositions(BaseModel):
    type: Literal["vehicle_positions"] = "vehicle_positions"
    ts_ms: int
    vehicles: list[VehiclePosition]


class LatencyStats(BaseModel):
    emit_to_normalize_p50: float = 0.0
    emit_to_normalize_p95: float = 0.0
    emit_to_fuse_p50: float = 0.0
    emit_to_fuse_p95: float = 0.0
    emit_to_alert_p50: float = 0.0
    emit_to_alert_p95: float = 0.0


class PipelineStats(BaseModel):
    type: Literal["pipeline_stats"] = "pipeline_stats"
    ts_ms: int
    msgs_per_s: dict[str, float]
    latency_ms: LatencyStats


# ── 11. Ground truth (simulator → API) ───────────────────────────────────────

class GroundTruth(BaseModel):
    gt_id: str
    hazard_type: DetectionType
    lat: float
    lon: float
    radius_m: float
    severity: float
    created_ms: int
    repaired_ms: Optional[int] = None
    is_hazard: bool = True

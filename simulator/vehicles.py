"""
vehicles.py – Vehicle kinematics, IMU/CAN signal synthesis, data-quality noise.

Implements algorithms.md § A signal model.

Each vehicle:
  - Follows random shortest-path routes on the road graph.
  - Target speed = edge_speed × U(0.7, 1.05), accel limits ±0.25 g.
  - Slows before turns.
  - GPS jitter σ = 3 m.
  - IMU: 1 s summary (az_max, az_min, az_std, ax_min, ax_max, peak_offset_ms).
  - Hazard effects applied by HazardRegistry.

Data-quality noise (DLQ demo):
  - 0.5% malformed JSON
  - 0.5% duplicate seq
  - 0.2% delayed 45 s
  - 0.1% GPS teleport
"""

from __future__ import annotations

import json
import math
import random
import time
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from simulator.hazards import HazardRegistry, GroundTruthHazard

_G = 9.80665  # m/s²
_MAX_ACCEL_G = 0.25
_GPS_JITTER_M = 3.0

# Class mix: 50% car, 25% two_wheeler, 15% truck, 10% bus
_CLASS_WEIGHTS = [("car", 0.50), ("two_wheeler", 0.25), ("truck", 0.15), ("bus", 0.10)]

# IMU class factors for vertical noise
_CLASS_FACTOR_IMU = {"car": 1.0, "truck": 0.8, "bus": 0.7, "two_wheeler": 1.6}


def _pick_class(rng: random.Random) -> str:
    r = rng.random()
    cumulative = 0.0
    for cls, w in _CLASS_WEIGHTS:
        cumulative += w
        if r < cumulative:
            return cls
    return "car"


def _jitter(lat: float, lon: float, sigma_m: float, rng: random.Random) -> tuple[float, float]:
    dlat = rng.gauss(0, sigma_m) / 111_111.0
    dlon = rng.gauss(0, sigma_m) / (111_111.0 * math.cos(math.radians(lat)))
    return lat + dlat, lon + dlon


@dataclass
class RoadEdge:
    u: int
    v: int
    lat_u: float
    lon_u: float
    lat_v: float
    lon_v: float
    speed_limit_kmh: float
    length_m: float


@dataclass
class Vehicle:
    vehicle_id: str
    vehicle_class: str
    lat: float
    lon: float
    speed_kmh: float
    heading_deg: float
    seq: int = 0
    _rng: random.Random = field(default_factory=random.Random, repr=False)
    _target_speed: float = 0.0
    _route: list[RoadEdge] = field(default_factory=list, repr=False)
    _edge_idx: int = 0
    _progress_m: float = 0.0
    _hmi_advisory: Optional[float] = None  # advisory speed from HMI alert
    _hmi_hazard_lat: float = 0.0
    _hmi_hazard_lon: float = 0.0
    _last_duplicate_seq: Optional[int] = None  # for DLQ duplicate injection
    _delay_buffer: list[bytes] = field(default_factory=list, repr=False)

    def set_hmi_alert(self, advisory_kmh: float, hazard_lat: float, hazard_lon: float) -> None:
        self._hmi_advisory = advisory_kmh
        self._hmi_hazard_lat = hazard_lat
        self._hmi_hazard_lon = hazard_lon


class VehicleFleet:
    """
    Creates and advances all vehicles each tick.
    """

    def __init__(
        self,
        edges: list[RoadEdge],
        num_vehicles: int,
        registry: HazardRegistry,
        rng_seed: int = 0,
    ) -> None:
        self._edges = edges
        self._registry = registry
        self._rng = random.Random(rng_seed)
        self._np_rng = np.random.default_rng(rng_seed)
        self._vehicles: list[Vehicle] = []
        self._create_vehicles(num_vehicles)

    def _create_vehicles(self, n: int) -> None:
        for i in range(n):
            cls = _pick_class(self._rng)
            # Spawn at a random edge midpoint
            edge = self._rng.choice(self._edges)
            mid_lat = (edge.lat_u + edge.lat_v) / 2
            mid_lon = (edge.lon_u + edge.lon_v) / 2
            speed_limit = edge.speed_limit_kmh
            target = speed_limit * self._rng.uniform(0.7, 1.05)
            v = Vehicle(
                vehicle_id=f"SIM-{cls.upper()[:3]}-{i:04d}",
                vehicle_class=cls,
                lat=mid_lat,
                lon=mid_lon,
                speed_kmh=target * 0.8,
                heading_deg=self._edge_heading(edge),
                seq=self._rng.randint(1, 1000),
                _target_speed=target,
                _route=[edge],
                _edge_idx=0,
                _progress_m=0.0,
            )
            v._rng = random.Random(self._rng.randint(0, 2**32))
            self._vehicles.append(v)

    @staticmethod
    def _edge_heading(edge: RoadEdge) -> float:
        dlat = edge.lat_v - edge.lat_u
        dlon = edge.lon_v - edge.lon_u
        return (math.degrees(math.atan2(dlon, dlat)) + 360) % 360

    @property
    def vehicles(self) -> list[Vehicle]:
        return self._vehicles

    def tick(self, dt_s: float = 1.0) -> None:
        """Advance all vehicles by dt_s seconds."""
        for v in self._vehicles:
            self._advance(v, dt_s)

    def _advance(self, v: Vehicle, dt_s: float) -> None:
        """Move vehicle along its route."""
        if not v._route:
            # Pick a new random edge
            v._route = [self._rng.choice(self._edges)]
            v._edge_idx = 0
            v._progress_m = 0.0

        edge = v._route[v._edge_idx]

        # Update speed toward target (±0.25 g accel)
        max_delta = _MAX_ACCEL_G * _G * dt_s * 3.6  # km/h per second
        diff = v._target_speed - v.speed_kmh
        v.speed_kmh += max(min(diff, max_delta), -max_delta)
        v.speed_kmh = max(0.0, v.speed_kmh)

        # Advance along edge
        dist_m = v.speed_kmh * 1000.0 / 3600.0 * dt_s
        v._progress_m += dist_m

        t = min(v._progress_m / max(edge.length_m, 0.1), 1.0)
        v.lat = edge.lat_u + t * (edge.lat_v - edge.lat_u)
        v.lon = edge.lon_u + t * (edge.lon_v - edge.lon_u)
        v.heading_deg = self._edge_heading(edge)

        if v._progress_m >= edge.length_m:
            # Move to next edge (pick randomly)
            v._edge_idx = 0
            v._route = [self._rng.choice(self._edges)]
            v._progress_m = 0.0
            # New target speed
            new_edge = v._route[0]
            v._target_speed = new_edge.speed_limit_kmh * v._rng.uniform(0.7, 1.05)

    def build_message(
        self,
        v: Vehicle,
        now_ms: int,
        is_faulty: bool = False,
    ) -> dict:
        """Build the telemetry JSON dict for one vehicle."""
        rng = v._rng

        lat, lon = _jitter(v.lat, v.lon, _GPS_JITTER_M, rng)
        speed = max(0.0, v.speed_kmh + rng.gauss(0, 0.5))

        cls = v.vehicle_class
        cf = _CLASS_FACTOR_IMU.get(cls, 1.0)
        road_roughness = rng.uniform(0.02, 0.06)
        az_baseline_std = road_roughness * cf * (0.6 + speed / 80)

        # Sample 100 "virtual" IMU ticks, take summary stats
        az_samples = self._np_rng.normal(0, az_baseline_std, 100)
        ax_samples = self._np_rng.normal(0, 0.02, 100)
        peak_idx = int(np.argmax(np.abs(az_samples)))

        # Faulty vehicle: inject random spikes anywhere
        if is_faulty and rng.random() < 0.05:  # ~5 s average interval at 100 Hz
            spike = rng.uniform(0.6, 1.2) * rng.choice([-1, 1])
            az_samples[peak_idx] = spike

        # Hazard effects
        ax_offset = 0.0
        wiper = 0
        abs_active = False
        esc_active = False
        brake_pressure = 0.0
        throttle_pct = max(0.0, min(100.0, 20 + rng.gauss(0, 5)))

        lateral_offset = rng.uniform(-1.5, 1.5)  # lane offset for pothole miss

        # Check POTHOLE
        h_p, d_p = self._registry.is_near(v.lat, v.lon, "POTHOLE", abs(lateral_offset))
        if h_p and d_p <= 2.5:
            A = h_p.severity * (0.4 + 0.012 * speed) * cf
            # negative-then-positive spike
            az_samples[40:50] -= A * 0.6
            az_samples[50:60] += A
            # Apply HMI advisory slow-down
            if v._hmi_advisory is not None:
                dist_to_haz = math.sqrt(
                    (v.lat - v._hmi_hazard_lat) ** 2 + (v.lon - v._hmi_hazard_lon) ** 2
                ) * 111_111
                if dist_to_haz < 150:
                    v._target_speed = min(v._target_speed, v._hmi_advisory)

        # Check SPEED_BREAKER
        h_sb, d_sb = self._registry.is_near(v.lat, v.lon, "SPEED_BREAKER")
        if h_sb and d_sb <= 10:
            if speed > 15:
                ax_offset = rng.uniform(-0.3, -0.15)  # pre-braking
                v._target_speed = rng.uniform(12, 18)
            bump = rng.uniform(0.3, 0.5)
            az_samples[30:50] += bump
            az_samples[50:70] -= bump

        # Check WATERLOGGING
        h_w, d_w = self._registry.is_near(v.lat, v.lon, "WATERLOGGING")
        if h_w and d_w <= h_w.radius_m and self._registry.rain_on:
            v._target_speed = min(v._target_speed, rng.uniform(8, 15))
            wiper = rng.choice([2, 3])
            if rng.random() < 0.3:
                abs_active = True
                brake_pressure = rng.uniform(1, 5)

        # Check SLIPPERY
        h_sl, d_sl = self._registry.is_near(v.lat, v.lon, "SLIPPERY")
        if h_sl and d_sl <= h_sl.radius_m and rng.random() < 0.5:
            abs_active = rng.random() < 0.5
            esc_active = not abs_active
            brake_pressure = rng.uniform(5, 15)

        # Check HARSH_BRAKE
        h_hb, d_hb = self._registry.is_near(v.lat, v.lon, "HARSH_BRAKE")
        if h_hb and d_hb <= h_hb.radius_m and rng.random() < 0.5:
            ax_offset = min(ax_offset, rng.uniform(-0.9, -0.5))
            brake_pressure = max(brake_pressure, rng.uniform(10, 25))

        ax_samples += ax_offset

        az_max = float(np.max(az_samples))
        az_min = float(np.min(az_samples))
        az_std = float(np.std(az_samples))
        ax_min = float(np.min(ax_samples))
        ax_max = float(np.max(ax_samples))
        peak_offset_ms = int(peak_idx * 10)  # 100 Hz → ms

        v.seq += 1

        return {
            "schema": "rs.telemetry.v1",
            "vehicle_id": v.vehicle_id,
            "vehicle_class": cls,
            "seq": v.seq,
            "ts_ms": now_ms,
            "gps": {
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "speed_kmh": round(speed, 2),
                "heading_deg": round(v.heading_deg, 1),
                "hdop": round(rng.uniform(0.8, 1.5), 2),
            },
            "imu": {
                "az_max_g": round(az_max, 4),
                "az_min_g": round(az_min, 4),
                "az_std_g": round(az_std, 4),
                "ax_min_g": round(ax_min, 4),
                "ax_max_g": round(ax_max, 4),
                "peak_offset_ms": peak_offset_ms,
            },
            "can": {
                "wheel_speed_kmh": round(speed + rng.gauss(0, 0.3), 2),
                "brake_pressure_bar": round(brake_pressure, 2),
                "abs_active": abs_active,
                "esc_active": esc_active,
                "wiper_level": wiper,
                "fog_lamp": self._registry.rain_on and rng.random() < 0.3,
                "throttle_pct": round(throttle_pct if brake_pressure == 0 else 0.0, 1),
            },
            "trace": {
                "emitted_ms": now_ms,
            },
        }


def inject_noise(msg_bytes: bytes, rng: random.Random) -> tuple[bytes, bool, bool]:
    """
    Inject data-quality noise per algorithms.md § A.
    Returns (payload_bytes, is_delayed, is_duplicate).

    0.5% malformed JSON (returns damaged bytes)
    0.5% duplicate (same seq as previous tick – handled in run.py)
    0.2% delayed by 45 s (caller skips this message for 45 s)
    0.1% GPS teleport (inject far-away coords)
    """
    r = rng.random()
    is_delayed = False
    is_duplicate = False

    if r < 0.001:  # 0.1% teleport
        try:
            d = json.loads(msg_bytes)
            d["gps"]["lat"] = rng.uniform(10, 35)
            d["gps"]["lon"] = rng.uniform(70, 90)
            return json.dumps(d).encode(), False, False
        except Exception:
            pass

    elif r < 0.003:  # 0.2% delayed
        is_delayed = True

    elif r < 0.008:  # 0.5% duplicate seq
        is_duplicate = True

    elif r < 0.013:  # 0.5% malformed JSON
        # Truncate the JSON to break it
        half = max(1, len(msg_bytes) // 2)
        return msg_bytes[:half], False, False

    return msg_bytes, is_delayed, is_duplicate

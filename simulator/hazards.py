"""
hazards.py – Ground-truth hazard registry.

Seed scenario (algorithms.md § A):
  - 12 potholes (severity 0.3–0.95)
  - 4 speed breakers
  - 2 slippery patches
  - 2 sudden-braking hotspots
  - 2 waterlogging zones (inactive until rain)

Provides:
  - is_near_hazard(lat, lon, htype) → (hazard | None, distance_m)
  - effect(vehicle, hazard, speed_kmh) → dict of signal modifications
  - POST to API_URL/sim/ground-truth if configured
"""

from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass, field
from typing import Optional

try:
    import httpx as _httpx
    _HAVE_HTTPX = True
except ImportError:
    _HAVE_HTTPX = False

_LAT0 = 12.9352
_LON0 = 77.6245
_HAVERSINE_R = 6_371_000.0


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * _HAVERSINE_R * math.asin(math.sqrt(a))


def _rand_offset(max_m: float = 1800.0) -> tuple[float, float]:
    """Random lat/lon offset within max_m metres of the centre."""
    r_m = random.uniform(50, max_m)
    theta = random.uniform(0, 2 * math.pi)
    dlat = (r_m * math.cos(theta)) / _HAVERSINE_R * math.degrees(1)
    dlon = (r_m * math.sin(theta)) / (_HAVERSINE_R * math.cos(math.radians(_LAT0))) * math.degrees(1)
    return _LAT0 + dlat, _LON0 + dlon


@dataclass
class GroundTruthHazard:
    gt_id: str
    hazard_type: str       # POTHOLE | SPEED_BREAKER | SLIPPERY | HARSH_BRAKE | WATERLOGGING
    lat: float
    lon: float
    radius_m: float
    severity: float        # 0–1
    is_hazard: bool        # False for SPEED_BREAKER (known road feature)
    created_ms: int
    repaired_ms: Optional[int] = None
    active: bool = True    # waterlogging zones start inactive


def _seed_hazards() -> list[GroundTruthHazard]:
    rng = random.Random(42)  # deterministic seed for reproducibility
    now = int(time.time() * 1000)
    hazards: list[GroundTruthHazard] = []

    # 12 potholes
    for i in range(12):
        lat, lon = _rand_offset(1800)
        # Apply rng-based offset for determinism
        lat += rng.uniform(-0.005, 0.005)
        lon += rng.uniform(-0.005, 0.005)
        hazards.append(GroundTruthHazard(
            gt_id=f"GT-POTHOLE-{i+1:02d}",
            hazard_type="POTHOLE",
            lat=lat, lon=lon,
            radius_m=rng.uniform(2, 5),
            severity=rng.uniform(0.3, 0.95),
            is_hazard=True,
            created_ms=now,
        ))

    # 4 speed breakers
    for i in range(4):
        lat = _LAT0 + rng.uniform(-0.015, 0.015)
        lon = _LON0 + rng.uniform(-0.015, 0.015)
        hazards.append(GroundTruthHazard(
            gt_id=f"GT-SPEEDBREAKER-{i+1:02d}",
            hazard_type="SPEED_BREAKER",
            lat=lat, lon=lon,
            radius_m=5.0,
            severity=0.5,
            is_hazard=False,  # known road feature
            created_ms=now,
            active=True,
        ))

    # 2 slippery patches
    for i in range(2):
        lat = _LAT0 + rng.uniform(-0.012, 0.012)
        lon = _LON0 + rng.uniform(-0.012, 0.012)
        hazards.append(GroundTruthHazard(
            gt_id=f"GT-SLIPPERY-{i+1:02d}",
            hazard_type="SLIPPERY",
            lat=lat, lon=lon,
            radius_m=20.0,
            severity=0.6,
            is_hazard=True,
            created_ms=now,
        ))

    # 2 sudden-braking hotspots
    for i in range(2):
        lat = _LAT0 + rng.uniform(-0.013, 0.013)
        lon = _LON0 + rng.uniform(-0.013, 0.013)
        hazards.append(GroundTruthHazard(
            gt_id=f"GT-HARSHBRAKE-{i+1:02d}",
            hazard_type="HARSH_BRAKE",
            lat=lat, lon=lon,
            radius_m=10.0,
            severity=0.5,
            is_hazard=True,
            created_ms=now,
        ))

    # 2 waterlogging zones (inactive until rain command)
    for i in range(2):
        lat = _LAT0 + rng.uniform(-0.010, 0.010)
        lon = _LON0 + rng.uniform(-0.010, 0.010)
        radius = rng.uniform(60, 120)
        hazards.append(GroundTruthHazard(
            gt_id=f"GT-WATERLOGGING-{i+1:02d}",
            hazard_type="WATERLOGGING",
            lat=lat, lon=lon,
            radius_m=radius,
            severity=0.7,
            is_hazard=True,
            created_ms=now,
            active=False,  # inactive until rain
        ))

    return hazards


class HazardRegistry:
    def __init__(self) -> None:
        self._hazards: list[GroundTruthHazard] = _seed_hazards()
        self.rain_on: bool = False

    @property
    def hazards(self) -> list[GroundTruthHazard]:
        return self._hazards

    def is_near(
        self,
        lat: float,
        lon: float,
        htype: str,
        lateral_offset_m: float = 0.0,
    ) -> tuple[Optional[GroundTruthHazard], float]:
        """
        Return (hazard, distance_m) for the nearest active hazard of htype within trigger radius,
        or (None, inf) if none.  lateral_offset_m: ~30% miss chance for potholes.
        """
        best: Optional[GroundTruthHazard] = None
        best_d = math.inf
        for h in self._hazards:
            if h.hazard_type != htype:
                continue
            if not h.active:
                continue
            if h.repaired_ms is not None:
                continue
            d = _haversine(lat, lon, h.lat, h.lon)
            trigger_r = h.radius_m
            if htype == "POTHOLE":
                trigger_r = 2.5  # 2.5 m trigger radius for pothole

            # Apply lateral miss for potholes
            effective_d = math.sqrt(d ** 2 + lateral_offset_m ** 2) if htype == "POTHOLE" else d

            if effective_d <= trigger_r and effective_d < best_d:
                best = h
                best_d = effective_d
        return best, best_d

    def set_rain(self, on: bool) -> None:
        self.rain_on = on
        for h in self._hazards:
            if h.hazard_type == "WATERLOGGING":
                h.active = on

    def repair(self, gt_id: Optional[str] = None, lat: Optional[float] = None,
               lon: Optional[float] = None) -> Optional[GroundTruthHazard]:
        now = int(time.time() * 1000)
        if gt_id:
            for h in self._hazards:
                if h.gt_id == gt_id:
                    h.repaired_ms = now
                    h.active = False
                    return h
        if lat is not None and lon is not None:
            best: Optional[GroundTruthHazard] = None
            best_d = math.inf
            for h in self._hazards:
                d = _haversine(lat, lon, h.lat, h.lon)
                if d < best_d:
                    best_d = d
                    best = h
            if best:
                best.repaired_ms = now
                best.active = False
            return best
        return None

    def add_hazard(self, htype: str, lat: float, lon: float, severity: float) -> GroundTruthHazard:
        idx = sum(1 for h in self._hazards if h.hazard_type == htype) + 1
        h = GroundTruthHazard(
            gt_id=f"GT-{htype}-DYN-{idx:02d}",
            hazard_type=htype,
            lat=lat, lon=lon,
            radius_m=5.0 if htype == "POTHOLE" else 15.0,
            severity=severity,
            is_hazard=(htype != "SPEED_BREAKER"),
            created_ms=int(time.time() * 1000),
            active=True,
        )
        self._hazards.append(h)
        return h

    def post_ground_truth(self, api_url: str, api_key: str) -> None:
        """POST all hazards to API_URL/sim/ground-truth."""
        if not _HAVE_HTTPX:
            return
        for h in self._hazards:
            payload = {
                "gt_id": h.gt_id,
                "hazard_type": h.hazard_type,
                "lat": h.lat,
                "lon": h.lon,
                "radius_m": h.radius_m,
                "severity": h.severity,
                "created_ms": h.created_ms,
                "repaired_ms": h.repaired_ms,
                "is_hazard": h.is_hazard,
            }
            try:
                with _httpx.Client(timeout=5) as client:
                    client.post(
                        f"{api_url}/sim/ground-truth",
                        json=payload,
                        headers={"X-RS-Key": api_key},
                    )
            except Exception as exc:
                print(f"[hazards] ground-truth POST failed: {exc}")

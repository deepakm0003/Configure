"""
geo.py – h3 v4 helpers, haversine distance, bearing, angle difference, is_ahead test.

Uses h3 v4 API: latlng_to_cell, cell_to_latlng, grid_disk, great_circle_distance.
All angles are in degrees, distances in metres.
"""

from __future__ import annotations

import math
import h3


# ── H3 helpers ────────────────────────────────────────────────────────────────

def latlng_to_cell(lat: float, lon: float, resolution: int) -> str:
    """Return the h3 cell index at the given resolution."""
    return h3.latlng_to_cell(lat, lon, resolution)


def cell_to_latlng(cell: str) -> tuple[float, float]:
    """Return (lat, lon) of the cell centre."""
    return h3.cell_to_latlng(cell)


def cell_disk(cell: str, k: int) -> set[str]:
    """Return all cells within k rings of cell (inclusive)."""
    return set(h3.grid_disk(cell, k))


def h3_great_circle_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Great-circle distance in metres using h3's built-in function.
    h3.great_circle_distance returns km by default; we request metres.
    """
    return h3.great_circle_distance((lat1, lon1), (lat2, lon2), unit="m")


# ── Haversine ─────────────────────────────────────────────────────────────────

_R_M = 6_371_000.0  # Earth radius in metres


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return great-circle distance in metres between two WGS-84 points."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * _R_M * math.asin(math.sqrt(a))


# ── Bearing ───────────────────────────────────────────────────────────────────

def bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Initial bearing (degrees, 0–360) from point 1 to point 2 (North = 0).
    """
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dlam = math.radians(lon2 - lon1)
    x = math.sin(dlam) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlam)
    b = math.degrees(math.atan2(x, y))
    return (b + 360) % 360


# ── Angle difference ─────────────────────────────────────────────────────────

def angle_diff(a: float, b: float) -> float:
    """
    Smallest signed angular difference (degrees) between headings a and b.
    Result is in [-180, 180].
    """
    diff = (b - a + 180) % 360 - 180
    return diff


def angle_diff_abs(a: float, b: float) -> float:
    """Absolute value of angle_diff (always ≥ 0)."""
    return abs(angle_diff(a, b))


# ── Is ahead ─────────────────────────────────────────────────────────────────

def is_ahead(
    vehicle_lat: float,
    vehicle_lon: float,
    vehicle_heading: float,
    target_lat: float,
    target_lon: float,
    max_angle_deg: float = 30.0,
    max_dist_m: float = 500.0,
    extra_dist_m: float = 0.0,
) -> tuple[bool, float, float]:
    """
    Return (ahead, distance_m, bearing_to_target).

    A target is "ahead" when:
      - The bearing from the vehicle to the target is within max_angle_deg of the
        vehicle's heading.
      - The distance is ≤ max_dist_m + extra_dist_m (extra_dist_m is used for
        waterlogging zone radius).
    """
    d = haversine(vehicle_lat, vehicle_lon, target_lat, target_lon)
    b = bearing(vehicle_lat, vehicle_lon, target_lat, target_lon)
    delta = angle_diff_abs(vehicle_heading, b)
    ahead = delta <= max_angle_deg and d <= (max_dist_m + extra_dist_m)
    return ahead, d, b

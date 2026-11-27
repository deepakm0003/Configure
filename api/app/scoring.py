"""
scoring.py – Live accuracy scoring per algorithms.md § F.

Recomputed every 2 s, broadcast as score_update.

- GT hazard is **detected** if any CONFIRMED hazard of the same type lies within 25 m
  (WATERLOGGING: within zone radius + 50 m).
- **Precision** = confirmed matching GT / all confirmed (excluding SPEED_BREAKER).
- **Recall** = detected active GT / active GT with ≥3 passes.
- **Classification accuracy:** speed-breaker GT wrongly CONFIRMED as POTHOLE → false positive.
- **Time-to-detect** = confirmed_ms - max(gt.created_ms, sim_start_ms), mean & p90.
- **False hazards prevented** = CANDIDATEs cleared as "unconfirmed" with mostly quarantined reporters.
"""

from __future__ import annotations

import asyncio
import time
import math
from typing import Any

from api.app.db import get_pool
from api.app.ws import hub


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return great-circle distance in metres."""
    R = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


async def compute_score() -> dict[str, Any]:
    """
    Compute precision, recall, TTD, false hazards prevented.
    
    Returns a dict ready for broadcast.
    """
    pool = get_pool()
    
    # Fetch all CONFIRMED hazards (excluding SPEED_BREAKER for precision/recall)
    confirmed_q = """
        SELECT hazard_id, hazard_type, lat, lon, confirmed_ms
        FROM hazards
        WHERE status = 'CONFIRMED'
    """
    confirmed_all = await pool.fetch(confirmed_q)
    confirmed = [dict(r) for r in confirmed_all if r["hazard_type"] != "SPEED_BREAKER"]
    
    # Fetch active ground truth (is_hazard=true, not repaired)
    gt_q = """
        SELECT gt_id, hazard_type, lat, lon, radius_m, created_ms, is_hazard
        FROM ground_truth
        WHERE is_hazard = TRUE AND repaired_ms IS NULL
    """
    gt_all = await pool.fetch(gt_q)
    gt_active = [dict(r) for r in gt_all]
    
    # For recall: filter GT to those with ≥3 vehicle passes (TODO: track pass count)
    # For now assume all active GT have been passed
    gt_eligible = gt_active
    
    # Precision: how many confirmed match a GT?
    matched_confirmed = 0
    for hz in confirmed:
        for gt in gt_active:
            if hz["hazard_type"] != gt["hazard_type"]:
                continue
            d = _haversine(hz["lat"], hz["lon"], gt["lat"], gt["lon"])
            thresh = 25.0 if gt["hazard_type"] != "WATERLOGGING" else gt["radius_m"] + 50.0
            if d <= thresh:
                matched_confirmed += 1
                break
    
    precision = matched_confirmed / len(confirmed) if confirmed else 0.0
    
    # Recall: how many GT have a confirmed match?
    detected_gt = 0
    for gt in gt_eligible:
        for hz in confirmed_all:  # include SPEED_BREAKER for detection
            if hz["hazard_type"] != gt["hazard_type"]:
                continue
            d = _haversine(hz["lat"], hz["lon"], gt["lat"], gt["lon"])
            thresh = 25.0 if gt["hazard_type"] != "WATERLOGGING" else gt["radius_m"] + 50.0
            if d <= thresh:
                detected_gt += 1
                break
    
    recall = detected_gt / len(gt_eligible) if gt_eligible else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    
    # Time-to-detect (TTD)
    ttd_samples: list[float] = []
    sim_start = 0  # TODO: track sim start time; for now use GT min created_ms
    if gt_active:
        sim_start = min(g["created_ms"] for g in gt_active)
    
    for gt in gt_eligible:
        for hz in confirmed_all:
            if hz["hazard_type"] != gt["hazard_type"]:
                continue
            d = _haversine(hz["lat"], hz["lon"], gt["lat"], gt["lon"])
            thresh = 25.0 if gt["hazard_type"] != "WATERLOGGING" else gt["radius_m"] + 50.0
            if d <= thresh and hz["confirmed_ms"]:
                ttd_ms = hz["confirmed_ms"] - max(gt["created_ms"], sim_start)
                if ttd_ms > 0:
                    ttd_samples.append(ttd_ms / 1000.0)  # seconds
                break
    
    ttd_mean = sum(ttd_samples) / len(ttd_samples) if ttd_samples else 0.0
    ttd_p90 = sorted(ttd_samples)[int(0.9 * len(ttd_samples))] if ttd_samples else 0.0
    
    # False hazards prevented (CANDIDATEs cleared as "unconfirmed")
    # TODO: track this in DB; for now estimate from status transitions
    false_prevented = 0  # placeholder
    
    return {
        "type": "score_update",
        "ts_ms": int(time.time() * 1000),
        "precision": round(precision, 3),
        "recall": round(recall, 3),
        "f1": round(f1, 3),
        "confirmed_hazards": len(confirmed),
        "active_gt_hazards": len(gt_eligible),
        "detected_gt_hazards": detected_gt,
        "ttd_mean_s": round(ttd_mean, 2),
        "ttd_p90_s": round(ttd_p90, 2),
        "false_hazards_prevented": false_prevented,
    }


async def scoring_loop() -> None:
    """Background task: compute score every 2 s and broadcast."""
    while True:
        await asyncio.sleep(2.0)
        try:
            score = await compute_score()
            hub.broadcast(score)
        except Exception as exc:
            print(f"[scoring] error: {exc}")

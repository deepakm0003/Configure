---
inclusion: always
---

# Data contracts (every topic, MQTT channel and API payload)

All payloads are JSON (UTF-8). Timestamps are **epoch milliseconds** (`*_ms`). Distances are in metres, speeds in km/h, accelerations in g. Every message has a `schema` field with a version. Topic names below omit the prefix and use dots for readability. The real name is `iiitd_` + the name with dots replaced by underscores (e.g. `telemetry.raw` → `iiitd_telemetry_raw`).

## 1. MQTT `rs/telemetry/<vehicle_id>` → Kafka `telemetry.raw` (key: vehicle_id)

The simulator publishes 1 message per vehicle per second. The IMU block is a **1-second edge summary** of 100 Hz samples (as a real TCU would send).

```json
{
  "schema": "rs.telemetry.v1",
  "vehicle_id": "SIM-CAR-0042",
  "vehicle_class": "car",            // car | bus | truck | two_wheeler
  "seq": 1834,
  "ts_ms": 1764300000123,
  "gps": { "lat": 12.93512, "lon": 77.62451, "speed_kmh": 38.4, "heading_deg": 87.0, "hdop": 0.9 },
  "imu": {
    "az_max_g": 0.08, "az_min_g": -0.07, "az_std_g": 0.031,   // vertical accel deviation from 1 g
    "ax_min_g": -0.05, "ax_max_g": 0.04,                         // longitudinal (negative = braking)
    "peak_offset_ms": 430                                        // when |az| peaked inside the window
  },
  "can": {
    "wheel_speed_kmh": 38.9, "brake_pressure_bar": 0.0,
    "abs_active": false, "esc_active": false,
    "wiper_level": 0,                 // 0 off, 1 low, 2 high, 3 max
    "fog_lamp": false, "throttle_pct": 22.0
  },
  "trace": { "emitted_ms": 1764300000125 }
}
```

## 2. `telemetry.clean` (key: vehicle_id)

The same as raw, plus:

```json
{
  "schema": "rs.telemetry.clean.v1",
  "h3_r11": "8b618925a2dbfff",
  "h3_r9": "89618925a2fffff",
  "quality": { "speed_mismatch": false },
  "trace": { "emitted_ms": 0, "normalize_ms": 0 }
}
```

## 3. `telemetry.dlq` (key: vehicle_id or "unknown")

```json
{ "schema": "rs.dlq.v1", "reason": "SCHEMA_INVALID|OUT_OF_RANGE|IMPOSSIBLE_JUMP|LATE_EVENT|CLOCK_SKEW|DUPLICATE",
  "detail": "implied speed 412 km/h", "raw": "<original payload as string, truncated 4 KB>", "ts_ms": 0 }
```

## 4. `road.events` = **cell observations** (key: h3 res-9 parent of the cell)

rs-detect emits **one observation per vehicle per res-11 cell traversal** (on cell exit, or every 10 s if the vehicle stays in one cell). An empty `detections` list is a **clean pass**, which rs-fuse uses as negative evidence.

```json
{
  "schema": "rs.observation.v1",
  "obs_id": "SIM-CAR-0042:1834",
  "vehicle_id": "SIM-CAR-0042",
  "vehicle_class": "car",
  "h3_r11": "8b618925a2dbfff",
  "h3_r9": "89618925a2fffff",
  "enter_ms": 1764300000123, "exit_ms": 1764300002400,
  "mean_speed_kmh": 36.2, "heading_deg": 87.0,
  "lat": 12.93512, "lon": 77.62451,
  "detections": [
    { "type": "POTHOLE", "severity": 0.72, "score": 6.8, "lat": 12.93514, "lon": 77.62455,
      "evidence": { "peak_g": 0.81, "speed_kmh": 41.0, "pre_decel_kmh": 2.0 } }
  ],
  "trace": { "emitted_ms": 0, "normalize_ms": 0, "detect_ms": 0 }
}
```

Detection `type` ∈ `POTHOLE | SPEED_BREAKER | WATERLOGGING | SLIPPERY | HARSH_BRAKE`.

## 5. `hazards.state` (key: hazard_id, **compacted**)

```json
{
  "schema": "rs.hazard.v1",
  "hazard_id": "POTHOLE:8b618925a2dbfff",
  "hazard_type": "POTHOLE",
  "status": "CONFIRMED",                       // CANDIDATE | CONFIRMED | DECAYING | CLEARED
  "status_reason": "3 vehicles, conf 0.91",
  "confidence": 0.91,
  "severity_score": 0.68,
  "severity_label": "HIGH",                    // LOW | MEDIUM | HIGH | CRITICAL
  "advisory_speed_kmh": 20,
  "h3_cell": "8b618925a2dbfff", "h3_res": 11,
  "lat": 12.93514, "lon": 77.62455,            // flat copy (for the PG connector)
  "centroid": { "lat": 12.93514, "lon": 77.62455 },
  "radius_m": 15,
  "distinct_vehicles": 3, "positive_reports": 4, "clean_passes": 1,
  "reporters": ["SIM-CAR-0042", "SIM-BUS-0007", "SIM-TRK-0013"],
  "first_seen_ms": 0, "confirmed_ms": 0, "cleared_ms": null, "last_update_ms": 0,
  "version": 7,
  "internal": { "log_odds": 2.31 },
  "trace": { "emitted_ms": 0, "normalize_ms": 0, "detect_ms": 0, "fuse_ms": 0 }
}
```

`trace` holds the trace of the observation that caused this update. 30 minutes after CLEARED, rs-fuse writes a tombstone (null value) for the key.

## 6. `vehicles.reputation` (key: vehicle_id, compacted)

```json
{ "schema": "rs.reputation.v1", "vehicle_id": "SIM-CAR-0099", "reputation": 0.18,
  "quarantined": true, "confirmed_reports": 1, "rejected_reports": 9, "updated_ms": 0 }
```

## 7. `hazards.critical` / `hazards.minor`

These are the same as `hazards.state` (the Split utility routes them unchanged).

## 8. `city.tickets` (after the Data utility)

```json
{ "hazard_id": "POTHOLE:8b6...", "issue": "POTHOLE", "status": "CONFIRMED", "severity_label": "CRITICAL",
  "severity_score": 0.86, "confidence": 0.95, "distinct_vehicles": 5,
  "location": { "lat": 12.93514, "lon": 77.62455 },
  "department": "BBMP Road Infrastructure", "ticket_type": "ROAD_HAZARD_REPAIR", "sla_hours": 24,
  "confirmed_ms": 0 }
```

The API must also accept the un-shaped `hazards.state` shape here (lean build without the Data utility): map `hazard_type` → `issue` and `centroid` → `location`.

## 9. `ui.events` (key: any) → HTTP Output → API `/ingest/ui-events`

A union type on `type`:

```json
{ "type": "hazard_update", "hazard": { /* rs.hazard.v1 */ } }
{ "type": "driver_alert", "alert_id": "SIM-CAR-0042:POTHOLE:8b6...:warning", "vehicle_id": "SIM-CAR-0042",
  "hazard_id": "POTHOLE:8b6...", "hazard_type": "POTHOLE", "severity_label": "HIGH", "level": "advisory|warning",
  "distance_m": 280, "eta_s": 26, "advisory_speed_kmh": 20, "message": "Pothole in 280 m - slow to 20 km/h",
  "ts_ms": 0, "trace": { "emitted_ms": 0, "normalize_ms": 0, "alert_ms": 0 } }
{ "type": "alert_outcome", "vehicle_id": "...", "hazard_id": "...", "speed_at_alert_kmh": 42, "speed_at_hazard_kmh": 19 }
{ "type": "reputation_update", "reputation": { /* rs.reputation.v1 */ } }
{ "type": "vehicle_positions", "ts_ms": 0, "vehicles": [ { "id": "SIM-CAR-0042", "c": "car", "lat": 0, "lon": 0, "h": 87, "s": 38.4 } ] }
{ "type": "pipeline_stats", "ts_ms": 0, "msgs_per_s": { "alert_in": 290 },
  "latency_ms": { "emit_to_normalize_p50": 0, "emit_to_normalize_p95": 0, "emit_to_fuse_p50": 0, "emit_to_fuse_p95": 0, "emit_to_alert_p50": 0, "emit_to_alert_p95": 0 } }
```

The HTTP Output may deliver one object **or an array** per POST. Accept both.

## 10. MQTT control channel (API → simulator) `rs/control`

```json
{ "cmd": "add_hazard", "hazard_type": "POTHOLE", "lat": 0, "lon": 0, "severity": 0.8 }
{ "cmd": "repair", "gt_id": "GT-POTHOLE-07" }                       // or { "cmd": "repair", "lat": 0, "lon": 0 }
{ "cmd": "rain", "on": true }                                        // activates waterlogging zones
{ "cmd": "faulty_vehicle", "count": 1 }
{ "cmd": "set_vehicle_count", "n": 300 }
{ "cmd": "hmi_alert", "vehicle_id": "SIM-CAR-0042", "advisory_speed_kmh": 20, "hazard_lat": 0, "hazard_lon": 0 }
```

## 11. Simulator → API `POST /sim/ground-truth` (header `X-RS-Key`)

```json
{ "gt_id": "GT-POTHOLE-07", "hazard_type": "POTHOLE", "lat": 0, "lon": 0, "radius_m": 5,
  "severity": 0.8, "created_ms": 0, "repaired_ms": null, "is_hazard": true }
```

Speed breakers are sent with `is_hazard: false` (used to score pothole vs speed-breaker classification).

## 12. Postgres tables (`api/db/schema.sql`)

- `hazards` (the PG Output connector upserts; PK `hazard_id`): hazard_type, status, confidence, severity_score, distinct_vehicles, lat, lon, h3_cell, first_seen_ms, last_update_ms, plus `geom geography(Point)` generated from lat/lon if PostGIS is available.
- `tickets` (PK `hazard_id`): issue, severity_label, department, sla_hours, status (`OPEN|REPAIR_DISPATCHED|CLOSED`), created_ms, dispatched_ms, closed_ms, repair_minutes.
- `ground_truth` (PK `gt_id`).
- `alert_outcomes` (vehicle_id, hazard_id, speed_at_alert_kmh, speed_at_hazard_kmh, ts_ms).

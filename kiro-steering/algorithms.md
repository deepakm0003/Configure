---
inclusion: always
---

# Algorithms (the source of truth for every stage)

All numeric constants are env-overridable defaults. `RS_DEMO_TIME_SCALE` (default 10) divides all half-lives and expiry timers so the demo runs in minutes, not hours.

## A. Simulator signal model (`simulator/`)

- **Road network:** `build_roads.py` uses osmnx to download the drivable graph within ~2.5 km of (12.9352, 77.6245) (Koramangala, Bengaluru) and saves edges with `speed_limit_kmh` (from `maxspeed`, or 30/40/50 by road type) to `data/roads.geojson`. Run it once. It is committed together with the OSM attribution.
- **Vehicles:** 150–300 (default 200). Class mix: 50% car, 25% two_wheeler, 15% truck, 10% bus. Each vehicle follows random shortest-path routes. Target speed = edge limit × U(0.7, 1.05), with acceleration limits of ±0.25 g and slowdown before turns. GPS jitter σ = 3 m.
- **IMU (internal 100 Hz, sent as a 1 s summary):** baseline vertical noise σ = road_roughness (0.02–0.06) × class_factor (two_wheeler 1.6, car 1.0, truck 0.8, bus 0.7) × (0.6 + speed/80).
- **Pothole crossing** (vehicle passes within 2.5 m of a pothole on the same edge; lateral lane offset U(-1.5, 1.5) m means ~30% of vehicles **miss** it): a negative-then-positive spike, amplitude `A = severity × (0.4 + 0.012 × speed_kmh) × class_factor` g, with no pre-braking (unless the vehicle has received an `hmi_alert`, in which case it slows to the advisory speed 150 m before).
- **Speed breaker** (`is_hazard=false` GT): the vehicle decelerates for 3–4 s before to 12–18 km/h (ax ≈ -0.15 to -0.3 g), then a symmetric double bump of 0.3–0.5 g.
- **Waterlogging zone** (radius 60–120 m, active only while `rain` is on): speed is capped at 8–15 km/h, wiper_level 2–3, occasional ABS.
- **Slippery patch:** 50% of passes have `abs_active` or `esc_active` = true with brake_pressure 5–15 bar.
- **Sudden-braking hotspot** (e.g. cattle crossing): 50% of passes brake with ax_min ≤ -0.5 g.
- **Faulty vehicle** (chaos): random |az| spikes of 0.6–1.2 g every 5–20 s anywhere.
- **Data-quality noise** (to show the DLQ): 0.5% malformed JSON fields, 0.5% duplicates (same seq), 0.2% delayed by 45 s, 0.1% GPS teleport.
- **Seed scenario:** 12 potholes (severity 0.3–0.95), 4 speed breakers, 2 slippery patches, 2 sudden-braking hotspots, 2 waterlogging zones (inactive until rain).

## B. rs-normalize (stateless except small per-vehicle caches)

1. Parse and validate (pydantic). On failure → DLQ `SCHEMA_INVALID`.
2. Range checks: lat/lon inside the configured bbox, speed 0–200, |az| ≤ 6 g → else `OUT_OF_RANGE`.
3. `ts_ms < now - RS_MAX_LATENESS_S*1000` → `LATE_EVENT`. `ts_ms > now + 5000` → `CLOCK_SKEW`.
4. Duplicates: per-vehicle LRU of the last 64 `seq` values → `DUPLICATE`.
5. Teleport: compare to the last accepted fix; implied speed > 200 km/h → `IMPOSSIBLE_JUMP` (don't update the last fix).
6. Enrich: `h3_r11`, `h3_r9`, and `quality.speed_mismatch = |gps.speed - wheel_speed| > 25`.
7. Produce to `telemetry.clean` keyed by vehicle_id. Stamp `trace.normalize_ms`.

## C. rs-detect (stateful per vehicle; input keyed by vehicle_id → partition affinity)

Per-vehicle state: EWMA mean μ and variance σ² of `peak = max(|az_max|, |az_min|)` (α = 0.05; update **only** on ticks with no detection), a speed ring buffer of the last 6 s, a 60 s EWMA cruising speed, and the current-cell accumulator.

Per tick (speed ≥ 10 km/h for bump checks):
- **Bump:** `z = (peak - μ)/max(σ, 0.01)`. A bump is when `z ≥ RS_Z_THRESHOLD (4.0)` and `peak ≥ 0.25 g`.
  - `pre_decel = speed(t-4s) - speed(t)`. If `pre_decel ≥ 12` and `min ax over the last 4 s ≤ -0.12 g` and `speed ≤ 25` → **SPEED_BREAKER**, otherwise **POTHOLE**.
  - `severity = clamp(peak / ((0.4 + 0.012 × speed) × class_factor), 0, 1)`.
- **WATERLOGGING:** `wiper_level ≥ 2` and `speed ≤ 20` and `speed ≤ 0.6 × cruising_speed` → severity = `1 - speed/cruising_speed`.
- **SLIPPERY:** `(abs_active or esc_active)` and `brake_pressure_bar < 20` and `ax_min_g > -0.3` → severity 0.6.
- **HARSH_BRAKE:** `ax_min_g ≤ -0.45` and speed(t-1 s) ≥ 25 → severity = `clamp((-ax_min - 0.45)/0.4 + 0.4, 0, 1)`.
- Accumulate detections in the current res-11 cell (keep the max severity per type). On **cell change** (or 10 s in the same cell), emit one observation to `road.events` keyed by `h3_r9`, with `detections` (possibly empty = clean pass) and mean speed. Stamp `trace.detect_ms`.

## D. rs-fuse (stateful per hazard; input keyed by res-9 cell)

**Hazard identity:** `hazard_id = f"{type}:{cell}"`. The cell resolution depends on the type: res 11 for POTHOLE, SPEED_BREAKER and SLIPPERY; res 10 for WATERLOGGING and HARSH_BRAKE. When a new positive detection arrives, first look for an existing non-CLEARED hazard of the same type in `grid_disk(cell, 1)` and merge into it (avoids splits at cell boundaries).

**Evidence model (log-odds):** prior `p0 = 0.05`, so `L0 = logit(0.05)`.
- **Positive** (a detection of type T from vehicle v): `L += W_POS[T] × rep(v) × (0.5 + 0.5 × severity)`, where `W_POS = {POTHOLE:1.4, SPEED_BREAKER:1.4, WATERLOGGING:1.6, SLIPPERY:1.2, HARSH_BRAKE:1.0}`. Only the **first** report from a vehicle within 10 min counts fully; repeats count ×0.2. Quarantined vehicles count 0.
- **Negative** (an observation in the hazard's cell without a type-T detection and `mean_speed ≥ 15`; for WATERLOGGING require `mean_speed ≥ 30`): `L -= W_NEG[T] × rep(v)`, where `W_NEG = {POTHOLE:0.35, SPEED_BREAKER:0.35, WATERLOGGING:0.7, SLIPPERY:0.4, HARSH_BRAKE:0.3}`. POTHOLE is low because ~30% of vehicles miss a pothole by lane.
- **Time decay** toward L0 with half-life `HL = {POTHOLE: 6 h, SPEED_BREAKER: 7 d, WATERLOGGING: 20 min, SLIPPERY: 1 h, HARSH_BRAKE: 2 h}` / RS_DEMO_TIME_SCALE. Applied in a 1 s sweep.
- `confidence = sigmoid(L)`.

**Lifecycle:**
- CANDIDATE → CONFIRMED: `confidence ≥ 0.8` **and** `distinct_vehicles ≥ RS_CONFIRM_MIN_VEHICLES (3)`.
- CONFIRMED → DECAYING: `confidence < 0.6`. DECAYING → CONFIRMED: `confidence ≥ 0.8`.
- DECAYING → CLEARED: `confidence < 0.2` (reason `repaired_or_resolved`).
- CANDIDATE → CLEARED: not confirmed within 15 min / scale (reason `unconfirmed`).
- SPEED_BREAKER hazards are labelled as known road features: severity LOW, never sent as tickets.

**Severity:** the median of the per-vehicle max severities. Labels: < 0.3 LOW, < 0.6 MEDIUM, < 0.8 HIGH, else CRITICAL. Advisory speed: CRITICAL 15, HIGH 20, MEDIUM 30, LOW none; WATERLOGGING always 10.

**Vehicle reputation** `rep ∈ [0.05, 0.99]`, initial 0.7:
- When a hazard becomes CONFIRMED: every reporter gets `rep += 0.05 × (1 - rep)`.
- When a CANDIDATE is CLEARED as `unconfirmed`: every reporter gets `rep -= 0.15 × rep`.
- When a vehicle reports type T in a cell where ≥ 3 *other* vehicles made clean passes in the last 10 min: `rep -= 0.1 × rep`.
- Quarantine when `rep < 0.25`. Release when `rep > 0.4` (rep slowly regains +0.01 per confirmed agreement).
- Emit `vehicles.reputation` on change (throttled to 1 per vehicle per 5 s).

**Emission:** emit `hazards.state` on every status change, and otherwise at most every 2 s per hazard when confidence changed by ≥ 0.05. Increment `version`. Write a tombstone 30 min (scaled) after CLEARED.

**Recovery:** on startup, read `hazards.state` and `vehicles.reputation` from earliest to the high-water mark (separate consumer, no group commit) and rebuild the dicts (`internal.log_odds` restores L). Then start the main consumer from committed offsets.

## E. rs-alert (stateful; inputs `hazards.state` + `telemetry.clean`)

- Keep active hazards (CONFIRMED or DECAYING, not SPEED_BREAKER) indexed by res-9 parent cell.
- For each clean telemetry with speed ≥ 5: candidates = hazards in `grid_disk(vehicle_r9, 2)`. For each candidate: `d = haversine`, `Δ = angle_diff(heading, bearing(vehicle → hazard))`. It is **ahead** if `Δ ≤ 30°` and `d ≤ RS_ALERT_RADIUS_M (500)` (WATERLOGGING: `d ≤ 500 + radius_m`).
- Level: `d ≤ 200` → `warning`, otherwise `advisory`. Cooldown per (vehicle, hazard) of 300 s; one escalation advisory → warning is allowed.
- Message: `"{Type} in {d} m - slow to {advisory} km/h"` (Type in human words: "Pothole", "Waterlogging", "Slippery road", "Sudden-braking zone").
- **Alert outcome:** after alerting, track the vehicle. When it comes within 20 m of the hazard, emit `alert_outcome` with the speed at alert vs the speed at the hazard.
- Forward every `hazards.state` update as `hazard_update` and every reputation change as `reputation_update` (it also consumes `vehicles.reputation`).
- Every 1 s emit `vehicle_positions` (the latest fix per vehicle seen in the last 10 s). Every 5 s emit `pipeline_stats` (latencies from `trace` fields, p50/p95 over a rolling 60 s window).

## F. Scoring (API `scoring.py`)

- A GT hazard is **detected** if any hazard of the same type that was ever CONFIRMED lies within 25 m (WATERLOGGING: within zone radius + 50 m).
- **Precision** = confirmed hazards matching a GT hazard / all confirmed hazards (excluding SPEED_BREAKER type). **Recall** = detected active GT hazards / active GT hazards (`is_hazard=true`) that at least 3 vehicles have passed.
- **Classification:** speed-breaker GT spots wrongly CONFIRMED as POTHOLE count as false positives.
- **Time-to-detect** = `confirmed_ms - max(gt.created_ms, sim_start_ms)`. Report the mean and p90.
- **False hazards prevented** = CANDIDATEs cleared as `unconfirmed` whose reporters were mostly quarantined.
- **Warning impact** = mean speed at hazard for warned vehicles vs unwarned passes.
- **Latency** = `api_receive_ms - trace.emitted_ms` for driver_alerts (p50/p95), plus the per-stage breakdown.

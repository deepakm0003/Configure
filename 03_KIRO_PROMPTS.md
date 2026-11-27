# RoadSense: Kiro Prompts (paste in order)

## How to set up Kiro (Sat 00:00 IST, not before)

1. Create the private GitHub repo `roadsense`, clone it, and open the folder in Kiro.
2. Copy the 5 files from this folder's `kiro-steering/` into `roadsense/.kiro/steering/` (`product.md`, `tech.md`, `structure.md`, `data-contracts.md`, `algorithms.md`). They have `inclusion: always`, so Kiro reads them on every request. **They are the source of truth**: if Kiro drifts, tell it "follow .kiro/steering/algorithms.md exactly".
3. For the big components (P3–P7), use Kiro **Spec mode**: paste the prompt, let Kiro write requirements → design → tasks, skim them, then run the tasks. For small fixes, use **Vibe** (chat) mode.
4. Optional Kiro **hook**: "On save of `pipeline/**/*.py`, run `pytest pipeline/tests -q` and report failures."
5. After each prompt: run it, check the "Done when" condition, then `git commit`. Never skip the commit (Condense drafts inactive for 3 h are deleted, and judges look at the commit history).

Replace `<PREFIX>` with your team prefix everywhere.

---

## P1: Repo skeleton

```
Create the repository skeleton exactly as described in .kiro/steering/structure.md.
- Root Dockerfile: python:3.11-slim, installs requirements.txt, copies pipeline/, CMD ["python","-m","pipeline.main"]. Non-root user. No secrets.
- requirements.txt: confluent-kafka, h3>=4, pydantic>=2, orjson, numpy.
- pipeline/main.py reads RS_ROLE (normalize|detect|fuse|alert) and dispatches to pipeline/stages/<role>.run(); unknown role -> exit with a clear error.
- .env.example listing EVERY env var used anywhere (pipeline, api, simulator, web) with placeholder values and a one-line comment each.
- LICENSE (MIT, current year, author placeholder), .gitignore (Python, Node, .env, data caches except simulator/data/roads.geojson).
- docker-compose.yml for LOCAL reproduction: eclipse-mosquitto (1883), postgis/postgis:16-3.4, api, web. A local Kafka is NOT included because Condense is the backbone; add a commented optional redpanda service only for offline unit testing.
- README.md stub with section headings: Problem, Solution, Architecture, How Condense is used, Run it, Data generator, Configuration, Demo, Limitations, AI tools & reused code disclosure, License.
Do not implement stage logic yet; create stubs with docstrings.
```
**Done when:** `docker build .` succeeds and `RS_ROLE=xyz python -m pipeline.main` prints a clear error.

---

## P2: Vehicle simulator (MQTT)

```
Implement simulator/ per .kiro/steering/algorithms.md section A and data-contracts.md sections 1, 10, 11.
- build_roads.py: osmnx, drivable graph within 2500 m of (12.9352, 77.6245); keep the largest strongly connected component; export simulator/data/roads.geojson with edge geometry, length_m, speed_limit_kmh (maxspeed or 30/40/50 by highway type). Print the OSM attribution.
- vehicles.py: Vehicle class with route following on the graph (networkx shortest paths between random nodes, new route on arrival), kinematics with accel limits, slowdown before sharp turns, lateral lane offset U(-1.5,1.5) m, 100 Hz internal IMU synthesis summarised every 1 s into the imu block, CAN block, GPS jitter 3 m. Speed-breaker pre-braking, pothole spikes, waterlogging/slippery/harsh-brake behaviours, faulty-vehicle random spikes, and hmi_alert handling (slow to the advisory speed 150 m before the hazard).
- hazards.py: GroundTruthRegistry with the seed scenario (12 potholes, 4 speed breakers with is_hazard=false, 2 slippery, 2 harsh-brake hotspots, 2 waterlogging zones inactive until rain) placed ON road edges; add/repair functions; posts each GT change to API POST {API_URL}/sim/ground-truth with header X-RS-Key (retry with backoff; if API_URL is unset, just log).
- control.py: subscribe to MQTT rs/control and handle add_hazard, repair (by gt_id or nearest within 30 m), rain on/off, faulty_vehicle count, set_vehicle_count, hmi_alert.
- run.py: asyncio loop; publish each vehicle at 1 Hz to rs/telemetry/<vehicle_id>, QoS 1, MQTT v3.1.1, TLS when MQTT_TLS=true; inject data-quality noise (0.5% malformed, 0.5% duplicate, 0.2% delayed 45 s, 0.1% teleport); trace.emitted_ms at publish time; log msgs/s every 5 s; --vehicles N flag; reconnect forever with backoff.
- config.py reads MQTT_HOST, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD, MQTT_TLS, API_URL, RS_API_KEY, SIM_VEHICLES, SIM_SEED.
- Add a --dry-run flag that prints 5 sample messages to stdout and exits.
Write unit tests for the pothole spike amplitude formula and the speed-breaker pre-braking behaviour.
```
**Done when:** the HiveMQ/EMQX web client shows `rs/telemetry/#` traffic at ~200 msg/s.

---

## P3: Common library + rs-normalize

```
Implement pipeline/rs_common/* and pipeline/stages/normalize.py.
rs_common/config.py:
- Load env; build TOPICS from RS_TOPIC_PREFIX (telemetry_raw, telemetry_clean, telemetry_dlq, road_events, hazards_state, vehicles_reputation, ui_events).
- ENV_ALIASES dict mapping our names (KAFKA_BOOTSTRAP_SERVERS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD, KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_MECHANISM, RS_INPUT_TOPICS, RS_OUTPUT_TOPIC) to a list of alternative names Condense may inject; the first one found wins. Leave a clear TODO comment block "Fill in Condense-injected env names here after checking the Condense app template".
- Allow input/output topic overrides from env (the Condense canvas sets them) while defaulting to TOPICS.
rs_common/kafka_io.py:
- make_consumer(group, topics), make_producer() with the settings in tech.md (SASL_SSL / SCRAM-SHA-512 when the creds are set, PLAINTEXT otherwise).
- run_loop(consumer, handler, producer, batch_size=200, poll_timeout=0.5): poll batches; the handler returns a list of (topic, key, value_dict); produce with orjson; poll the producer; commit offsets synchronously after the produce callbacks succeed; exponential backoff on KafkaException; graceful SIGTERM shutdown (flush + commit + close).
- send_dlq(producer, reason, detail, raw).
rs_common/models.py: pydantic models for every contract in data-contracts.md.
rs_common/geo.py: h3 v4 helpers (cell_r11, cell_r9, parent, disk), haversine_m, bearing_deg, angle_diff_deg, is_ahead(vehicle_lat, vehicle_lon, heading, target_lat, target_lon, max_angle).
rs_common/metrics.py: rolling 60 s window; msgs in/out per second; latency p50/p95 from trace fields; log a JSON line every 10 s.
stages/normalize.py: pure function normalize(raw_bytes, state, now_ms) -> (clean_dict | None, dlq_dict | None) implementing algorithms.md section B, plus run() wiring it to Kafka (group RS_GROUP, input telemetry.raw, outputs telemetry.clean keyed by vehicle_id and telemetry.dlq).
Tests: one per DLQ reason plus the happy path.
```
**Then in Condense:** create the `rsnormalize` app (guide §3), build, test with the Publish/Subscribe testers, publish v1, deploy on the canvas.
**Done when:** `prefix.telemetry.clean` and `prefix.telemetry.dlq` both have messages; the DLQ shows all four noise reasons.

---

## P4: rs-detect

```
Implement pipeline/stages/detect.py per algorithms.md section C and the observation contract in data-contracts.md section 4.
- VehicleState dataclass: EWMA mu/var of peak (update only on non-detection ticks, alpha 0.05, warm-up of 10 ticks before any bump detection), deque of (ts, speed, ax_min) for the last 6 s, 60 s EWMA cruising speed, current-cell accumulator {cell, enter_ms, speeds[], detections{type: best}}.
- Pure function step(state, clean_msg) -> list[observation] (usually empty; one when the cell changes or 10 s pass in the same cell).
- Classification: POTHOLE vs SPEED_BREAKER using pre-deceleration, WATERLOGGING, SLIPPERY, HARSH_BRAKE exactly as specified; severity formulas as specified; class_factor table shared with the simulator values (car 1.0, two_wheeler 1.6, truck 0.8, bus 0.7).
- Evict vehicle states idle > 5 min.
- run(): consume telemetry.clean, produce road.events keyed by obs.h3_r9, stamp trace.detect_ms.
Tests: synthetic sequences for a pothole (no pre-braking) -> POTHOLE; a speed breaker (pre-braking to 15 km/h) -> SPEED_BREAKER; a clean drive -> only empty observations; a waterlogging slow crawl with wipers -> WATERLOGGING.
```
**Then in Condense:** app `rsdetect` → publish v1 → deploy (`telemetry.clean` → `road.events`).
**Done when:** the Subscribe tester on `road.events` shows observations, and some have POTHOLE or SPEED_BREAKER detections near the seeded spots.

---

## P5: rs-fuse (the core; take your time)

```
Implement pipeline/stages/fuse.py per algorithms.md section D and data-contracts.md sections 5-6.
- HazardState dataclass (all hazard.v1 fields + log_odds, per-vehicle last_report_ms, per-vehicle max severity, recent clean-pass list (vehicle, ts)).
- Reputation store {vehicle_id: Reputation}.
- Pure functions (fully unit-tested):
  apply_observation(hazards, reps, obs, now_ms) -> list[emitted messages]
  sweep(hazards, reps, now_ms) -> list[emitted messages]  (decay, lifecycle timers, tombstones)
  confidence(L), resolution_for(type), find_merge_target(hazards, type, cell)
- Lifecycle, evidence weights, decay half-lives scaled by RS_DEMO_TIME_SCALE, reputation rules and quarantine exactly as specified. Emission throttling: status change -> always; otherwise <= 1 per 2 s per hazard and only if |Δconfidence| >= 0.05; reputation <= 1 per vehicle per 5 s.
- Emit flat lat/lon AND centroid, severity_label, advisory_speed_kmh, status_reason (human readable), version++, internal.log_odds, trace copied from the triggering observation with fuse_ms stamped.
- Startup recovery: read hazards.state and vehicles.reputation from the beginning to the high-water marks using a consumer with group.id f"{RS_GROUP}.recovery.{uuid4}" and enable.auto.commit=false; rebuild state; log the counts. Then start the main loop on road.events.
- The main loop also runs sweep() every 1 s (call it from the poll loop based on elapsed time; no threads).
Tests:
 1) 3 distinct reputable vehicles report a POTHOLE in the same cell -> CONFIRMED; 1 vehicle reporting 5 times -> stays CANDIDATE.
 2) After CONFIRMED, 15 clean passes at 35 km/h -> DECAYING then CLEARED.
 3) A faulty vehicle reports potholes in 20 different cells where others pass clean -> reputation < 0.25 -> quarantined -> no CONFIRMED hazard is created.
 4) A detection in a neighbouring cell merges into the existing hazard (grid_disk 1).
 5) Recovery: serialize state -> rebuild -> identical confidence.
```
**Then in Condense:** app `rsfuse` → publish v1 → deploy (`road.events` → `hazards.state` + `vehicles.reputation`).
**Done when:** seeded potholes appear as CONFIRMED in `hazards.state` within ~1–2 minutes and no speed breaker is ever CONFIRMED as a POTHOLE.

---

## P6: rs-alert

```
Implement pipeline/stages/alert.py per algorithms.md section E and data-contracts.md section 9.
- Consume three topics with one consumer: hazards.state (maintain an active-hazard index by res-9 parent; tombstones remove), vehicles.reputation, telemetry.clean.
- On startup, read hazards.state from the beginning to the high-water mark first (same recovery pattern as fuse) so alerts work immediately after a restart.
- For each telemetry: candidate lookup via grid_disk(r9, 2); ahead test; advisory/warning levels; cooldown 300 s with one escalation; human-readable message; driver_alert with trace (alert_ms stamped).
- Alert outcome tracking: after an alert, watch that vehicle; when within 20 m of the hazard, emit alert_outcome; drop tracking after 120 s.
- Forward hazard_update and reputation_update to ui.events.
- Every 1 s emit vehicle_positions (vehicles seen in the last 10 s; compact keys as in the contract); every 5 s emit pipeline_stats using trace fields (p50/p95 over 60 s).
- All messages go to ui.events; key = vehicle_id or hazard_id.
Tests: a vehicle heading toward a hazard at 400 m -> advisory; at 150 m -> warning escalation; a vehicle heading away -> no alert; cooldown suppresses duplicates.
```
**Then in Condense:** app `rsalert` → publish → deploy. Then add the **Split utility** (#6), the **Data utility** (#7) and the **PostgreSQL Output** (#8) exactly as in guide §4–§6. Create the DB tables first (P7 does that, or run `api/db/schema.sql` manually in the Neon SQL editor).
**Done when:** `ui.events` shows driver_alert messages; the Postgres `hazards` table has rows; `hazards.critical` receives CRITICAL/HIGH confirmed hazards.

---

## P7: API gateway (FastAPI)

```
Implement api/ per structure.md and data-contracts.md sections 8-12, algorithms.md section F.
- db/schema.sql: tables hazards (exact columns the PG Output connector writes, PK hazard_id, plus a nullable PostGIS geom if the extension exists), tickets, ground_truth, alert_outcomes. Apply idempotently at startup (CREATE ... IF NOT EXISTS).
- Auth: every /ingest/* and /sim/ground-truth requires header X-RS-Key == RS_API_KEY (constant-time compare). /sim/control and the repair endpoint require the same key, sent by the web app from VITE_RS_CONTROL_KEY (demo only; document this).
- POST /ingest/ui-events: accept an object or an array; stamp api_ms; update in-memory state (latest hazards, latest vehicle positions, reputations, latency histograms, alert outcomes -> DB); broadcast to WebSocket clients. Must handle 300+ msgs/s: use an asyncio.Queue per client with a max size, dropping the oldest vehicle_positions first.
- POST /ingest/tickets: accept the shaped ticket OR a raw hazard (lean build); upsert into tickets (status OPEN); broadcast ticket_created.
- POST /sim/ground-truth: upsert GT.
- POST /sim/control: validate the command and publish to MQTT rs/control (paho, TLS).
- POST /api/tickets/{hazard_id}/repair: set REPAIR_DISPATCHED, publish {"cmd":"repair","lat":..,"lon":..}; when a hazard_update with status CLEARED arrives for that id -> CLOSED, repair_minutes computed, broadcast ticket_closed.
- When a driver_alert arrives, also publish {"cmd":"hmi_alert", ...} to rs/control so the simulated vehicle reacts (closes the in-vehicle HMI loop).
- GET /api/snapshot (hazards from DB + memory, tickets, score, latest positions, reputations, stats), GET /api/score, GET /api/metrics, GET /health.
- WS /ws: send the snapshot on connect, then stream {type, ...} events. Heartbeat every 15 s.
- scoring.py implements algorithms.md section F; recompute every 2 s and broadcast score_update.
- Dockerfile for Render (uvicorn, PORT env). CORS for the Vercel domain via env.
Tests: ingest accepts both object and array; scoring precision/recall on a hand-made fixture.
```
**Deploy:** Render web service (root `api/`), set the env vars, then create Condense **HTTP Outputs #9 and #10** (guide §7).
**Done when:** `wscat -c wss://<api>/ws` streams live hazard updates and driver alerts **coming from Condense**.

---

## P8: Dashboard (React)

```
Build web/ (React 18 + Vite + TS + Tailwind + MapLibre GL + deck.gl + Recharts). Dark control-room theme, desktop 1440x900 first, still usable at 390 px width.
Data: connect to VITE_API_WS (auto-reconnect with backoff), load GET /api/snapshot first, then apply streamed events to a single zustand store.
Layout: left = big map; right = tabbed side panel; top bar = live KPIs (vehicles online, msgs/s, e2e latency p95, confirmed hazards, open tickets, precision/recall).
Map (MapLibre with a free dark style, e.g. a CARTO dark-matter style URL, attribution shown):
 - ScatterplotLayer of vehicles (colour by class; quarantined vehicles in red with a ring).
 - H3HexagonLayer of hazards: fill colour by type, opacity by confidence, CANDIDATE dashed outline, CONFIRMED pulsing (animate radius), DECAYING fading, CLEARED hidden after 10 s.
 - Waterlogging hazards drawn as translucent blue circles of radius_m.
 - Click a hazard -> popover with status, confidence sparkline (keep history client-side), vehicles, severity, first seen, advisory speed.
Tabs:
 1) Driver view: pick a vehicle (or "follow random"); mini map following it, plus a large HUD banner showing its latest driver_alert with a distance countdown; colour advisory amber / warning red.
 2) City ops: ticket list sorted by severity then age; SLA countdown; evidence; "Mark repaired" button -> POST repair; closed tickets show repair_minutes.
 3) Accuracy: precision, recall, F1, mean/p90 time-to-detect, speed-breaker misclassifications, false hazards prevented, warning impact (avg speed at hazard warned vs unwarned) as Recharts bars.
 4) Pipeline: msgs/s and latency p50/p95 per stage (normalize/detect/fuse/alert/api) as live line charts; DLQ counts by reason if available; a static diagram of the Condense pipeline.
 5) Chaos: buttons -> POST /sim/control: Add pothole at map click, Start/stop rain, Add faulty vehicle, Vehicles 100/200/300.
Event toasts for new CONFIRMED hazards and closed tickets. Keep components small; no backend logic in the frontend.
```
**Deploy:** Vercel (root `web/`), env `VITE_API_WS`, `VITE_API_HTTP`, `VITE_RS_CONTROL_KEY`.
**Done when:** the full story in the master plan's demo storyboard works live.

---

## P9: Tuning + hardening (Sun morning)

```
Run the simulator with 200 vehicles for 10 minutes against the live pipeline and look at /api/score and /api/metrics.
1) If precision < 0.9 or recall < 0.85, propose specific changes to RS_Z_THRESHOLD, W_POS/W_NEG and confirm thresholds with reasoning, keeping algorithms.md as the documented source (update it too).
2) Add a scripts/scenario_demo.py that replays the exact video storyboard via /sim/control with timed steps (add pothole near a busy road, add faulty vehicle, rain on, repair) so the demo is repeatable.
3) Audit every Kafka loop for: reconnect/backoff, graceful shutdown, commit-after-produce, no unbounded memory (cap dicts, evict idle). Add a soak test note to the README.
4) Make sure no secret appears in the repo: scan the tree and git history for keys; list anything found.
```

---

## P10: Docs and submission artifacts

```
Write the submission documents:
- docs/architecture.md: the Mermaid diagram (sources -> MQTT broker -> Condense MQTT Input -> topics -> 4 custom transforms -> Split & Data utilities -> PG/HTTP outputs -> API -> dashboard; plus the control loop) and a stage-by-stage table (inputs, outputs, key, state, guarantees). Also export it as docs/architecture.png (use mermaid-cli via npx, or describe how).
- docs/condense-setup.md: every Condense component with its exact configuration (no secret values), topic list with partitions/cleanup, how to grant judges access.
- README.md complete: problem with the MoRTH stats (2,385 pothole deaths in 2024; 9,438 in 2020-2024), target users, solution, why real time, architecture image, how Condense is used (bullet per component), quick start (local docker compose + cloud deploy), data generator explanation, configuration table (names only), demo steps, results (paste the latest precision/recall/latency numbers), what is simulated, limitations, next steps (real TCUs over MQTT, OEM/V2X delivery, municipality API integration, vehicle-class-specific calibration, ML-learned thresholds), AI tools disclosure (Kiro and Claude: what they produced and how I verified it: unit tests, live scoring), reused code/assets and licenses (OSM ODbL, MapLibre BSD, deck.gl MIT, CARTO basemap terms), MIT license.
- docs/writeup.md: problem, solution, Condense usage, tech stack, challenges, limitations, next steps (≈700 words) for the Google Form.
```

---

## Debug prompts (use when stuck)

- **Consumer gets nothing:** "The rsdetect app on Condense starts but consumes 0 messages. Here are its logs: <paste>. Check group id prefix, topic names from env, auto.offset.reset, SASL config, and the ENV_ALIASES mapping."
- **Crash loop:** "Condense shows CrashLoopBackOff for rsfuse. Logs: <paste>. Find the root cause and fix it with a minimal change; add a test that reproduces it."
- **Too many false potholes:** "Precision is 0.6. Here are 20 false CONFIRMED hazards with their reporters and evidence: <paste /api/snapshot excerpt>. Diagnose which rule lets them through and propose a fix consistent with algorithms.md."
- **Latency high:** "p95 emit→alert latency is 4 s. Here are pipeline_stats: <paste>. Identify the slow stage (batching/linger/HTTP output batch timeout) and tune it."

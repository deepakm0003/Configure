# RoadSense

Real-time road-hazard intelligence network for the Configure 2026 hackathon (Condense × Bosch MPS).

## 🚀 Quick Start

```bash
# 1. Clone
git clone https://github.com/deepakm0003/Configure.git
cd Configure

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run tests (31 tests, all passing)
pytest -v

# 4. Set up environment
cp .env.example .env
# Edit .env with your Kafka credentials

# 5. Create Kafka topics (run once)
python scripts/create_topics.py

# 6. Run simulator
python -m simulator.run --vehicles 200

# 7. Run pipeline stages (each in separate terminal)
RS_ROLE=normalize python -m pipeline.main
RS_ROLE=detect python -m pipeline.main
RS_ROLE=fuse python -m pipeline.main
RS_ROLE=alert python -m pipeline.main
```

## 📋 What It Does

Connected vehicles act as road sensors. Their GPS + IMU + CAN telemetry flows through a **Condense-managed Kafka pipeline** that:

1. **Validates & normalizes** telemetry (DLQ for bad data)
2. **Detects** per-vehicle road anomalies:
   - 🕳️ Potholes (z-score spike, no pre-braking)
   - 🚧 Speed breakers (spike + pre-decel + low speed)
   - 🌊 Waterlogging (wipers on, slow speed)
   - 🧊 Slippery patches (ABS/ESC active, low brake)
   - 🚨 Sudden-braking hotspots
3. **Fuses** evidence from many vehicles into hazards with lifecycle:
   - CANDIDATE → CONFIRMED (≥3 vehicles, conf ≥ 0.8) → DECAYING → CLEARED
   - Vehicle reputation (0.05–0.99): faulty sensors get quarantined
   - Clean passes decay hazards after repair
4. **Alerts** approaching drivers:
   - 📍 Distance/ETA/advisory speed
   - ⚠️ Advisory (200–500 m) or 🚨 Warning (<200 m)
   - 300 s cooldown, one escalation allowed
5. **Routes** critical hazards to city maintenance tickets

## 🏗️ Architecture

```
Telemetry (1 msg/vehicle/s) → rs-normalize → rs-detect → rs-fuse → rs-alert → UI
                                     ↓            ↓           ↓          ↓
                               telemetry_clean  road_events  hazards  alerts
                                     +                         +
                                 telemetry_dlq            reputation
```

**Topics:**
- `iiitd_telemetry_raw` (12 partitions, 1h retention)
- `iiitd_telemetry_clean` (12 partitions, 2h retention)
- `iiitd_telemetry_dlq` (4 partitions, 24h retention)
- `iiitd_road_events` (12 partitions, 2h retention)
- `iiitd_hazards_state` (4 partitions, **compacted**)
- `iiitd_vehicles_reputation` (4 partitions, **compacted**)
- `iiitd_ui_events` (4 partitions, 1h retention)

**Seed Scenario:** 12 potholes (severity 0.3–0.95), 4 speed breakers, 2 slippery patches, 2 sudden-braking hotspots, 2 waterlogging zones (rain-activated).

## 🧪 Tests (31 passing)

```bash
pytest -v
# normalize: 11 tests (DLQ reasons: SCHEMA_INVALID, OUT_OF_RANGE, LATE_EVENT, 
#                      CLOCK_SKEW, DUPLICATE, IMPOSSIBLE_JUMP, plus valid)
# detect:    6 tests (pothole, speed-breaker, clean, waterlogging on/off)
# fuse:      6 tests (3-vehicle confirm, single-vehicle candidate, clean decay,
#                     quarantine, neighbour merge, recovery round-trip)
# alert:     5 tests (advisory, warning, behind, cooldown, speed-breaker filtered)
```

## 🎯 Differentiators (what wins the hackathon)

1. **Multi-vehicle consensus:** A hazard is confirmed only when ≥3 distinct vehicles agree, weighted by reputation (0.7 initial, boosted/penalized by confirm/reject).
2. **Self-healing:** Clean passes count as negative evidence → hazards clear after repair.
3. **Quarantine faulty sensors:** Vehicles with reputation < 0.25 are quarantined (reports count 0).
4. **Pothole vs speed-breaker classification:** Pre-braking pattern distinguishes them.
5. **Live accuracy:** Precision/recall/time-to-detect vs simulator ground truth.
6. **End-to-end latency:** p50/p95 from `trace.emitted_ms` through all 4 stages.

## 📂 Repository Structure

```
roadsense/
├── Dockerfile                   # Pipeline image (python:3.11-slim, non-root)
├── requirements.txt             # confluent-kafka, h3>=4, pydantic>=2, orjson, numpy
├── pipeline/
│   ├── main.py                  # Entrypoint: dispatches on RS_ROLE
│   ├── rs_common/
│   │   ├── config.py            # Env loading, ENV_ALIASES for Condense injection
│   │   ├── kafka_io.py          # Resilient consumer/producer, run_loop, DLQ
│   │   ├── models.py            # Pydantic v2 models for every contract
│   │   ├── geo.py               # h3 v4 helpers, haversine, bearing, is_ahead
│   │   └── metrics.py           # Rolling throughput + p50/p95 latency
│   ├── stages/
│   │   ├── normalize.py         # Validate, range checks, duplicate/teleport, enrich
│   │   ├── detect.py            # EWMA z-score, 5 detection types, cell accumulator
│   │   ├── fuse.py              # Log-odds evidence, lifecycle, reputation, 1s sweep
│   │   └── alert.py             # is_ahead check, advisory/warning, cooldown, outcomes
│   └── tests/                   # 31 pytest unit tests
├── simulator/
│   ├── build_roads.py           # osmnx → roads.geojson (Bengaluru, 2.5 km radius)
│   ├── vehicles.py              # Kinematics, IMU/CAN synthesis, hazard effects, noise
│   ├── hazards.py               # Seed scenario + ground-truth registry
│   ├── control.py               # Kafka/MQTT control handlers (add/repair/rain/chaos)
│   └── run.py                   # Asyncio main loop, Kafka or MQTT transport
├── scripts/
│   ├── create_topics.py         # Creates all 11 topics with correct config
│   └── tail.py                  # Prints last N messages from any topic
├── DEPLOYMENT.md                # 📘 Full Condense deployment guide
└── .env.example                 # All env vars (NO secrets)
```

## 🚢 Deployment (Condense)

See **[DEPLOYMENT.md](./DEPLOYMENT.md)** for complete step-by-step instructions.

**TL;DR:**
1. Create topics once: `python scripts/create_topics.py`
2. Create 4 Condense apps from this repo, one per `RS_ROLE={normalize,detect,fuse,alert}`
3. Set env vars (bootstrap server, username, password, `RS_TOPIC_PREFIX=iiitd_`)
4. Build, publish, deploy each app
5. Run simulator locally or as 5th app: `python -m simulator.run`

## 📊 Accuracy Scoring (API endpoint)

```python
# POST /sim/ground-truth (simulator posts on startup)
# GET /metrics/accuracy (API computes precision/recall/TTD)
```

- **Precision:** confirmed hazards matching GT / all confirmed
- **Recall:** detected active GT hazards / active GT hazards with ≥3 passes
- **Time-to-detect:** mean & p90 of `confirmed_ms - gt.created_ms`
- **False positives prevented:** CANDIDATEs cleared as "unconfirmed" (quarantined reporters)

## 📜 License

MIT. Map data © OpenStreetMap contributors ([ODbL](https://www.openstreetmap.org/copyright)).

## 🏆 Configure 2026 Hackathon

**Team:** IIIT Delhi  
**Challenge:** Real-time road hazard detection with Condense × Bosch MPS  
**GitHub:** https://github.com/deepakm0003/Configure.git

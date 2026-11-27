# RoadSense Deployment Guide

This guide walks through deploying RoadSense on **Condense** (the managed Kafka platform).

## Prerequisites

- GitHub repo: https://github.com/deepakm0003/Configure.git
- Condense account with Kafka cluster access
- Kafka credentials (bootstrap server, username, password)

## Step 1: Create Kafka Topics

**Run this ONCE from within the Condense environment** (or any host with network access to the Kafka cluster):

```bash
# Set env vars from your Condense Kafka connection details
export KAFKA_BOOTSTRAP_SERVERS=20.213.158.116:9094
export KAFKA_SASL_USERNAME=iiitd
export KAFKA_SASL_PASSWORD=U2i91UPq8z2-3AB3GImft4Q7QKUG8cXk
export KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
export KAFKA_SASL_MECHANISM=SCRAM-SHA-512
export RS_TOPIC_PREFIX=iiitd_

# Install deps
pip install confluent-kafka

# Create all 11 topics
python scripts/create_topics.py
```

**Topics created:**
- `iiitd_telemetry_raw` (12 partitions, 1h retention)
- `iiitd_telemetry_clean` (12 partitions, 2h retention)
- `iiitd_telemetry_dlq` (4 partitions, 24h retention)
- `iiitd_road_events` (12 partitions, 2h retention)
- `iiitd_hazards_state` (4 partitions, **compacted**)
- `iiitd_vehicles_reputation` (4 partitions, **compacted**)
- `iiitd_hazards_critical` (4 partitions, 24h retention)
- `iiitd_hazards_minor` (4 partitions, 24h retention)
- `iiitd_city_tickets` (4 partitions, 7d retention)
- `iiitd_ui_events` (4 partitions, 1h retention)
- `iiitd_sim_control` (1 partition, 1h retention)

## Step 2: Deploy Pipeline Stages to Condense

Create **4 separate apps** in Condense, one per pipeline stage.

### App 1: rs-normalize

**Git repo:** https://github.com/deepakm0003/Configure.git  
**Dockerfile:** `./Dockerfile` (root)  
**Environment variables:**
```
KAFKA_BOOTSTRAP_SERVERS=<from Condense>
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_USERNAME=iiitd
KAFKA_SASL_PASSWORD=U2i91UPq8z2-3AB3GImft4Q7QKUG8cXk
RS_TOPIC_PREFIX=iiitd_
RS_ROLE=normalize
RS_MAX_LATENESS_S=60
RS_BBOX_LAT_MIN=12.80
RS_BBOX_LAT_MAX=13.10
RS_BBOX_LON_MIN=77.45
RS_BBOX_LON_MAX=77.80
```

**What it does:** Consumes `iiitd_telemetry_raw`, validates/enriches/de-duplicates, produces to `iiitd_telemetry_clean` and `iiitd_telemetry_dlq`.

---

### App 2: rs-detect

**Git repo:** https://github.com/deepakm0003/Configure.git  
**Dockerfile:** `./Dockerfile` (root)  
**Environment variables:**
```
KAFKA_BOOTSTRAP_SERVERS=<from Condense>
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_USERNAME=iiitd
KAFKA_SASL_PASSWORD=U2i91UPq8z2-3AB3GImft4Q7QKUG8cXk
RS_TOPIC_PREFIX=iiitd_
RS_ROLE=detect
RS_Z_THRESHOLD=4.0
```

**What it does:** Consumes `iiitd_telemetry_clean`, detects potholes/speed-breakers/waterlogging/slippery-patches/harsh-braking, produces to `iiitd_road_events`.

---

### App 3: rs-fuse

**Git repo:** https://github.com/deepakm0003/Configure.git  
**Dockerfile:** `./Dockerfile` (root)  
**Environment variables:**
```
KAFKA_BOOTSTRAP_SERVERS=<from Condense>
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_USERNAME=iiitd
KAFKA_SASL_PASSWORD=U2i91UPq8z2-3AB3GImft4Q7QKUG8cXk
RS_TOPIC_PREFIX=iiitd_
RS_ROLE=fuse
RS_CONFIRM_MIN_VEHICLES=3
RS_DEMO_TIME_SCALE=10
```

**What it does:** Consumes `iiitd_road_events`, fuses observations into hazards with lifecycle (CANDIDATE → CONFIRMED → DECAYING → CLEARED), tracks vehicle reputation, produces to `iiitd_hazards_state` (compacted) and `iiitd_vehicles_reputation` (compacted).

**⚠️ Important:** On startup, reads `iiitd_hazards_state` and `iiitd_vehicles_reputation` from earliest to rebuild in-memory state. If topics are very large, increase startup timeout.

---

### App 4: rs-alert

**Git repo:** https://github.com/deepakm0003/Configure.git  
**Dockerfile:** `./Dockerfile` (root)  
**Environment variables:**
```
KAFKA_BOOTSTRAP_SERVERS=<from Condense>
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_USERNAME=iiitd
KAFKA_SASL_PASSWORD=U2i91UPq8z2-3AB3GImft4Q7QKUG8cXk
RS_TOPIC_PREFIX=iiitd_
RS_ROLE=alert
RS_ALERT_RADIUS_M=500
```

**What it does:** Consumes `iiitd_hazards_state` + `iiitd_telemetry_clean` + `iiitd_vehicles_reputation`, emits driver alerts, alert outcomes, vehicle positions (1 s), pipeline stats (5 s) to `iiitd_ui_events`.

---

## Step 3: Deploy the Simulator (optional)

The simulator is a **standalone app** (not Dockerized by default; run locally or package separately).

**Local run:**
```bash
# 1. Download road network once
pip install osmnx
python -m simulator.build_roads

# 2. Set Kafka env vars
export KAFKA_BOOTSTRAP_SERVERS=20.213.158.116:9094
export KAFKA_SASL_USERNAME=iiitd
export KAFKA_SASL_PASSWORD=U2i91UPq8z2-3AB3GImft4Q7QKUG8cXk
export RS_TOPIC_PREFIX=iiitd_
export SIM_TRANSPORT=kafka
export SIM_NUM_VEHICLES=200

# 3. Run
python -m simulator.run
```

The simulator produces 1 message/vehicle/second to `iiitd_telemetry_raw`, injecting the seed scenario (12 potholes, 4 speed breakers, etc.) plus data-quality noise.

---

## Step 4: Verify the Pipeline

```bash
# Check DLQ for rejected messages
python scripts/tail.py iiitd_telemetry_dlq --n 10

# Check confirmed hazards
python scripts/tail.py iiitd_hazards_state --n 20

# Check driver alerts
python scripts/tail.py iiitd_ui_events --n 50
```

---

## Scaling Notes

- **rs-normalize** and **rs-detect**: Kafka consumer group auto-balances across replicas; each replica handles a subset of partitions. Scale to N replicas ≤ partition count (12).
- **rs-fuse**: Stateful (in-memory hazards + reputation dicts rebuilt from compacted topics on startup). DO NOT scale beyond 1 replica unless you partition by res-9 cell (not currently implemented). For very high throughput, run multiple instances with different `RS_INPUT_TOPIC` overrides and shard by geography.
- **rs-alert**: Subscribe to 3 topics; can scale if partitions > 1 replica, but state is per-hazard so partitioning doesn't help much. Scale horizontally only if output topic `iiitd_ui_events` becomes a bottleneck.

---

## Troubleshooting

**"Broker transport failure"**  
Check firewall/VPC rules. Condense Kafka must be reachable from the deployment host.

**"TopicAuthorizationException"**  
The ACL only allows resources starting with `iiitd_`. Check `RS_TOPIC_PREFIX=iiitd_`.

**"Offset reset policy"**  
Default is `latest`. To reprocess from the beginning (e.g. after a code fix), manually reset offsets:
```bash
kafka-consumer-groups --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --command-config client.properties \
  --group iiitd_cg_normalize --reset-offsets --to-earliest --execute --all-topics
```

**Fuse not confirming hazards**  
Check `RS_CONFIRM_MIN_VEHICLES` (default 3). The log-odds model requires 5-6 vehicles at high severity (0.8+) to reach confidence ≥ 0.8. Increase `RS_DEMO_TIME_SCALE` to accelerate time decay.

---

## Tests

All 31 tests pass locally:
```bash
pip install -r requirements.txt
pytest -v
```

**Coverage:**
- **normalize**: 11 tests (one per DLQ reason + valid + speed mismatch)
- **detect**: 6 tests (pothole, speed breaker, clean, waterlogging on/off)
- **fuse**: 6 tests (3-vehicle confirm, single-vehicle candidate, clean decay, quarantine, merge, recovery)
- **alert**: 5 tests (advisory, warning, behind, cooldown, speed-breaker filtered)

---

## License

MIT. Map data © OpenStreetMap contributors ([ODbL](https://www.openstreetmap.org/copyright)).

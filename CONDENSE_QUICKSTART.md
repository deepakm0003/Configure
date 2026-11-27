# Condense Quick Start Guide

## Pre-flight Checklist

✅ Code pushed to: https://github.com/deepakm0003/Configure.git  
✅ 31 tests passing locally  
✅ Kafka credentials from Condense:
   - Bootstrap: `20.213.158.116:9094`
   - Username: `iiitd`
   - Password: `<YOUR_KAFKA_PASSWORD>`

---

## Step 1: Create Topics

**⚠️ Run this ONCE from a machine with network access to the Kafka cluster.**

If running from local Windows (may have firewall issues), skip to Condense deployment and create topics from within a Condense container shell.

```bash
# Set environment
export KAFKA_BOOTSTRAP_SERVERS=20.213.158.116:9094
export KAFKA_SASL_USERNAME=iiitd
export KAFKA_SASL_PASSWORD=<YOUR_KAFKA_PASSWORD>
export KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
export KAFKA_SASL_MECHANISM=SCRAM-SHA-512
export RS_TOPIC_PREFIX=iiitd_

# Install
pip install confluent-kafka

# Create all 11 topics
python scripts/create_topics.py
```

**Expected output:**
```
Creating 11 topic(s)…
  OK     'iiitd_telemetry_raw'
  OK     'iiitd_telemetry_clean'
  OK     'iiitd_telemetry_dlq'
  ...
```

---

## Step 2: Create 4 Apps in Condense

### App 1: **rs-normalize**

**New Application > From Git Repository**

| Field | Value |
|-------|-------|
| Repository URL | `https://github.com/deepakm0003/Configure.git` |
| Branch | `main` |
| Dockerfile Path | `./Dockerfile` |
| App Name | `rs-normalize` |

**Environment Variables:**
```
KAFKA_BOOTSTRAP_SERVERS=20.213.158.116:9094
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_USERNAME=iiitd
KAFKA_SASL_PASSWORD=<YOUR_KAFKA_PASSWORD>
RS_TOPIC_PREFIX=iiitd_
RS_ROLE=normalize
RS_MAX_LATENESS_S=60
RS_BBOX_LAT_MIN=12.80
RS_BBOX_LAT_MAX=13.10
RS_BBOX_LON_MIN=77.45
RS_BBOX_LON_MAX=77.80
```

**Build > Publish > Deploy**

---

### App 2: **rs-detect**

Same repo, same Dockerfile.

**Environment Variables:**
```
KAFKA_BOOTSTRAP_SERVERS=20.213.158.116:9094
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_USERNAME=iiitd
KAFKA_SASL_PASSWORD=<YOUR_KAFKA_PASSWORD>
RS_TOPIC_PREFIX=iiitd_
RS_ROLE=detect
RS_Z_THRESHOLD=4.0
```

**Build > Publish > Deploy**

---

### App 3: **rs-fuse**

Same repo, same Dockerfile.

**Environment Variables:**
```
KAFKA_BOOTSTRAP_SERVERS=20.213.158.116:9094
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_USERNAME=iiitd
KAFKA_SASL_PASSWORD=<YOUR_KAFKA_PASSWORD>
RS_TOPIC_PREFIX=iiitd_
RS_ROLE=fuse
RS_CONFIRM_MIN_VEHICLES=3
RS_DEMO_TIME_SCALE=10
```

**⚠️ IMPORTANT:** Increase startup timeout to 120s (rs-fuse rebuilds state from compacted topics).

**Build > Publish > Deploy**

---

### App 4: **rs-alert**

Same repo, same Dockerfile.

**Environment Variables:**
```
KAFKA_BOOTSTRAP_SERVERS=20.213.158.116:9094
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_USERNAME=iiitd
KAFKA_SASL_PASSWORD=<YOUR_KAFKA_PASSWORD>
RS_TOPIC_PREFIX=iiitd_
RS_ROLE=alert
RS_ALERT_RADIUS_M=500
```

**Build > Publish > Deploy**

---

## Step 3: Verify Deployment

Check logs in Condense:

**rs-normalize:**
```json
{"ts": 1732..., "level": "INFO", "role": "normalize", "msg": "starting", "input": "iiitd_telemetry_raw", "output": "iiitd_telemetry_clean", "dlq": "iiitd_telemetry_dlq"}
```

**rs-detect:**
```json
{"ts": 1732..., "level": "INFO", "role": "detect", "msg": "starting", "input": "iiitd_telemetry_clean", "output": "iiitd_road_events"}
```

**rs-fuse:**
```json
{"ts": 1732..., "level": "INFO", "role": "fuse", "msg": "recovery_start"}
{"ts": 1732..., "level": "INFO", "role": "fuse", "msg": "recovery_done", "hazards": 0, "reputations": 0}
{"ts": 1732..., "level": "INFO", "role": "fuse", "msg": "starting", "input": "iiitd_road_events"}
```

**rs-alert:**
```json
{"ts": 1732..., "level": "INFO", "role": "alert", "msg": "starting", "ui_out": "iiitd_ui_events"}
```

---

## Step 4: Run the Simulator

**Option A: Locally** (easiest for demo)

```bash
# 1. Download road network once
pip install osmnx
python -m simulator.build_roads

# 2. Set Kafka env vars
export KAFKA_BOOTSTRAP_SERVERS=20.213.158.116:9094
export KAFKA_SASL_USERNAME=iiitd
export KAFKA_SASL_PASSWORD=<YOUR_KAFKA_PASSWORD>
export RS_TOPIC_PREFIX=iiitd_
export SIM_TRANSPORT=kafka
export SIM_NUM_VEHICLES=200

# 3. Run (produces 200 msgs/s to iiitd_telemetry_raw)
python -m simulator.run
```

**Option B: Containerize and deploy as 5th Condense app**

Create `simulator/Dockerfile`:
```dockerfile
FROM python:3.11-slim
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt osmnx
COPY simulator/ ./simulator/
RUN python -m simulator.build_roads
CMD ["python", "-m", "simulator.run"]
```

Deploy with same Kafka env vars + `SIM_TRANSPORT=kafka` + `SIM_NUM_VEHICLES=200`.

---

## Step 5: Inspect the Pipeline

Use the monitoring UI in Condense or run scripts locally:

```bash
# Check DLQ for rejected messages
python scripts/tail.py iiitd_telemetry_dlq --n 10

# Check confirmed hazards
python scripts/tail.py iiitd_hazards_state --n 20

# Check driver alerts
python scripts/tail.py iiitd_ui_events --n 50
```

---

## Troubleshooting

**"Broker transport failure" when creating topics locally**  
Your local machine can't reach `20.213.158.116:9094`. Two options:
1. Run `scripts/create_topics.py` from **inside** a Condense container shell.
2. Ask Condense to whitelist your IP or use a VPN.

**Apps crash on startup**  
Check environment variables are set correctly. Missing `KAFKA_SASL_PASSWORD` or wrong `RS_TOPIC_PREFIX` will cause auth failures.

**rs-fuse timeout on startup**  
If `iiitd_hazards_state` is very large, increase the app's startup timeout in Condense settings.

**No messages in ui_events**  
Check all 4 apps are running and consuming. Verify simulator is producing to `iiitd_telemetry_raw`:
```bash
python scripts/tail.py iiitd_telemetry_raw --n 5
```

---

## Next Steps

1. Build a **React dashboard** that consumes `iiitd_ui_events` via WebSocket (or HTTP polling).
2. Add **API endpoints** to query `iiitd_hazards_state` (read from Kafka or mirror to Postgres).
3. Implement **city repair workflow**: consume `iiitd_city_tickets`, track SLA, emit repair-time metrics.
4. Deploy the **frontend** to Condense (separate app with NGINX serving React build).

---

## 🎉 You're Done!

The pipeline is now live on Condense:
- **Simulator** produces 200 msgs/s
- **rs-normalize** validates & enriches
- **rs-detect** finds potholes, speed breakers, waterlogging, etc.
- **rs-fuse** confirms hazards with multi-vehicle consensus
- **rs-alert** sends driver warnings

**GitHub:** https://github.com/deepakm0003/Configure.git  
**Docs:** [DEPLOYMENT.md](./DEPLOYMENT.md) | [README.md](./README.md)

# RoadSense: Condense Platform Guide (click by click)

This guide is based on the public Condense docs (docs.condense.io, v2.6 and v3.0 pages). The hackathon workspace UI may differ slightly. **If something below doesn't match your screen, trust the screen and the starter kit, then ask in #condense-help.**

> **YOUR REAL VALUES (from the Kafka sandbox console):** prefix **`iiitd_`** (topics and groups must start with it). Write every name below as `iiitd_` + the name with dots replaced by underscores, e.g. `prefix.telemetry.raw` → **`iiitd_telemetry_raw`**, `prefix.cg.fuse` → `iiitd_cg_fuse`. Bootstrap `20.219.168.118:9093`, **SASL_PLAINTEXT**, **SCRAM-SHA-512**, username `iiitd`, and the password only in env, never in git. Topics are created at kafka-sandbox.condense.io → Topics. Pipelines and apps live at app.condense.io (workspace IIITD). **Use the built-in Vapr assistant** ("Create an application", "Deploy a published application") to save time.

Throughout, `prefix` means the team prefix you received with your Kafka login (for example `byte-runners`). Every topic and consumer group must start with it.

---

## 0. Before the build window (Fri 27 Nov)

1. Sign in to your team workspace. Confirm the workspace name matches your team.
2. **Environment check:** run the sample pipeline from the starter kit and confirm you can see topics. Teams that fail this check cannot start.
3. Open the **Kafka Admin Dashboard / Kafka management portal** login you were sent. Find:
   - your **prefix**
   - the bootstrap server address, port and auth method (the docs list **SASL SCRAM-SHA-512** or mTLS over TLS)
   - the **usage limits** (topics, partitions, message size, retention, throughput)
4. Open **Applications → New application → start from a template**. Read the template code and write down:
   - **the env var names Condense injects** for the Kafka bootstrap servers, credentials, input topic(s) and output topic(s). The public docs don't list them. Our code reads them in one place (`rs_common/config.py`), so you only change that one file.
   - whether the Dockerfile must be at the repo root
   - which port an app must listen on if "Expose HTTP(s)" is used
5. Ask in #condense-help: **"Do transforms and no-code utilities count toward the 10-connector limit?"** Choose the full build (10 components) or the lean build (8) based on the answer.
6. Create the external accounts: GitHub (private repo `roadsense`), HiveMQ Cloud **or** EMQX Serverless (free MQTT with TLS), Neon **or** Supabase (Postgres), Render/Railway (API), Vercel (frontend).

> You may *read* everything on Friday, but do not create the pipeline or write backend code before **Sat 00:00 IST**.

---

## 1. Create the topics (Sat ~03:00)

In the Kafka Admin Dashboard → **Resource Management → Topics** (or the Condense Topics page), create:

| Topic | Partitions | Retention / cleanup |
|---|---|---|
| `prefix.telemetry.raw` | 3 | 6 h, delete |
| `prefix.telemetry.clean` | 3 | 6 h, delete |
| `prefix.telemetry.dlq` | 1 | 24 h, delete |
| `prefix.road.events` | 3 | 24 h, delete |
| `prefix.hazards.state` | 3 | **cleanup.policy = compact** |
| `prefix.vehicles.reputation` | 1 | **cleanup.policy = compact** |
| `prefix.hazards.critical` | 1 | 24 h, delete |
| `prefix.hazards.minor` | 1 | 24 h, delete |
| `prefix.city.tickets` | 1 | 24 h, delete |
| `prefix.ui.events` | 1 | 6 h, delete |

If compaction can't be set, use a long retention for `hazards.state`. rs-fuse still rebuilds its state by replaying the topic from the beginning.

Consumer groups (created automatically when the apps start; names must use the prefix): `prefix.cg.normalize`, `prefix.cg.detect`, `prefix.cg.fuse`, `prefix.cg.alert`.

---

## 2. Component #1: MQTT Input connector (vehicle telemetry)

Path: workspace menu → **Library → Types → Input → MQTT** → fill in the **Environment Variables** tab → **Deploy Connector**.

| Field | Value |
|---|---|
| Title / Connector Name | `rs-mqtt-telemetry-in` |
| Broker URL | your HiveMQ/EMQX host, e.g. `xxxx.s1.eu.hivemq.cloud` |
| Broker Port | `8883` (TLS) |
| Client ID | `rs-condense-in-1` |
| MQTT Topic | `rs/telemetry/#` |
| Auth | Username & Password (a broker user created just for Condense) |
| TLS | One-Way TLS |
| QoS Level | 1 |
| Kafka Topic | `prefix.telemetry.raw` |
| Payload Format | JSON |

The connector supports MQTT **v3.1.1** only. Set the simulator's client to v3.1.1 as well.

**Fallback A:** **HTTP Input** in **Webhook** mode. The simulator POSTs batches to the webhook endpoint, with Output Kafka Topic = `prefix.telemetry.raw`, JSON format and API-key auth.
**Fallback B:** the simulator produces straight to Kafka with the SASL credentials. Use this only if both connectors fail, and say so in the write-up.

Check: in **Applications → Test your application → Subscribe tester**, subscribe to `prefix.telemetry.raw` and confirm that JSON messages arrive.

---

## 3. Custom transforms (#2–#5): one repo, one image, four apps

We use **one Git repo** with a root `Dockerfile`. The same image runs any stage; the `RS_ROLE` env var picks which one (`normalize | detect | fuse | alert`). This avoids any uncertainty about subfolder support.

### 3.1 Create each application

Repeat four times, once per role:

1. **Applications → New Application**
   - Name (lowercase, 3–63 chars): `rsnormalize`, `rsdetect`, `rsfuse`, `rsalert`
   - Git provider: GitHub → repo `roadsense` → branch `main` (the branch can only be changed until the first build)
2. Open it in the **browser IDE** (File Explorer, Code Editor, ENVs panel). Check that `Dockerfile` and `requirements.txt` are detected.
3. **ENVs panel:** set the env vars from the table below. Pass secrets as ENVs and never bake them into the image.
4. **Build Application:** Docker Image Name e.g. `rs-pipeline`, Tag `v1.0.0`. Watch the build logs.
5. **Test:** use the **Publish tester** to send a sample message to the input topic and the **Subscribe tester** to see the output.
6. **Publish** as **Transform**, version `v1` (published versions are immutable, so bump to `v1.1` for each fix).

### 3.2 Deploy each transform on the pipeline canvas

**Pipeline → + Add Transform → Custom →** choose the app + version → set the topics and env → **Deploy**. The canvas draws a line automatically when one block's **Output Topic exactly matches** the next block's **Input Topic**.

| # | App | Input topic(s) | Output topic(s) | Key env vars |
|---|---|---|---|---|
| 2 | `rsnormalize` | `prefix.telemetry.raw` | `prefix.telemetry.clean`, `prefix.telemetry.dlq` | `RS_ROLE=normalize`, `RS_GROUP=prefix.cg.normalize`, `RS_MAX_LATENESS_S=30` |
| 3 | `rsdetect` | `prefix.telemetry.clean` | `prefix.road.events` | `RS_ROLE=detect`, `RS_GROUP=prefix.cg.detect`, `RS_Z_THRESHOLD=4.0` |
| 4 | `rsfuse` | `prefix.road.events` | `prefix.hazards.state`, `prefix.vehicles.reputation` | `RS_ROLE=fuse`, `RS_GROUP=prefix.cg.fuse`, `RS_CONFIRM_MIN_VEHICLES=3`, `RS_CONFIRM_CONF=0.8`, `RS_CLEAR_CONF=0.2`, `RS_DEMO_TIME_SCALE=10` |
| 5 | `rsalert` | `prefix.hazards.state`, `prefix.vehicles.reputation`, `prefix.telemetry.clean` | `prefix.ui.events` | `RS_ROLE=alert`, `RS_GROUP=prefix.cg.alert`, `RS_ALERT_RADIUS_M=500`, `RS_ALERT_COOLDOWN_S=300` |

rs-detect emits one observation per vehicle per cell. An observation with no detections is a **clean pass**, so rs-fuse gets its positive and negative evidence from the single topic `road.events`, already partitioned by cell.

Shared env on all four (map these to whatever Condense injects, see step 0.4): `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SECURITY_PROTOCOL=SASL_SSL`, `KAFKA_SASL_MECHANISM=SCRAM-SHA-512`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `RS_TOPIC_PREFIX=prefix`, `LOG_LEVEL=INFO`.

> If the canvas form only accepts **one** input topic, the app reads its second topic from an env var (`RS_EXTRA_INPUT_TOPICS`) and subscribes to both itself. The code supports this.

---

## 4. Component #6: Split utility (no-code)

**Transforms → Split Utility** (or + Add Transform → Prebuilt → Split)

- Utility Name: `rs-split-severity`
- Source Kafka Topic: `prefix.hazards.state` → let it read the schema
- Strategy: **First Match Only**
- Profile 1, `critical` → Destination `prefix.hazards.critical`
  - Rule group (AND): `status` **Equals** `CONFIRMED` AND `severity_score` **Greater Than** `0.6`
- Profile 2, `minor` → Destination `prefix.hazards.minor`
  - Rule group: `status` **Equals** `CONFIRMED`
- Check the live preview with a sample hazard JSON, then **Deploy Utility**.

## 5. Component #7: Data utility (no-code), full build only

**Transforms → Data Utility**

- Utility Name: `rs-ticket-shaper`
- Input Topic `prefix.hazards.critical` → Output Topic `prefix.city.tickets`
- Rule operations:
  - **Add field** `department` = `"BBMP Road Infrastructure"`
  - **Add field** `ticket_type` = `"ROAD_HAZARD_REPAIR"`
  - **Add field** `sla_hours` = `24`
  - **Rename** `hazard_type` → `issue`
  - **Copy** `centroid.lat` → `location.lat` and `centroid.lon` → `location.lon` (build a nested structure)
  - **Remove** `reporters` (keeps the ticket payload small)
- Check the preview, then **Deploy Utility**.

## 6. Component #8: PostgreSQL Output (hazard history)

**Library → Types → Output → PostgreSQL**

| Field | Value |
|---|---|
| Title | `rs-pg-hazards-out` |
| Server Name | Neon/Supabase host |
| Server Port | `5432` |
| Database Name | `roadsense` |
| Username / Password | a DB user that can only write to the `hazards` table |
| Topic (Input) | `prefix.hazards.state` |
| Query | see below |

```sql
INSERT INTO hazards (hazard_id, hazard_type, status, confidence, severity_score,
                     distinct_vehicles, lat, lon, h3_cell, first_seen_ms, last_update_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (hazard_id) DO UPDATE SET
  status = EXCLUDED.status, confidence = EXCLUDED.confidence,
  severity_score = EXCLUDED.severity_score, distinct_vehicles = EXCLUDED.distinct_vehicles,
  last_update_ms = EXCLUDED.last_update_ms;
```

Map the `?` placeholders in order to the event fields: `hazard_id, hazard_type, status, confidence, severity_score, distinct_vehicles, centroid.lat, centroid.lon, h3_cell, first_seen_ms, last_update_ms`. If nested fields can't be mapped, rs-fuse also emits flat `lat` and `lon` fields, and that's why. The API creates the table on startup (`api/db/schema.sql`).

## 7. Components #9 and #10: HTTP Output connectors → API gateway

**Library → Types → Output → HTTP**

| Field | #9 `rs-http-ui-out` | #10 `rs-http-tickets-out` |
|---|---|---|
| Input Kafka Topic | `prefix.ui.events` | `prefix.city.tickets` |
| Message Format | JSON | JSON |
| Endpoint URL | `https://<your-api>.onrender.com/ingest/ui-events` | `https://<your-api>.onrender.com/ingest/tickets` |
| HTTP Method | POST | POST |
| Content Type | application/json | application/json |
| Auth | API Key → header `X-RS-Key: <random 32 chars>` | same |
| Batch Size / Timeout | 50 / 200 ms (low latency) | 1 / 1000 ms |
| Error handling | Retry, interval 1 s, max 5 | Retry |

The API must accept both a single JSON object and a JSON array (batch). The code handles both.

---

## 8. Observability (show this in the video; it counts toward the 30%)

- **Pipeline view:** every block green (Running), live CPU and memory per block, real-time logs.
- **Transforms:** throughput, processing latency, error rate, version history (show v1 → v1.1).
- **Runtime alerts:** create **Consumer Lag** alerts on `rsfuse` and `rsalert` (threshold e.g. 500 messages for 60 s) and a **CrashLoopBackOff** alert on every custom app. Show one firing when you scale the vehicles to 500.
- **DLQ:** open the Subscribe tester on `prefix.telemetry.dlq` and show the reason codes (`SCHEMA_INVALID`, `IMPOSSIBLE_JUMP`, `LATE_EVENT`, `DUPLICATE`).

## 9. Judge access (submission item 3)

- Put the pipeline link in the form. In the README, explain how judges can view it (ask organizers in #general-queries whether they get read access automatically or you must add a Viewer).
- **Never** put passwords in the form or the repo.

## 10. Troubleshooting

| Symptom | Fix |
|---|---|
| No line between blocks on the canvas | Output topic ≠ input topic (check spelling and the prefix) |
| App crash-loops | Check the build logs and stdout. Usually a missing env var or a wrong SASL mechanism |
| Messages stuck / consumer lag growing | Increase partitions, or check that the app commits offsets. Batch producer flush |
| MQTT connector up but no data | Broker ACL, TLS port 8883, MQTT v3.1.1, topic filter `rs/telemetry/#` |
| A field is locked when editing | Delete and redeploy that block (locked fields can't be edited) |
| App deleted | Drafts inactive for 3 h are auto-deleted. Your code lives in Git, so re-create the app from the repo |

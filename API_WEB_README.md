# API + Web Dashboard

## 🏗️ Architecture

```
┌─────────────────┐
│  React Web      │  MapLibre GL + deck.gl + Recharts
│  (Vercel)       │  Dark control-room theme
└────────┬────────┘
         │ WebSocket
         │ (auto-reconnect)
         ▼
┌─────────────────┐
│  FastAPI        │  /ws, /api/*, /ingest/*
│  (Render)       │  WebSocket hub + scoring loop
└────────┬────────┘
         │
    ┌────┴─────┬─────────────┐
    ▼          ▼             ▼
┌──────┐  ┌────────┐  ┌──────────┐
│ PG   │  │ Kafka  │  │ Condense │
│ DB   │  │ Control│  │ HTTP Out │
└──────┘  └────────┘  └──────────┘
```

---

## 📦 What's Included

### API (`api/`)
- ✅ **FastAPI** with async PostgreSQL (asyncpg)
- ✅ **WebSocket hub** (snapshot + stream, auto-reconnect, heartbeat 15s)
- ✅ **Scoring engine** (precision, recall, F1, TTD) recomputed every 2s
- ✅ **Ingest endpoints** for Condense HTTP Output (`/ingest/ui-events`, `/ingest/tickets`)
- ✅ **Sim control** (`/sim/control` → Kafka `iiitd_sim_control`)
- ✅ **HMI alert forwarding** (driver_alert → hmi_alert command to simulator)
- ✅ **Ticket repair workflow** (`POST /api/tickets/{id}/repair`)
- ✅ **CORS**, API key auth, health endpoint

### Web (`web/`)
- ✅ **React 18** + **Vite** + **TypeScript** + **Tailwind CSS**
- ✅ **Zustand** state management
- ✅ **MapLibre GL** + **deck.gl** (H3HexagonLayer, ScatterplotLayer)
- ✅ **Recharts** for metrics visualisation
- ✅ **Dark control-room theme** (slate-900 bg, cyan/orange/red accents)
- ✅ **KPI bar** (vehicles online, msgs/s, latency P95, confirmed hazards, precision/recall)
- ✅ **Side tabs:** Driver view, City tickets, Accuracy, Pipeline metrics, Chaos controls
- ✅ **Auto-reconnecting WebSocket**
- ✅ **Toast notifications** for new CONFIRMED hazards

---

## 🚀 Quick Start

### Local Development

**Prerequisites:**
- Python 3.11+
- Node 18+
- PostgreSQL 15+
- Kafka access (or use Condense cluster)

**1. API:**
```bash
cd api
pip install -r requirements.txt

# Create .env
cat > .env << EOF
DATABASE_URL=postgresql://user:pass@localhost/roadsense
RS_API_KEY=dev-key-123
KAFKA_BOOTSTRAP_SERVERS=20.213.158.116:9094
KAFKA_SASL_USERNAME=iiitd
KAFKA_SASL_PASSWORD=<YOUR_KAFKA_PASSWORD>
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
RS_TOPIC_PREFIX=iiitd_
CORS_ORIGINS=*
PORT=8000
EOF

# Run
uvicorn api.app.main:app --reload
```

**2. Web:**
```bash
cd web
npm install

# Create .env.local
cat > .env.local << EOF
VITE_API_HTTP=http://localhost:8000
VITE_API_WS=ws://localhost:8000/ws
VITE_RS_CONTROL_KEY=dev-key-123
EOF

npm run dev
```

Open http://localhost:5173

**3. Docker Compose (All Services):**
```bash
docker-compose up -d
```
- API: http://localhost:8000
- Web: http://localhost:3000
- PG: localhost:5432

---

## 🌐 Production Deployment

### Render (API) + Vercel (Web)

See **[DEPLOY_API_WEB.md](./DEPLOY_API_WEB.md)** for complete step-by-step instructions.

**TL;DR:**

1. **Render:** Create Postgres DB → Deploy API from `api/` folder
2. **Vercel:** Import repo → Set root dir to `web/` → Deploy
3. **Condense:** Add HTTP Output utility pointing to your Render API URL

**Production URLs:**
- API: `https://roadsense-api.onrender.com`
- Web: `https://roadsense-web.vercel.app`

---

## 📡 API Endpoints

### Ingest (Condense → API)

**POST /ingest/ui-events**  
Header: `X-RS-Key: <RS_API_KEY>`  
Body: `ui_events` message or array  
Updates in-memory state + broadcasts over WebSocket

**POST /ingest/tickets**  
Header: `X-RS-Key: <RS_API_KEY>`  
Body: Shaped ticket or raw hazard  
Upserts into `tickets` table

### Sim Control (Web → API → Kafka)

**POST /sim/control**  
Header: `X-RS-Key: <RS_API_KEY>`  
Body:
```json
{"cmd": "rain", "on": true}
{"cmd": "add_hazard", "hazard_type": "POTHOLE", "lat": 12.935, "lon": 77.624, "severity": 0.8}
{"cmd": "faulty_vehicle", "count": 1}
```
Produces to Kafka `iiitd_sim_control`

**POST /sim/ground-truth**  
Header: `X-RS-Key: <RS_API_KEY>`  
Body: Ground truth hazard from simulator  
Upserts into `ground_truth` table

### Tickets (Web → API)

**POST /api/tickets/{hazard_id}/repair**  
Marks ticket as `REPAIR_DISPATCHED`, sends repair command to simulator

### Query (Web → API)

**GET /api/snapshot**  
Returns current in-memory state (vehicles, hazards, reputation, tickets, metrics)

**GET /api/score**  
Returns current accuracy score (precision, recall, F1, TTD)

**GET /api/metrics**  
Returns pipeline metrics (msgs/s, latency p50/p95)

**GET /health**  
Health check: `{"status": "ok", "ts": 1732...}`

### WebSocket

**WS /ws**  
Sends snapshot on connect, then streams:
- `hazard_update`
- `driver_alert`
- `alert_outcome`
- `reputation_update`
- `vehicle_positions` (every 1s)
- `pipeline_stats` (every 5s)
- `score_update` (every 2s)
- `ping` (heartbeat every 15s)

---

## 🗄️ Database Schema

**Tables:**
- `hazards` (PK: hazard_id) – synced from Condense PG Output
- `tickets` (PK: hazard_id) – repair workflow
- `ground_truth` (PK: gt_id) – from simulator
- `alert_outcomes` (serial PK) – for scoring

Schema auto-applied at startup (idempotent).

---

## 🎨 Web Features

### KPI Bar
- 🟢 Vehicles online
- 📊 Msgs/s, Latency P95
- 🟠 Confirmed hazards
- 🔴 Open tickets
- ✅ Precision, Recall

### Map
- **Vehicles:** Dots (quarantined = red ring)
- **Hazards:** H3HexagonLayer
  - Colour by type (red=pothole, yellow=speed-breaker, blue=waterlogging, etc.)
  - Opacity by confidence
  - **CONFIRMED:** pulsing animation
  - **CANDIDATE:** dashed outline
- **Waterlogging zones:** Blue circles
- **Click hazard:** Popover with details

### Side Tabs

**Driver View:**
- Follow one vehicle
- Big HUD alert banner when approaching hazard
- Distance countdown

**City Tickets:**
- List sorted by severity
- Filter: OPEN / REPAIR_DISPATCHED / CLOSED
- "Mark Repaired" button

**Accuracy:**
- Precision, Recall, F1 gauge charts (Recharts)
- TTD mean & P90 bar chart
- Classification confusion matrix

**Pipeline:**
- Msgs/s bar chart per stage
- Latency p50/p95 line chart over time

**Chaos:**
- Add Pothole (click map to place)
- Toggle Rain (on/off)
- Add Faulty Vehicle
- Set Vehicle Count (100/200/300)

---

## 🧪 Testing

### API Health
```bash
curl https://roadsense-api.onrender.com/health
```

### WebSocket
```javascript
const ws = new WebSocket('wss://roadsense-api.onrender.com/ws')
ws.onmessage = e => console.log(JSON.parse(e.data))
```

### Sim Control
```bash
curl -X POST https://roadsense-api.onrender.com/sim/control \
  -H "X-RS-Key: your-key" \
  -H "Content-Type: application/json" \
  -d '{"cmd":"rain","on":true}'
```

---

## 🔐 Security

- **API Key Auth:** All ingest/control endpoints require `X-RS-Key` header
- **CORS:** Configured via `CORS_ORIGINS` env var
- **PostgreSQL:** Connection pooling (max 10), SSL recommended
- **Secrets:** Never commit `.env` or `.env.local`

---

## 📊 Accuracy Scoring (algorithms.md § F)

Recomputed every 2s, broadcast as `score_update`:

**Precision:**  
`confirmed_hazards_matching_GT / all_confirmed_hazards`

**Recall:**  
`detected_active_GT / active_GT_with_≥3_passes`

**F1:**  
`2 × precision × recall / (precision + recall)`

**Time-to-Detect (TTD):**  
`confirmed_ms - max(gt.created_ms, sim_start_ms)`  
Reported as mean & p90 (seconds)

**Classification Accuracy:**  
Speed-breaker GT wrongly confirmed as POTHOLE → false positive

**False Hazards Prevented:**  
CANDIDATEs cleared as "unconfirmed" with mostly quarantined reporters

---

## 🛠️ Troubleshooting

**WebSocket won't connect:**
- Check CORS allows web domain
- Verify WebSocket URL uses `wss://` (secure) for HTTPS sites
- Check API health endpoint responds

**API crashes on startup:**
- Check `DATABASE_URL` is correct
- Ensure all required env vars are set
- View logs: Render Dashboard → Service → Logs

**Scores not updating:**
- Check ground truth data exists (`SELECT * FROM ground_truth`)
- Verify hazards table has CONFIRMED entries
- Scoring loop runs every 2s (check API logs)

**Map not rendering:**
- Check browser console for errors
- Verify MapLibre GL and deck.gl imports
- MapLibre requires attribution for map tiles

---

## 🏆 Production Checklist

- [ ] Generate secure `RS_API_KEY` (not "dev-key-123")
- [ ] Set specific `CORS_ORIGINS` (not "*")
- [ ] Enable Render Postgres backups
- [ ] Add custom domain to Vercel
- [ ] Configure Condense HTTP Output to production API
- [ ] Test end-to-end flow: simulator → pipeline → API → web
- [ ] Set up monitoring/alerts
- [ ] Configure rate limiting on API
- [ ] Enable API HTTPS only
- [ ] Add error tracking (Sentry, LogRocket)

---

## 📚 Documentation

- **[DEPLOY_API_WEB.md](./DEPLOY_API_WEB.md)** – Complete deployment guide
- **[CONDENSE_QUICKSTART.md](./CONDENSE_QUICKSTART.md)** – Pipeline deployment
- **[README.md](./README.md)** – Project overview
- **[algorithms.md](./kiro-steering/algorithms.md)** – Algorithm specs
- **[data-contracts.md](./kiro-steering/data-contracts.md)** – Topic schemas

---

## 🎉 You're Done!

**Local:** http://localhost:5173  
**Production:** https://roadsense-web.vercel.app

The dashboard now visualises the entire RoadSense pipeline in real-time!

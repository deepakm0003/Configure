# API + Web Deployment Guide

## API Deployment (Render)

### Step 1: Create PostgreSQL Database on Render

1. Go to https://dashboard.render.com
2. Click **New** → **PostgreSQL**
3. Name: `roadsense-db`
4. Instance Type: Free (or Starter for production)
5. Click **Create Database**
6. Copy the **Internal Database URL** (starts with `postgres://`)

### Step 2: Deploy API to Render

1. Click **New** → **Web Service**
2. Connect your GitHub repo: `https://github.com/deepakm0003/Configure.git`
3. Configure:
   - **Name**: `roadsense-api`
   - **Root Directory**: `api`
   - **Environment**: `Docker`
   - **Dockerfile Path**: `api/Dockerfile`
   - **Instance Type**: Free (or Starter)

4. **Environment Variables:**
   ```
   DATABASE_URL=<Internal Database URL from Step 1>
   RS_API_KEY=your-secret-api-key-here
   KAFKA_BOOTSTRAP_SERVERS=20.213.158.116:9094
   KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
   KAFKA_SASL_MECHANISM=SCRAM-SHA-512
   KAFKA_SASL_USERNAME=iiitd
   KAFKA_SASL_PASSWORD=U2i91UPq8z2-3AB3GImft4Q7QKUG8cXk
   RS_TOPIC_PREFIX=iiitd_
   CORS_ORIGINS=*
   PORT=10000
   ```

5. Click **Create Web Service**

6. Wait for deployment (5-10 minutes)

7. Your API will be available at: `https://roadsense-api.onrender.com`

### Step 3: Configure Condense HTTP Output

In Condense, add an **HTTP Output** utility:

**Target:** `https://roadsense-api.onrender.com/ingest/ui-events`  
**Headers:**
```
X-RS-Key: your-secret-api-key-here
Content-Type: application/json
```
**Input Topic:** `iiitd_ui_events`

---

## Web Deployment (Vercel)

### Step 1: Install Vercel CLI (optional)

```bash
npm i -g vercel
```

### Step 2: Deploy to Vercel

**Option A: Via Vercel Dashboard**

1. Go to https://vercel.com/new
2. Import your GitHub repo: `https://github.com/deepakm0003/Configure.git`
3. Configure:
   - **Framework Preset**: Vite
   - **Root Directory**: `web`
   - **Build Command**: `npm run build`
   - **Output Directory**: `dist`

4. **Environment Variables:**
   ```
   VITE_API_HTTP=https://roadsense-api.onrender.com
   VITE_API_WS=wss://roadsense-api.onrender.com/ws
   VITE_RS_CONTROL_KEY=your-secret-api-key-here
   ```

5. Click **Deploy**

6. Your site will be live at: `https://roadsense-web.vercel.app`

**Option B: Via CLI**

```bash
cd web
npm install
vercel --prod
# Follow prompts, set env vars when asked
```

### Step 3: Update CORS on API

Update the API's `CORS_ORIGINS` env var on Render:
```
CORS_ORIGINS=https://roadsense-web.vercel.app,http://localhost:5173
```

---

## Local Development

### API (Local)

```bash
cd api

# Create .env
cat > .env << EOF
DATABASE_URL=postgresql://user:pass@localhost:5432/roadsense
RS_API_KEY=dev-key-123
KAFKA_BOOTSTRAP_SERVERS=20.213.158.116:9094
KAFKA_SASL_USERNAME=iiitd
KAFKA_SASL_PASSWORD=U2i91UPq8z2-3AB3GImft4Q7QKUG8cXk
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
RS_TOPIC_PREFIX=iiitd_
CORS_ORIGINS=*
PORT=8000
EOF

# Install
pip install -r requirements.txt

# Run
uvicorn api.app.main:app --reload --port 8000
```

API will be at: http://localhost:8000

### Web (Local)

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

Web will be at: http://localhost:5173

---

## Docker Compose (All Services Local)

Create `docker-compose.yml` at root:

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: roadsense
      POSTGRES_USER: roadsense
      POSTGRES_PASSWORD: dev123
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

  api:
    build:
      context: ./api
      dockerfile: Dockerfile
    ports:
      - "8000:8000"
    environment:
      DATABASE_URL: postgresql://roadsense:dev123@postgres:5432/roadsense
      RS_API_KEY: dev-key-123
      KAFKA_BOOTSTRAP_SERVERS: 20.213.158.116:9094
      KAFKA_SASL_USERNAME: iiitd
      KAFKA_SASL_PASSWORD: U2i91UPq8z2-3AB3GImft4Q7QKUG8cXk
      KAFKA_SECURITY_PROTOCOL: SASL_PLAINTEXT
      KAFKA_SASL_MECHANISM: SCRAM-SHA-512
      RS_TOPIC_PREFIX: iiitd_
      CORS_ORIGINS: "*"
      PORT: 8000
    depends_on:
      - postgres

  web:
    build:
      context: ./web
      dockerfile: Dockerfile
    ports:
      - "80:80"
    environment:
      VITE_API_HTTP: http://localhost:8000
      VITE_API_WS: ws://localhost:8000/ws
      VITE_RS_CONTROL_KEY: dev-key-123

volumes:
  pgdata:
```

Run:
```bash
docker-compose up -d
```

---

## Environment Variables Summary

### API (Render)

| Variable | Value | Required |
|----------|-------|----------|
| `DATABASE_URL` | Postgres connection string | ✅ |
| `RS_API_KEY` | Secret key for auth | ✅ |
| `KAFKA_BOOTSTRAP_SERVERS` | `20.213.158.116:9094` | ✅ |
| `KAFKA_SASL_USERNAME` | `iiitd` | ✅ |
| `KAFKA_SASL_PASSWORD` | `U2i91UPq8z2-3AB3GImft4Q7QKUG8cXk` | ✅ |
| `KAFKA_SECURITY_PROTOCOL` | `SASL_PLAINTEXT` | ✅ |
| `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512` | ✅ |
| `RS_TOPIC_PREFIX` | `iiitd_` | ✅ |
| `CORS_ORIGINS` | `*` or specific domains | ✅ |
| `PORT` | `10000` (Render default) | Auto |

### Web (Vercel)

| Variable | Value | Required |
|----------|-------|----------|
| `VITE_API_HTTP` | `https://roadsense-api.onrender.com` | ✅ |
| `VITE_API_WS` | `wss://roadsense-api.onrender.com/ws` | ✅ |
| `VITE_RS_CONTROL_KEY` | Same as API's `RS_API_KEY` | ✅ |

---

## Testing Deployment

### 1. Health Check
```bash
curl https://roadsense-api.onrender.com/health
# {"status":"ok","ts":1732...}
```

### 2. WebSocket Connection
Open browser console on https://roadsense-web.vercel.app:
```javascript
const ws = new WebSocket('wss://roadsense-api.onrender.com/ws')
ws.onmessage = (e) => console.log(JSON.parse(e.data))
```

### 3. POST UI Event (Test Ingest)
```bash
curl -X POST https://roadsense-api.onrender.com/ingest/ui-events \
  -H "X-RS-Key: your-secret-api-key-here" \
  -H "Content-Type: application/json" \
  -d '{"type":"pipeline_stats","ts_ms":1732000000000,"msgs_per_s":{},"latency_ms":{}}'
```

### 4. Sim Control
```bash
curl -X POST https://roadsense-api.onrender.com/sim/control \
  -H "X-RS-Key: your-secret-api-key-here" \
  -H "Content-Type: application/json" \
  -d '{"cmd":"rain","on":true}'
```

---

## Troubleshooting

**API won't start on Render:**
- Check logs: Dashboard → Service → Logs
- Verify `DATABASE_URL` is correct
- Ensure all required env vars are set

**Web can't connect to API:**
- Check CORS: API must allow web domain
- Verify WebSocket URL uses `wss://` (secure)
- Check API health endpoint responds

**WebSocket disconnects:**
- Render free tier sleeps after 15 min inactivity
- Upgrade to paid tier or use keep-alive pings

**Database connection errors:**
- Render Postgres has connection limit (default 97)
- API uses pool of max 10 connections
- Check Render dashboard for DB metrics

---

## Production Checklist

- [ ] Generate secure `RS_API_KEY` (not "dev-key-123")
- [ ] Set specific `CORS_ORIGINS` (not "*")
- [ ] Enable Render Postgres backups
- [ ] Add custom domain to Vercel
- [ ] Set up monitoring/alerts in Render
- [ ] Configure Condense HTTP Output to production API URL
- [ ] Test end-to-end: simulator → pipeline → API → web

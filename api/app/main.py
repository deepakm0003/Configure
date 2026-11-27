"""
main.py – FastAPI application with WebSocket hub, scoring, ingest, and control.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any

import orjson
from confluent_kafka import Producer
from fastapi import FastAPI, WebSocket, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.app.config import load as load_config
from api.app.db import init_db, close_db, get_pool
from api.app.ws import hub
from api.app.scoring import compute_score, scoring_loop


cfg = load_config()

# In-memory state (last 60 s of vehicle positions, all active hazards, reputation, tickets)
_state: dict[str, Any] = {
    "vehicles": {},            # vehicle_id → {lat, lon, h, s, c, ts_ms}
    "hazards": {},             # hazard_id → HazardState dict
    "reputation": {},          # vehicle_id → reputation dict
    "tickets": {},             # hazard_id → ticket dict
    "metrics": {               # pipeline stats
        "msgs_per_s": {},
        "latency_ms": {},
    },
}

# Kafka producer for sim control
_producer: Producer | None = None


def _auth_key(x_rs_key: str | None) -> None:
    """Raise 401 if key is invalid."""
    if x_rs_key != cfg.api_key:
        raise HTTPException(status_code=401, detail="Unauthorized")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown hooks."""
    global _producer
    
    # Startup
    await init_db(cfg.database_url)
    await hub.start()
    
    # Start Kafka producer if credentials present
    if cfg.kafka_bootstrap_servers:
        _producer = Producer(cfg.kafka_producer_conf())
    
    # Start scoring loop
    asyncio.create_task(scoring_loop())
    
    yield
    
    # Shutdown
    await hub.stop()
    await close_db()
    if _producer:
        _producer.flush(5)


app = FastAPI(title="RoadSense API", lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=cfg.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "ts": int(time.time() * 1000)}


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """WebSocket: snapshot on connect, then stream updates."""
    snapshot = {
        "vehicles": list(_state["vehicles"].values()),
        "hazards": list(_state["hazards"].values()),
        "reputation": list(_state["reputation"].values()),
        "tickets": list(_state["tickets"].values()),
        "metrics": _state["metrics"],
    }
    await hub.connect(ws, snapshot)
    await hub.send_loop(ws)


# ── Ingest ───────────────────────────────────────────────────────────────────

@app.post("/ingest/ui-events")
async def ingest_ui_events(
    request: Request,
    x_rs_key: str | None = Header(None),
):
    """
    Ingest ui_events from rs-alert (Condense HTTP Output).
    Accept one object OR an array.
    """
    _auth_key(x_rs_key)
    
    raw = await request.body()
    data = orjson.loads(raw)
    events = data if isinstance(data, list) else [data]
    
    now_ms = int(time.time() * 1000)
    
    for evt in events:
        evt["api_ms"] = now_ms
        evt_type = evt.get("type")
        
        if evt_type == "hazard_update":
            hz = evt.get("hazard", {})
            _state["hazards"][hz.get("hazard_id")] = hz
            hub.broadcast(evt)
            
            # If CLEARED, close ticket
            if hz.get("status") == "CLEARED":
                await _close_ticket(hz)
        
        elif evt_type == "driver_alert":
            hub.broadcast(evt)
            # Also send HMI alert to simulator
            await _send_hmi_alert(evt)
        
        elif evt_type == "alert_outcome":
            hub.broadcast(evt)
            # Store in DB
            await _store_alert_outcome(evt)
        
        elif evt_type == "reputation_update":
            rep = evt.get("reputation", {})
            _state["reputation"][rep.get("vehicle_id")] = rep
            hub.broadcast(evt)
        
        elif evt_type == "vehicle_positions":
            # Update in-memory state (prune old)
            cutoff = now_ms - 60_000
            for v in evt.get("vehicles", []):
                v["ts_ms"] = evt.get("ts_ms", now_ms)
                _state["vehicles"][v["id"]] = v
            _state["vehicles"] = {k: v for k, v in _state["vehicles"].items() if v.get("ts_ms", 0) >= cutoff}
            hub.broadcast(evt)
        
        elif evt_type == "pipeline_stats":
            _state["metrics"] = evt
            hub.broadcast(evt)
    
    return {"status": "ok", "count": len(events)}


@app.post("/ingest/tickets")
async def ingest_tickets(
    request: Request,
    x_rs_key: str | None = Header(None),
):
    """
    Ingest city tickets (from Condense Data utility or direct hazard writes).
    Accept shaped ticket OR raw hazard.
    """
    _auth_key(x_rs_key)
    
    raw = await request.body()
    data = orjson.loads(raw)
    tickets = data if isinstance(data, list) else [data]
    
    pool = get_pool()
    
    for t in tickets:
        # Map raw hazard to ticket shape if needed
        if "hazard_type" in t and "issue" not in t:
            t["issue"] = t["hazard_type"]
        if "centroid" in t and "location" not in t:
            t["location"] = t["centroid"]
        
        # Upsert ticket
        hid = t.get("hazard_id")
        if not hid:
            continue
        
        await pool.execute(
            """
            INSERT INTO tickets (hazard_id, issue, severity_label, severity_score, confidence,
                                 distinct_vehicles, lat, lon, department, ticket_type, sla_hours, confirmed_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (hazard_id) DO UPDATE SET
                severity_score = EXCLUDED.severity_score,
                confidence = EXCLUDED.confidence,
                distinct_vehicles = EXCLUDED.distinct_vehicles
            """,
            hid,
            t.get("issue", t.get("hazard_type", "UNKNOWN")),
            t.get("severity_label", "MEDIUM"),
            t.get("severity_score", 0.5),
            t.get("confidence", 0.0),
            t.get("distinct_vehicles", 0),
            t.get("location", t.get("centroid", {})).get("lat", 0.0),
            t.get("location", t.get("centroid", {})).get("lon", 0.0),
            t.get("department", "BBMP Road Infrastructure"),
            t.get("ticket_type", "ROAD_HAZARD_REPAIR"),
            t.get("sla_hours", 24),
            t.get("confirmed_ms", 0),
        )
        
        _state["tickets"][hid] = t
    
    return {"status": "ok", "count": len(tickets)}


# ── Sim Control ───────────────────────────────────────────────────────────────

@app.post("/sim/ground-truth")
async def sim_ground_truth(
    request: Request,
    x_rs_key: str | None = Header(None),
):
    """POST ground truth from simulator."""
    _auth_key(x_rs_key)
    
    raw = await request.body()
    gt = orjson.loads(raw)
    
    pool = get_pool()
    await pool.execute(
        """
        INSERT INTO ground_truth (gt_id, hazard_type, lat, lon, radius_m, severity, created_ms, repaired_ms, is_hazard)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (gt_id) DO UPDATE SET
            repaired_ms = EXCLUDED.repaired_ms
        """,
        gt["gt_id"],
        gt["hazard_type"],
        gt["lat"],
        gt["lon"],
        gt["radius_m"],
        gt["severity"],
        gt["created_ms"],
        gt.get("repaired_ms"),
        gt.get("is_hazard", True),
    )
    
    return {"status": "ok"}


@app.post("/sim/control")
async def sim_control(
    cmd: dict,
    x_rs_key: str | None = Header(None),
):
    """POST control command to Kafka iiitd_sim_control topic."""
    _auth_key(x_rs_key)
    
    if not _producer:
        raise HTTPException(status_code=503, detail="Kafka producer not configured")
    
    topic = f"{cfg.topic_prefix}sim_control"
    _producer.produce(topic, value=orjson.dumps(cmd))
    _producer.poll(0)
    
    return {"status": "ok", "command": cmd.get("cmd")}


# ── Tickets ───────────────────────────────────────────────────────────────────

@app.post("/api/tickets/{hazard_id}/repair")
async def repair_ticket(hazard_id: str):
    """Mark ticket as REPAIR_DISPATCHED and send repair command to simulator."""
    pool = get_pool()
    
    await pool.execute(
        """
        UPDATE tickets SET status = 'REPAIR_DISPATCHED', dispatched_at = NOW()
        WHERE hazard_id = $1
        """,
        hazard_id,
    )
    
    # Send repair command to simulator
    if _producer:
        cmd = {"cmd": "repair", "gt_id": hazard_id}
        topic = f"{cfg.topic_prefix}sim_control"
        _producer.produce(topic, value=orjson.dumps(cmd))
        _producer.poll(0)
    
    return {"status": "ok", "hazard_id": hazard_id}


async def _close_ticket(hazard: dict) -> None:
    """Close ticket when hazard is CLEARED."""
    pool = get_pool()
    hid = hazard.get("hazard_id")
    confirmed_ms = hazard.get("confirmed_ms", 0)
    cleared_ms = hazard.get("cleared_ms", 0)
    
    if confirmed_ms and cleared_ms:
        repair_minutes = (cleared_ms - confirmed_ms) / 60_000.0
    else:
        repair_minutes = None
    
    await pool.execute(
        """
        UPDATE tickets SET status = 'CLOSED', closed_at = NOW(), repair_minutes = $2
        WHERE hazard_id = $1 AND status != 'CLOSED'
        """,
        hid,
        repair_minutes,
    )


# ── HMI Alert ─────────────────────────────────────────────────────────────────

async def _send_hmi_alert(alert: dict) -> None:
    """Send hmi_alert command to simulator when a driver_alert is emitted."""
    if not _producer:
        return
    
    cmd = {
        "cmd": "hmi_alert",
        "vehicle_id": alert.get("vehicle_id"),
        "advisory_speed_kmh": alert.get("advisory_speed_kmh", 20),
        "hazard_lat": _state["hazards"].get(alert.get("hazard_id"), {}).get("lat", 0.0),
        "hazard_lon": _state["hazards"].get(alert.get("hazard_id"), {}).get("lon", 0.0),
    }
    
    topic = f"{cfg.topic_prefix}sim_control"
    _producer.produce(topic, value=orjson.dumps(cmd))
    _producer.poll(0)


async def _store_alert_outcome(outcome: dict) -> None:
    """Store alert outcome in DB."""
    pool = get_pool()
    await pool.execute(
        """
        INSERT INTO alert_outcomes (vehicle_id, hazard_id, speed_at_alert_kmh, speed_at_hazard_kmh, ts_ms)
        VALUES ($1, $2, $3, $4, $5)
        """,
        outcome.get("vehicle_id"),
        outcome.get("hazard_id"),
        outcome.get("speed_at_alert_kmh", 0.0),
        outcome.get("speed_at_hazard_kmh", 0.0),
        int(time.time() * 1000),
    )


# ── API Endpoints ─────────────────────────────────────────────────────────────

@app.get("/api/snapshot")
async def get_snapshot() -> dict:
    """Return current in-memory state."""
    return {
        "vehicles": list(_state["vehicles"].values()),
        "hazards": list(_state["hazards"].values()),
        "reputation": list(_state["reputation"].values()),
        "tickets": list(_state["tickets"].values()),
        "metrics": _state["metrics"],
    }


@app.get("/api/score")
async def get_score() -> dict:
    """Return current accuracy score."""
    score = await compute_score()
    return score


@app.get("/api/metrics")
async def get_metrics() -> dict:
    """Return pipeline metrics."""
    return _state["metrics"]

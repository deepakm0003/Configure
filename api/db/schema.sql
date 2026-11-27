-- RoadSense PostgreSQL schema
-- Idempotent: safe to run multiple times

-- Hazards table (matches Condense PostgreSQL Output connector writes)
CREATE TABLE IF NOT EXISTS hazards (
    hazard_id TEXT PRIMARY KEY,
    hazard_type TEXT NOT NULL,
    status TEXT NOT NULL,
    confidence REAL NOT NULL,
    severity_score REAL NOT NULL,
    severity_label TEXT NOT NULL,
    advisory_speed_kmh INTEGER,
    h3_cell TEXT NOT NULL,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    distinct_vehicles INTEGER NOT NULL DEFAULT 0,
    positive_reports INTEGER NOT NULL DEFAULT 0,
    clean_passes INTEGER NOT NULL DEFAULT 0,
    first_seen_ms BIGINT NOT NULL,
    confirmed_ms BIGINT,
    cleared_ms BIGINT,
    last_update_ms BIGINT NOT NULL,
    version INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hazards_status ON hazards(status);
CREATE INDEX IF NOT EXISTS idx_hazards_type ON hazards(hazard_type);
CREATE INDEX IF NOT EXISTS idx_hazards_updated ON hazards(last_update_ms DESC);

-- City tickets (repair workflow)
CREATE TABLE IF NOT EXISTS tickets (
    hazard_id TEXT PRIMARY KEY,
    issue TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN', -- OPEN | REPAIR_DISPATCHED | CLOSED
    severity_label TEXT NOT NULL,
    severity_score REAL NOT NULL,
    confidence REAL NOT NULL,
    distinct_vehicles INTEGER NOT NULL DEFAULT 0,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    department TEXT,
    ticket_type TEXT DEFAULT 'ROAD_HAZARD_REPAIR',
    sla_hours INTEGER,
    confirmed_ms BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dispatched_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    repair_minutes INTEGER
);

CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets(status);
CREATE INDEX IF NOT EXISTS idx_tickets_severity ON tickets(severity_label, severity_score DESC);

-- Ground truth (from simulator)
CREATE TABLE IF NOT EXISTS ground_truth (
    gt_id TEXT PRIMARY KEY,
    hazard_type TEXT NOT NULL,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    radius_m REAL NOT NULL,
    severity REAL NOT NULL,
    created_ms BIGINT NOT NULL,
    repaired_ms BIGINT,
    is_hazard BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gt_type ON ground_truth(hazard_type);
CREATE INDEX IF NOT EXISTS idx_gt_active ON ground_truth(is_hazard, repaired_ms);

-- Alert outcomes (for scoring)
CREATE TABLE IF NOT EXISTS alert_outcomes (
    id SERIAL PRIMARY KEY,
    vehicle_id TEXT NOT NULL,
    hazard_id TEXT NOT NULL,
    speed_at_alert_kmh REAL NOT NULL,
    speed_at_hazard_kmh REAL NOT NULL,
    ts_ms BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_outcomes_hazard ON alert_outcomes(hazard_id);
CREATE INDEX IF NOT EXISTS idx_alert_outcomes_ts ON alert_outcomes(ts_ms DESC);

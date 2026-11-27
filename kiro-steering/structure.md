---
inclusion: always
---

# Repository structure

```
roadsense/
├── Dockerfile                  # root image for ALL pipeline stages (Condense builds this)
├── requirements.txt            # pipeline deps (Condense detects this)
├── pipeline/
│   ├── main.py                 # entrypoint: reads RS_ROLE and runs that stage
│   ├── rs_common/
│   │   ├── config.py           # env loading + ENV_ALIASES for Condense-injected names
│   │   ├── kafka_io.py         # resilient consumer/producer factory, run_loop(), DLQ helper
│   │   ├── models.py           # pydantic models for every topic contract
│   │   ├── geo.py              # h3 helpers, haversine, bearing, "is ahead" test
│   │   └── metrics.py          # rolling throughput + latency percentiles, periodic log
│   ├── stages/
│   │   ├── normalize.py        # pure logic + runner
│   │   ├── detect.py
│   │   ├── fuse.py
│   │   └── alert.py
│   └── tests/                  # pytest unit tests for the pure algorithms
├── simulator/
│   ├── config.py
│   ├── build_roads.py          # one-time: osmnx → data/roads.geojson (Bengaluru area)
│   ├── vehicles.py             # vehicle kinematics + IMU/CAN signal synthesis
│   ├── hazards.py              # ground-truth hazard registry + effects on signals
│   ├── control.py              # MQTT control-channel handlers (add/repair hazard, chaos)
│   ├── run.py                  # asyncio main: publish rs/telemetry/<vehicle_id>
│   └── data/roads.geojson
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app/
│   │   ├── main.py             # FastAPI app, routers, startup (schema migrate)
│   │   ├── config.py
│   │   ├── ws.py               # WebSocket hub (broadcast, per-client queues)
│   │   ├── ingest.py           # POST /ingest/ui-events, /ingest/tickets (X-RS-Key auth)
│   │   ├── sim_control.py      # POST /sim/* → MQTT rs/control
│   │   ├── scoring.py          # ground truth vs hazards: precision/recall/TTD
│   │   ├── metrics.py          # latency p50/p95, msgs/s
│   │   └── db.py
│   └── db/schema.sql
├── web/                        # React + Vite + TS dashboard
├── docs/
│   ├── architecture.md         # Mermaid diagram + explanation
│   ├── architecture.png
│   ├── condense-setup.md       # exact Condense configuration used
│   └── writeup.md
├── docker-compose.yml          # local: mosquitto + postgres/postgis + api + web (for reproducing)
├── .env.example
├── LICENSE                     # MIT
└── README.md
```

Conventions:
- Topic name constants live only in `rs_common/config.py` (`TOPICS.telemetry_raw`, ...).
- Pure algorithm functions in `stages/*.py` take and return plain models and never touch Kafka. Runners wire them to Kafka.
- Every stage file starts with a docstring explaining its inputs, outputs, state and guarantees (judges read this).

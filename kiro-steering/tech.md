---
inclusion: always
---

# Tech stack and conventions

## Languages and libraries
- **Python 3.11** for the simulator, pipeline transforms and API.
  - Kafka: `confluent-kafka` (librdkafka). Producer: `acks=all`, `enable.idempotence=true`, `linger.ms=20`, `compression.type=lz4`. Consumer: `enable.auto.commit=false`, commit after the output is produced (at-least-once), `auto.offset.reset=latest` (except rs-fuse state rebuild, which reads `hazards.state` from `earliest`).
  - Geo: `h3` **v4 API** (`latlng_to_cell`, `cell_to_latlng`, `grid_disk`, `great_circle_distance`).
  - Validation: `pydantic` v2. JSON: `orjson`.
  - MQTT: `paho-mqtt` 2.x using **MQTT v3.1.1** (`protocol=mqtt.MQTTv311`), TLS on port 8883.
  - API: `fastapi`, `uvicorn[standard]`, `asyncpg`, WebSockets.
  - Simulator: `osmnx` (offline precompute only), `numpy`, `asyncio`.
  - Tests: `pytest`.
- **Frontend:** React 18 + Vite + TypeScript, MapLibre GL, deck.gl (`H3HexagonLayer`, `ScatterplotLayer`), Tailwind CSS, Recharts. Dark "control room" theme.

## Config
- All runtime config comes from env vars, read **only** in `pipeline/rs_common/config.py` (pipeline), `api/app/config.py` (API) and `simulator/config.py`.
- Kafka env names used by our code: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`. `config.py` must also accept the alias names Condense injects into apps; keep a clearly marked `ENV_ALIASES` dict so they can be added in one place.
- Topics are built as `f"{RS_TOPIC_PREFIX}{name}"` with `RS_TOPIC_PREFIX=iiitd_` and underscore names: `telemetry_raw`, `telemetry_clean`, `telemetry_dlq`, `road_events`, `hazards_state`, `vehicles_reputation`, `hazards_critical`, `hazards_minor`, `city_tickets`, `ui_events`. Consumer groups: `iiitd_cg_<role>`. The ACL only allows resources starting with `iiitd_`. Never hard-code the prefix.
- Kafka cluster: `KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT`, `KAFKA_SASL_MECHANISM=SCRAM-SHA-512`. The bootstrap address, username and password come only from env (never commit them).
- `RS_ROLE` ∈ {normalize, detect, fuse, alert} selects the pipeline stage. One Docker image runs all stages.

## Reliability rules
- Every consumer loop: catch `KafkaException`, log, exponential backoff (0.5 s → 30 s), keep running. Never crash on one bad message; send it to the DLQ (normalize) or log and skip (other stages).
- Graceful shutdown on SIGTERM: flush the producer, commit offsets, close the consumer.
- Structured JSON logs to stdout (`ts`, `level`, `role`, `msg`, extra fields). Every 10 s each stage logs throughput (msgs in/out per sec) and processing-latency p50/p95.
- Every message carries `trace.emitted_ms` (set by the simulator) and each stage appends `trace.<role>_ms`, so end-to-end latency can be measured.

## Style
- Type hints everywhere, small pure functions for the algorithms (easy to unit test), with I/O kept in thin `runner` modules.
- No over-engineering: no Kafka Streams, Flink or Faust. Plain consumer/producer loops with in-memory state plus a compacted topic for recovery.

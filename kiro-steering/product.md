---
inclusion: always
---

# Product: RoadSense

RoadSense is a real-time road-hazard intelligence network built for the Configure 2026 mobility hackathon (Condense × Bosch MPS). Connected vehicles act as road sensors. Their GPS, CAN and IMU telemetry streams through a **Condense** pipeline (managed Kafka) that:

1. validates and normalizes telemetry,
2. detects per-vehicle road anomalies (pothole, speed breaker, waterlogging, slippery patch, sudden-braking spot),
3. combines evidence from many vehicles into hazards with a confidence score and a lifecycle (CANDIDATE → CONFIRMED → DECAYING → CLEARED), weighting each vehicle by its reputation,
4. warns vehicles that are approaching confirmed hazards,
5. routes critical hazards to city repair tickets and stores history in Postgres.

## Users
- **Drivers / fleet operators:** get a warning before reaching a hazard ("Pothole in 300 m, slow to 20 km/h").
- **City road-maintenance teams:** get a prioritised, evidence-backed ticket queue with repair-time tracking.
- **Fleet managers:** see less vehicle damage and downtime.

## Hard rules (from the hackathon rules; never break them)
- Ingestion, processing, transformation and routing **must run on Condense**. Never bypass the pipeline in the demo path.
- All data is **synthetic**. No real personal location data. No control of real vehicles.
- Every Kafka topic and consumer group name starts with the team prefix (env `RS_TOPIC_PREFIX`).
- No secrets in code or git. Everything comes from env vars. Provide a `.env.example` with placeholder values only.
- Kafka clients must handle reconnects, retries and broker outages gracefully.
- The project is released under the MIT license. Third-party data/licenses are listed in the README (OpenStreetMap ODbL).

## Differentiators (keep these working, they are what wins)
- A hazard is confirmed only when several distinct vehicles agree, with each report weighted by the vehicle's reputation.
- Vehicles that pass smoothly count as evidence against a hazard, so hazards clear themselves after repair.
- A faulty-sensor vehicle gets quarantined automatically, so it can't create false hazards.
- Live accuracy scoring against the simulator's ground truth (precision, recall, time-to-detect) and end-to-end latency p50/p95.
- Speed breakers and potholes are told apart by the braking pattern before the bump.

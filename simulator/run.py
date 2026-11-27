"""
run.py – Simulator entrypoint.

Usage:
  python -m simulator.run [--vehicles N] [--dry-run]

Flags:
  --vehicles N   Number of vehicles (overrides SIM_NUM_VEHICLES env var).
  --dry-run      Print 5 sample messages and exit.

Transport (SIM_TRANSPORT env):
  kafka (default) – produce directly to iiitd_telemetry_raw keyed by vehicle_id.
  mqtt            – publish to rs/telemetry/<vehicle_id> with paho MQTT v3.1.1.

Control channel:
  kafka mode: consume from iiitd_sim_control.
  mqtt mode:  subscribe to rs/control.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
import time
from pathlib import Path
from typing import Optional

import orjson

from simulator import config as sim_cfg_mod
from simulator.hazards import HazardRegistry
from simulator.vehicles import VehicleFleet, inject_noise

_ROADS_PATH = Path(__file__).parent / "data" / "roads.geojson"
_TICK_S = 1.0


def _load_edges() -> list:
    """Load road edges from GeoJSON."""
    from simulator.vehicles import RoadEdge

    if not _ROADS_PATH.exists():
        print(f"ERROR: {_ROADS_PATH} not found. Run: python -m simulator.build_roads", file=sys.stderr)
        sys.exit(1)

    with open(_ROADS_PATH, encoding="utf-8") as f:
        gj = json.load(f)

    edges = []
    for feat in gj.get("features", []):
        props = feat.get("properties", {})
        geom = feat.get("geometry", {})
        coords = geom.get("coordinates", [])

        if geom.get("type") == "LineString" and len(coords) >= 2:
            lat_u = coords[0][1]
            lon_u = coords[0][0]
            lat_v = coords[-1][1]
            lon_v = coords[-1][0]
            edges.append(RoadEdge(
                u=props.get("u", 0),
                v=props.get("v", 0),
                lat_u=lat_u,
                lon_u=lon_u,
                lat_v=lat_v,
                lon_v=lon_v,
                speed_limit_kmh=float(props.get("speed_limit_kmh", 30)),
                length_m=float(props.get("length_m", 50)),
            ))

    if not edges:
        print("ERROR: No edges loaded from roads.geojson", file=sys.stderr)
        sys.exit(1)

    print(f"[run] Loaded {len(edges)} road edges")
    return edges


def run_dry(cfg, edges: list, n: int = 5) -> None:
    """Print n sample messages and exit."""
    registry = HazardRegistry()
    fleet = VehicleFleet(edges, min(n, 5), registry, rng_seed=99)
    fleet.tick(1.0)
    now_ms = int(time.time() * 1000)
    for v in fleet.vehicles[:n]:
        msg = fleet.build_message(v, now_ms)
        print(json.dumps(msg, indent=2))
    sys.exit(0)


def run_kafka(cfg, edges: list, num_vehicles: int) -> None:
    from confluent_kafka import Producer

    registry = HazardRegistry()

    # Post ground truth to API
    if cfg.api_url and cfg.api_key:
        registry.post_ground_truth(cfg.api_url, cfg.api_key)

    fleet = VehicleFleet(edges, num_vehicles, registry)

    from simulator.control import KafkaControlHandler
    ctrl = KafkaControlHandler(registry, fleet, cfg)
    ctrl.start()

    producer = Producer(cfg.kafka_producer_conf())
    noise_rng = random.Random(42)
    delayed_queue: list[tuple[float, bytes, str]] = []  # (emit_at, payload, vehicle_id)

    print(f"[run] Kafka mode: {num_vehicles} vehicles → {cfg.telemetry_topic}")
    try:
        while True:
            tick_start = time.monotonic()
            now_ms = int(time.time() * 1000)

            fleet.tick(_TICK_S)

            faulty_ids = {v.vehicle_id for v in fleet.vehicles[:ctrl.faulty_count]}

            for v in fleet.vehicles:
                is_faulty = v.vehicle_id in faulty_ids
                msg_dict = fleet.build_message(v, now_ms, is_faulty=is_faulty)
                raw = orjson.dumps(msg_dict)

                noised, is_delayed, is_dup = inject_noise(raw, noise_rng)

                if is_delayed:
                    delayed_queue.append((time.monotonic() + 45, noised, v.vehicle_id))
                    continue

                if is_dup and v._last_duplicate_seq is not None:
                    # Resend the previous seq
                    dup_dict = msg_dict.copy()
                    dup_dict["seq"] = v._last_duplicate_seq
                    noised = orjson.dumps(dup_dict)
                v._last_duplicate_seq = msg_dict["seq"]

                producer.produce(
                    cfg.telemetry_topic,
                    value=noised,
                    key=v.vehicle_id.encode(),
                )

            # Flush delayed messages that are due
            remaining = []
            for emit_at, payload, vid in delayed_queue:
                if time.monotonic() >= emit_at:
                    producer.produce(cfg.telemetry_topic, value=payload, key=vid.encode())
                else:
                    remaining.append((emit_at, payload, vid))
            delayed_queue = remaining

            producer.poll(0)
            producer.flush(0)

            # Sleep until next tick
            elapsed = time.monotonic() - tick_start
            sleep = max(0.0, _TICK_S - elapsed)
            time.sleep(sleep)

    except KeyboardInterrupt:
        pass
    finally:
        ctrl.stop()
        producer.flush(5)
        print("[run] stopped")


def run_mqtt(cfg, edges: list, num_vehicles: int) -> None:
    try:
        import paho.mqtt.client as mqtt
    except ImportError:
        print("ERROR: paho-mqtt not installed.", file=sys.stderr)
        sys.exit(1)

    registry = HazardRegistry()
    if cfg.api_url and cfg.api_key:
        registry.post_ground_truth(cfg.api_url, cfg.api_key)

    fleet = VehicleFleet(edges, num_vehicles, registry)

    from simulator.control import MQTTControlHandler
    ctrl = MQTTControlHandler(registry, fleet, cfg)
    ctrl.start()

    client = mqtt.Client(protocol=mqtt.MQTTv311)
    if cfg.mqtt_username:
        client.username_pw_set(cfg.mqtt_username, cfg.mqtt_password)
    client.tls_set()
    client.connect(cfg.mqtt_host, cfg.mqtt_port)
    client.loop_start()

    noise_rng = random.Random(42)
    print(f"[run] MQTT mode: {num_vehicles} vehicles → rs/telemetry/<id>")

    try:
        while True:
            tick_start = time.monotonic()
            now_ms = int(time.time() * 1000)
            fleet.tick(_TICK_S)
            faulty_ids = {v.vehicle_id for v in fleet.vehicles[:ctrl.faulty_count]}

            for v in fleet.vehicles:
                is_faulty = v.vehicle_id in faulty_ids
                msg_dict = fleet.build_message(v, now_ms, is_faulty=is_faulty)
                raw = orjson.dumps(msg_dict)
                noised, is_delayed, _ = inject_noise(raw, noise_rng)
                if not is_delayed:
                    client.publish(f"rs/telemetry/{v.vehicle_id}", noised, qos=0)

            elapsed = time.monotonic() - tick_start
            time.sleep(max(0.0, _TICK_S - elapsed))

    except KeyboardInterrupt:
        pass
    finally:
        ctrl.stop()
        client.loop_stop()
        client.disconnect()
        print("[run] stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description="RoadSense Simulator")
    parser.add_argument("--vehicles", type=int, default=None, help="Number of vehicles")
    parser.add_argument("--dry-run", action="store_true", help="Print 5 sample messages and exit")
    args = parser.parse_args()

    cfg = sim_cfg_mod.load(num_vehicles=args.vehicles)
    edges = _load_edges()

    if args.dry_run:
        run_dry(cfg, edges)
        return

    if cfg.transport == "mqtt":
        run_mqtt(cfg, edges, cfg.num_vehicles)
    else:
        run_kafka(cfg, edges, cfg.num_vehicles)


if __name__ == "__main__":
    main()

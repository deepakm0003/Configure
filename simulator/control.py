"""
control.py – Control channel handlers.

Reads control commands from:
  - Kafka topic iiitd_sim_control  (when SIM_TRANSPORT=kafka)
  - MQTT topic rs/control          (when SIM_TRANSPORT=mqtt)

Supported commands (data-contracts.md § 10):
  - add_hazard
  - repair
  - rain
  - faulty_vehicle
  - set_vehicle_count
  - hmi_alert
"""

from __future__ import annotations

import json
import threading
import time
from typing import Optional, Callable

from simulator.hazards import HazardRegistry
from simulator.vehicles import VehicleFleet


class ControlHandler:
    """
    Abstract base for control channel handlers.
    Subclassed per transport.
    """

    def __init__(self, registry: HazardRegistry, fleet: VehicleFleet) -> None:
        self._registry = registry
        self._fleet = fleet
        self._faulty_count = 0
        self._target_vehicle_count: Optional[int] = None

    def handle(self, raw: str | bytes | dict) -> None:
        try:
            if isinstance(raw, (bytes, str)):
                cmd = json.loads(raw)
            else:
                cmd = raw
        except json.JSONDecodeError as exc:
            print(f"[control] bad command JSON: {exc}")
            return

        action = cmd.get("cmd", "")

        if action == "add_hazard":
            h = self._registry.add_hazard(
                htype=cmd["hazard_type"],
                lat=cmd["lat"],
                lon=cmd["lon"],
                severity=cmd.get("severity", 0.5),
            )
            print(f"[control] added hazard {h.gt_id}")

        elif action == "repair":
            gt_id = cmd.get("gt_id")
            lat = cmd.get("lat")
            lon = cmd.get("lon")
            h = self._registry.repair(gt_id=gt_id, lat=lat, lon=lon)
            print(f"[control] repaired {h.gt_id if h else 'unknown'}")

        elif action == "rain":
            on = bool(cmd.get("on", False))
            self._registry.set_rain(on)
            print(f"[control] rain {'ON' if on else 'OFF'}")

        elif action == "faulty_vehicle":
            count = int(cmd.get("count", 1))
            self._faulty_count = count
            print(f"[control] faulty vehicles set to {count}")

        elif action == "set_vehicle_count":
            n = int(cmd.get("n", 200))
            self._target_vehicle_count = n
            print(f"[control] target vehicle count set to {n}")

        elif action == "hmi_alert":
            vehicle_id = cmd.get("vehicle_id", "")
            adv_speed = float(cmd.get("advisory_speed_kmh", 20))
            h_lat = float(cmd.get("hazard_lat", 0))
            h_lon = float(cmd.get("hazard_lon", 0))
            for v in self._fleet.vehicles:
                if v.vehicle_id == vehicle_id:
                    v.set_hmi_alert(adv_speed, h_lat, h_lon)
                    break

        else:
            print(f"[control] unknown command: {action!r}")

    @property
    def faulty_count(self) -> int:
        return self._faulty_count

    @property
    def target_vehicle_count(self) -> Optional[int]:
        return self._target_vehicle_count


class KafkaControlHandler(ControlHandler):
    """Read control commands from the Kafka sim_control topic."""

    def __init__(self, registry: HazardRegistry, fleet: VehicleFleet, cfg) -> None:
        super().__init__(registry, fleet)
        self._cfg = cfg
        self._thread: Optional[threading.Thread] = None
        self._stop = False

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True, name="control-kafka")
        self._thread.start()

    def stop(self) -> None:
        self._stop = True

    def _run(self) -> None:
        from confluent_kafka import Consumer, KafkaError
        consumer = Consumer(self._cfg.kafka_consumer_conf("iiitd_cg_sim_control"))
        consumer.subscribe([self._cfg.sim_control_topic])
        try:
            while not self._stop:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[control] Kafka error: {msg.error()}")
                    continue
                self.handle(msg.value())
        finally:
            consumer.close()


class MQTTControlHandler(ControlHandler):
    """Read control commands from the MQTT rs/control topic."""

    def __init__(self, registry: HazardRegistry, fleet: VehicleFleet, cfg) -> None:
        super().__init__(registry, fleet)
        self._cfg = cfg
        self._client = None

    def start(self) -> None:
        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            print("[control] paho-mqtt not installed; control channel disabled")
            return

        client = mqtt.Client(protocol=mqtt.MQTTv311)
        if self._cfg.mqtt_username:
            client.username_pw_set(self._cfg.mqtt_username, self._cfg.mqtt_password)
        client.tls_set()
        client.on_message = self._on_message
        client.connect(self._cfg.mqtt_host, self._cfg.mqtt_port)
        client.subscribe("rs/control")
        self._client = client
        threading.Thread(target=client.loop_forever, daemon=True, name="control-mqtt").start()

    def _on_message(self, _client, _userdata, message) -> None:
        self.handle(message.payload)

    def stop(self) -> None:
        if self._client:
            self._client.disconnect()

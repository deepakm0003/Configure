"""
config.py – Simulator configuration from env vars.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


def _get(name: str, default: str) -> str:
    return os.getenv(name, default)


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise EnvironmentError(f"Required env var {name!r} is not set.")
    return val


@dataclass(frozen=True)
class SimConfig:
    # Transport
    transport: str              # "kafka" | "mqtt"

    # Kafka (used when transport == "kafka")
    kafka_bootstrap_servers: str
    kafka_security_protocol: str
    kafka_sasl_mechanism: str
    kafka_sasl_username: str
    kafka_sasl_password: str
    topic_prefix: str
    telemetry_topic: str        # = prefix + "telemetry_raw"
    sim_control_topic: str      # = prefix + "sim_control"

    # MQTT (used when transport == "mqtt")
    mqtt_host: str
    mqtt_port: int
    mqtt_username: str
    mqtt_password: str

    # Sim parameters
    num_vehicles: int
    demo_time_scale: float

    # API for ground-truth upload (optional)
    api_url: Optional[str]
    api_key: Optional[str]

    def kafka_producer_conf(self) -> dict:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "security.protocol": self.kafka_security_protocol,
            "sasl.mechanism": self.kafka_sasl_mechanism,
            "sasl.username": self.kafka_sasl_username,
            "sasl.password": self.kafka_sasl_password,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 20,
            "compression.type": "lz4",
        }

    def kafka_consumer_conf(self, group_id: str) -> dict:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "security.protocol": self.kafka_security_protocol,
            "sasl.mechanism": self.kafka_sasl_mechanism,
            "sasl.username": self.kafka_sasl_username,
            "sasl.password": self.kafka_sasl_password,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }


def load(num_vehicles: Optional[int] = None) -> SimConfig:
    transport = _get("SIM_TRANSPORT", "kafka")
    prefix = _get("RS_TOPIC_PREFIX", "iiitd_")

    # Kafka credentials are always needed (even mqtt mode may need Kafka control)
    try:
        bs = _require("KAFKA_BOOTSTRAP_SERVERS")
        ku = _require("KAFKA_SASL_USERNAME")
        kp = _require("KAFKA_SASL_PASSWORD")
    except EnvironmentError:
        bs = ""
        ku = ""
        kp = ""

    n_vehicles = num_vehicles if num_vehicles is not None else int(_get("SIM_NUM_VEHICLES", "200"))

    return SimConfig(
        transport=transport,
        kafka_bootstrap_servers=bs,
        kafka_security_protocol=_get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
        kafka_sasl_mechanism=_get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
        kafka_sasl_username=ku,
        kafka_sasl_password=kp,
        topic_prefix=prefix,
        telemetry_topic=f"{prefix}telemetry_raw",
        sim_control_topic=f"{prefix}sim_control",
        mqtt_host=_get("MQTT_HOST", "localhost"),
        mqtt_port=int(_get("MQTT_PORT", "8883")),
        mqtt_username=_get("MQTT_USERNAME", ""),
        mqtt_password=_get("MQTT_PASSWORD", ""),
        num_vehicles=n_vehicles,
        demo_time_scale=float(_get("RS_DEMO_TIME_SCALE", "10")),
        api_url=os.getenv("API_URL"),
        api_key=os.getenv("API_KEY"),
    )

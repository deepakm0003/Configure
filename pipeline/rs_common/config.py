"""
config.py – centralised env-var loading for every pipeline stage.

All runtime configuration is read here and only here.
ENV_ALIASES maps Condense-injected variable names to our canonical names;
add entries in the TODO block below when Condense docs are available.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


# ── ENV_ALIASES ───────────────────────────────────────────────────────────────
# Maps alternative variable names (e.g. Condense-injected) → our canonical name.
# TODO: add Condense alias names here once the Condense environment is known.
# Example:
#   "CONDENSE_KAFKA_BROKERS": "KAFKA_BOOTSTRAP_SERVERS",
#   "CONDENSE_KAFKA_USER":    "KAFKA_SASL_USERNAME",
ENV_ALIASES: dict[str, str] = {
    # "CONDENSE_KAFKA_BROKERS": "KAFKA_BOOTSTRAP_SERVERS",
}


def _resolve(name: str) -> Optional[str]:
    """Return env var value, checking aliases first."""
    for alias, canonical in ENV_ALIASES.items():
        if canonical == name:
            val = os.getenv(alias)
            if val:
                return val
    return os.getenv(name)


def _require(name: str) -> str:
    val = _resolve(name)
    if not val:
        raise EnvironmentError(f"Required env var {name!r} is not set.")
    return val


def _get(name: str, default: str) -> str:
    val = _resolve(name)
    return val if val else default


# ── Topics ────────────────────────────────────────────────────────────────────
# Built as RS_TOPIC_PREFIX + underscore_name.
# Never hard-code the prefix; always use this dataclass.

@dataclass(frozen=True)
class _Topics:
    prefix: str
    # Input / output topic overrides from env
    _input_override: Optional[str]
    _output_override: Optional[str]

    def _t(self, name: str) -> str:
        return f"{self.prefix}{name}"

    # Raw names (suffix after prefix)
    @property
    def telemetry_raw(self) -> str:
        return self._t("telemetry_raw")

    @property
    def telemetry_clean(self) -> str:
        return self._t("telemetry_clean")

    @property
    def telemetry_dlq(self) -> str:
        return self._t("telemetry_dlq")

    @property
    def road_events(self) -> str:
        return self._t("road_events")

    @property
    def hazards_state(self) -> str:
        return self._t("hazards_state")

    @property
    def vehicles_reputation(self) -> str:
        return self._t("vehicles_reputation")

    @property
    def hazards_critical(self) -> str:
        return self._t("hazards_critical")

    @property
    def hazards_minor(self) -> str:
        return self._t("hazards_minor")

    @property
    def city_tickets(self) -> str:
        return self._t("city_tickets")

    @property
    def ui_events(self) -> str:
        return self._t("ui_events")

    @property
    def sim_control(self) -> str:
        return self._t("sim_control")

    def stage_input(self, default: str) -> str:
        return self._input_override or default

    def stage_output(self, default: str) -> str:
        return self._output_override or default


@dataclass(frozen=True)
class Config:
    # Kafka
    bootstrap_servers: str
    security_protocol: str
    sasl_mechanism: str
    sasl_username: str
    sasl_password: str

    # Role
    role: str  # normalize | detect | fuse | alert

    # Topics
    topics: _Topics

    # Algorithm parameters
    max_lateness_s: float
    z_threshold: float
    confirm_min_vehicles: int
    alert_radius_m: float
    demo_time_scale: float

    # Geographic bounding box
    bbox_lat_min: float
    bbox_lat_max: float
    bbox_lon_min: float
    bbox_lon_max: float

    def consumer_group(self) -> str:
        return f"{self.topics.prefix}cg_{self.role}"

    def kafka_producer_conf(self) -> dict:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "sasl.mechanism": self.sasl_mechanism,
            "sasl.username": self.sasl_username,
            "sasl.password": self.sasl_password,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 20,
            "compression.type": "lz4",
        }

    def kafka_consumer_conf(self, group_id: Optional[str] = None, offset_reset: str = "latest") -> dict:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "sasl.mechanism": self.sasl_mechanism,
            "sasl.username": self.sasl_username,
            "sasl.password": self.sasl_password,
            "group.id": group_id or self.consumer_group(),
            "enable.auto.commit": False,
            "auto.offset.reset": offset_reset,
        }


def load() -> Config:
    prefix = _get("RS_TOPIC_PREFIX", "iiitd_")
    input_override = _resolve("RS_INPUT_TOPIC") or None
    output_override = _resolve("RS_OUTPUT_TOPIC") or None

    return Config(
        bootstrap_servers=_require("KAFKA_BOOTSTRAP_SERVERS"),
        security_protocol=_get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
        sasl_mechanism=_get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
        sasl_username=_require("KAFKA_SASL_USERNAME"),
        sasl_password=_require("KAFKA_SASL_PASSWORD"),
        role=_get("RS_ROLE", "normalize"),
        topics=_Topics(
            prefix=prefix,
            _input_override=input_override,
            _output_override=output_override,
        ),
        max_lateness_s=float(_get("RS_MAX_LATENESS_S", "60")),
        z_threshold=float(_get("RS_Z_THRESHOLD", "4.0")),
        confirm_min_vehicles=int(_get("RS_CONFIRM_MIN_VEHICLES", "3")),
        alert_radius_m=float(_get("RS_ALERT_RADIUS_M", "500")),
        demo_time_scale=float(_get("RS_DEMO_TIME_SCALE", "10")),
        bbox_lat_min=float(_get("RS_BBOX_LAT_MIN", "12.80")),
        bbox_lat_max=float(_get("RS_BBOX_LAT_MAX", "13.10")),
        bbox_lon_min=float(_get("RS_BBOX_LON_MIN", "77.45")),
        bbox_lon_max=float(_get("RS_BBOX_LON_MAX", "77.80")),
    )

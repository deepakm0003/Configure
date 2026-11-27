"""
config.py – API configuration from env vars.
"""

import os
from dataclasses import dataclass
from typing import Optional


def _get(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise EnvironmentError(f"Required env var {name!r} is not set.")
    return val


@dataclass(frozen=True)
class APIConfig:
    # Server
    port: int
    host: str
    
    # Database
    database_url: str
    
    # API security
    api_key: str
    
    # Kafka (for sim control commands)
    kafka_bootstrap_servers: str
    kafka_security_protocol: str
    kafka_sasl_mechanism: str
    kafka_sasl_username: str
    kafka_sasl_password: str
    topic_prefix: str
    
    # CORS
    cors_origins: list[str]
    
    # WebSocket
    ws_heartbeat_interval: float = 15.0
    ws_queue_max_size: int = 1000
    
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


def load() -> APIConfig:
    cors = _get("CORS_ORIGINS", "*")
    origins = [o.strip() for o in cors.split(",")]
    
    return APIConfig(
        port=int(_get("PORT", "8000")),
        host=_get("HOST", "0.0.0.0"),
        database_url=_require("DATABASE_URL"),
        api_key=_require("RS_API_KEY"),
        kafka_bootstrap_servers=_get("KAFKA_BOOTSTRAP_SERVERS", ""),
        kafka_security_protocol=_get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
        kafka_sasl_mechanism=_get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
        kafka_sasl_username=_get("KAFKA_SASL_USERNAME", ""),
        kafka_sasl_password=_get("KAFKA_SASL_PASSWORD", ""),
        topic_prefix=_get("RS_TOPIC_PREFIX", "iiitd_"),
        cors_origins=origins,
    )

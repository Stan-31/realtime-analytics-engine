"""Env-driven settings for the consumer process."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # Kafka
    bootstrap_servers: str
    ticks_topic: str
    dlq_topic: str
    consumer_group: str

    # Aggregation
    window_seconds: float
    tick_interval_seconds: float

    # DB
    pg_dsn: str
    db_batch_size: int
    db_batch_interval_ms: int

    # WebSocket
    ws_host: str
    ws_port: int
    ws_send_timeout_seconds: float

    @classmethod
    def from_env(cls) -> "Settings":
        user = os.getenv("POSTGRES_USER", "analytics")
        pw = os.getenv("POSTGRES_PASSWORD", "analytics")
        host = os.getenv("POSTGRES_HOST", "timescaledb")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB", "analytics")
        dsn = f"postgresql://{user}:{pw}@{host}:{port}/{db}"
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            ticks_topic=os.getenv("TICKS_TOPIC", "market.ticks"),
            dlq_topic=os.getenv("DLQ_TOPIC", "market.ticks.dlq"),
            consumer_group=os.getenv("CONSUMER_GROUP", "analytics-consumer"),
            window_seconds=float(os.getenv("WINDOW_SECONDS", "10")),
            tick_interval_seconds=float(os.getenv("TICK_INTERVAL_SECONDS", "1.0")),
            pg_dsn=dsn,
            db_batch_size=int(os.getenv("DB_BATCH_SIZE", "200")),
            db_batch_interval_ms=int(os.getenv("DB_BATCH_INTERVAL_MS", "500")),
            ws_host=os.getenv("WS_HOST", "0.0.0.0"),
            ws_port=int(os.getenv("WS_PORT", "8765")),
            ws_send_timeout_seconds=float(os.getenv("WS_SEND_TIMEOUT_SECONDS", "1.0")),
        )

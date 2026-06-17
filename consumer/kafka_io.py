"""Kafka ingest with DLQ-on-validation-failure.

`consume_loop` reads `market.ticks`, validates each message against the `Tick`
schema, and routes valid ticks into the aggregator. Invalid messages are
forwarded raw to the DLQ topic with the failure reason in a header so the
operator can inspect them later — the loop itself never raises.
"""

from __future__ import annotations

import asyncio
import logging

import orjson
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from pydantic import ValidationError

from aggregator import Aggregator
from config import Settings
from schemas import Tick

log = logging.getLogger("consumer.kafka")


async def _connect_consumer(settings: Settings) -> AIOKafkaConsumer:
    backoff = 1.0
    while True:
        consumer = AIOKafkaConsumer(
            settings.ticks_topic,
            bootstrap_servers=settings.bootstrap_servers,
            group_id=settings.consumer_group,
            enable_auto_commit=True,
            auto_offset_reset="latest",
            max_poll_records=500,
            fetch_max_wait_ms=50,
        )
        try:
            await consumer.start()
            log.info("consumer subscribed to %s", settings.ticks_topic)
            return consumer
        except KafkaConnectionError as exc:
            log.warning("kafka not ready (%s); retrying in %.1fs", exc, backoff)
            await consumer.stop()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 10.0)


async def _connect_dlq_producer(settings: Settings) -> AIOKafkaProducer:
    backoff = 1.0
    while True:
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.bootstrap_servers,
            acks=1,
            linger_ms=50,
            compression_type="lz4",
        )
        try:
            await producer.start()
            log.info("dlq producer connected (topic=%s)", settings.dlq_topic)
            return producer
        except KafkaConnectionError as exc:
            log.warning("dlq producer not ready (%s); retrying in %.1fs", exc, backoff)
            await producer.stop()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 10.0)


def _decode(raw: bytes) -> dict:
    return orjson.loads(raw)


async def _send_to_dlq(
    producer: AIOKafkaProducer,
    settings: Settings,
    key: bytes | None,
    payload: bytes,
    reason: str,
) -> None:
    headers = [("error", reason.encode("utf-8", errors="replace")[:512])]
    try:
        await producer.send(settings.dlq_topic, key=key, value=payload, headers=headers)
    except Exception:  # noqa: BLE001
        # DLQ failure should not take the loop down.
        log.exception("failed to publish to DLQ topic=%s", settings.dlq_topic)


async def consume_loop(
    settings: Settings,
    aggregator: Aggregator,
    stop: asyncio.Event,
) -> None:
    consumer = await _connect_consumer(settings)
    dlq = await _connect_dlq_producer(settings)
    total_ok = 0
    total_bad = 0
    try:
        async for msg in consumer:
            if stop.is_set():
                break
            payload = msg.value
            key = msg.key
            try:
                data = _decode(payload)
            except Exception as exc:  # noqa: BLE001
                total_bad += 1
                await _send_to_dlq(dlq, settings, key, payload, f"decode_error: {exc!r}")
                continue
            try:
                tick = Tick.model_validate(data)
            except ValidationError as exc:
                total_bad += 1
                await _send_to_dlq(dlq, settings, key, payload, f"validation_error: {exc.errors()}")
                continue
            aggregator.add(tick.symbol, tick.price, tick.ts)
            total_ok += 1
            if total_ok % 50_000 == 0:
                log.info("ingest progress ok=%d bad=%d", total_ok, total_bad)
    finally:
        log.info("ingest stopping: ok=%d bad=%d", total_ok, total_bad)
        await consumer.stop()
        await dlq.stop()

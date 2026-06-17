"""Async market-data producer.

Emits simulated tick events for a configurable basket of symbols using a
geometric Brownian motion (GBM) price walk. Rate-limited via a token-bucket
sleep loop, keyed by symbol so partitions stay sticky downstream, and
deliberately injects a small fraction of malformed messages so the consumer's
DLQ path is observable end-to-end.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import random
import signal
import sys
import time
from dataclasses import dataclass

import orjson
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

try:
    import uvloop  # type: ignore[import-not-found]

    uvloop.install()
except ImportError:  # pragma: no cover - Windows / fallback
    pass


# --- configuration ----------------------------------------------------------


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    topic: str
    events_per_second: int
    symbols: tuple[str, ...]
    malformed_rate: float
    log_interval_seconds: float

    @classmethod
    def from_env(cls) -> "Settings":
        symbols_raw = os.getenv("SYMBOLS", "AAPL,MSFT,GOOG,AMZN,NVDA")
        symbols = tuple(s.strip().upper() for s in symbols_raw.split(",") if s.strip())
        if not symbols:
            raise ValueError("SYMBOLS must list at least one symbol")
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            topic=os.getenv("TICKS_TOPIC", "market.ticks"),
            events_per_second=int(os.getenv("EVENTS_PER_SECOND", "2500")),
            symbols=symbols,
            malformed_rate=float(os.getenv("MALFORMED_RATE", "0.001")),
            log_interval_seconds=float(os.getenv("PRODUCER_LOG_INTERVAL_SECONDS", "5")),
        )


SEED_PRICES: dict[str, float] = {
    "AAPL": 185.0,
    "MSFT": 425.0,
    "GOOG": 175.0,
    "AMZN": 195.0,
    "NVDA": 1180.0,
}


# --- GBM price walk ---------------------------------------------------------


class GBMWalker:
    """Per-symbol geometric Brownian motion price generator.

    Parameters are picked so the series wiggles in a way that looks plausible
    on a 60-second rolling chart without ever blowing up to absurd values.
    """

    # mu (drift) ~ 0; sigma chosen so 1s steps move ~0.05% on average.
    DRIFT = 0.0
    VOL = 0.0008
    DT = 1.0  # seconds-equivalent per step

    def __init__(self, symbol: str, seed_price: float, rng: random.Random) -> None:
        self.symbol = symbol
        self.price = seed_price
        self._rng = rng

    def step(self) -> float:
        z = self._rng.gauss(0.0, 1.0)
        # exact GBM update: S_t = S_{t-1} * exp((mu - 0.5 sigma^2) dt + sigma sqrt(dt) z)
        drift = (self.DRIFT - 0.5 * self.VOL * self.VOL) * self.DT
        diffusion = self.VOL * math.sqrt(self.DT) * z
        self.price = max(0.01, self.price * math.exp(drift + diffusion))
        return self.price


# --- producer loop ----------------------------------------------------------


def _build_payload(symbol: str, price: float, ts_ns: int) -> bytes:
    return orjson.dumps(
        {
            "symbol": symbol,
            "price": round(price, 4),
            "ts": ts_ns / 1_000_000_000,
        }
    )


def _build_malformed_payload(symbol: str) -> bytes:
    # Two failure shapes: missing field, and bad type. Alternate them.
    shape = random.choice(("missing_price", "bad_type", "not_json"))
    if shape == "missing_price":
        return orjson.dumps({"symbol": symbol, "ts": time.time()})
    if shape == "bad_type":
        return orjson.dumps({"symbol": symbol, "price": "not-a-number", "ts": time.time()})
    return b"<<not json at all>>"


async def _send_loop(
    producer: AIOKafkaProducer,
    settings: Settings,
    walkers: list[GBMWalker],
    stop: asyncio.Event,
) -> None:
    log = logging.getLogger("producer")
    # Token-bucket pacing in 100ms slices. At 2500 eps the slice is 250 events,
    # which keeps the loop responsive to SIGTERM without burning CPU.
    slice_seconds = 0.1
    target_per_slice = max(1, int(round(settings.events_per_second * slice_seconds)))

    sent_window = 0
    window_start = time.monotonic()

    while not stop.is_set():
        slice_started = time.monotonic()
        for _ in range(target_per_slice):
            walker = walkers[random.randrange(len(walkers))]
            symbol = walker.symbol
            price = walker.step()
            if random.random() < settings.malformed_rate:
                payload = _build_malformed_payload(symbol)
            else:
                payload = _build_payload(symbol, price, time.time_ns())
            # fire-and-forget send_and_wait would block per-message; just send.
            await producer.send(settings.topic, key=symbol.encode("ascii"), value=payload)
            sent_window += 1

        # Pacing: sleep whatever's left of this 100ms slice.
        elapsed = time.monotonic() - slice_started
        sleep_for = slice_seconds - elapsed
        if sleep_for > 0:
            try:
                await asyncio.wait_for(stop.wait(), timeout=sleep_for)
            except asyncio.TimeoutError:
                pass

        # Periodic throughput log.
        if time.monotonic() - window_start >= settings.log_interval_seconds:
            window_seconds = time.monotonic() - window_start
            rate = sent_window / window_seconds if window_seconds > 0 else 0.0
            log.info(
                "emitted %d events in last %.1fs (%.0f eps target=%d)",
                sent_window,
                window_seconds,
                rate,
                settings.events_per_second,
            )
            sent_window = 0
            window_start = time.monotonic()


async def _connect_producer(settings: Settings) -> AIOKafkaProducer:
    log = logging.getLogger("producer")
    # Retry until Kafka is reachable — compose healthcheck should usually beat
    # us here but on cold boot we sometimes race.
    backoff = 1.0
    while True:
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.bootstrap_servers,
            linger_ms=20,
            acks=1,
            compression_type="lz4",
            max_batch_size=64 * 1024,
            request_timeout_ms=15000,
        )
        try:
            await producer.start()
            log.info("connected to kafka at %s", settings.bootstrap_servers)
            return producer
        except KafkaConnectionError as exc:
            log.warning("kafka not ready (%s); retrying in %.1fs", exc, backoff)
            await producer.stop()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 10.0)


async def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    log = logging.getLogger("producer")
    settings = Settings.from_env()
    log.info(
        "starting producer: target=%d eps symbols=%s topic=%s malformed=%.3f%%",
        settings.events_per_second,
        ",".join(settings.symbols),
        settings.topic,
        settings.malformed_rate * 100,
    )

    rng = random.Random()
    walkers = [
        GBMWalker(symbol, SEED_PRICES.get(symbol, 100.0), rng) for symbol in settings.symbols
    ]

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - Windows fallback
            pass

    producer = await _connect_producer(settings)
    try:
        await _send_loop(producer, settings, walkers, stop)
    finally:
        log.info("stopping producer")
        await producer.stop()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

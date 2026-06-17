"""Composition root for the consumer process.

Phase 6 wiring: kafka ingest → aggregator → tick loop → batched DB writer.
WebSocket fan-out (phase 7) joins next.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
import time

try:
    import uvloop  # type: ignore[import-not-found]

    uvloop.install()
except ImportError:  # pragma: no cover - Windows / fallback
    pass

from aggregator import Aggregate, Aggregator
from config import Settings
from db_writer import db_writer_loop
from kafka_io import consume_loop

log = logging.getLogger("consumer.main")


async def _tick_loop(
    settings: Settings,
    aggregator: Aggregator,
    db_queue: asyncio.Queue[list[Aggregate]],
    stop: asyncio.Event,
) -> None:
    """Emit a snapshot every `tick_interval_seconds` and fan it out."""
    interval = settings.tick_interval_seconds
    next_tick = time.monotonic() + interval
    last_log = time.monotonic()
    while not stop.is_set():
        sleep_for = max(0.0, next_tick - time.monotonic())
        try:
            await asyncio.wait_for(stop.wait(), timeout=sleep_for)
            break
        except asyncio.TimeoutError:
            pass
        next_tick += interval
        snapshot = aggregator.snapshot()
        if not snapshot:
            continue
        # Hand off to the DB writer. Queue is unbounded by design — a single
        # snapshot is at most ~len(symbols) rows so the memory ceiling is tiny.
        db_queue.put_nowait(snapshot)
        # Throttle the human-readable log line to once per ~5s so the
        # container logs stay readable at 1Hz.
        if time.monotonic() - last_log >= 5.0:
            preview = ", ".join(
                f"{a.symbol}={a.avg_price:.2f}(n={a.sample_count})" for a in snapshot
            )
            log.info("snapshot %s", preview)
            last_log = time.monotonic()


def _supervise(task: asyncio.Task, stop: asyncio.Event) -> None:
    """If any core task dies, set the stop event so the process exits cleanly."""

    def _cb(t: asyncio.Task) -> None:
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            log.exception("task %s crashed; signalling shutdown", t.get_name(), exc_info=exc)
            stop.set()

    task.add_done_callback(_cb)


async def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    settings = Settings.from_env()
    log.info(
        "starting consumer: kafka=%s topic=%s window=%.1fs tick=%.2fs",
        settings.bootstrap_servers,
        settings.ticks_topic,
        settings.window_seconds,
        settings.tick_interval_seconds,
    )

    aggregator = Aggregator(window_seconds=settings.window_seconds)
    db_queue: asyncio.Queue[list[Aggregate]] = asyncio.Queue()
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - Windows fallback
            pass

    ingest = asyncio.create_task(consume_loop(settings, aggregator, stop), name="ingest")
    tick = asyncio.create_task(
        _tick_loop(settings, aggregator, db_queue, stop), name="tick"
    )
    db = asyncio.create_task(db_writer_loop(settings, db_queue, stop), name="db")
    tasks = (ingest, tick, db)
    for t in tasks:
        _supervise(t, stop)

    await stop.wait()
    log.info("shutdown requested; cancelling tasks")
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    log.info("consumer stopped")
    crashed = any(t.done() and not t.cancelled() and t.exception() for t in tasks)
    return 1 if crashed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

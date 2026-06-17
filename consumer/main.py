"""Composition root for the consumer process.

Phase 5 wiring: kafka ingest → aggregator → log-only tick loop. Subsequent
phases add the batched DB writer (phase 6) and the WebSocket fan-out (phase 7).
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

from aggregator import Aggregator
from config import Settings
from kafka_io import consume_loop

log = logging.getLogger("consumer.main")


async def _tick_loop(
    settings: Settings,
    aggregator: Aggregator,
    stop: asyncio.Event,
) -> None:
    """Emit a snapshot every `tick_interval_seconds`.

    For phase 5 this is log-only so the pipeline is observable end-to-end
    before the DB writer and WS hub land.
    """
    interval = settings.tick_interval_seconds
    next_tick = time.monotonic() + interval
    while not stop.is_set():
        sleep_for = max(0.0, next_tick - time.monotonic())
        try:
            await asyncio.wait_for(stop.wait(), timeout=sleep_for)
            break
        except asyncio.TimeoutError:
            pass
        next_tick += interval
        snapshot = aggregator.snapshot()
        if snapshot:
            preview = ", ".join(f"{a.symbol}={a.avg_price:.2f}(n={a.sample_count})" for a in snapshot)
            log.info("snapshot %s", preview)


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
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - Windows fallback
            pass

    ingest = asyncio.create_task(consume_loop(settings, aggregator, stop), name="ingest")
    tick = asyncio.create_task(_tick_loop(settings, aggregator, stop), name="tick")
    for t in (ingest, tick):
        _supervise(t, stop)

    await stop.wait()
    log.info("shutdown requested; cancelling tasks")
    for t in (ingest, tick):
        t.cancel()
    await asyncio.gather(ingest, tick, return_exceptions=True)
    log.info("consumer stopped")
    # Non-zero if a task crashed (and so set the stop event itself).
    crashed = any(t.done() and not t.cancelled() and t.exception() for t in (ingest, tick))
    return 1 if crashed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

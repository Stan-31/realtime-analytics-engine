"""Batched TimescaleDB writer.

Drains aggregate snapshots from an asyncio.Queue and flushes via
`asyncpg.Connection.copy_records_to_table` whenever batch size or time
threshold trips first. COPY is materially faster than executemany at the
row counts this pipeline produces.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging

import asyncpg

from aggregator import Aggregate
from config import Settings

log = logging.getLogger("consumer.db")

# `aggregates` columns, in declared order. Used by COPY.
_COLUMNS = ("ts", "symbol", "avg_price", "sample_count")


async def _connect_pool(settings: Settings) -> asyncpg.Pool:
    backoff = 1.0
    while True:
        try:
            pool = await asyncpg.create_pool(
                dsn=settings.pg_dsn,
                min_size=1,
                max_size=4,
                command_timeout=15,
            )
            # Validate the connection up front so a misconfigured DSN fails
            # loudly rather than waiting until first flush.
            async with pool.acquire() as conn:
                await conn.execute("SELECT 1")
            log.info("postgres pool ready (%s)", _redact_dsn(settings.pg_dsn))
            return pool
        except Exception as exc:  # noqa: BLE001
            log.warning("postgres not ready (%s); retrying in %.1fs", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 10.0)


def _redact_dsn(dsn: str) -> str:
    # postgresql://user:pw@host:port/db -> postgresql://user:***@host:port/db
    try:
        head, tail = dsn.split("://", 1)
        creds, rest = tail.split("@", 1)
        if ":" in creds:
            user = creds.split(":", 1)[0]
            return f"{head}://{user}:***@{rest}"
    except ValueError:
        pass
    return dsn


def _to_record(agg: Aggregate) -> tuple:
    return (
        dt.datetime.fromtimestamp(agg.ts, tz=dt.timezone.utc),
        agg.symbol,
        float(agg.avg_price),
        int(agg.sample_count),
    )


async def _flush(pool: asyncpg.Pool, batch: list[tuple]) -> None:
    if not batch:
        return
    async with pool.acquire() as conn:
        # ON CONFLICT semantics: the snapshot tick is at 1Hz wallclock so
        # collisions on (ts, symbol) are vanishingly rare, but if the consumer
        # restarts within the same second we'd hit the PK. COPY into a temp
        # staging table, then INSERT ... ON CONFLICT DO UPDATE, gives both
        # throughput and idempotency.
        async with conn.transaction():
            await conn.execute(
                "CREATE TEMP TABLE IF NOT EXISTS _agg_staging "
                "(LIKE aggregates INCLUDING DEFAULTS) ON COMMIT DROP"
            )
            await conn.copy_records_to_table(
                "_agg_staging", records=batch, columns=_COLUMNS
            )
            await conn.execute(
                """
                INSERT INTO aggregates (ts, symbol, avg_price, sample_count)
                SELECT ts, symbol, avg_price, sample_count FROM _agg_staging
                ON CONFLICT (ts, symbol) DO UPDATE
                  SET avg_price    = EXCLUDED.avg_price,
                      sample_count = EXCLUDED.sample_count
                """
            )


async def db_writer_loop(
    settings: Settings,
    queue: asyncio.Queue[list[Aggregate]],
    stop: asyncio.Event,
) -> None:
    pool = await _connect_pool(settings)
    batch: list[tuple] = []
    last_flush = asyncio.get_event_loop().time()
    flush_interval = settings.db_batch_interval_ms / 1000.0
    flushed_rows = 0
    flushes = 0

    try:
        while not stop.is_set():
            timeout = max(0.0, flush_interval - (asyncio.get_event_loop().time() - last_flush))
            try:
                snapshot = await asyncio.wait_for(queue.get(), timeout=timeout)
                batch.extend(_to_record(a) for a in snapshot)
                queue.task_done()
            except asyncio.TimeoutError:
                pass

            now = asyncio.get_event_loop().time()
            size_trip = len(batch) >= settings.db_batch_size
            time_trip = batch and (now - last_flush) * 1000.0 >= settings.db_batch_interval_ms
            if size_trip or time_trip:
                try:
                    await _flush(pool, batch)
                    flushed_rows += len(batch)
                    flushes += 1
                    if flushes % 50 == 0:
                        log.info("db flushed rows=%d flushes=%d", flushed_rows, flushes)
                except Exception:  # noqa: BLE001
                    log.exception("flush failed; dropping batch of %d rows", len(batch))
                finally:
                    batch.clear()
                    last_flush = now
    finally:
        if batch:
            log.info("draining final batch of %d rows on shutdown", len(batch))
            try:
                await _flush(pool, batch)
            except Exception:  # noqa: BLE001
                log.exception("final flush failed")
        await pool.close()
        log.info("db writer stopped: rows=%d flushes=%d", flushed_rows, flushes)

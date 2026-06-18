# Architecture

## Service topology

```
┌──────────────┐    ┌─────────────┐    ┌──────────────────┐    ┌──────────────┐
│  producer    │───▶│   Kafka     │───▶│    consumer      │───▶│ TimescaleDB  │
│  (Python     │    │ (KRaft mode,│    │   (Python +      │    │ (hypertable) │
│   asyncio)   │    │  no ZK)     │    │    asyncio)      │    └──────────────┘
└──────────────┘    └─────────────┘    │                  │
                          ▲            │  ┌────────────┐  │    ┌──────────────┐
                          │            │  │ sliding    │  │    │   frontend   │
                          │            │  │ window agg │  │───▶│  (React +    │
                    ┌─────┴────┐       │  └────────────┘  │ WS │   Nginx +    │
                    │  DLQ     │◀──────│   DLQ producer   │    │   Recharts)  │
                    │ topic    │       └──────────────────┘    └──────────────┘
                    └──────────┘
```

All five services run on a single user-defined bridge network
(`analytics-net`); only `frontend` (`:8080`), `kafka` (`:9092`) and
`timescaledb` (`:5432`) publish ports to the host. The WebSocket hub on
`consumer:8765` is reachable only via nginx's `/ws` reverse proxy.

## Data contract

### `market.ticks` (Kafka topic, 8 partitions)

| Field    | Type      | Notes                                              |
| -------- | --------- | -------------------------------------------------- |
| `symbol` | string    | 1–16 chars; partition key                          |
| `price`  | float > 0 | quote in the symbol's native currency              |
| `ts`     | float > 0 | epoch seconds (fractional), event-time              |

Encoded with `orjson`. Producer key is the symbol bytes so all ticks for
one symbol land on the same partition and preserve order downstream.

### `market.ticks.dlq` (Kafka topic, default partitions)

Same key/value as the original message; failure reason is in the
`error` header (≤ 512 bytes UTF-8). Two failure shapes are intentionally
emitted by the producer (`MALFORMED_RATE`, default 0.1 %):

- decode failure (`<<not json at all>>`)
- validation failure (missing `price`, or `price` of wrong type)

### `aggregates` (Postgres / TimescaleDB hypertable)

```sql
CREATE TABLE aggregates (
    ts           TIMESTAMPTZ      NOT NULL,
    symbol       TEXT             NOT NULL,
    avg_price    DOUBLE PRECISION NOT NULL,
    sample_count INTEGER          NOT NULL,
    PRIMARY KEY (ts, symbol)
);
SELECT create_hypertable('aggregates', 'ts', chunk_time_interval => INTERVAL '1 hour');
CREATE INDEX aggregates_symbol_ts ON aggregates (symbol, ts DESC);
```

Writes use `COPY ... FROM STDIN` into a `TEMP` staging table followed by
`INSERT ... ON CONFLICT (ts, symbol) DO UPDATE`. The temp table is
dropped at commit so the path is fully idempotent across consumer
restarts (re-emitted snapshots overwrite the prior row rather than
crashing on the PK).

### WebSocket frame (`/ws`)

```json
{
  "type": "snapshot",
  "items": [
    { "ts": 1718700000.123, "symbol": "AAPL", "avg_price": 185.41, "sample_count": 312 }
  ]
}
```

Plus a one-shot `{"type":"hello"}` on connect so the dashboard's loading
state can clear without waiting for the next tick.

## Consumer task graph

```
              ┌──────────────────────────────┐
   kafka ───▶ │ ingest (consume_loop)        │ ──▶ aggregator.add(...)
              │  • pydantic validate         │
              │  • DLQ on failure            │
              └──────────────────────────────┘

              ┌──────────────────────────────┐     db_queue
              │ tick (1 Hz)                  │ ──▶ ┌──────┐
              │  • aggregator.snapshot()     │     │      │ ──▶ db_writer ──▶ TimescaleDB
              │  • hub.broadcast(snapshot)   │     └──────┘
              └──────────────────────────────┘ ──▶ WebSocketHub ──▶ frontend

              ┌──────────────────────────────┐
              │ ws (websockets.serve)        │ ──▶ register/unregister clients
              └──────────────────────────────┘
```

All four tasks share a single `asyncio.Event` (`stop`). Any unhandled
task exception is logged and sets the event so the whole process exits
non-zero; Docker's `restart: unless-stopped` brings it back.

## Sliding-window aggregation

For each symbol the consumer keeps a `collections.deque[(ts, price)]`.

- `add(symbol, price, ts)` appends; no eviction in the hot path.
- `snapshot(now)` evicts entries older than `now - WINDOW_SECONDS`, then
  computes the mean price and sample count for each non-empty deque.
- Symbols with **zero samples** in the window are **skipped** rather
  than emitted as `null`, so the frontend chart never receives gaps it
  would have to render specially.

This is single-asyncio-thread / lock-free by design. At 2.5k events/s
across 5 symbols the worst-case deque length is ~5,000 entries
(`WINDOW_SECONDS * EVENTS_PER_SECOND / len(SYMBOLS)`); the per-tick
linear scan is well under 1 ms.

## Throughput notes

- Producer: 100 ms slice = 250 events/slice at 2,500 eps; pacing is a
  `wait_for(stop.wait(), timeout=…)` so SIGTERM aborts within one slice.
- Consumer ingest: `fetch_max_wait_ms=50`, `max_poll_records=500` — at
  steady state we batch ~125 records per poll loop, well clear of CPU
  saturation.
- DB writer: dual flush triggers (`DB_BATCH_SIZE=200` rows OR
  `DB_BATCH_INTERVAL_MS=500`). Even with 5 symbols the snapshot loop
  produces only ~5 rows/s, so the time trigger dominates in practice
  and the size trigger is the headroom for larger baskets.
- WS hub: every send is `asyncio.wait_for(ws.send(...), timeout=1.0s)`.
  Timeouts or `ConnectionClosed` evict the client; the broadcast loop
  cannot stall on a slow consumer.

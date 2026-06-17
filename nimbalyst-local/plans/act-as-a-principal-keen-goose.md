# Real-Time Analytics Engine — Implementation Plan

## Context

The repo is a near-empty scaffold (`README.md` stub + `nimbalyst-local/`). The goal is to deliver a production-shaped, *locally-runnable* end-to-end pipeline that demonstrates the canonical streaming-analytics pattern: **simulated market data → Kafka → stream processor (sliding-window aggregation + DLQ + batched persistence + live fanout) → TimescaleDB + browser dashboard**. Everything runs via a single `docker compose up`.

This is a portfolio/reference build, so the design priorities are:

1. **Correctness & legibility over micro-optimization** — every container has a single, clear responsibility.
2. **Realistic throughput** — sustain ≥ 2,000 events/sec from one producer instance without backpressure stalls.
3. **Operational hygiene** — health checks, graceful shutdown, structured logs, DLQ, idempotent schema init, `.env`-driven config.
4. **Tracked progress** — incremental git commits, one per phase, so the repo history reads as a tutorial.

---

## Target architecture

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

**Why these choices (principal-engineer rationale):**

- **Kafka in KRaft mode** (`bitnami/kafka:3.7`) — no Zookeeper container, simpler topology, modern default.
- **Python consumer with `aiokafka` + `asyncpg` + `websockets`** — 2k msg/s is comfortably inside asyncio's envelope; single-language stack reduces cognitive load. Go is a fine alternative if you want a perf flex; flagged as a question below.
- **TimescaleDB** via `timescale/timescaledb:latest-pg16` — hypertable on `ts` column gives O(log n) inserts + compression-ready.
- **Recharts** for the dashboard — declarative React-idiomatic API, lighter cognitive load than Chart.js for streaming data.
- **Vite + Nginx multi-stage build** — production-grade static asset serving with WebSocket reverse-proxy to the consumer.

---

## Repository layout

```
.
├── README.md                       # phase-by-phase quickstart & troubleshooting
├── .gitignore
├── .env.example                    # all tunables (broker, topics, batch sizes, DB creds)
├── docker-compose.yml              # 5 services: kafka, timescaledb, producer, consumer, frontend
├── docs/
│   └── architecture.md             # diagrams + data contracts
├── timescaledb/
│   └── init/
│       └── 01_schema.sql           # idempotent: CREATE EXTENSION, tables, hypertable, indexes
├── producer/
│   ├── Dockerfile                  # python:3.12-slim, non-root
│   ├── requirements.txt            # aiokafka, orjson, uvloop
│   └── producer.py                 # asyncio loop, 5 symbols, GBM price walk, rate-limited
├── consumer/
│   ├── Dockerfile                  # python:3.12-slim, non-root
│   ├── requirements.txt            # aiokafka, asyncpg, websockets, orjson, pydantic, uvloop
│   ├── main.py                     # composition root: wires tasks, signal handlers
│   ├── config.py                   # env-driven settings
│   ├── schemas.py                  # pydantic models for validation (malformed → DLQ)
│   ├── aggregator.py               # per-symbol deque-based sliding window (10s, tick=1s)
│   ├── kafka_io.py                 # consumer + DLQ producer
│   ├── db_writer.py                # asyncpg COPY-based batch insert
│   └── ws_hub.py                   # WebSocket fan-out with backpressure handling
└── frontend/
    ├── Dockerfile                  # multi-stage: node build → nginx serve
    ├── nginx.conf                  # gzip, /ws proxy_pass with Upgrade headers
    ├── package.json                # react, recharts, vite
    ├── vite.config.js
    ├── index.html
    └── src/
        ├── main.jsx
        ├── App.jsx
        ├── hooks/useLiveStream.js  # WS client with auto-reconnect + ring buffer
        └── components/
            └── LiveChart.jsx       # Recharts LineChart, 60s rolling viewport
```

---

## Component specs

### 1. Producer (`producer/producer.py`)
- Asyncio loop emits **tick events** for 5 symbols (AAPL, MSFT, GOOG, AMZN, NVDA).
- Geometric Brownian Motion price walk per symbol → realistic-looking series.
- Rate-limited to **2,500 events/sec aggregate** via a token-bucket sleep loop; configurable via `EVENTS_PER_SECOND` env var.
- Sends to topic `market.ticks` keyed by symbol (so partitions are sticky per symbol → preserves order downstream).
- Serializes with `orjson` for speed.
- Injects ~0.1% deliberately malformed messages (missing field / bad type) so the DLQ path is observable.

### 2. Kafka (`docker-compose.yml` service)
- `bitnami/kafka:3.7` in KRaft single-node mode.
- Topics auto-created on first publish: `market.ticks` (8 partitions), `market.ticks.dlq` (1 partition).
- Healthcheck: `kafka-topics.sh --bootstrap-server localhost:9092 --list`.
- Exposes 9092 on the host for debugging.

### 3. TimescaleDB (`timescaledb/init/01_schema.sql`)
```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE TABLE IF NOT EXISTS aggregates (
    ts          TIMESTAMPTZ      NOT NULL,
    symbol      TEXT             NOT NULL,
    avg_price   DOUBLE PRECISION NOT NULL,
    sample_count INTEGER         NOT NULL,
    PRIMARY KEY (ts, symbol)
);
SELECT create_hypertable('aggregates', 'ts', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 hour');
CREATE INDEX IF NOT EXISTS aggregates_symbol_ts ON aggregates (symbol, ts DESC);
```
Mounted into `/docker-entrypoint-initdb.d/` so it runs once on first start.

### 4. Consumer — the heart of the system
Composition pattern (`main.py`): start three concurrent `asyncio.Task`s sharing a single `Aggregator` instance.

- **Task A — `consume_loop`**: `aiokafka.AIOKafkaConsumer` reads `market.ticks`, validates with pydantic. Valid → `aggregator.add(tick)`. Invalid → re-publish raw payload to `market.ticks.dlq` with error reason in headers; never crashes the loop.
- **Task B — `tick_loop`** (1 Hz): every second, calls `aggregator.snapshot()` which returns the 10-second moving average per symbol. Snapshot is fanned out to **(a)** an internal `asyncio.Queue` for the DB writer, and **(b)** the WebSocket hub.
- **Task C — `db_writer_loop`**: drains the DB queue and flushes whenever batch ≥ 200 rows **or** ≥ 500 ms elapsed (whichever first), using `asyncpg`'s `copy_records_to_table` for high-throughput inserts.
- **WebSocket server**: `websockets.serve(...)` on `:8765`, registers clients into a set, broadcasts each snapshot. Slow clients are dropped (non-blocking `send` with `asyncio.wait_for` timeout) to protect the hot path.

**Sliding window algorithm** (`aggregator.py`):
- Per-symbol `collections.deque[(ts, price)]`.
- On `add`: append, then pop-left while `now - head_ts > 10s`.
- `snapshot()`: average of the deque's `price` values per symbol → list of records.
- Pure in-memory, lock-free (single asyncio thread).

Graceful shutdown: SIGTERM handler cancels tasks, flushes pending DB batch, closes Kafka clients.

### 5. Frontend (`frontend/`)
- Vite-built React SPA.
- `useLiveStream` hook opens `ws://<host>/ws` (proxied by Nginx → `consumer:8765`), exponential-backoff reconnect, keeps a rolling 60-second buffer keyed by symbol.
- `LiveChart` renders one `<LineChart>` per symbol with Recharts, animated transitions disabled (smooth streaming look without jank).
- Nginx config: serve `/`, proxy `/ws` with `Upgrade: websocket` headers, set `proxy_read_timeout 3600s`.

### 6. `docker-compose.yml`
- Single network `analytics-net`.
- Volumes: `timescale-data`, `kafka-data`.
- Service dependencies via `depends_on` + healthchecks so the consumer waits for Kafka and DB to be ready.
- All ports configurable via `.env`. Defaults: TimescaleDB 5432, Kafka 9092, WebSocket 8765, Frontend 8080.

---

## Files I will create (representative; full set listed in *Repository layout*)

Critical to get right:
- `consumer/aggregator.py` — sliding-window correctness, especially edge cases around the 1-second tick when no data has arrived for a symbol (emit `null` vs skip → I'll **skip** symbols with zero samples in the window to avoid polluting the chart).
- `consumer/db_writer.py` — must use `asyncpg.Connection.copy_records_to_table`, not `executemany`, to hit insert throughput.
- `consumer/main.py` — task supervision: if any task dies, the whole process exits non-zero so Docker restarts it.
- `frontend/nginx.conf` — the `/ws` `Upgrade` headers are the #1 thing people get wrong; I'll get this right on first pass.

---

## Git commit plan (one commit per phase)

| # | Commit message | Adds |
| --- | --- | --- |
| 1 | `chore: repo scaffold, .gitignore, .env.example` | gitignore, env template, README skeleton |
| 2 | `infra: docker-compose with kafka (KRaft) + timescaledb` | compose file, healthchecks, volumes |
| 3 | `db: hypertable schema + idempotent init script` | `timescaledb/init/01_schema.sql` |
| 4 | `feat(producer): async GBM market-data producer` | producer module + Dockerfile |
| 5 | `feat(consumer): kafka ingest + sliding-window aggregator + DLQ` | aggregator, kafka_io, schemas |
| 6 | `feat(consumer): batched asyncpg writer for TimescaleDB` | db_writer + wiring |
| 7 | `feat(consumer): websocket broadcast hub` | ws_hub + wiring |
| 8 | `feat(frontend): react + recharts dashboard, nginx ws proxy` | frontend + Dockerfile + nginx.conf |
| 9 | `docs: end-to-end README with run/verify/teardown` | README, docs/architecture.md |

If the directory is not already a git repo, phase 0 is `git init && git branch -M main`. **I will not push to GitHub or create a remote without explicit confirmation** — local commits only by default.

---

## Verification plan

After `docker compose up -d --build`, the following checks confirm the pipeline end-to-end:

1. **Kafka is reachable & topics exist:**
   `docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list` → shows `market.ticks` and (after a minute) `market.ticks.dlq`.
2. **Producer is hitting target rate:** `docker compose logs --tail=20 producer` → log line `emitted N events in last second` should show ≥ 2000.
3. **DLQ is receiving the synthetic malformed messages:**
   `docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic market.ticks.dlq --from-beginning --max-messages 5`.
4. **TimescaleDB has aggregates:**
   `docker compose exec timescaledb psql -U analytics -d analytics -c "SELECT symbol, COUNT(*), MAX(ts) FROM aggregates GROUP BY symbol;"` → 5 rows, counts climbing.
5. **WebSocket frames arrive:**
   Open `http://localhost:8080` in a browser — 5 line charts should animate smoothly within ~2 seconds of page load. (I'll spot-check this via the chromeflow MCP if you want a screenshot in the final report.)
6. **Backpressure / restart resilience:**
   `docker compose restart consumer` — frontend reconnects automatically, charts resume without page refresh.

---

## Decisions locked in

- **Consumer language:** Python (`aiokafka` + `asyncpg` + `websockets`).
- **Frontend chart library:** Recharts.
- **GitHub:** create a new **public** repo under the authenticated user (`stanleysullivan316@gmail.com` account) via the GitHub MCP, add it as `origin`, and push `main` after each phase commit. Repo name: `realtime-analytics-engine` (will check for collisions and append a suffix if taken).

## Git/remote workflow

After `git init` (phase 0), the remote-creation flow is:

1. `mcp__github__create_repository` — name `realtime-analytics-engine`, public, no auto-init (we already have local commits).
2. `git remote add origin <html_url>.git`
3. After **each** phase commit: `git push -u origin main` (first push uses `-u`; subsequent are plain `git push`). This means every checkpoint is visible on GitHub as soon as it's made — not just at the end.

I'll stop and confirm with you if the repo name is taken or the GitHub MCP returns a permissions error, rather than silently picking a different name.

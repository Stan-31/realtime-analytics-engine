# Real-Time Analytics Engine

End-to-end streaming pipeline that runs entirely on your laptop:

```
producer → Kafka (KRaft) → asyncio consumer → TimescaleDB + browser dashboard
                                  └─▶ DLQ topic for malformed messages
```

Single `docker compose up --build` brings up five containers on one network
(Kafka, TimescaleDB, producer, consumer, frontend) and produces a live chart
at <http://localhost:8080>.

See [`docs/architecture.md`](docs/architecture.md) for the data contract,
sequence diagrams, and design rationale.

---

## Stack

| Component | Tech                                                   |
| --------- | ------------------------------------------------------ |
| Broker    | Kafka 3.7 (KRaft, no Zookeeper)                        |
| Storage   | TimescaleDB on PostgreSQL 16 (hypertable on `ts`)      |
| Producer  | Python asyncio + `aiokafka`, GBM price walk            |
| Consumer  | Python asyncio + `aiokafka` / `asyncpg` / `websockets` |
| Frontend  | React + Vite + Recharts, Nginx static + WS proxy       |

## Layout

```
docker-compose.yml      # 5 services on one network
.env.example            # all tunables
producer/               # market-tick producer
consumer/               # ingest + aggregate + persist + broadcast
timescaledb/init/       # idempotent schema bootstrap
frontend/               # Vite SPA + nginx WS proxy
docs/                   # architecture notes
```

---

## Quickstart

Requirements: Docker Desktop ≥ 4.30 (or Docker Engine 26+) with the Compose v2
plugin. ~2 GB free RAM for the stack.

```bash
cp .env.example .env             # tweak ports / throughput if you like
docker compose up -d --build     # first build takes ~2-3 minutes
```

Once the stack is up:

| What                  | Where                                 |
| --------------------- | ------------------------------------- |
| Dashboard             | <http://localhost:8080>               |
| TimescaleDB (psql)    | `localhost:5432`, user/db `analytics` |
| Kafka (host clients)  | `localhost:9092`                      |
| Consumer WS (direct)  | `ws://localhost:8080/ws`              |

Tear down:

```bash
docker compose down              # stop + remove containers
docker compose down -v           # also wipe kafka-data + timescale-data volumes
```

---

## Verification

After `docker compose up -d --build`, these checks confirm the pipeline
end-to-end.

**1. Topics exist**

```bash
docker compose exec kafka \
  /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Expect `market.ticks` immediately and `market.ticks.dlq` within ~30 s
(after the producer has emitted its first malformed message).

**2. Producer is hitting target rate**

```bash
docker compose logs --tail=20 producer
```

Look for `emitted N events in last 5.0s` — `N / 5` should be ≥ 2000 by
default.

**3. DLQ receives the synthetic malformed messages**

```bash
docker compose exec kafka \
  /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic market.ticks.dlq --from-beginning --max-messages 5 \
  --property print.headers=true
```

**4. TimescaleDB has aggregates climbing**

```bash
docker compose exec timescaledb psql -U analytics -d analytics \
  -c "SELECT symbol, COUNT(*), MAX(ts) FROM aggregates GROUP BY symbol;"
```

You should see one row per configured symbol with counts that grow each
time you run the query.

**5. WebSocket frames arrive in the browser**

Open <http://localhost:8080>. Within ~2 seconds the "Waiting for the first
snapshot…" placeholder should clear and one chart per symbol should start
sliding left-to-right at ~1 Hz.

**6. Restart resilience**

```bash
docker compose restart consumer
```

Charts pause briefly and resume — the frontend's `useLiveStream` hook
reconnects with exponential backoff (capped at 5 s).

---

## Configuration

Every knob lives in `.env` and is read by exactly one service. Key ones:

| Variable                  | Default                  | Effect                                   |
| ------------------------- | ------------------------ | ---------------------------------------- |
| `EVENTS_PER_SECOND`       | `2500`                   | Producer token-bucket target rate        |
| `SYMBOLS`                 | `AAPL,MSFT,GOOG,AMZN,NVDA` | Producer symbol basket                  |
| `MALFORMED_RATE`          | `0.001`                  | Fraction of producer messages sent bad   |
| `WINDOW_SECONDS`          | `10`                     | Aggregator sliding-window length         |
| `TICK_INTERVAL_SECONDS`   | `1.0`                    | Snapshot emission cadence                |
| `DB_BATCH_SIZE`           | `200`                    | Rows before a DB flush is forced         |
| `DB_BATCH_INTERVAL_MS`    | `500`                    | Time before a partial batch flushes      |
| `WS_PORT`                 | `8765`                   | Consumer WS hub port (internal)          |
| `FRONTEND_EXTERNAL_PORT`  | `8080`                   | Host port nginx exposes the SPA on       |

---

## Troubleshooting

**Frontend shows "reconnecting…" forever.** The consumer crashed; check
`docker compose logs --tail=80 consumer`. Most common cause: Postgres or
Kafka not healthy yet — the consumer has retry loops with exponential
backoff but a misconfigured DSN won't ever succeed.

**Browser tab connects but charts never draw.** Open DevTools → Network →
WS and look for the `/ws` socket. If it shows status 502, nginx couldn't
reach `consumer:8765` — verify with
`docker compose exec frontend wget -qO- http://consumer:8765 || true`
(the WS handshake will fail without an Upgrade header but the TCP connect
should succeed).

**Aggregates table empty.** Either the consumer never connected to
Postgres (see logs) or the snapshot loop is yielding empty results — the
latter happens when no valid ticks have landed yet. Run check **2** above
to confirm the producer is actually publishing.

**Kafka container restarts on boot.** Volume left over from an earlier
KRaft format with a different node id. `docker compose down -v` clears
the volume and re-formats fresh on the next `up`.

**Want to scale up consumers?** They share the `analytics-consumer`
consumer group, so running two replicas partitions the 8-partition
`market.ticks` topic across both. Each replica also exposes its own
WebSocket hub, so the nginx upstream would need to round-robin if you
truly horizontally scaled the consumer; out of scope for the default
single-instance build.

---

## Repo workflow

Each commit on `main` corresponds to one phase of
[`nimbalyst-local/plans/act-as-a-principal-keen-goose.md`](nimbalyst-local/plans/act-as-a-principal-keen-goose.md).
The history reads top-to-bottom as a tutorial of how the system was
assembled. `git log --oneline` is the shortest tour.

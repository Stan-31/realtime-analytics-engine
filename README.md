# Real-Time Analytics Engine

End-to-end streaming pipeline: simulated market ticks → Kafka → asyncio
consumer (sliding-window aggregation + DLQ + batched persistence + live
fanout) → TimescaleDB + browser dashboard. Single `docker compose up`.

> Phase 1 scaffold. Full quickstart, architecture, and verification steps land
> in phase 9. See `nimbalyst-local/plans/act-as-a-principal-keen-goose.md`
> for the implementation plan.

## Stack at a glance

| Component | Tech |
| --- | --- |
| Broker | Kafka 3.7 (KRaft, no Zookeeper) |
| Storage | TimescaleDB on PostgreSQL 16 (hypertable on `ts`) |
| Producer | Python asyncio + `aiokafka`, GBM price walk |
| Consumer | Python asyncio + `aiokafka` / `asyncpg` / `websockets` |
| Frontend | React + Vite + Recharts, Nginx static + WS proxy |

## Layout

```
docker-compose.yml      # 5 services on one network
.env.example            # all tunables
producer/               # market-tick producer
consumer/               # ingest + aggregate + persist + broadcast
timescaledb/init/       # idempotent schema bootstrap
frontend/               # Vite SPA + Nginx WS proxy
docs/                   # architecture notes
```

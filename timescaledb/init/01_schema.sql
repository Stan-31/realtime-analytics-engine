-- ---------------------------------------------------------------------------
-- Real-Time Analytics Engine — schema bootstrap.
-- Mounted into /docker-entrypoint-initdb.d so postgres runs it once on first
-- start of an empty data volume. Every statement is idempotent so re-running
-- against an existing volume is a no-op.
-- ---------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS aggregates (
    ts           TIMESTAMPTZ      NOT NULL,
    symbol       TEXT             NOT NULL,
    avg_price    DOUBLE PRECISION NOT NULL,
    sample_count INTEGER          NOT NULL,
    PRIMARY KEY (ts, symbol)
);

SELECT create_hypertable(
    'aggregates',
    'ts',
    if_not_exists       => TRUE,
    chunk_time_interval => INTERVAL '1 hour'
);

CREATE INDEX IF NOT EXISTS aggregates_symbol_ts
    ON aggregates (symbol, ts DESC);

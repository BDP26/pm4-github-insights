-- ════════════════════════════════════════════════════════════════
--  GitHub Events — TimescaleDB Schema
--  Run once on first startup (mounted as docker-entrypoint-initdb.d)
-- ════════════════════════════════════════════════════════════════

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ── Raw events hypertable ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS events (
    time            TIMESTAMPTZ        NOT NULL,
    event_id        TEXT               NOT NULL,
    event_type      TEXT               NOT NULL,
    actor           TEXT               NOT NULL,
    repo            TEXT               NOT NULL,
    detail          TEXT,

    -- Geographic enrichment
    location        TEXT,
    country         TEXT,
    country_code    CHAR(2),
    timezone        TEXT,
    lat             DOUBLE PRECISION,
    lng             DOUBLE PRECISION,

    -- GitHub profile enrichment
    company         TEXT,
    public_repos    INT,

    -- Raw JSON payload (for future reprocessing)
    payload         JSONB,

    PRIMARY KEY (time, event_id)
);

-- Convert to hypertable, partitioned by time (1 day chunks)
SELECT create_hypertable(
    'events', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

-- ── Indexes ──────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_events_type         ON events (event_type, time DESC);
CREATE INDEX IF NOT EXISTS idx_events_country      ON events (country, time DESC);
CREATE INDEX IF NOT EXISTS idx_events_actor        ON events (actor, time DESC);
CREATE INDEX IF NOT EXISTS idx_events_repo         ON events (repo, time DESC);
CREATE INDEX IF NOT EXISTS idx_events_payload_gin  ON events USING GIN (payload);

-- ── Compression (after 7 days, chunks are compressed ~10-20x) ───
ALTER TABLE events SET (
    timescaledb.compress,
    timescaledb.compress_segmentby  = 'event_type, country_code',
    timescaledb.compress_orderby    = 'time DESC'
);
SELECT add_compression_policy('events', INTERVAL '7 days', if_not_exists => TRUE);

-- ── Retention policy (drop data older than 90 days) ──────────────
SELECT add_retention_policy('events', INTERVAL '90 days', if_not_exists => TRUE);

-- ════════════════════════════════════════════════════════════════
--  Continuous Aggregates  (pre-computed rollups, auto-refreshed)
-- ════════════════════════════════════════════════════════════════

-- 1. Event-type counts per 5-minute bucket
CREATE MATERIALIZED VIEW IF NOT EXISTS event_stats_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time) AS bucket,
    event_type,
    count(*)                       AS event_count
FROM events
GROUP BY bucket, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'event_stats_5m',
    start_offset => INTERVAL '1 hour',
    end_offset   => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists     => TRUE
);

-- 2. Country-level counts per 5-minute bucket
CREATE MATERIALIZED VIEW IF NOT EXISTS country_stats_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time) AS bucket,
    country,
    country_code,
    count(*)                       AS event_count,
    count(DISTINCT actor)          AS unique_actors
FROM events
GROUP BY bucket, country, country_code
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'country_stats_5m',
    start_offset => INTERVAL '1 hour',
    end_offset   => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists     => TRUE
);

-- 3. Per-hour actor leaderboard
CREATE MATERIALIZED VIEW IF NOT EXISTS actor_stats_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    actor,
    country,
    company,
    count(*)                    AS event_count,
    count(DISTINCT repo)        AS repos_touched
FROM events
GROUP BY bucket, actor, country, company
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'actor_stats_1h',
    start_offset => INTERVAL '3 hours',   -- must cover >= 2 x 1h buckets
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists     => TRUE
);

-- ════════════════════════════════════════════════════════════════
--  Handy views for Grafana queries
-- ════════════════════════════════════════════════════════════════

-- Live event rate (last 30 min, per minute)
CREATE OR REPLACE VIEW v_event_rate_1m AS
SELECT
    time_bucket('1 minute', time) AS minute,
    event_type,
    count(*) AS events
FROM events
WHERE time > now() - INTERVAL '30 minutes'
GROUP BY minute, event_type
ORDER BY minute DESC;

-- Top countries last 24 h
CREATE OR REPLACE VIEW v_top_countries_24h AS
SELECT
    country,
    country_code,
    count(*)          AS events,
    count(DISTINCT actor) AS actors
FROM events
WHERE time > now() - INTERVAL '24 hours'
  AND country IS NOT NULL
GROUP BY country, country_code
ORDER BY events DESC
LIMIT 30;

-- Top repos last 24 h
CREATE OR REPLACE VIEW v_top_repos_24h AS
SELECT
    repo,
    count(*)              AS events,
    count(DISTINCT actor) AS actors
FROM events
WHERE time > now() - INTERVAL '24 hours'
GROUP BY repo
ORDER BY events DESC
LIMIT 30;

-- Recent raw events (Grafana "logs" panel)
CREATE OR REPLACE VIEW v_recent_events AS
SELECT
    time, event_type, actor, repo, detail,
    country, timezone, company, public_repos
FROM events
ORDER BY time DESC
LIMIT 200;

-- Grafana read-only user
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') THEN
    CREATE ROLE grafana_reader LOGIN PASSWORD 'grafana_readonly';
    GRANT CONNECT ON DATABASE github_events TO grafana_reader;
    GRANT USAGE ON SCHEMA public TO grafana_reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT ON TABLES TO grafana_reader;
  END IF;
END
$$;
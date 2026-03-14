-- ════════════════════════════════════════════════════════════════
--  GitHub Events — TimescaleDB Star Schema (Normalized)
-- ════════════════════════════════════════════════════════════════

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ── 1. Dimension: Users ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    username        TEXT PRIMARY KEY,
    fetched_at      TIMESTAMPTZ,
    company         TEXT,
    location        TEXT,
    country         TEXT,
    country_code    CHAR(2),
    lat             DOUBLE PRECISION,
    lng             DOUBLE PRECISION,
    public_repos    INTEGER,
    followers       INTEGER,
    last_active     TIMESTAMPTZ
);

-- ── 2. Dimension: Organizations ──────────────────────────────────
CREATE TABLE IF NOT EXISTS organizations (
    login           TEXT PRIMARY KEY,
    fetched_at      TIMESTAMPTZ,
    name            TEXT,
    description     TEXT,
    location        TEXT,
    lat             DOUBLE PRECISION,
    lng             DOUBLE PRECISION,
    is_verified     BOOLEAN DEFAULT FALSE,
    public_repos    INTEGER,
    created_at      TIMESTAMPTZ
);

-- ── 3. Junction: Organization Members ────────────────────────────
CREATE TABLE IF NOT EXISTS organization_members (
    org_login       TEXT REFERENCES organizations(login),
    user_username   TEXT REFERENCES users(username),
    role            TEXT,
    PRIMARY KEY (org_login, user_username)
);

-- ── 4. Dimension: Repositories ───────────────────────────────────
CREATE TABLE IF NOT EXISTS repos (
    repo_id             INTEGER PRIMARY KEY,
    fetched_at          TIMESTAMPTZ,
    name                TEXT NOT NULL,
    full_name           TEXT NOT NULL,
    owner_login         TEXT NOT NULL, -- Kann User oder Org sein
    owner_type          TEXT,          -- 'User' oder 'Organization'
    description         TEXT,
    language            TEXT,
    license_spdx        TEXT,
    topics              TEXT[],        -- PostgreSQL Array für Tags
    stargazers_count    INTEGER DEFAULT 0,
    forks_count         INTEGER DEFAULT 0,
    size                INTEGER,
    created_at          TIMESTAMPTZ,
    pushed_at           TIMESTAMPTZ
);

-- ── 5. Fact Table: Events (Hypertable) ───────────────────────────
CREATE TABLE IF NOT EXISTS events (
    time            TIMESTAMPTZ NOT NULL,
    event_id        TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    actor_username  TEXT NOT NULL, -- FK zu users
    repo_id         INTEGER NOT NULL, -- FK zu repos
    detail          TEXT,
    payload         JSONB,
    PRIMARY KEY (time, event_id)
);

-- In Hypertable umwandeln (1 day chunks)
SELECT create_hypertable(
    'events', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

-- ── Indexes für Joins ────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_events_actor ON events (actor_username, time DESC);
CREATE INDEX IF NOT EXISTS idx_events_repo  ON events (repo_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_repos_owner  ON repos (owner_login);
CREATE INDEX IF NOT EXISTS idx_repos_lang   ON repos (language);

-- ── Compression & Retention ──────────────────────────────────────
ALTER TABLE events SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'event_type',
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('events', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('events', INTERVAL '90 days', if_not_exists => TRUE);

-- ════════════════════════════════════════════════════════════════
--  Continuous Aggregates (angepasst an neues Schema)
-- ════════════════════════════════════════════════════════════════

-- 1. Event-Typen per 5m
CREATE MATERIALIZED VIEW IF NOT EXISTS event_stats_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time) AS bucket,
    event_type,
    count(*)                       AS event_count
FROM events
GROUP BY bucket, event_type;

-- 2. Länder-Statistik (JOIN mit Users nötig)
-- Hinweis: Continuous Aggs unterstützen oft keine komplexen Joins direkt.
-- Daher aggregieren wir hier nur IDs und nutzen eine View für den Namen.
CREATE MATERIALIZED VIEW IF NOT EXISTS country_ids_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', e.time) AS bucket,
    u.country_code,
    count(*) AS event_count
FROM events e
JOIN users u ON e.actor_username = u.username
GROUP BY bucket, u.country_code;

-- ════════════════════════════════════════════════════════════════
--  Handy Views für Grafana (mit Joins)
-- ════════════════════════════════════════════════════════════════

-- Top Repos der letzten 24h (Join Fact -> Dimension)
CREATE OR REPLACE VIEW v_top_repos_24h AS
SELECT
    r.full_name,
    r.language,
    count(e.event_id) AS events,
    count(DISTINCT e.actor_username) AS unique_users
FROM events e
JOIN repos r ON e.repo_id = r.repo_id
WHERE e.time > now() - INTERVAL '24 hours'
GROUP BY r.full_name, r.language
ORDER BY events DESC;

-- Live Map View
CREATE OR REPLACE VIEW v_geo_events AS
SELECT
    e.time,
    u.country_code,
    u.lat,
    u.lng,
    e.event_type
FROM events e
JOIN users u ON e.actor_username = u.username
WHERE e.time > now() - INTERVAL '1 hour'
  AND u.lat IS NOT NULL;

-- Grafana User
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') THEN
    CREATE ROLE grafana_reader LOGIN PASSWORD 'grafana_readonly';
    GRANT CONNECT ON DATABASE github_events TO grafana_reader;
    GRANT USAGE ON SCHEMA public TO grafana_reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_reader;
  END IF;
END
$$;
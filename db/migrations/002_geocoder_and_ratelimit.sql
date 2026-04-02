-- Migration 002: geocoder claim column + rate limit snapshot table

-- Allow geocoder to claim rows atomically (prevents duplicate Nominatim requests)
ALTER TABLE users         ADD COLUMN IF NOT EXISTS geo_claimed_at TIMESTAMPTZ;
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS geo_claimed_at TIMESTAMPTZ;

-- Store GitHub API rate limit snapshots from producer and consumer
CREATE TABLE IF NOT EXISTS rate_limit_snapshots (
    id           SERIAL,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source       TEXT NOT NULL CHECK (source IN ('producer', 'consumer')),
    resource     TEXT,             -- 'core', 'search', etc.
    limit_       INTEGER,
    used         INTEGER,
    remaining    INTEGER,
    reset_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ratelimit_recorded
    ON rate_limit_snapshots (recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_ratelimit_source
    ON rate_limit_snapshots (source, recorded_at DESC);

SELECT create_hypertable('rate_limit_snapshots', 'recorded_at', if_not_exists => TRUE);

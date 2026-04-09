-- Migration 003: Geocoder Request Logs Table

-- Store Photon geocoding request logs (similar to request_logs but for geocoder)
-- Note: recorded_at must be part of the PRIMARY KEY for TimescaleDB hypertable partitioning
CREATE TABLE IF NOT EXISTS geocoder_request_logs (
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    id               BIGSERIAL,
    request_success  BOOLEAN NOT NULL,
    sent_at          TIMESTAMPTZ NOT NULL,
    received_at      TIMESTAMPTZ,
    elapsed_s        FLOAT,
    method           TEXT,
    url              TEXT,
    location_query   TEXT,                -- The location string being geocoded
    status_code      INTEGER,
    reason           TEXT,
    response_bytes   INTEGER,
    redirects        INTEGER,
    final_url        TEXT,
    http_version     INTEGER,
    encoding         TEXT,
    request_headers  JSONB,
    response_headers JSONB,
    error            TEXT,
    PRIMARY KEY (recorded_at, id)
);

-- Create hypertable for time-series optimization
SELECT create_hypertable('geocoder_request_logs', 'recorded_at', if_not_exists => TRUE);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_geocoder_recorded
    ON geocoder_request_logs (recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_geocoder_success
    ON geocoder_request_logs (request_success, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_geocoder_location
    ON geocoder_request_logs (location_query, recorded_at DESC);


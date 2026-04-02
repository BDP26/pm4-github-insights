-- Migration 003: GraphQL batching logging columns

ALTER TABLE request_logs
  ADD COLUMN IF NOT EXISTS request_type TEXT DEFAULT 'rest',
  ADD COLUMN IF NOT EXISTS batch_size   INT  DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS token_id     TEXT DEFAULT NULL;

ALTER TABLE rate_limit_snapshots
  ADD COLUMN IF NOT EXISTS token_id TEXT DEFAULT NULL;

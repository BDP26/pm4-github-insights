-- db/migrations/001_add_is_bot_to_users.sql
-- Migration: add is_bot column and clean up bot entries incorrectly stored in organizations

-- 1. Add is_bot column to users (safe: ADD COLUMN IF NOT EXISTS)
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS is_bot BOOLEAN NOT NULL DEFAULT FALSE;

-- 2. Remove bot entries from organization_members where they appear as the
--    member (user_username) — direct LIKE match covers all cases regardless
--    of how the bot ended up stored.
DELETE FROM organization_members
WHERE user_username LIKE '%[bot]%';

-- 3. Remove bots that were incorrectly stored as org_login
DELETE FROM organization_members
WHERE org_login LIKE '%[bot]%';

-- 4. Remove bots from organizations table
DELETE FROM organizations
WHERE login LIKE '%[bot]%';

-- 5. Drop and recreate country_ids_5m continuous aggregate to exclude bots.
--    CASCADE is intentional: no other objects depend on this view.
--    NOTE: if run twice, the view is cleanly recreated each time.
DROP MATERIALIZED VIEW IF EXISTS country_ids_5m CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS country_ids_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', e.time) AS bucket,
    u.country_code,
    count(*) AS event_count
FROM events e
JOIN users u ON e.actor_username = u.username
WHERE u.is_bot = FALSE
GROUP BY bucket, u.country_code;

-- 6. Update v_geo_events to exclude bots explicitly
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
  AND u.lat IS NOT NULL
  AND u.is_bot = FALSE;

-- 7. Update v_top_repos_24h: unique_users count should exclude bots
CREATE OR REPLACE VIEW v_top_repos_24h AS
SELECT
    r.full_name,
    r.language,
    count(e.event_id) AS events,
    count(DISTINCT CASE WHEN u.is_bot IS DISTINCT FROM TRUE THEN e.actor_username END) AS unique_users
FROM events e
JOIN repos r ON e.repo_id = r.repo_id
LEFT JOIN users u ON e.actor_username = u.username
WHERE e.time > now() - INTERVAL '24 hours'
GROUP BY r.full_name, r.language
ORDER BY events DESC;

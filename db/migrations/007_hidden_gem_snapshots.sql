-- ════════════════════════════════════════════════════════════════
--  007 — Hidden Gem Snapshot Tables
-- ════════════════════════════════════════════════════════════════
--  Adds four tables for storing scheduled hidden gem score snapshots.
--  Idempotent: safe to re-apply.
-- ════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS hidden_gem_snapshot_runs (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    interval_hours  INT         NOT NULL CHECK (interval_hours > 0),
    alpha           FLOAT       NOT NULL DEFAULT 1.0,
    beta            FLOAT       NOT NULL DEFAULT 1.0,
    repo_count      INT,
    user_count      INT,
    org_count       INT
);

CREATE TABLE IF NOT EXISTS hidden_gem_snapshot_repos (
    snapshot_id          INT  NOT NULL REFERENCES hidden_gem_snapshot_runs(id) ON DELETE CASCADE,
    repo_id              INT  NOT NULL,
    full_name            TEXT NOT NULL,
    name                 TEXT,
    owner_login          TEXT,
    language             TEXT,
    license_spdx         TEXT,
    topics               TEXT[],
    sig_score            FLOAT,
    rank                 INT,
    count_stars_interval INT,
    count_forks_interval INT,
    total_stars          INT,
    total_forks          INT,
    PRIMARY KEY (snapshot_id, repo_id)
);

CREATE TABLE IF NOT EXISTS hidden_gem_snapshot_users (
    snapshot_id            INT  NOT NULL REFERENCES hidden_gem_snapshot_runs(id) ON DELETE CASCADE,
    username               TEXT NOT NULL,
    total_score            FLOAT,
    best_repo_score        FLOAT,
    best_repo              TEXT,
    hidden_gem_count       INT,
    active_repos_in_window INT,
    PRIMARY KEY (snapshot_id, username)
);

CREATE TABLE IF NOT EXISTS hidden_gem_snapshot_orgs (
    snapshot_id                INT  NOT NULL REFERENCES hidden_gem_snapshot_runs(id) ON DELETE CASCADE,
    org_login                  TEXT NOT NULL,
    org_repos_total_score      FLOAT,
    org_repos_best_score       FLOAT,
    org_active_repos           INT,
    org_hidden_gem_count       INT,
    member_repos_total_score   FLOAT,
    member_repos_best_score    FLOAT,
    member_active_repos        INT,
    member_active_users        INT,
    member_hidden_gem_count    INT,
    PRIMARY KEY (snapshot_id, org_login)
);

CREATE INDEX IF NOT EXISTS idx_snapshot_runs_interval
    ON hidden_gem_snapshot_runs(interval_hours, run_at DESC);
       
CREATE INDEX IF NOT EXISTS idx_snapshot_repos_fullname
    ON hidden_gem_snapshot_repos(full_name, snapshot_id);

CREATE INDEX IF NOT EXISTS idx_snapshot_users_username
    ON hidden_gem_snapshot_users(username, snapshot_id);

CREATE INDEX IF NOT EXISTS idx_snapshot_orgs_login
    ON hidden_gem_snapshot_orgs(org_login, snapshot_id);

DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') THEN
    EXECUTE 'GRANT SELECT ON hidden_gem_snapshot_runs TO grafana_reader';          
    EXECUTE 'GRANT SELECT ON hidden_gem_snapshot_repos TO grafana_reader';
    EXECUTE 'GRANT SELECT ON hidden_gem_snapshot_users TO grafana_reader';
    EXECUTE 'GRANT SELECT ON hidden_gem_snapshot_orgs TO grafana_reader';
    EXECUTE 'GRANT USAGE ON SEQUENCE hidden_gem_snapshot_runs_id_seq TO grafana_reader';
  END IF;
END $$;

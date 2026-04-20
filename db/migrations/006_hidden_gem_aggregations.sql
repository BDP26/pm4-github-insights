-- ════════════════════════════════════════════════════════════════
--  006 — Hidden Gem Aggregations (Drill-Down Functions & Views)
-- ════════════════════════════════════════════════════════════════
--  Adds:
--    - hidden_gem_repo_scores(...)   core repo-level scoring (filtered)
--    - hidden_gem_user_scores(...)   user-level aggregation
--    - hidden_gem_org_scores(...)    org-level aggregation (split by source)
--    - v_repo_languages / v_repo_licenses / v_repo_topics helper views
--  Idempotent: safe to re-apply on a running database.
-- ════════════════════════════════════════════════════════════════

-- Drop existing signatures first to allow return-type changes.
DROP FUNCTION IF EXISTS hidden_gem_repo_scores(FLOAT,FLOAT,INT,TEXT[],TEXT[],TEXT[],INT,INT,INT);
DROP FUNCTION IF EXISTS hidden_gem_user_scores(FLOAT,FLOAT,INT,TEXT[],TEXT[],TEXT[]);
DROP FUNCTION IF EXISTS hidden_gem_org_scores(FLOAT,FLOAT,INT,TEXT[],TEXT[],TEXT[]);


-- ── Core: repo-level hidden-gem scores, with optional filters ────
CREATE OR REPLACE FUNCTION hidden_gem_repo_scores(
    p_alpha     FLOAT,
    p_beta      FLOAT,
    p_hours     INT,
    p_languages TEXT[] DEFAULT NULL,
    p_licenses  TEXT[] DEFAULT NULL,
    p_topics    TEXT[] DEFAULT NULL,
    p_min_stars INT    DEFAULT 5,
    p_min_forks INT    DEFAULT 1,
    p_top_n     INT    DEFAULT 1000
)
RETURNS TABLE (
    repo_id                INT,
    full_name              TEXT,
    name                   TEXT,
    owner_login            TEXT,
    owner_type             TEXT,
    language               TEXT,
    license_spdx           TEXT,
    topics                 TEXT[],
    count_stars_interval   INT,
    count_forks_interval   INT,
    baseline_stars         INT,
    baseline_forks         INT,
    total_stars            INT,
    total_forks            INT,
    repo_age_hours         FLOAT,
    lambda                 FLOAT,
    poisson_cdf            FLOAT,
    sig_score              FLOAT,
    last_event_in_window   TIMESTAMPTZ
)
LANGUAGE sql
STABLE
AS $$
WITH
  config AS (
    SELECT
      date_trunc('minute', NOW())::timestamp AS ts_now,
      make_interval(hours => p_hours)        AS time_window
  ),
  repo_events AS (
    SELECT
        e.repo_id,
        SUM(CASE WHEN e.detail = 'starred'    THEN 1 ELSE 0 END)::INT AS count_stars_interval,
        SUM(CASE WHEN e.detail LIKE 'forked%' THEN 1 ELSE 0 END)::INT AS count_forks_interval,
        MAX(e.time)                                                   AS last_event_in_window
    FROM events e, config cfg
    WHERE e.time >= cfg.ts_now - cfg.time_window
    GROUP BY e.repo_id
    ORDER BY count_stars_interval DESC, count_forks_interval DESC
    LIMIT p_top_n
  ),
  repo_baseline AS (
    SELECT
        r.repo_id,
        r.name,
        r.full_name,
        r.owner_login,
        r.owner_type,
        r.language,
        r.license_spdx,
        r.topics,
        r.created_at,
        SUM(CASE WHEN e.detail = 'starred'    THEN 1 ELSE 0 END)::INT AS count_stars_all,
        SUM(CASE WHEN e.detail LIKE 'forked%' THEN 1 ELSE 0 END)::INT AS count_forks_all,
        r.forks_count                                                 AS init_forks,
        r.stargazers_count                                            AS init_stars
    FROM events e
    JOIN repos r ON r.repo_id = e.repo_id
    WHERE e.repo_id IN (SELECT rep.repo_id FROM repo_events rep)
      AND (p_languages IS NULL OR cardinality(p_languages) = 0 OR r.language     = ANY(p_languages))
      AND (p_licenses  IS NULL OR cardinality(p_licenses)  = 0 OR r.license_spdx = ANY(p_licenses))
      AND (p_topics    IS NULL OR cardinality(p_topics)    = 0 OR r.topics      && p_topics)
    GROUP BY r.repo_id
  ),
  repo_overview AS (
    SELECT
      re.repo_id,
      rb.name,
      rb.full_name,
      rb.owner_login,
      rb.owner_type,
      rb.language,
      rb.license_spdx,
      rb.topics,
      rb.created_at,
      re.count_stars_interval,
      re.count_forks_interval,
      re.last_event_in_window,
      (rb.count_stars_all + rb.init_stars)                                  AS total_stars,
      (rb.init_stars + rb.count_stars_all - re.count_stars_interval)        AS baseline_stars,
      (rb.init_forks + rb.count_forks_all - re.count_forks_interval)        AS baseline_forks,
      (rb.count_forks_all + rb.init_forks)                                  AS total_forks,
      EXTRACT(EPOCH FROM (SELECT ts_now FROM config) - rb.created_at::timestamp) / 3600 AS repo_age_hours
    FROM repo_events re
    JOIN repo_baseline rb ON rb.repo_id = re.repo_id
  ),
  hidden_gem_calc AS (
    SELECT
      ro.*,
      calc_lambda(
          p_alpha, p_beta,
          ro.baseline_stars::INT, ro.baseline_forks::INT,
          ro.count_stars_interval::INT, ro.count_forks_interval::INT,
          GREATEST(ro.repo_age_hours::FLOAT, 1.0),
          (SELECT time_window FROM config)
      ) AS lambda
    FROM repo_overview ro
  )
SELECT
    hc.repo_id,
    hc.full_name,
    hc.name,
    hc.owner_login,
    hc.owner_type,
    hc.language,
    hc.license_spdx,
    hc.topics,
    hc.count_stars_interval,
    hc.count_forks_interval,
    hc.baseline_stars,
    hc.baseline_forks,
    hc.total_stars,
    hc.total_forks,
    hc.repo_age_hours,
    hc.lambda,
    scores.poisson_cdf,
    scores.sig_score,
    hc.last_event_in_window
FROM hidden_gem_calc hc,
LATERAL calc_rising_star_scores(
    numb_stars_forks        := (p_alpha * hc.count_stars_interval::FLOAT + p_beta * hc.count_forks_interval::FLOAT)::INT,
    lambda                  := hc.lambda,
    count_stars_in_interval := hc.count_stars_interval::INT,
    count_forks_in_interval := hc.count_forks_interval::INT,
    min_stars               := p_min_stars,
    min_forks               := p_min_forks
) AS scores;
$$;


-- ── User-level aggregation ───────────────────────────────────────
CREATE OR REPLACE FUNCTION hidden_gem_user_scores(
    p_alpha     FLOAT,
    p_beta      FLOAT,
    p_hours     INT,
    p_languages TEXT[] DEFAULT NULL,
    p_licenses  TEXT[] DEFAULT NULL,
    p_topics    TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    username                TEXT,
    total_score             FLOAT,
    best_repo_score         FLOAT,
    best_repo               TEXT,
    hidden_gem_count        INT,
    active_repos_in_window  INT,
    last_event_in_window    TIMESTAMPTZ
)
LANGUAGE sql
STABLE
AS $$
WITH scored AS (
    SELECT *
    FROM hidden_gem_repo_scores(p_alpha, p_beta, p_hours, p_languages, p_licenses, p_topics)
    WHERE owner_type = 'User' AND sig_score IS NOT NULL
),
ranked AS (
    SELECT
        owner_login,
        full_name,
        sig_score,
        ROW_NUMBER() OVER (PARTITION BY owner_login ORDER BY sig_score DESC NULLS LAST) AS rn
    FROM scored
),
top_repo AS (
    SELECT owner_login, full_name FROM ranked WHERE rn = 1
)
SELECT
    s.owner_login                                                 AS username,
    SUM(s.sig_score)::FLOAT                                       AS total_score,
    MAX(s.sig_score)::FLOAT                                       AS best_repo_score,
    MAX(t.full_name)                                              AS best_repo,
    COUNT(*) FILTER (WHERE s.sig_score >= 3)::INT                 AS hidden_gem_count,
    COUNT(*)::INT                                                 AS active_repos_in_window,
    MAX(s.last_event_in_window)                                   AS last_event_in_window
FROM scored s
LEFT JOIN top_repo t ON t.owner_login = s.owner_login
GROUP BY s.owner_login;
$$;


-- ── Org-level aggregation (split: org-owned vs member-owned) ─────
CREATE OR REPLACE FUNCTION hidden_gem_org_scores(
    p_alpha     FLOAT,
    p_beta      FLOAT,
    p_hours     INT,
    p_languages TEXT[] DEFAULT NULL,
    p_licenses  TEXT[] DEFAULT NULL,
    p_topics    TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    org_login                   TEXT,
    org_repos_total_score       FLOAT,
    org_repos_best_score        FLOAT,
    org_active_repos            INT,
    org_hidden_gem_count        INT,
    member_repos_total_score    FLOAT,
    member_repos_best_score     FLOAT,
    member_active_repos         INT,
    member_active_users         INT,
    member_hidden_gem_count     INT
)
LANGUAGE sql
STABLE
AS $$
WITH scored AS (
    SELECT *
    FROM hidden_gem_repo_scores(p_alpha, p_beta, p_hours, p_languages, p_licenses, p_topics)
    WHERE sig_score IS NOT NULL
),
org_owned AS (
    SELECT
        owner_login AS org_login,
        SUM(sig_score)::FLOAT                           AS org_repos_total_score,
        MAX(sig_score)::FLOAT                           AS org_repos_best_score,
        COUNT(*)::INT                                   AS org_active_repos,
        COUNT(*) FILTER (WHERE sig_score >= 3)::INT     AS org_hidden_gem_count
    FROM scored
    WHERE owner_type = 'Organization'
    GROUP BY owner_login
),
member_owned AS (
    SELECT
        om.org_login,
        SUM(s.sig_score)::FLOAT                         AS member_repos_total_score,
        MAX(s.sig_score)::FLOAT                         AS member_repos_best_score,
        COUNT(*)::INT                                   AS member_active_repos,
        COUNT(DISTINCT s.owner_login)::INT              AS member_active_users,
        COUNT(*) FILTER (WHERE s.sig_score >= 3)::INT   AS member_hidden_gem_count
    FROM scored s
    JOIN organization_members om ON om.user_username = s.owner_login
    WHERE s.owner_type = 'User'
    GROUP BY om.org_login
)
SELECT
    COALESCE(o.org_login, m.org_login)                  AS org_login,
    COALESCE(o.org_repos_total_score, 0)                AS org_repos_total_score,
    COALESCE(o.org_repos_best_score, 0)                 AS org_repos_best_score,
    COALESCE(o.org_active_repos, 0)                     AS org_active_repos,
    COALESCE(o.org_hidden_gem_count, 0)                 AS org_hidden_gem_count,
    COALESCE(m.member_repos_total_score, 0)             AS member_repos_total_score,
    COALESCE(m.member_repos_best_score, 0)              AS member_repos_best_score,
    COALESCE(m.member_active_repos, 0)                  AS member_active_repos,
    COALESCE(m.member_active_users, 0)                  AS member_active_users,
    COALESCE(m.member_hidden_gem_count, 0)              AS member_hidden_gem_count
FROM org_owned o
FULL OUTER JOIN member_owned m ON m.org_login = o.org_login;
$$;


-- ── Templating helper views ──────────────────────────────────────
CREATE OR REPLACE VIEW v_repo_languages AS
  SELECT DISTINCT language AS value FROM repos WHERE language IS NOT NULL;

CREATE OR REPLACE VIEW v_repo_licenses AS
  SELECT DISTINCT license_spdx AS value FROM repos WHERE license_spdx IS NOT NULL;

CREATE OR REPLACE VIEW v_repo_topics AS
  SELECT DISTINCT unnest(topics) AS value FROM repos WHERE topics IS NOT NULL;


-- ── Grants for Grafana read-only user ────────────────────────────
DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') THEN
    EXECUTE 'GRANT EXECUTE ON FUNCTION hidden_gem_repo_scores(FLOAT,FLOAT,INT,TEXT[],TEXT[],TEXT[],INT,INT,INT) TO grafana_reader';
    EXECUTE 'GRANT EXECUTE ON FUNCTION hidden_gem_user_scores(FLOAT,FLOAT,INT,TEXT[],TEXT[],TEXT[]) TO grafana_reader';
    EXECUTE 'GRANT EXECUTE ON FUNCTION hidden_gem_org_scores(FLOAT,FLOAT,INT,TEXT[],TEXT[],TEXT[]) TO grafana_reader';
    EXECUTE 'GRANT SELECT ON v_repo_languages, v_repo_licenses, v_repo_topics TO grafana_reader';
  END IF;
END $$;

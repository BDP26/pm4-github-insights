
----------------- CALC POISSINB CDF (qpois())

CREATE OR REPLACE FUNCTION F_poisson(
    numb_stars       INT,      -- upper bound (inclusive)
    lambda  FLOAT     -- expected rate
)
RETURNS FLOAT
LANGUAGE SQL
STABLE
AS $$
    SELECT LEAST(GREATEST(1.0 - F_poisson_sf(numb_stars, lambda), 0.0), 1.0)
$$;

CREATE OR REPLACE FUNCTION F_poisson_sf(
    k       INT,
    lambda  FLOAT
)
RETURNS FLOAT
LANGUAGE plpgsql
IMMUTABLE STRICT
AS $$
DECLARE
    log_term  FLOAT;
    log_sf    FLOAT;
    max_iter  INT := GREATEST(k + 200, 1000);
    i         INT;
BEGIN
    IF lambda <= 0 THEN RETURN 1.0; END IF;

    -- When k is far below lambda, P(X > k | lambda) ≈ 1.0.
    -- Avoids log-sum-exp underflow: the algorithm would need to sum up to ~lambda
    -- terms but max_iter stops at k+1000, missing the bulk of the probability mass.
    IF k::FLOAT < lambda - 30.0 * SQRT(lambda) THEN
        RETURN 1.0;
    END IF;

    -- Build log P(X = k+1) iteratively
    log_term := -lambda;
    FOR i IN 1..(k + 1) LOOP
        log_term := log_term + LN(lambda) - LN(i::FLOAT);
    END LOOP;

    log_sf := log_term;

    -- Accumulate upper tail via log-sum-exp
    FOR i IN (k + 2)..max_iter LOOP
        log_term := log_term + LN(lambda) - LN(i::FLOAT);
        -- Guard against underflow: if log_term is negligibly small vs log_sf, exit early.
        -- This prevents EXP() from being called with extremely negative arguments.
        EXIT WHEN (log_term - log_sf) < -708.0;
        log_sf   := log_sf + LN(1.0 + EXP(log_term - log_sf));
    END LOOP;

    -- Guard against float8 underflow: EXP(x) throws "value out of range" for x < ~-745.
    -- A log_sf this negative means SF ≈ 0 (observed >> expected → maximally significant).
    IF log_sf < -708.0 THEN RETURN 0.0; END IF;
    RETURN LEAST(EXP(log_sf), 1.0);
END;
$$;



----------------- CALC POISSINB CDF (qpois())
CREATE OR REPLACE FUNCTION calc_lambda(
    alpha                     FLOAT,
    beta                      FLOAT,
    baseline_stars            INT,
    baseline_forks            INT,
    count_stars_in_intervall  INT,
    count_forks_in_intervall  INT,
    repo_age                  FLOAT,
    time_interval             INTERVAL
)
RETURNS FLOAT
LANGUAGE plpgsql AS $$
DECLARE 
  timeinterval_as_float FLOAT;
  safe_age FLOAT;
BEGIN 
  timeinterval_as_float := (EXTRACT(EPOCH FROM time_interval) / 3600.0);
  safe_age := GREATEST(repo_age, 1.0);  -- clamp to min 1 hour

  -- For young repos: use ONLY the baseline snapshot (API data),
  -- never the interval counts, to avoid circular testing
  IF timeinterval_as_float >= safe_age THEN
    RETURN (( alpha * baseline_stars + beta * baseline_forks ) / safe_age ) * safe_age;
  ELSE 
    RETURN (( alpha * baseline_stars + beta * baseline_forks ) / safe_age ) * timeinterval_as_float;
  END IF;
END;
$$;



----------------- CALC Score / Signifikanz (score von 3 ~ 95% signifikanz )
CREATE OR REPLACE FUNCTION calc_sig_score(
    pois_cdf                 FLOAT,
    count_stars_in_intervall INT,
    count_forks_in_intervall INT,
    min_stars                INT,
    min_forks                INT
)
RETURNS FLOAT
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    IF (count_stars_in_intervall < min_stars) OR (count_forks_in_intervall < min_forks) THEN
        RETURN NULL;
    END IF;

    IF pois_cdf >= 1.0 THEN
        RETURN NULL;
    END IF;

    -- Clamp to avoid -LN(0) overflow
    RETURN -LN(GREATEST(1.0 - pois_cdf, 1e-15));
END;
$$;






----------------- COMBINED POISSONM_CDF AND SIGSCORE

CREATE OR REPLACE FUNCTION calc_rising_star_scores(
    numb_stars_forks         INT,    -- x_t: observed stars in window
    lambda                   FLOAT,  -- expected rate from calc_lambda()
    count_stars_in_interval  INT,    -- stars observed in window (for noise guard)
    count_forks_in_interval  INT,    -- forks observed in window (for noise guard)
    min_stars                INT,    -- noise guard: minimum stars for valid score
    min_forks                INT     -- noise guard: minimum forks for valid score
)
RETURNS TABLE (
    poisson_cdf  FLOAT,  -- P(X <= x_t | lambda), display only, derived as 1 - SF
    sig_score    FLOAT   -- significance score: (-ln(SF) / 34.5) * 10, range 0-10
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_sf  FLOAT;
BEGIN
    v_sf := F_poisson_sf(numb_stars_forks - 1, lambda);

    RETURN QUERY
    SELECT
        1.0 - v_sf,  -- cdf nur zur Anzeige
        CASE
            WHEN (count_stars_in_interval < min_stars) OR (count_forks_in_interval < min_forks) THEN NULL
            WHEN v_sf <= 0 THEN NULL
            ELSE (-LN(GREATEST(v_sf, 1e-15)) / 34.5) * 10.0
        END;
END;
$$;
------------ E N D    OF   F U N C T I O N S-------------------------------------


CREATE INDEX IF NOT EXISTS idx_events_time_repo ON events (time DESC, repo_id, detail);
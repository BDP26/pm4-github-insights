

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






----------------- CALC LAMBDA

CREATE OR REPLACE FUNCTION calc_lambda(
    alpha                     FLOAT,
    beta                      FLOAT,
    baseline_stars            INT,     -- Stand der Stars vor Zeitfenster
    baseline_forks            INT,     -- Stand der Forks vor Zeitfenster
    count_stars_in_intervall  INT,     -- Count wie viele Stars dazu kamen
    count_forks_in_intervall  INT,
    repo_age                  FLOAT,   -- Repo alter in Stunden
    time_interval             INTERVAL     -- Beobachtungsfenster
)
RETURNS FLOAT
LANGUAGE plpgsql as $$
DECLARE 
  timeinterval_as_float FLOAT;
  
BEGIN 
  timeinterval_as_float := (EXTRACT(EPOCH FROM time_interval) / 3600.0);
  IF timeinterval_as_float >= repo_age THEN
    RETURN (( alpha * count_stars_in_intervall + beta * count_forks_in_intervall ) / repo_age ) * timeinterval_as_float;
  ELSE 
    RETURN (( alpha * baseline_stars + beta * baseline_forks ) / repo_age ) * timeinterval_as_float;
  END IF;
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
    poisson_cdf  FLOAT,  -- P(X <= x_t | lambda)
    sig_score    FLOAT   -- significance score: -ln(1 - cdf)
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_cdf FLOAT;  -- intermediate CDF value to avoid double calculation
BEGIN
    -- calculate CDF once and reuse
    v_cdf := F_poisson(numb_stars_forks - 1, lambda);

    RETURN QUERY
    SELECT
        v_cdf,
        calc_sig_score(v_cdf, count_stars_in_interval, count_forks_in_interval, min_stars, min_forks);
END;
$$;
------------ E N D    OF   F U N C T I O N S-------------------------------------


CREATE INDEX IF NOT EXISTS idx_events_time_repo ON events (time DESC, repo_id, detail);
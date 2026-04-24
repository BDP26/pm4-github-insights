"""
SnapshotScheduler
─────────────────
Periodically captures hidden gem scores from the DB and writes them to
snapshot tables. Designed to be extracted to a standalone container:
the class has zero FastAPI imports — it only needs an asyncpg.Pool.
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SnapshotConfig:
    interval_hours: list[int] = field(default_factory=lambda: [24, 168, 730])
    alpha: float = 1.0
    beta: float = 1.0
    min_stars: int = 5
    min_forks: int = 1
    top_n: int = 1000
    query_timeout_s: float = 300.0  # per-query timeout for scoring functions


class SnapshotScheduler:
    def __init__(self, pool: asyncpg.Pool, config: SnapshotConfig) -> None:
        self._pool = pool
        self._config = config
        self._scheduler = AsyncIOScheduler()

    async def start(self) -> None:
        """Register one APScheduler job per interval and start the scheduler."""
        for hours in self._config.interval_hours:
            self._scheduler.add_job(
                self._run_snapshot,
                "interval",
                hours=hours,
                args=[hours],
                id=f"snapshot_{hours}h",
                next_run_time=datetime.now(timezone.utc),  # run immediately on startup
                replace_existing=True,
            )
            log.info("Registered snapshot job: every %sh", hours)
        self._scheduler.start()
        log.info("SnapshotScheduler started with intervals: %s", self._config.interval_hours)

    async def stop(self) -> None:
        """Gracefully shut down APScheduler."""
        self._scheduler.shutdown(wait=False)
        log.info("SnapshotScheduler stopped")

    async def trigger(self, interval_hours: int) -> None:
        """Manually trigger a snapshot for a given interval (e.g. from an API endpoint)."""
        await self._run_snapshot(interval_hours)

    async def _run_snapshot(self, interval_hours: int) -> None:
        """Core snapshot logic: call DB scoring functions and persist results."""
        log.info("Starting snapshot: interval=%sh", interval_hours)
        run_id: int | None = None
        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    # 1. Insert the run record and get its ID
                    run_id = await conn.fetchval(
                        """
                        INSERT INTO hidden_gem_snapshot_runs (run_at, interval_hours, alpha, beta)
                        VALUES (NOW(), $1, $2, $3)
                        RETURNING id
                        """,
                        interval_hours,
                        self._config.alpha,
                        self._config.beta,
                    )

                    # 2. Capture repo scores
                    repo_rows = await conn.fetch(
                        """
                        SELECT
                            repo_id, full_name, name, owner_login, language,
                            license_spdx, topics,
                            sig_score,
                            ROW_NUMBER() OVER (ORDER BY sig_score DESC NULLS LAST) AS rank,
                            count_stars_interval, count_forks_interval,
                            total_stars, total_forks
                        FROM hidden_gem_repo_scores(
                            $1::float, $2::float, $3::int,
                            NULL, NULL, NULL,
                            $4::int, $5::int, $6::int
                        )
                        WHERE sig_score IS NOT NULL
                        """,
                        self._config.alpha,
                        self._config.beta,
                        interval_hours,
                        self._config.min_stars,
                        self._config.min_forks,
                        self._config.top_n,
                        timeout=self._config.query_timeout_s,
                    )

                    if repo_rows:
                        await conn.executemany(
                            """
                            INSERT INTO hidden_gem_snapshot_repos
                                (snapshot_id, repo_id, full_name, name, owner_login,
                                 language, license_spdx, topics, sig_score, rank,
                                 count_stars_interval, count_forks_interval,
                                 total_stars, total_forks)
                            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                            ON CONFLICT (snapshot_id, repo_id) DO NOTHING
                            """,
                            [
                                (
                                    run_id,
                                    r["repo_id"], r["full_name"], r["name"], r["owner_login"],
                                    r["language"], r["license_spdx"], r["topics"],
                                    r["sig_score"], r["rank"],
                                    r["count_stars_interval"], r["count_forks_interval"],
                                    r["total_stars"], r["total_forks"],
                                )
                                for r in repo_rows
                            ],
                        )

                    # 3. Capture user scores
                    user_rows = await conn.fetch(
                        """
                        SELECT username, total_score, best_repo_score, best_repo,
                               hidden_gem_count, active_repos_in_window
                        FROM hidden_gem_user_scores($1::float, $2::float, $3::int,
                                                    NULL, NULL, NULL)
                        """,
                        self._config.alpha,
                        self._config.beta,
                        interval_hours,
                        timeout=self._config.query_timeout_s,
                    )

                    if user_rows:
                        await conn.executemany(
                            """
                            INSERT INTO hidden_gem_snapshot_users
                                (snapshot_id, username, total_score, best_repo_score,
                                 best_repo, hidden_gem_count, active_repos_in_window)
                            VALUES ($1,$2,$3,$4,$5,$6,$7)
                            ON CONFLICT (snapshot_id, username) DO NOTHING
                            """,
                            [
                                (
                                    run_id,
                                    r["username"], r["total_score"], r["best_repo_score"],
                                    r["best_repo"], r["hidden_gem_count"],
                                    r["active_repos_in_window"],
                                )
                                for r in user_rows
                            ],
                        )

                    # 4. Capture org scores
                    org_rows = await conn.fetch(
                        """
                        SELECT org_login, org_repos_total_score, org_repos_best_score,
                               org_active_repos, org_hidden_gem_count,
                               member_repos_total_score, member_repos_best_score,
                               member_active_repos, member_active_users,
                               member_hidden_gem_count
                        FROM hidden_gem_org_scores($1::float, $2::float, $3::int,
                                                   NULL, NULL, NULL)
                        """,
                        self._config.alpha,
                        self._config.beta,
                        interval_hours,
                        timeout=self._config.query_timeout_s,
                    )

                    if org_rows:
                        await conn.executemany(
                            """
                            INSERT INTO hidden_gem_snapshot_orgs
                                (snapshot_id, org_login, org_repos_total_score,
                                 org_repos_best_score, org_active_repos,
                                 org_hidden_gem_count, member_repos_total_score,
                                 member_repos_best_score, member_active_repos,
                                 member_active_users, member_hidden_gem_count)
                            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                            ON CONFLICT (snapshot_id, org_login) DO NOTHING
                            """,
                            [
                                (
                                    run_id,
                                    r["org_login"],
                                    r["org_repos_total_score"], r["org_repos_best_score"],
                                    r["org_active_repos"], r["org_hidden_gem_count"],
                                    r["member_repos_total_score"], r["member_repos_best_score"],
                                    r["member_active_repos"], r["member_active_users"],
                                    r["member_hidden_gem_count"],
                                )
                                for r in org_rows
                            ],
                        )

                    # 5. Update run record with counts
                    await conn.execute(
                        """
                        UPDATE hidden_gem_snapshot_runs
                        SET repo_count = $2, user_count = $3, org_count = $4
                        WHERE id = $1
                        """,
                        run_id,
                        len(repo_rows),
                        len(user_rows),
                        len(org_rows),
                    )
        except Exception:
            log.exception(
                "Snapshot FAILED: interval=%sh run_id=%s", interval_hours, run_id
            )
            raise

        log.info(
            "Snapshot complete: id=%s interval=%sh repos=%s users=%s orgs=%s",
            run_id, interval_hours, len(repo_rows), len(user_rows), len(org_rows),
        )

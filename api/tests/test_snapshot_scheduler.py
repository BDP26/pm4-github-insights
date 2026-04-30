import asyncpg
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from scheduler import SnapshotConfig, SnapshotScheduler


@pytest.fixture
def config() -> SnapshotConfig:
    return SnapshotConfig(interval_hours=[24], alpha=1.0, beta=1.0)


@pytest.fixture
def mock_pool() -> MagicMock:
    pool = MagicMock()
    conn = AsyncMock()
    conn.fetchval = AsyncMock(return_value=1)
    conn.fetch = AsyncMock(return_value=[])
    conn.executemany = AsyncMock()
    conn.execute = AsyncMock()
    # conn.transaction() must return an async context manager (not a coroutine)
    txn_ctx = MagicMock()
    txn_ctx.__aenter__ = AsyncMock(return_value=None)
    txn_ctx.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=txn_ctx)
    # pool.acquire() must return an async context manager directly (not a coroutine)
    acquire_ctx = MagicMock()
    acquire_ctx.__aenter__ = AsyncMock(return_value=conn)
    acquire_ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = acquire_ctx
    return pool


def test_snapshot_config_defaults() -> None:
    cfg = SnapshotConfig()
    assert cfg.interval_hours == [24, 168, 730]
    assert cfg.alpha == 1.0
    assert cfg.beta == 1.0
    assert cfg.min_stars == 5
    assert cfg.min_forks == 1
    assert cfg.top_n == 1000


def test_snapshot_config_custom() -> None:
    cfg = SnapshotConfig(interval_hours=[48], alpha=2.0, beta=0.5)
    assert cfg.interval_hours == [48]
    assert cfg.alpha == 2.0


@pytest.mark.asyncio
async def test_run_snapshot_inserts_run_record(
    mock_pool: MagicMock, config: SnapshotConfig
) -> None:
    with patch("scheduler.snapshot_scheduler.AsyncIOScheduler"):
        scheduler = SnapshotScheduler(mock_pool, config)
        await scheduler._run_snapshot(24)

    conn = mock_pool.acquire.return_value.__aenter__.return_value
    # Should insert a run record
    conn.fetchval.assert_awaited_once()
    insert_sql = conn.fetchval.call_args[0][0]
    assert "INSERT INTO hidden_gem_snapshot_runs" in insert_sql


@pytest.mark.asyncio
async def test_run_snapshot_updates_counts(
    mock_pool: MagicMock, config: SnapshotConfig
) -> None:
    with patch("scheduler.snapshot_scheduler.AsyncIOScheduler"):
        scheduler = SnapshotScheduler(mock_pool, config)
        await scheduler._run_snapshot(24)

    conn = mock_pool.acquire.return_value.__aenter__.return_value
    conn.execute.assert_awaited_once()
    update_sql = conn.execute.call_args[0][0]
    assert "UPDATE hidden_gem_snapshot_runs" in update_sql


@pytest.mark.asyncio
async def test_run_snapshot_reraises_on_db_error(
    mock_pool: MagicMock, config: SnapshotConfig
) -> None:
    conn = mock_pool.acquire.return_value.__aenter__.return_value
    conn.fetchval.side_effect = Exception("connection reset")
    with patch("scheduler.snapshot_scheduler.AsyncIOScheduler"):
        scheduler = SnapshotScheduler(mock_pool, config)
        with pytest.raises(Exception, match="connection reset"):
            await scheduler._run_snapshot(24)


def test_snapshot_config_retry_defaults() -> None:
    cfg = SnapshotConfig()
    assert cfg.max_retries == 10
    assert cfg.retry_delay_s == 5.0


def test_snapshot_config_retry_custom() -> None:
    cfg = SnapshotConfig(max_retries=3, retry_delay_s=2.0)
    assert cfg.max_retries == 3
    assert cfg.retry_delay_s == 2.0


@pytest.mark.asyncio
async def test_run_snapshot_uses_provided_timeout(mock_pool: MagicMock) -> None:
    """_run_snapshot must pass the explicit query_timeout_s to every conn.fetch call."""
    cfg = SnapshotConfig(query_timeout_s=300.0)
    with patch("scheduler.snapshot_scheduler.AsyncIOScheduler"):
        scheduler = SnapshotScheduler(mock_pool, cfg)
        await scheduler._run_snapshot(24, query_timeout_s=600.0)

    conn = mock_pool.acquire.return_value.__aenter__.return_value
    # All three fetch calls (repo, user, org scoring) must use timeout=600.0
    for call in conn.fetch.call_args_list:
        assert call.kwargs.get("timeout") == 600.0, (
            f"Expected timeout=600.0 but got {call.kwargs.get('timeout')}"
        )


@pytest.mark.asyncio
async def test_retry_succeeds_after_transient_failure(mock_pool: MagicMock) -> None:
    """_run_with_retry retries after a transient error and eventually succeeds."""
    cfg = SnapshotConfig(max_retries=10, retry_delay_s=0.0, query_timeout_s=100.0)
    with patch("scheduler.snapshot_scheduler.AsyncIOScheduler"):
        scheduler = SnapshotScheduler(mock_pool, cfg)

    call_count = 0
    timeouts_seen: list[float] = []

    async def fake_run_snapshot(interval_hours: int, query_timeout_s: float | None = None) -> None:
        nonlocal call_count
        call_count += 1
        timeouts_seen.append(query_timeout_s)
        if call_count == 1:
            raise asyncpg.exceptions.ConnectionDoesNotExistError()

    with patch.object(scheduler, "_run_snapshot", side_effect=fake_run_snapshot):
        await scheduler._run_with_retry(24)

    assert call_count == 2
    assert timeouts_seen[0] == 100.0   # attempt 1: base × 1
    assert timeouts_seen[1] == 200.0   # attempt 2: base × 2


@pytest.mark.asyncio
async def test_retry_exhausted_raises(mock_pool: MagicMock) -> None:
    """_run_with_retry raises after max_retries + 1 total attempts."""
    cfg = SnapshotConfig(max_retries=3, retry_delay_s=0.0, query_timeout_s=100.0)
    with patch("scheduler.snapshot_scheduler.AsyncIOScheduler"):
        scheduler = SnapshotScheduler(mock_pool, cfg)

    call_count = 0

    async def always_fails(interval_hours: int, query_timeout_s: float | None = None) -> None:
        nonlocal call_count
        call_count += 1
        raise asyncpg.exceptions.ConnectionDoesNotExistError()

    with patch.object(scheduler, "_run_snapshot", side_effect=always_fails):
        with pytest.raises(asyncpg.exceptions.ConnectionDoesNotExistError):
            await scheduler._run_with_retry(24)

    assert call_count == 4  # 1 initial + 3 retries


@pytest.mark.asyncio
async def test_non_transient_error_raises_immediately(mock_pool: MagicMock) -> None:
    """A non-transient exception is not retried — it surfaces on the first attempt."""
    cfg = SnapshotConfig(max_retries=10, retry_delay_s=0.0)
    with patch("scheduler.snapshot_scheduler.AsyncIOScheduler"):
        scheduler = SnapshotScheduler(mock_pool, cfg)

    call_count = 0

    async def raises_value_error(interval_hours: int, query_timeout_s: float | None = None) -> None:
        nonlocal call_count
        call_count += 1
        raise ValueError("not a transient error")

    with patch.object(scheduler, "_run_snapshot", side_effect=raises_value_error):
        with pytest.raises(ValueError, match="not a transient error"):
            await scheduler._run_with_retry(24)

    assert call_count == 1  # no retries


@pytest.mark.asyncio
async def test_timeout_multiplier(mock_pool: MagicMock) -> None:
    """Timeout passed to _run_snapshot must be query_timeout_s × (attempt_number)."""
    cfg = SnapshotConfig(max_retries=10, retry_delay_s=0.0, query_timeout_s=50.0)
    with patch("scheduler.snapshot_scheduler.AsyncIOScheduler"):
        scheduler = SnapshotScheduler(mock_pool, cfg)

    timeouts_seen: list[float] = []
    call_count = 0

    async def capture_and_fail(interval_hours: int, query_timeout_s: float | None = None) -> None:
        nonlocal call_count
        call_count += 1
        timeouts_seen.append(query_timeout_s)
        if call_count < 4:
            raise asyncpg.exceptions.ConnectionDoesNotExistError()

    with patch.object(scheduler, "_run_snapshot", side_effect=capture_and_fail):
        await scheduler._run_with_retry(24)

    assert timeouts_seen == [50.0, 100.0, 150.0, 200.0]


@pytest.mark.asyncio
async def test_start_registers_run_with_retry(mock_pool: MagicMock, config: SnapshotConfig) -> None:
    """APScheduler jobs must be registered with _run_with_retry, not _run_snapshot."""
    with patch("scheduler.snapshot_scheduler.AsyncIOScheduler") as MockScheduler:
        mock_sched = MockScheduler.return_value
        mock_sched.add_job = MagicMock()
        mock_sched.start = MagicMock()

        scheduler = SnapshotScheduler(mock_pool, config)
        await scheduler.start()

        assert mock_sched.add_job.call_count == len(config.interval_hours)
        for call in mock_sched.add_job.call_args_list:
            registered_fn = call.args[0]
            assert registered_fn == scheduler._run_with_retry, (
                f"Expected _run_with_retry but got {registered_fn.__name__}"
            )

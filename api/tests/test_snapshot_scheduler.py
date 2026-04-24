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

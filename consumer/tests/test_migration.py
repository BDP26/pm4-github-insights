"""Smoke-test that migration 003 SQL is valid and idempotent."""
import pathlib

MIGRATION = pathlib.Path(__file__).parents[2] / "db" / "migrations" / "003_graphql_logging.sql"


def test_migration_file_exists():
    assert MIGRATION.exists(), f"Migration file not found: {MIGRATION}"


def test_migration_contains_required_alters():
    sql = MIGRATION.read_text()
    assert "ADD COLUMN IF NOT EXISTS request_type" in sql
    assert "ADD COLUMN IF NOT EXISTS batch_size" in sql
    assert "ADD COLUMN IF NOT EXISTS token_id" in sql
    # rate_limit_snapshots also gets token_id
    assert sql.count("ADD COLUMN IF NOT EXISTS token_id") == 2


def test_migration_uses_if_not_exists():
    """All ALTER statements must be idempotent."""
    sql = MIGRATION.read_text()
    add_column_lines = [l for l in sql.splitlines() if "ADD COLUMN" in l]
    for line in add_column_lines:
        assert "IF NOT EXISTS" in line, f"Missing IF NOT EXISTS: {line}"

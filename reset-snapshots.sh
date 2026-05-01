#!/usr/bin/env bash
# reset-snapshots.sh — Wipe all snapshot data and rebuild with the current scoring formula.
#
# Run this AFTER prod-run.sh when the scoring formula has changed (e.g. 004 migration updated).
# Only snapshot tables are touched — all raw event data is preserved.
#
# Usage:
#   ./reset-snapshots.sh [compose-profile]   # default profile: Prod

set -euo pipefail

PROFILE="${1:-Prod}"
API_URL="http://localhost:8000"
INTERVALS=(24 168 730)

# Docker Compose project name (same logic as prod-run.sh)
PROJECT=$(basename "$(pwd)" | tr '[:upper:]' '[:lower:]')

info()  { echo "[reset-snapshots] ==> $*"; }
error() { echo "[reset-snapshots] ERROR: $*" >&2; exit 1; }

check_compose_file() {
    [[ -f "docker-compose.yml" ]] || error "docker-compose.yml not found. Run from project root."
}

# ── Wait until the API is healthy ────────────────────────────────────────────
wait_for_api() {
    info "Waiting for API to be ready at $API_URL/health ..."
    local attempts=0
    until curl -sf "$API_URL/health" > /dev/null 2>&1; do
        attempts=$((attempts + 1))
        if [[ $attempts -ge 30 ]]; then
            error "API did not become healthy after 60 s. Check: docker compose --profile $PROFILE logs api"
        fi
        sleep 2
    done
    info "API is ready."
}

check_compose_file

# ── 1. Verify the stack is running ───────────────────────────────────────────
info "Checking stack status..."
if ! docker compose --profile "$PROFILE" ps -q 2>/dev/null | grep -q .; then
    error "Stack is not running. Run ./prod-run.sh first."
fi

wait_for_api

# ── 2. Re-apply 004 + 006 migrations (scoring functions) ─────────────────────
# docker-entrypoint-initdb.d only runs on first DB init — the volume persists
# across restarts, so function changes are never picked up automatically.
#
# F_poisson must be dropped first: CREATE OR REPLACE cannot rename parameters.
info "Dropping F_poisson to allow parameter rename ..."
docker compose --profile "$PROFILE" exec -T timescaledb \
    psql -U github -d github_events -c \
    "DROP FUNCTION IF EXISTS F_poisson(integer, double precision) CASCADE;"

info "Re-applying db/migrations/004_hidden_gem_functions.sql ..."
docker compose --profile "$PROFILE" exec -T timescaledb \
    psql -U github -d github_events \
    --set ON_ERROR_STOP=1 \
    -f /docker-entrypoint-initdb.d/05_migration_004.sql \
    && info "004 migration applied."

# 006 defines hidden_gem_repo_scores / user_scores / org_scores — also not
# re-applied automatically. Must come AFTER 004 (depends on its functions).
info "Re-applying db/migrations/006_hidden_gem_aggregations.sql ..."
docker compose --profile "$PROFILE" exec -T timescaledb \
    psql -U github -d github_events \
    --set ON_ERROR_STOP=1 \
    -f /docker-entrypoint-initdb.d/07_migration_006.sql \
    && info "006 migration applied."

# ── 3. Wipe snapshot tables via psql inside the timescaledb container ────────
info "Clearing all snapshot data (TRUNCATE ... CASCADE)..."
docker compose --profile "$PROFILE" exec -T timescaledb \
    psql -U github -d github_events -c \
    "TRUNCATE hidden_gem_snapshot_runs CASCADE;" \
    && info "Snapshot tables cleared."

# ── 3. Trigger fresh snapshots for all intervals ─────────────────────────────
for hours in "${INTERVALS[@]}"; do
    info "Triggering snapshot for interval=${hours}h ..."
    response=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$API_URL/api/hidden-gems/snapshots/trigger?interval_hours=${hours}")
    if [[ "$response" == "200" ]]; then
        info "  interval=${hours}h → OK (202/200)"
    else
        info "  interval=${hours}h → HTTP $response (check API logs)"
    fi
done

# ── 4. Show live logs so you can watch progress ──────────────────────────────
info "Done — snapshot jobs are running in background."
echo ""
echo "  Watch progress:  docker compose --profile $PROFILE logs -f api"
echo "  Note: the 730h snapshot can take several minutes depending on data volume."

#!/usr/bin/env bash
# prod-run.sh — Pull, rebuild, and (re)start the GitHub Events pipeline.
#
# Steps:
#   1. git pull
#   2. Stop running stack if it exists (all data volumes preserved)
#   3. Remove grafana-data volume only (so provisioned dashboards always reload)
#   4. Rebuild all consumer/producer images with --no-cache
#   5. Start the stack in detached mode

set -euo pipefail

PROFILE="${1:-Prod}"
PHOTON_REGION="planet"

# Docker Compose names volumes as <project>_<volume_name>.
# The project name is the lowercase directory name (Compose default).
PROJECT=$(basename "$(pwd)" | tr '[:upper:]' '[:lower:]')
GRAFANA_VOLUME="${PROJECT}_grafana-data"

info()  { echo "[prod-run] ==> $*"; }
check_compose_file() {
    if [[ ! -f "docker-compose.yml" ]]; then
        echo "[prod-run] ERROR: docker-compose.yml not found. Run this script from the project root." >&2
        exit 1
    fi
}

check_compose_file

# ── 1. Pull latest code ──────────────────────────────────────────────────────
info "Pulling latest code..."
git pull

# ── 2. Stop running stack (keep all data volumes) ───────────────────────────
info "Checking for running containers..."
if docker compose --profile "$PROFILE" ps -q 2>/dev/null | grep -q .; then
    info "Stack is running — stopping (kafka-data and timescale-data volumes preserved)..."
    docker compose --profile "$PROFILE" down
else
    info "Stack is not running."
fi

# ── 3. Remove grafana volume (always reload provisioned dashboards) ──────────
info "Removing Grafana volume ($GRAFANA_VOLUME)..."
if docker volume rm "$GRAFANA_VOLUME" 2>/dev/null; then
    info "Grafana volume removed."
else
    info "Grafana volume not found — skipping."
fi

# ── 4. Rebuild with no cache ─────────────────────────────────────────────────
info "Building images (--no-cache)..."
PHOTON_REGION="$PHOTON_REGION" docker compose --profile "$PROFILE" build --no-cache

# ── 5. Start the stack ───────────────────────────────────────────────────────
info "Starting stack..."
PHOTON_REGION="$PHOTON_REGION" docker compose --profile "$PROFILE" up -d

info "Done."
echo ""
echo "  Frontend will be rebuilt from frontend/package.json during docker compose build"
echo "  If you changed frontend dependencies, the next server deploy must rebuild the frontend image"
echo "  Follow all logs:      docker compose --profile $PROFILE logs -f"
echo "  Follow consumer logs: docker logs -f github-consumer-0"
echo "  Kafka UI:             http://localhost:8080"
echo "  Photon region:        $PHOTON_REGION"

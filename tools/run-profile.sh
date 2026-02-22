#!/bin/bash
# Run this script from the same directory, e.g. bash run.sh up
# Small wrapper script to export required environment variables into shell
# all arguments are passed to docker compose
# e.g.: bash run.sh up, bash run.sh up --build, bash run.sh down, bash run.sh stop, ...
set -e

echo "Loading environment variables from infDB .env file..."
set -a
[ -f $(dirname "$0")/../.env ] && . $(dirname "$0")/../.env
set +a

echo "Loading environment variables from local .env file..."
set -a
[ -f $(dirname "$0")/.env ] && . $(dirname "$0")/.env
set +a

# Extract profile and additional parameter
PROFILE="${1:-$PROFILE}"
PARAM="${2:-$AGS}"
OPTIONS="${3:-}"
PROJECT="infdb_${PROFILE}_${PARAM}"

# Use the shared infdb network for all tool runs
export INFDB_NETWORK="${INFDB_NETWORK:-infdb-infdb-demo_network}"

echo "Starting docker compose..."
export AGS="$PARAM"
docker compose -f "$(dirname "$0")/compose.yml" \
    -p "$PROJECT" \
    --profile "$PROFILE" up\
    --remove-orphans --abort-on-container-failure
# Stop and remove containers, networks, images, and volumes created by up
docker compose -f "$(dirname "$0")/compose.yml" \
    -p "$PROJECT" \
    --profile "$PROFILE" down\
    --volumes --remove-orphans  # --rmi all

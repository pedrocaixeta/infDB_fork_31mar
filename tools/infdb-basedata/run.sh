#!/bin/bash
# e.g.: bash run.sh up, bash run.sh up --build, bash run.sh down, bash run.sh stop, ...
set -e

echo "Loading environment variables from infDB .env file..."
set -a
[ -f $(dirname "$0")/../.env ] && . $(dirname "$0")/../.env
set +a

echo "Loading environment variables from local .env file..."
set -a
[ -f $(dirname "$0")/.env ] && . $(dirname "$0")/.env_infdb_basedata.env
set +a

PARAM="${1:-$AGS}"
OPTIONS="${2:-}"

echo "Starting docker compose..."
export AGS="$PARAM"

# echo "Starting docker compose..."
docker compose -f "$(dirname "$0")/compose.yml" up $OPTIONS
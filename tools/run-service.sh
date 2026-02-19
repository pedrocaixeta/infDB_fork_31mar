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

# Extract SERVICE and additional parameter
SERVICE="${1:-$SERVICE}"
PARAM="${2:-$AGS}"
OPTIONS="${3:-$OPTIONS}"

echo "Starting docker compose single service $SERVICE"
export AGS="$PARAM"
docker compose -f "$(dirname "$0")/compose.yml" up --no-deps --remove-orphans $OPTIONS "$SERVICE" 
docker compose -f "$(dirname "$0")/compose.yml" down --volumes --rmi all --remove-orphans "$SERVICE"
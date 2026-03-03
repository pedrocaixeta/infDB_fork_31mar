#!/bin/bash
# Small wrapper script to export required environment variables into shell
# and pass arguments to docker compose
# e.g.: bash run-tool.sh TOOLNAME AGS or bash run-tool.sh TOOLNAME AGS --build
set -e

echo "Loading environment variables from infDB .env file..."
set -a
[ -f $(dirname "$0")/../.env ] && . $(dirname "$0")/../.env
set +a

echo "Loading environment variables from local .env file..."
set -a
[ -f $(dirname "$0")/.env ] && . $(dirname "$0")/.env
set +a

# Extract TOOL and additional parameter
TOOL="${1:-$TOOL}"
PARAM="${2:-$AGS}"
OPTIONS="${3:-$OPTIONS}"

echo "Starting docker compose single tool $TOOL"
export AGS="$PARAM"
docker compose -f "$(dirname "$0")/compose.yml" up --no-deps --remove-orphans $OPTIONS "$TOOL" 
docker compose -f "$(dirname "$0")/compose.yml" down --volumes --rmi all --remove-orphans "$TOOL"
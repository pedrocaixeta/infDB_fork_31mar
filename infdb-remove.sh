#!/usr/bin/env bash
# set -euo pipefail

# ----------------------------------------------------------------------
# infDB remove script
# ----------------------------------------------------------------------

# Load environment variables from .env file
set -a
[ -f .env ] && . .env
set +a

# Get uid and gid
export UID
export GID=$(id -g)

# Stop and remove existing containers
docker compose --profile "*" down -v --remove-orphans

echo "=== Successfully removed InfDB. ==="
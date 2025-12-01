#!/usr/bin/env bash
# set -euo pipefail

# ----------------------------------------------------------------------
# infDB Remove Script
# ----------------------------------------------------------------------

# Load environment variables from .env file
set -a
[ -f .env ] && . .env
set +a

# Stop and remove existing containers
docker compose --profile "*" down -v --remove-orphans

# Remove associated volumes
echo "=== Removing data directory at ${BASE_PATH_BASE}/${BASE_NAME} ==="
rm -rf "${BASE_PATH_BASE}/${BASE_NAME}"

echo "=== Successfully removed InfDB. ==="
#!/usr/bin/env bash
# set -euo pipefail

# ----------------------------------------------------------------------
# infDB Remove Script
# ----------------------------------------------------------------------

# Stop and remove existing containers
docker compose --profile "*" down -v --remove-orphans

echo "=== Successfully removed InfDB. ==="
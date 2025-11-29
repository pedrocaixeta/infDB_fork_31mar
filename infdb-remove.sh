#!/usr/bin/env bash
# set -euo pipefail

# ----------------------------------------------------------------------
# infDB Remove Script
# ----------------------------------------------------------------------

# Stop and remove existing containers
docker compose down -v --remove-orphans

echo "=== Done! InfDB has been removed. ==="
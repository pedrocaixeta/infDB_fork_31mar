#!/usr/bin/env bash
# set -euo pipefail

# ----------------------------------------------------------------------
# infDB Setup Script
# ----------------------------------------------------------------------
if [ ! -f configs/config-infdb.yml ]; then
    echo "=== Copy config-infdb.yml from template ==="
    cp configs/config-infdb.yml.template configs/config-infdb.yml
fi

# Pull latest images
echo "=== Pull latest docker images ==="
docker compose pull

# Create infDB docker setup
echo "=== Create infDB setup compose file ==="
docker compose -f services/infdb-setup/compose.yml up

# Stop and remove existing containers
docker compose -f compose.yml down -v --remove-orphans

echo "=== Run infDB ==="
docker compose -f compose.yml up -d

echo "=== Done! InfDB is ready. ==="

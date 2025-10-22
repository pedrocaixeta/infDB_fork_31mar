#!/usr/bin/env bash
# set -euo pipefail

# ----------------------------------------------------------------------
# infDB Setup Script
# ----------------------------------------------------------------------
if [ ! -f configs/config-infdb.yml ]; then
    echo "=== Copy config-infdb.yml from template ==="
    cp configs/config-infdb.yml.template configs/config-infdb.yml
fi

docker compose -f services/infdb-setup/compose.yml up

echo "=== Run infDB ==="
docker compose -f compose.yml up -d

echo "=== Done! InfDB is ready. ==="

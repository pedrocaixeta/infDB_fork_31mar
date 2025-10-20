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

echo "=== Wait for core services to initialize ==="
sleep 5

echo "=== Run infDB-loader ==="
docker compose -f tools/infdb-loader/compose.yml up

echo "=== Run infdb-basedata ==="
docker compose -f tools/infdb-basedata/compose.yml up

echo "=== Run ro-heat ==="
docker compose -f tools/ro-heat/compose.yml up

echo "=== Run kwp ==="
docker compose -f tools/kwp/compose.yml up

echo "=== Done! InfDB is ready. ==="

#!/usr/bin/env bash
# set -euo pipefail

# ----------------------------------------------------------------------
# infDB with Linear Heat Density Setup Script
# ----------------------------------------------------------------------

echo "=== Run infDB-loader ==="
mkdir -p ../data/infdb-loader
docker compose -f tools/infdb-loader/compose.yml up

echo "=== Run infdb-basedata ==="
docker compose -f tools/infdb-basedata/compose.yml up

echo "=== Run ro-heat ==="
docker compose -f tools/ro-heat/compose.yml up

echo "=== Run kwp ==="
docker compose -f tools/kwp/compose.yml up

echo "=== Done! InfDB with linear heat density is ready. ==="

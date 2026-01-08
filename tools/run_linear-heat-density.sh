#!/usr/bin/env bash
# set -euo pipefail

# ----------------------------------------------------------------------
# infDB with Linear Heat Density Setup Script
# ----------------------------------------------------------------------
echo "Loading environment variables from .env file..."
set -a
[ -f $(dirname "$0")/../.env ] && . $(dirname "$0")/../.env
set +a

echo "=== Run infDB-loader ==="
bash infdb-import.sh

echo "=== Run infdb-basedata ==="
bash tools/infdb-basedata/run.sh up

echo "=== Run ro-heat ==="
bash tools/ro-heat/run.sh up

echo "=== Run kwp ==="
bash tools/kwp/run.sh up

echo "=== Done! InfDB with linear heat density is ready. ==="

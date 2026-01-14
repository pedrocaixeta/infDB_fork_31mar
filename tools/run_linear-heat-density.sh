#!/usr/bin/env bash
# set -euo pipefail

# ----------------------------------------------------------------------
# infDB with Linear Heat Density Setup Script
# ----------------------------------------------------------------------
echo "Loading environment variables from .env file..."
set -a
[ -f $(dirname "$0")/../.env ] && . $(dirname "$0")/../.env
set +a

# echo "=== Run infDB-loader ==="
# bash infdb-import.sh

echo "=== Run infdb-basedata ==="
bash $(dirname "$0")/infdb-basedata/run.sh

echo "=== Run ro-heat ==="
bash $(dirname "$0")/ro-heat/run.sh

echo "=== Run kwp ==="
bash $(dirname "$0")/kwp/run.sh

echo "=== Done! InfDB with linear heat density is ready. ==="

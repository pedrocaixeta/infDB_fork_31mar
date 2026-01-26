#!/usr/bin/env bash

# ----------------------------------------------------------------------
# infDB with pylovo generation
# ----------------------------------------------------------------------

set -e

# Get the script directory
SCRIPT_DIR="$(dirname "$0")"

# Load environment variables from root .env file
echo "Loading environment variables from .env file..."
set -a
[ -f "$SCRIPT_DIR/../.env" ] && . "$SCRIPT_DIR/../.env"
set +a

echo "=== Run infdb-basedata ==="
bash "$SCRIPT_DIR/infdb-basedata/run.sh"

echo "=== Run pylovo-generation ==="
docker compose -f "$SCRIPT_DIR/pylovo-generation/compose.yml" run --rm --build pylovo-generation

echo "=== Done! InfDB with pylovo grids is ready. ==="

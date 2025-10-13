#!/usr/bin/env bash
# set -euo pipefail

# ----------------------------------------------------------------------
# infDB Setup Script
# ----------------------------------------------------------------------
# Configure your instance name here
BRANCH_NAME="develop"
# ----------------------------------------------------------------------

echo "=== Checkout branch '${BRANCH_NAME}' ==="
git checkout "${BRANCH_NAME}"
# git submodule update --init --recursive

echo "=== Setup infDB (generate config files) ==="
docker compose -f services/infdb-setup/compose.yml up

echo "=== Run infDB ==="
docker compose -f compose.yml up -d

echo "=== Run infDB-loader ==="
docker compose -f tools/infdb-loader/compose.yml up

echo "=== Run infdb-basedata ==="
docker compose -f tools/infdb-basedata/compose.yml up

echo "=== Run ro-heat ==="
docker compose -f tools/ro-heat/compose.yml up

echo "=== Run kwp ==="
docker compose -f tools/kwp/compose.yml up

echo "=== Done! Instance '${INSTANCE_NAME}' on branch '${BRANCH_NAME}' is ready. ==="

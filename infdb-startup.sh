#!/usr/bin/env bash
# set -euo pipefail

# ----------------------------------------------------------------------
# infDB Setup Script
# ----------------------------------------------------------------------

# # Delete .env file if it exists for testing purposes
# if [ -f .env ]; then
#     echo "=== Deleting .env file ==="
#     rm .env
# fi

# Check if .env file exists, if not create from template
if [ ! -f .env ]; then
    echo "=== Creating .env from template ==="
    cp .env.template .env
    echo "=== .env file created. Please review and customize it as needed. ==="
fi

# Load environment variables from .env file
set -a  # automatically export all variables
source .env
set +a

# # Pull latest images
echo "=== Pull latest docker images ==="
docker compose pull

# Create Postgres data directory if it doesn't exist
if [ ! -z "${SERVICES_POSTGRES_PATH_BASE}" ]; then
    echo "=== Making Postgres data directory ==="
    echo "${SERVICES_POSTGRES_PATH_BASE}"
    mkdir -p "${SERVICES_POSTGRES_PATH_BASE}"
fi

echo "=== Starting infDB ==="
docker compose up -d

echo "=== Successfully started InfDB. ==="


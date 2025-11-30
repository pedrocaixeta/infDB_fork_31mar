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
# Use export to make them available to docker compose
if [ -f .env ]; then
    export $(grep -v '^#' .env | grep -v '^$' | xargs)
fi

# # Pull latest images
echo "=== Pull latest docker images ==="
docker compose pull

# Create Postgres data directory if it doesn't exist
echo "=== Ensuring data directory exists at ${BASE_PATH_BASE}/${BASE_NAME} ==="
mkdir -p "${BASE_PATH_BASE}/${BASE_NAME}"


echo "=== Starting infDB ==="
docker compose up -d

echo "=== Successfully started InfDB. ==="


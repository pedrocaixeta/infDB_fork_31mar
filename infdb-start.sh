#!/usr/bin/env bash
# set -euo pipefail

# ----------------------------------------------------------------------
# infDB startup script
# ----------------------------------------------------------------------

# Check if .env file exists, if not create from template
if [ ! -f .env ]; then
    echo "=== Creating .env from template ==="
    cp .env.template .env
    echo "=== .env file created. Please review and customize it as needed. ==="
fi

# Load environment variables from .env file
set -a
[ -f .env ] && . .env
set +a

# Get uid and gid
export UID
export GID=$(id -g)

# Pull latest images
echo "=== Pull latest docker images ==="
docker compose pull

echo "=== Starting infDB ==="
docker compose "$@" #up -d --build

# echo "=== Importing data infDB ==="
# bash infdb-import.sh

echo "=== Successfully started InfDB. ==="


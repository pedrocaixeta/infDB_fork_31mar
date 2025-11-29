#!/usr/bin/env bash
set -e

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Navigate to the script directory so relative paths work as expected
cd "$SCRIPT_DIR"

# Check if .env file exists
if [ -f .env ]; then
    # Load environment variables
    set -a
    source .env
    set +a
fi

# Create data directory if INFDB_LOADER_DATA_PATH is set
if [ ! -z "${INFDB_LOADER_DATA_PATH}" ]; then
    echo "=== Ensuring data directory exists: ${INFDB_LOADER_DATA_PATH} ==="
    mkdir -p "${INFDB_LOADER_DATA_PATH}"
fi

# Run Docker Compose
echo "=== Starting infdb-loader ==="
docker compose up

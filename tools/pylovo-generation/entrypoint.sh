#!/bin/bash
set -e

# ==============================================================================
# Map root .env variables to pylovo expected variables BEFORE Python runs
# The env_file in compose.yml loads SERVICES_POSTGRES_* variables from ../../.env
# We must export them before any Python code runs that imports pylovo modules
# ==============================================================================
export DBNAME="${SERVICES_POSTGRES_DB}"
export DBUSER="${SERVICES_POSTGRES_USER}"
export HOST="${SERVICES_POSTGRES_HOST}"
export PORT="${SERVICES_POSTGRES_EXPOSED_PORT}"
export PASSWORD="${SERVICES_POSTGRES_PASSWORD}"
export TARGET_SCHEMA="${TARGET_SCHEMA:-pylovo}"
export INFDB_SOURCE_SCHEMA="${INFDB_SOURCE_SCHEMA:-basedata}"

echo "Database connection settings:"
echo "  DBNAME: $DBNAME"
echo "  DBUSER: $DBUSER"
echo "  HOST: $HOST"
echo "  PORT: $PORT"
echo "  TARGET_SCHEMA: $TARGET_SCHEMA"
echo ""

# Extract AGS (municipality code) from pylovo config
echo "Getting AGS from pylovo config..."
AGS=$(grep 'ags:' /app/configs/config-pylovo-generation.yml | sed -E 's/.*ags:[[:space:]]*"?([0-9]+)"?.*/\1/')

if [ -z "$AGS" ]; then
    echo "Error: Could not extract AGS from config file"
    exit 1
fi

echo "AGS from config: $AGS"

# Change to pylovo directory where package is installed
cd /app/pylovo

# Ensure pylovo-setup has been run (in case it failed during build)
echo "Set up pylovo database..."
uv run pylovo-setup

# Generate synthetic grids for the municipality
echo "Generating synthetic grids for AGS: $AGS..."
uv run pylovo-generate --ags $AGS

echo "Grid generation completed successfully!"

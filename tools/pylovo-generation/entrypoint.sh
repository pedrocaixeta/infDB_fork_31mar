#!/bin/bash
set -e

# ==============================================================================
# Load schema configuration from config-pylovo-generation.yml
# ==============================================================================
CONFIG_FILE="/app/configs/config-pylovo-generation.yml"
# Extract input_schema and output_schema from config file
INPUT_SCHEMA=$(grep -A 5 'data:' "$CONFIG_FILE" | grep 'input_schema:' | sed 's/.*input_schema:[[:space:]]*//' | sed 's/#.*//' | tr -d '"' | xargs)
OUTPUT_SCHEMA=$(grep -A 5 'data:' "$CONFIG_FILE" | grep 'output_schema:' | sed 's/.*output_schema:[[:space:]]*//' | sed 's/#.*//' | tr -d '"' | xargs)


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
export TARGET_SCHEMA="${OUTPUT_SCHEMA}"
export INFDB_SOURCE_SCHEMA="${INPUT_SCHEMA}"

echo "Database connection settings:"
echo "  DBNAME: $DBNAME"
echo "  DBUSER: $DBUSER"
echo "  HOST: $HOST"
echo "  PORT: $PORT"
echo "  TARGET_SCHEMA: $TARGET_SCHEMA"
echo ""

# ==============================================================================
# Get AGS codes - either from environment variable or interactive selection
# ==============================================================================

# Option 1: User-specified AGS via environment variable (for automation)
if [ -n "$PYLOVO_AGS" ]; then
    echo "Using AGS from PYLOVO_AGS environment variable: $PYLOVO_AGS"
    AGS_LIST="$PYLOVO_AGS"

# Option 2: Interactive selection from database
else
    echo "Fetching available AGS codes from opendata.scope table..."
    # Get all available AGS codes from database (disable set -e to handle errors)
    set +e
    AVAILABLE_AGS=$(psql "postgresql://$DBUSER:$PASSWORD@$HOST:$PORT/$DBNAME" -t -c "SELECT \"AGS\" FROM opendata.scope WHERE \"AGS\" IS NOT NULL" 2>&1)
    QUERY_EXIT_CODE=$?
    set -e

    # Clean up the result and remove leading zeros
    AVAILABLE_AGS=$(echo "$AVAILABLE_AGS" | tr -d ' ' | grep -v '^$' | sed 's/^0*//')



    # Prompt user for selection
    echo "Enter AGS codes to process as follows:"
    echo "  - Single AGS: AGS"
    echo "  - Multiple AGS (comma-separated): AGS1,AGS2"
    echo "  - All available: all"
    # Display available AGS codes (without leading zeros)
    echo "=========================================="
    echo "Available municipalities (AGS codes):"
    echo "$AVAILABLE_AGS"
    echo "=========================================="
    echo ""
    read -p "Your selection: " USER_INPUT

    # Process user input
    if [ "$USER_INPUT" = "all" ]; then
        AGS_LIST=$(echo "$AVAILABLE_AGS" | tr '\n' ',' | sed 's/,$//')
        echo "Selected: All municipalities ($AGS_LIST)"
    elif [ -n "$USER_INPUT" ]; then
        AGS_LIST="$USER_INPUT"
        echo "Selected: $AGS_LIST"
    else
        echo "Error: No AGS codes entered."
        exit 1
    fi
fi

# Change to pylovo directory where package is installed
cd /app/pylovo

# Ensure pylovo-setup has been run
echo ""
echo "=========================================="
echo "Setting up pylovo database..."
echo "=========================================="
uv run pylovo-setup

# Generate synthetic grids for all municipalities (pylovo handles the list internally)
echo ""
echo "=========================================="
echo "Generating synthetic grids for AGS: $AGS_LIST"
echo "=========================================="
uv run pylovo-generate --ags "$AGS_LIST"

echo ""
echo "=========================================="
echo "Grid generation completed!"
echo "=========================================="


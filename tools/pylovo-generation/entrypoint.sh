#!/bin/bash
set -e

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

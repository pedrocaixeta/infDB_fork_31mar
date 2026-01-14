#!/bin/bash
set -e

# Install infdb package from mounted directory
echo "Installing infdb package..."
cd /app/pylovo
uv pip install /app/mnt/infdb-package

# Extract AGS (municipality code) from pylovo config
echo "Getting AGS from pylovo config..."
AGS=$(uv run python -c "
import yaml
with open('/app/configs/config-pylovo-generation.yml', 'r') as f:
    config = yaml.safe_load(f)
    ags = config['data']['ags']
    print(ags)
")

echo "AGS from config: $AGS"

# Setup pylovo database
echo "Setting up pylovo database..."
uv run pylovo-setup

# Generate synthetic grids for the municipality
echo "Generating synthetic grids for AGS: $AGS..."
uv run pylovo-generate --ags $AGS

echo "Grid generation completed successfully!"

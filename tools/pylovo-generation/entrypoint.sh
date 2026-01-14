#!/bin/bash
set -e

# Install infdb package from mounted directory
echo "Installing infdb package..."
cd /app/pylovo
uv pip install /app/mnt/infdb-package

# Extract AGS (municipality code) from InfDB configuration
# Remove leading zero as pylovo expects AGS without it
echo "Getting AGS from InfDB config..."
AGS=$(uv run python -c "
from infdb import InfDB
infdb = InfDB(tool_name='pylovo-generation')
ags = infdb.get_value(['base', 'scope'])
# Remove leading zero by converting to int and back to string
print(str(int(ags)))
")

echo "AGS from config: $AGS"

# Setup pylovo database
echo "Setting up pylovo database..."
uv run pylovo-setup

# Generate synthetic grids for the municipality
echo "Generating synthetic grids for AGS: $AGS..."
uv run pylovo-generate --ags $AGS

echo "Grid generation completed successfully!"

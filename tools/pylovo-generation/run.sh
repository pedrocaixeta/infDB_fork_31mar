#!/bin/bash
set -e

# echo "Starting docker compose..."
docker compose -f "$(dirname "$0")/compose.yml" up --build
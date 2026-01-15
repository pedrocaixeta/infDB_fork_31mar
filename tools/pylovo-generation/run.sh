#!/bin/bash
# Run this script from the same directory, e.g. bash run.sh up
# e.g.: bash run.sh up, bash run.sh up --build, bash run.sh down, bash run.sh stop, ...
set -e

echo "Starting docker compose..."
docker compose -f "$(dirname "$0")/compose.yml" run --rm --build pylovo-generation
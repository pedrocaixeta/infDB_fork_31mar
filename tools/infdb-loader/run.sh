#!/bin/bash
# Run this script from the same directory, e.g. bash run.sh up
# Small wrapper script to export required environment variables into shell
# all arguments are passed to docker compose
# e.g.: bash run.sh up, bash run.sh up --build, bash run.sh down, bash run.sh stop, ...
set -e

echo "Loading environment variables from .env file..."
set -a
[ -f ../../.env ] && . ../../.env
set +a


echo "Starting docker compose..."
docker compose "$@"

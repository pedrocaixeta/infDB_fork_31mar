#!/bin/bash

echo "Loading environment variables from infDB .env file..."
set -a
[ -f $(dirname "$0")/../.env ] && . $(dirname "$0")/../.env
set +a

echo "Loading environment variables from local .env file..."
set -a
[ -f $(dirname "$0")/.env ] && . $(dirname "$0")/.env
set +a

usage() {
    echo "Usage: $0 (-p <profile> | -t <tool>)"
    echo "  -p <profile>  Run profile (e.g., linear)"
    echo "  -t <tool>     Run tool"
    exit 1
}

# Check for required parameters
if [[ $# -ne 3 ]]; then
    usage
fi

# Extract NAME and additional parameters
NAME="$2"
PARAM="$3"
OPTIONS="$4"

case "$1" in
    -p)        
        echo "Running profile: $NAME"

        PROJECT="infdb_${NAME}_${PARAM}"
        # Use the shared infdb network for all tool runs
        export INFDB_NETWORK="${INFDB_NETWORK:-infdb-infdb-demo_network}"
        export AGS="$PARAM"
        docker compose -f "$(dirname "$0")/compose.yml" \
            -p "$PROJECT" \
            --profile "$NAME" up\
            --remove-orphans
        # Stop and remove containers, networks, images, and volumes created by up
        docker compose -f "$(dirname "$0")/compose.yml" \
            -p "$PROJECT" \
            --profile "$NAME" down\
            --volumes --remove-orphans  # --rmi all
        ;;
    -t)
        echo "Running tool: $NAME"
        
        export AGS="$PARAM"
        docker compose -f "$(dirname "$0")/compose.yml" up --no-deps --remove-orphans $OPTIONS "$NAME" 
        docker compose -f "$(dirname "$0")/compose.yml" down --volumes --remove-orphans "$NAME"
        ;;
    *)
        usage
        ;;
esac
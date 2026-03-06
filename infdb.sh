#!/usr/bin/env bash
set -e

# ----------------------------------------------------------------------
# infDB command script
# Usage:
#   ./infdb.sh start [docker compose args]
#   ./infdb.sh import [docker compose args]
#   ./infdb.sh stop
#   ./infdb.sh remove
# ----------------------------------------------------------------------

# Ensure relative path works
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

print_usage() {
    cat <<'EOF'
Usage:
  ./infdb.sh start [docker compose args]
  ./infdb.sh import [docker compose args]
  ./infdb.sh stop
  ./infdb.sh remove

Examples:
  ./infdb.sh start -d
  ./infdb.sh import --build
  ./infdb.sh stop
  ./infdb.sh remove
EOF
}

ensure_from_template() {
    local target_file="$1"
    local template_file="$2"

    if [ ! -f "$target_file" ]; then
        echo "=== Creating $target_file from template ==="
        cp "$template_file" "$target_file"
        echo "=== $target_file file created. Please review and customize it as needed. ==="
    fi
}

cmd_start() {
    echo "=== Pull latest docker images ==="
    docker compose pull --ignore-buildable

    echo "=== Starting infDB ==="
    if [ "$#" -eq 0 ]; then
        docker compose up -d --pull never
    else
        docker compose up --pull never "$@"
    fi

    echo "=== Successfully started InfDB. ==="
}

cmd_import() {
    ensure_from_template "configs/config-infdb-import.yml" "configs/config-infdb-import.yml.template"
    echo "=== Importing data ==="
    docker compose --profile "import" up "$@"
}

cmd_stop() {
    echo "=== Stopping infDB ==="
    docker compose --profile "*" down
    echo "Successfully stopped all InfDB services."
}

cmd_remove() {
    echo "=== Removing service $1 including data  ==="
    docker compose --profile "$1" down -v --remove-orphans
}

if [ $# -lt 1 ]; then
    print_usage
    exit 1
fi

ensure_from_template ".env" ".env.template"
export UID GID="$(id -g)"

COMMAND="$1"
shift
case "$COMMAND" in
    start)
        cmd_start "$@"
        ;;
    import)
        cmd_import "$@"
        ;;
    stop)
        cmd_stop "$@"
        ;;
    remove)
        cmd_remove "$@"
        ;;
    -h|--help|help)
        print_usage
        ;;
    *)
        echo "Unknown command: $COMMAND"
        echo
        print_usage
        exit 1
        ;;
esac

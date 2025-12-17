#!/bin/bash
#
# Toolchain Runner for buildings-to-street -> linear-heat-density
#
# This script runs the buildings-to-street and linear-heat-density tools in sequence,
# validating configurations before execution.
#

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOOLS_DIR="$SCRIPT_DIR"

# Tool directories
BUILDINGS_TO_STREET_DIR="$TOOLS_DIR/buildings-to-street"
LINEAR_HEAT_DENSITY_DIR="$TOOLS_DIR/linear-heat-density"

# Config files
BUILDINGS_TO_STREET_CONFIG="$BUILDINGS_TO_STREET_DIR/configs/config-buildings-to-street.yml"
LINEAR_HEAT_DENSITY_CONFIG="$LINEAR_HEAT_DENSITY_DIR/configs/config-linear-heat-density.yml"

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to extract YAML value (simple parser for our use case)
get_yaml_value() {
    local file=$1
    local key_path=$2

    # Use Python for reliable YAML parsing
    python3 -c "
import yaml
import sys

try:
    with open('$file', 'r') as f:
        config = yaml.safe_load(f)

    keys = '$key_path'.split('.')
    value = config
    for key in keys:
        value = value[key]

    print(value if value is not None else '')
except Exception as e:
    sys.exit(1)
"
}

# Validate that required files exist
validate_files() {
    log_info "Validating required files..."

    if [ ! -f "$BUILDINGS_TO_STREET_CONFIG" ]; then
        log_error "buildings-to-street config file not found: $BUILDINGS_TO_STREET_CONFIG"
        exit 1
    fi

    if [ ! -f "$LINEAR_HEAT_DENSITY_CONFIG" ]; then
        log_error "linear-heat-density config file not found: $LINEAR_HEAT_DENSITY_CONFIG"
        exit 1
    fi

    if [ ! -f "$BUILDINGS_TO_STREET_DIR/compose.yml" ]; then
        log_error "buildings-to-street compose.yml not found"
        exit 1
    fi

    if [ ! -f "$LINEAR_HEAT_DENSITY_DIR/compose.yml" ]; then
        log_error "linear-heat-density compose.yml not found"
        exit 1
    fi

    log_success "All required files found"
}

# Validate Python/PyYAML availability
validate_dependencies() {
    log_info "Checking dependencies..."

    if ! command -v python3 &> /dev/null; then
        log_error "Python3 is required but not found. Please install Python3."
        exit 1
    fi

    if ! python3 -c "import yaml" 2>/dev/null; then
        log_error "PyYAML is required but not installed. Install with: pip install pyyaml"
        exit 1
    fi

    if ! command -v docker &> /dev/null; then
        log_error "Docker is required but not found. Please install Docker."
        exit 1
    fi

    log_success "All dependencies available"
}

# Validate configuration compatibility
validate_configs() {
    log_info "Validating configuration compatibility..."

    # Extract output schema and table from buildings-to-street
    local b2s_output_schema=$(get_yaml_value "$BUILDINGS_TO_STREET_CONFIG" "buildings-to-street.data.output.schema")
    local b2s_output_table=$(get_yaml_value "$BUILDINGS_TO_STREET_CONFIG" "buildings-to-street.data.output.table")

    if [ -z "$b2s_output_schema" ] || [ -z "$b2s_output_table" ]; then
        log_error "Could not read output schema/table from buildings-to-street config"
        exit 1
    fi

    log_info "buildings-to-street output: schema='$b2s_output_schema', table='$b2s_output_table'"

    # Extract input schema and table from linear-heat-density
    local lhd_input_schema=$(get_yaml_value "$LINEAR_HEAT_DENSITY_CONFIG" "linear-heat-density.data.input.buildings-to-streets.schema")
    local lhd_input_table=$(get_yaml_value "$LINEAR_HEAT_DENSITY_CONFIG" "linear-heat-density.data.input.buildings-to-streets.table")

    if [ -z "$lhd_input_schema" ] || [ -z "$lhd_input_table" ]; then
        log_error "Could not read buildings-to-streets input schema/table from linear-heat-density config"
        exit 1
    fi

    log_info "linear-heat-density input: schema='$lhd_input_schema', table='$lhd_input_table'"

    # Extract streets configuration from buildings-to-street
    local b2s_streets_schema=$(get_yaml_value "$BUILDINGS_TO_STREET_CONFIG" "buildings-to-street.data.streets.schema")
    local b2s_streets_table=$(get_yaml_value "$BUILDINGS_TO_STREET_CONFIG" "buildings-to-street.data.streets.table")
    local b2s_streets_id=$(get_yaml_value "$BUILDINGS_TO_STREET_CONFIG" "buildings-to-street.data.streets.id-column")
    local b2s_streets_geom=$(get_yaml_value "$BUILDINGS_TO_STREET_CONFIG" "buildings-to-street.data.streets.geom-column")

    if [ -z "$b2s_streets_schema" ] || [ -z "$b2s_streets_table" ]; then
        log_error "Could not read streets schema/table from buildings-to-street config"
        exit 1
    fi

    log_info "buildings-to-street streets: schema='$b2s_streets_schema', table='$b2s_streets_table', id-column='$b2s_streets_id', geom-column='$b2s_streets_geom'"

    # Extract streets configuration from linear-heat-density
    local lhd_streets_schema=$(get_yaml_value "$LINEAR_HEAT_DENSITY_CONFIG" "linear-heat-density.data.input.streets.schema")
    local lhd_streets_table=$(get_yaml_value "$LINEAR_HEAT_DENSITY_CONFIG" "linear-heat-density.data.input.streets.table")
    local lhd_streets_id=$(get_yaml_value "$LINEAR_HEAT_DENSITY_CONFIG" "linear-heat-density.data.input.streets.id-column")
    local lhd_streets_geom=$(get_yaml_value "$LINEAR_HEAT_DENSITY_CONFIG" "linear-heat-density.data.input.streets.geom-column")

    if [ -z "$lhd_streets_schema" ] || [ -z "$lhd_streets_table" ]; then
        log_error "Could not read streets schema/table from linear-heat-density config"
        exit 1
    fi

    log_info "linear-heat-density streets: schema='$lhd_streets_schema', table='$lhd_streets_table', id-column='$lhd_streets_id', geom-column='$lhd_streets_geom'"

    # Validate compatibility
    local config_error=0

    # Validate buildings-to-streets output/input match
    if [ "$b2s_output_schema" != "$lhd_input_schema" ]; then
        log_error "Schema mismatch!"
        log_error "  buildings-to-street output schema: '$b2s_output_schema'"
        log_error "  linear-heat-density input schema:  '$lhd_input_schema'"
        config_error=1
    fi

    if [ "$b2s_output_table" != "$lhd_input_table" ]; then
        log_error "Table mismatch!"
        log_error "  buildings-to-street output table: '$b2s_output_table'"
        log_error "  linear-heat-density input table:  '$lhd_input_table'"
        config_error=1
    fi

    # Validate streets configuration match
    if [ "$b2s_streets_schema" != "$lhd_streets_schema" ]; then
        log_error "Streets schema mismatch!"
        log_error "  buildings-to-street streets schema: '$b2s_streets_schema'"
        log_error "  linear-heat-density streets schema: '$lhd_streets_schema'"
        log_error "  → Fix in: $BUILDINGS_TO_STREET_CONFIG or $LINEAR_HEAT_DENSITY_CONFIG"
        config_error=1
    fi

    if [ "$b2s_streets_table" != "$lhd_streets_table" ]; then
        log_error "Streets table mismatch!"
        log_error "  buildings-to-street streets table: '$b2s_streets_table'"
        log_error "  linear-heat-density streets table: '$lhd_streets_table'"
        log_error "  → Fix in: $BUILDINGS_TO_STREET_CONFIG or $LINEAR_HEAT_DENSITY_CONFIG"
        config_error=1
    fi

    if [ "$b2s_streets_id" != "$lhd_streets_id" ]; then
        log_error "Streets id-column mismatch!"
        log_error "  buildings-to-street streets id-column: '$b2s_streets_id'"
        log_error "  linear-heat-density streets id-column: '$lhd_streets_id'"
        log_error "  → Fix in: $BUILDINGS_TO_STREET_CONFIG or $LINEAR_HEAT_DENSITY_CONFIG"
        config_error=1
    fi

    if [ "$b2s_streets_geom" != "$lhd_streets_geom" ]; then
        log_error "Streets geom-column mismatch!"
        log_error "  buildings-to-street streets geom-column: '$b2s_streets_geom'"
        log_error "  linear-heat-density streets geom-column: '$lhd_streets_geom'"
        log_error "  → Fix in: $BUILDINGS_TO_STREET_CONFIG or $LINEAR_HEAT_DENSITY_CONFIG"
        config_error=1
    fi

    if [ $config_error -eq 1 ]; then
        log_error "Configuration validation failed. Please fix the configuration mismatches above."
        exit 1
    fi

    log_success "Configuration validation passed"
}

# Run buildings-to-street
run_buildings_to_street() {
    log_info "========================================"
    log_info "Running buildings-to-street..."
    log_info "========================================"

    cd "$BUILDINGS_TO_STREET_DIR"

    if ! docker compose up --abort-on-container-exit; then
        log_error "buildings-to-street failed to complete successfully"
        exit 1
    fi

    # Clean up containers
    docker compose down

    log_success "buildings-to-street completed successfully"
}

# Run linear-heat-density
run_linear_heat_density() {
    log_info "========================================"
    log_info "Running linear-heat-density..."
    log_info "========================================"

    cd "$LINEAR_HEAT_DENSITY_DIR"

    if ! docker compose up --abort-on-container-exit; then
        log_error "linear-heat-density failed to complete successfully"
        exit 1
    fi

    # Clean up containers
    docker compose down

    log_success "linear-heat-density completed successfully"
}

# Main execution
main() {
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  Toolchain Runner${NC}"
    echo -e "${GREEN}  buildings-to-street → linear-heat-density${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""

    # Validation phase
    validate_dependencies
    validate_files
    validate_configs

    echo ""
    log_info "All validations passed. Starting toolchain execution..."
    echo ""

    # Execution phase
    run_buildings_to_street
    echo ""
    run_linear_heat_density

    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  Toolchain completed successfully!${NC}"
    echo -e "${GREEN}========================================${NC}"
}

# Run main function
main "$@"

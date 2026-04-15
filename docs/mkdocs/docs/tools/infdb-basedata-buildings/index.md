# InfDB-basedata-buildings
The tool `infDB-basedata-buildings` processes building-related data as fundamental data basis for various applications and analyses.

<!-- ## Contents
- Objective (Scope, Motivation)
- Architecture (Design, Implementation)
    - Structure (Project/Code)
    - Data Pipeline
    - Code (Classes and Functions)
    - Dependencies
- Usage (Quick Start, Requirements, Configuration) -->


## Usage
Details on how to use the tool can be found in the [Usage](usage.md) section.

## Architecture
The architecture of the `infDB-basedata-buildings` tool is based on the InfDB Dev Container template as described in the [Tools Template](../dev-container/index.md) section. The tool is implemented in Python and utilizes the `pyinfdb` package for interaction with infDB. The database operations are executed using SQL queries. More details can be found in the [Architecture](architecture.md) section.

## Data Pipeline
More details can be found in the [Data Pipeline](data-pipeline.md) section.

## Tool Overview
- Loads and cleans LOD2 building data for the chosen AGS (removing implausibly small buildings), additionally importing key attributes such as height, area, and floor count
- Distributes census population across buildings proportionally by volume, then converts occupant estimates to household counts using grid-cell-level average household sizes
- Assigns construction periods to buildings by sampling from census statistics per grid cell
- Classifies residential buildings into types (Single-family, Terraced, Multi-family, Apartment) based on area, number of floors, and neighbourhood patterns. Missing types are defaulted to Apartment Building, then rebalances type distributions against census totals per grid cell
- Attaches geographic weather data from OpenMeteo to each building
- Outputs a final enriched building dataset ready for downstream tools

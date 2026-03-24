# Usage

## Requirements Input Data
- LOD2 building data
- Census 2022

## Configuration
The tool can be configured via the configuration file `configs/config-infdb-basedata-buildings.yaml`. An example configuration is shown below:

```yaml title="configs/config-infdb-basedata-buildings.yaml"
infdb-basedata-buildings:
    config-infdb: "config-infdb.yml" # only filename - change path in ".env" file "CONFIG_INFDB_PATH"
    logging:
        path: "infdb-basedata-buildings.log"
        level: "INFO" # ERROR, WARNING, INFO, DEBUG
    hosts:
        postgres:
            user: None
            password: None
            db: None
            host: None
            exposed_port: None
            epsg: None # 3035 (Europe)
    data:
        input_schema: opendata  # (1)
        output_schema: basedata # (2)
        census_building_type_resolution: 1km #1km, 100m # (3)
        random_seed: 0.98   # (4)
```

1. Set the input schema where the raw data is stored
2. Set the output schema where the processed data will be stored
3. Set the building type resolution for the census data (e.g., 1km or 100m)
4. Set a random seed for reproducibility

## Run Single AGS
To run the tool for a single AGS, you can use the bash script `tools/tools.sh`:
```bash
bash tools/tools.sh -t infdb-basedata-buildings AGS
```
## Run Multiple AGS
The `run_ags.py` script allows you to run a profile or a single tool for multiple AGS in parallel. The script uses the `uv` to manage the python packages and dependencies.
```bash
# Single Tool
uv run python3 tools/run_ags.py -t infdb-basedata-buildings [-a AGS1,AGS2,... -n NUM_WORKERS -c]
```
More details about the parameters can be found in the [Tools](../index.md) section.

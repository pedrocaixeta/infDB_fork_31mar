# infdb-basedata

The `infdb-basedata` tool creates a fundamental data basis for infrastructure modeling, planning, and analysis by merging various opendata sources into consolidated datasets. The focus lies on building information on the building level.

**Data Sources Used**
    
- [Census of Germany 2022 (Zensus 2022)](https://www.destatis.de/DE/Themen/Gesellschaft-Umwelt/Bevoelkerung/Zensus2022/_inhalt.html)
- [Official geogrids](https://gdz.bkg.bund.de/index.php/default/geographische-gitter-fur-deutschland-in-utm-projektion-geogitter-national.html) by the [Federal Agency for Cartography and Geodesy Germany (BKG)](https://www.bkg.bund.de/)
- [3D building models LoD2 Germany (LoD2-DE)](https://gdz.bkg.bund.de/index.php/default/3d-gebaudemodelle-lod2-deutschland-lod2-de.html) by the [Federal Agency for Cartography and Geodesy Germany (BKG)](https://www.bkg.bund.de/)

## Key Features

- **Consolidation**: Validates data integrity and types
- **Disaggregation**: Sophisticated geographical disaggregation of statistical information with higher spatial resolution
- **Validation**: Comparison of generated synthetic building data with official statistics for accuracy



## Output Data

The output of the tool are merged and consolidated datasets containing building data with attributes such as:

- Building type (residential, commercial, industrial)
- Year of construction
- Number of floors
- Number of households
- Number of inhabitants
- Total floor area
- Building volume
- Energy performance indicators (if available)





## Usage
If you want to run the tool to create the merged datasets, please execute:
```bash
bash tools/infdb-basedata/run.sh
```

## Output

The output datasets are stored in the `basedata` schema of the infDB PostgreSQL database. The main tables created are:

- `basedata.buildings`: Contains detailed building information with attributes such as type, construction year, number of floors, households, inhabitants, floor area, and volume.
- `basedata.buildings_grid`: Contains building data from Zensus on a grid level as basis for spatial redistribution.


# Configuration 
The configuration of the tool can be done via the configuration YAML file:
```bash title="configs/config-infdb-basedata.yml"
infdb-basedata:
    config-infdb: "config-infdb.yml" # only filename - change path in ".env" file "CONFIG_INFDB_PATH"
    logging:
        path: "infdb-basedata.log"
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
        output_schema: basedata   # (2) 
```

1. Specify the schema where the opendata comes from.
2. Specify the schema where the data should be stored.
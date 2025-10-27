# Tools Directory

## Setup infDB-loader

The configuration can be done via [tools/indfb-loader/configs/config-loader.yml](tools/indfb-loader/configs/config-loader.yml)
```yaml
loader:
    name: demo-sonthofen
    scope:  # AGS (Amtlicher Gemeindeschlüssel)
        # - "09162000"  # Munich
        - "09780139"  # Sonthofen
        # - "09780116"  # Bolsterlang
        # - "09162000" # M
        # - "09185149" # ND
        # - "09474126" # FO
        # - "09261000" # LA
    multiproccesing: 
        status: not-active
        max_cores: 2    # max cores since of memory limitations to 2
    config-infdb: "config-infdb.yml" # only filename - change path in ".env" file "CONFIG_INFDB_PATH"
    path:
        base: "data" # only foldername - change path in ".env" file "LOADER_DATA_PATH"
        opendata: "{loader/path/base}/opendata/"
        processed: "{loader/path/base}/{loader/name}"
    logging:
        path: "{loader/path/base}/loader.log"
        level: "DEBUG" # ERROR, WARNING, INFO, DEBUG
    hosts:
        postgres:
            user: None
            password: None
            db: None
            host: None
            exposed_port: None
            epsg: None # 3035 (Europe)
    sources:
        package:
            status: active
            url: http://ds1.need.energy:8123/opendata.zip
            path: 
                base: "{loader/path/base}"
                processed: "{loader/path/opendata}"

        lod2:
            status: active
            url:
                - "https://geodaten.bayern.de/odd/a/lod2/citygml/meta/metalink/#scope.meta4"    #scope placeholder for AGS
            path:
                lod2: "{loader/path/opendata}/lod2/"
                gml: "{loader/path/opendata}/lod2/{loader/name}"
        ...
```

**Hint:** In case you move the infdb-loader source folder outside of the folder tools in repo or want to change the location where the downloaded data is stored, the paths to data and to configs folder need to be defined in [.env](.env)
```bash
CONFIG_INFDB_PATH=../infdb/configs  # Change if you moved the "configs" folder
LOADER_DATA_PATH=./     # Change if you moved the "data" folder
```

Once you adjusted the configuration files with the command above, you need to finally start the infDB-loader and start importing:

### Run infDB-loader
```bash
docker compose -f tools/infdb-loader/compose.yml up
```

### Remove LOD2 data
```bash
docker run --rm --add-host=host.docker.internal:host-gateway 3dcitydb/citydb-tool delete --delete-mode=delete -H host.docker.internal -d citydb -u citydb_user -p citydb_password -P 5432
```

## Linear Heat Density
To execute all steps for Linear Heat Density calculation, run the following script:
```bash
./tools/run_linear-heat-density.sh
```

Alternatively, you can run each step separately:

### Run infdb-basedata
```bash
docker compose -f tools/infdb-basedata/compose.yml up
```

### Run ro-heat
```bash
docker compose -f tools/ro-heat/compose.yml up
```

### Run kwp
```bash
docker compose -f tools/kwp/compose.yml up
```
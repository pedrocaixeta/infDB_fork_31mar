# Process data

## Prequires
- Docker
- Have database set up with open data (see `tools/infdb-loader`)


### Source Code
- The folder contains source code to create three tables in the `pylovo_input`:
  - `configs` configuration files
  - `sql` sql files to run
  - `src` python source files
  - `main.py` entry point


### Process data
Paths in .env
```yml
CONFIG_INFDB_PATH=../../configs/  # Path to the configs directory of infDB
```
Settings in configs/config-preprocessor.yml
```bash
    # on linux and macos
    bash run.sh up
```

### Resetting tables / basedata-schema
To remove the tables created by infdb-basedata, run the following command:
```SQL
DELETE FROM public.databasechangelog WHERE labels like '%infdb-basedata%';
```
Then, the next run of infdb-basedata triggers liquibase to drop the basedata-schema and recreate all tables, index and constraints.
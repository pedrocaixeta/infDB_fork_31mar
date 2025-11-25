import time
import numpy as np
import pandas as pd
from infdb import InfDB
from entise.core.generator import TimeSeriesGenerator

from src import basic_refurbishment
from src import eureca_code
from src import timedata
import os
import io
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from sqlalchemy import text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Parameters
rng = np.random.default_rng(seed=42)
end_of_simulation_year = 2025
simulation_year = 2024
construction_year_col = "construction_year"
schema = "ro_heat"


def _upsert_metadata_and_get_ids(engine, infdblog, output_schema, objectids):
    """Upsert metadata for all series and return mapping (name, objectid, source) -> id using
    PostgreSQL dialect insert with on_conflict_do_update + returning (Fix A).
    """
    series_defs = [
        ("ro_heat_indoor_temperature", "Indoor temperature for building", "synthetic", "°C"),
        ("ro_heat_heating_load", "Heating load for building", "synthetic", "W"),
        ("ro_heat_cooling_load", "Cooling load for building", "synthetic", "W"),
    ]

    # Build values
    records = []
    for objectid in objectids:
        for name, description, typ, unit in series_defs:
            records.append({
                "name": name,
                "description": description,
                "type": typ,
                "unit": unit,
                "changelog": 0,
                "objectid": str(objectid),
                "source": "ro-heat",
            })

    if not records:
        return {}

    # Reflect table and construct dialect-aware upsert with RETURNING
    md = MetaData()
    meta_table = Table("entise_ts_metadata", md, schema=output_schema, autoload_with=engine)

    meta_map = {}

    batch_size = 1000
    with engine.begin() as conn:
        for start in range(0, len(records), batch_size):
            batch = records[start:start + batch_size]
            insert_stmt = pg_insert(meta_table).values(batch)
            upsert_stmt = (
                insert_stmt.on_conflict_do_update(
                    index_elements=[meta_table.c.name, meta_table.c.objectid, meta_table.c.source],
                    set_={
                        "unit": insert_stmt.excluded.unit,
                        "type": insert_stmt.excluded.type,
                        "description": insert_stmt.excluded.description,
                    },
                )
                .returning(meta_table.c.id, meta_table.c.name, meta_table.c.objectid, meta_table.c.source)
            )
            res = conn.execute(upsert_stmt)
            for row in res.mappings():
                meta_map[(row["name"], row["objectid"], row["source"])] = row["id"]

    infdblog.info(
        f"Upserted metadata for {len(objectids)} buildings; total series rows: {len(records)}"
    )
    return meta_map


def main():
    # Load InfDB handler
    infdbhandler = InfDB(tool_name="ro-heat")

    # Database connection
    infdbclient_citydb = infdbhandler.connect()

    # Logger setup
    infdblog = infdbhandler.get_log()

    # Start message
    infdblog.info(f"Starting {infdbhandler.get_toolname()} tool")

    # Setup database engine
    engine = infdbclient_citydb.get_db_engine()

    # Get configuration values
    input_schema = infdbhandler.get_config_value(["ro-heat", "data", "input_schema"])
    output_schema = infdbhandler.get_config_value(["ro-heat", "data", "output_schema"])

    try:

        sql = f"DROP SCHEMA IF EXISTS {output_schema} CASCADE;"
        infdbclient_citydb.execute_query(sql)
        sql = f"CREATE SCHEMA IF NOT EXISTS {output_schema};"
        infdbclient_citydb.execute_query(sql)
        infdblog.info(f"output schema: {output_schema} created successfully")

        SQL_QUERY = f"""
                    DROP TABLE IF EXISTS {output_schema}.temp_rc_calculation CASCADE;

                    CREATE TABLE {output_schema}.temp_rc_calculation AS
                    WITH wall_data AS (SELECT building_objectid,
                                              SUM(area) AS wall_surface_area
                                       FROM (SELECT regexp_replace(f.objectid, '_[^_]*-.*$', '') AS building_objectid,
                                                    CAST(p.val_string AS double precision)       AS area
                                             FROM feature f
                                                      JOIN geometry_data gd ON f.id = gd.feature_id
                                                      JOIN property p ON gd.feature_id = p.feature_id
                                             WHERE f.objectclass_id = 709 -- WallSurface
                                               AND p.name = 'Flaeche') sub
                                       GROUP BY building_objectid),
                         roof_data AS (SELECT building_objectid,
                                              SUM(area) AS roof_surface_area
                                       FROM (SELECT regexp_replace(f.objectid, '_[^_]*-.*$', '') AS building_objectid,
                                                    CAST(p.val_string AS double precision)       AS area
                                             FROM feature f
                                                      JOIN geometry_data gd ON f.id = gd.feature_id
                                                      JOIN property p ON gd.feature_id = p.feature_id
                                             WHERE f.objectclass_id = 712 -- RoofSurface
                                               AND p.name = 'Flaeche') sub
                                       GROUP BY building_objectid)

                    SELECT b.objectid                                                            AS building_objectid,
                           b.floor_area,
                           b.floor_number,
                           b.building_type,
                           b.construction_year,
                           -- Reduce wall surface by the assumed window area, see below
                           wd.wall_surface_area - b.floor_area * b.floor_number * 0.75 * 0.2 AS wall_area,
                           rd.roof_surface_area                                              AS roof_area,
                           -- Assume heated area = b.floor_area * b.floor_number * 0.75
                           -- Assume window area to be 0.2 m² per heated area ()
                           b.floor_area * b.floor_number * 0.75 * 0.2                        AS window_area
                    FROM {input_schema}.buildings b
                             LEFT JOIN wall_data wd ON b.objectid = wd.building_objectid
                             LEFT JOIN roof_data rd ON b.objectid = rd.building_objectid;

                    SELECT *
                    from {output_schema}.temp_rc_calculation
                    WHERE building_type IS NOT NULL \
                    """

        buildings = pd.read_sql(SQL_QUERY, engine)

        infdblog.debug(f"Loaded {len(buildings)} buildings from the database.")
        infdblog.debug(buildings.head())

        random_years = np.full(len(buildings), np.nan)

        # Define class-to-range mapping
        age_class_ranges = {
            "-1919": (1860, 1918),
            "1919-1948": (1919, 1948),
            "1949-1978": (1949, 1978),
            "1979-1990": (1979, 1990),
            "1991-2000": (1991, 2000),
            "2001-2010": (2001, 2010),
            "2011-2019": (2011, 2019),
            "2020-": (2020, end_of_simulation_year),
        }

        # For each class, find matching rows and assign random years
        for age_class, (start, end) in age_class_ranges.items():
            mask = buildings[construction_year_col] == age_class
            count = sum(mask)
            random_years[mask] = rng.integers(start, end, size=count, endpoint=True)

        buildings[construction_year_col] = random_years.astype(int)

        refurbishment_parameters = {
            "outer_wall": {
                "distribution": lambda gen, parameters: gen.normal(**parameters),
                "distribution_parameters": {"loc": 40, "scale": 10},
            },
            "rooftop": {
                "distribution": lambda gen, parameters: gen.normal(**parameters),
                "distribution_parameters": {"loc": 50, "scale": 10},
            },
            "window": {
                "distribution": lambda gen, parameters: gen.normal(**parameters),
                "distribution_parameters": {"loc": 30, "scale": 10},
            },
        }

        infdblog.debug("Starting refurbishment simulation")
        refurbed_df = basic_refurbishment.simulate_refurbishment(
            buildings,
            end_of_simulation_year,
            refurbishment_parameters,
            rng,
            age_column=construction_year_col,
            provide_last_refurb_only=True,
        )
        infdblog.debug("Refurbishment simulation completed")
        infdblog.debug(refurbed_df.info())
        infdblog.debug(refurbed_df.head())

        infdbclient_citydb.execute_query("DROP TABLE IF EXISTS ro_heat.buildings_rc CASCADE")
        refurbed_df.to_sql(
            "buildings_rc", engine, if_exists="replace", schema=schema, index=False
        )
        infdblog.debug("Refurbished data writing to database")

        infdblog.debug("Starting construction of building elements")
        # Run SQL: 02_create_layer_view
        infdbclient_citydb.execute_sql_files("sql", ["02_create_layer_view.sql"])

        elements = pd.read_sql("""SELECT *
                                  FROM v_element_layer_data""", engine)

        # TODO: sort by layer_index according to EUReCA specification
        # TODO: Handling of windows
        infdblog.debug("Starting construction of building elements")
        elements = elements[elements["element_name"] != "Window"]

        elements["materials"] = elements.apply(
            lambda x: eureca_code.Material(
                x["name"], x["thickness"], x["thermal_conduc"], x["heat_capac"], x["density"]
            ),
            axis=1,
        )

        constructions = (
            elements.groupby(["building_objectid", "element_name", "area"])["materials"]
            .apply(list)
            .reset_index()
        )

        # Map tabula to EUReCA names
        tabula_eureca_element_name_mapping = {
            "GroundFloor": "GroundFloor",
            "OuterWall": "ExtWall",
            "Rooftop": "Roof",
        }

        constructions["construction_obj"] = constructions.apply(
            lambda row: eureca_code.Construction(
                name=f"B{row['building_objectid']}_{row['element_name']}",
                materials_list=row["materials"],
                construction_type=tabula_eureca_element_name_mapping[row["element_name"]],
            ),
            axis=1,
        )

        constructions["resistance"] = constructions.apply(
            lambda row: 1 / ((1 / row["construction_obj"].thermal_resistance) * row["area"]),
            axis=1,
        )

        constructions["capacitance"] = constructions.apply(
            lambda row: row["construction_obj"].k_int * row["area"], axis=1
        )

        rc_values = (
            constructions.groupby("building_objectid")[["capacitance", "resistance"]].sum().sort_values(
                "building_objectid")
        )

        bld2ts = timedata.get_bld2ts(database_connection=engine)

        all_ts_df = timedata.get_all_timeseries_data(database_connection=engine,
                                                     start=pd.Timestamp(f"{simulation_year}-01-01"),
                                                     end=pd.Timestamp(f"{simulation_year}-12-31"))
        all_ts_df.index.name = 'datetime'
        all_ts_df.rename(columns={"value": "air_temperature[C]"}, inplace=True)
        all_ts_df = all_ts_df.reset_index()
        data = {x: y.sort_index() for x, y in all_ts_df.groupby('ts_metadata_id')}

        # Preparation for EnTiSe
        entise_input = rc_values.reset_index().rename(columns={"building_objectid": "id"})
        entise_input = entise_input.rename(
            columns={'resistance': 'resistance[K W-1]', 'capacitance': 'capacitance[J K-1]'}, errors=True)
        entise_input["hvac"] = "1R1C"
        entise_input["min_temperature[C]"] = 20.0
        entise_input["max_temperature[C]"] = 24.0
        entise_input["gains_solar"] = 0.0

        entise_input = entise_input.merge(
            bld2ts[['bld_objectid', 'ts_metadata_id']].rename(columns={"ts_metadata_id": "weather"}),
            left_on="id",
            right_on="bld_objectid",
            how="left",
        ).drop(columns=["bld_objectid"])

        entise_input = entise_input.iloc[:500, :]

        # Initialize the generator
        gen = TimeSeriesGenerator()
        gen.add_objects(entise_input)

        # Generate time series and summary
        summary, dict_df = gen.generate(data, workers=os.cpu_count())

        # Summary
        summary.index.name = "building_objectid"
        summary.to_sql(
            "entise_summary",
            con=engine,
            if_exists="replace",
            schema=output_schema,
            index=True,
            method="multi",
            # chunksize=1000,
        )
        infdblog.info(summary.head())

        # Time Series
        infdblog.debug("Writing EnTiSe output time series to database")

        # Create metadata table if not exists
        metadata_sql = f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.entise_ts_metadata (
            id SERIAL PRIMARY KEY,
            name text,
            description text,
            grid_id text,
            type text,
            unit text,
            changelog integer,
            objectid text,
            source text
        );
        """
        try:
            infdbclient_citydb.execute_query(metadata_sql)
        except Exception as e:
            infdblog.error("Failed to create metadata table: %s", e)

        # Ensure unique constraint for metadata upserts
        try:
            unique_idx_sql = f"""
            CREATE UNIQUE INDEX IF NOT EXISTS entise_ts_metadata_uniq
            ON {output_schema}.entise_ts_metadata (name, objectid, source);
            """
            infdbclient_citydb.execute_query(unique_idx_sql)
        except Exception as e:
            infdblog.error("Failed to create unique index on metadata: %s", e)

        # Ensure table exists with appropriate types (grid_id, time, temperature, ts_id)

        table_name = 'entise_ts_data'

        # Try to ensure TimescaleDB extension is available (best-effort)
        try:
            infdbclient_citydb.execute_query("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        except Exception as e:
            infdblog.warning("Could not ensure timescaledb extension (continuing): %s", e)

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.{table_name} (
            ts_metadata_id integer,
            time timestamptz,
            value double precision
        )
        WITH (
            timescaledb.hypertable,
            timescaledb.partition_column='time',
            timescaledb.segmentby='ts_metadata_id'
        );
        """
        try:
            infdbclient_citydb.execute_query(create_sql)
        except Exception as e:
            infdblog.error("Failed to create timeseries table with Timescale hypertable syntax: %s", e)
            # Fallback: create as a plain Postgres table
            try:
                create_sql_plain = f"""
                CREATE TABLE IF NOT EXISTS {output_schema}.{table_name} (
                    ts_metadata_id integer,
                    time timestamptz,
                    value double precision
                );
                """
                infdbclient_citydb.execute_query(create_sql_plain)
                infdblog.info("Created plain Postgres timeseries table as fallback (no TimescaleDB features).")
            except Exception as e2:
                infdblog.error("Failed to create plain timeseries table: %s", e2)
                raise

        # Upload using baseline
        upload_start = time.perf_counter()
        upload_timeseries_baseline(
            engine=engine,
            output_schema=output_schema,
            table_name=table_name,
            dict_df=dict_df,
            infdblog=infdblog,
        )
        upload_dt = time.perf_counter() - upload_start
        infdblog.info(f"Baseline batch upload completed in {upload_dt:.2f} seconds")
        print(f"Baseline batch upload completed in {upload_dt:.2f} seconds")

        infdblog.info("Ro-heat successfully completed")

    except Exception as e:
        infdblog.error(f"Something went wrong: {str(e)}")
        raise e


def build_timeseries_df(dict_df, meta_map, infdblog):
    """Build a single DataFrame with all time series rows.

    Columns: ts_metadata_id, time, value
    """
    column_map = {
        "ro_heat_indoor_temperature": "indoor_temperature[C]",
        "ro_heat_heating_load": "heating:load[W]",
        "ro_heat_cooling_load": "cooling:load[W]",
    }

    frames = []
    total_series = 0

    for objectid, row in dict_df.items():
        hvac_df = row.get("hvac") if isinstance(row, dict) else row["hvac"]
        if hvac_df is None or hvac_df.empty:
            continue

        # assume hvac_df.index already datetime-like (as EnTiSe usually provides)
        idx = hvac_df.index

        for name, col in column_map.items():
            key = (name, str(objectid), "ro-heat")
            ts_id = meta_map.get(key)

            if ts_id is None or col not in hvac_df.columns:
                continue

            values = hvac_df[col].values
            if len(values) == 0:
                continue

            df_part = pd.DataFrame(
                {
                    "ts_metadata_id": ts_id,
                    "time": idx,
                    "value": values,
                }
            )
            frames.append(df_part)
            total_series += 1

    if not frames:
        infdblog.info("No time series rows to upload (frames list empty).")
        return pd.DataFrame(columns=["ts_metadata_id", "time", "value"]), 0

    ts_df = pd.concat(frames, ignore_index=True)

    # Ensure time is datetime with UTC tz; keep it simple for baseline
    ts_df["time"] = pd.to_datetime(ts_df["time"], utc=True, errors="coerce")
    ts_df = ts_df[ts_df["time"].notna()]

    infdblog.info(
        f"Built timeseries DataFrame with {len(ts_df)} rows across {total_series} series."
    )
    return ts_df, total_series


def upload_timeseries_baseline(engine, output_schema, table_name, dict_df, infdblog):
    """Baseline uploader:
       - upsert metadata
       - build a single DataFrame with all rows
       - single COPY into final table
       No staging, no chunking, no concurrency.
    """
    # 1) Upsert metadata
    objectids = [str(objid) for objid in dict_df.keys()]
    meta_start = time.perf_counter()
    meta_map = _upsert_metadata_and_get_ids(engine, infdblog, output_schema, objectids)
    meta_dt = time.perf_counter() - meta_start
    infdblog.info(f"Metadata upsert completed in {meta_dt:.2f}s for {len(meta_map)} series IDs")

    # 2) Build full DataFrame
    build_start = time.perf_counter()
    ts_df, total_series = build_timeseries_df(dict_df, meta_map, infdblog)
    build_dt = time.perf_counter() - build_start
    infdblog.info(
        f"Built full timeseries DataFrame in {build_dt:.2f}s "
        f"({len(ts_df):,} rows across {total_series} series)."
    )

    if ts_df.empty:
        infdblog.info("Timeseries DataFrame is empty; nothing to upload.")
        return

    # 3) Drop index on target table (if any)
    conn = engine.raw_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"DROP INDEX IF EXISTS entise_ts_data_idx;")
        conn.commit()
    except Exception as e:
        infdblog.debug(f"Could not drop entise_ts_data_idx: {e}")
        conn.rollback()

    # 4) Single COPY using CSV
    copy_start = time.perf_counter()
    buf = io.StringIO()
    ts_df.to_csv(buf, index=False, header=False, date_format="%Y-%m-%dT%H:%M:%S%z")
    buf.seek(0)

    try:
        # Optional: faster at the cost of weaker durability during this transaction
        try:
            cur.execute("SET LOCAL synchronous_commit = off;")
        except Exception:
            pass

        copy_sql = (
            f"COPY {output_schema}.{table_name} "
            f"(ts_metadata_id, time, value) FROM STDIN WITH (FORMAT csv)"
        )
        cur.copy_expert(copy_sql, buf)
        conn.commit()
    finally:
        cur.close()
        conn.close()

    copy_dt = time.perf_counter() - copy_start
    rows = len(ts_df)
    rps = rows / copy_dt if copy_dt > 0 else rows
    infdblog.info(
        f"Baseline COPY: {rows:,} rows inserted in {copy_dt:.2f}s "
        f"({rps:,.0f} rows/s)"
    )

    # 5) Recreate index and ANALYZE
    conn = engine.raw_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS entise_ts_data_idx "
            f"ON {output_schema}.{table_name} (ts_metadata_id, time DESC);"
        )
        cur.execute(f"ANALYZE {output_schema}.{table_name};")
        conn.commit()
    except Exception as e:
        infdblog.warning(f"Failed to recreate index or ANALYZE: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()

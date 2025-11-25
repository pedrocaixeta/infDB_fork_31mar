import time
import numpy as np
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_datetime64tz_dtype
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


def _coalesce(value, default):
    return default if value is None else value


def _as_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "y", "on")
    if value is None:
        return False
    return bool(value)


def _as_int(value, default=None):
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def copy_timeseries_chunk(engine, output_schema, table_name, df, conn=None, sync_commit_off=False):
    """COPY a DataFrame chunk into Postgres. Returns number of rows written.

    If `conn` is provided, it will be reused and NOT committed here (caller manages commits).
    If `sync_commit_off` is True, sets SET LOCAL synchronous_commit = off for the active xact.
    """
    if df.empty:
        return 0

    cols = ["ts_metadata_id", "time", "value"]
    out = df[cols]

    # Normalize time only if needed
    time_col = out["time"]

    if not is_datetime64_any_dtype(time_col):
        # Not datetime-like at all → parse and localize to UTC
        out = out.copy()
        out["time"] = pd.to_datetime(time_col, utc=True)
    else:
        # Datetime-like: ensure it is tz-aware UTC
        if not is_datetime64tz_dtype(time_col):
            # naive datetime64 → localize to UTC
            out = out.copy()
            out["time"] = time_col.dt.tz_localize("UTC")
        # else: already tz-aware; keep as-is

    out = out[out["time"].notna()]

    buf = io.StringIO()
    out.to_csv(buf, index=False, header=False, date_format="%Y-%m-%dT%H:%M:%S%z")
    buf.seek(0)

    created_conn = False
    if conn is None:
        conn = engine.raw_connection()
        created_conn = True
    try:
        cur = conn.cursor()
        try:
            if sync_commit_off:
                try:
                    cur.execute("SET LOCAL synchronous_commit = off;")
                except Exception:
                    # Ignore if not permitted by server/role
                    pass
            cur.copy_expert(
                f"COPY {output_schema}.{table_name} (ts_metadata_id, time, value) FROM STDIN WITH (FORMAT csv)",
                buf,
            )
        finally:
            cur.close()
        if created_conn:
            conn.commit()
    finally:
        if created_conn:
            conn.close()
    return len(out)



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


def batch_upload_timeseries_concurrent(engine, infdblog, output_schema, table_name, dict_df,
                                       chunk_rows=1_000_000, max_workers=4, sync_commit_off=True):
    """Batch upsert metadata, then COPY time series in chunked, concurrent uploads.
    Robust to individual chunk failures: collects exceptions and reports at the end.

    Alignment with single-threaded path:
      - Same column_map and metadata upsert
      - Per-chunk and overall throughput logging
      - UTC enforcement and column order handled by copy_timeseries_chunk
    """
    t_start = time.perf_counter()
    # 1) Upsert metadata and get IDs
    objectids = [str(objid) for objid in dict_df.keys()]
    meta_map = _upsert_metadata_and_get_ids(engine, infdblog, output_schema, objectids)

    # 2) Prepare mapping from logical series name to dataframe column
    column_map = {
        "ro_heat_indoor_temperature": "indoor_temperature[C]",
        "ro_heat_heating_load": "heating:load[W]",
        "ro_heat_cooling_load": "cooling:load[W]",
    }

    # 3) Stream frames into chunked COPY tasks
    frames = []
    pending_rows = 0
    submitted_rows = 0
    futures = []
    future_start = {}
    total_series = 0
    errors = []

    def submit_chunk(executor, frames_list):
        nonlocal submitted_rows
        combined = pd.concat(frames_list, ignore_index=True)
        submitted_rows += len(combined)
        # wrap with timing by using executor to run copy_timeseries_chunk directly
        fut = executor.submit(copy_timeseries_chunk, engine, output_schema, table_name, combined, None, sync_commit_off)
        future_start[fut] = time.perf_counter()
        return fut

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for objectid, row in dict_df.items():
            hvac_df = row.get("hvac") if isinstance(row, dict) else row["hvac"]
            if hvac_df is None:
                continue
            for name, col in column_map.items():
                key = (name, str(objectid), "ro-heat")
                ts_id = meta_map.get(key)
                if ts_id is None:
                    continue
                if col not in hvac_df.columns:
                    continue
                ts = hvac_df[col]
                df = pd.DataFrame({
                    "ts_metadata_id": ts_id,
                    "time": ts.index,
                    "value": ts.values,
                })
                frames.append(df)
                pending_rows += len(df)
                total_series += 1

                if pending_rows >= chunk_rows:
                    futures.append(submit_chunk(executor, frames))
                    frames = []
                    pending_rows = 0
                    # throttle: keep at most 2x workers outstanding
                    if len(futures) >= max_workers * 2:
                        done, not_done = wait(futures, return_when=FIRST_COMPLETED)
                        # log completions
                        for fut in done:
                            try:
                                rows = fut.result()
                                dt = time.perf_counter() - future_start.pop(fut, t_start)
                                rps = rows / dt if dt > 0 else rows
                                infdblog.info(f"COPY chunk completed: {rows} rows in {dt:.2f}s ({rps:,.0f} rows/s)")
                            except Exception as e:
                                errors.append(e)
                                infdblog.error(f"COPY chunk failed: {e}")
                        futures = list(not_done)

        # flush remainder
        if frames:
            futures.append(submit_chunk(executor, frames))
            frames = []
            pending_rows = 0

        # wait all
        for fut in as_completed(futures):
            try:
                rows = fut.result()
                dt = time.perf_counter() - future_start.pop(fut, t_start)
                rps = rows / dt if dt > 0 else rows
                infdblog.info(f"COPY chunk completed: {rows} rows in {dt:.2f}s ({rps:,.0f} rows/s)")
            except Exception as e:
                errors.append(e)
                infdblog.error(f"COPY chunk failed: {e}")

    if errors:
        infdblog.error(f"{len(errors)} COPY chunk(s) failed; first error: {errors[0]}")
        raise RuntimeError("One or more COPY chunks failed; see logs for details")

    total_dt = time.perf_counter() - t_start
    overall_rps = submitted_rows / total_dt if total_dt > 0 else submitted_rows
    infdblog.info(f"Uploaded {submitted_rows} rows across {total_series} series using up to {max_workers} workers in chunks of {chunk_rows}; overall throughput: {overall_rps:,.0f} rows/s")


def batch_upload_timeseries_simple(
    engine,
    infdblog,
    output_schema,
    table_name,
    dict_df,
    chunk_rows=1_000_000,
    use_staging=True,
    sync_commit_off=True,
    copy_commit_every=1,
):
    """Simplified, single-threaded uploader: upsert metadata, then COPY rows in large chunks.

    Phase 1 optimizations:
      - Larger chunk size by default (1M rows)
      - Reuse a single DB connection/cursor across COPY calls
      - Optionally SET LOCAL synchronous_commit=off for faster WAL
      - Optional UNLOGGED staging table with final server-side merge
      - Per-chunk timing logs and overall throughput
    """
    column_map = {
        "ro_heat_indoor_temperature": "indoor_temperature[C]",
        "ro_heat_heating_load": "heating:load[W]",
        "ro_heat_cooling_load": "cooling:load[W]",
    }

    # 1) Upsert metadata and get IDs
    meta_start = time.perf_counter()
    objectids = [str(objid) for objid in dict_df.keys()]
    meta_map = _upsert_metadata_and_get_ids(engine, infdblog, output_schema, objectids)
    meta_dur = time.perf_counter() - meta_start
    infdblog.info(f"Metadata upsert completed in {meta_dur:.2f}s for {len(meta_map)} series IDs")

    # 2) Prepare connection and (optional) staging
    conn = engine.raw_connection()
    cur = conn.cursor()
    target_table = table_name
    if use_staging:
        staging_table = f"{table_name}_staging"
        # Create UNLOGGED staging (no index)
        cur.execute(
            f"CREATE UNLOGGED TABLE IF NOT EXISTS {output_schema}.{staging_table} ("
            f"ts_metadata_id integer, time timestamptz, value double precision);"
        )
        # Truncate old content to avoid duplicates
        cur.execute(f"TRUNCATE {output_schema}.{staging_table};")
        conn.commit()
        target_table = staging_table

    # 3) Accumulate frames and flush synchronously when reaching chunk_rows
    frames = []
    pending_rows = 0
    total_rows = 0
    total_series = 0
    chunks_written = 0
    t0 = time.perf_counter()

    def flush():
        nonlocal frames, pending_rows, total_rows, chunks_written
        if not frames:
            return 0
        combined = pd.concat(frames, ignore_index=True)
        st = time.perf_counter()
        written = copy_timeseries_chunk(
            engine, output_schema, target_table, combined, conn=conn, sync_commit_off=sync_commit_off
        )
        # Commit policy: commit after each chunk or per N chunks
        chunks_written += 1
        if chunks_written % max(1, int(copy_commit_every)) == 0:
            try:
                conn.commit()
            except Exception:
                pass
        dt = time.perf_counter() - st
        rps = written / dt if dt > 0 else written
        infdblog.info(f"Flushed COPY chunk: {written} rows in {dt:.2f}s ({rps:,.0f} rows/s)")
        total_rows += written
        frames.clear()
        pending_rows = 0
        return written

    for objectid, row in dict_df.items():
        hvac_df = row.get("hvac") if isinstance(row, dict) else row["hvac"]
        if hvac_df is None:
            continue

        per_building_frames = []

        for name, col in column_map.items():
            key = (name, str(objectid), "ro-heat")
            ts_id = meta_map.get(key)
            if ts_id is None or col not in hvac_df.columns:
                continue

            ts = hvac_df[col]
            per_building_frames.append(pd.DataFrame({
                "ts_metadata_id": ts_id,
                "time": ts.index,
                "value": ts.values,
            }))
            total_series += 1

        if not per_building_frames:
            continue

        building_df = pd.concat(per_building_frames, ignore_index=True)

        frames.append(building_df)
        pending_rows += len(building_df)

        if pending_rows >= chunk_rows:
            flush()

    flush()  # final

    # Finalize staging merge and cleanup
    if use_staging:
        st = time.perf_counter()
        cur.execute(
            f"INSERT INTO {output_schema}.{table_name} (ts_metadata_id, time, value) "
            f"SELECT ts_metadata_id, time, value FROM {output_schema}.{target_table};"
        )
        conn.commit()
        merge_dt = time.perf_counter() - st
        infdblog.info(f"Merged staging into {table_name} in {merge_dt:.2f}s")
        # Drop staging to reclaim space
        cur.execute(f"DROP TABLE IF EXISTS {output_schema}.{target_table};")
        conn.commit()
    else:
        # Ensure any uncommitted COPY chunks are persisted when writing directly to final table
        try:
            conn.commit()
        except Exception:
            pass

    cur.close()
    conn.close()

    total_dt = time.perf_counter() - t0
    overall_rps = total_rows / total_dt if total_dt > 0 else total_rows
    infdblog.info(
        f"Uploaded {total_rows} rows across {total_series} series in chunks of {chunk_rows} (single-threaded); "
        f"overall throughput: {overall_rps:,.0f} rows/s"
    )


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

        elements = pd.read_sql("""SELECT * FROM v_element_layer_data""", engine)

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
            constructions.groupby("building_objectid")[["capacitance", "resistance"]].sum().sort_values("building_objectid")
        )

        bld2ts = timedata.get_bld2ts(database_connection=engine)

        all_ts_df = timedata.get_all_timeseries_data(database_connection=engine, start=pd.Timestamp(f"{simulation_year}-01-01"), end=pd.Timestamp(f"{simulation_year}-12-31"))
        all_ts_df.index.name = 'datetime'
        all_ts_df.rename(columns={"value": "air_temperature[C]"}, inplace=True)
        all_ts_df = all_ts_df.reset_index()
        data = {x: y.sort_index() for x, y in all_ts_df.groupby('ts_metadata_id')}

        # Preparation for EnTiSe
        entise_input = rc_values.reset_index().rename(columns={"building_objectid": "id"})
        entise_input = entise_input.rename(columns={'resistance': 'resistance[K W-1]', 'capacitance': 'capacitance[J K-1]'}, errors=True)
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

        # Phase 0/1: read upload config with safe defaults
        try:
            # Respect explicit False values; only use defaults when key is missing (None)
            concurrent_val = _coalesce(infdbhandler.get_config_value(["ro-heat", "upload", "concurrent"]), False)
            chunk_rows_val = _coalesce(infdbhandler.get_config_value(["ro-heat", "upload", "chunk_rows"]), 1_000_000)
            use_staging_val = _coalesce(infdbhandler.get_config_value(["ro-heat", "upload", "use_staging"]), True)
            drop_idx_val = _coalesce(infdbhandler.get_config_value(["ro-heat", "upload", "drop_indexes_on_load"]), True)
            sync_commit_off_val = _coalesce(infdbhandler.get_config_value(["ro-heat", "upload", "sync_commit_off"]), True)
            copy_commit_every_val = _coalesce(infdbhandler.get_config_value(["ro-heat", "upload", "copy_commit_every"]), 1)
            workers_val = _coalesce(infdbhandler.get_config_value(["ro-heat", "upload", "workers"]), 4)

            upload_cfg = {
                "concurrent": _as_bool(concurrent_val),
                "chunk_rows": _as_int(chunk_rows_val, 1_000_000),
                "use_staging": _as_bool(use_staging_val),
                "drop_indexes_on_load": _as_bool(drop_idx_val),
                "sync_commit_off": _as_bool(sync_commit_off_val),
                "copy_commit_every": _as_int(copy_commit_every_val, 1),
                "workers": _as_int(workers_val, 4),
            }
        except Exception:
            upload_cfg = {"concurrent": False, "chunk_rows": 1_000_000, "use_staging": True,
                          "drop_indexes_on_load": True, "sync_commit_off": True, "copy_commit_every": 1,
                          "workers": 4}

        # If requested, drop non-essential index(es) before bulk load; recreate after
        if upload_cfg["drop_indexes_on_load"]:
            try:
                infdbclient_citydb.execute_query(
                    f"DROP INDEX IF EXISTS entise_ts_data_idx;"
                )
            except Exception:
                # Index may be schema-qualified on some setups
                try:
                    infdbclient_citydb.execute_query(
                        f"DROP INDEX IF EXISTS {output_schema}.entise_ts_data_idx;"
                    )
                except Exception as e:
                    infdblog.debug(f"No existing index to drop: {e}")
        else:
            # Otherwise ensure index exists before load (default behavior)
            try:
                idx_sql = f"""
                CREATE INDEX IF NOT EXISTS entise_ts_data_idx
                ON {output_schema}.{table_name} (ts_metadata_id, time DESC);
                """
                infdbclient_citydb.execute_query(idx_sql)
            except Exception as e:
                infdblog.warning("Failed to create index on timeseries table: %s", e)

        # Batch upload of all time series
        start = time.perf_counter()
        if _as_bool(upload_cfg["concurrent"]):
            # Use existing concurrent path (Phase 0 flag-controlled); chunk size still applies
            batch_upload_timeseries_concurrent(
                engine=engine,
                infdblog=infdblog,
                output_schema=output_schema,
                table_name=table_name,
                dict_df=dict_df,
                chunk_rows=int(upload_cfg["chunk_rows"]),
                max_workers=_as_int(upload_cfg.get("workers", 4), 4),
                sync_commit_off=_as_bool(upload_cfg.get("sync_commit_off", True)),
            )
        else:
            batch_upload_timeseries_simple(
                engine=engine,
                infdblog=infdblog,
                output_schema=output_schema,
                table_name=table_name,
                dict_df=dict_df,
                chunk_rows=int(upload_cfg["chunk_rows"]),
                use_staging=_as_bool(upload_cfg["use_staging"]),
                sync_commit_off=_as_bool(upload_cfg["sync_commit_off"]),
                copy_commit_every=_as_int(upload_cfg["copy_commit_every"], 1),
            )
        end = time.perf_counter()
        infdblog.info(f"Batch upload completed in {end - start:.2f} seconds")
        print(f"Batch upload completed in {end - start:.2f} seconds")

        # Recreate performance index and ANALYZE after the load if it was dropped
        if upload_cfg["drop_indexes_on_load"]:
            try:
                idx_sql = f"""
                CREATE INDEX IF NOT EXISTS entise_ts_data_idx
                ON {output_schema}.{table_name} (ts_metadata_id, time DESC);
                """
                infdbclient_citydb.execute_query(idx_sql)
            except Exception as e:
                infdblog.warning("Failed to create index on timeseries table: %s", e)
            try:
                infdbclient_citydb.execute_query(f"ANALYZE {output_schema}.{table_name};")
            except Exception as e:
                infdblog.debug(f"ANALYZE failed: {e}")

        infdblog.info("Ro-heat sucessfully completed")

    except Exception as e:
        infdblog.error(f"Something went wrong: {str(e)}")
        raise e


if __name__ == "__main__":
    main()

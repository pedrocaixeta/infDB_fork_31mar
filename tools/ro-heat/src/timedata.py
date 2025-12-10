# Get time series data for a given location and time range

import io
import time

import pandas as pd
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert


def get_hourly_temperature_2m(objectid, database_connection, start_time=None, end_time=None):
    query = f"""
        SELECT time, value from opendata.openmeteo_ts_data
        JOIN opendata.openmeteo_ts_metadata 
        ON opendata.openmeteo_ts_data.ts_metadata_id = opendata.openmeteo_ts_metadata.id
        JOIN basedata.bld2grid ON opendata.openmeteo_ts_metadata.grid_id = basedata.bld2grid.id
        WHERE objectid='{objectid}' and
            openmeteo_ts_metadata.name='openmeteo_hourly_temperature_2m' and
                time >= '{start_time}'
            AND time <  '{end_time}'
        ORDER BY time ASC;
    """
    df = pd.read_sql(sql=query, con=database_connection)
    df.set_index("time", inplace=True)

    return df


"""
        data = {
            "weather": pd.DataFrame(
                {
                    "temp_out": df_hourly_temperature2m["value"].values,
                    "datetime": df_hourly_temperature2m.index,
                }
            )
        }
"""


def get_distinct_building_ids(database_connection):
    query = """
        SELECT DISTINCT objectid 
        FROM opendata.buildings_lod2
    """
    df = pd.read_sql(sql=query, con=database_connection)
    return df["objectid"].tolist()


def get_all_timeseries_data(database_connection, start, end):
    query = f"""
        SELECT *
        FROM opendata.openmeteo_ts_data
        WHERE time >= '{start}' AND time < '{end}'
    """
    df = pd.read_sql(sql=query, con=database_connection)
    df.set_index("time", inplace=True)

    return df


def get_bld2ts(database_connection):
    query = """
        SELECT *
        FROM basedata.bld2ts
    """
    df = pd.read_sql(sql=query, con=database_connection)

    return df


def post_timeseries_data(database_connection, df_timeseries):
    df_timeseries.to_sql(
        "openmeteo_ts_data", con=database_connection, schema="opendata", if_exists="append", index=False
    )


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
            records.append(
                {
                    "name": name,
                    "description": description,
                    "type": typ,
                    "unit": unit,
                    "changelog": 0,
                    "objectid": str(objectid),
                    "source": "ro-heat",
                }
            )

    if not records:
        return {}

    # Reflect table and construct dialect-aware upsert with RETURNING
    md = MetaData()
    meta_table = Table("entise_ts_metadata", md, schema=output_schema, autoload_with=engine)

    meta_map = {}

    batch_size = 1000
    with engine.begin() as conn:
        for start in range(0, len(records), batch_size):
            batch = records[start : start + batch_size]
            insert_stmt = pg_insert(meta_table).values(batch)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[meta_table.c.name, meta_table.c.objectid, meta_table.c.source],
                set_={
                    "unit": insert_stmt.excluded.unit,
                    "type": insert_stmt.excluded.type,
                    "description": insert_stmt.excluded.description,
                },
            ).returning(meta_table.c.id, meta_table.c.name, meta_table.c.objectid, meta_table.c.source)
            res = conn.execute(upsert_stmt)
            for row in res.mappings():
                meta_map[(row["name"], row["objectid"], row["source"])] = row["id"]

    infdblog.info(f"Upserted metadata for {len(objectids)} buildings; total series rows: {len(records)}")
    return meta_map


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

    infdblog.info(f"Built timeseries DataFrame with {len(ts_df)} rows across {total_series} series.")
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
        f"Built full timeseries DataFrame in {build_dt:.2f}s ({len(ts_df):,} rows across {total_series} series)."
    )

    if ts_df.empty:
        infdblog.info("Timeseries DataFrame is empty; nothing to upload.")
        return

    # 3) Drop index on target table (if any)
    conn = engine.raw_connection()
    cur = conn.cursor()
    try:
        cur.execute("DROP INDEX IF EXISTS entise_ts_data_idx;")
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

        copy_sql = f"COPY {output_schema}.{table_name} (ts_metadata_id, time, value) FROM STDIN WITH (FORMAT csv)"
        cur.copy_expert(copy_sql, buf)
        conn.commit()
    finally:
        cur.close()
        conn.close()

    copy_dt = time.perf_counter() - copy_start
    rows = len(ts_df)
    rps = rows / copy_dt if copy_dt > 0 else rows
    infdblog.info(f"Baseline COPY: {rows:,} rows inserted in {copy_dt:.2f}s ({rps:,.0f} rows/s)")

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

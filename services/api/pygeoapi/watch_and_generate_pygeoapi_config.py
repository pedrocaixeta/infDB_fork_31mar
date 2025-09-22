#!/usr/bin/env python3
import os, sys, time, tempfile, pathlib
from datetime import datetime, timedelta, timezone

import yaml
import psycopg
from psycopg.rows import dict_row

OUT = pathlib.Path("pygeoapi-config.yml")

# ---------- env ----------
def env(name, default=None, required=False):
    v = os.getenv(name, default)
    if required and (v is None or v == ""):
        print(f"[ERR] missing required env: {name}", file=sys.stderr); sys.exit(2)
    return v

def build_dsn():
    user = env("SERVICES_CITYDB_USER", required=True)
    pwd  = env("SERVICES_CITYDB_PASSWORD", required=True)
    db   = env("SERVICES_CITYDB_DB", required=True)
    host = env("SERVICES_CITYDB_HOST", required=True)
    port = int(env("SERVICES_CITYDB_EXPOSED_PORT", "5432"))
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"

PYGEO_PORT = int(env("SERVICES_PYGEOAPI_PORT", "5000"))
DEFAULT_EPSG = env("SERVICES_CITYDB_EPSG")
DSN = build_dsn()

# ---------- io helpers ----------

def get_schema_signature(conn) -> str:
    """
    Deterministic signature of all geometry/geography tables/columns (incl. SRID when known).
    If this string changes, the schema relevant to the config changed.
    """
    with conn.cursor() as cur:
        cur.execute("""
            WITH geomcols AS (
              SELECT
                n.nspname   AS schema_name,
                c.relname   AS table_name,
                a.attname   AS geom_col,
                COALESCE(gc.srid, gg.srid) AS srid
              FROM pg_attribute a
              JOIN pg_class c      ON c.oid = a.attrelid
              JOIN pg_namespace n  ON n.oid = c.relnamespace
              JOIN pg_type t       ON t.oid = a.atttypid
              LEFT JOIN public.geometry_columns  gc
                     ON gc.f_table_schema = n.nspname
                    AND gc.f_table_name   = c.relname
                    AND gc.f_geometry_column = a.attname
              LEFT JOIN public.geography_columns gg
                     ON gg.f_table_schema = n.nspname
                    AND gg.f_table_name   = c.relname
                    AND gg.f_geography_column = a.attname
              WHERE a.attnum > 0 AND NOT a.attisdropped
                AND c.relkind IN ('r','v','m','f','p')  -- tables/views/mviews/foreign/partitioned
                AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
                AND t.typname IN ('geometry','geography')
            )
            SELECT COALESCE(
                     string_agg(
                       schema_name || '.' || table_name || '.' || geom_col || ':' || COALESCE(srid::text, ''),
                       '|' ORDER BY schema_name, table_name, geom_col
                     ),
                     ''
                   ) AS sig
            FROM geomcols;
        """)
        row = cur.fetchone()
        return row["sig"] or ""

def get_dml_signature_geom(conn) -> int:
    """
    Sums INSERT/UPDATE/DELETE/HOT UPDATE counters, but **only** for tables that
    contain a geometry/geography column. If this number changes, data changed.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(SUM(s.n_tup_ins + s.n_tup_upd + s.n_tup_del + s.n_tup_hot_upd), 0) AS dml_sum
            FROM pg_stat_user_tables s
            JOIN pg_class c ON c.oid = s.relid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
              AND EXISTS (
                SELECT 1
                FROM pg_attribute a
                JOIN pg_type t ON t.oid = a.atttypid
                WHERE a.attrelid = c.oid
                  AND a.attnum > 0 AND NOT a.attisdropped
                  AND t.typname IN ('geometry','geography')
              );
        """)
        row = cur.fetchone()
        return int(row["dml_sum"] or 0)
    


def atomic_write_yaml(obj, out_path: pathlib.Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=out_path.parent, suffix=".tmp") as tmp:
        yaml.safe_dump(obj, tmp, sort_keys=False)
        tmp.flush(); os.fsync(tmp.fileno())
        tmp_name = tmp.name
    os.replace(tmp_name, out_path)

# ---------- db helpers ----------
def table_pk_column(cur, schema, table):
    cur.execute("""
        SELECT a.attname AS col
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indisprimary AND n.nspname = %s AND c.relname = %s
        ORDER BY a.attnum LIMIT 1
    """, (schema, table))
    row = cur.fetchone()
    if row: return row["col"]
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s AND column_name='id' LIMIT 1
    """, (schema, table))
    row = cur.fetchone()
    if row: return row["column_name"]
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
          AND data_type IN ('integer','bigint','uuid','smallint')
        ORDER BY ordinal_position LIMIT 1
    """, (schema, table))
    row = cur.fetchone()
    return row["column_name"] if row else None

def geometry_sources(cur):
    out = []
    def has_view(view_schema, view_name):
        cur.execute("""
          SELECT 1 FROM information_schema.tables 
          WHERE table_schema=%s AND table_name=%s LIMIT 1
        """, (view_schema, view_name))
        return cur.fetchone() is not None

    got_geom = has_view("public", "geometry_columns") or has_view("topology", "geometry_columns")
    if got_geom:
        try:
            cur.execute("""
              SELECT f_table_schema AS schema, f_table_name AS table,
                     f_geometry_column AS geom_col, srid, type AS geom_type
              FROM public.geometry_columns ORDER BY 1,2
            """)
            for r in cur.fetchall(): out.append({**r, "is_geography": False})
        except Exception: pass

    got_geog = has_view("public", "geography_columns")
    if got_geog:
        try:
            cur.execute("""
              SELECT f_table_schema AS schema, f_table_name AS table,
                     f_geography_column AS geom_col, srid, type AS geom_type
              FROM public.geography_columns ORDER BY 1,2
            """)
            for r in cur.fetchall(): out.append({**r, "is_geography": True})
        except Exception: pass

    if out: return out

    cur.execute("""
      WITH cols AS (
        SELECT n.nspname AS schema, c.relname AS table, a.attname AS geom_col, t.typname AS typ
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_type t ON t.oid = a.atttypid
        WHERE a.attnum > 0 AND NOT a.attisdropped
          AND c.relkind IN ('r','v','m','f','p')
          AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
          AND t.typname IN ('geometry','geography')
      )
      SELECT schema, table, geom_col, typ AS typname
      FROM cols ORDER BY 1,2;
    """)
    rows = cur.fetchall()
    for r in rows:
        out.append({
            "schema": r["schema"], "table": r["table"], "geom_col": r["geom_col"],
            "srid": int(DEFAULT_EPSG) if DEFAULT_EPSG else None,
            "geom_type": None, "is_geography": (r["typname"] == "geography"),
        })
    return out

# ---------- build ----------
def build_config_on_conn(conn):
    with conn.cursor() as cur:
        geoms = geometry_sources(cur)
        resources = {}
        for g in geoms:
            schema, table, geom_col = g["schema"], g["table"], g["geom_col"]
            srid = g.get("srid")
            rid = f"{table}"

            title = f"{table} table (postgis Provider)"
            descr = f"{table} table (postgis)"
            kws = [t for t in table.replace("-", "_").split("_") if t] or [table]

            # Build CRS URIs (if SRID known)
            epsg_uri = f"http://www.opengis.net/def/crs/EPSG/0/{int(srid)}" if srid else None
            crs_list = ["http://www.opengis.net/def/crs/OGC/1.3/CRS84"]
            if epsg_uri:
                crs_list.extend([epsg_uri, "http://www.opengis.net/def/crs/EPSG/0/3035"])

            provider = {
                "type": "feature",
                "name": "PostgreSQL",
                "data": {
                    "host": os.getenv("SERVICES_CITYDB_HOST", "citydb"),
                    "port": int(os.getenv("SERVICES_CITYDB_EXPOSED_PORT", "5432")),
                    "dbname": os.getenv("SERVICES_CITYDB_DB"),
                    "user": os.getenv("SERVICES_CITYDB_USER"),
                    "password": os.getenv("SERVICES_CITYDB_PASSWORD"),
                    "search_path": [schema],
                },
                "id_field": table_pk_column(cur, schema, table),
                "table": table,             
                "geom_field": geom_col,
                **({"storage_crs": epsg_uri} if epsg_uri else {}),
                "crs": crs_list,
            }
            if srid:
                provider["srid"] = int(srid)

            resource = {
                "type": "collection",
                "title": title,
                "description": descr,
                "keywords": kws,
                "extents": {
                    "spatial": {
                        "bbox": [-180, -90, 180, 90],
                        "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
                    },
                    "temporal": {"begin": None, "end": None},
                },
                "providers": [provider],
            }

            resources[rid] = resource

        config = {
            "server": {
                "bind": {"host": "0.0.0.0", "port": PYGEO_PORT},
                "url": f"http://localhost:{PYGEO_PORT}",
                "mimetype": "application/json; charset=UTF-8",
                "encoding": "utf-8", "gzip": False, "limit": 1000,
                "language": "en-US", "cors": True, "pretty_print": True, "admin": False,
                "limits": {"default_items": 10, "max_items": 50},
                "map": {"url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
                        "attribution": ('&copy; <a href="https://openstreetmap.org/copyright">'
                                        'OpenStreetMap contributors</a>')},
                "ogc_schemas_location": "/schemas.opengis.net",
            },
            "logging": {"level": "DEBUG"},
            "metadata": {
                "identification": {
                    "title": "pygeoapi Demo instance - running latest GitHub version",
                    "description": "pygeoapi provides an API to geospatial data",
                    "keywords": ["geospatial", "data", "api"],
                    "keywords_type": "theme",
                    "terms_of_service": "https://creativecommons.org/licenses/by/4.0/",
                    "url": "https://github.com/geopython/pygeoapi",
                },
                "license": {"name": "CC-BY 4.0 license", "url": "https://creativecommons.org/licenses/by/4.0/"},
                "provider": {"name": "pygeoapi Development Team", "url": "https://pygeoapi.io"},
                "contact": {"name": "Infdb Development Team", "position": "Developers",
                            "address": "Technical University of Munich", "city": "Munich"},
            },
            "resources": resources,
        }
        atomic_write_yaml(config, OUT)
        print(f"[{datetime.now(timezone.utc).isoformat()}Z] wrote {OUT.resolve()} with {len(resources)} resource(s)")

# ---------- main loop ----------
def listen_and_rebuild():
    backoff = 2
    poll_interval = 1.0     # seconds between checks
    min_rebuild_gap = 3.0   # throttle: min seconds between rebuilds

    while True:
        try:
            conn = psycopg.connect(DSN, row_factory=dict_row)
            conn.autocommit = True
            print("Connected to Postgres; watching geometry tables for data *and* schema changes")

            # Initial build so the file exists
            build_config_on_conn(conn)
            last_schema_sig = get_schema_signature(conn)
            last_dml_sig    = get_dml_signature_geom(conn)
            last_built_at   = time.monotonic()

            print(f"Initial schema signature = {last_schema_sig!r}")
            print(f"Initial DML signature    = {last_dml_sig}")
            backoff = 2  # reset backoff

            while True:
                time.sleep(poll_interval)

                schema_sig = get_schema_signature(conn)
                dml_sig    = get_dml_signature_geom(conn)

                changed_schema = (schema_sig != last_schema_sig)
                changed_dml    = (dml_sig    != last_dml_sig)
                enough_time    = (time.monotonic() - last_built_at) >= min_rebuild_gap

                if (changed_schema or changed_dml) and enough_time:
                    if changed_schema and changed_dml:
                        print("Schema and data changed; rebuilding config")
                    elif changed_schema:
                        print("Schema changed; rebuilding config")
                    else:
                        print(f"Data changed {last_dml_sig} -> {dml_sig}; rebuilding config")

                    build_config_on_conn(conn)
                    last_schema_sig = schema_sig
                    last_dml_sig    = dml_sig
                    last_built_at   = time.monotonic()

        except psycopg.OperationalError as e:
            print(f"[WARN] DB connection problem: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

        except Exception as e:
            print(f"[ERR] Unexpected error in loop: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    try:
        listen_and_rebuild()
    except KeyboardInterrupt:
        print("Exiting.")

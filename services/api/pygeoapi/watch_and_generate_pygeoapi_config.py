import os
import sys
import time
import tempfile
import pathlib
import json
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
import yaml

# =========================
# ===== Module Constants ===
# =========================

OUTPUT_CONFIG_PATH: pathlib.Path = pathlib.Path("pygeoapi-config.yml")

LOGGER_NAME: str = "pygeoapi_config_gen"

CRS84_URI: str = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
EPSG_25832_URI: str = "http://www.opengis.net/def/crs/EPSG/0/25832"

GERMANY_BBOX_CRS84: List[float] = [5.866315, 47.270111, 15.041932, 55.058384]

# =========================
# ========= Env ===========
# =========================

def env(var_name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    """Read an environment variable with optional default and required check.

    Args:
        var_name: Name of the environment variable.
        default: Default value when the variable is missing/empty.
        required: If True, exit when the variable is missing/empty.

    Returns:
        The environment variable value (or default).

    Raises:
        SystemExit: If `required` is True and the variable is missing/empty.
    """
    value = os.getenv(var_name, default)
    if required and (value is None or value == ""):
        log.error("Missing required env variable: %s", var_name)
        sys.exit(2)
    return value


def _setup_logging() -> logging.Logger:
    """Configure and return the module logger."""
    level = (env("LOG_LEVEL", "INFO") or "INFO").upper()
    level_num = getattr(logging, level, logging.INFO)
    logging.basicConfig(level=level_num, format="%(asctime)s %(levelname)s %(message)s")
    return logging.getLogger(LOGGER_NAME)


log = _setup_logging()

# ---------- derived configuration constants (centralized env reads) ----------

PYGEOAPI_PORT: int = int(env("SERVICES_PYGEOAPI_PORT", "5000") or "5000")
PYGEOAPI_URL: Optional[str] = env("SERVICES_PYGEOAPI_BASE_URL")
PYGEOAPI_HOST: Optional[str] = env("SERVICES_PYGEOAPI_BASE_HOST")

POSTGRES_USER: str = env("SERVICES_POSTGRES_USER", required=True) or ""
POSTGRES_PASSWORD: str = env("SERVICES_POSTGRES_PASSWORD", required=True) or ""
POSTGRES_DB: str = env("SERVICES_POSTGRES_DB", required=True) or ""
POSTGRES_HOST: str = "postgres"
POSTGRES_PORT: int = int(env("SERVICES_POSTGRES_EXPOSED_PORT", "5432") or "5432")

FALLBACK_EPSG: int = int(env("FALLBACK_EPSG", "25832") or "25832")  # keep default 25832
FORCE_CRS84_ONLY: bool = (str(env("FORCE_CRS84_ONLY", "false")).lower() in ("1", "true", "yes", "y"))

_SRID_OVERRIDES_ENV: str = env("SRID_OVERRIDES", "") or ""
try:
    SRID_OVERRIDES: Dict[str, int] = json.loads(_SRID_OVERRIDES_ENV) if _SRID_OVERRIDES_ENV else {}
except Exception:
    log.warning("SRID_OVERRIDES is not valid JSON: %r (ignored)", _SRID_OVERRIDES_ENV)
    SRID_OVERRIDES = {}

# Force DB-side transform controls (targets, exclusions)
FORCE_DB_TRANSFORM_TABLES_RAW: str = env("FORCE_DB_TRANSFORM_TABLES", "*") or "*"
_RAW_ITEMS: List[str] = [t.strip() for t in FORCE_DB_TRANSFORM_TABLES_RAW.split(",") if t.strip()]
_EXCLUDES: set[str] = {t[1:] for t in _RAW_ITEMS if t.startswith("!")}
_FORCE_SET: set[str] = {t for t in _RAW_ITEMS if not t.startswith("!")}
FORCE_DB_TRANSFORM_ALL: bool = ("*" in _FORCE_SET) or ("ALL" in _FORCE_SET)


def build_dsn() -> str:
    """Construct a PostgreSQL DSN from centralized env-derived constants.

    Returns:
        Connection DSN string.
    """
    return f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


DB_DSN: str = build_dsn()

# =========================
# ======= IO helpers ======
# =========================

class NoAliasDumper(yaml.SafeDumper):
    """YAML dumper that disables anchors/aliases."""
    def ignore_aliases(self, data: Any) -> bool:
        """Disable YAML anchors/aliases to keep output stable."""
        return True


def atomic_write_yaml(obj: Dict[str, Any], out_path: pathlib.Path) -> None:
    """Atomically write a YAML file.

    Args:
        obj: Python object to serialize.
        out_path: Destination path for the YAML file.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", delete=False, dir=out_path.parent, suffix=".tmp", encoding="utf-8"
    ) as tmp_file:
        yaml.dump(obj, tmp_file, Dumper=NoAliasDumper, sort_keys=False, allow_unicode=True)
        tmp_file.flush()
        os.fsync(tmp_file.fileno())
        tmp_name = tmp_file.name
    os.replace(tmp_name, out_path)

# ===============================
# ====== Change detection =======
# ===============================

def get_schema_signature(connection: psycopg.Connection[Any]) -> str:
    """Return a stable signature of geometry-bearing columns in the DB.

    Args:
        connection: psycopg connection.

    Returns:
        Concatenated signature string (empty string if none).
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
            WITH geomcols AS (
              SELECT
                n.nspname   AS schema_name,
                c.relname   AS table_name,
                a.attname   AS geom_col,
                COALESCE(gc.srid, gg.srid) AS srid
              FROM pg_attribute a
              JOIN pg_class c ON c.oid = a.attrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
              JOIN pg_type t ON t.oid = a.atttypid
              LEFT JOIN public.geometry_columns gc
                ON gc.f_table_schema=n.nspname AND gc.f_table_name=c.relname AND gc.f_geometry_column=a.attname
              LEFT JOIN public.geography_columns gg
                ON gg.f_table_schema=n.nspname AND gg.f_table_name=c.relname AND gg.f_geography_column=a.attname
              WHERE a.attnum>0 AND NOT a.attisdropped
                AND c.relkind IN ('r','v','m','f','p')
                AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
                AND t.typname IN ('geometry','geography')
            )
            SELECT COALESCE(
              string_agg(schema_name||'.'||table_name||'.'||geom_col||':'||COALESCE(srid::text,''),'|' ORDER BY schema_name,table_name,geom_col),
              ''
            ) AS sig
            FROM geomcols;
            """
        )
        row = cursor.fetchone()
        return (row or {}).get("sig") or ""


def get_dml_signature_geom(connection: psycopg.Connection[Any]) -> int:
    """Return a monotonic-ish DML counter across geometry-bearing tables.

    Args:
        connection: psycopg connection.

    Returns:
        Sum of DML counters (insert/update/delete/hot_update).
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COALESCE(SUM(s.n_tup_ins + s.n_tup_upd + s.n_tup_del + s.n_tup_hot_upd), 0) AS dml_sum
            FROM pg_stat_user_tables s
            JOIN pg_class c ON c.oid = s.relid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
              AND EXISTS (
                SELECT 1 FROM pg_attribute a
                JOIN pg_type t ON t.oid=a.atttypid
                WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND t.typname IN ('geometry','geography')
              );
            """
        )
        row = cursor.fetchone()
        return int((row or {}).get("dml_sum") or 0)

# =========================
# ======= DB helpers ======
# =========================

def list_columns(cursor: psycopg.Cursor[Any], schema: str, table: str) -> List[Tuple[str, str]]:
    """List column names and types for a table.

    Args:
        cursor: psycopg cursor.
        schema: Schema name.
        table: Table name.

    Returns:
        List of (column_name, udt_name) tuples ordered by ordinal position.
    """
    cursor.execute(
        """
        SELECT column_name, udt_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return [(r["column_name"], r["udt_name"]) for r in cursor.fetchall()]


def geometry_sources(cursor: psycopg.Cursor[Any]) -> List[Dict[str, Any]]:
    """Enumerate geometry/geography sources in the DB.

    Uses geometry_columns/geography_columns if available; falls back to catalogs.

    Args:
        cursor: psycopg cursor.

    Returns:
        List of dicts with keys: schema, table, geom_col, srid, geom_type, is_geography.
    """
    sources: List[Dict[str, Any]] = []

    def has_view(view_schema: str, view_name: str) -> bool:
        cursor.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s LIMIT 1
            """,
            (view_schema, view_name),
        )
        return cursor.fetchone() is not None

    has_geometry_view = has_view("public", "geometry_columns") or has_view("topology", "geometry_columns")
    if has_geometry_view:
        try:
            cursor.execute(
                """
                SELECT f_table_schema AS schema, f_table_name AS table,
                       f_geometry_column AS geom_col, srid, type AS geom_type
                FROM public.geometry_columns
                ORDER BY 1,2,3
                """
            )
            for row in cursor.fetchall():
                sources.append({**row, "is_geography": False})
        except Exception:
            pass

    has_geography_view = has_view("public", "geography_columns")
    if has_geography_view:
        try:
            cursor.execute(
                """
                SELECT f_table_schema AS schema, f_table_name AS table,
                       f_geography_column AS geom_col, srid, type AS geom_type
                FROM public.geography_columns
                ORDER BY 1,2,3
                """
            )
            for row in cursor.fetchall():
                sources.append({**row, "is_geography": True})
        except Exception:
            pass

    if sources:
        return sources

    cursor.execute(
        """
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
        FROM cols
        ORDER BY 1,2,3;
        """
    )
    for row in cursor.fetchall():
        sources.append(
            {
                "schema": row["schema"],
                "table": row["table"],
                "geom_col": row["geom_col"],
                "srid": None,
                "geom_type": None,
                "is_geography": (row["typname"] == "geography"),
            }
        )
    return sources


def pick_id_column(cursor: psycopg.Cursor[Any], schema: str, table: str) -> Optional[str]:
    """Pick the first column containing 'id' (case-insensitive).

    Args:
        cursor: psycopg cursor.
        schema: Schema name.
        table: Table name.

    Returns:
        Identifier column name, or None if not found.
    """
    cols = list_columns(cursor, schema, table)
    for name, _typ in cols:
        if "id" in name.lower():
            return name
    return None

# =============================
# ===== SRID resolution =======
# =============================

def resolve_srid(
    cursor: psycopg.Cursor[Any],
    schema: str,
    table: str,
    geometry_column: str,
    srid_hint: Optional[int],
) -> int:
    """Resolve SRID using overrides, hints, sampling, or fallback.

    Args:
        cursor: psycopg cursor.
        schema: Schema name.
        table: Table name.
        geometry_column: Geometry column name.
        srid_hint: Optional SRID hint (may be None).

    Returns:
        Determined SRID (EPSG code).
    """
    key = f"{schema}.{table}"
    if key in SRID_OVERRIDES:
        return int(SRID_OVERRIDES[key])
    if isinstance(srid_hint, int) and srid_hint > 0:
        return int(srid_hint)
    try:
        query = sql.SQL(
            """
            SELECT ST_SRID({geom}) AS srid
            FROM {schema}.{table}
            WHERE {geom} IS NOT NULL
            LIMIT 1
            """
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            geom=sql.Identifier(geometry_column),
        )
        cursor.execute(query)
        row = cursor.fetchone()
        if row and row["srid"]:
            return int(row["srid"])
    except Exception:
        pass
    return FALLBACK_EPSG

# ==========================================
# == helper: ensure an EPSG:25832 view  ====
# ==========================================

def ensure_25832_view(
    cursor: psycopg.Cursor[Any],
    schema: str,
    table: str,
    id_column: str,
    geom_column: str,
    non_geom_properties: List[str],
) -> str:
    """Create or replace `<schema>.<table>__25832` with geometry in EPSG:25832.

    Rules:
      * If row SRID = 25832 → pass through.
      * If row SRID = 0 (unknown/None) → ST_SetSRID(..., 25832).
      * If row SRID = 3035 → ST_Transform(..., 25832).
      * Otherwise → pass through.

    Args:
        cursor: psycopg cursor.
        schema: Schema name.
        table: Base table name.
        id_column: Identifier column to project.
        geom_column: Geometry column to transform/assign.
        non_geom_properties: Additional non-geometry property names.

    Returns:
        The created view name.
    """
    view_name = f"{table}__25832"
    props_no_id_geom = [p for p in non_geom_properties if p not in (id_column, geom_column)]

    id_ident = sql.Identifier(id_column).as_string(cursor)
    geom_ident = sql.Identifier(geom_column).as_string(cursor)

    geom_expr = f"""
        CASE
          WHEN ST_SRID({geom_ident}) = 25832 THEN {geom_ident}
          WHEN ST_SRID({geom_ident}) = 0 THEN ST_SetSRID({geom_ident}, 25832)
          WHEN ST_SRID({geom_ident}) = 3035 THEN ST_Transform({geom_ident}, 25832)
          ELSE {geom_ident}
        END AS {geom_ident}
    """

    select_parts = [id_ident, geom_expr] + [sql.Identifier(p).as_string(cursor) for p in props_no_id_geom]
    select_clause = ", ".join(select_parts)

    drop_sql = f"""
        DROP VIEW IF EXISTS
            {sql.Identifier(schema).as_string(cursor)}.{sql.Identifier(view_name).as_string(cursor)}
        CASCADE;
    """
    create_sql = f"""
        CREATE VIEW
            {sql.Identifier(schema).as_string(cursor)}.{sql.Identifier(view_name).as_string(cursor)}
        AS
        SELECT {select_clause}
        FROM {sql.Identifier(schema).as_string(cursor)}.{sql.Identifier(table).as_string(cursor)};
    """

    cursor.execute(drop_sql)
    cursor.execute(create_sql)
    return view_name

# =========================
# ========= build =========
# =========================

def build_config_on_conn(connection: psycopg.Connection[Any]) -> None:
    """Scan DB, assemble pygeoapi config, and write YAML atomically.

    Args:
        connection: psycopg connection.
    """
    skipped = 0
    with connection.cursor() as cursor:
        geometry_columns = geometry_sources(cursor)

        geometries_by_table: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
        for geom in geometry_columns:
            geometries_by_table[(geom["schema"], geom["table"])].append(geom)

        resources: Dict[str, Dict[str, Any]] = {}

        for (schema, table), geom_list in geometries_by_table.items():
            key = f"{schema}.{table}"

            if table.endswith("__crs84") or table.endswith("__25832"):
                log.info("[SKIP] %s: generated helper view", key)
                skipped += 1
                continue

            geometry_column_names = [g["geom_col"] for g in geom_list]
            if "geom" not in geometry_column_names:
                log.info("[SKIP] %s: no geometry column literally named 'geom'", key)
                skipped += 1
                continue

            id_field = pick_id_column(cursor, schema, table)
            if not id_field:
                log.info("[SKIP] %s: no column containing 'id'", key)
                skipped += 1
                continue

            geometry_column_entry = next(g for g in geom_list if g["geom_col"] == "geom")
            geom_field = "geom"

            srid_code = resolve_srid(
                cursor, schema=schema, table=table, geometry_column=geom_field,
                srid_hint=geometry_column_entry.get("srid"),
            )
            epsg_uri = f"http://www.opengis.net/def/crs/EPSG/0/{srid_code}"

            bbox_crs84: List[float] = GERMANY_BBOX_CRS84[:]

            columns = list_columns(cursor, schema, table)
            non_geom_properties = [name for name, typ in columns if typ not in ("geometry", "geography")]

            resource_id = f"{table}"
            resource_title = f"{table}"
            resource_description = f"{table}"
            keywords = [token for token in table.replace("-", "_").split("_") if token] or [table]

            table_value = table
            srid_code_effective = srid_code
            epsg_uri_effective = epsg_uri

            try:
                # If detected SRID is None/0 or 3035 -> create a 25832 view (gated by FORCE_DB_TRANSFORM_*).
                # If detected SRID is 25832 -> just assign (no view).
                if srid_code in (0, 3035):
                    if (FORCE_DB_TRANSFORM_ALL or (key in _FORCE_SET)) and (key not in _EXCLUDES):
                        table_value = ensure_25832_view(
                            cursor=cursor,
                            schema=schema,
                            table=table,
                            id_column=id_field,
                            geom_column=geom_field,
                            non_geom_properties=non_geom_properties,
                        )
                    srid_code_effective = 25832
                    epsg_uri_effective = EPSG_25832_URI

                elif srid_code == 25832:
                    srid_code_effective = 25832
                    epsg_uri_effective = EPSG_25832_URI

            except Exception as err:
                log.warning("[SKIP] %s: failed to create __25832 view (%s)", key, err)
                skipped += 1
                continue

            advertised_crs = [CRS84_URI, epsg_uri_effective] if not FORCE_CRS84_ONLY else [CRS84_URI]

            provider_block = {
                "type": "feature",
                "name": "PostgreSQL",
                "data": {
                    "host": POSTGRES_HOST,
                    "port": POSTGRES_PORT,
                    "dbname": POSTGRES_DB,
                    "user": POSTGRES_USER,
                    "password": POSTGRES_PASSWORD,
                    "search_path": [schema],
                },
                "id_field": id_field,
                "table": table_value,
                "geom_field": geom_field,
                "geom_format": "geojson",
                "properties": [p for p in non_geom_properties if p != geom_field],
                "storage_crs": epsg_uri_effective,
                "crs": advertised_crs,
                "srid": srid_code_effective,
            }

            resources[resource_id] = {
                "type": "collection",
                "title": resource_title,
                "description": resource_description,
                "keywords": keywords,
                "extents": {
                    "spatial": {"bbox": bbox_crs84, "crs": CRS84_URI},
                    "temporal": {"begin": None, "end": None},
                },
                "providers": [provider_block],
            }

        config_document: Dict[str, Any] = {
            "server": {
                "bind": {"host": "0.0.0.0", "port": PYGEOAPI_PORT},
                "url": f"http://{PYGEOAPI_HOST}:{PYGEOAPI_PORT}",
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

        atomic_write_yaml(config_document, OUTPUT_CONFIG_PATH)
        log.info(
            "Wrote %s with %d resource(s). Skipped %d table(s).",
            OUTPUT_CONFIG_PATH.resolve(), len(resources), skipped
        )

# =========================
# ======= main loop =======
# =========================

def listen_and_rebuild() -> None:
    """Connect, build config, and rebuild on schema/DML changes."""
    reconnect_backoff_seconds: float = 2
    poll_interval_seconds: float = 1.0
    min_rebuild_gap_seconds: float = 3.0

    while True:
        try:
            connection = psycopg.connect(DB_DSN, row_factory=dict_row)
            connection.autocommit = True

            build_config_on_conn(connection)
            last_schema_signature = get_schema_signature(connection)
            last_dml_signature = get_dml_signature_geom(connection)
            last_built_monotonic = time.monotonic()
            reconnect_backoff_seconds = 2

            while True:
                time.sleep(poll_interval_seconds)

                current_schema_signature = get_schema_signature(connection)
                current_dml_signature = get_dml_signature_geom(connection)

                schema_changed: bool = current_schema_signature != last_schema_signature
                dml_changed: bool = current_dml_signature != last_dml_signature
                enough_time_elapsed: bool = (time.monotonic() - last_built_monotonic) >= min_rebuild_gap_seconds

                if (schema_changed or dml_changed) and enough_time_elapsed:
                    build_config_on_conn(connection)
                    last_schema_signature = current_schema_signature
                    last_dml_signature = current_dml_signature
                    last_built_monotonic = time.monotonic()

        except psycopg.OperationalError as err:
            log.warning("DB connection problem: %s", err)
            time.sleep(reconnect_backoff_seconds)
            reconnect_backoff_seconds = min(reconnect_backoff_seconds * 2, 60)

        except Exception as err:
            log.error("Unexpected error in loop: %s", err)
            time.sleep(reconnect_backoff_seconds)
            reconnect_backoff_seconds = min(reconnect_backoff_seconds * 2, 60)


if __name__ == "__main__":
    try:
        listen_and_rebuild()
    except KeyboardInterrupt:
        pass

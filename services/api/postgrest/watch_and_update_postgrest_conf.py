#!/usr/bin/env python3
import os, sys, time, re, tempfile, pathlib
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row

# ---------------- paths / env ----------------
def env(name, default=None, required=False):
    v = os.getenv(name, default)
    if required and (v is None or v == ""):
        print(f"[ERR] missing required env: {name}", file=sys.stderr); sys.exit(2)
    return v

def build_dsn():
    user = env("SERVICES_CITYDB_USER",   required=True)
    pwd  = env("SERVICES_CITYDB_PASSWORD", required=True)
    db   = env("SERVICES_CITYDB_DB",     required=True)
    host = env("SERVICES_CITYDB_HOST",   required=True)
    port = int(env("SERVICES_CITYDB_EXPOSED_PORT", "5432"))
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"

DSN = build_dsn()
CHANNEL = os.getenv("POSTGREST_DB_CHANNEL", "pgrst")
POLL_INTERVAL = float(os.getenv("POSTGREST_WATCH_INTERVAL_SEC", "1.0"))
MIN_REBUILD_GAP = float(os.getenv("POSTGREST_MIN_RELOAD_GAP_SEC", "3.0"))

# Exclusions for schema list (comma-separated globs)
DEFAULT_EXCLUDES = "pg_*,information_schema,postgrest"
EXCLUDE_SCHEMAS = [s.strip() for s in os.getenv("POSTGREST_EXCLUDE_SCHEMAS", DEFAULT_EXCLUDES).split(",") if s.strip()]

# Accept file *or* directory for POSTGREST_CONF_PATH
conf_path_env = os.getenv("POSTGREST_CONF_PATH", "/conf/postgrest.conf")
CONF_PATH = pathlib.Path(conf_path_env)
if CONF_PATH.is_dir():
    CONF_DIR = CONF_PATH
    CONF_PATH = CONF_DIR / "postgrest.conf"
else:
    CONF_DIR = CONF_PATH.parent

# ---------------- helpers ----------------
def utcnow():
    return datetime.now(timezone.utc).isoformat()

def fnmatch_any(name: str, patterns) -> bool:
    import fnmatch
    return any(fnmatch.fnmatch(name, p) for p in patterns)

def atomic_write_text(text: str, out_path: pathlib.Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=out_path.parent, suffix=".tmp") as tmp:
        tmp.write(text)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_name = tmp.name
    os.replace(tmp_name, out_path)
    os.chmod(out_path, int(os.getenv("POSTGREST_CONF_MODE", "0644"), 8))
    try:
        os.chmod(out_path.parent, int(os.getenv("POSTGREST_CONF_DIR_MODE", "0755"), 8))
    except Exception:
        pass

def read_text(path: pathlib.Path) -> str:
    try:
        return path.read_text()
    except FileNotFoundError:
        return ""

def ensure_conf_exists():
    """
    Ensure the PostgREST config file exists. If missing, create a minimal one
    using envs, but leave content minimal so we only manage db-schemas later.
    """
    if CONF_PATH.exists() and CONF_PATH.is_file():
        return
    CONF_DIR.mkdir(parents=True, exist_ok=True)

    # Derive reasonable defaults
    db_uri = (
        os.getenv("PGRST_DB_URI")
        or os.getenv("DB_URI")
        or "postgresql://{user}:{pwd}@{host}:{port}/{db}".format(
            user=os.getenv("SERVICES_CITYDB_USER", "postgres"),
            pwd=os.getenv("SERVICES_CITYDB_PASSWORD", ""),
            host=os.getenv("SERVICES_CITYDB_HOST", "localhost"),
            port=os.getenv("SERVICES_CITYDB_EXPOSED_PORT", "5432"),
            db=os.getenv("SERVICES_CITYDB_DB", "postgres"),
        )
    )
    anon = (
        os.getenv("PGRST_DB_ANON_ROLE")
        or os.getenv("POSTGREST_DB_ANON_ROLE")
        or os.getenv("SERVICES_CITYDB_USER", "anon")
    )
    port = os.getenv("PGRST_SERVER_PORT") or os.getenv("SERVICES_POSTGREST_PORT") or "3000"

    default_conf = (
        f'db-uri = "{db_uri}"\n'
        f'db-anon-role = "{anon}"\n'
        f"server-port = {port}\n"
        f'db-schemas = ""\n'
    )
    atomic_write_text(default_conf, CONF_PATH)
    print(f"[{utcnow()}] created {CONF_PATH} with minimal defaults")

def get_user_schemas(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT nspname
            FROM pg_namespace
            WHERE nspname NOT LIKE 'pg_%'
              AND nspname <> 'information_schema'
            ORDER BY nspname;
        """)
        rows = cur.fetchall()
    schemas = [r["nspname"] for r in rows]
    schemas = [s for s in schemas if not fnmatch_any(s, EXCLUDE_SCHEMAS)]
    return schemas

def compute_signature(schemas: list[str]) -> str:
    return "|".join(schemas)

# Matches a whole line like: db-schemas = "opendata,public"
CONF_LINE_RE = re.compile(r'^\s*db-schemas\s*=\s*"(?:[^"\\]|\\.)*"\s*$', re.MULTILINE)

def render_conf_with_schemas(original: str, schemas_csv: str) -> str:
    line = f'db-schemas = "{schemas_csv}"'
    if CONF_LINE_RE.search(original):
        return CONF_LINE_RE.sub(line, original, count=1)
    if original and not original.endswith("\n"):
        original += "\n"
    return original + line + "\n"

def notify_postgrest_reload(conn):
    # Tell PostgREST to reload both config and schema cache
    with conn.cursor() as cur:
        cur.execute("SELECT pg_notify(%s, 'reload config')", (CHANNEL,))
        cur.execute("SELECT pg_notify(%s, 'reload schema')", (CHANNEL,))

# ---------------- main loop ----------------
def loop():
    backoff = 2.0
    last_sig = ""
    last_reload_t = 0.0

    while True:
        try:
            # Ensure config file exists before doing anything else
            ensure_conf_exists()

            with psycopg.connect(DSN, row_factory=dict_row) as conn:
                conn.autocommit = True
                print(f"[{utcnow()}] Connected; watching schemas → {CONF_PATH}")

                # initial apply
                schemas = get_user_schemas(conn)
                sig = compute_signature(schemas)
                conf = read_text(CONF_PATH)
                new_conf = render_conf_with_schemas(conf, ",".join(schemas))
                if new_conf != conf:
                    atomic_write_text(new_conf, CONF_PATH)
                    print(f"[{utcnow()}] wrote {CONF_PATH} (schemas: {schemas or '∅'})")
                    notify_postgrest_reload(conn)
                    last_reload_t = time.monotonic()
                last_sig = sig
                backoff = 2.0

                # poll loop
                while True:
                    time.sleep(POLL_INTERVAL)
                    schemas = get_user_schemas(conn)
                    sig = compute_signature(schemas)
                    if sig != last_sig and (time.monotonic() - last_reload_t) >= MIN_REBUILD_GAP:
                        conf = read_text(CONF_PATH)
                        new_conf = render_conf_with_schemas(conf, ",".join(schemas))
                        if new_conf != conf:
                            atomic_write_text(new_conf, CONF_PATH)
                            print(f"[{utcnow()}] schemas changed → {schemas or '∅'}; updated config")
                            notify_postgrest_reload(conn)
                            last_reload_t = time.monotonic()
                        else:
                            print(f"[{utcnow()}] schemas changed but config already in sync")
                        last_sig = sig

        except psycopg.OperationalError as e:
            print(f"[WARN] DB connection problem: {e}")
            time.sleep(backoff); backoff = min(backoff * 2, 60)
        except Exception as e:
            print(f"[ERR] Unexpected error: {e}")
            time.sleep(backoff); backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    try:
        loop()
    except KeyboardInterrupt:
        print("Exiting.")

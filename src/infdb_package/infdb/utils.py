import logging
import os
import subprocess
import tempfile
from typing import Any, Optional, Iterable

import yaml

from .client import InfdbClient
from .config import InfdbConfig


# ============================== Constants ==============================

LOGGER_NAME: str = "infdb.utils"
DEFAULT_DB_NAME: str = "postgres"
FILE_ENCODING: str = "utf-8"

# Module-level logger
log = logging.getLogger(LOGGER_NAME)


# ============================== Env helpers ==============================

def read_env(var_name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    """
    Read an environment variable with optional default and required check.
    """
    v = os.getenv(var_name, default)
    if required and (v is None or v == ""):
        log.error("Missing required env variable: %s", var_name)
        raise SystemExit(2)
    return v


def build_dsn_from_env(
    user_var: str,
    pwd_var: str,
    db_var: str,
    host_var: str,
    port_var: str,
    default_host: str,
    default_port: str,
) -> str:
    """
    Build a PostgreSQL DSN string from common environment variables.
    """
    user = read_env(user_var, required=True) or ""
    pwd = read_env(pwd_var, required=True) or ""
    db = read_env(db_var, required=True) or ""
    host = read_env(host_var, default_host) or default_host
    port = read_env(port_var, default_port) or default_port
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"


# ============================== Filesystem helpers ==============================

def ensure_dir_exists(path: str) -> str:
    """
    Ensure a directory (or a file's parent directory) exists; return absolute path.
    """
    if not path:
        raise ValueError("path must be a non-empty string")

    abs_path = os.path.abspath(path)
    is_dir_like = os.path.isdir(abs_path) or abs_path.endswith(os.sep)
    target_dir = abs_path if is_dir_like else os.path.dirname(abs_path)

    if target_dir:
        os.makedirs(target_dir, exist_ok=True)

    return abs_path


def _atomic_write(binary: bool, data: Any, output_path: str, file_mode: str | None = None, dir_mode: str | None = None) -> str:
    """
    Internal: write text/bytes atomically and optionally set chmods.
    """
    if not output_path:
        raise ValueError("output_path must be a non-empty string")

    path = output_path if os.path.isabs(output_path) else os.path.abspath(output_path)
    out_dir = os.path.dirname(path)
    os.makedirs(out_dir, exist_ok=True)

    mode = "wb" if binary else "w"
    with tempfile.NamedTemporaryFile(mode, delete=False, dir=out_dir, suffix=".tmp", encoding=None if binary else FILE_ENCODING) as tmp:
        if binary:
            tmp.write(data)                  # bytes
        else:
            tmp.write(str(data))             # text
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_name = tmp.name
    os.replace(tmp_name, path)

    if file_mode:
        os.chmod(path, int(file_mode, 8))
    if dir_mode:
        try:
            os.chmod(out_dir, int(dir_mode, 8))
        except Exception:
            pass

    return path


def atomic_write_text(text: str, output_path: str, file_mode: str | None = None, dir_mode: str | None = None) -> str:
    """
    Atomically write text to a file. Optionally apply chmod to file/dir.
    """
    return _atomic_write(binary=False, data=text, output_path=output_path, file_mode=file_mode, dir_mode=dir_mode)


def atomic_write_yaml(data: Any, output_path: str, file_mode: str | None = None, dir_mode: str | None = None) -> str:
    """
    Atomically serialize a Python object to YAML and write to a file.
    """
    yaml_text = yaml.safe_dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True)
    return atomic_write_text(yaml_text, output_path, file_mode=file_mode, dir_mode=dir_mode)


def read_text(input_path: str) -> str:
    """
    Read a text file; return empty string if it does not exist.
    """
    try:
        with open(input_path, "r", encoding=FILE_ENCODING) as f:
            return f.read()
    except FileNotFoundError:
        return ""


def write_yaml(data: Any, output_path: str) -> None:
    """
    Backward-compatible non-atomic YAML writer.
    Prefer `atomic_write_yaml` for config files.
    """
    path = output_path if os.path.isabs(output_path) else os.path.abspath(output_path)
    ensure_dir_exists(path)
    with open(path, "w", encoding=FILE_ENCODING) as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)


# ============================== Shell helper ==============================

def do_cmd(cmd: str) -> int:
    """
    Execute a shell command, streaming output to the logger.
    """
    if not cmd:
        raise ValueError("cmd must be a non-empty string")

    log.info("Executing command: %s", cmd)
    process = subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    if process.stdout:
        for line in process.stdout:
            log.info(line.rstrip())
    return_code = process.wait()
    if return_code == 0:
        log.info("Command completed successfully.")
    else:
        log.error("Command failed with return code %s", return_code)
    return return_code


# ============================== DB convenience ==============================

def do_sql_query(
    query: str,
    cfg: InfdbConfig,
    db_name: str = DEFAULT_DB_NAME,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Run a single SQL statement using InfdbClient, then close the connection.
    """
    _log = logger or log
    _log.debug("Running SQL on %s: %s", db_name, query)
    with InfdbClient(cfg, _log, db_name=db_name) as db:
        db.execute_query(query)


def get_db_engine(
    cfg: InfdbConfig,
    db_name: str = DEFAULT_DB_NAME,
    logger: Optional[logging.Logger] = None,
):
    """
    Return a SQLAlchemy Engine using InfdbClient (URL building is centralized).
    """
    _log = logger or log
    with InfdbClient(cfg, _log, db_name=db_name) as db:
        return db.get_db_engine()


# ============================== Misc helpers ==============================

def compute_signature(items: Iterable[str]) -> str:
    """
    Produce a stable signature string from an iterable of strings.
    """
    return "|".join(items)

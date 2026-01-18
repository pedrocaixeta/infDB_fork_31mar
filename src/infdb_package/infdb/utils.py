import logging
import os
import subprocess  # nosec B404
import tempfile
from typing import Any, Iterable, List, Optional, cast

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


def read_env(var_name: str, default: Optional[str] = None, required: bool = False) -> str:
    """
    Read an environment variable with optional default and required check.

    Args:
        var_name: Name of the environment variable to read.
        default: Value to return if the environment variable is not set.
        required: If True, treat a missing or empty value as an error and exit.

    Returns:
        The environment variable value, or `default` if it is not set. If `required`
            is True and the value is missing/empty, the function does not return.

    Raises:
        SystemExit: Exits with status code 2 if `required` is True and the variable
            is missing or empty.
    """
    v = os.getenv(var_name)
    is_missing = (v is None) or (v == "")
    if required and is_missing:
        log.error("Missing required env variable: %s", var_name)
        raise SystemExit(2)
    if is_missing:
        return default if default is not None else ""
    return cast(str, v)


def build_dsn_from_env(
    user_var: str,
    pwd_var: str,
    db_var: str,
    host_var: str,
    port_var: int,
) -> str:
    """
    Build a PostgreSQL DSN string from common environment variables.

    Args:
        user_var: Database username.
        pwd_var: Database password.
        db_var: Database name.
        host_var: Database host.
        port_var: Database port.

    Returns:
        A PostgreSQL connection URL (DSN) in the form:
            `postgresql://<user>:<password>@<host>:<port>/<db>`.
    """
    return f"postgresql://{user_var}:{pwd_var}@{host_var}:{port_var}/{db_var}"


# ============================== Filesystem helpers ==============================


def ensure_dir_exists(path: str) -> str:
    """
    Ensure a directory (or a file's parent directory) exists; return absolute path.

    Args:
        path: A directory path or a file path.

    Returns:
        The absolute version of `path`.

    Raises:
        ValueError: If `path` is an empty string.
    """
    if not path:
        raise ValueError("path must be a non-empty string")

    abs_path = os.path.abspath(path)
    is_dir_like = os.path.isdir(abs_path) or abs_path.endswith(os.sep)
    target_dir = abs_path if is_dir_like else os.path.dirname(abs_path)

    if target_dir:
        os.makedirs(target_dir, exist_ok=True)

    return abs_path


def _atomic_write(
    binary: bool, data: Any, output_path: str, file_mode: str | None = None, dir_mode: str | None = None
) -> str:
    """
    Internal: write text/bytes atomically and optionally set chmods.

    Args:
        binary: If True, write `data` as bytes using binary mode (`wb`).
            If False, write `str(data)` using text mode (`w`) with `FILE_ENCODING`.
        data: The content to write. Must be `bytes` when `binary` is True; otherwise
            it will be coerced to `str`.
        output_path: Destination file path (absolute or relative). Parent directories
            will be created if needed.
        file_mode: Optional file permission mode (octal string, e.g. `"644"` or `"600"`).
        dir_mode: Optional directory permission mode (octal string) to apply to the
            destination directory.

    Returns:
        The absolute path of the written file.

    Raises:
        ValueError: If `output_path` is empty.
        OSError: If writing, syncing, replacing, or chmod operations fail (except
            directory chmod failures, which are logged and ignored).
    """
    if not output_path:
        raise ValueError("output_path must be a non-empty string")

    path = output_path if os.path.isabs(output_path) else os.path.abspath(output_path)
    out_dir = os.path.dirname(path)
    os.makedirs(out_dir, exist_ok=True)

    mode = "wb" if binary else "w"
    with tempfile.NamedTemporaryFile(
        mode, delete=False, dir=out_dir, suffix=".tmp", encoding=None if binary else FILE_ENCODING
    ) as tmp:
        if binary:
            tmp.write(data)  # bytes
        else:
            tmp.write(str(data))  # text
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_name = tmp.name
    os.replace(tmp_name, path)

    if file_mode:
        os.chmod(path, int(file_mode, 8))
    if dir_mode:
        try:
            os.chmod(out_dir, int(dir_mode, 8))
        except Exception as exc:
            log.exception("Exception occurred during _atomic_write(): %s", exc)

    return path


def atomic_write_text(text: str, output_path: str, file_mode: str | None = None, dir_mode: str | None = None) -> str:
    """
    Atomically write text to a file. Optionally apply chmod to file/dir.

    Args:
        text: Text content to write.
        output_path: Destination file path (absolute or relative). Parent directories
            will be created if needed.
        file_mode: Optional file permission mode (octal string, e.g. `"644"` or `"600"`).
        dir_mode: Optional directory permission mode (octal string) to apply to the
            destination directory.

    Returns:
        The absolute path of the written file.

    Raises:
        ValueError: If `output_path` is empty.
        OSError: If writing, syncing, or replacing the file fails (and potentially
            if applying `file_mode` fails).
    """
    return _atomic_write(binary=False, data=text, output_path=output_path, file_mode=file_mode, dir_mode=dir_mode)


def atomic_write_yaml(data: Any, output_path: str, file_mode: str | None = None, dir_mode: str | None = None) -> str:
    """
    Atomically serialize a Python object to YAML and write to a file.

    Args:
        data: Python object to serialize to YAML (e.g., dict, list).
        output_path: Destination file path (absolute or relative). Parent directories
            will be created if needed.
        file_mode: Optional file permission mode (octal string, e.g. `"644"` or `"600"`).
        dir_mode: Optional directory permission mode (octal string) to apply to the
            destination directory.

    Returns:
        The absolute path of the written YAML file.

    Raises:
        ValueError: If `output_path` is empty.
        yaml.YAMLError: If the object cannot be serialized to YAML.
        OSError: If writing, syncing, or replacing the file fails (and potentially
            if applying `file_mode` fails).
    """
    yaml_text = yaml.safe_dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True)
    return atomic_write_text(yaml_text, output_path, file_mode=file_mode, dir_mode=dir_mode)


def read_text(input_path: str) -> str:
    """
    Read a text file; return empty string if it does not exist.

    Args:
        input_path: Path to the file to read.

    Returns:
        The file contents as a string. If the file does not exist, returns an
            empty string.

    Raises:
        OSError: For IO errors other than `FileNotFoundError` (e.g., permission
            denied).
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

    Args:
        data: Python object to serialize to YAML (e.g., dict, list).
        output_path: Destination file path (absolute or relative). Parent
            directories will be created if needed.

    Returns:
        None.

    Raises:
        ValueError: If `output_path` is invalid (propagated from `ensure_dir_exists`).
        yaml.YAMLError: If the object cannot be serialized to YAML.
        OSError: If the file cannot be opened or written (e.g., permission denied).
    """
    path = output_path if os.path.isabs(output_path) else os.path.abspath(output_path)
    ensure_dir_exists(path)
    with open(path, "w", encoding=FILE_ENCODING) as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)


# ============================== Shell helper ==============================


def do_cmd(cmd: str | List[str], is_shell_interpreted: bool = False) -> int:
    """
    Execute a shell command, streaming output to the logger.

    Args:
        cmd: Command to run. Can be a string or a list of strings.
        is_shell_interpreted: If True, run command through the shell.
               Default is False for security. **Warning:** Setting is_shell_interpreted=True
                 is considered unsafe in general and should be used with caution!

    Returns:
        The process exit code (0 indicates success; non-zero indicates failure).

    Raises:
        ValueError: If `cmd` is empty.
        OSError: If the process cannot be started (e.g., command not found).
    """
    if not cmd:
        raise ValueError("cmd must be a non-empty string")

    log.info("Executing command: %s", cmd)
    process = subprocess.Popen(
        cmd,
        shell=is_shell_interpreted,  # nosec B602
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

    Args:
        query: SQL statement to execute.
        cfg: `InfdbConfig` instance used to resolve database connection parameters.
        db_name: Name of the database to connect to. Defaults to `DEFAULT_DB_NAME`.
        logger: Optional logger to use for debug output. If not provided, the
            module-level logger is used.

    Raises:
        psycopg2.Error: If establishing the connection or executing the SQL fails.
        Exception: Propagates any other exceptions raised by `InfdbClient`.
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

    Args:
        cfg: `InfdbConfig` instance used to resolve database connection parameters.
        db_name: Name of the database to connect to. Defaults to `DEFAULT_DB_NAME`.
        logger: Optional logger to use for diagnostics. If not provided, the
            module-level logger is used.

    Returns:
        A SQLAlchemy `Engine` connected to the configured database.

    Raises:
        psycopg2.Error: If establishing the underlying connection fails.
        Exception: Propagates any other exceptions raised by `InfdbClient`.
    """
    _log = logger or log
    with InfdbClient(cfg, _log, db_name=db_name) as db:
        return db.get_db_engine()


# ============================== Misc helpers ==============================


def compute_signature(items: Iterable[str]) -> str:
    """
    Produce a stable signature string from an iterable of strings.

    Args:
        items: Iterable of strings to combine in order.

    Returns:
        A single string formed by joining `items` with a pipe (`|`) separator.
    """
    return "|".join(items)

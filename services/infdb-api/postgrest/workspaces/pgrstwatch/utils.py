import os
import logging
from typing import Optional, cast, Iterable #, List,

# ============================== Constants ==============================

LOGGER_NAME: str = "infdb.utils"
DEFAULT_DB_NAME: str = "postgres"
FILE_ENCODING: str = "utf-8"

# Module-level logger
log = logging.getLogger(LOGGER_NAME)

def read_text(input_path: str) -> str:
    """
    Reads a text file; returns empty string if it does not exist.

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

   
def read_env(var_name: str, default: Optional[str] = None, required: bool = False) -> str:
    """
    Reads an environment variable with optional default and required check.

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
    Builds a PostgreSQL DSN string from common environment variables.

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

# ============================== Misc helpers ==============================


def compute_signature(items: Iterable[str]) -> str:
    """
    Produces a stable signature string from an iterable of strings.

    Args:
        items: Iterable of strings to combine in order.

    Returns:
        A single string formed by joining `items` with a pipe (`|`) separator.
    """
    return "|".join(items)
from typing import Any, Dict

import subprocess

from infdb import InfDB


# ============================== Constants ==============================

TOOL_NAME: str = "infdb-init"
CONFIG_DIR: str = "configs"
CITYDB_CREATE_SCRIPT: str = "/tmp/3dcitydb/postgresql/shell-scripts/unix/create-db.sh"
PGBIN_PATH: str = "/var/lib/postgresql/17/bin"


# ============================== Helpers ===============================

def build_citydb_env(params: Dict[str, Any]) -> Dict[str, str]:
    """Build the environment mapping for the 3DCityDB setup script.

    Args:
        params: DB parameters returned by InfDB config (host, exposed_port, db, user, password, epsg).

    Returns:
        A dict of environment variables (all values coerced to strings).
    """
    return {
        "PGBIN": PGBIN_PATH,
        "PGHOST": str(params["host"]),
        "PGPORT": str(params["exposed_port"]),
        "CITYDB": str(params["db"]),
        "PGUSER": str(params["user"]),
        "PGPASSWORD": str(params["password"]),
        "SRID": str(params["epsg"]),
        "HEIGHT_EPSG": "0",
        "CHANGELOG": "no",
    }


# ============================== Entry Point ===========================

def main() -> None:
    """Initialize InfDB, assemble env, and run the 3DCityDB create script."""
    # Initialize InfDB (config + logging)
    inf = InfDB(tool_name=TOOL_NAME, config_path=CONFIG_DIR)
    log = inf.get_log()

    log.info("Starting %s tool", inf.get_toolname())

    # DB parameters for 3DCityDB install
    params = inf.infdbconfig.get_db_parameters("postgres")

    # Ensure all env values are strings
    env = build_citydb_env(params)
    log.debug("Environment variables set: %s", env)

    # Install 3DCityDB extension
    log.info("Installing 3DcityDB extension")
    subprocess.run(
        ["bash", CITYDB_CREATE_SCRIPT],
        env=env,
        check=True,
    )

    log.info("Successfully finished %s tool", inf.get_toolname())


if __name__ == "__main__":
    main()

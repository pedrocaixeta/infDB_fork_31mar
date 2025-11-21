import json
import os
import secrets
from typing import Any, Dict

import infdb
from infdb.utils import write_yaml


# ============================== Constants ==============================

DEFAULT_TOOL_NAME: str = "infdb"
DEFAULT_CONFIG_DIR: str = "configs"
DEFAULT_ENV_OUT: str = "mnt/infdb-root/.env"
DEFAULT_COMPOSE_OUT: str = "mnt/infdb-root/compose.yml"
GENERATED_DIR: str = "mnt/infdb-root/.generated/"
PGADMIN_SERVERS_OUT: str = GENERATED_DIR
PG_SERVICE_CONF_OUT: str = "mnt/infdb-root/services/qgis_webclient/"
PGADMIN_GROUP_NAME: str = "infDB"
PGADMIN_HOST: str = "postgres"
PGADMIN_PORT: int = 5432
JWT_SECRET_BYTES: int = 48
ENV_KEY_SEP: str = "_"


# One shared config object (adjust tool_name if your file is named differently)
cfg = infdb.InfdbConfig(tool_name=DEFAULT_TOOL_NAME, config_path=DEFAULT_CONFIG_DIR)


# ============================== Helpers ================================

def _flatten_dict(d: Dict[str, Any], parent_key: str = "", sep: str = ENV_KEY_SEP) -> Dict[str, Any]:
    """Flatten nested dictionary with KEY paths joined by `sep` (old behavior)."""
    items: Dict[str, Any] = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(_flatten_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items


# ============================== Writers =================================

def write_env_file(file_path: str = ".env") -> None:
    """Write configuration for services into a .env file (old behavior)."""
    path = os.path.join(file_path)
    with open(path, "w", encoding="utf-8") as f:
        flattened_config = _flatten_dict(cfg.get_config(), sep=ENV_KEY_SEP)
        for key, value in flattened_config.items():
            env_key = key.upper()
            v = "" if value is None else str(value)
            if "PATH" in env_key:
                if not os.path.isabs(v):
                    if "SERVICE" in env_key and "COMPOSE_FILE" not in env_key:
                        v = os.path.join("..", "..", v)
                    else:
                        v = os.path.join(".", v)
            f.write(f"{env_key}={v}\n")

        f.write("JWT_SECRET_KEY=" + secrets.token_hex(JWT_SECRET_BYTES) + "\n")

    print(f".env  file written to {path}")


def write_compose_file(output_path: str) -> None:
    """Auto-generate the docker compose file (old behavior)."""
    output: Dict[str, Any] = {
        "name": f"{cfg.get_value(['base', 'name'])}",
        "include": [],  # loader by default should exist
        "volumes": {},
        "networks": {
            cfg.get_value(["base", "network_name"]): {
                "name": cfg.get_value(["base, 'network_name"]) if False else cfg.get_value(["base", "network_name"]),
                "driver": "bridge",
            }
        },
    }

    services = cfg.get_value(["services"]) or {}
    for service_name, props in services.items():
        if isinstance(props, dict) and props.get("status") == "active":
            path = cfg.get_value(["services", service_name, "path", "compose_file"])
            output["include"].append(path)
    write_yaml(output, output_path)
    print(f".docker compose file written to {output_path}")


def setup_pgadmin_servers(output_path: str) -> None:
    """Build pgAdmin-compatible structure (old behavior with fixed HOST)."""
    servers_json: Dict[str, Any] = {"Servers": {}}
    pgpass_entries = []
    host = PGADMIN_HOST
    port = PGADMIN_PORT

    for server_id, service in enumerate(["postgres"], 1):
        servers_json["Servers"][server_id] = {
            "Name": service,
            "Group": PGADMIN_GROUP_NAME,
            "Host": host,
            "Port": port,
            "MaintenanceDB": cfg.get_value(["services", service, "db"]),
            "Username": cfg.get_value(["services", service, "user"]),
            "SSLMode": "prefer",
            "Password": cfg.get_value(["services", service, "password"]),
            "PassFile": "/pgadmin4/.pgpass",
        }

        pgpass_entries.append(
            f"{host}:{port}:{service}:{cfg.get_value(['services', service, 'user'])}:{cfg.get_value(['services', service, 'password'])}"
        )

    os.makedirs(output_path, exist_ok=True)
    with open(os.path.join(output_path, "servers.json"), "w", encoding="utf-8") as f:
        json.dump(servers_json, f, indent=4)

    pgpass_path = os.path.join(output_path, ".pgpass")
    with open(pgpass_path, "w", encoding="utf-8") as f:
        f.write("\n".join(pgpass_entries) + "\n")
    os.chmod(pgpass_path, 0o600)
    print(f".pgpass written to {pgpass_path}")


def write_pg_service_conf(output_path: str) -> None:
    """Write a pg_service.conf file to enable PostgreSQL connection shortcuts.

    Example:
        `psql service=infdb_citydb`
    """
    services = ["postgres"]  # , "timescaledb"
    port = 5432
    host = "postgres"

    lines = []
    for service in services:
        db = cfg.get_value(["services", service, "db"])
        user = cfg.get_value(["services", service, "user"])
        password = cfg.get_value(["services", service, "password"])
        service_name = f"infdb_{service}"

        lines.append(f"[{service_name}]")
        lines.append(f"host={host}")
        lines.append(f"port={port}")
        lines.append(f"dbname={db}")
        lines.append(f"user={user}")
        lines.append(f"password={password}")
        lines.append("")

    lines.append("[qwc_configdb]")
    lines.append("host=qwc-postgis")
    lines.append("port=5432")
    lines.append("dbname=qwc_services")
    lines.append("user=qwc_admin")
    lines.append("password=qwc_admin")
    lines.append("sslmode=disable")
    lines.append("")

    lines.append("[qwc_geodb]")
    lines.append("host=qwc-postgis")
    lines.append("port=5432")
    lines.append("dbname=qwc_services")
    lines.append("user=qwc_service_write")
    lines.append("password=qwc_service_write")
    lines.append("sslmode=disable")
    lines.append("")

    os.makedirs(output_path, exist_ok=True)
    pg_service_path = os.path.join(output_path, "pg_service.conf")
    with open(pg_service_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"pg_service.conf written to {pg_service_path}")


def create_postgres_volume() -> None:
    """Create the PostgreSQL data volume directory if it does not exist."""
    # base_path = cfg.get_value(["services", "postgres", "path", "base"])
    name = cfg.get_value(["base", "name"])
    base_path = os.path.join("mnt/infdb-data", name, "postgres")
    os.makedirs(base_path, exist_ok=True)
    print(f"PostgreSQL data volume directory ensured at {base_path}")
# Problem: Path to data volume is needed in compose.yml before this can run

# ============================== Script Entry ===========================

if __name__ == "__main__":
    write_env_file(DEFAULT_ENV_OUT)
    write_compose_file(DEFAULT_COMPOSE_OUT)

    os.makedirs(GENERATED_DIR, exist_ok=True)
    setup_pgadmin_servers(PGADMIN_SERVERS_OUT)
    # write_pg_service_conf("mnt/infdb-root/.generated/")
    write_pg_service_conf(PG_SERVICE_CONF_OUT)
    create_postgres_volume()

    print("Setup completed successfully. Configuration files generated.")

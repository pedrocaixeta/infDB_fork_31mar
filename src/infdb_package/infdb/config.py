import logging
import os
import re
from copy import deepcopy
from typing import Any, Dict, List, Optional

import yaml

# ============================== Constants ==============================

CONFIG_FILE_TEMPLATE: str = "config-{tool}.yml"
DATA_BASE_DIR: str = os.path.join("..", "data")
SETUP_BASE_DIR: str = "."
FILE_ENCODING: str = "utf-8"


class InfdbConfig:
    """Read and resolve tool-specific YAML config with optional InfDB base merge."""

    def __init__(self, tool_name: str, config_basedir: str) -> None:
        """Initializes the configuration for a tool.

        Args:
            tool_name: The tool identifier (used to select the YAML file and section).
            config_basedir: Base directory containing config files (defaults to 'configs').
        """
        self.tool_name: str = tool_name
        self.log: logging.Logger = logging.getLogger(__name__)
        self.config_path: str = os.path.join(
            config_basedir,
            CONFIG_FILE_TEMPLATE.format(tool=tool_name),
        )
        self._CONFIG: Dict[str, Any] = self._merge_configs(self.config_path)

    def __str__(self) -> str:
        """Returns a string representation of the InfdbConfig."""
        return f"InfdbConfig(tool='{self.tool_name}', path='{self.config_path}')"

    # ---------------- internal helpers ----------------

    def _load_config(self, path: str) -> Dict[str, Any]:
        """Loads a YAML file. Raises FileNotFoundError if the file is missing."""
        if os.path.exists(path):
            with open(path, "r", encoding=FILE_ENCODING) as file:
                return yaml.safe_load(file) or {}
        else:
            self.log.debug("Config file '%s' not found.", path)
            raise FileNotFoundError(f"Config file '{path}' not found.")

    def _merge_configs(self, base_path: str) -> Dict[str, Any]:
        """Loads tool config and (optionally) merges shared InfDB base config, quietly."""
        self.log.debug("Loading configuration from '%s'", base_path)
        configs = self._load_config(base_path)
        if not configs:
            return {}

        return self._resolve_yaml_placeholders(configs)

    def _flatten_dict(self, data: Dict[str, Any], parent_key: str = "", sep: str = "/") -> Dict[str, Any]:
        """Flattens nested dictionaries into path-like keys."""
        items: Dict[str, Any] = {}
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, dict):
                items.update(self._flatten_dict(value, parent_key=new_key, sep=sep))
            else:
                items[new_key] = value
        return items

    def _replace_placeholders(self, data: Any, flat_map: Dict[str, Any]) -> Any:
        """Recursively replaces {placeholders} in strings using a flattened map."""
        if isinstance(data, dict):
            return {k: self._replace_placeholders(v, flat_map) for k, v in data.items()}
        if isinstance(data, list):
            return [self._replace_placeholders(item, flat_map) for item in data]
        if isinstance(data, str):
            pattern = re.compile(r"{([^{}]+)}")
            out = data
            while True:
                match = pattern.search(out)
                if not match:
                    break
                key = match.group(1)
                replacement = flat_map.get(key)
                if replacement is None:
                    break
                out = out.replace(f"{{{key}}}", str(replacement))
            return out
        return data

    def _resolve_yaml_placeholders(self, yaml_data: Dict[str, Any]) -> Dict[str, Any]:
        """Resolves intra-file {placeholders} using flattened key/value paths."""
        flat_map = self._flatten_dict(yaml_data)
        return self._replace_placeholders(deepcopy(yaml_data), flat_map)

    # ---------------- public API ----------------

    def get_config(self) -> Dict[str, Any]:
        """Returns the fully merged and resolved configuration dictionary."""
        return self._CONFIG

    def get_value(self, keys: List[str]) -> Any:
        """Safely traverses nested keys; returns None if the path is missing.

        Args:
            keys: Ordered key path within the configuration.

        Returns:
            The value at the specified path, or None if any segment is missing.

        Raises:
            ValueError: If keys is empty.
        """
        if not keys:
            raise ValueError("keys must be a non-empty list")
        element: Any = self.get_config()
        for key in keys:
            if not isinstance(element, dict) or key not in element:
                return None
            element = element.get(key, {})
        return element

    def get_path(self, keys: List[str], type: str) -> str:
        """Resolves a path from config and maps it to a filesystem location.

        Args:
            keys: Ordered key path within the configuration.
            type: One of {'loader', 'heat', 'package', 'setup'} controlling base dir.

        Returns:
            Absolute filesystem path derived from the config value.
        """
        path = self.get_value(keys)
        if not os.path.isabs(path):
            path = os.path.join(DATA_BASE_DIR, path)  # mounted data dir within docker
        path = os.path.abspath(path)
        return path

    @staticmethod
    def get_root_path() -> str:
        """Returns the project root path (two levels up from this file)."""
        return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    def get_db_parameters(self, db_name: str = "postgres") -> Dict[str, str]:
        """Returns database connection parameters for a given service from config-toolname.yml.

        Adopt it from environment variables if set to "None".
        Host is set to "host.docker.internal" if "None".

        Args:
            db_name: Name of the DB service section to read.

        Returns:
            Final parameters dictionary for the requested service.
        """

        db_params_service = self.get_value([self.tool_name, "hosts", db_name])
        for key in db_params_service:
            if db_params_service[key] == "None":
                if key == "host":
                    db_params_service[key] = "host.docker.internal"
                else:
                    db_params_service[key] = os.getenv(f"SERVICES_{db_name.upper()}_{key.upper()}")

        return db_params_service

    def get_env_parameters(self, key, infdb) -> Optional[str]:
        """Returns a dictionary of environment variables for this tool.

        Args:
            key: Environment variable name (case-insensitive).
            infdb: An InfDB object used for logging.

        Returns:
            A dictionary of environment variables.

        Raises:
            ValueError: If the environment variable ``key.upper()`` is not set.
        """

        env_param = os.getenv(key.upper())
        if env_param is None:
            infdb.get_logger().error(f"Environment variable '{key.upper()}' is not set.")
            raise ValueError(f"Environment variable '{key.upper()}' is not set.")

        return env_param

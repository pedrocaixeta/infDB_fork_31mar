import logging
import os
import re
from copy import deepcopy
from typing import Any, Dict, List, Optional

import yaml


# ============================== Constants ==============================

DEFAULT_CONFIG_DIR: str = "configs"
CONFIG_FILE_TEMPLATE: str = "config-{tool}.yml"
INFDB_BASE_DIR: str = os.path.join("mnt", "configs-infdb")
LOADER_BASE_DIR: str = os.path.join("mnt", "data")
SETUP_BASE_DIR: str = "."
FILE_ENCODING: str = "utf-8"


class InfdbConfig:
    """Read and resolve tool-specific YAML config with optional InfDB base merge."""

    def __init__(self, tool_name: str, config_path: Optional[str] = DEFAULT_CONFIG_DIR) -> None:
        """Initialize configuration for a tool.

        Args:
            tool_name: The tool identifier (used to select the YAML file and section).
            config_path: Base directory containing config files (defaults to 'configs').
        """
        self.tool_name: str = tool_name
        self.log: logging.Logger = logging.getLogger(__name__)
        base_dir = config_path
        self.config_path: str = os.path.join(base_dir, CONFIG_FILE_TEMPLATE.format(tool=tool_name))
        self._CONFIG: Dict[str, Any] = self._merge_configs(self.config_path)

    def __str__(self) -> str:
        return f"InfdbConfig(tool='{self.tool_name}', path='{self.config_path}')"

    # ---------------- internal helpers ----------------

    def _load_config(self, path: str) -> Dict[str, Any]:
        """Load a YAML file. Raise FileNotFoundError if the file is missing."""
        if os.path.exists(path):
            with open(path, "r", encoding=FILE_ENCODING) as file:
                return yaml.safe_load(file) or {}
        else:
            self.log.debug("Config file '%s' not found.", path)
            raise FileNotFoundError(f"Config file '{path}' not found.")

    def _merge_configs(self, base_path: str) -> Dict[str, Any]:
        """Load tool config and (optionally) merge shared InfDB base config, quietly."""
        self.log.debug("Loading configuration from '%s'", base_path)
        configs = self._load_config(base_path)
        if not configs:
            return {}

        # OPTIONAL merge of shared InfDB config: skip silently if not present
        tool_block = configs.get(self.tool_name) or {}
        base_filename: Optional[str] = tool_block.get("config-infdb")
        if base_filename:
            base_path_infdb = os.path.join(INFDB_BASE_DIR, base_filename)
            self.log.debug("Merging InfDB base config from '%s'", base_path_infdb)
            if os.path.exists(base_path_infdb):
                configs.update(self._load_config(base_path_infdb))
            else:
                self.log.debug("InfDB base config '%s' not found (skipping).", base_path_infdb)
        else:
            self.log.debug("No '%s.config-infdb' defined — skipping base merge.", self.tool_name)

        return self._resolve_yaml_placeholders(configs)

    def _flatten_dict(self, data: Dict[str, Any], parent_key: str = "", sep: str = "/") -> Dict[str, Any]:
        """Flatten nested dictionaries into path-like keys."""
        items: Dict[str, Any] = {}
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, dict):
                items.update(self._flatten_dict(value, parent_key=new_key, sep=sep))
            else:
                items[new_key] = value
        return items

    def _replace_placeholders(self, data: Any, flat_map: Dict[str, Any]) -> Any:
        """Recursively replace {placeholders} in strings using a flattened map."""
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
        """Resolve intra-file {placeholders} using flattened key/value paths."""
        flat_map = self._flatten_dict(yaml_data)
        return self._replace_placeholders(deepcopy(yaml_data), flat_map)

    # ---------------- public API ----------------

    def get_config(self) -> Dict[str, Any]:
        """Return the fully merged and resolved configuration dictionary."""
        return self._CONFIG

    def get_value(self, keys: List[str]) -> Any:
        """Safely traverse nested keys; returns None if the path is missing.

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
        """Resolve a path from config and map it to a filesystem location.

        Args:
            keys: Ordered key path within the configuration.
            type: One of {'loader', 'heat', 'package', 'setup'} controlling base dir.

        Returns:
            Absolute filesystem path derived from the config value.
        """
        path = self.get_value(keys)
        if not os.path.isabs(path):
            if type == "loader":
                path = os.path.join(LOADER_BASE_DIR, path)
            elif type == "heat" or type == "package":
                path = os.path.join(self.get_root_path(), path)
            elif type == "setup":
                print("We are in the setup yaaaay!!!")
                path = os.path.join(SETUP_BASE_DIR, path)
        path = os.path.abspath(path)
        return path

    @staticmethod
    def get_root_path() -> str:
        """Return the project root path (two levels up from this file)."""
        return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    def get_db_parameters(self, service_name: str="postgres") -> Dict[str, Any]:
        """Merge DB params: tool hosts override services; host defaults to host.docker.internal.

        Args:
            service_name: Name of the DB service section to read.

        Returns:
            Final parameters dictionary for the requested service.
        """
        dict_config = self.get_config()
        parameters_loader: Dict[str, Any] = self.get_value([self.tool_name, "hosts", service_name]) or {}

        if "services" in dict_config:
            parameters: Dict[str, Any] = dict(self.get_value(["services", service_name]) or {})
            for key, loader_val in (parameters_loader or {}).items():
                if key == "host":
                    parameters[key] = "host.docker.internal"
                elif loader_val not in (None, "None"):
                    parameters[key] = loader_val
        else:
            parameters = parameters_loader

        return parameters or {}

import logging
import os
import re
import yaml
from copy import deepcopy


class InfdbConfig:
    """Class to handle InfDB configuration loading and retrieval."""

    def __init__(self, tool_name, config_path=None):
        self.tool_name = tool_name
        self.log = logging.getLogger(__name__)
        self.config_path = os.path.join(config_path, f"config-{tool_name}.yml")
        
        
        self._CONFIG = self._merge_configs(self.config_path)

    
    def __str__(self):
        return f"InfdbConfig for tool '{self.tool_name}' with config: {self._CONFIG}"

    
    def _load_config(self, path: str):
        if os.path.exists(path):
            with open(path, "r") as file:
                return yaml.safe_load(file)
        else:
            self.log.error(f"Configuration file '{path}' does not exist.")
            return None


    def _merge_configs(self, base_path):
        # base_path = os.path.join(
        #     "configs", "config-preprocessor.yml"
        # )  # hardcoded in compose.yml btw. .env file
        logging.debug(f"Loading configuration from '{base_path}'")
        logging.debug(f"File in '{base_path}': '{os.listdir(os.path.dirname(base_path))}'")

        # first get the base config
        configs = self._load_config(base_path)

        # Load sub configs defined under config.yaml configs field
        # dir_infdb_config = os.environ.get("CONFIG_INFDB_PATH", "")
        filename = configs[self.tool_name]["config-infdb"]
        path_infdb_config = os.path.join(
            "mnt", "configs-infdb", filename
        )  # hardcoded because of docker mount in compose.yml
        self.log.debug(f"Loading configuration from '{path_infdb_config}'")
        if os.path.exists(path_infdb_config):
            configs.update(self._load_config(path_infdb_config))
        else:
            self.log.error(f"Failed to load {path_infdb_config}")

        # Resolve placeholders in the config
        config_resolved = self._resolve_yaml_placeholders(configs)
        return config_resolved


    def get_value(self, keys):
        if not keys:
            raise ValueError("keys must be a non-empty list")

        config = self.get_config()

        element = config
        for key in keys:
            if key not in element:
                self.log.error(f"Key '{key}' not found in configuration.")
                return None

            element = element.get(key, {})

        return element


    def get_path(self, keys):
        path = self.get_value(keys)
        if not os.path.isabs(path):
            path = os.path.join(self.get_root_path(), path)
        path = os.path.abspath(path)
        return path


    # def get_root_path(self):
    #     # Get project root path
    #     root_path = os.path.dirname(
    #         os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    #     )
    #     return root_path


    def _flatten_dict(self, d, parent_key="", sep="/"):
        """Flatten nested dictionary with keys joined by /."""
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(self._flatten_dict(v, parent_key=new_key, sep=sep))
            else:
                items[new_key] = v
        return items


    def _replace_placeholders(self, data, flat_map):
        """Recursively replace placeholders like {a/b} using flat_map."""
        if isinstance(data, dict):
            return {k: self._replace_placeholders(v, flat_map) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._replace_placeholders(item, flat_map) for item in data]
        elif isinstance(data, str):
            pattern = re.compile(r"{([^{}]+)}")
            while True:
                match = pattern.search(data)
                if not match:
                    break
                key = match.group(1)
                replacement = flat_map.get(key)
                if replacement is None:
                    break  # unresolved
                data = data.replace(f"{{{key}}}", str(replacement))
            return data
        else:
            return data


    def _resolve_yaml_placeholders(self, yaml_data: dict) -> dict:
        """Resolve {a/b} placeholders in a YAML dictionary."""
        flat_map = self._flatten_dict(yaml_data)
        resolved = self._replace_placeholders(deepcopy(yaml_data), flat_map)
        return resolved


    def get_config(self):
        config = self._CONFIG
        return config


    def get_db_parameters(self, service_name: str):
        """
        Retrieves and merges database connection parameters for a given service.
        This function loads parameters from two sources: a configuration loader and an optional
        'config-infdb' configuration. If both sources are available, parameters from the loader
        override those from 'config-infdb'. The 'host' parameter is set to 'host.docker.internal'
        by default. Logs are generated for overridden keys and missing parameters.
        Args:
            service_name (str): The name of the service for which to retrieve database parameters.
        Returns:
            dict: A dictionary containing the merged database connection parameters for the specified service.
        Logs:
            - Debug messages for configuration source and overridden keys.
            - Error messages if any required parameter is missing.
        """

        parameters_loader = self.get_value([self.tool_name, "hosts", service_name])

        # Adopt settings if config-infdb exists
        dict_config = self.get_config()
        if "services" in dict_config:
            parameters = self.get_value(["services", service_name])
            self.log.debug(f"Using infdb configuration for: {service_name}")

            # Override config-infdb by config-loader
            keys = parameters_loader.keys()
            for key in keys:
                if key == "host":
                    parameters[key] = "host.docker.internal"  # default to localhost

                if parameters_loader[key] != "None":
                    parameters[key] = parameters_loader[key]
                    self.log.debug(f"Key overridden: key = {parameters_loader[key]}")
        else:
            # Use settings from config-loader
            parameters = parameters_loader
            self.log.debug(f"Using loader configuration for: {service_name}")

        # Check if parameters are found
        for key in parameters.keys():
            if parameters[key] is None:
                self.log.error(f"Service '{service_name}' not found in configuration.")

        return parameters

"""GeneratorConfig - load và validate config từ YAML."""
from typing import Any, Dict, Optional
from pathlib import Path
import yaml

class GeneratorConfig:
    def __init__(self, config_path: Path) -> None:
        """
        Initialize the GeneratorConfig instance.
        Args:
            config_path (Path): Path to the YAML configuration file.
        Returns:
            None
        """
        # TODO: Initialize YAML parser and load the config file
        self.config_path = config_path
        self.config = yaml.safe_load(config_path.read_text())

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """
        Retrieve a configuration value by key.
        Args:
            key (str): The configuration key to look up.
            default (Optional[Any]): Default value to return if the key is not found.
        Returns:
            Any: The configuration value or the default value.
        """
        return self.config.get(key, default)
    def as_dict(self) -> Dict[str, Any]:
        """
        Get the entire configuration as a dictionary.
        Returns:
            Dict[str, Any]: The complete configuration dictionary.
        """
        return self.config

    def summary(self) -> str:
        """
        Generate a summary string of the current configuration.
        Returns:
            str: A formatted summary of the configuration.
        """
        content = yaml.dump(self.config, indent=2, sort_keys=False)
        return f"Config Summary:\n{content}"
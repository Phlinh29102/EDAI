"""Pipeline configuration helpers."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from data_generator.core.config import GeneratorConfig


DEFAULT_CONFIG_PATH = Path("config/default.yaml")


def load_config(config_path: str | Path = DEFAULT_CONFIG_PATH) -> GeneratorConfig:
    """Load a YAML pipeline configuration file."""
    return GeneratorConfig(Path(config_path))


def get_data_dir(
    config: GeneratorConfig,
    key: str,
    default: Optional[str] = None,
) -> str:
    """Return a configured data directory by key."""
    data_dir: Dict[str, Any] = config.get("data_dir", {})
    value = data_dir.get(key, default)
    if value is None:
        raise KeyError(f"Missing data_dir.{key}")
    return str(value)

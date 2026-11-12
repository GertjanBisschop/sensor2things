"""Configuration schema and loading logic using Pydantic."""

import json
import logging
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from pipeline.error import ConfigError


class LogLevel(StrEnum):
    """Logging level for the application."""

    debug = "debug"
    info = "info"
    warning = "warning"
    error = "error"
    critical = "critical"

    def as_logging_level(self) -> int:
        """Converts the LogLevel enum to a string suitable for logging."""

        match self:
            case self.debug:
                return logging.DEBUG
            case self.info:
                return logging.INFO
            case self.warning:
                return logging.WARNING
            case self.error:
                return logging.ERROR
            case self.critical:
                return logging.CRITICAL


class Config(BaseModel):
    """Configuration for the application"""

    log_level: LogLevel = Field(
        default=LogLevel.info,
        description="Logging level for the application.",
    )
    source: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        description="Configuration for the source connector",
    )
    sink: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        description="Configuration for the sink connector",
    )
    convertor: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        description="Configuration for the convertor connector",
    )

    @staticmethod
    def from_path(path: Path) -> Config:
        """Load configuration from a file (yaml or json)"""
        if not path.exists() or not path.is_file():
            raise ConfigError(f'"{path}" is not a valid config file!')

        with path.open() as ifh:
            match path.suffix.lower():
                case ".yml" | ".yaml":
                    config_dict = yaml.safe_load(ifh) or {}
                case ".json":
                    config_dict = json.load(ifh)
                case _:
                    raise ConfigError(f'"{path}" has an unsupported file type!')

        return Config.model_validate(config_dict)

"""Abstract base classes for pipeline artifacts."""

import asyncio
from abc import ABC, abstractmethod, abstractproperty
from typing import Any

from pydantic import BaseModel

from pipeline.utils import classproperty


class BaseArtifact(ABC):
    """Abstract base class for pipeline artifacts."""

    @classproperty
    @abstractproperty
    def name(cls) -> str:
        """Name of the artifact."""
        raise NotImplementedError

    @abstractmethod
    async def run(self) -> None:
        """Start running the pipeline artifact logic."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop running the pipeline artifact logic."""

    @abstractmethod
    async def health_check(self) -> bool:
        """Check the health status of the pipeline artifact."""


class BaseSourceConnector[ConfigType: BaseModel](BaseArtifact):
    """Abstract interface for source data ingestion connectors."""

    def __init__(self, queue: asyncio.Queue[Any], config: ConfigType | None = None) -> None:
        self._queue = queue
        self._config = config
        self._running = False


class BaseConvertor[ConfigType: BaseModel](BaseArtifact):
    """Abstract interface for data convertors bridging source and sink queues."""

    def __init__(
        self,
        source_queue: asyncio.Queue[Any],
        sink_queue: asyncio.Queue[Any],
        config: ConfigType | None = None,
    ) -> None:
        self._source_queue = source_queue
        self._sink_queue = sink_queue
        self._config = config
        self._running = False


class BaseSinkConnector[ConfigType: BaseModel](BaseArtifact):
    """Abstract interface for sink data dispatching connectors."""

    def __init__(self, queue: asyncio.Queue[Any], config: ConfigType | None = None) -> None:
        self._queue = queue
        self._config = config
        self._running = False

"""Pipeline orchestrator coordinating source connectors, convertors, and sink connectors."""

import asyncio
import logging
from typing import ClassVar

from pipeline.error import (
    ArtifactNotFoundError,
    OperationNotSupportedError,
)
from pipeline.models import (
    BaseConvertor,
    BaseSinkConnector,
    BaseSourceConnector,
    Config,
)
from pipeline.registry import CONVERTORS, SINKS, SOURCES

log = logging.getLogger(__name__)


class Orchestrator:
    """Orchestrates asynchronous data ingestion, conversion, and dispatch."""

    _convertors: ClassVar[dict[str, type[BaseConvertor]]] = CONVERTORS
    _sources: ClassVar[dict[str, type[BaseSourceConnector]]] = SOURCES
    _sinks: ClassVar[dict[str, type[BaseSinkConnector]]] = SINKS

    def __init__(self, config: Config) -> None:
        self._config = config
        source_queue = asyncio.Queue()
        sink_queue = asyncio.Queue()

        source_factory, source_config = self._get_source_connector_factory()
        self.source_connector = source_factory(queue=source_queue, config=source_config)
        convertor_factory, convertor_config = self._get_convertor_factory()
        self.convertor = convertor_factory(
            source_queue=source_queue,
            sink_queue=sink_queue,
            config=convertor_config,
        )
        sink_factory, sink_config = self._get_sink_connector_factory()
        self.sink_connector = sink_factory(
            queue=sink_queue,
            config=sink_config,
        )
        self._tasks: list[asyncio.Task[None]] = []
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    async def run(self) -> None:
        """Run the complete pipeline until interrupted or stopped."""
        self._running = True
        log.info("Starting pipeline Orchestration ...")
        # await self.initialize_thing()

        source_task = asyncio.create_task(self.source_connector.run())
        convertor_task = asyncio.create_task(self.convertor.run())
        sink_task = asyncio.create_task(self.sink_connector.run())
        self._tasks = [source_task, convertor_task, sink_task]

        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            log.info("Pipeline received cancellation signal.")
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop all pipeline workers gracefully."""
        self._running = False
        await self.source_connector.stop()
        await self.convertor.stop()
        await self.sink_connector.stop()
        for task in self._tasks:
            if not task.done():
                task.cancel()
        log.info("Pipeline Orchestrator stopped.")

    def _get_source_connector_factory(self) -> tuple[type[BaseSourceConnector], dict]:
        """Resolve and return the registered source connector class and its configuration dictionary."""
        connector_names = self._config.source.keys()
        if len(connector_names) != 1:
            raise OperationNotSupportedError("Only one source connector is supported.")

        connector_name = next(iter(connector_names))
        if connector_name not in self._sources:
            raise ArtifactNotFoundError(f"Source connector {connector_name} not found.")

        return self._sources[connector_name], self._config.source[connector_name]

    def _get_sink_connector_factory(self) -> tuple[type[BaseSinkConnector], dict]:
        """Resolve and return the registered sink connector class and its configuration dictionary."""
        connector_names = self._config.sink.keys()
        if len(connector_names) != 1:
            raise OperationNotSupportedError("Only one sink connector is supported.")

        connector_name = next(iter(connector_names))
        if connector_name not in self._sinks:
            raise ArtifactNotFoundError(f"Sink connector {connector_name} not found.")

        return self._sinks[connector_name], self._config.sink[connector_name]

    def _get_convertor_factory(self) -> tuple[type[BaseConvertor], dict]:
        """Resolve and return the registered convertor class and its configuration dictionary."""
        connector_names = self._config.convertor.keys()
        if len(connector_names) != 1:
            raise OperationNotSupportedError("Only one convertor is supported.")

        connector_name = next(iter(connector_names))
        if connector_name not in self._convertors:
            raise ArtifactNotFoundError(f"Convertor {connector_name} not found.")

        return self._convertors[connector_name], self._config.convertor[connector_name]

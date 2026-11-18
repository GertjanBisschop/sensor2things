"""Pipeline orchestrator coordinating source connectors, convertors, and sink connectors."""

import asyncio
import logging
from typing import Any, ClassVar

from pipeline.error import (
    ArtifactNotFoundError,
    ConvertorError,
    OperationNotSupportedError,
    SinkConnectorError,
    SourceConnectorError,
)
from pipeline.models import (
    BaseArtifact,
    BaseConvertor,
    BaseSinkConnector,
    BaseSourceConnector,
    Config,
)
from pipeline.registry import CONVERTORS, SINKS, SOURCES

logger = logging.getLogger(__name__)


def _get_artifact_factory[T: BaseArtifact](
    artifacts: dict[str, type[T]],
    artifact_config: dict[str, Any],
    artifact_type: str,
) -> tuple[type[T], dict]:
    artifact_names = artifact_config.keys()
    if len(artifact_names) != 1:
        raise OperationNotSupportedError(f"Only one artifact of type {artifact_type} is supported.")

    artifact_name = next(iter(artifact_names))
    if artifact_name not in artifacts:
        raise ArtifactNotFoundError(f"Artifact {artifact_name} of type {artifact_type} not found.")

    return artifacts[artifact_name], artifact_config[artifact_name]


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
        logger.info("Starting pipeline Orchestration ...")

        source_task = asyncio.create_task(self.source_connector.run())
        convertor_task = asyncio.create_task(self.convertor.run())
        sink_task = asyncio.create_task(self.sink_connector.run())
        self._tasks = [source_task, convertor_task, sink_task]

        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            logger.info("Pipeline received cancellation signal.")
        except SourceConnectorError as e:
            logger.error(f"Source connector exited: {e}")
        except SinkConnectorError as e:
            logger.error(f"Sink connector exited: {e}")
        except ConvertorError as e:
            logger.error(f"Convertor exited: {e}")
        finally:
            logger.info("Pipeline Orchestrator stopping.")
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
        logger.info("Pipeline Orchestrator stopped.")

    def _get_source_connector_factory(self) -> tuple[type[BaseSourceConnector], dict]:
        """Resolve and return the registered source connector class and its configuration dictionary."""
        return _get_artifact_factory(self._sources, self._config.source, "source")

    def _get_sink_connector_factory(self) -> tuple[type[BaseSinkConnector], dict]:
        """Resolve and return the registered sink connector class and its configuration dictionary."""
        return _get_artifact_factory(self._sinks, self._config.sink, "sink")

    def _get_convertor_factory(self) -> tuple[type[BaseConvertor], dict]:
        """Resolve and return the registered convertor class and its configuration dictionary."""
        return _get_artifact_factory(self._convertors, self._config.convertor, "convertor")

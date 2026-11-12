import asyncio
import logging

from pipeline.models import (
    BaseConvertor,
    ST2STConfig,
    STUpdate,
)
from pipeline.registry import register_pipeline_artifact
from pipeline.utils import classproperty

logger = logging.getLogger(__name__)


@register_pipeline_artifact
class SensorThingsToSensorThingsConvertor(BaseConvertor[ST2STConfig]):
    """Asynchronous convertor to convert SensorThings to SensorThings"""

    _config: ST2STConfig

    def __init__(
        self,
        source_queue: asyncio.Queue[STUpdate],
        sink_queue: asyncio.Queue[STUpdate],
        config: ST2STConfig,
    ) -> None:
        super().__init__(source_queue=source_queue, sink_queue=sink_queue, config=config)

    @classproperty
    def name(cls) -> str:
        return "sensorthings_sensorthings"

    async def health_check(self) -> bool:
        """Check if convertor is healthy."""
        return True

    async def run(self) -> None:
        """Start reading from source queue, converting, and writing to sink queue."""
        logger.info("SensorThings to SensorThings convertor worker starting.")
        self._running = True
        while self._running:
            try:
                st_update = await asyncio.wait_for(self._source_queue.get(), timeout=0.1)
                await self._sink_queue.put(st_update)
                self._source_queue.task_done()
            except TimeoutError:
                continue
            except asyncio.CancelledError:
                break
        logger.info("SensorThings to SensorThings convertor worker stopped.")

    async def stop(self) -> None:
        """Stop conversion loop."""
        logger.debug("SensorThings to SensorThings convertor worker stopping.")
        self._running = False

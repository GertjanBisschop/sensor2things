"""SignalK source connector for ingesting messages over WebSocket & REST."""

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta

import websockets
from pydantic import ValidationError

from pipeline.models import (
    BaseSourceConnector,
    SKConfig,
    SKDelta,
    SKDeltaSourceType,
    SKDeltaUpdate,
    SKHello,
    SKObservedProperty,
    SKPosition,
    SKSensor,
    SKUpdate,
    SKUpdateType,
    SKVessel,
)
from pipeline.registry import register_pipeline_artifact
from pipeline.utils import (
    SignalKRestAPI,
    SignalKRestAPIError,
    classproperty,
)

NAVIGATION_PATH = "navigation.position"

logger = logging.getLogger(__name__)


@register_pipeline_artifact
class SignalKConnector(BaseSourceConnector[SKConfig]):
    """Asynchronous source connector for streaming SignalK data into an asyncio queue."""

    _config: SKConfig

    def __init__(
        self,
        queue: asyncio.Queue[SKUpdate],
        config: SKConfig,
    ) -> None:
        super().__init__(queue=queue, config=config)

        self._max_msg_age = timedelta(minutes=config.max_msg_age_minutes)
        self._known_sensors = set()
        self._known_observations: dict[str, set[str]] = {}
        self._rest_api = SignalKRestAPI(config)
        self._ws_task: asyncio.Task[None] | None = None
        self._ws_url: str = f"{config.ws_base_url}/stream?subscribe=none&token="

    @classproperty
    def name(cls) -> str:
        return "signalk"

    async def health_check(self) -> bool:
        """Check if SignalK REST API is reachable."""
        try:
            await self._rest_api.get_identity()
        except SignalKRestAPIError:
            return False

        return True

    async def run(self) -> None:
        """Start streaming SignalK delta messages into the queue."""
        logger.info("SignalK source connector worker starting")
        self._running = True
        try:
            await self._send_initial_vessel_state()

            token = await self._rest_api.get_token()
            ws_url = f"{self._ws_url}{token}"
            async with websockets.connect(ws_url) as ws:
                hello_msg = await ws.recv()
                _ = self._process_hello_message(hello_msg)

                await self._unsubscribe(ws)
                await self._subscribe(ws)
                logger.info("Subscription active, streaming SignalK deltas...")

                async for msg in ws:
                    logger.debug(f"received message: {msg}")
                    if not self._running:
                        break

                    delta = self._process_delta_message(msg)
                    if delta is not None:
                        # Ensure we send over the required observation metadata
                        for update in delta.updates:
                            await self._send_sensor_metadata(delta.context, update)
                            await self._send_observation_metadata(delta.context, update)

                        # Send the actual delta
                        await self._queue.put(
                            SKUpdate(
                                type=SKUpdateType.DELTA,
                                payload=delta,
                            )
                        )
        except SignalKRestAPIError as e:
            logger.error(f"SignalK REST API error: {e}")
        except asyncio.CancelledError:
            logger.info("SignalK source connector task was cancelled.")
        except websockets.WebSocketException as e:
            logger.error(f"WebSocket connection error: {e}")
        finally:
            logger.info("SignalK source connector worker stopped")
            self._running = False

    async def stop(self) -> None:
        """Stop ingestion loop."""
        logger.debug("SignalK source connector worker stopping")
        self._running = False
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()

    async def _send_initial_vessel_state(self):
        identity = await self._rest_api.get_identity()
        position = await self._get_vessel_position()
        name = await self._get_vessel_name()

        vessel = SKVessel(id=identity, name=name, position=position)
        logger.debug(f"sending vessel update: {vessel}")

        await self._queue.put(SKUpdate(type=SKUpdateType.VESSEL_METADATA, payload=vessel))

    async def _get_vessel_name(self) -> str | None:
        """Fetch vessel name from the SignalK REST API."""
        try:
            name = await self._rest_api.get_name()
        except SignalKRestAPIError as e:
            logger.warning(f"Failed to retrieve vessel name: {e}")
            return None

        return name

    async def _get_vessel_position(self) -> SKPosition | None:
        """Fetch vessel position from the SignalK REST API."""
        try:
            name = await self._rest_api.get_position()
        except SignalKRestAPIError as e:
            logger.warning(f"Failed to retrieve vessel position: {e}")
            return None

        return name

    def _process_hello_message(self, hello_msg: str | bytes) -> SKHello | None:
        """Parse and validate the initial server hello message received upon WebSocket connection."""
        try:
            return SKHello.model_validate_json(hello_msg)
        except ValidationError as e:
            logger.error(f"Invalid hello message: {e}")

        return None

    def _process_delta_message(self, msg: str | bytes) -> SKDelta | None:
        """Parse and filter a SignalK Delta JSON message based on configured message age thresholds."""
        try:
            delta = SKDelta.model_validate_json(msg)
            now = datetime.now(UTC)
            updates = []
            for update in delta.updates:
                if update.timestamp and now - update.timestamp > self._max_msg_age:
                    continue
                updates.append(update)

            delta.updates = updates
            logger.debug(f"Retained {len(updates)} updates for processing")

            return delta if updates else None
        except ValidationError as e:
            logger.error(f"Invalid delta message: {e} ({msg}). Skipping!")

        return None

    async def _unsubscribe(self, ws: websockets.ClientConnection) -> None:
        """Send an unsubscribe command over the WebSocket to clear default subscriptions."""
        await ws.send(json.dumps({"context": "*", "unsubscribe": [{"path": "*"}]}))

    async def _subscribe(self, ws: websockets.ClientConnection) -> None:
        """Send subscription requests over the WebSocket according to the configured paths and context."""
        subscriptions = self._config.subscriptions
        await ws.send(
            json.dumps(
                {
                    "context": subscriptions.context,
                    "subscribe": [sub.model_dump() for sub in subscriptions.subscriptions],
                }
            )
        )

    async def _send_sensor_metadata(self, context: str, update: SKDeltaUpdate) -> None:
        """Lazily fetch sensor metadata via REST and push a SENSOR_METADATA update if not seen before."""
        sensor_label = update.source.label
        sensor_source = (
            update.source.src
            if update.source.type == SKDeltaSourceType.NMEA2000
            else update.source.talker
        )

        # Skip when the delta contains only navigation updates
        if all(value.path.startswith(NAVIGATION_PATH) for value in update.values):
            return

        sensor_name = f"{sensor_label}.{sensor_source}"
        if sensor_name not in self._known_sensors:
            self._known_sensors.add(sensor_name)
            logger.debug(f"requesting metadata for label: {sensor_label}, source: {sensor_source}")
            metadata = await self._rest_api.get_source_metadata(sensor_label, sensor_source)
            sensor_metadata = SKSensor(context=context, name=sensor_name, meta=metadata)
            logger.debug(f"received metadata for sensor: {sensor_metadata}")
            await self._queue.put(
                SKUpdate(type=SKUpdateType.SENSOR_METADATA, payload=sensor_metadata)
            )

    async def _send_observation_metadata(self, context: str, update: SKDeltaUpdate) -> None:
        """Lazily fetch observed property metadata via REST and push an OBSERVATION_METADATA update if not seen before."""
        for value in update.values:
            observation_path = value.path
            # Skip when the observed property is navigation
            if observation_path.startswith(NAVIGATION_PATH):
                continue

            sensor_label = update.source.label
            sensor_source = (
                update.source.src
                if update.source.type == SKDeltaSourceType.NMEA2000
                else update.source.talker
            )

            observation_source = f"{sensor_label}.{sensor_source}"
            known_observations = self._known_observations.setdefault(observation_source, set())

            if observation_path not in known_observations:
                known_observations.add(observation_path)
                logger.debug(f"requesting metadata for observation: {observation_path}")
                metadata = await self._rest_api.get_observation_metadata(
                    observation_path.replace(".", "/")
                )
                observation_metadata = SKObservedProperty(
                    context=context, path=observation_path, source=observation_source, meta=metadata
                )
                logger.debug(f"received metadata for observation: {observation_metadata}")
                await self._queue.put(
                    SKUpdate(
                        type=SKUpdateType.OBSERVED_PROPERTY_METADATA, payload=observation_metadata
                    )
                )

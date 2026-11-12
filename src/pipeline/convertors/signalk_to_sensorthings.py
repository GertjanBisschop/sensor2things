"""Convertor transforming SignalK Delta messages into SensorThings payloads."""

import asyncio
import logging
import uuid

from pydantic import BaseModel

from pipeline.models import (
    BaseConvertor,
    SK2STConfig,
    SKDelta,
    SKDeltaSourceType,
    SKDeltaUpdate,
    SKDeltaValue,
    SKObservedProperty,
    SKObservedPropertyContext,
    SKObservedPropertyContextKey,
    SKObservedPropertyMetaMultiple,
    SKSensor,
    SKSensorContext,
    SKSensorContextKey,
    SKUpdate,
    SKUpdateType,
    SKVessel,
    SKVesselContext,
    SKVesselContextKey,
    STLocation,
    STObservation,
    STObservedProperty,
    STSensor,
    STThing,
    STUnitOfMeasurement,
    STUpdate,
    STUpdateType,
)
from pipeline.registry import register_pipeline_artifact
from pipeline.utils import classproperty

ENVIRONMENT_PATH = "environment"
NAVIGATION_PATH = "navigation.position"
SEPARATOR = "#"

logger = logging.getLogger(__name__)


def _create_st_location_name(vessel_id: str, navigation_source: str) -> str:
    return f"{vessel_id}{SEPARATOR}{navigation_source}{SEPARATOR}{uuid.uuid7()}"


@register_pipeline_artifact
class SignalKToSensorThingsConvertor(BaseConvertor[SK2STConfig]):
    """Asynchronous worker that consumes Delta messages from source queue and produces STAPayloads to sink queue."""

    _config: SK2STConfig

    def __init__(
        self,
        source_queue: asyncio.Queue[SKUpdate],
        sink_queue: asyncio.Queue[STUpdate],
        config: SK2STConfig,
        context_mapping: dict | None = None,
    ) -> None:
        super().__init__(source_queue=source_queue, sink_queue=sink_queue, config=config)
        self._context_mapping = context_mapping

        self._conversion_handlers = {
            SKUpdateType.VESSEL_METADATA: (SKVessel, self._convert_vessel_update),
            SKUpdateType.SENSOR_METADATA: (SKSensor, self._convert_sensor_update),
            SKUpdateType.OBSERVED_PROPERTY_METADATA: (
                SKObservedProperty,
                self._convert_observed_property_update,
            ),
            SKUpdateType.DELTA: (SKDelta, self._convert_delta_update),
        }

    @classproperty
    def name(cls) -> str:
        return "signalk_sensorthings"

    async def health_check(self) -> bool:
        """Check if convertor is healthy."""
        return True

    async def run(self) -> None:
        """Start reading from source queue, converting, and writing to sink queue."""
        logger.info("SignalK to SensorThings convertor worker starting.")
        self._running = True
        while self._running:
            try:
                sk_update = await asyncio.wait_for(self._source_queue.get(), timeout=0.1)
                st_updates = self._convert_update(sk_update)
                for st_update in st_updates:
                    await self._sink_queue.put(st_update)
                self._source_queue.task_done()
            except TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except (AttributeError, KeyError, ValueError, RuntimeError) as e:
                logger.error(f"Error during delta conversion: {e}")
        logger.info("SignalK to SensorThings convertor worker stopped.")

    async def stop(self) -> None:
        """Stop conversion loop."""
        logger.debug("SignalK to SensorThings convertor worker stopping.")
        self._running = False

    def _convert_update(self, sk_update: SKUpdate) -> list[STUpdate]:
        """Dispatch a SignalK update to its corresponding converter method based on update type."""
        update_type, conversion_func = self._conversion_handlers.get(sk_update.type, (None, None))
        if update_type is None:
            logger.error(f"No handler for {sk_update.type.name}: skipping!")
            return []

        if not isinstance(sk_update.payload, update_type):
            logger.error(
                f"Invalid payload type for {sk_update.type.name}: {type(sk_update.payload)}"
            )
            return []

        return conversion_func(sk_update.payload)

    def _convert_vessel_update(self, vessel: SKVessel) -> list[STUpdate]:
        """Convert a SignalK Vessel update into a SensorThings Thing upsert update."""
        location = None
        if vessel.position:
            location = STLocation(
                latitude=vessel.position.latitude,
                longitude=vessel.position.longitude,
                name=_create_st_location_name(vessel.id, vessel.position.source),
                thing_name=vessel.id,
            )

        vessel_context = None
        if self._context_mapping:
            key = SKVesselContextKey(id=vessel.id)
            vessel_ctx = self._context_mapping.get(key)
            if isinstance(vessel_ctx, SKVesselContext):
                vessel_context = vessel_ctx

        return [
            STUpdate(
                type=STUpdateType.UPSERT_THING,
                payload=STThing(
                    id=vessel.id,
                    definition=vessel_context.definition if vessel_context else None,
                    # TODO: add the description / metadata based on infra info
                    location=location,
                    properties={
                        "sk_id": vessel.id,
                        "sk_name": vessel.name or "<Name not set>",
                    },
                ),
            )
        ]

    def _convert_sensor_update(self, sensor_meta: SKSensor) -> list[STUpdate]:
        """Convert a SignalK Sensor metadata update into a SensorThings Sensor upsert update."""
        properties = sensor_meta.meta
        if isinstance(properties, BaseModel):
            properties = properties.model_dump()
        elif isinstance(properties, dict):
            properties = {}
            for key, value in properties.items():
                if isinstance(value, BaseModel):
                    properties[key] = value.model_dump()

        sensor_context = None
        if self._context_mapping:
            key = SKSensorContextKey(context=sensor_meta.context, name=sensor_meta.name)
            sensor_ctx = self._context_mapping.get(key)
            if isinstance(sensor_ctx, SKSensorContext):
                sensor_context = sensor_ctx

        return [
            STUpdate(
                type=STUpdateType.UPSERT_SENSOR,
                payload=STSensor(
                    name=f"{sensor_meta.context}#{sensor_meta.name}",
                    description=f"Sensor located at {sensor_meta.name}",
                    encoding_type="text/html",
                    # TODO: add definition / metadata based on infra info
                    properties={
                        **properties,
                        "definition": sensor_context.definition if sensor_context else None,
                    },
                ),
            )
        ]

    def _convert_observed_property_update(self, obs_property: SKObservedProperty) -> list[STUpdate]:
        """Convert a SignalK ObservedProperty metadata update into SensorThings ObservedProperty and UnitOfMeasurement updates."""
        properties = obs_property.meta
        if isinstance(properties, BaseModel):
            properties = properties.model_dump()

        obs_prop_context = None
        unit_context = None
        if self._context_mapping:
            key = SKObservedPropertyContextKey(
                context=obs_property.context,
                source=obs_property.source,
                path=obs_property.path,
            )
            obs_ctx = self._context_mapping.get(key)
            if isinstance(obs_ctx, SKObservedPropertyContext):
                obs_prop_context = obs_ctx
                unit_context = obs_ctx.unit

        updates = []
        if isinstance(obs_property.meta, SKObservedPropertyMetaMultiple):
            for name, props in obs_property.meta.properties.items():
                prop_name = (
                    f"{obs_property.context}#{obs_property.source}#{obs_property.path}.{name}"
                )
                updates.append(
                    STUpdate(
                        type=STUpdateType.UPSERT_OBSERVED_PROPERTY,
                        payload=STObservedProperty(
                            name=prop_name,
                            description=props.description or "<Description was not set>",
                            definition=obs_prop_context.definition if obs_prop_context else None,
                            properties=props.model_dump(),
                        ),
                    )
                )
                updates.append(
                    STUpdate(
                        type=STUpdateType.CREATE_UNIT_OF_MEASUREMENT,
                        payload=STUnitOfMeasurement(
                            name=prop_name,
                            symbol=unit_context.symbol if unit_context else props.units,
                            definition=unit_context.definition
                            if unit_context
                            else "<Definition not set>",
                        ),
                    )
                )
        else:
            prop_name = f"{obs_property.context}#{obs_property.source}#{obs_property.path}"
            updates.append(
                STUpdate(
                    type=STUpdateType.UPSERT_OBSERVED_PROPERTY,
                    payload=STObservedProperty(
                        name=prop_name,
                        description=properties.get("description", "<Description was not set>"),
                        definition=obs_prop_context.definition if obs_prop_context else None,
                        properties=properties,
                    ),
                )
            )
            updates.append(
                STUpdate(
                    type=STUpdateType.CREATE_UNIT_OF_MEASUREMENT,
                    payload=STUnitOfMeasurement(
                        name=prop_name,
                        symbol=unit_context.symbol
                        if unit_context
                        else properties.get("units", "<Symbol not set>"),
                        definition=unit_context.definition
                        if unit_context
                        else "<Definition not set>",
                    ),
                )
            )

        return updates

    def _convert_delta_update(self, delta: SKDelta) -> list[STUpdate]:
        """Convert a SignalK Delta update into SensorThings Location or Observation updates."""
        updates = []

        for update in delta.updates:
            update_source = (
                update.source.src
                if update.source.type == SKDeltaSourceType.NMEA2000
                else update.source.talker
            )
            update_source = f"{update.source.label}.{update_source}"
            for value in update.values:
                if value.path == NAVIGATION_PATH:
                    updates.append(
                        STUpdate(
                            type=STUpdateType.UPSERT_LOCATION,
                            payload=STLocation(
                                thing_name=delta.context,
                                name=_create_st_location_name(delta.context, update_source),
                                latitude=value.value["latitude"],
                                longitude=value.value["longitude"],
                            ),
                        )
                    )
                elif value.path.startswith(ENVIRONMENT_PATH):
                    updates.extend(self._convert_delta_environment_update(delta, update, value))

        return updates

    def _convert_delta_environment_update(
        self,
        delta: SKDelta,
        update: SKDeltaUpdate,
        value: SKDeltaValue,
    ) -> list[STUpdate]:
        """Convert an environment metric update into one or more SensorThings Observation updates."""
        updates = []

        sensor_source = (
            update.source.src
            if update.source.type == SKDeltaSourceType.NMEA2000
            else update.source.talker
        )
        sensor_name = f"{delta.context}{SEPARATOR}{update.source.label}.{sensor_source}"
        property_name = f"{sensor_name}{SEPARATOR}{value.path}"
        if isinstance(value.value, dict):
            for val_name, val_value in value.value.items():
                updates.append(
                    STUpdate(
                        type=STUpdateType.CREATE_OBSERVATION,
                        payload=STObservation(
                            name=f"{property_name}.{val_name}",
                            thing_name=delta.context,
                            property_name=f"{property_name}.{val_name}",
                            sensor_name=sensor_name,
                            phenomenon_time=update.timestamp,
                            result=val_value,
                        ),
                    )
                )
        else:
            updates.append(
                STUpdate(
                    type=STUpdateType.CREATE_OBSERVATION,
                    payload=STObservation(
                        name=property_name,
                        thing_name=delta.context,
                        property_name=property_name,
                        sensor_name=sensor_name,
                        phenomenon_time=update.timestamp,
                        result=value.value,
                    ),
                )
            )

        return updates

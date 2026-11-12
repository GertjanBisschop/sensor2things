"""SensorThings source connector for ingesting messages via REST."""

import asyncio
import logging
from datetime import datetime

from frost_sta_client import Location, Thing

from pipeline.models import (
    BaseSourceConnector,
    STConfig,
    STFeatureOfInterest,
    STHistoricalLocation,
    STLocation,
    STObservation,
    STObservedProperty,
    STOrder,
    STOrderBy,
    STSensor,
    STThing,
    STUnitOfMeasurement,
    STUpdate,
    STUpdateType,
)
from pipeline.registry import register_pipeline_artifact
from pipeline.utils import (
    SensorThingsService,
    SensorThingsServiceError,
    SensorThingsSynchronisationPoint,
    classproperty,
)

logger = logging.getLogger(__name__)


def _extract_coordinates(location: Location) -> tuple[float, float]:
    """Extract longitude and latitude from a SensorThings Location object."""
    geometry = location.location
    longitude, latitude = 0.0, 0.0

    if isinstance(geometry, dict) and geometry.get("type", "<unknown>") == "Point":
        longitude, latitude = geometry["coordinates"]

    return longitude, latitude


@register_pipeline_artifact
class SensorThingsConnector(BaseSourceConnector[STConfig]):
    """Asynchronous source connector for streaming SensorThings data into an asyncio queue"""

    _config: STConfig

    def __init__(self, queue: asyncio.Queue[STUpdate], config: STConfig) -> None:
        super().__init__(queue=queue, config=config)

        self._service = SensorThingsService(config)
        self._things: dict[str, Thing] = {}
        self._last_known_location_name: dict[str, str] = {}
        self._last_known_historical_location_time: dict[str, str] = {}
        self._last_known_observation_time: dict[str, str] = {}

    @classproperty
    def name(cls) -> str:
        return "sensorthings"

    async def health_check(self) -> bool:
        """Check if SensorThings service endpoint is responsive."""
        try:
            await self._service.get_things(1)
        except SensorThingsServiceError:
            return False

        return True

    async def run(self) -> None:
        """Start listening for SensorThings updates."""
        logger.info("SensorThings source connector worker starting.")
        self._running = True
        await self._load_synchronisation_point()
        try:
            await self._sync_things_states()
            while self._running:
                await asyncio.sleep(self._config.poll_interval_seconds)
                await self._sync_things_states()
        except SensorThingsServiceError as e:
            logger.error(f"SensorThings service Error: {e}")
        except asyncio.CancelledError:
            logger.info("SensorThings source connector task was cancelled.")
        finally:
            logger.info("SensorThings source connector worker stopped.")
            self._running = False

    async def stop(self) -> None:
        """Stop listening for SensorThings updates."""
        logger.debug("SensorThings source connector worker stopping.")
        self._running = False

    async def _load_synchronisation_point(self) -> None:
        """Load synchronisation point from file."""
        if self._config.synchronisation_point is not None:
            sync_point = SensorThingsSynchronisationPoint(self._config.synchronisation_point)

            historical_locations = await sync_point.get_historical_locations()
            self._last_known_historical_location_time = {
                thing_name: dt.isoformat() for thing_name, dt in historical_locations.items()
            }

            observations = await sync_point.get_observations()
            self._last_known_observation_time = {
                thing_name: dt.isoformat() for thing_name, dt in observations.items()
            }

    async def _sync_things_states(self) -> None:
        """Sync thing states of SensorThings service."""
        await self._sync_things()
        await self._sync_historical_locations(with_locations=True)
        await self._sync_things_location()
        await self._sync_sensors()
        await self._sync_observed_properties()
        await self._sync_observations()

    async def _sync_things(self) -> None:
        """Sync things of SensorThings service."""
        things = await self._service.get_things()
        logger.info(f"Syncing {len(things)} things.")
        for thing in things:
            if thing.name is None:
                logger.warning(f"Thing name is None: {thing}, skipping!")
                continue

            properties = {}
            if isinstance(thing.properties, dict):
                properties = thing.properties

            await self._queue.put(
                STUpdate(
                    type=STUpdateType.UPSERT_THING,
                    payload=STThing(
                        id=thing.name,
                        description=thing.description,
                        properties=properties,
                    ),
                )
            )

            self._things[thing.name] = thing

    async def _sync_historical_locations(self, with_locations: bool = False) -> None:
        """Sync historical locations of SensorThings service."""
        for thing in self._things.values():
            if thing.name is None:
                continue

            later_than = self._last_known_historical_location_time.get(thing.name)
            later_than = f"time gt {later_than}" if later_than else None
            historical_locations = await self._service.get_historical_locations_by_thing(
                thing=thing,
                expand=["Locations"],
                filter=later_than,
                order_by=STOrderBy(criteria="time", order=STOrder.ASC),
            )
            logger.info(f"Syncing {len(historical_locations)} historical locations.")

            for historical_location in historical_locations:
                locations = historical_location.locations
                if locations is None or len(locations.entities) == 0:
                    logger.warning(
                        f"Historical locations has no locations set: {historical_location}, skipping!"
                    )
                    continue

                if with_locations:
                    await self._upsert_locations(locations.entities)

                location_name = locations.entities[0].name
                await self._queue.put(
                    STUpdate(
                        type=STUpdateType.UPSERT_HISTORICAL_LOCATION,
                        payload=STHistoricalLocation(
                            location_name=location_name,
                            thing_name=thing.name,
                            time=datetime.fromisoformat(historical_location.time),
                        ),
                    )
                )

                self._last_known_historical_location_time[thing.name] = historical_location.time

    async def _sync_things_location(self) -> None:
        """Sync locations of SensorThings service."""
        for thing in self._things.values():
            thing_name = thing.name or f"st:{thing.id}"
            locations = await self._service.get_locations_by_thing(thing)
            if len(locations) == 0:
                continue

            latest_location = locations[-1].name or f"st:{locations[-1].id}"
            if self._last_known_location_name.get(thing_name, "") != latest_location:
                await self._upsert_locations(locations)
                self._last_known_location_name[thing_name] = latest_location

    async def _upsert_locations(self, locations: list[Location]) -> None:
        for location in locations:
            thing_name = None
            if location.things and location.things.entities:
                thing_name = location.things.entities[0].name

            longitude, latitude = _extract_coordinates(location)
            await self._queue.put(
                STUpdate(
                    type=STUpdateType.UPSERT_LOCATION,
                    payload=STLocation(
                        thing_name=thing_name,
                        description=location.description,
                        latitude=latitude,
                        longitude=longitude,
                        name=location.name or f"st:{location.id}",
                    ),
                )
            )

    async def _sync_sensors(self) -> None:
        """Sync sensors of SensorThings service."""
        sensors = await self._service.get_sensors()
        logger.info(f"Syncing {len(sensors)} sensors.")

        for sensor in sensors:
            await self._queue.put(
                STUpdate(
                    type=STUpdateType.UPSERT_SENSOR,
                    payload=STSensor(
                        name=sensor.name,
                        description=sensor.description,
                        encoding_type=sensor.encoding_type,
                        metadata=sensor.metadata,
                        properties=sensor.properties,
                    ),
                )
            )

    async def _sync_observed_properties(self) -> None:
        """Sync observed properties of SensorThings service."""
        observed_properties = await self._service.get_observed_properties()
        logger.info(f"Syncing {len(observed_properties)} observed properties.")

        for observed_property in observed_properties:
            if observed_property.name is None:
                continue

            await self._queue.put(
                STUpdate(
                    type=STUpdateType.UPSERT_OBSERVED_PROPERTY,
                    payload=STObservedProperty(
                        name=observed_property.name,
                        description=observed_property.description,
                        definition=observed_property.definition,
                        properties=observed_property.properties,
                    ),
                )
            )

    async def _sync_observations(self) -> None:
        """Sync observations of SensorThings service."""
        datastreams = await self._service.get_datastreams(
            expand=["Thing", "Sensor", "ObservedProperty"],
        )
        logger.info(f"Syncing {len(datastreams)} datastreams.")

        for datastream in datastreams:
            datastream_name = datastream.name or f"st#{datastream.id}"
            if datastream.unit_of_measurement is not None:
                await self._queue.put(
                    STUpdate(
                        type=STUpdateType.CREATE_UNIT_OF_MEASUREMENT,
                        payload=STUnitOfMeasurement(
                            name=datastream.unit_of_measurement.name or "<Name not set>",
                            symbol=datastream.unit_of_measurement.symbol or "<Symbol not set>",
                            definition=datastream.unit_of_measurement.definition
                            or "<Definition not set>",
                        ),
                    )
                )

            later_than = self._last_known_observation_time.get(datastream_name)
            later_than = f"phenomenonTime gt {later_than}" if later_than else None
            observations = await self._service.get_observations_by_datastream(
                datastream,
                expand=["FeatureOfInterest"],
                filter=later_than,
                order_by=STOrderBy(criteria="phenomenonTime", order=STOrder.ASC),
            )
            logger.info(f"Syncing {len(observations)} observations for {datastream_name}.")

            if datastream.thing is None or datastream.thing.name is None:
                logger.warning(
                    f"Skipping datastream {datastream_name} because it has no valid thing."
                )
                continue

            if datastream.sensor is None or datastream.sensor.name is None:
                logger.warning(
                    f"Skipping datastream {datastream_name} because it has no valid sensor."
                )
                continue

            if datastream.observed_property is None or datastream.observed_property.name is None:
                logger.warning(
                    f"Skipping datastream {datastream_name} because it has no valid observed property."
                )
                continue

            for observation in observations:
                feature_of_interest = None
                if observation.feature_of_interest is not None:
                    feature_of_interest = STFeatureOfInterest(
                        name=observation.feature_of_interest.name or datastream_name,
                        description=observation.feature_of_interest.description,
                        encoding_type=observation.feature_of_interest.encoding_type or "text/html",
                        feature=observation.feature_of_interest.feature,
                        properties=observation.feature_of_interest.properties or {},
                    )

                await self._queue.put(
                    STUpdate(
                        type=STUpdateType.CREATE_OBSERVATION,
                        payload=STObservation(
                            name=datastream_name,
                            feature_of_interest=feature_of_interest,
                            thing_name=datastream.thing.name,
                            property_name=datastream.observed_property.name,
                            sensor_name=datastream.sensor.name,
                            phenomenon_time=observation.phenomenon_time,
                            result=observation.result,
                            result_time=observation.result_time,
                        ),
                    )
                )

                self._last_known_observation_time[datastream_name] = observation.phenomenon_time

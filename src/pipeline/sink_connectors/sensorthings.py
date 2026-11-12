"""SensorThings API sink connector for persisting entities and observations."""

import asyncio
import logging
from datetime import datetime

# isort: off
from frost_sta_client import (
    Datastream as FrostDatastream,
    FeatureOfInterest as FrostFeatureOfInterest,
    HistoricalLocation as FrostHistoricalLocation,
    Location as FrostLocation,
    Observation as FrostObservation,
    ObservedProperty as FrostObservedProperty,
    Sensor as FrostSensor,
    Thing as FrostThing,
    UnitOfMeasurement as FrostUnitOfMeasurement,
)

# isort: on
from geojson import Point

from pipeline.models import (
    BaseSinkConnector,
    STConfig,
    STHistoricalLocation,
    STLocation,
    STObservation,
    STObservedProperty,
    STOrderBy,
    STSensor,
    STThing,
    STUnitOfMeasurement,
    STUpdate,
    STUpdateType,
)
from pipeline.registry import register_pipeline_artifact
from pipeline.utils import (
    EntityNotFoundError,
    SensorThingsService,
    SensorThingsServiceError,
    SensorThingsSynchronisationPoint,
    classproperty,
)

logger = logging.getLogger(__name__)


@register_pipeline_artifact
class SensorThingsConnector(BaseSinkConnector[STConfig]):
    """Asynchronous sink connector that consumes payloads from a queue and updates SensorThings."""

    _config: STConfig

    def __init__(
        self,
        queue: asyncio.Queue[STUpdate],
        config: STConfig,
    ) -> None:
        super().__init__(queue=queue, config=config)

        self._service = SensorThingsService(config)
        self._payload_handlers = {
            STUpdateType.UPSERT_THING: (STThing, self._upsert_thing),
            STUpdateType.UPSERT_LOCATION: (STLocation, self._upsert_location),
            STUpdateType.UPSERT_HISTORICAL_LOCATION: (
                STHistoricalLocation,
                self._upsert_historical_location,
            ),
            STUpdateType.UPSERT_SENSOR: (STSensor, self._upsert_sensor),
            STUpdateType.UPSERT_OBSERVED_PROPERTY: (
                STObservedProperty,
                self._upsert_observed_property,
            ),
            STUpdateType.CREATE_UNIT_OF_MEASUREMENT: (
                STUnitOfMeasurement,
                self._create_unit_of_measurement,
            ),
            STUpdateType.CREATE_OBSERVATION: (STObservation, self._create_observation),
        }

        self._things: dict[str, FrostThing] = {}
        self._datastreams: dict[str, FrostDatastream] = {}
        self._units_of_measurement: dict[str, FrostUnitOfMeasurement] = {}
        self._last_known_location: STLocation | None = None
        self._last_known_historical_location_time: dict[str, datetime] = {}
        self._last_known_observation_time: dict[str, datetime] = {}
        self._synchronisation_point: SensorThingsSynchronisationPoint | None = None

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
        """Start listening for STAPayloads on the sink queue."""
        logger.info("SensorThings sink connector worker starting.")
        self._running = True
        await self._load_synchronisation_point()
        await self._update_synchronisation_point()
        while self._running:
            try:
                update: STUpdate = await asyncio.wait_for(self._queue.get(), timeout=0.1)
                await self._process_update(update)
                self._queue.task_done()
            except TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except (AttributeError, KeyError, ValueError, RuntimeError) as e:
                logger.error(f"Error processing SensorThings sink payload: {e}")
        logger.info("SensorThings sink connector worker stopped.")

    async def stop(self) -> None:
        """Stop processing payloads."""
        logger.debug("SensorThings sink connector worker stopping ")
        await self._update_synchronisation_point()
        await self._store_synchronisation_point()
        self._running = False

    async def _load_synchronisation_point(self):
        """Load the synchronisation point."""
        if self._config.synchronisation_point is not None:
            self._synchronisation_point = SensorThingsSynchronisationPoint(
                self._config.synchronisation_point
            )
            self._last_known_historical_location_time = (
                await self._synchronisation_point.get_historical_locations()
            )
            self._last_known_observation_time = await self._synchronisation_point.get_observations()

    async def _update_synchronisation_point(self):
        """Update the synchronisation point."""
        try:
            things = await self._service.get_things()
            for thing in things:
                if thing.name is None:
                    continue

                historical_locations = await self._service.get_historical_locations_by_thing(
                    thing, amount=1, order_by=STOrderBy(criteria="time")
                )

                if historical_locations:
                    self._last_known_historical_location_time[thing.name] = datetime.fromisoformat(
                        historical_locations[0].time
                    )

            datastreams = await self._service.get_datastreams()
            for datastream in datastreams:
                if datastream.name is None:
                    continue

                observations = await self._service.get_observations_by_datastream(
                    datastream, amount=1, order_by=STOrderBy(criteria="phenomenonTime")
                )

                if observations:
                    self._last_known_observation_time[datastream.name] = datetime.fromisoformat(
                        observations[0].phenomenon_time
                    )
        except SensorThingsServiceError:
            logger.warning("Failed to retrieve synchronisation point. Will continue without it!")

    async def _store_synchronisation_point(self):
        """Store the synchronisation point."""
        if (
            self._config.synchronisation_point is not None
            and self._synchronisation_point is not None
        ):
            self._synchronisation_point.set_historical_locations(
                self._last_known_historical_location_time
            )
            self._synchronisation_point.set_observations(self._last_known_observation_time)

            await self._synchronisation_point.store()

    async def _process_update(self, update: STUpdate) -> None:
        """Route a SensorThings update to its appropriate entity handler."""
        payload_type, update_func = self._payload_handlers.get(update.type, (None, None))
        if payload_type is None:
            logger.error(f"No handler for {update.type.name}: skipping!")
            return

        if not isinstance(update.payload, payload_type):
            logger.error(
                f"Invalid payload type for {update.type.name}: {type(update.payload)}. Skipping!"
            )
            return

        await update_func(update.payload)

    async def _upsert_thing(self, thing: STThing) -> None:
        """Upsert a Thing entity and its optional initial location in FROST-Server."""
        st_thing = self._things.get(thing.id)
        if st_thing is None:
            logger.debug(f"upserting thing {thing}")
            st_thing = FrostThing(
                name=thing.id,
                description=thing.description or "<Description was not set>",
                properties=thing.properties,
            )

            await self._service.upsert_thing(st_thing)
            self._things[thing.id] = st_thing

        location = thing.location
        if location:
            st_location = FrostLocation(
                name=location.name,
                description=location.description or "<Description not set>",
                location=Point((location.longitude, location.latitude)),
                encoding_type="application/geo+json",
                things=[st_thing],
            )
            await self._service.upsert_location(st_location)

            self._last_known_location = thing.location

    async def _upsert_location(self, location: STLocation) -> None:
        """Update or insert a Location entity associated with an existing Thing."""
        things = []
        if location.thing_name and location.thing_name in self._things:
            st_thing = self._things.get(location.thing_name)
            if st_thing is None:
                logger.warning(
                    f"Thing {location.thing_name} not found, will add location without linking!"
                )
            else:
                things.append(st_thing)

        st_location = FrostLocation(
            name=location.name,
            description=location.description or "<Description not set>",
            location=Point((location.longitude, location.latitude)),
            encoding_type="application/geo+json",
            things=things,
        )

        await self._service.upsert_location(st_location)
        self._last_known_location = location

    async def _upsert_historical_location(self, historical_location: STHistoricalLocation) -> None:
        """Upsert a HistoricalLocation entity in FROST-Server."""
        thing_name = historical_location.thing_name
        st_thing = self._things.get(thing_name)
        if st_thing is None:
            logger.warning(f"Thing {thing_name} not found, skipping location update!")
            return

        if (
            thing_name in self._last_known_historical_location_time
            and historical_location.time <= self._last_known_historical_location_time[thing_name]
        ):
            logger.debug(
                "Historical location is older than previous synchronisation point. skipping!"
            )
            return

        try:
            st_location = await self._service.get_location(historical_location.location_name)
        except EntityNotFoundError:
            logger.warning(
                f"Failed to retrieve location {historical_location.location_name}, Skipping!"
            )
            return

        st_historical_location = FrostHistoricalLocation(
            thing=st_thing,
            time=historical_location.time.isoformat(),
            locations=[st_location],
        )

        await self._service.create_historical_location(st_historical_location)

    async def _upsert_sensor(self, sensor: STSensor) -> None:
        """Upsert a Sensor entity in FROST-Server."""
        st_sensor = FrostSensor(
            name=sensor.name,
            description=sensor.description or "<Description not set>",
            encoding_type=sensor.encoding_type,
            # TODO: link to infra.vliz.be,
            metadata=sensor.metadata or "<Metadata not set>",
            properties=sensor.properties,
        )

        await self._service.upsert_sensor(st_sensor)

    async def _upsert_observed_property(self, observed_prop: STObservedProperty) -> None:
        """Upsert an ObservedProperty entity in FROST-Server."""
        st_observed_prop = FrostObservedProperty(
            name=observed_prop.name,
            description=observed_prop.description or "<Description not set>",
            definition=observed_prop.definition or "<Definition not set>",
            properties=observed_prop.properties,
        )

        await self._service.upsert_observed_property(st_observed_prop)

    async def _create_unit_of_measurement(self, unit_of_measurement: STUnitOfMeasurement) -> None:
        """Cache a UnitOfMeasurement definition for subsequent Datastream creation."""
        if not unit_of_measurement.name in self._units_of_measurement:
            self._units_of_measurement[unit_of_measurement.name] = FrostUnitOfMeasurement(
                name=unit_of_measurement.name,
                symbol=unit_of_measurement.symbol,
                definition=unit_of_measurement.definition,
            )

    async def _create_observation(self, observation: STObservation) -> None:
        """Create an Observation and its Datastream (if not already existing) in FROST-Server."""
        datastream_name = observation.name
        if datastream_name not in self._datastreams:
            await self._create_datastream(datastream_name, observation)

        if (
            datastream_name in self._last_known_observation_time
            and observation.phenomenon_time <= self._last_known_observation_time[datastream_name]
        ):
            logger.debug("Observation is older than previous synchronisation point. skipping!")
            return

        st_datastream = self._datastreams[datastream_name]
        st_feature_of_interest = None
        if observation.feature_of_interest is not None:
            st_feature_of_interest = FrostFeatureOfInterest(
                **observation.feature_of_interest.model_dump()
            )

        if st_feature_of_interest is None and self._last_known_location:
            st_feature_of_interest = FrostFeatureOfInterest(
                name=observation.name,
                description="Position where observation is made (approximated).",
                encoding_type="application/geo+json",
                feature=Point(
                    (
                        self._last_known_location.longitude,
                        self._last_known_location.latitude,
                    )
                ),
                properties={},
            )

        st_observation = FrostObservation(
            phenomenon_time=observation.phenomenon_time,
            result=observation.result,
            result_time=observation.result_time or observation.phenomenon_time,
            datastream=st_datastream,
            feature_of_interest=st_feature_of_interest,
        )

        await self._service.create_observation(st_observation)

    async def _create_datastream(self, datastream_name: str, observation: STObservation) -> None:
        """Create and upsert a Datastream linking Thing, Sensor, ObservedProperty, and UnitOfMeasurement."""
        thing = self._things[observation.thing_name]
        sensor = await self._service.get_sensor(observation.sensor_name)
        observed_prop = await self._service.get_observed_property(observation.property_name)
        unit_of_measurement = self._units_of_measurement.get(
            observed_prop.name or "",
            FrostUnitOfMeasurement(
                name=f"<Unit of Measurement for {observed_prop.name} unset>",
                symbol="<Symbol not set>",
                definition="<Definition not set>",
            ),
        )

        st_datastream = FrostDatastream(
            name=datastream_name,
            description=f"Datastream created from sensor {sensor.name} that is observing {observed_prop.name} while mounted on {thing.name}.",
            observation_type="http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            thing=thing,
            sensor=sensor,
            observed_property=observed_prop,
            unit_of_measurement=unit_of_measurement,
            properties={},
        )

        await self._service.upsert_datastream(st_datastream)
        self._datastreams[datastream_name] = st_datastream

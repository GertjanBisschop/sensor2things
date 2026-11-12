import asyncio
import functools
import logging
from collections.abc import Sequence
from urllib.parse import urlsplit, urlunsplit

import frost_sta_client as fsta
import requests
from frost_sta_client import (
    Datastream,
    Entity,
    FeatureOfInterest,
    HistoricalLocation,
    Location,
    Observation,
    ObservedProperty,
    Sensor,
    Thing,
)
from frost_sta_client.dao.base import BaseDao
from furl import furl

from pipeline.error import PipelineError
from pipeline.models.sensorthings import Config as STConfig
from pipeline.models.sensorthings import OrderBy

FROST_STA_EXCEPTIONS = (
    requests.exceptions.RequestException,
    ValueError,
    KeyError,
    AttributeError,
    TypeError,
)

logger = logging.getLogger(__name__)


class SensorThingsServiceError(PipelineError):
    """Error raised when there is an issue with the SensorThings service."""


class EntityNotFoundError(SensorThingsServiceError):
    """Error raised when an entity is not found in the SensorThings service."""


def _normalize_url(target_url: str | furl, base_url: str) -> str:
    """Rewrite scheme and host/port of target_url to match base_url."""
    target_parts = urlsplit(str(target_url))
    base_parts = urlsplit(base_url)

    # Replace only scheme and netloc (host:port), preserving path, query, and fragments
    rewritten_parts = target_parts._replace(
        scheme=base_parts.scheme,
        netloc=base_parts.netloc,
    )
    return str(urlunsplit(rewritten_parts))


class SensorThingsService:
    """SensorThings service."""

    def __init__(self, config: STConfig):
        self._config = config
        auth_handler = None
        if config.username and config.password:
            auth_handler = fsta.AuthHandler(username=config.username, password=config.password)

        logger.debug(f"Connecting SensorThings service to {config.base_url}")
        self._service = fsta.SensorThingsService(config.base_url, auth_handler=auth_handler)

        # Wrap execute to transparently normalize host/port on all outgoing requests
        original_execute = self._service.execute

        @functools.wraps(original_execute)
        def _execute_normalized(method: str, url: str, **kwargs):
            rewritten_url = _normalize_url(url, config.base_url)
            return original_execute(method, rewritten_url, **kwargs)

        self._service.execute = _execute_normalized

        self._endpoint_map: dict[type[Entity], str] = {
            Datastream: "datastreams",
            FeatureOfInterest: "features_of_interest",
            HistoricalLocation: "historical_locations",
            Location: "locations",
            Observation: "observations",
            ObservedProperty: "observed_properties",
            Sensor: "sensors",
            Thing: "things",
        }

    async def get_things(
        self,
        amount: int = 0,
        expand: Sequence[str] | None = None,
    ) -> list[Thing]:
        """Query available Thing entities."""
        return await self._find_entities(Thing, amount=amount, expand=expand)

    async def get_thing(self, thing_name: str) -> Thing:
        """Fetch a Thing entity by its name."""
        return await self._find_entity_by_name(Thing, thing_name)

    async def upsert_thing(self, thing: Thing) -> None:
        """Create or update a Thing entity."""
        await self._upsert_entity(thing)

    async def get_locations(self, expand: Sequence[str] | None = None) -> list[Location]:
        """Query available Location entities."""
        return await self._find_entities(Location, expand=expand)

    async def get_locations_by_thing(self, thing: Thing) -> list[Location]:
        """Query available Location entities by Thing."""
        endpoint = thing.get_locations()

        return await self._get_entities_by_endpoint(
            Location,
            endpoint=endpoint,
        )

    async def get_location(self, location_name: str) -> Location:
        """Fetch a Location entity by its name."""
        return await self._find_entity_by_name(Location, location_name)

    async def upsert_location(self, location: Location) -> None:
        """Create or update a Location entity."""
        await self._upsert_entity(location)

    async def get_historical_locations_by_thing(
        self,
        thing: Thing,
        amount: int = 0,
        expand: Sequence[str] | None = None,
        filter: str | None = None,
        order_by: OrderBy | None = None,
    ) -> list[HistoricalLocation]:
        """Query available HistoricalLocation entities."""
        endpoint = thing.get_historical_locations()

        return await self._get_entities_by_endpoint(
            HistoricalLocation,
            endpoint=endpoint,
            amount=amount,
            expand=expand,
            filter=filter,
            order_by=order_by,
        )

    async def create_historical_location(self, location: HistoricalLocation) -> None:
        """Create a HistoricalLocation entity."""
        await self._create_entity(location)

    async def upsert_historical_location(self, location: HistoricalLocation) -> None:
        """Create or update a HistoricalLocation entity."""
        await self._upsert_entity(location)

    async def get_sensors(self) -> list[Sensor]:
        """Query available Sensor entities."""
        return await self._find_entities(Sensor)

    async def get_sensor(self, sensor_name: str) -> Sensor:
        """Fetch a Sensor entity by its name."""
        return await self._find_entity_by_name(Sensor, sensor_name)

    async def upsert_sensor(self, sensor: Sensor) -> None:
        """Create or update a Sensor entity."""
        await self._upsert_entity(sensor)

    async def get_observed_properties(self) -> list[ObservedProperty]:
        """Query available ObservedProperty entities."""
        return await self._find_entities(ObservedProperty)

    async def get_observed_property(self, observed_property_name: str) -> ObservedProperty:
        """Fetch an ObservedProperty entity by its name."""
        return await self._find_entity_by_name(ObservedProperty, observed_property_name)

    async def upsert_observed_property(self, observed_property: ObservedProperty) -> None:
        """Create or update an ObservedProperty entity."""
        await self._upsert_entity(observed_property)

    async def get_datastreams(self, expand: Sequence[str] | None = None) -> list[Datastream]:
        """Query available Datastream entities."""
        return await self._find_entities(Datastream, expand=expand)

    async def get_datastream(self, datastream_name: str) -> Datastream:
        """Fetch a Datastream entity by its name."""
        return await self._find_entity_by_name(Datastream, datastream_name)

    async def upsert_datastream(self, datastream: Datastream) -> None:
        """Create or update a Datastream entity."""
        await self._upsert_entity(datastream)

    async def get_observations_by_datastream(
        self,
        datastream: Datastream,
        amount: int = 0,
        expand: Sequence[str] | None = None,
        filter: str | None = None,
        order_by: OrderBy | None = None,
    ) -> list[Observation]:
        """Query available Observation entities for a datastream."""
        endpoint = datastream.get_observations()

        return await self._get_entities_by_endpoint(
            Observation,
            endpoint=endpoint,
            amount=amount,
            expand=expand,
            filter=filter,
            order_by=order_by,
        )

    async def create_observation(self, observation: Observation) -> None:
        """Create a new Observation entity."""
        await self._create_entity(observation)

    async def _find_entity_by_name[T: Entity](self, result_type: type[T], name: str) -> T:
        """Find a unique entity of the given type matching the name filter."""
        filter = f"name eq '{name}'"
        result = await self._find_entities(result_type, filter=filter)
        if len(result) == 0:
            raise EntityNotFoundError(
                f"Entity[{result_type.__name__}] with name '{name}' not found"
            )

        return result[0]

    async def _find_entities[T: Entity](
        self,
        result_type: type[T],
        amount: int = 0,
        expand: Sequence[str] | None = None,
        filter: str | None = None,
        order_by: OrderBy | None = None,
    ) -> list[T]:
        """Execute an OData filter query against the SensorThings endpoint for result_type."""
        try:
            endpoint_name = self._endpoint_map.get(result_type, f"{result_type.__name__.lower()}s")
            logger.debug(f"finding endpoint getter: {endpoint_name}")
            endpoint = getattr(self._service, endpoint_name, None)
            if endpoint is None:
                raise SensorThingsServiceError(f"Unable to get endpoint for {result_type.__name__}")

            return await self._get_entities_by_endpoint(
                result_type,
                endpoint=endpoint(),
                amount=amount,
                expand=expand,
                filter=filter,
                order_by=order_by,
            )
        except FROST_STA_EXCEPTIONS as e:
            raise SensorThingsServiceError(f"Error finding entity: {e}")

    async def _get_entities_by_endpoint[T: Entity](
        self,
        result_type: type[T],
        endpoint: BaseDao,
        amount: int = 0,
        expand: Sequence[str] | None = None,
        filter: str | None = None,
        order_by: OrderBy | None = None,
    ) -> list[T]:
        def _get_entities(
            amount: int,
            expand: Sequence[str] | None,
            filter: str | None,
            order_by: OrderBy | None,
        ) -> list[T]:
            query = endpoint.query()
            if filter is not None:
                query = query.filter(filter)

            if amount > 0:
                query = query.top(amount)

            if expand is not None:
                query = query.expand(",".join(expand))

            if order_by is not None:
                query = query.orderby(
                    order_by.criteria,
                    order_by.order.value,
                )

            entity_list = query.list()
            entities: list[T] = []
            if hasattr(entity_list, "__iter__"):
                for item in entity_list:
                    entities.append(item)
                    if 0 < amount <= len(entities):
                        break
            else:
                entities = entity_list.entities

            return entities

        return await asyncio.to_thread(_get_entities, amount, expand, filter, order_by)

    async def _create_entity(self, entity: Entity) -> None:
        """Dispatch entity creation call to FROST-Server in a worker thread."""
        try:
            await asyncio.to_thread(self._service.create, entity)
        except FROST_STA_EXCEPTIONS as e:
            raise SensorThingsServiceError(f"Error creating entity: {e}")

    async def _update_entity(self, entity: Entity) -> None:
        """Dispatch entity update call to FROST-Server in a worker thread."""
        try:
            await asyncio.to_thread(self._service.update, entity)
        except FROST_STA_EXCEPTIONS as e:
            raise SensorThingsServiceError(f"Error updating entity: {e}")

    async def _upsert_entity(self, entity: Entity) -> None:
        """Upsert an entity by querying for existing instance by name, setting ID, and updating or creating."""
        try:
            entity_name = getattr(entity, "name", None)
            if entity_name is None:
                logger.warning(
                    f"Entity {entity} has no name, unable to upsert. Will create new entity!"
                )
                raise EntityNotFoundError

            existing_entity = await self._find_entity_by_name(type(entity), entity_name)
            entity.id = existing_entity.id
            await self._update_entity(entity)
        except EntityNotFoundError:
            await self._create_entity(entity)

import asyncio
from unittest.mock import AsyncMock

import pytest

from pipeline.models.sensorthings import (
    Config,
    Location,
    Thing,
    Update,
    UpdateType,
)
from pipeline.sink_connectors import STConnector


@pytest.fixture
def mock_sensorthings_service(mocker):
    service_cls = mocker.patch("pipeline.sink_connectors.sensorthings.SensorThingsService")
    mock_service = AsyncMock()
    service_cls.return_value = mock_service

    return mock_service


@pytest.mark.asyncio
async def test_upsert_thing(mock_sensorthings_service):
    config = Config()
    queue = asyncio.Queue()
    connector = STConnector(queue, config)
    connector_task = asyncio.create_task(connector.run())

    await queue.put(
        Update(
            type=UpdateType.UPSERT_THING,
            payload=Thing(
                id="vessels.urn:mrn:signalk:uuid:c130874d-4b4d-4188-91a1-71eeda7d7f12",
                description="Oh vessel, my vessel!",
                properties={"sk_name": "my_vessel"},
            ),
        )
    )

    await asyncio.sleep(0.05)
    await connector.stop()
    await connector_task

    mock_sensorthings_service.upsert_thing.assert_called_once()
    mock_sensorthings_service.upsert_location.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_thing_with_location(mock_sensorthings_service):
    config = Config()
    queue = asyncio.Queue()
    connector = STConnector(queue, config)
    connector_task = asyncio.create_task(connector.run())

    await queue.put(
        Update(
            type=UpdateType.UPSERT_THING,
            payload=Thing(
                id="vessels.urn:mrn:signalk:uuid:c130874d-4b4d-4188-91a1-71eeda7d7f12",
                description="Oh vessel, my vessel!",
                properties={"sk_name": "my_vessel"},
                location=Location(
                    latitude=51.2050,
                    longitude=2.1234,
                    name="vessels.urn:mrn:signalk:uuid:c130874d-4b4d-4188-91a1-71eeda7d7f12#nmea0183-sample-data.GP",
                ),
            ),
        )
    )

    await asyncio.sleep(0.05)
    await connector.stop()
    await connector_task

    mock_sensorthings_service.upsert_thing.assert_called_once()
    mock_sensorthings_service.upsert_location.assert_called_once()


@pytest.mark.asyncio
async def test_upsert_location(mock_sensorthings_service):
    config = Config()
    queue = asyncio.Queue()
    connector = STConnector(queue, config)
    connector_task = asyncio.create_task(connector.run())

    await queue.put(
        Update(
            type=UpdateType.UPSERT_THING,
            payload=Thing(
                id="vessels.urn:mrn:signalk:uuid:c130874d-4b4d-4188-91a1-71eeda7d7f12",
                description="Oh vessel, my vessel!",
                properties={"sk_name": "my_vessel"},
            ),
        )
    )
    await queue.put(
        Update(
            type=UpdateType.UPSERT_LOCATION,
            payload=Location(
                thing_name="vessels.urn:mrn:signalk:uuid:c130874d-4b4d-4188-91a1-71eeda7d7f12",
                latitude=51.2050,
                longitude=2.1234,
                name="vessels.urn:mrn:signalk:uuid:c130874d-4b4d-4188-91a1-71eeda7d7f12#nmea0183-sample-data.GP",
            ),
        )
    )

    await asyncio.sleep(0.05)
    await connector.stop()
    await connector_task

    mock_sensorthings_service.upsert_thing.assert_called()
    mock_sensorthings_service.upsert_location.assert_called_once()


@pytest.mark.asyncio
async def test_upsert_location_without_thing(mock_sensorthings_service):
    config = Config()
    queue = asyncio.Queue()
    connector = STConnector(queue, config)
    connector_task = asyncio.create_task(connector.run())

    await queue.put(
        Update(
            type=UpdateType.UPSERT_LOCATION,
            payload=Location(
                thing_name="vessels.urn:mrn:signalk:uuid:c130874d-4b4d-4188-91a1-71eeda7d7f12",
                latitude=51.2050,
                longitude=2.1234,
                name="vessels.urn:mrn:signalk:uuid:c130874d-4b4d-4188-91a1-71eeda7d7f12#nmea0183-sample-data.GP",
            ),
        )
    )

    await asyncio.sleep(0.05)
    await connector.stop()
    await connector_task

    mock_sensorthings_service.upsert_thing.assert_not_called()
    mock_sensorthings_service.upsert_location.assert_called()

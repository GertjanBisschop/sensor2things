from unittest.mock import MagicMock

import pytest

# isort: off
from frost_sta_client import (
    Datastream as STDatastream,
    Location as STLocation,
    ObservedProperty as STObservedProperty,
    Sensor as STSensor,
    Thing as STThing,
)

# isort: on
from pipeline.models.sensorthings import (
    Config,
)
from pipeline.utils.sensorthings_service import (
    SensorThingsService,
    SensorThingsServiceError,
)


@pytest.fixture
def mock_frost_sta_service(mocker):
    service_cls = mocker.patch("frost_sta_client.SensorThingsService")
    mock_service = MagicMock()
    service_cls.return_value = mock_service

    mock_list = MagicMock()
    mock_list.entities = []

    mock_query = MagicMock()
    mock_query.expand.return_value = mock_query
    mock_query.filter.return_value = mock_query
    mock_query.list.return_value = mock_list
    mock_query.orderby.return_value = mock_query
    mock_query.top.return_value = mock_query

    mock_dao = MagicMock()
    mock_dao.query.return_value = mock_query

    mock_service.things.return_value = mock_dao
    mock_service.locations.return_value = mock_dao
    mock_service.sensors.return_value = mock_dao
    mock_service.observed_properties.return_value = mock_dao
    mock_service.datastreams.return_value = mock_dao

    return mock_service, mock_list


@pytest.mark.asyncio
async def test_service_get_things(mock_frost_sta_service):
    config = Config()
    service = SensorThingsService(config)
    things = await service.get_things()

    assert len(things) == 0


@pytest.mark.asyncio
async def test_service_get_things_multiple(mock_frost_sta_service):
    _, mock_list = mock_frost_sta_service

    mock_list.entities = [
        STThing(id="1"),
        STThing(id="2"),
    ]
    mock_list.__iter__.return_value = iter(mock_list.entities)

    config = Config()
    service = SensorThingsService(config)
    things = await service.get_things()

    assert len(things) == 2


@pytest.mark.asyncio
async def test_service_get_thing(mock_frost_sta_service):
    _, mock_list = mock_frost_sta_service

    thing_name = "thing1"
    mock_list.entities = [
        STThing(id="1", name=thing_name),
    ]
    mock_list.__iter__.return_value = iter(mock_list.entities)

    config = Config()
    service = SensorThingsService(config)
    thing = await service.get_thing(thing_name)

    assert thing.name == thing_name


@pytest.mark.asyncio
async def test_service_get_thing_not_found(mock_frost_sta_service):
    with pytest.raises(SensorThingsServiceError):
        config = Config()
        service = SensorThingsService(config)

        thing_name = "thing1"
        await service.get_thing(thing_name)


@pytest.mark.asyncio
async def test_service_create_thing(mock_frost_sta_service):
    config = Config()
    service = SensorThingsService(config)

    st_thing = STThing(name="thing1")
    await service.upsert_thing(st_thing)


@pytest.mark.asyncio
async def test_service_update_thing(mock_frost_sta_service):
    _, mock_list = mock_frost_sta_service

    thing_name = "thing1"
    st_thing = STThing(id="1", name=thing_name)
    mock_list.entities = [
        st_thing,
    ]
    mock_list.__iter__.return_value = iter(mock_list.entities)

    config = Config()
    service = SensorThingsService(config)

    await service.upsert_thing(st_thing)


@pytest.mark.asyncio
async def test_service_create_thing_fail(mock_frost_sta_service):
    with pytest.raises(SensorThingsServiceError):
        mock_service, _ = mock_frost_sta_service
        mock_service.create.side_effect = ValueError("Unable to create entity")

        config = Config()
        service = SensorThingsService(config)

        st_thing = STThing(name="thing1")
        await service.upsert_thing(st_thing)


@pytest.mark.asyncio
async def test_service_update_thing_fail(mock_frost_sta_service):
    with pytest.raises(SensorThingsServiceError):
        mock_service, mock_list = mock_frost_sta_service
        mock_service.update.side_effect = ValueError("Unable to create entity")

        thing_name = "thing1"
        st_thing = STThing(id="1", name=thing_name)
        mock_list.entities = [
            st_thing,
        ]
        mock_list.__iter__.return_value = iter(mock_list.entities)

        config = Config()
        service = SensorThingsService(config)

        await service.upsert_thing(st_thing)


@pytest.mark.asyncio
async def test_service_get_location(mock_frost_sta_service):
    _, mock_list = mock_frost_sta_service

    location_name = "location1"
    mock_list.entities = [
        STLocation(id="1", name=location_name),
    ]
    mock_list.__iter__.return_value = iter(mock_list.entities)

    config = Config()
    service = SensorThingsService(config)
    location = await service.get_location(location_name)

    assert location.name == location_name


@pytest.mark.asyncio
async def test_service_get_location_not_found(mock_frost_sta_service):
    with pytest.raises(SensorThingsServiceError):
        config = Config()
        service = SensorThingsService(config)

        location_name = "location1"
        await service.get_location(location_name)


@pytest.mark.asyncio
async def test_service_get_sensor(mock_frost_sta_service):
    _, mock_list = mock_frost_sta_service

    sensor_name = "sensor1"
    mock_list.entities = [
        STSensor(id="1", name=sensor_name),
    ]
    mock_list.__iter__.return_value = iter(mock_list.entities)

    config = Config()
    service = SensorThingsService(config)
    sensor = await service.get_sensor(sensor_name)

    assert sensor.name == sensor_name


@pytest.mark.asyncio
async def test_service_get_sensor_not_found(mock_frost_sta_service):
    with pytest.raises(SensorThingsServiceError):
        config = Config()
        service = SensorThingsService(config)

        sensor_name = "sensor1"
        await service.get_sensor(sensor_name)


@pytest.mark.asyncio
async def test_service_get_observed_property(mock_frost_sta_service):
    _, mock_list = mock_frost_sta_service

    observed_property_name = "observed_property1"
    mock_list.entities = [
        STObservedProperty(id="1", name=observed_property_name),
    ]
    mock_list.__iter__.return_value = iter(mock_list.entities)

    config = Config()
    service = SensorThingsService(config)
    observed_property = await service.get_observed_property(observed_property_name)

    assert observed_property.name == observed_property_name


@pytest.mark.asyncio
async def test_service_get_observed_property_not_found(mock_frost_sta_service):
    with pytest.raises(SensorThingsServiceError):
        config = Config()
        service = SensorThingsService(config)

        observed_property_name = "observed_property1"
        await service.get_observed_property(observed_property_name)


@pytest.mark.asyncio
async def test_service_get_datastream(mock_frost_sta_service):
    _, mock_list = mock_frost_sta_service

    datastream_name = "datastream1"
    mock_list.entities = [
        STDatastream(id="1", name=datastream_name),
    ]
    mock_list.__iter__.return_value = iter(mock_list.entities)

    config = Config()
    service = SensorThingsService(config)
    datastream = await service.get_datastream(datastream_name)

    assert datastream.name == datastream_name


@pytest.mark.asyncio
async def test_service_get_datastream_not_found(mock_frost_sta_service):
    with pytest.raises(SensorThingsServiceError):
        config = Config()
        service = SensorThingsService(config)

        datastream_name = "datastream1"
        await service.get_datastream(datastream_name)

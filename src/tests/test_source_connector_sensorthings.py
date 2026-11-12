import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

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
from frost_sta_client.model.ext.entity_list import EntityList
# isort: on

from pipeline.models import (
    STConfig,
    STFeatureOfInterest,
    STHistoricalLocation,
    STLocation,
    STObservation,
    STObservedProperty,
    STSensor,
    STThing,
    STUnitOfMeasurement,
    STUpdate,
    STUpdateType,
)
from pipeline.source_connectors import STConnector
from pipeline.source_connectors.sensorthings import _extract_coordinates
from pipeline.utils import SensorThingsServiceError


@pytest.fixture
def mock_sensorthings_service(mocker):
    service_cls = mocker.patch("pipeline.source_connectors.sensorthings.SensorThingsService")
    mock_service = AsyncMock()
    service_cls.return_value = mock_service

    mock_service.get_things.return_value = []
    mock_service.get_historical_locations_by_thing.return_value = []
    mock_service.get_locations_by_thing.return_value = []
    mock_service.get_sensors.return_value = []
    mock_service.get_observed_properties.return_value = []
    mock_service.get_datastreams.return_value = []
    mock_service.get_observations_by_datastream.return_value = []

    return mock_service


@pytest.fixture
def mock_single_pass(mocker):
    """Mocks asyncio.sleep in sensorthings connector to cancel after initial sync."""

    async def _cancel_sleep(_):
        raise asyncio.CancelledError()

    return mocker.patch(
        "pipeline.source_connectors.sensorthings.asyncio.sleep", side_effect=_cancel_sleep
    )


def verify_queue_item(item: STUpdate, update_type: STUpdateType, payload_type: type) -> None:
    assert item.type == update_type
    assert isinstance(item.payload, payload_type)


def test_sensorthings_connector_name():
    assert STConnector.name == "sensorthings"


@pytest.mark.asyncio
async def test_sensorthings_health_check_success(mock_sensorthings_service):
    mock_sensorthings_service.get_things.return_value = [FrostThing(id="1", name="vessel-1")]

    queue = asyncio.Queue()
    connector = STConnector(queue, STConfig(host="localhost", port=8080))

    is_healthy = await connector.health_check()

    assert is_healthy is True
    mock_sensorthings_service.get_things.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_sensorthings_health_check_failure(mock_sensorthings_service):
    mock_sensorthings_service.get_things.side_effect = SensorThingsServiceError("Connection failed")

    queue = asyncio.Queue()
    connector = STConnector(queue, STConfig(host="localhost", port=8080))

    is_healthy = await connector.health_check()

    assert is_healthy is False
    mock_sensorthings_service.get_things.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_sensorthings_initial_sync_empty(mock_sensorthings_service, mock_single_pass):
    queue = asyncio.Queue()
    connector = STConnector(queue, STConfig(host="localhost", port=8080, poll_interval_seconds=1))
    await connector.run()

    assert queue.qsize() == 0


@pytest.mark.asyncio
async def test_sensorthings_initial_sync_full(mock_sensorthings_service, mock_single_pass):
    thing = FrostThing(
        id="1", name="vessel-1", description="Main vessel", properties={"type": "research-vessel"}
    )

    location = FrostLocation(
        id="loc-1",
        name="loc-1",
        description="Location 1",
        location={"type": "Point", "coordinates": [2.5, 51.5]},
    )
    thing_entities = EntityList("Thing")
    thing_entities.entities = [thing]
    location.things = thing_entities

    hist_location = FrostHistoricalLocation(id="hl-1", time="2026-10-20T10:00:00+00:00")
    loc_entities = EntityList("Location")
    loc_entities.entities = [location]
    hist_location.locations = loc_entities

    sensor = FrostSensor(
        id="sensor-1",
        name="sensor-depth",
        description="Echo sounder",
        encoding_type="application/pdf",
        metadata="http://example.com/sensor",
        properties={"model": "ES-100"},
    )
    observed_property = FrostObservedProperty(
        id="prop-1",
        name="water-depth",
        description="Depth below surface",
        definition="http://vocab.nerc.ac.uk/depth",
        properties={},
    )
    uom = FrostUnitOfMeasurement(
        name="metre", symbol="m", definition="http://qudt.org/vocab/unit/M"
    )

    datastream = FrostDatastream(
        id="ds-1",
        name="vessel-1/depth",
        description="Datastream for vessel depth",
        unit_of_measurement=uom,
        thing=thing,
        sensor=sensor,
        observed_property=observed_property,
    )

    foi = FrostFeatureOfInterest(
        id="foi-1",
        name="foi-1",
        description="FOI at coordinate",
        encoding_type="application/geo+json",
        feature={"type": "Point", "coordinates": [2.5, 51.5]},
        properties={"station": "A1"},
    )
    observation = FrostObservation(
        id="obs-1",
        phenomenon_time="2026-10-20T10:00:00+00:00",
        result=18.42,
        result_time="2026-10-20T10:00:05+00:00",
        feature_of_interest=foi,
    )

    mock_sensorthings_service.get_things.return_value = [thing]
    mock_sensorthings_service.get_historical_locations_by_thing.return_value = [hist_location]
    mock_sensorthings_service.get_locations_by_thing.return_value = [location]
    mock_sensorthings_service.get_sensors.return_value = [sensor]
    mock_sensorthings_service.get_observed_properties.return_value = [observed_property]
    mock_sensorthings_service.get_datastreams.return_value = [datastream]
    mock_sensorthings_service.get_observations_by_datastream.return_value = [observation]

    queue = asyncio.Queue()
    connector = STConnector(queue, STConfig(host="localhost", port=8080, poll_interval_seconds=1))
    await connector.run()

    assert queue.qsize() == 8

    # 1. Thing
    item = await queue.get()
    verify_queue_item(item, STUpdateType.UPSERT_THING, STThing)
    assert item.payload.id == "vessel-1"
    assert item.payload.description == "Main vessel"
    assert item.payload.properties == {"type": "research-vessel"}

    # 2. Location from HistoricalLocation sync
    item = await queue.get()
    verify_queue_item(item, STUpdateType.UPSERT_LOCATION, STLocation)
    assert item.payload.thing_name == "vessel-1"
    assert item.payload.name == "loc-1"
    assert item.payload.longitude == 2.5
    assert item.payload.latitude == 51.5

    # 3. Historical Location
    item = await queue.get()
    verify_queue_item(item, STUpdateType.UPSERT_HISTORICAL_LOCATION, STHistoricalLocation)
    assert item.payload.location_name == "loc-1"
    assert item.payload.thing_name == "vessel-1"
    assert item.payload.time == datetime(2026, 10, 20, 10, 0, 0, tzinfo=UTC)

    # 4. Location from ThingsLocation sync
    item = await queue.get()
    verify_queue_item(item, STUpdateType.UPSERT_LOCATION, STLocation)
    assert item.payload.thing_name == "vessel-1"
    assert item.payload.name == "loc-1"

    # 5. Sensor
    item = await queue.get()
    verify_queue_item(item, STUpdateType.UPSERT_SENSOR, STSensor)
    assert item.payload.name == "sensor-depth"
    assert item.payload.metadata == "http://example.com/sensor"

    # 6. Observed Property
    item = await queue.get()
    verify_queue_item(item, STUpdateType.UPSERT_OBSERVED_PROPERTY, STObservedProperty)
    assert item.payload.name == "water-depth"
    assert item.payload.definition == "http://vocab.nerc.ac.uk/depth"

    # 7. Unit of Measurement
    item = await queue.get()
    verify_queue_item(item, STUpdateType.CREATE_UNIT_OF_MEASUREMENT, STUnitOfMeasurement)
    assert item.payload.name == "metre"
    assert item.payload.symbol == "m"

    # 8. Observation
    item = await queue.get()
    verify_queue_item(item, STUpdateType.CREATE_OBSERVATION, STObservation)
    assert item.payload.name == "vessel-1/depth"
    assert item.payload.thing_name == "vessel-1"
    assert item.payload.sensor_name == "sensor-depth"
    assert item.payload.property_name == "water-depth"
    assert item.payload.result == 18.42
    assert isinstance(item.payload.feature_of_interest, STFeatureOfInterest)
    assert item.payload.feature_of_interest.name == "foi-1"


@pytest.mark.asyncio
async def test_sensorthings_sync_subsequent_polling(mock_sensorthings_service, mocker):
    thing = FrostThing(id="1", name="vessel-1", description="Main vessel", properties={})
    sensor = FrostSensor(id="sensor-1", name="sensor-depth", description="Echo sounder")
    observed_property = FrostObservedProperty(id="prop-1", name="water-depth", description="Depth")
    datastream = FrostDatastream(
        id="ds-1",
        name="vessel-1/depth",
        thing=thing,
        sensor=sensor,
        observed_property=observed_property,
    )

    obs1 = FrostObservation(
        id="obs-1",
        phenomenon_time="2026-10-20T10:00:00+00:00",
        result=10.0,
    )
    obs2 = FrostObservation(
        id="obs-2",
        phenomenon_time="2026-10-20T10:01:00+00:00",
        result=12.5,
    )

    mock_sensorthings_service.get_things.return_value = [thing]
    mock_sensorthings_service.get_historical_locations_by_thing.return_value = []
    mock_sensorthings_service.get_locations_by_thing.return_value = []
    mock_sensorthings_service.get_sensors.return_value = [sensor]
    mock_sensorthings_service.get_observed_properties.return_value = [observed_property]
    mock_sensorthings_service.get_datastreams.return_value = [datastream]
    mock_sensorthings_service.get_observations_by_datastream.side_effect = [
        [obs1],
        [obs2],
        [],
    ]

    queue = asyncio.Queue()
    connector = STConnector(queue, STConfig(host="localhost", port=8080, poll_interval_seconds=1))

    sleep_count = 0

    async def mock_sleep(_):
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count >= 2:
            raise asyncio.CancelledError()

    mocker.patch("pipeline.source_connectors.sensorthings.asyncio.sleep", side_effect=mock_sleep)

    await connector.run()

    # Should have: 2 thing, 2 sensor, 2 observed property, 2 observations
    assert queue.qsize() == 8

    # Check that subsequent call used the filter with previous phenomenonTime
    assert mock_sensorthings_service.get_observations_by_datastream.call_count >= 2
    second_call_kwargs = mock_sensorthings_service.get_observations_by_datastream.call_args_list[
        1
    ].kwargs
    assert second_call_kwargs.get("filter") == "phenomenonTime gt 2026-10-20T10:00:00+00:00"


@pytest.mark.asyncio
async def test_sensorthings_skip_invalid_entities(mock_sensorthings_service, mock_single_pass):
    # 1. Thing with name=None should be skipped
    unnamed_thing = FrostThing(id="none-thing")
    unnamed_thing.name = None  # type: ignore[assignment]
    valid_thing = FrostThing(id="1", name="vessel-1")

    # 2. Historical location without locations should be skipped
    hl_no_loc = FrostHistoricalLocation(id="hl-empty", time="2026-10-20T10:00:00+00:00")
    hl_no_loc.locations = None

    # 3. Observed property with name=None should be skipped
    unnamed_prop = FrostObservedProperty(id="prop-none")
    unnamed_prop.name = None  # type: ignore[assignment]
    valid_prop = FrostObservedProperty(id="prop-1", name="depth")

    # 4. Datastream missing thing/sensor/observed_property should be skipped
    ds_missing_thing = FrostDatastream(
        id="ds-no-thing",
        name="ds-no-thing",
        thing=None,
        sensor=FrostSensor(id="s-1", name="sensor-1"),
        observed_property=valid_prop,
    )
    ds_missing_sensor = FrostDatastream(
        id="ds-no-sensor",
        name="ds-no-sensor",
        thing=valid_thing,
        sensor=None,
        observed_property=valid_prop,
    )
    ds_missing_prop = FrostDatastream(
        id="ds-no-prop",
        name="ds-no-prop",
        thing=valid_thing,
        sensor=FrostSensor(id="s-1", name="sensor-1"),
        observed_property=None,
    )
    valid_ds = FrostDatastream(
        id="ds-valid",
        name="vessel-1/depth",
        thing=valid_thing,
        sensor=FrostSensor(id="s-1", name="sensor-1"),
        observed_property=valid_prop,
    )
    valid_obs = FrostObservation(
        id="obs-1",
        phenomenon_time="2026-10-20T10:00:00+00:00",
        result=5.0,
        feature_of_interest=None,
    )

    mock_sensorthings_service.get_things.return_value = [unnamed_thing, valid_thing]
    mock_sensorthings_service.get_historical_locations_by_thing.return_value = [hl_no_loc]
    mock_sensorthings_service.get_locations_by_thing.return_value = []
    mock_sensorthings_service.get_sensors.return_value = []
    mock_sensorthings_service.get_observed_properties.return_value = [unnamed_prop, valid_prop]
    mock_sensorthings_service.get_datastreams.return_value = [
        ds_missing_thing,
        ds_missing_sensor,
        ds_missing_prop,
        valid_ds,
    ]
    mock_sensorthings_service.get_observations_by_datastream.return_value = [valid_obs]

    queue = asyncio.Queue()
    connector = STConnector(queue, STConfig(host="localhost", port=8080, poll_interval_seconds=1))
    await connector.run()

    # Queue should contain only valid_thing, valid_prop, and valid_obs
    assert queue.qsize() == 3

    item1 = await queue.get()
    verify_queue_item(item1, STUpdateType.UPSERT_THING, STThing)
    assert item1.payload.id == "vessel-1"

    item2 = await queue.get()
    verify_queue_item(item2, STUpdateType.UPSERT_OBSERVED_PROPERTY, STObservedProperty)
    assert item2.payload.name == "depth"

    item3 = await queue.get()
    verify_queue_item(item3, STUpdateType.CREATE_OBSERVATION, STObservation)
    assert item3.payload.result == 5.0
    assert item3.payload.feature_of_interest is None


@pytest.mark.parametrize(
    "geometry, expected_coords",
    [
        ({"type": "Point", "coordinates": [3.1415, 51.205]}, (3.1415, 51.205)),
        ({"type": "Polygon", "coordinates": [[[0, 0], [1, 1], [0, 1], [0, 0]]]}, (0.0, 0.0)),
        ({"type": "LineString", "coordinates": [[0, 0], [1, 1]]}, (0.0, 0.0)),
        ("invalid-geometry-string", (0.0, 0.0)),
        (None, (0.0, 0.0)),
        ({}, (0.0, 0.0)),
    ],
)
def test_extract_coordinates(geometry, expected_coords):
    loc = FrostLocation(id="loc-test", name="test-location", location=geometry)
    coords = _extract_coordinates(loc)
    assert coords == expected_coords


@pytest.mark.asyncio
async def test_sensorthings_fallback_entity_names(mock_sensorthings_service, mock_single_pass):
    thing = FrostThing(id="42", name="vessel-fallback")
    location = FrostLocation(id="99", location={"type": "Point", "coordinates": [1.0, 2.0]})
    location.name = None  # type: ignore[assignment]
    sensor = FrostSensor(id="s-1", name="sensor-1")
    observed_property = FrostObservedProperty(id="p-1", name="prop-1")
    uom = FrostUnitOfMeasurement()
    uom.name = None  # type: ignore[assignment]
    uom.symbol = None  # type: ignore[assignment]
    uom.definition = None  # type: ignore[assignment]
    datastream = FrostDatastream(
        id="101",
        unit_of_measurement=uom,
        thing=thing,
        sensor=sensor,
        observed_property=observed_property,
    )
    datastream.name = None  # type: ignore[assignment]
    foi = FrostFeatureOfInterest(id="foi-1", feature={"type": "Point", "coordinates": [1.0, 2.0]})
    foi.name = None  # type: ignore[assignment]
    observation = FrostObservation(
        id="obs-1",
        phenomenon_time="2026-10-20T10:00:00+00:00",
        result=42.0,
        feature_of_interest=foi,
    )

    mock_sensorthings_service.get_things.return_value = [thing]
    mock_sensorthings_service.get_historical_locations_by_thing.return_value = []
    mock_sensorthings_service.get_locations_by_thing.return_value = [location]
    mock_sensorthings_service.get_sensors.return_value = [sensor]
    mock_sensorthings_service.get_observed_properties.return_value = [observed_property]
    mock_sensorthings_service.get_datastreams.return_value = [datastream]
    mock_sensorthings_service.get_observations_by_datastream.return_value = [observation]

    queue = asyncio.Queue()
    connector = STConnector(queue, STConfig(host="localhost", port=8080, poll_interval_seconds=1))
    await connector.run()

    items = []
    while not queue.empty():
        items.append(await queue.get())

    # Verify fallback name for Location: st:99
    loc_item = next(it for it in items if it.type == STUpdateType.UPSERT_LOCATION)
    assert loc_item.payload.name == "st:99"

    # Verify fallback for UnitOfMeasurement
    uom_item = next(it for it in items if it.type == STUpdateType.CREATE_UNIT_OF_MEASUREMENT)
    assert uom_item.payload.name == "<Name not set>"
    assert uom_item.payload.symbol == "<Symbol not set>"
    assert uom_item.payload.definition == "<Definition not set>"

    # Verify fallback for Datastream (st#101) & FOI
    obs_item = next(it for it in items if it.type == STUpdateType.CREATE_OBSERVATION)
    assert obs_item.payload.name == "st#101"
    assert obs_item.payload.feature_of_interest.name == "st#101"


@pytest.mark.asyncio
async def test_sensorthings_service_error_handling(mock_sensorthings_service):
    mock_sensorthings_service.get_things.side_effect = SensorThingsServiceError(
        "Service unavailable"
    )

    queue = asyncio.Queue()
    connector = STConnector(queue, STConfig(host="localhost", port=8080, poll_interval_seconds=1))
    await connector.run()

    # Connector should have caught error and stopped without crashing
    assert not connector._running
    assert queue.qsize() == 0


@pytest.mark.asyncio
async def test_sensorthings_stop():
    queue = asyncio.Queue()
    connector = STConnector(queue, STConfig(host="localhost", port=8080))
    connector._running = True

    await connector.stop()

    assert not connector._running

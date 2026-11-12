import asyncio

import pytest

from pipeline.convertors import SK2STConvertor
from pipeline.models import (
    SK2STConfig,
    SKObservedProperty,
    SKObservedPropertyContext,
    SKObservedPropertyContextKey,
    SKObservedPropertyDisplayUnits,
    SKObservedPropertyMetaMultiple,
    SKObservedPropertyMetaProperties,
    SKObservedPropertyMetaSingle,
    SKPosition,
    SKSensor,
    SKSensorContext,
    SKSensorContextKey,
    SKSensorMetaN0183,
    SKSensorMetaN2000,
    SKUnitContext,
    SKUpdate,
    SKUpdateType,
    SKVessel,
    SKVesselContext,
    SKVesselContextKey,
    STUpdateType,
)


@pytest.mark.context
@pytest.mark.asyncio
async def test_vessel_context_mapping_injects_definition():
    """When a VesselContextKey matches, the Thing definition should be set from the context."""
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    context_mapping = {
        SKVesselContextKey(id="urn:signalk:vessels:test_vessel"): SKVesselContext(
            definition="https://example.org/def/vessel/test"
        )
    }
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
        context_mapping=context_mapping,
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.VESSEL_METADATA,
            payload=SKVessel(
                id="urn:signalk:vessels:test_vessel",
                name="Test Vessel",
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 1
    st_update = await sink_queue.get()
    assert st_update.type == STUpdateType.UPSERT_THING
    assert st_update.payload.definition == "https://example.org/def/vessel/test"


@pytest.mark.context
@pytest.mark.asyncio
async def test_vessel_context_mapping_no_match():
    """When no VesselContextKey matches, the Thing definition should be None."""
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    context_mapping = {
        SKVesselContextKey(id="urn:signalk:vessels:other_vessel"): SKVesselContext(
            definition="https://example.org/def/vessel/other"
        )
    }
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
        context_mapping=context_mapping,
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.VESSEL_METADATA,
            payload=SKVessel(
                id="urn:signalk:vessels:test_vessel",
                name="Test Vessel",
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 1
    st_update = await sink_queue.get()
    assert st_update.type == STUpdateType.UPSERT_THING
    assert st_update.payload.definition is None


@pytest.mark.context
@pytest.mark.asyncio
async def test_vessel_context_mapping_empty_dict():
    """When context_mapping is an empty dict, the Thing definition should be None."""
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
        context_mapping={},
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.VESSEL_METADATA,
            payload=SKVessel(
                id="urn:signalk:vessels:test_vessel",
                name="Test Vessel",
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 1
    st_update = await sink_queue.get()
    assert st_update.type == STUpdateType.UPSERT_THING
    assert st_update.payload.definition is None


@pytest.mark.context
@pytest.mark.asyncio
async def test_sensor_context_mapping_injects_definition():
    """When a SensorContextKey matches, the Sensor definition should be set from the context."""
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    context_mapping = {
        SKSensorContextKey(context="vessels.self", name="test.sensor"): SKSensorContext(
            definition="https://example.org/def/sensor/test"
        )
    }
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
        context_mapping=context_mapping,
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.SENSOR_METADATA,
            payload=SKSensor(
                context="vessels.self",
                name="test.sensor",
                meta=SKSensorMetaN0183(
                    talker="II",
                    sentences={"VHW": "2026-10-26T21:56:20.589Z"},
                ),
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 1
    st_update = await sink_queue.get()
    assert st_update.type == STUpdateType.UPSERT_SENSOR
    assert st_update.payload.properties.get("definition") == "https://example.org/def/sensor/test"


@pytest.mark.context
@pytest.mark.asyncio
async def test_sensor_context_mapping_no_match():
    """When no SensorContextKey matches, the Sensor definition should be None."""
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    context_mapping = {
        SKSensorContextKey(context="vessels.self", name="other.sensor"): SKSensorContext(
            definition="https://example.org/def/sensor/other"
        )
    }
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
        context_mapping=context_mapping,
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.SENSOR_METADATA,
            payload=SKSensor(
                context="vessels.self",
                name="test.sensor",
                meta=SKSensorMetaN0183(
                    talker="II",
                    sentences={"VHW": "2026-10-26T21:56:20.589Z"},
                ),
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 1
    st_update = await sink_queue.get()
    assert st_update.type == STUpdateType.UPSERT_SENSOR
    assert st_update.payload.properties.get("definition") is None


@pytest.mark.context
@pytest.mark.asyncio
async def test_sensor_context_mapping_n2000():
    """Sensor context mapping should work with NMEA2000 sensor metadata as well."""
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    context_mapping = {
        SKSensorContextKey(context="vessels.self", name="n2k.sensor"): SKSensorContext(
            definition="https://example.org/def/sensor/n2k"
        )
    }
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
        context_mapping=context_mapping,
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.SENSOR_METADATA,
            payload=SKSensor(
                context="vessels.self",
                name="n2k.sensor",
                meta={
                    "n2k": SKSensorMetaN2000(
                        src="115",
                        pgns={"1234": "2026-10-26T21:56:20.589Z"},
                    ),
                },
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 1
    st_update = await sink_queue.get()
    assert st_update.type == STUpdateType.UPSERT_SENSOR
    assert st_update.payload.properties.get("definition") == "https://example.org/def/sensor/n2k"


@pytest.mark.context
@pytest.mark.asyncio
async def test_observed_property_single_context_mapping_injects_definition():
    """When an ObservedPropertyContextKey matches for a single meta, definition and unit should be injected."""
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    context_mapping = {
        SKObservedPropertyContextKey(
            context="vessels.self",
            source="test.sensor",
            path="environment.temperature",
        ): SKObservedPropertyContext(
            definition="https://example.org/def/property/temp",
            unit=SKUnitContext(
                definition="https://example.org/def/unit/celsius",
                symbol="°C",
            ),
        )
    }
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
        context_mapping=context_mapping,
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.OBSERVED_PROPERTY_METADATA,
            payload=SKObservedProperty(
                context="vessels.self",
                source="test.sensor",
                path="environment.temperature",
                meta=SKObservedPropertyMetaSingle(
                    description="Water temperature",
                    displayUnits=SKObservedPropertyDisplayUnits(
                        category="temperature",
                        targetUnit="C",
                        formula=None,
                        inverseFormula=None,
                        symbol="C",
                        displayFormat="0.0",
                    ),
                    units="C",
                ),
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 2
    updates = []
    while not sink_queue.empty():
        updates.append(await sink_queue.get())

    op_update = next(u for u in updates if u.type == STUpdateType.UPSERT_OBSERVED_PROPERTY)
    unit_update = next(u for u in updates if u.type == STUpdateType.CREATE_UNIT_OF_MEASUREMENT)

    assert op_update.payload.definition == "https://example.org/def/property/temp"
    assert unit_update.payload.symbol == "°C"
    assert unit_update.payload.definition == "https://example.org/def/unit/celsius"


@pytest.mark.context
@pytest.mark.asyncio
async def test_observed_property_single_context_mapping_no_match():
    """When no ObservedPropertyContextKey matches for a single meta, defaults should be used."""
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    context_mapping = {
        SKObservedPropertyContextKey(
            context="vessels.self",
            source="other.sensor",
            path="environment.temperature",
        ): SKObservedPropertyContext(
            definition="https://example.org/def/property/other",
            unit=SKUnitContext(
                definition="https://example.org/def/unit/other",
                symbol="X",
            ),
        )
    }
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
        context_mapping=context_mapping,
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.OBSERVED_PROPERTY_METADATA,
            payload=SKObservedProperty(
                context="vessels.self",
                source="test.sensor",
                path="environment.temperature",
                meta=SKObservedPropertyMetaSingle(
                    description="Water temperature",
                    displayUnits=SKObservedPropertyDisplayUnits(
                        category="temperature",
                        targetUnit="C",
                        formula=None,
                        inverseFormula=None,
                        symbol="C",
                        displayFormat="0.0",
                    ),
                    units="C",
                ),
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 2
    updates = []
    while not sink_queue.empty():
        updates.append(await sink_queue.get())

    op_update = next(u for u in updates if u.type == STUpdateType.UPSERT_OBSERVED_PROPERTY)
    unit_update = next(u for u in updates if u.type == STUpdateType.CREATE_UNIT_OF_MEASUREMENT)

    assert op_update.payload.definition is None
    assert unit_update.payload.symbol == "C"
    assert unit_update.payload.definition == "<Definition not set>"


@pytest.mark.context
@pytest.mark.asyncio
async def test_observed_property_multiple_context_mapping_injects_definition():
    """When an ObservedPropertyContextKey matches for a multiple meta, definition and unit should be injected for each sub-property."""
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    context_mapping = {
        SKObservedPropertyContextKey(
            context="vessels.self",
            source="test.sensor",
            path="environment.current",
        ): SKObservedPropertyContext(
            definition="https://example.org/def/property/current",
            unit=SKUnitContext(
                definition="https://example.org/def/unit/ms",
                symbol="m/s",
            ),
        )
    }
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
        context_mapping=context_mapping,
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.OBSERVED_PROPERTY_METADATA,
            payload=SKObservedProperty(
                context="vessels.self",
                source="test.sensor",
                path="environment.current",
                meta=SKObservedPropertyMetaMultiple(
                    description="Current vector",
                    properties={
                        "drift": SKObservedPropertyMetaProperties(
                            type="number",
                            description="Speed component",
                            example=3.12,
                            units="m/s",
                        ),
                        "setTrue": SKObservedPropertyMetaProperties(
                            type="number",
                            description="Direction component",
                            example=2.155,
                            units="rad",
                        ),
                    },
                ),
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 4
    updates = []
    while not sink_queue.empty():
        updates.append(await sink_queue.get())

    op_updates = [u for u in updates if u.type == STUpdateType.UPSERT_OBSERVED_PROPERTY]
    unit_updates = [u for u in updates if u.type == STUpdateType.CREATE_UNIT_OF_MEASUREMENT]

    assert len(op_updates) == 2
    assert len(unit_updates) == 2

    for op in op_updates:
        assert op.payload.definition == "https://example.org/def/property/current"
    for unit in unit_updates:
        assert unit.payload.symbol == "m/s"
        assert unit.payload.definition == "https://example.org/def/unit/ms"


@pytest.mark.context
@pytest.mark.asyncio
async def test_observed_property_dict_context_mapping_injects_definition():
    """When an ObservedPropertyContextKey matches for a dict meta, definition and unit should be injected."""
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    context_mapping = {
        SKObservedPropertyContextKey(
            context="vessels.self",
            source="test.sensor",
            path="environment.pressure",
        ): SKObservedPropertyContext(
            definition="https://example.org/def/property/pressure",
            unit=SKUnitContext(
                definition="https://example.org/def/unit/hpa",
                symbol="hPa",
            ),
        )
    }
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
        context_mapping=context_mapping,
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.OBSERVED_PROPERTY_METADATA,
            payload=SKObservedProperty(
                context="vessels.self",
                source="test.sensor",
                path="environment.pressure",
                meta={
                    "description": "Atmospheric pressure",
                    "units": "hPa",
                },
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 2
    updates = []
    while not sink_queue.empty():
        updates.append(await sink_queue.get())

    op_update = next(u for u in updates if u.type == STUpdateType.UPSERT_OBSERVED_PROPERTY)
    unit_update = next(u for u in updates if u.type == STUpdateType.CREATE_UNIT_OF_MEASUREMENT)

    assert op_update.payload.definition == "https://example.org/def/property/pressure"
    assert unit_update.payload.symbol == "hPa"
    assert unit_update.payload.definition == "https://example.org/def/unit/hpa"


@pytest.mark.context
@pytest.mark.asyncio
async def test_context_mapping_with_mixed_keys():
    """A single context_mapping dict can contain all three key types and they should all resolve correctly."""
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    context_mapping = {
        SKVesselContextKey(id="urn:signalk:vessels:mixed"): SKVesselContext(
            definition="https://example.org/def/vessel/mixed"
        ),
        SKSensorContextKey(context="vessels.self", name="mixed.sensor"): SKSensorContext(
            definition="https://example.org/def/sensor/mixed"
        ),
        SKObservedPropertyContextKey(
            context="vessels.self",
            source="mixed.sensor",
            path="environment.salinity",
        ): SKObservedPropertyContext(
            definition="https://example.org/def/property/salinity",
            unit=SKUnitContext(
                definition="https://example.org/def/unit/psu",
                symbol="PSU",
            ),
        ),
    }
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
        context_mapping=context_mapping,
    )
    convertor_task = asyncio.create_task(convertor.run())

    # Send vessel update
    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.VESSEL_METADATA,
            payload=SKVessel(
                id="urn:signalk:vessels:mixed",
                name="Mixed Vessel",
            ),
        )
    )

    # Send sensor update
    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.SENSOR_METADATA,
            payload=SKSensor(
                context="vessels.self",
                name="mixed.sensor",
                meta=SKSensorMetaN0183(
                    talker="II",
                    sentences={"CTD": "2026-10-26T21:56:20.589Z"},
                ),
            ),
        )
    )

    # Send observed property update
    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.OBSERVED_PROPERTY_METADATA,
            payload=SKObservedProperty(
                context="vessels.self",
                source="mixed.sensor",
                path="environment.salinity",
                meta={
                    "description": "Salinity",
                    "units": "PSU",
                },
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 4
    updates = []
    while not sink_queue.empty():
        updates.append(await sink_queue.get())

    thing_update = next(u for u in updates if u.type == STUpdateType.UPSERT_THING)
    sensor_update = next(u for u in updates if u.type == STUpdateType.UPSERT_SENSOR)
    op_update = next(u for u in updates if u.type == STUpdateType.UPSERT_OBSERVED_PROPERTY)
    unit_update = next(u for u in updates if u.type == STUpdateType.CREATE_UNIT_OF_MEASUREMENT)

    assert thing_update.payload.definition == "https://example.org/def/vessel/mixed"
    assert (
        sensor_update.payload.properties.get("definition") == "https://example.org/def/sensor/mixed"
    )
    assert op_update.payload.definition == "https://example.org/def/property/salinity"
    assert unit_update.payload.symbol == "PSU"
    assert unit_update.payload.definition == "https://example.org/def/unit/psu"


@pytest.mark.context
@pytest.mark.asyncio
async def test_context_mapping_wrong_value_type_ignored():
    """If a key maps to a non-context value, it should be ignored and definition should be None."""
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    context_mapping = {
        SKVesselContextKey(id="urn:signalk:vessels:test_vessel"): "not-a-context-object"
    }
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
        context_mapping=context_mapping,
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.VESSEL_METADATA,
            payload=SKVessel(
                id="urn:signalk:vessels:test_vessel",
                name="Test Vessel",
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 1
    st_update = await sink_queue.get()
    assert st_update.type == STUpdateType.UPSERT_THING
    assert st_update.payload.definition is None


@pytest.mark.context
@pytest.mark.asyncio
async def test_vessel_with_position_and_context_mapping():
    """Vessel with position data should still get context definition injected."""
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    context_mapping = {
        SKVesselContextKey(id="urn:signalk:vessels:geo_vessel"): SKVesselContext(
            definition="https://example.org/def/vessel/geo"
        )
    }
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
        context_mapping=context_mapping,
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.VESSEL_METADATA,
            payload=SKVessel(
                id="urn:signalk:vessels:geo_vessel",
                name="Geo Vessel",
                position=SKPosition(
                    latitude=51.0,
                    longitude=3.0,
                    source="test",
                ),
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 1
    st_update = await sink_queue.get()
    assert st_update.type == STUpdateType.UPSERT_THING
    assert st_update.payload.definition == "https://example.org/def/vessel/geo"
    assert st_update.payload.location is not None
    assert st_update.payload.location.latitude == 51.0
    assert st_update.payload.location.longitude == 3.0

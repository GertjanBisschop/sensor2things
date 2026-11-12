import asyncio
from datetime import UTC, datetime

import pytest

from pipeline.convertors import SK2STConvertor
from pipeline.models import (
    SK2STConfig,
    SKDelta,
    SKDeltaSourceN0183,
    SKDeltaSourceN2000,
    SKDeltaSourceType,
    SKDeltaUpdate,
    SKDeltaValue,
    SKObservedProperty,
    SKObservedPropertyDisplayUnits,
    SKObservedPropertyMetaMultiple,
    SKObservedPropertyMetaProperties,
    SKObservedPropertyMetaSingle,
    SKSensor,
    SKSensorMetaN0183,
    SKSensorMetaN2000,
    SKUpdate,
    SKUpdateType,
    SKVessel,
)


@pytest.mark.asyncio
async def test_vessel_update_conversion():
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.VESSEL_METADATA,
            payload=SKVessel(
                id="test_vessel",
                name="Test Vessel",
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 1


@pytest.mark.asyncio
async def test_sensor_n0183_update_conversion():
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.SENSOR_METADATA,
            payload=SKSensor(
                context="test_vessel",
                name="test.sensor",
                meta=SKSensorMetaN0183(
                    talker="II",
                    sentences={
                        "VHW": "2026-10-26T21:56:20.589Z",
                        "VPW": "2026-10-26T21:56:20.200Z",
                    },
                ),
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 1


@pytest.mark.asyncio
async def test_sensor_n2000_update_conversion():
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.SENSOR_METADATA,
            payload=SKSensor(
                context="test_vessel",
                name="test.sensor",
                meta={
                    "n2k": SKSensorMetaN2000(
                        src="115",
                        pgns={
                            "1234": "2026-10-26T21:56:20.589Z",
                            "5678": "2026-10-26T21:56:20.200Z",
                        },
                    ),
                },
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 1


@pytest.mark.asyncio
async def test_sensor_random_update_conversion():
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.SENSOR_METADATA,
            payload=SKSensor(
                context="test_vessel",
                name="test.sensor",
                meta={
                    "random": "test",
                    "more": "info",
                },
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 1


@pytest.mark.asyncio
async def test_observed_property_single_update_conversion():
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.OBSERVED_PROPERTY_METADATA,
            payload=SKObservedProperty(
                context="test_vessel",
                source="test.sensor",
                path="test.observed_property",
                meta=SKObservedPropertyMetaSingle(
                    description="test description",
                    displayUnits=SKObservedPropertyDisplayUnits(
                        category="speed",
                        targetUnit="kn",
                        formula="value * 1.94384",
                        inverseFormula="value * 0.514444",
                        symbol="kn",
                        displayFormat="0.0",
                    ),
                    units="m/s",
                ),
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 2


@pytest.mark.asyncio
async def test_observed_property_multiple_update_conversion():
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.OBSERVED_PROPERTY_METADATA,
            payload=SKObservedProperty(
                context="test_vessel",
                source="test.sensor",
                path="test.observed_property",
                meta=SKObservedPropertyMetaMultiple(
                    description="test description",
                    properties={
                        "drift": SKObservedPropertyMetaProperties(
                            type="number",
                            description="The speed component of the water current vector",
                            example=3.12,
                            units="m/s",
                        ),
                        "setTrue": SKObservedPropertyMetaProperties(
                            type="number",
                            description="The direction component of the water current vector referenced to true (geographic) north",
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


@pytest.mark.asyncio
async def test_observed_property_dict_update_conversion():
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.OBSERVED_PROPERTY_METADATA,
            payload=SKObservedProperty(
                context="test_vessel",
                source="test.sensor",
                path="test.observed_property",
                meta={
                    "description": "test description",
                    "units": "m/s",
                },
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 2


@pytest.mark.asyncio
async def test_delta_nav_update_conversion():
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.DELTA,
            payload=SKDelta(
                context="test_vessel",
                updates=[
                    SKDeltaUpdate(
                        source=SKDeltaSourceN0183(
                            sentence="GLL",
                            talker="GP",
                            type=SKDeltaSourceType.NMEA0183,
                            label="nmea0183-sample-data",
                        ),
                        source_ref="nmea0183-sample-data.GP",
                        timestamp=datetime.now(UTC),
                        values=[
                            SKDeltaValue(
                                path="navigation.position",
                                value={
                                    "latitude": 123.45,
                                    "longitude": 123.45,
                                },
                            )
                        ],
                    )
                ],
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 1


@pytest.mark.asyncio
async def test_delta_env_update_conversion():
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.DELTA,
            payload=SKDelta(
                context="test_vessel",
                updates=[
                    SKDeltaUpdate(
                        source=SKDeltaSourceN0183(
                            sentence="VDR",
                            talker="II",
                            type=SKDeltaSourceType.NMEA0183,
                            label="nmea0183-sample-data",
                        ),
                        source_ref="nmea0183-sample-data.II",
                        timestamp=datetime.now(UTC),
                        values=[
                            SKDeltaValue(
                                path="environment.current",
                                value={"setTrue": 0, "setMagnetic": 0, "drift": 0},
                            )
                        ],
                    )
                ],
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 3


@pytest.mark.asyncio
async def test_delta_env_n2000_update_conversion():
    src_queue = asyncio.Queue()
    sink_queue = asyncio.Queue()
    convertor = SK2STConvertor(
        source_queue=src_queue,
        sink_queue=sink_queue,
        config=SK2STConfig(),
    )
    convertor_task = asyncio.create_task(convertor.run())

    await src_queue.put(
        SKUpdate(
            type=SKUpdateType.DELTA,
            payload=SKDelta(
                context="test_vessel",
                updates=[
                    SKDeltaUpdate(
                        source=SKDeltaSourceN2000(
                            label="n2k-sample-data",
                            type=SKDeltaSourceType.NMEA2000,
                            pgn=128267,
                            src="115",
                        ),
                        source_ref="n2k-sample-data.115",
                        timestamp=datetime.now(UTC),
                        values=[
                            SKDeltaValue(
                                path="environment.depth.belowTransducer",
                                value=13.63,
                            )
                        ],
                    )
                ],
            ),
        )
    )

    await asyncio.sleep(0.1)
    await convertor.stop()
    await convertor_task

    assert sink_queue.qsize() == 1

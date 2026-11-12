import asyncio
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from pipeline.models import (
    SKConfig,
    SKDelta,
    SKObservedProperty,
    SKPosition,
    SKSensor,
    SKUpdate,
    SKUpdateType,
    SKVessel,
)
from pipeline.source_connectors import SKConnector


@pytest.fixture
def mock_rest_api(mocker):
    rest_api_cls = mocker.patch("pipeline.source_connectors.signalk.SignalKRestAPI")
    mock_rest_api = AsyncMock()
    rest_api_cls.return_value = mock_rest_api

    mock_rest_api.get_identity.return_value = "self"
    mock_rest_api.get_name.return_value = None
    mock_rest_api.get_position.return_value = SKPosition(
        latitude=59.895,
        longitude=23.346,
        description="The position of the vessel in 2 or 3 dimensions (WGS84 datum)",
        source="nmea0183-sample-data.GP",
    )
    mock_rest_api.get_token.return_value = "my_token"

    return mock_rest_api


@pytest.fixture
def mock_ws(mocker):
    mock_ws = AsyncMock()
    mock_ws.__aenter__.return_value = mock_ws
    mock_ws.recv.return_value = """{
            "name": "signalk-server",
            "version": "2.33.0",
            "self": "vessels.urn:mrn:signalk:uuid:c130874d-4b4d-4188-91a1-71eeda7d7f12",
            "roles": [
                "master",
                "main"
            ],
            "timestamp": "2026-10-20T10:34:17.325Z",
            "serverStartId": "963b2505-66c5-4502-8fbb-989257959e25"
        }"""
    mocker.patch("websockets.connect", return_value=mock_ws)

    return mock_ws


def verify_queue_item(item: SKUpdate, update_type: SKUpdateType, payload_type: type) -> None:
    assert item.type == update_type
    assert isinstance(item.payload, payload_type)


@pytest.mark.asyncio
async def test_signalk_hello(mock_rest_api, mock_ws):
    mock_rest_api.get_name.return_value = "My-Vessel"

    queue = asyncio.Queue()
    connector = SKConnector(queue, SKConfig(username="admin", password="admin"))
    connector_task = asyncio.create_task(connector.run())
    await asyncio.sleep(0.05)
    await connector.stop()
    await connector_task

    assert queue.qsize() == 1

    item = await queue.get()
    verify_queue_item(item, SKUpdateType.VESSEL_METADATA, SKVessel)


source_metadata = [
    {
        "talker": "II",
        "sentences": {
            "VHW": "2026-10-20T14:36:56.334Z",
            "VPW": "2026-10-20T14:36:56.355Z",
            "VTG": "2026-10-20T14:36:56.393Z",
            "MWV": "2026-10-20T14:36:56.420Z",
            "VWT": "2026-10-20T14:36:56.453Z",
            "DBT": "2026-10-20T14:36:56.565Z",
            "VDR": "2026-10-20T14:36:56.303Z",
        },
    },
    {
        "n2k": {
            "pgns": {
                "128259": "2026-10-22T09:19:59.953Z",
                "128267": "2026-10-22T09:19:59.128Z",
                "128275": "2026-10-22T09:19:59.595Z",
                "130306": "2026-10-22T09:19:59.890Z",
                "130311": "2026-10-22T09:19:59.014Z",
            },
            "src": "115",
        }
    },
]
observation_metadata = [
    {
        "meta": {
            "description": "Depth below Transducer",
            "units": "m",
            "displayUnits": {
                "category": "depth",
                "targetUnit": "m",
                "formula": "value",
                "inverseFormula": "value",
                "symbol": "m",
                "displayFormat": "0.0",
            },
        },
        "value": 15.63,
        "$source": "nmea0183-sample-data.II",
        "timestamp": "2026-10-20T08:53:45.607Z",
        "sentence": "DBT",
    },
    {
        "meta": {
            "description": "Depth below Transducer",
            "units": "m",
            "displayUnits": {
                "category": "distance",
                "targetUnit": "naut-mile",
                "formula": "value * 0.0005399568034557236",
                "inverseFormula": "value / 0.0005399568034557236",
                "symbol": "nmi",
                "displayFormat": "0.0",
            },
        },
        "value": 31.74,
        "$source": "n2k-sample-data.115",
        "timestamp": "2026-10-22T09:28:20.825Z",
        "pgn": 128267,
    },
]
delta_messages = [
    [
        {
            "context": "vessels.urn:mrn:signalk:uuid:c130874d-4b4d-4188-91a1-71eeda7d7f12",
            "updates": [
                {
                    "source": {
                        "sentence": "DBT",
                        "talker": "II",
                        "type": "NMEA0183",
                        "label": "nmea0183-sample-data",
                    },
                    "$source": "nmea0183-sample-data.II",
                    "timestamp": f"{(datetime.now(UTC) - timedelta(seconds=5)).isoformat()}",
                    "values": [{"path": "environment.depth.belowTransducer", "value": 21.98}],
                }
            ],
        },
        {
            "context": "vessels.urn:mrn:signalk:uuid:c130874d-4b4d-4188-91a1-71eeda7d7f12",
            "updates": [
                {
                    "source": {
                        "sentence": "DBT",
                        "talker": "II",
                        "type": "NMEA0183",
                        "label": "nmea0183-sample-data",
                    },
                    "$source": "nmea0183-sample-data.II",
                    "timestamp": f"{datetime.now(UTC).isoformat()}",
                    "values": [{"path": "environment.depth.belowTransducer", "value": 22.16}],
                }
            ],
        },
    ],
    [
        {
            "context": "vessels.urn:mrn:signalk:uuid:b2be3c6a-a724-4a66-83d8-beb893ad52d0",
            "updates": [
                {
                    "source": {
                        "label": "n2k-sample-data",
                        "type": "NMEA2000",
                        "pgn": 128267,
                        "src": "115",
                    },
                    "$source": "n2k-sample-data.115",
                    "timestamp": f"{(datetime.now(UTC) - timedelta(seconds=5)).isoformat()}",
                    "values": [{"path": "environment.depth.belowTransducer", "value": 13.54}],
                }
            ],
        },
        {
            "context": "vessels.urn:mrn:signalk:uuid:b2be3c6a-a724-4a66-83d8-beb893ad52d0",
            "updates": [
                {
                    "source": {
                        "label": "n2k-sample-data",
                        "type": "NMEA2000",
                        "pgn": 128267,
                        "src": "115",
                    },
                    "$source": "n2k-sample-data.115",
                    "timestamp": f"{datetime.now(UTC).isoformat()}",
                    "values": [{"path": "environment.depth.belowTransducer", "value": 13.63}],
                }
            ],
        },
    ],
]
delta_data = list(zip(source_metadata, observation_metadata, delta_messages))


@pytest.mark.parametrize("src_metadata, obs_metadata, delta_msgs", delta_data)
@pytest.mark.asyncio
async def test_signalk_hello_process_delta(
    mock_rest_api, mock_ws, src_metadata, obs_metadata, delta_msgs
):
    mock_rest_api.get_source_metadata.return_value = src_metadata
    mock_rest_api.get_observation_metadata.return_value = obs_metadata
    messages = [json.dumps(msg) for msg in delta_msgs[:1]]
    mock_ws.__aiter__.return_value = iter(messages)

    queue = asyncio.Queue()
    connector = SKConnector(queue, SKConfig(username="admin", password="admin"))
    connector_task = asyncio.create_task(connector.run())
    await asyncio.sleep(0.05)
    await connector.stop()
    await connector_task

    assert queue.qsize() == 4

    item = await queue.get()
    verify_queue_item(item, SKUpdateType.VESSEL_METADATA, SKVessel)
    item = await queue.get()
    verify_queue_item(item, SKUpdateType.SENSOR_METADATA, SKSensor)
    item = await queue.get()
    verify_queue_item(item, SKUpdateType.OBSERVED_PROPERTY_METADATA, SKObservedProperty)
    item = await queue.get()
    verify_queue_item(item, SKUpdateType.DELTA, SKDelta)


@pytest.mark.parametrize("src_metadata, obs_metadata, delta_msgs", delta_data)
@pytest.mark.asyncio
async def test_signalk_hello_process_deltas(
    mock_rest_api, mock_ws, src_metadata, obs_metadata, delta_msgs
):
    mock_rest_api.get_source_metadata.return_value = src_metadata
    mock_rest_api.get_observation_metadata.return_value = obs_metadata
    messages = [json.dumps(msg) for msg in delta_msgs]
    mock_ws.__aiter__.return_value = iter(messages)

    queue = asyncio.Queue()
    connector = SKConnector(queue, SKConfig(username="admin", password="admin"))
    connector_task = asyncio.create_task(connector.run())
    await asyncio.sleep(0.05)
    await connector.stop()
    await connector_task

    assert queue.qsize() == 5

    item = await queue.get()
    verify_queue_item(item, SKUpdateType.VESSEL_METADATA, SKVessel)
    item = await queue.get()
    verify_queue_item(item, SKUpdateType.SENSOR_METADATA, SKSensor)
    item = await queue.get()
    verify_queue_item(item, SKUpdateType.OBSERVED_PROPERTY_METADATA, SKObservedProperty)
    item = await queue.get()
    verify_queue_item(item, SKUpdateType.DELTA, SKDelta)
    item = await queue.get()
    verify_queue_item(item, SKUpdateType.DELTA, SKDelta)

from unittest.mock import AsyncMock, MagicMock

import pytest

from pipeline.models import (
    SKConfig,
    SKPosition,
)
from pipeline.utils.signalk_rest_api import (
    SignalKRestAPI,
    SignalKRestAPIError,
)


@pytest.fixture
def mock_http_async(mocker):
    client_cls = mocker.patch("httpx2.AsyncClient")
    mock_client = AsyncMock()
    client_cls.return_value = mock_client

    mock_post_response = MagicMock()
    mock_post_response.json.return_value = {"token": "my_token"}

    mock_client.__aenter__.return_value = mock_client
    mock_client.post.return_value = mock_post_response

    return mock_client


@pytest.mark.asyncio
async def test_signalk_rest_api_get_identity(mock_http_async):
    ref_identity = "vessels.urn:mrn:signalk:uuid:5b522cab-78bf-484a-a24a-12e1d63906d1"
    mock_get_response = MagicMock()
    mock_get_response.json.return_value = ref_identity
    mock_http_async.get.return_value = mock_get_response

    api = SignalKRestAPI(SKConfig(username="admin", password="admin"))
    identity = await api.get_identity()

    assert identity == ref_identity


@pytest.mark.asyncio
async def test_signalk_rest_api_get_identity_invalid(mock_http_async):
    with pytest.raises(SignalKRestAPIError):
        mock_get_response = MagicMock()
        mock_get_response.json.return_value = {"id": "self"}
        mock_http_async.get.return_value = mock_get_response

        api = SignalKRestAPI(SKConfig(username="admin", password="admin"))
        await api.get_identity()


position_data = [
    {
        "meta": {
            "description": "The position of the vessel in 2 or 3 dimensions (WGS84 datum)",
            "properties": {
                "longitude": {
                    "type": "number",
                    "description": "Longitude",
                    "units": "deg",
                    "example": 4.98765245,
                },
                "latitude": {
                    "type": "number",
                    "description": "Latitude",
                    "units": "deg",
                    "example": 52.0987654,
                },
                "altitude": {"type": "number", "description": "Altitude", "units": "m"},
            },
        },
        "value": {"latitude": 59.7138918, "longitude": 24.7299066},
        "$source": "nmea0183-sample-data.GP",
        "timestamp": "2026-10-22T08:42:39.669Z",
        "sentence": "GLL",
    },
    {
        "meta": {
            "description": "The position of the vessel in 2 or 3 dimensions (WGS84 datum)",
            "properties": {
                "longitude": {
                    "type": "number",
                    "description": "Longitude",
                    "units": "deg",
                    "example": 4.98765245,
                },
                "latitude": {
                    "type": "number",
                    "description": "Latitude",
                    "units": "deg",
                    "example": 52.0987654,
                },
                "altitude": {"type": "number", "description": "Altitude", "units": "m"},
            },
        },
        "value": {"longitude": 24.7299066, "latitude": 59.7138918},
        "$source": "n2k-sample-data.160",
        "timestamp": "2014-08-15T19:06:18.580Z",
        "pgn": 129025,
    },
]


@pytest.mark.parametrize("pos_data", position_data)
@pytest.mark.asyncio
async def test_signalk_rest_api_get_position(mock_http_async, pos_data):
    mock_get_response = MagicMock()
    mock_get_response.json.return_value = pos_data
    mock_http_async.get.return_value = mock_get_response

    api = SignalKRestAPI(SKConfig(username="admin", password="admin"))
    position = await api.get_position()

    assert position == SKPosition(
        latitude=59.7138918,
        longitude=24.7299066,
        description="The position of the vessel in 2 or 3 dimensions (WGS84 datum)",
        source=pos_data["$source"],
    )


@pytest.mark.asyncio
async def test_signalk_rest_api_get_position_invalid(mock_http_async):
    with pytest.raises(SignalKRestAPIError):
        mock_get_response = MagicMock()
        mock_get_response.json.return_value = {}
        mock_http_async.get.return_value = mock_get_response

        api = SignalKRestAPI(SKConfig(username="admin", password="admin"))
        await api.get_position()


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


@pytest.mark.parametrize("src_metadata", source_metadata)
@pytest.mark.asyncio
async def test_signalk_rest_api_get_source_metadata(mock_http_async, src_metadata):
    mock_get_response = MagicMock()
    mock_get_response.json.return_value = src_metadata
    mock_http_async.get.return_value = mock_get_response

    api = SignalKRestAPI(SKConfig(username="admin", password="admin"))
    await api.get_source_metadata(label="label", source="source")


@pytest.mark.asyncio
async def test_signalk_rest_api_get_source_metadata_invalid(mock_http_async):
    with pytest.raises(SignalKRestAPIError):
        mock_get_response = MagicMock()
        mock_get_response.json.return_value = ""
        mock_http_async.get.return_value = mock_get_response

        api = SignalKRestAPI(SKConfig(username="admin", password="admin"))
        await api.get_source_metadata(label="nmea0183-sample-data", source="GP")


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


@pytest.mark.parametrize("obs_metadata", observation_metadata)
@pytest.mark.asyncio
async def test_signalk_rest_api_get_observation_metadata(mock_http_async, obs_metadata):
    mock_get_response = MagicMock()
    mock_get_response.json.return_value = obs_metadata
    mock_http_async.get.return_value = mock_get_response

    api = SignalKRestAPI(SKConfig(username="admin", password="admin"))
    await api.get_observation_metadata(observation="environment/depth/belowTransducer")


@pytest.mark.asyncio
async def test_signalk_rest_api_get_observation_metadata_invalid(mock_http_async):
    with pytest.raises(SignalKRestAPIError):
        mock_get_response = MagicMock()
        mock_get_response.json.return_value = {}
        mock_http_async.get.return_value = mock_get_response

        api = SignalKRestAPI(SKConfig(username="admin", password="admin"))
        await api.get_observation_metadata(observation="environment/depth/belowTransducer")

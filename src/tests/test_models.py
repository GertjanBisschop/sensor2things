import json
import logging
from datetime import UTC, datetime, timedelta

import pytest

from pipeline.models import (
    Config,
    LogLevel,
    SKConfig,
    SKDelta,
    SKDeltaSourceN0183,
    SKDeltaSourceN2000,
    SKObservedProperty,
    SKObservedPropertyMetaMultiple,
    SKObservedPropertyMetaSingle,
    SKSensor,
    SKSensorMetaN0183,
    SKSensorMetaN2000,
    STConfig,
)

logger = logging.getLogger(__name__)


def test_config_from_json(json_config_file):
    config = Config.from_path(json_config_file)
    assert config.log_level == LogLevel.debug
    assert len(config.source) != 0
    assert len(config.sink) != 0
    assert len(config.convertor) != 0


def test_config_from_yaml(yaml_config_file):
    config = Config.from_path(yaml_config_file)
    assert config.log_level == LogLevel.debug
    assert len(config.source) != 0
    assert len(config.sink) != 0
    assert len(config.convertor) != 0


@pytest.mark.parametrize("fixture_name", ["json_config_file", "yaml_config_file"])
def test_signalk_source_config(fixture_name: str, request):
    config_file = request.getfixturevalue(fixture_name)
    config = Config.from_path(config_file)
    source_dict = config.source["signalk"]
    signalk_config = SKConfig(**source_dict)

    assert signalk_config.host == "signalk"
    assert signalk_config.port == 3001
    assert signalk_config.username == "admin"
    assert signalk_config.password == "password"
    assert signalk_config.server_name == "signalk"
    assert signalk_config.max_msg_age_minutes == 10
    assert signalk_config.subscriptions.context == "vessels.self"
    assert len(signalk_config.subscriptions.subscriptions) == 2

    assert (
        signalk_config.base_url
        == f"{signalk_config.protocol_rest}://{signalk_config.host}:{signalk_config.port}/{signalk_config.server_name}/v1"
    )
    assert (
        signalk_config.ws_base_url
        == f"{signalk_config.protocol_ws}://{signalk_config.host}:{signalk_config.port}/{signalk_config.server_name}/v1"
    )

    for sub in signalk_config.subscriptions.subscriptions:
        assert sub.format == "delta"
        assert sub.path in ["environment.*", "navigation.position"]
        assert sub.period == 10000
        assert sub.policy == "fixed"


@pytest.mark.parametrize("fixture_name", ["json_config_file", "yaml_config_file"])
def test_sensorthings_sink_config(fixture_name: str, request):
    config_file = request.getfixturevalue(fixture_name)
    config = Config.from_path(config_file)
    sink_dict = config.sink["sensorthings"]
    sensorthings_config = STConfig(**sink_dict)

    assert sensorthings_config.host == "frost"
    assert sensorthings_config.port == 8081
    assert sensorthings_config.username is None
    assert sensorthings_config.password is None
    assert (
        sensorthings_config.base_url
        == f"{sensorthings_config.protocol}://{sensorthings_config.host}:{sensorthings_config.port}/FROST-Server/v1.1"
    )


delta_messages = [
    (
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
        SKDeltaSourceN0183,
    ),
    (
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
        SKDeltaSourceN2000,
    ),
]


@pytest.mark.parametrize("delta_message, delta_source_type", delta_messages)
def test_signalk_delta_parsing(delta_message, delta_source_type):
    delta_nmea_0183 = json.dumps(delta_message)
    delta = SKDelta.model_validate_json(delta_nmea_0183)
    assert len(delta.updates) == 1
    assert isinstance(delta.updates[0].source, delta_source_type)


def test_signalk_sensor_meta_n0183():
    context = "vessels.urn:mrn:signalk:uuid:4810bf6b-06d7-4afa-afdf-8b578ecad7da"
    name = "nmea0183-sample-data.II"
    metadata = {
        "talker": "II",
        "sentences": {
            "VHW": "2026-10-26T14:43:11.621Z",
            "VPW": "2026-10-26T14:43:11.641Z",
            "VTG": "2026-10-26T14:43:11.680Z",
            "MWV": "2026-10-26T14:43:11.707Z",
            "VWT": "2026-10-26T14:43:11.332Z",
            "DBT": "2026-10-26T14:43:11.443Z",
            "VDR": "2026-10-26T14:43:11.589Z",
        },
    }
    sensor = SKSensor(context=context, name=name, meta=metadata)
    assert isinstance(sensor.meta, SKSensorMetaN0183)


def test_signalk_sensor_meta_n2000():
    context = "vessels.urn:mrn:signalk:uuid:4810bf6b-06d7-4afa-afdf-8b578ecad7da"
    name = "nmea0183-sample-data.II"
    metadata = {
        "n2k": {
            "pgns": {
                "128259": "2026-10-26T10:00:01.432Z",
                "128267": "2026-10-26T10:00:00.934Z",
                "128275": "2026-10-26T10:00:01.452Z",
                "130306": "2026-10-26T10:00:00.793Z",
                "130311": "2026-10-26T10:00:01.457Z",
            },
            "src": "115",
        }
    }
    sensor = SKSensor(context=context, name=name, meta=metadata)
    assert isinstance(sensor.meta, dict)
    assert all(isinstance(value, SKSensorMetaN2000) for value in sensor.meta.values())


def test_signalk_observed_property_meta_single():
    context = "vessels.urn:mrn:signalk:uuid:4810bf6b-06d7-4afa-afdf-8b578ecad7da"
    source = "nmea0183-sample-data.II"
    path = "environment.depth.belowTransducer"
    meta = {
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
    }
    observed_property = SKObservedProperty(context=context, source=source, path=path, meta=meta)
    assert isinstance(observed_property.meta, SKObservedPropertyMetaSingle)


def test_signalk_observed_property_meta_multiple():
    context = "vessels.urn:mrn:signalk:uuid:4810bf6b-06d7-4afa-afdf-8b578ecad7da"
    source = "nmea0183-sample-data.II"
    path = "environment.current"
    meta = {
        "description": "Direction and strength of current affecting the vessel",
        "properties": {
            "drift": {
                "type": "number",
                "description": "The speed component of the water current vector",
                "example": 3.12,
                "units": "m/s",
            },
            "setTrue": {
                "type": "number",
                "description": "The direction component of the water current vector referenced to true (geographic) north",
                "example": 2.155,
                "units": "rad",
            },
            "setMagnetic": {
                "type": "number",
                "description": "The direction component of the water current vector referenced to magnetic north",
                "example": 2.29,
                "units": "rad",
            },
        },
    }
    observed_property = SKObservedProperty(context=context, source=source, path=path, meta=meta)
    assert isinstance(observed_property.meta, SKObservedPropertyMetaMultiple)

"""Testing connectivity and health checks for SignalK and SensorThings connectors."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from pipeline.models import SKConfig, STConfig
from pipeline.sink_connectors import STConnector
from pipeline.source_connectors import SKConnector


@pytest.mark.asyncio
async def test_signalk_connector_health_check(mocker):
    rest_api_cls = mocker.patch("pipeline.source_connectors.signalk.SignalKRestAPI")
    mock_rest_api = AsyncMock()
    rest_api_cls.return_value = mock_rest_api

    queue = asyncio.Queue()
    connector = SKConnector(queue=queue, config=SKConfig(username="admin", password="password"))
    is_healthy = await connector.health_check()
    assert is_healthy is True


@pytest.mark.asyncio
async def test_sensorthings_connector_health_check(mocker):
    service_cls = mocker.patch("pipeline.sink_connectors.sensorthings.SensorThingsService")
    mock_service = AsyncMock()
    service_cls.return_value = mock_service

    queue = asyncio.Queue()
    sink = STConnector(queue=queue, config=STConfig())
    is_healthy = await sink.health_check()
    assert is_healthy is True

"""Integration tests for SignalK to SensorThings pipeline using Testcontainers."""

import asyncio
import contextlib
import json
import os
import shutil
import time
from pathlib import Path
from typing import Any

import httpx2
import pytest
import requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

from pipeline.models import Config, STConfig
from pipeline.orchestrator import Orchestrator
from pipeline.utils.sensorthings_service import SensorThingsService


def _create_frost_stack(request=None) -> dict[str, Any]:
    """Spin up PostGIS database and FROST-Server containers connected on a shared network."""
    network = Network()
    network.create()

    db = (
        DockerContainer("postgis/postgis:18-3.6-alpine")
        .with_network(network)
        .with_network_aliases("database")
        .with_env("POSTGRES_DB", "sensorthings")
        .with_env("POSTGRES_USER", "sensorthings")
        .with_env("POSTGRES_PASSWORD", "ChangeMe")
        .with_exposed_ports(5432)
    )
    db.start()
    time.sleep(2)

    frost = (
        DockerContainer("fraunhoferiosb/frost-server:latest")
        .with_network(network)
        .with_env("serviceRootUrl", "http://localhost:8080/FROST-Server")
        .with_env("plugins_modelLoader_enable", "true")
        .with_env("plugins_multiDatastream_enable", "false")
        .with_env("plugins_actuation_enable", "false")
        .with_env("http_cors_enable", "true")
        .with_env("http_cors_allowed_origins", "*")
        .with_env("persistence_db_driver", "org.postgresql.Driver")
        .with_env("persistence_db_url", "jdbc:postgresql://database:5432/sensorthings")
        .with_env("persistence_db_username", "sensorthings")
        .with_env("persistence_db_password", "ChangeMe")
        .with_env("persistence_autoUpdateDatabase", "true")
        .with_env("persistence_countMode", "LIMIT_ESTIMATE")
        .with_exposed_ports(8080)
    )
    frost.start()
    frost_port = int(frost.get_exposed_port(8080))
    frost_host = frost.get_container_host_ip()

    # Health check for FROST server
    frost_url = f"http://{frost_host}:{frost_port}/FROST-Server/v1.1"
    ready = False
    for _ in range(45):
        with contextlib.suppress(requests.RequestException):
            resp = requests.get(f"{frost_url}/Things", timeout=2)
            if resp.status_code == 200:
                ready = True
                break
        time.sleep(1)

    if not ready:
        frost.stop()
        db.stop()
        network.remove()
        pytest.fail("FROST-Server failed to become healthy within timeout.")

    def cleanup():
        with contextlib.suppress(Exception):
            frost.stop()
        with contextlib.suppress(Exception):
            db.stop()
        with contextlib.suppress(Exception):
            network.remove()

    if request:
        request.addfinalizer(cleanup)

    return {
        "host": frost_host,
        "port": frost_port,
        "base_url": frost_url,
    }


@pytest.fixture(scope="module")
def frost_stack(request) -> dict[str, Any]:
    """Spin up PostGIS database and FROST-Server containers connected on a shared network."""
    return _create_frost_stack(request)


@pytest.fixture(scope="module")
def frost_sink_stack(request) -> dict[str, Any]:
    """Spin up a second PostGIS database and FROST-Server container stack for sync testing."""
    return _create_frost_stack(request)


@pytest.fixture(scope="module")
def signalk_data_dir(request) -> Path:
    tmp_data_dir = Path("data/.tmp")
    signalk_data_dir = tmp_data_dir / "signalk"
    signalk_data_dir.mkdir(exist_ok=True, parents=True)
    signalk_data_dir.chmod(0o1777)

    signalk_security_context = json.dumps(
        {
            "allow_readonly": False,
            "expiration": "NEVER",
            "secretKey": "fa1451eaf5f197f2fc5535bbd5fac4711a30e99ee3b1d0584e28efc8250968e6b43577a39a0a434937129fc50ab2ebd92438de9b98698f7c6e2ea4577b8f5ea68e47acda95479dbb6739434dc1dc76ca3ec81f312a375097d15af7a14e3977c4553c66e402db8d999477a96f24f54548989c628a6dc886ee971ae4be3864b31ff4d075f121dc1719077fd3c1ca56927d1cec0fd6d18efcb2edd8cbc774c314b643723c7cd7ca94d59daecd55efd3d17b1960299c7b0fe91adef21db9bacb69d8c181437c456c185b7da8662009121ead7857ec9958c45272c2cff426af87afe376a2082d82fe49a8b7615f5b2d1458834b3ef66e4946b6667753df7f208ef712",
            "users": [
                {
                    "username": "admin",
                    "type": "admin",
                    "password": "$2a$10$jzWyOvj9QvTzzo0owQP8x.cbQ8DF0lhe12/w297wVXyZRy0ZxygBu",
                },
                {
                    "username": "readonly",
                    "type": "readonly",
                    "password": "$2a$10$VG8iSm9yCGgWs0Knqm7WdOXLofYRJ9KDKNo1sKyjsAW2/Tj6xeElm",
                },
            ],
            "devices": [],
            "immutableConfig": False,
            "acls": [],
            "allowDeviceAccessRequests": True,
            "allowNewUserRegistration": True,
        },
        indent=2,
    )
    signalk_security_file = signalk_data_dir / "security.json"
    with open(signalk_security_file, "w") as f:
        f.write(signalk_security_context)
    signalk_security_file.chmod(0o666)

    def cleanup():
        shutil.rmtree(tmp_data_dir)

    request.addfinalizer(cleanup)

    return signalk_data_dir


@pytest.fixture(scope="module")
def signalk_container(request, signalk_data_dir: Path) -> dict[str, Any]:
    """Spin up SignalK server container with sample NMEA0183 data generator and preconfigured credentials."""
    data_dir = signalk_data_dir.resolve()
    uid = os.getuid()
    gid = os.getgid()
    signalk = (
        DockerContainer("signalk/signalk-server:latest")
        .with_volume_mapping(data_dir, "/home/node/.signalk", mode="rw")
        .with_env("HOME", "/home/node")
        .with_command("--override-timestamps --sample-nmea0183-data")
        .with_exposed_ports(3000)
        .with_kwargs(user=f"{uid}:{gid}", group_add=["1000"])
    )
    signalk.start()
    sk_port = int(signalk.get_exposed_port(3000))
    sk_host = signalk.get_container_host_ip()

    # Health check for SignalK server
    sk_base_url = f"http://{sk_host}:{sk_port}/signalk/v1"
    ready = False
    token = None
    for _ in range(30):
        with contextlib.suppress(requests.RequestException):
            resp = requests.post(
                f"{sk_base_url}/auth/login",
                json={"username": "readonly", "password": "signalk_ro"},
                timeout=2,
            )
            if resp.status_code == 200:
                token = resp.json().get("token")
                ready = True
                break
        time.sleep(1)

    if not ready:
        signalk.stop()
        pytest.fail("SignalK server failed to authenticate and become healthy within timeout.")

    def cleanup():
        with contextlib.suppress(Exception):
            signalk.stop()

    request.addfinalizer(cleanup)

    return {
        "host": sk_host,
        "port": sk_port,
        "base_url": sk_base_url,
        "token": token,
        "username": "readonly",
        "password": "signalk_ro",
    }


@pytest.fixture
def pipeline_config(signalk_container: dict[str, Any], frost_stack: dict[str, Any]) -> Config:
    """Generate pipeline Config referencing the active Testcontainer instances."""
    return Config(
        source={
            "signalk": {
                "host": signalk_container["host"],
                "port": signalk_container["port"],
                "username": signalk_container["username"],
                "password": signalk_container["password"],
            }
        },
        convertor={"signalk_sensorthings": {}},
        sink={
            "sensorthings": {
                "host": frost_stack["host"],
                "port": frost_stack["port"],
            }
        },
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sk2st1_container_setup_and_fixtures(
    signalk_container: dict[str, Any],
    frost_stack: dict[str, Any],
):
    """Step 1: Testcontainer Setup & Fixtures - Verify container health and connectivity."""
    # Verify SignalK server endpoint
    async with httpx2.AsyncClient() as client:
        sk_resp = await client.get(
            f"{signalk_container['base_url']}/api/self",
            headers={"Authorization": f"Bearer {signalk_container['token']}"},
            timeout=5,
        )
        assert sk_resp.status_code == 200
        assert "vessels." in sk_resp.text

        # Verify FROST server endpoint
        frost_resp = await client.get(f"{frost_stack['base_url']}/Things", timeout=5)
        assert frost_resp.status_code == 200
        assert "value" in frost_resp.json()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sk2st2_basic_connectivity_and_vessel_thing_ingestion(
    pipeline_config: Config,
    frost_stack: dict[str, Any],
):
    """Step 2: Basic Connectivity & Vessel/Thing Test - Ingest vessel data into FROST server."""
    orchestrator = Orchestrator(pipeline_config)
    task = asyncio.create_task(orchestrator.run())

    # Allow the pipeline to fetch vessel metadata and post initial Thing and Location
    await asyncio.sleep(4)
    await orchestrator.stop()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    service = SensorThingsService(STConfig(host=frost_stack["host"], port=frost_stack["port"]))
    things = await service.get_things()
    assert len(things) >= 1

    thing = things[0]
    assert thing.name is not None
    assert "vessels." in thing.name

    # Verify Thing was also created in REST API with Locations
    async with httpx2.AsyncClient() as client:
        resp = await client.get(f"{frost_stack['base_url']}/Things?$expand=Locations")
        assert resp.status_code == 200
        data = resp.json().get("value", [])
        assert len(data) >= 1
        assert len(data[0].get("Locations", [])) >= 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sk2st3_sensor_data_flow_integration(
    pipeline_config: Config,
    frost_stack: dict[str, Any],
):
    """Step 3: Sensor Data Flow Integration Test - Stream sensor metrics and verify ST entities."""
    orchestrator = Orchestrator(pipeline_config)
    task = asyncio.create_task(orchestrator.run())

    # Stream delta messages for 8 seconds
    await asyncio.sleep(8)
    await orchestrator.stop()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    async with httpx2.AsyncClient() as client:
        # Verify Sensors were created
        sensors_resp = await client.get(f"{frost_stack['base_url']}/Sensors")
        assert sensors_resp.status_code == 200
        sensors = sensors_resp.json().get("value", [])
        assert len(sensors) > 0

        # Verify ObservedProperties were created
        obs_prop_resp = await client.get(f"{frost_stack['base_url']}/ObservedProperties")
        assert obs_prop_resp.status_code == 200
        obs_props = obs_prop_resp.json().get("value", [])
        assert len(obs_props) > 0

        # Verify Datastreams were created
        ds_resp = await client.get(f"{frost_stack['base_url']}/Datastreams")
        assert ds_resp.status_code == 200
        datastreams = ds_resp.json().get("value", [])
        assert len(datastreams) > 0

        # Verify Observations were recorded
        obs_resp = await client.get(f"{frost_stack['base_url']}/Observations")
        assert obs_resp.status_code == 200
        observations = obs_resp.json().get("value", [])
        assert len(observations) > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sk2st4_full_end_to_end_workflow(
    pipeline_config: Config,
    frost_stack: dict[str, Any],
):
    """Step 4: Full End-to-End Workflow & Verification - Comprehensive scenario test."""
    orchestrator = Orchestrator(pipeline_config)
    task = asyncio.create_task(orchestrator.run())

    # Run complete streaming pipeline
    await asyncio.sleep(10)
    await orchestrator.stop()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    async with httpx2.AsyncClient() as client:
        # Check complete entity hierarchy: Thing -> Datastreams -> Observations, Sensor, ObservedProperty
        resp = await client.get(
            f"{frost_stack['base_url']}/Things?$expand=Locations,Datastreams($expand=Sensor,ObservedProperty,Observations)"
        )
        assert resp.status_code == 200
        things = resp.json().get("value", [])
        assert len(things) >= 1

        vessel_thing = things[0]
        assert "vessels." in vessel_thing["name"]
        assert len(vessel_thing["Locations"]) >= 1

        datastreams = vessel_thing.get("Datastreams", [])
        assert len(datastreams) > 0

        total_observations = 0
        for ds in datastreams:
            assert ds.get("Sensor") is not None
            assert ds.get("ObservedProperty") is not None
            observations = ds.get("Observations", [])
            total_observations += len(observations)

        assert total_observations > 0


@pytest.mark.asyncio
async def test_st2st1_orchestrator_pipeline_flow(mocker):
    """Verify that Orchestrator wires SensorThings source -> convertor -> sink."""
    from unittest.mock import AsyncMock

    from frost_sta_client import Thing as FrostThing

    source_service_mock = AsyncMock()
    sink_service_mock = AsyncMock()

    st_thing = FrostThing(
        id="1",
        name="thing_1",
        description="Vessel 1",
        properties={"sk_name": "Research Vessel"},
    )
    source_service_mock.get_things.return_value = [st_thing]
    source_service_mock.get_locations.return_value = []
    source_service_mock.get_historical_locations.return_value = []
    source_service_mock.get_sensors.return_value = []
    source_service_mock.get_observed_properties.return_value = []
    source_service_mock.get_datastreams.return_value = []
    source_service_mock.get_features_of_interest.return_value = []
    source_service_mock.get_observations.return_value = []

    mocker.patch(
        "pipeline.source_connectors.sensorthings.SensorThingsService",
        return_value=source_service_mock,
    )
    mocker.patch(
        "pipeline.sink_connectors.sensorthings.SensorThingsService",
        return_value=sink_service_mock,
    )

    config = Config(
        source={"sensorthings": STConfig(host="source-frost", port=8080).model_dump()},
        convertor={"sensorthings_sensorthings": {}},
        sink={"sensorthings": STConfig(host="sink-frost", port=8080).model_dump()},
    )

    orchestrator = Orchestrator(config)
    task = asyncio.create_task(orchestrator.run())

    await asyncio.sleep(0.1)
    await orchestrator.stop()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    sink_service_mock.upsert_thing.assert_called_once()
    assert sink_service_mock.upsert_thing.call_args[0][0].name == "thing_1"
    assert sink_service_mock.upsert_thing.call_args[0][0].properties["sk_name"] == "Research Vessel"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_st2st2_end_to_end(
    frost_stack: dict[str, Any],
    frost_sink_stack: dict[str, Any],
):
    """Step 5: SensorThings-to-SensorThings E2E - Sync entities between SensorThings instances."""
    source_service = SensorThingsService(
        STConfig(host=frost_stack["host"], port=frost_stack["port"])
    )
    sink_service = SensorThingsService(
        STConfig(host=frost_sink_stack["host"], port=frost_sink_stack["port"])
    )

    st_things = await source_service.get_things()
    assert len(st_things) > 0
    st_datastreams = await source_service.get_datastreams()
    assert len(st_datastreams) > 0
    st_sensors = await source_service.get_sensors()
    assert len(st_sensors) > 0
    st_observed_properties = await source_service.get_observed_properties()
    assert len(st_observed_properties) > 0
    st_locations = await source_service.get_locations()
    assert len(st_locations) > 0

    config = Config(
        source={
            "sensorthings": STConfig(
                host=frost_stack["host"], port=frost_stack["port"]
            ).model_dump()
        },
        convertor={"sensorthings_sensorthings": {}},
        sink={
            "sensorthings": STConfig(
                host=frost_sink_stack["host"], port=frost_sink_stack["port"]
            ).model_dump()
        },
    )

    orchestrator = Orchestrator(config)
    task = asyncio.create_task(orchestrator.run())

    await asyncio.sleep(4)
    await orchestrator.stop()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    sink_things = await sink_service.get_things()
    assert len(sink_things) >= len(st_things)
    sink_datastreams = await sink_service.get_datastreams()
    assert len(sink_datastreams) >= len(st_datastreams)
    sink_sensors = await sink_service.get_sensors()
    assert len(sink_sensors) >= len(st_sensors)
    sink_observed_properties = await sink_service.get_observed_properties()
    assert len(sink_observed_properties) >= len(st_observed_properties)
    sink_locations = await sink_service.get_locations()
    assert len(sink_locations) >= len(st_locations)

import json
from pathlib import Path

import pytest
import yaml

CONFIG_DICT = {
    "log_level": "debug",
    "source": {
        "signalk": {"host": "signalk", "port": 3001, "username": "admin", "password": "password"}
    },
    "sink": {"sensorthings": {"host": "frost", "port": 8081}},
    "convertor": {"signalk_sensorthings": {"drop_old_messages": False}},
}


@pytest.fixture
def json_config_file(tmp_path) -> Path:
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()
    config_json = json.dumps(CONFIG_DICT)
    config_file = test_dir / "config.json"
    config_file.write_text(config_json)

    return config_file


@pytest.fixture
def yaml_config_file(tmp_path) -> Path:
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()
    config_file = test_dir / "config.yaml"
    with config_file.open("w") as cfh:
        yaml.dump(CONFIG_DICT, cfh)

    return config_file


# function executed right after test items collected but before test run
def pytest_collection_modifyitems(config, items):
    if not config.getoption("-m"):
        skip_me = pytest.mark.skip(reason="use `-m integration` to run this test")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_me)

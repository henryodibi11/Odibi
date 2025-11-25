import os
import tempfile

import pytest

from odibi.utils import load_yaml_with_env


def test_load_yaml_with_env_success(monkeypatch):
    content = """
    key: ${TEST_VAR}
    other: ${env:OTHER_VAR}
    mixed: prefix_${TEST_VAR}_suffix
    number: ${NUMBER_VAR}
    """

    monkeypatch.setenv("TEST_VAR", "value1")
    monkeypatch.setenv("OTHER_VAR", "value2")
    monkeypatch.setenv("NUMBER_VAR", "123")

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".yaml") as f:
        f.write(content)
        path = f.name

    try:
        config = load_yaml_with_env(path)
        assert config["key"] == "value1"
        assert config["other"] == "value2"
        assert config["mixed"] == "prefix_value1_suffix"
        assert config["number"] == 123
    finally:
        os.remove(path)


def test_load_yaml_with_env_missing_var(monkeypatch):
    content = "key: ${MISSING_VAR}"

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".yaml") as f:
        f.write(content)
        path = f.name

    try:
        monkeypatch.delenv("MISSING_VAR", raising=False)
        with pytest.raises(ValueError, match="Missing environment variable: MISSING_VAR"):
            load_yaml_with_env(path)
    finally:
        os.remove(path)


def test_load_yaml_with_env_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_yaml_with_env("non_existent_file.yaml")

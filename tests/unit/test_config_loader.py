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


def test_pattern_block_normalized_to_transformer():
    """Test that pattern: block is normalized to transformer: + params:."""
    content = """
    pipelines:
      - pipeline: test
        nodes:
          - name: dim_customer
            pattern:
              type: dimension
              params:
                natural_key: customer_id
                surrogate_key: customer_sk
    """

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".yaml") as f:
        f.write(content)
        path = f.name

    try:
        config = load_yaml_with_env(path)
        node = config["pipelines"][0]["nodes"][0]
        assert "pattern" not in node, "pattern block should be removed"
        assert node["transformer"] == "dimension"
        assert node["params"]["natural_key"] == "customer_id"
        assert node["params"]["surrogate_key"] == "customer_sk"
    finally:
        os.remove(path)


def test_pattern_block_preserves_existing_params():
    """Test that pattern.params merges with existing params."""
    content = """
    pipelines:
      - pipeline: test
        nodes:
          - name: test_node
            params:
              existing_key: existing_value
            pattern:
              type: fact
              params:
                grain:
                  - order_id
    """

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".yaml") as f:
        f.write(content)
        path = f.name

    try:
        config = load_yaml_with_env(path)
        node = config["pipelines"][0]["nodes"][0]
        assert node["transformer"] == "fact"
        assert node["params"]["existing_key"] == "existing_value"
        assert node["params"]["grain"] == ["order_id"]
    finally:
        os.remove(path)


class TestEnvironmentsOverride:
    """Tests for environments override functionality."""

    def test_environments_override_connections(self):
        """Test that environments block overrides connections."""
        content = """
        connections:
          data_lake:
            type: local
            base_path: ./dev_data

        environments:
          prod:
            connections:
              data_lake:
                base_path: /prod/data
        """

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".yaml") as f:
            f.write(content)
            path = f.name

        try:
            # Without env - should use dev path
            config = load_yaml_with_env(path)
            assert config["connections"]["data_lake"]["base_path"] == "./dev_data"

            # With prod env - should override
            config = load_yaml_with_env(path, env="prod")
            assert config["connections"]["data_lake"]["base_path"] == "/prod/data"
        finally:
            os.remove(path)

    def test_environments_override_system(self):
        """Test that environments block can override system config."""
        content = """
        system:
          connection: local_dev
          environment: dev

        environments:
          prod:
            system:
              connection: sql_server
              environment: prod
        """

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".yaml") as f:
            f.write(content)
            path = f.name

        try:
            config = load_yaml_with_env(path, env="prod")
            assert config["system"]["connection"] == "sql_server"
            assert config["system"]["environment"] == "prod"
        finally:
            os.remove(path)

    def test_environments_nonexistent_env_uses_base(self):
        """Test that non-existent env uses base config."""
        content = """
        connections:
          data_lake:
            type: local
            base_path: ./dev_data

        environments:
          prod:
            connections:
              data_lake:
                base_path: /prod/data
        """

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".yaml") as f:
            f.write(content)
            path = f.name

        try:
            # qat env doesn't exist, should use base
            config = load_yaml_with_env(path, env="qat")
            assert config["connections"]["data_lake"]["base_path"] == "./dev_data"
        finally:
            os.remove(path)

    def test_environments_external_file_override(self):
        """Test that env.{env}.yaml file is applied."""

        with tempfile.TemporaryDirectory() as tmpdir:
            # Main config
            main_content = """
            connections:
              data_lake:
                type: local
                base_path: ./dev_data
            """

            # env.prod.yaml override
            env_content = """
            connections:
              data_lake:
                base_path: /prod/data
            """

            main_path = os.path.join(tmpdir, "config.yaml")
            env_path = os.path.join(tmpdir, "env.prod.yaml")

            with open(main_path, "w") as f:
                f.write(main_content)
            with open(env_path, "w") as f:
                f.write(env_content)

            config = load_yaml_with_env(main_path, env="prod")
            assert config["connections"]["data_lake"]["base_path"] == "/prod/data"

"""Remote session wiring, tested offline without Databricks credentials."""

import sys
import traceback
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pydantic import TypeAdapter, ValidationError

from odibi.config import ConnectionConfig, DatabricksConnectionConfig
from odibi.connections.databricks import DatabricksConnection
from odibi.engine.spark_engine import SparkEngine
from odibi.pipeline import PipelineManager
from odibi.utils.config_loader import load_yaml_with_env


def settings(**overrides):
    return (
        dict(
            host="https://workspace.example.com",
            client_id="test-principal",
            client_secret="test-oauth-secret",
            cluster_id="0123-test",
        )
        | overrides
    )


@pytest.fixture
def sdk(monkeypatch):
    """Only replace the external boundary, not Odibi session selection."""
    connect = ModuleType("databricks.connect")
    core = ModuleType("databricks.sdk.core")

    def sdk_config(**kw):
        # Like the SDK, fill omitted values from ambient environment/profile.
        kw["cluster_id"] = kw.get("cluster_id") or "ambient-cluster"
        kw["serverless_compute_id"] = kw.get("serverless_compute_id") or "auto"
        config = SimpleNamespace(**kw)
        config.as_dict = lambda: config.__dict__
        return config

    core.Config = sdk_config
    connect.DatabricksSession = MagicMock()
    builder = connect.DatabricksSession.builder
    session = MagicMock(name="remote_session")
    builder.sdkConfig.return_value.getOrCreate.return_value = session
    monkeypatch.setitem(sys.modules, "databricks.connect", connect)
    monkeypatch.setitem(sys.modules, "databricks.sdk.core", core)
    sql = ModuleType("pyspark.sql")
    sql.SparkSession = MagicMock()
    sql.SparkSession.getActiveSession.return_value = MagicMock(name="unrelated_active")
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    return builder, session


@pytest.mark.parametrize("serverless", [False, True])
def test_yaml_factory_engine_remote_session(tmp_path, monkeypatch, sdk, serverless):
    monkeypatch.setenv("TEST_DB_SECRET", "test-oauth-secret")
    target = "serverless: true" if serverless else "cluster_id: 0123-test"
    path = tmp_path / "connections.yaml"
    path.write_text(
        "remote:\n  type: databricks\n  host: https://workspace.example.com\n"
        "  client_id: test-principal\n  client_secret: ${TEST_DB_SECRET}\n  " + target + "\n"
    )
    raw = load_yaml_with_env(path)
    config = TypeAdapter(ConnectionConfig).validate_python(raw["remote"])
    assert isinstance(config, DatabricksConnectionConfig)
    assert "test-oauth-secret" not in repr(config)
    connections = PipelineManager._build_connections({"remote": config})
    connection = connections["remote"]
    builder, session = sdk
    assert connection._session is None
    connection.validate()
    engine = SparkEngine(connections=connections)
    assert engine.spark is session
    # A second engine (system catalog vs pipeline) uses the same remote session.
    assert SparkEngine(connections=connections).spark is session
    assert builder.sdkConfig.call_count == 1
    sdk_config = builder.sdkConfig.call_args.args[0]
    assert sdk_config.host == "https://workspace.example.com"
    assert sdk_config.auth_type == "oauth-m2m"
    assert sdk_config.client_id == "test-principal"
    assert sdk_config.client_secret == "test-oauth-secret"
    assert getattr(sdk_config, "cluster_id", None) == (None if serverless else "0123-test")
    assert getattr(sdk_config, "serverless_compute_id", None) == ("auto" if serverless else None)
    with pytest.raises(ValueError, match="not data paths"):
        connection.get_path("table")


@pytest.mark.parametrize(
    "overrides",
    [
        {"cluster_id": None},
        {"serverless": True},
        {"client_secret": ""},
        {"host": "http://workspace.example.com"},
        {"host": "https://user:secret@workspace.example.com"},
        {"host": "https://workspace.example.com/path"},
        {"clustr_id": "typo"},
    ],
)
def test_invalid_config_fails_in_typed_union(overrides):
    with pytest.raises(ValidationError):
        TypeAdapter(ConnectionConfig).validate_python(
            {"type": "databricks", **settings(**overrides)}
        )


def test_explicit_session_wins_and_multiple_remotes_rejected(sdk):
    connection = DatabricksConnection(**settings())
    explicit = MagicMock(name="explicit")
    connections = {"a": connection, "b": DatabricksConnection(**settings())}
    assert SparkEngine(connections=connections, spark_session=explicit).spark is explicit
    assert connection._session is None
    with pytest.raises(ValueError, match="only one"):
        SparkEngine(connections=connections)


def test_remote_error_is_sanitized_and_never_falls_back(sdk):
    builder, _ = sdk
    builder.sdkConfig.side_effect = RuntimeError("echo test-oauth-secret")
    connection = DatabricksConnection(**settings())
    with pytest.raises(ConnectionError, match="session setup failed") as exc:
        SparkEngine(connections={"remote": connection})
    rendered = "".join(
        traceback.format_exception(type(exc.value), exc.value, exc.value.__traceback__)
    )
    assert "test-oauth-secret" not in rendered
    assert connection._session is None


def test_missing_connect_has_install_guidance(monkeypatch):
    monkeypatch.setitem(sys.modules, "databricks.connect", None)
    connection = DatabricksConnection(**settings())
    with pytest.raises(ImportError, match="without pyspark or delta-spark"):
        SparkEngine(connections={"remote": connection})


def test_manager_does_not_fall_back_to_local_catalog(sdk):
    builder, _ = sdk
    builder.sdkConfig.side_effect = RuntimeError("credentials rejected")
    config = MagicMock()
    config.engine = "spark"
    config.logging.structured = False
    config.logging.level.value = "WARNING"
    config.lineage = None
    with pytest.raises(ConnectionError, match="session setup failed"):
        PipelineManager(config, {"remote": DatabricksConnection(**settings())})

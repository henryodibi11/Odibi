"""Remote session wiring, tested offline without Databricks credentials."""

import os
from importlib.metadata import version
import json
import subprocess
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


def test_explicit_credentials_ignore_missing_ambient_profile(tmp_path, monkeypatch):
    """Exercise real SDK config loading without network or an actual session."""
    core = pytest.importorskip("databricks.sdk.core")
    profile_file = tmp_path / "databrickscfg"
    profile_file.write_text("[DEFAULT]\nhost = https://unrelated.example.com\n")
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(profile_file))
    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "missing-profile")
    # Only suppress network/auth boundaries, not environment or file loading.
    monkeypatch.setattr(core.Config, "_resolve_host_metadata", lambda self: None, raising=False)
    monkeypatch.setattr(core.Config, "init_auth", lambda self: None)
    connect = ModuleType("databricks.connect")
    connect.DatabricksSession = MagicMock()
    monkeypatch.setitem(sys.modules, "databricks.connect", connect)
    builder = connect.DatabricksSession.builder
    session = builder.sdkConfig.return_value.getOrCreate.return_value
    assert DatabricksConnection(**settings()).get_spark_session() is session
    config = builder.sdkConfig.call_args.args[0]
    assert config.host == "https://workspace.example.com"
    assert config.client_id == "test-principal"
    assert config.auth_type == "oauth-m2m"
    assert os.environ["DATABRICKS_CONFIG_PROFILE"] == "missing-profile"


@pytest.fixture
def profile_runtime(tmp_path, monkeypatch):
    """Real SDK config/CLI auth; only subprocess, host discovery and Spark are fake."""
    core = pytest.importorskip("databricks.sdk.core")
    if tuple(int(p) for p in version("databricks-sdk").split(".")[:3]) < (0, 166, 0):
        pytest.skip("Named-profile integration requires SDK>=0.166.0")
    path = tmp_path / "profiles"
    path.write_text(
        "[DEFAULT]\nhost=https://default.example.com\ncluster_id=default-cluster\n"
        "auth_type=pat\ntoken=default-secret\n"
        "[chosen]\nhost=https://chosen.example.com\nauth_type=databricks-cli\n"
        "cluster_id=chosen-cluster\n"
    )
    for name, value in {
        "DATABRICKS_CONFIG_FILE": str(path),
        "DATABRICKS_CONFIG_PROFILE": "nonexistent",
        "DATABRICKS_HOST": "https://ambient.example.com",
        "DATABRICKS_TOKEN": "ambient-secret",
        "DATABRICKS_CLIENT_ID": "ambient-principal",
        "DATABRICKS_CLIENT_SECRET": "ambient-client-secret",
        "DATABRICKS_AUTH_TYPE": "oauth-m2m",
        "DATABRICKS_CLUSTER_ID": "ambient-cluster",
        "DATABRICKS_SERVERLESS_COMPUTE_ID": "auto",
        "ARM_CLIENT_ID": "ambient-azure-id",
        "ARM_CLIENT_SECRET": "ambient-azure-secret",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(core.Config, "_resolve_host_metadata", lambda self: None, raising=False)
    monkeypatch.setattr(
        "odibi.connections.databricks.shutil.which", lambda name: "/fake/databricks"
    )
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        if command[1] == "version":
            payload = {"Major": 0, "Minor": 296, "Patch": 0}
        else:
            assert command[1:5] == ["auth", "token", "--profile", "chosen"]
            payload = {
                "access_token": "chosen-oauth",
                "token_type": "Bearer",
                "expiry": "2099-01-01T00:00:00Z",
            }
        output = json.dumps(payload)
        return subprocess.CompletedProcess(
            command, 0, output if kwargs.get("text") else output.encode()
        )

    monkeypatch.setattr(subprocess, "run", run)
    connect = ModuleType("databricks.connect")
    connect.DatabricksSession = MagicMock()
    monkeypatch.setitem(sys.modules, "databricks.connect", connect)
    return path, connect.DatabricksSession.builder, commands


@pytest.mark.parametrize("target", [{}, {"cluster_id": "override"}, {"serverless": True}])
def test_named_profile_pins_identity_and_target(profile_runtime, target):
    path, builder, commands = profile_runtime
    before_env, before_file = dict(os.environ), path.read_bytes()
    config = TypeAdapter(ConnectionConfig).validate_python(
        {"type": "databricks", "profile": "chosen", **target}
    )
    connection = PipelineManager._build_connections({"compute": config})["compute"]
    session = connection.get_spark_session()
    assert connection.get_spark_session() is session
    sdk_config = builder.sdkConfig.call_args.args[0]
    assert sdk_config.profile == "chosen"
    assert sdk_config.host == "https://chosen.example.com"
    assert sdk_config.auth_type == "databricks-cli"
    assert sdk_config.authenticate()["Authorization"] == "Bearer chosen-oauth"
    assert (
        not sdk_config.token and not sdk_config.client_secret and not sdk_config.azure_client_secret
    )
    assert sdk_config.cluster_id == (
        None if target.get("serverless") else target.get("cluster_id", "chosen-cluster")
    )
    assert sdk_config.serverless_compute_id == ("auto" if target.get("serverless") else None)
    assert any(command[1] == "auth" for command in commands)
    assert dict(os.environ) == before_env
    assert path.read_bytes() == before_file


@pytest.mark.parametrize(
    "contents",
    [
        "[DEFAULT]\nhost=https://default.example.com\nauth_type=databricks-cli\ncluster_id=default\n",
        "[chosen]\nauth_type=databricks-cli\ncluster_id=target\n",
        "[chosen]\nhost=https://chosen.example.com\nauth_type=pat\ncluster_id=target\n",
        "[chosen]\nhost=https://chosen.example.com\nauth_type=databricks-cli\n",
        "[chosen]\nhost=https://chosen.example.com\nauth_type=databricks-cli\ncluster_id=target\nserverless_compute_id=auto\n",
    ],
)
def test_invalid_named_profile_has_no_ambient_or_default_fallback(profile_runtime, contents):
    path, builder, _ = profile_runtime
    path.write_text(contents)
    with pytest.raises(ConnectionError, match="Named-profile mode") as exc:
        DatabricksConnection(profile="chosen").get_spark_session()
    rendered = "".join(traceback.format_exception(exc.value))
    assert "ambient-secret" not in rendered and "default-secret" not in rendered
    builder.sdkConfig.assert_not_called()


def test_named_profile_auth_error_is_sanitized(profile_runtime, monkeypatch):
    _, _, _ = profile_runtime
    original_run = subprocess.run

    def fail(command, **kwargs):
        if command[1] == "version":
            return original_run(command, **kwargs)
        raise RuntimeError("Authorization: Bearer secret-echo")

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(ConnectionError) as exc:
        DatabricksConnection(profile="chosen").get_spark_session()
    assert "secret-echo" not in "".join(traceback.format_exception(exc.value))


@pytest.mark.parametrize("dependency", ["sdk", "cli", "missing-cli"])
def test_profile_dependencies_fail_closed(profile_runtime, monkeypatch, dependency):
    _, builder, _ = profile_runtime
    if dependency == "sdk":
        monkeypatch.setattr("odibi.connections.databricks.version", lambda name: "0.165.0")
    elif dependency == "missing-cli":
        monkeypatch.setattr("odibi.connections.databricks.shutil.which", lambda name: None)
    else:
        monkeypatch.setattr(
            subprocess,
            "run",
            lambda *a, **kw: subprocess.CompletedProcess(
                a[0], 0, '{"Major":0,"Minor":206,"Patch":0}'
            ),
        )
    with pytest.raises(ConnectionError, match="Named-profile mode"):
        DatabricksConnection(profile="chosen").get_spark_session()
    builder.sdkConfig.assert_not_called()


def test_profile_serverless_target_can_be_overridden(profile_runtime):
    path, builder, _ = profile_runtime
    path.write_text(
        path.read_text().replace("cluster_id=chosen-cluster", "serverless_compute_id=auto")
    )
    DatabricksConnection(profile="chosen", cluster_id="explicit-cluster").get_spark_session()
    config = builder.sdkConfig.call_args.args[0]
    assert config.cluster_id == "explicit-cluster"
    assert config.serverless_compute_id is None


@pytest.mark.parametrize(
    "extra",
    [
        {"host": "https://other.example.com"},
        {"client_id": "id"},
        {"client_secret": "secret"},
        {"serverless": True, "cluster_id": "target"},
    ],
)
def test_profile_mode_rejects_mixed_configuration(extra):
    with pytest.raises(ValidationError):
        DatabricksConnectionConfig(profile="chosen", **extra)

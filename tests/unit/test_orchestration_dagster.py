import subprocess
import pytest
from unittest.mock import MagicMock

from odibi.orchestration.dagster import DagsterFactory


# Dummy classes to simulate ProjectConfig structure
class DummyNode:
    def __init__(self, name, depends_on, description=""):
        self.name = name
        self.depends_on = depends_on
        self.description = description


class DummyPipeline:
    def __init__(self, pipeline, nodes):
        self.pipeline = pipeline
        self.nodes = nodes


class DummyConfig:
    def __init__(self, pipelines):
        self.pipelines = pipelines


# Pytest fixtures for dummy configurations
@pytest.fixture
def dummy_config_with_asset():
    node = DummyNode(name="node-1", depends_on=["node-0"], description="Test node description")
    pipeline = DummyPipeline(pipeline="pipeline1", nodes=[node])
    return DummyConfig(pipelines=[pipeline])


@pytest.fixture
def dummy_config_empty():
    return DummyConfig(pipelines=[])


# Dummy implementations for Dagster globals
class DummyDefinitions:
    def __init__(self, assets):
        self.assets = assets


def dummy_asset(*args, **kwargs):
    def decorator(fn):
        # Store the decorator arguments for testing purposes
        fn._dummy_asset_info = kwargs
        return fn

    return decorator


class DummyAssetExecutionContext:
    pass


# Test case: ImportError when Dagster is not installed
def test_create_definitions_import_error(monkeypatch, dummy_config_with_asset):
    """
    Ensure that create_definitions raises ImportError when 'Definitions' is not available.
    """
    import odibi.orchestration.dagster as dagster_module

    if "Definitions" in dagster_module.__dict__:
        monkeypatch.delitem(dagster_module.__dict__, "Definitions")
    factory = DagsterFactory(dummy_config_with_asset)
    with pytest.raises(ImportError, match="Dagster not installed. Run 'pip install dagster'"):
        factory.create_definitions()


# Test case: Empty configuration returns no assets
def test_create_definitions_empty(monkeypatch, dummy_config_empty):
    """
    Ensure that create_definitions returns an empty asset list when there are no pipelines.
    """
    import odibi.orchestration.dagster as dagster_module

    monkeypatch.setitem(dagster_module.__dict__, "Definitions", DummyDefinitions)
    monkeypatch.setitem(dagster_module.__dict__, "asset", dummy_asset)
    monkeypatch.setitem(
        dagster_module.__dict__, "AssetExecutionContext", DummyAssetExecutionContext
    )
    factory = DagsterFactory(dummy_config_empty)
    defs_obj = factory.create_definitions()
    assert isinstance(defs_obj, DummyDefinitions)
    assert defs_obj.assets == []


# Test case: Successful asset execution with subprocess returning zero
def test_execute_asset_success(monkeypatch, dummy_config_with_asset):
    """
    Verify that the generated asset function executes successfully when subprocess returns 0.
    """
    import odibi.orchestration.dagster as dagster_module

    monkeypatch.setitem(dagster_module.__dict__, "Definitions", DummyDefinitions)
    monkeypatch.setitem(dagster_module.__dict__, "asset", dummy_asset)
    monkeypatch.setitem(
        dagster_module.__dict__, "AssetExecutionContext", DummyAssetExecutionContext
    )

    factory = DagsterFactory(dummy_config_with_asset)
    defs_obj = factory.create_definitions()
    # There should be one asset function created
    assert len(defs_obj.assets) == 1
    asset_fn = defs_obj.assets[0]

    # Verify that the asset decorator applied the correct parameters
    asset_info = getattr(asset_fn, "_dummy_asset_info", None)
    expected_asset_name = "node_1"  # node-1 becomes node_1
    expected_deps = ["node_0"]
    expected_group = "pipeline1"
    expected_description = "Test node description"
    expected_compute_kind = "odibi"
    expected_op_tags = {"odibi/pipeline": "pipeline1", "odibi/node": "node-1"}
    assert asset_info.get("name") == expected_asset_name
    assert asset_info.get("deps") == expected_deps
    assert asset_info.get("group_name") == expected_group
    assert asset_info.get("description") == expected_description
    assert asset_info.get("compute_kind") == expected_compute_kind
    assert asset_info.get("op_tags") == expected_op_tags

    # Create a dummy context with a logger mock
    dummy_logger = MagicMock()
    DummyContext = type("DummyContext", (), {})
    dummy_context = DummyContext()
    dummy_context.log = dummy_logger

    # Simulate subprocess.run returning success
    dummy_completed_proc = subprocess.CompletedProcess(
        args=["odibi", "run", "--pipeline", "pipeline1", "--node", "node-1"],
        returncode=0,
        stdout="success output",
        stderr="",
    )

    def fake_run(cmd, capture_output, text):
        return dummy_completed_proc

    monkeypatch.setattr(subprocess, "run", fake_run)

    # Execute the asset function and verify logger calls
    asset_fn(dummy_context)
    dummy_logger.info.assert_any_call("Running Odibi node: node-1 in pipeline pipeline1")
    dummy_logger.info.assert_any_call("success output")
    dummy_logger.error.assert_not_called()


# Test case: Asset execution fails when subprocess returns non-zero
def test_execute_asset_failure(monkeypatch, dummy_config_with_asset):
    """
    Verify that the asset function raises an exception when subprocess returns a non-zero code.
    """
    import odibi.orchestration.dagster as dagster_module

    monkeypatch.setitem(dagster_module.__dict__, "Definitions", DummyDefinitions)
    monkeypatch.setitem(dagster_module.__dict__, "asset", dummy_asset)
    monkeypatch.setitem(
        dagster_module.__dict__, "AssetExecutionContext", DummyAssetExecutionContext
    )

    factory = DagsterFactory(dummy_config_with_asset)
    defs_obj = factory.create_definitions()
    assert len(defs_obj.assets) == 1
    asset_fn = defs_obj.assets[0]

    dummy_logger = MagicMock()
    DummyContext = type("DummyContext", (), {})
    dummy_context = DummyContext()
    dummy_context.log = dummy_logger

    dummy_failed_proc = subprocess.CompletedProcess(
        args=["odibi", "run", "--pipeline", "pipeline1", "--node", "node-1"],
        returncode=1,
        stdout="",
        stderr="error occurred",
    )

    def fake_run(cmd, capture_output, text):
        return dummy_failed_proc

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(Exception, match="Odibi execution failed: error occurred"):
        asset_fn(dummy_context)


# Test case: Successful asset execution logs stderr when present but does not raise error
def test_execute_asset_success_with_stderr(monkeypatch, dummy_config_with_asset):
    """
    Verify that the asset function logs stderr when present, yet succeeds if returncode is 0.
    """
    import odibi.orchestration.dagster as dagster_module

    monkeypatch.setitem(dagster_module.__dict__, "Definitions", DummyDefinitions)
    monkeypatch.setitem(dagster_module.__dict__, "asset", dummy_asset)
    monkeypatch.setitem(
        dagster_module.__dict__, "AssetExecutionContext", DummyAssetExecutionContext
    )

    factory = DagsterFactory(dummy_config_with_asset)
    defs_obj = factory.create_definitions()
    assert len(defs_obj.assets) == 1
    asset_fn = defs_obj.assets[0]

    dummy_logger = MagicMock()
    DummyContext = type("DummyContext", (), {})
    dummy_context = DummyContext()
    dummy_context.log = dummy_logger

    dummy_completed_proc = subprocess.CompletedProcess(
        args=["odibi", "run", "--pipeline", "pipeline1", "--node", "node-1"],
        returncode=0,
        stdout="some output",
        stderr="warning message",
    )

    def fake_run(cmd, capture_output, text):
        return dummy_completed_proc

    monkeypatch.setattr(subprocess, "run", fake_run)

    asset_fn(dummy_context)
    dummy_logger.info.assert_any_call("Running Odibi node: node-1 in pipeline pipeline1")
    dummy_logger.info.assert_any_call("some output")
    dummy_logger.error.assert_any_call("warning message")

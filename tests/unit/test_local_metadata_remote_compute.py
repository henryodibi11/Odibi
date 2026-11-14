"""Remote compute must not own explicitly client-local metadata or state."""

from pathlib import Path
import sys
from types import ModuleType
from unittest.mock import MagicMock

import pytest
from deltalake import DeltaTable

from odibi.catalog import CatalogManager
from odibi.config import ProjectConfig
from odibi.connections.databricks import DatabricksConnection
from odibi.connections.local import LocalConnection
from odibi.connections.unity_catalog import UnityCatalogConnection
from odibi.node import Node, NodeResult
from odibi.pipeline import PipelineManager


@pytest.fixture
def remote_project(tmp_path, monkeypatch):
    remote = MagicMock()
    remote.version = "4.1.0"
    remote.catalog.tableExists.return_value = False
    remote.read.format.return_value.load.side_effect = AssertionError("remote metadata read")
    remote.createDataFrame.side_effect = AssertionError("remote metadata write")
    monkeypatch.setattr(DatabricksConnection, "get_spark_session", lambda self: remote)
    sql = ModuleType("pyspark.sql")
    sql.SparkSession = MagicMock()
    sql.SparkSession.getActiveSession.return_value = remote
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    config = ProjectConfig.model_validate(
        {
            "project": "local_metadata",
            "engine": "spark",
            "connections": {
                "data": {"type": "local", "base_path": str(tmp_path)},
                "compute": {
                    "type": "databricks",
                    "host": "https://workspace.example.com",
                    "client_id": "synthetic-client",
                    "client_secret": "synthetic-secret",
                    "cluster_id": "synthetic-cluster",
                },
            },
            "system": {
                "connection": "data",
                "path": "catalog",
                "environment": "test",
                "async_derived_updates": False,
            },
            "story": {"connection": "data", "path": "stories", "async_generation": False},
            "pipelines": [
                {
                    "pipeline": "explore",
                    "nodes": [
                        {
                            "name": "sample",
                            "read": {"connection": "data", "format": "csv", "path": "unused.csv"},
                        }
                    ],
                }
            ],
        }
    )
    connections = PipelineManager._build_connections(config.connections)
    return config, connections, remote


@pytest.mark.parametrize("environment", [None, "test"])
def test_local_catalog_and_node_state_survive_remote_compute_restart(remote_project, environment):
    config, connections, remote = remote_project
    config.system.environment = environment
    manager = PipelineManager(config, connections)
    catalog = manager.catalog_manager
    assert catalog.spark is None
    assert manager.get_pipeline("explore").engine.spark is remote
    assert all(DeltaTable(path).version() == 0 for path in catalog.tables.values())
    catalog.log_run("failed-run", "explore", "sample", "FAILED")
    pipeline = manager.get_pipeline("explore")
    node = Node(
        config.pipelines[0].nodes[0],
        pipeline.context,
        pipeline.engine,
        connections,
        catalog_manager=catalog,
        pipeline_name="explore",
    )
    assert node.state_manager.get_last_run_status("explore", "sample") is False
    assert node.state_manager.get_hwm("explore.sample") is None
    node.state_manager.set_hwm("explore.sample", 17)
    catalog.log_run("recovered-run", "explore", "sample", "SUCCESS", rows_processed=3)
    restarted = PipelineManager(config, connections)
    pipeline = restarted.get_pipeline("explore")
    restored = Node(
        config.pipelines[0].nodes[0],
        pipeline.context,
        pipeline.engine,
        connections,
        catalog_manager=restarted.catalog_manager,
        pipeline_name="explore",
    )
    assert restored.state_manager.get_hwm("explore.sample") == 17
    assert restored.state_manager.get_last_run_status("explore", "sample") is True
    assert len(DeltaTable(catalog.tables["meta_runs"]).to_pandas()) == 2


def test_pipeline_state_story_and_catalog_reread(remote_project, tmp_path, monkeypatch):
    config, connections, remote = remote_project

    # Only the remote node boundary is replaced; orchestration, metadata, state and
    # story generation use their real implementations and persisted local Delta.
    monkeypatch.setattr(
        Node,
        "execute",
        lambda self: NodeResult(
            node_name=self.config.name, success=True, duration=0.1, rows_processed=3
        ),
    )
    manager = PipelineManager(config, connections)
    first = manager.run("explore")
    assert first.completed == ["sample"]
    assert first.story_path
    assert (tmp_path / "stories") in Path(first.story_path).parents
    assert Path(first.story_path).is_file()
    restarted = PipelineManager(config, connections)
    second = restarted.run("explore", resume_from_failure=True)
    assert second.completed == ["sample"]
    catalog = restarted.catalog_manager
    runs = DeltaTable(catalog.tables["meta_node_runs"]).to_pandas()
    assert runs["status"].tolist() == ["SUCCESS", "SUCCESS"]
    assert runs["node_name"].tolist() == ["sample", "sample"]
    assert len(DeltaTable(catalog.tables["meta_pipeline_runs"]).to_pandas()) == 2
    assert not DeltaTable(catalog.tables["meta_daily_stats"]).to_pandas().empty
    assert restarted.get_pipeline("explore").engine.spark is remote


@pytest.mark.parametrize("storage", ["uri", "uc", "local_spark"])
def test_existing_spark_catalog_selection_is_preserved(remote_project, monkeypatch, storage):
    config, connections, remote = remote_project
    if storage == "uri":
        connections["data"] = LocalConnection("dbfs:/metadata")
    elif storage == "uc":
        connections["data"] = UnityCatalogConnection("main", "metadata", create_schema=False)
    else:
        from pyspark.sql import SparkSession

        connections.pop("compute")
        config.connections.pop("compute")
        monkeypatch.setattr(SparkSession, "getActiveSession", lambda: remote)
    monkeypatch.setattr(CatalogManager, "bootstrap", lambda self: None)
    manager = PipelineManager(config, connections)
    assert manager.catalog_manager.spark is remote
    assert manager.get_pipeline("explore").engine.spark is remote

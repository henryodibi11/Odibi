import os

import pytest

from odibi.catalog import CatalogManager
from odibi.config import SystemConfig
from odibi.engine.pandas_engine import PandasEngine


@pytest.fixture
def catalog_manager(tmp_path):
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")

    # Initialize PandasEngine
    engine = PandasEngine(config={})

    return CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)


def test_bootstrap_local(catalog_manager):
    """Test that bootstrap creates system tables locally."""
    catalog_manager.bootstrap()

    # Check directories exist
    for table_path in catalog_manager.tables.values():
        assert os.path.exists(table_path), f"Table path {table_path} not created"
        # Check content exists (Delta log or Parquet file)
        assert len(os.listdir(table_path)) > 0, f"Table path {table_path} is empty"


def test_log_run_local(catalog_manager):
    """Test logging a run to the system catalog."""
    catalog_manager.bootstrap()

    run_id = "test_run_123"
    catalog_manager.log_run(
        run_id=run_id,
        pipeline_name="test_pipeline",
        node_name="test_node",
        status="SUCCESS",
        rows_processed=100,
        duration_ms=500,
    )

    # Read back to verify
    df = catalog_manager._read_local_table(catalog_manager.tables["meta_runs"])
    assert not df.empty

    # Check values
    row = df[df["run_id"] == run_id].iloc[0]
    assert row["pipeline_name"] == "test_pipeline"
    assert row["node_name"] == "test_node"
    assert row["status"] == "SUCCESS"
    assert row["rows_processed"] == 100


def test_register_asset_local(catalog_manager):
    """Test registering an asset."""
    catalog_manager.bootstrap()

    table_name = "bronze.users"
    path = "/data/bronze/users"

    catalog_manager.register_asset(
        project_name="test_project",
        table_name=table_name,
        path=path,
        format="parquet",
        pattern_type="snapshot",
    )

    # Read back
    df = catalog_manager._read_local_table(catalog_manager.tables["meta_tables"])
    assert not df.empty
    assert table_name in df["table_name"].values

    # Test resolving path
    resolved_path = catalog_manager.resolve_table_path(table_name)
    assert resolved_path == path

    # Test resolving non-existent path
    assert catalog_manager.resolve_table_path("fake_table") is None


def test_get_average_volume(catalog_manager):
    """Test calculating average volume metrics."""
    catalog_manager.bootstrap()

    # Log multiple runs
    node_name = "volume_node"
    catalog_manager.log_run("run1", "pipe", node_name, "SUCCESS", 100, 10)
    catalog_manager.log_run("run2", "pipe", node_name, "SUCCESS", 200, 10)
    catalog_manager.log_run("run3", "pipe", node_name, "FAILED", 0, 10)  # Should be ignored

    # Calculate average
    avg = catalog_manager.get_average_volume(node_name)

    assert avg == 150.0  # (100 + 200) / 2

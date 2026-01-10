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


@pytest.fixture
def catalog_manager_with_env(tmp_path):
    """CatalogManager with environment configured."""
    config = SystemConfig(connection="local", path="_odibi_system", environment="dev")
    base_path = str(tmp_path / "_odibi_system")
    engine = PandasEngine(config={})
    return CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)


def test_log_run_includes_environment(catalog_manager_with_env):
    """Test that log_run includes environment in the record."""
    catalog_manager_with_env.bootstrap()

    run_id = "env_test_run"
    catalog_manager_with_env.log_run(
        run_id=run_id,
        pipeline_name="test_pipeline",
        node_name="test_node",
        status="SUCCESS",
        rows_processed=50,
        duration_ms=100,
    )

    df = catalog_manager_with_env._read_local_table(catalog_manager_with_env.tables["meta_runs"])
    assert not df.empty
    assert "environment" in df.columns

    row = df[df["run_id"] == run_id].iloc[0]
    assert row["environment"] == "dev"


def test_log_runs_batch_includes_environment(catalog_manager_with_env):
    """Test that log_runs_batch includes environment in all records."""
    catalog_manager_with_env.bootstrap()

    records = [
        {
            "run_id": "batch_run_1",
            "pipeline_name": "pipe1",
            "node_name": "node1",
            "status": "SUCCESS",
        },
        {
            "run_id": "batch_run_2",
            "pipeline_name": "pipe1",
            "node_name": "node2",
            "status": "FAILED",
        },
    ]
    catalog_manager_with_env.log_runs_batch(records)

    df = catalog_manager_with_env._read_local_table(catalog_manager_with_env.tables["meta_runs"])
    assert len(df) == 2
    assert all(df["environment"] == "dev")


def test_log_run_without_environment(catalog_manager):
    """Test that log_run works when environment is not configured (None)."""
    catalog_manager.bootstrap()

    run_id = "no_env_run"
    catalog_manager.log_run(
        run_id=run_id,
        pipeline_name="test_pipeline",
        node_name="test_node",
        status="SUCCESS",
    )

    df = catalog_manager._read_local_table(catalog_manager.tables["meta_runs"])
    row = df[df["run_id"] == run_id].iloc[0]
    assert (
        row["environment"] is None
        or (hasattr(row["environment"], "__len__") and len(str(row["environment"])) == 0)
        or str(row["environment"]) == "None"
    )


class TestSqlServerSystemBackend:
    """Tests for SqlServerSystemBackend."""

    def test_sql_server_backend_initialization(self):
        """Test SqlServerSystemBackend can be initialized with mock connection."""
        from unittest.mock import MagicMock
        from odibi.state import SqlServerSystemBackend

        mock_conn = MagicMock()
        backend = SqlServerSystemBackend(
            connection=mock_conn,
            schema_name="test_schema",
            environment="dev",
        )

        assert backend.schema_name == "test_schema"
        assert backend.environment == "dev"
        assert backend.connection == mock_conn

    def test_sql_server_backend_log_run(self):
        """Test log_run calls execute with correct SQL."""
        from unittest.mock import MagicMock
        from odibi.state import SqlServerSystemBackend

        mock_conn = MagicMock()
        backend = SqlServerSystemBackend(
            connection=mock_conn,
            schema_name="odibi_system",
            environment="prod",
        )
        backend._tables_created = True  # Skip table creation

        backend.log_run(
            run_id="run_123",
            pipeline_name="test_pipe",
            node_name="test_node",
            status="SUCCESS",
            rows_processed=100,
            duration_ms=500,
        )

        # Verify execute was called
        assert mock_conn.execute.called
        call_args = mock_conn.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]

        assert "INSERT INTO [odibi_system].[meta_runs]" in sql
        assert params["run_id"] == "run_123"
        assert params["pipeline"] == "test_pipe"
        assert params["env"] == "prod"

    def test_sql_server_backend_set_hwm(self):
        """Test set_hwm uses MERGE statement."""
        from unittest.mock import MagicMock
        from odibi.state import SqlServerSystemBackend

        mock_conn = MagicMock()
        backend = SqlServerSystemBackend(
            connection=mock_conn,
            schema_name="odibi_system",
            environment="dev",
        )
        backend._tables_created = True

        backend.set_hwm("my_key", {"last_value": "2024-01-01"})

        assert mock_conn.execute.called
        call_args = mock_conn.execute.call_args
        sql = call_args[0][0]

        assert "MERGE" in sql
        assert "[odibi_system].[meta_state]" in sql

    def test_sql_server_backend_get_hwm(self):
        """Test get_hwm queries meta_state table."""
        from unittest.mock import MagicMock
        from odibi.state import SqlServerSystemBackend

        mock_conn = MagicMock()
        mock_conn.execute.return_value = [('{"last_value": "2024-01-01"}',)]

        backend = SqlServerSystemBackend(
            connection=mock_conn,
            schema_name="odibi_system",
            environment="dev",
        )
        backend._tables_created = True

        result = backend.get_hwm("my_key")

        assert result == {"last_value": "2024-01-01"}
        assert mock_conn.execute.called


class TestSystemSync:
    """Tests for system sync functionality."""

    def test_sync_from_config_model(self):
        """Test SyncFromConfig Pydantic model."""
        from odibi.config import SyncFromConfig

        config = SyncFromConfig(
            connection="local_parquet",
            path=".odibi/system/",
        )
        assert config.connection == "local_parquet"
        assert config.path == ".odibi/system/"
        assert config.schema_name is None

    def test_system_config_with_sync_from(self):
        """Test SystemConfig with sync_from field."""
        from odibi.config import SystemConfig, SyncFromConfig

        config = SystemConfig(
            connection="sql_server",
            schema_name="odibi_system",
            environment="prod",
            sync_from=SyncFromConfig(
                connection="local_parquet",
                path=".odibi/system/",
            ),
        )
        assert config.sync_from is not None
        assert config.sync_from.connection == "local_parquet"
        assert config.environment == "prod"

    def test_sync_system_data_both_tables(self, tmp_path):
        """Test sync_system_data syncs both runs and state."""
        from unittest.mock import MagicMock, patch
        from odibi.state import sync_system_data, CatalogStateBackend

        source = MagicMock(spec=CatalogStateBackend)
        target = MagicMock()
        target.set_hwm_batch = MagicMock()

        with patch("odibi.state._sync_runs", return_value=5) as mock_runs:
            with patch("odibi.state._sync_state", return_value=3) as mock_state:
                result = sync_system_data(source, target)

        assert result == {"runs": 5, "state": 3}
        mock_runs.assert_called_once_with(source, target)
        mock_state.assert_called_once_with(source, target)

    def test_sync_system_data_runs_only(self, tmp_path):
        """Test sync_system_data with tables filter."""
        from unittest.mock import MagicMock, patch
        from odibi.state import sync_system_data, CatalogStateBackend

        source = MagicMock(spec=CatalogStateBackend)
        target = MagicMock()

        with patch("odibi.state._sync_runs", return_value=10) as mock_runs:
            with patch("odibi.state._sync_state", return_value=0) as mock_state:
                result = sync_system_data(source, target, tables=["runs"])

        assert result == {"runs": 10, "state": 0}
        mock_runs.assert_called_once()
        mock_state.assert_not_called()

    def test_create_sync_source_backend_local(self, tmp_path):
        """Test create_sync_source_backend with local connection."""
        from odibi.state import create_sync_source_backend, CatalogStateBackend
        from odibi.config import SyncFromConfig

        sync_from = SyncFromConfig(
            connection="local_dev",
            path=".odibi/system",
        )
        connections = {
            "local_dev": {"type": "local", "base_path": str(tmp_path)},
        }

        backend = create_sync_source_backend(
            sync_from_config=sync_from,
            connections=connections,
            project_root=str(tmp_path),
        )

        assert isinstance(backend, CatalogStateBackend)
        assert ".odibi/system/meta_runs" in backend.meta_runs_path
        assert ".odibi/system/meta_state" in backend.meta_state_path

    def test_create_sync_source_backend_missing_connection(self, tmp_path):
        """Test create_sync_source_backend raises on missing connection."""
        from odibi.state import create_sync_source_backend
        from odibi.config import SyncFromConfig

        sync_from = SyncFromConfig(
            connection="nonexistent",
            path=".odibi/system",
        )
        connections = {}

        with pytest.raises(ValueError) as exc_info:
            create_sync_source_backend(
                sync_from_config=sync_from,
                connections=connections,
            )
        assert "nonexistent" in str(exc_info.value)

    def test_sync_runs_sql_to_sql(self):
        """Test syncing runs from SQL Server to SQL Server."""
        from unittest.mock import MagicMock
        from odibi.state import _sync_runs, SqlServerSystemBackend

        mock_conn = MagicMock()
        mock_conn.execute.return_value = [
            ("run1", "pipeline1", "node1", "SUCCESS", 100, 50, "{}"),
            ("run2", "pipeline1", "node2", "FAILED", 0, 10, "{}"),
        ]

        source = SqlServerSystemBackend(
            connection=mock_conn,
            schema_name="source_schema",
            environment=None,
        )
        source._tables_created = True

        target = MagicMock(spec=SqlServerSystemBackend)
        target.log_runs_batch = MagicMock()

        count = _sync_runs(source, target)

        assert count == 2
        target.log_runs_batch.assert_called_once()
        records = target.log_runs_batch.call_args[0][0]
        assert len(records) == 2
        assert records[0]["run_id"] == "run1"
        assert records[1]["status"] == "FAILED"

    def test_sync_state_sql_to_sql(self):
        """Test syncing state from SQL Server to SQL Server."""
        from unittest.mock import MagicMock
        from odibi.state import _sync_state, SqlServerSystemBackend

        mock_conn = MagicMock()
        mock_conn.execute.return_value = [
            ("hwm_key1", '{"value": 123}'),
            ("hwm_key2", '"2024-01-01"'),
        ]

        source = SqlServerSystemBackend(
            connection=mock_conn,
            schema_name="source_schema",
            environment=None,
        )
        source._tables_created = True

        target = MagicMock()
        target.set_hwm_batch = MagicMock()

        count = _sync_state(source, target)

        assert count == 2
        target.set_hwm_batch.assert_called_once()
        records = target.set_hwm_batch.call_args[0][0]
        assert len(records) == 2
        assert records[0]["key"] == "hwm_key1"
        assert records[0]["value"] == {"value": 123}


class TestSystemSyncIntegration:
    """Integration tests for system sync with real Delta tables."""

    def test_sync_delta_to_delta_runs(self, tmp_path):
        """Test syncing runs from one Delta location to another."""
        from datetime import datetime, timezone
        import pandas as pd
        from deltalake import write_deltalake, DeltaTable

        from odibi.state import (
            CatalogStateBackend,
            sync_system_data,
        )

        # Setup source with sample run data
        source_path = tmp_path / "source"
        source_runs_path = str(source_path / "meta_runs")

        sample_runs = pd.DataFrame(
            [
                {
                    "run_id": "run_001",
                    "pipeline_name": "bronze_pipeline",
                    "node_name": "ingest_customers",
                    "status": "SUCCESS",
                    "rows_processed": 1000,
                    "duration_ms": 500,
                    "metrics_json": "{}",
                    "environment": "dev",
                    "timestamp": datetime.now(timezone.utc),
                    "date": datetime.now(timezone.utc).date(),
                },
                {
                    "run_id": "run_002",
                    "pipeline_name": "bronze_pipeline",
                    "node_name": "ingest_orders",
                    "status": "SUCCESS",
                    "rows_processed": 5000,
                    "duration_ms": 1200,
                    "metrics_json": '{"avg_order_value": 42.50}',
                    "environment": "dev",
                    "timestamp": datetime.now(timezone.utc),
                    "date": datetime.now(timezone.utc).date(),
                },
                {
                    "run_id": "run_003",
                    "pipeline_name": "silver_pipeline",
                    "node_name": "transform_customers",
                    "status": "FAILED",
                    "rows_processed": 0,
                    "duration_ms": 100,
                    "metrics_json": '{"error": "null key"}',
                    "environment": "dev",
                    "timestamp": datetime.now(timezone.utc),
                    "date": datetime.now(timezone.utc).date(),
                },
            ]
        )

        write_deltalake(source_runs_path, sample_runs, mode="overwrite")

        # Setup target (empty)
        target_path = tmp_path / "target"
        target_runs_path = str(target_path / "meta_runs")
        target_state_path = str(target_path / "meta_state")

        source_backend = CatalogStateBackend(
            meta_runs_path=source_runs_path,
            meta_state_path=str(source_path / "meta_state"),
            environment="dev",
        )

        target_backend = CatalogStateBackend(
            meta_runs_path=target_runs_path,
            meta_state_path=target_state_path,
            environment="prod",
        )

        # Sync only runs
        result = sync_system_data(source_backend, target_backend, tables=["runs"])

        assert result["runs"] == 3
        assert result["state"] == 0

        # Verify target has the data
        dt = DeltaTable(target_runs_path)
        target_df = dt.to_pandas()

        assert len(target_df) == 3
        assert set(target_df["run_id"]) == {"run_001", "run_002", "run_003"}
        assert all(target_df["environment"] == "prod")

    def test_sync_delta_to_delta_state(self, tmp_path):
        """Test syncing HWM state from one Delta location to another."""
        from datetime import datetime, timezone
        import pandas as pd
        from deltalake import write_deltalake, DeltaTable

        from odibi.state import (
            CatalogStateBackend,
            sync_system_data,
        )

        # Setup source with sample state data
        source_path = tmp_path / "source"
        source_state_path = str(source_path / "meta_state")

        sample_state = pd.DataFrame(
            [
                {
                    "key": "bronze_pipeline.ingest_customers.hwm",
                    "value": '"2024-06-15T10:30:00Z"',
                    "environment": "dev",
                    "updated_at": datetime.now(timezone.utc),
                },
                {
                    "key": "bronze_pipeline.ingest_orders.hwm",
                    "value": '{"last_id": 99999, "last_date": "2024-06-15"}',
                    "environment": "dev",
                    "updated_at": datetime.now(timezone.utc),
                },
            ]
        )

        write_deltalake(source_state_path, sample_state, mode="overwrite")

        # Setup target
        target_path = tmp_path / "target"
        target_state_path = str(target_path / "meta_state")

        source_backend = CatalogStateBackend(
            meta_runs_path=str(source_path / "meta_runs"),
            meta_state_path=source_state_path,
            environment="dev",
        )

        target_backend = CatalogStateBackend(
            meta_runs_path=str(target_path / "meta_runs"),
            meta_state_path=target_state_path,
            environment="prod",
        )

        # Sync only state
        result = sync_system_data(source_backend, target_backend, tables=["state"])

        assert result["runs"] == 0
        assert result["state"] == 2

        # Verify target has the data
        dt = DeltaTable(target_state_path)
        target_df = dt.to_pandas()

        assert len(target_df) == 2
        assert "bronze_pipeline.ingest_customers.hwm" in target_df["key"].values

    def test_sync_delta_to_delta_full(self, tmp_path):
        """Test full sync of both runs and state."""
        from datetime import datetime, timezone
        import pandas as pd
        from deltalake import write_deltalake, DeltaTable

        from odibi.state import (
            CatalogStateBackend,
            sync_system_data,
        )

        # Setup source
        source_path = tmp_path / "source"

        sample_runs = pd.DataFrame(
            [
                {
                    "run_id": "full_run_001",
                    "pipeline_name": "test_pipeline",
                    "node_name": "node_a",
                    "status": "SUCCESS",
                    "rows_processed": 100,
                    "duration_ms": 50,
                    "metrics_json": "{}",
                    "environment": "dev",
                    "timestamp": datetime.now(timezone.utc),
                    "date": datetime.now(timezone.utc).date(),
                },
            ]
        )
        write_deltalake(str(source_path / "meta_runs"), sample_runs, mode="overwrite")

        sample_state = pd.DataFrame(
            [
                {
                    "key": "test_pipeline.node_a.hwm",
                    "value": '"2024-01-01"',
                    "environment": "dev",
                    "updated_at": datetime.now(timezone.utc),
                },
            ]
        )
        write_deltalake(str(source_path / "meta_state"), sample_state, mode="overwrite")

        # Setup target
        target_path = tmp_path / "target"

        source_backend = CatalogStateBackend(
            meta_runs_path=str(source_path / "meta_runs"),
            meta_state_path=str(source_path / "meta_state"),
            environment="dev",
        )

        target_backend = CatalogStateBackend(
            meta_runs_path=str(target_path / "meta_runs"),
            meta_state_path=str(target_path / "meta_state"),
            environment="prod",
        )

        # Full sync
        result = sync_system_data(source_backend, target_backend)

        assert result["runs"] == 1
        assert result["state"] == 1

        # Verify both tables
        runs_df = DeltaTable(str(target_path / "meta_runs")).to_pandas()
        state_df = DeltaTable(str(target_path / "meta_state")).to_pandas()

        assert len(runs_df) == 1
        assert runs_df.iloc[0]["run_id"] == "full_run_001"

        assert len(state_df) == 1
        assert state_df.iloc[0]["key"] == "test_pipeline.node_a.hwm"

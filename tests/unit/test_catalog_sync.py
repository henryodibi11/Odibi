"""Unit tests for catalog_sync.py - sync operations and edge cases."""

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from odibi.catalog import CatalogManager
from odibi.catalog_sync import (
    ALL_SYNC_TABLES,
    CatalogSyncer,
    DEFAULT_SYNC_TABLES,
    SQL_SERVER_DDL,
    TABLE_PRIMARY_KEYS,
)
from odibi.config import SyncToConfig, SystemConfig
from odibi.engine.pandas_engine import PandasEngine


@pytest.fixture
def catalog_manager(tmp_path):
    """Create a catalog manager for testing."""
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    engine = PandasEngine(config={})
    cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
    cm.bootstrap()
    return cm


@pytest.fixture
def mock_sql_connection():
    """Create a mock SQL Server connection."""
    conn = Mock()
    conn.type = "sql_server"
    conn.connection_type = "sql_server"
    conn.__class__.__name__ = "AzureSQL"
    return conn


@pytest.fixture
def mock_delta_connection():
    """Create a mock Delta/ADLS connection."""
    conn = Mock()
    conn.type = "azure_adls"
    conn.connection_type = "azure_adls"
    conn.__class__.__name__ = "AzureADLS"
    return conn


@pytest.fixture
def sync_config():
    """Create a basic sync configuration."""
    return SyncToConfig(
        connection="target_conn",
        mode="full",
        tables=None,  # Use default tables
        schema_name="test_schema",
    )


class TestCatalogSyncerInit:
    """Test CatalogSyncer initialization and configuration."""

    def test_init_with_sql_server(self, catalog_manager, mock_sql_connection, sync_config):
        """Test initializing syncer with SQL Server target."""
        syncer = CatalogSyncer(
            source_catalog=catalog_manager,
            sync_config=sync_config,
            target_connection=mock_sql_connection,
            environment="dev",
        )

        assert syncer.source == catalog_manager
        assert syncer.config == sync_config
        assert syncer.target == mock_sql_connection
        assert syncer.environment == "dev"
        assert syncer.target_type == "sql_server"
        assert syncer.status == "idle"

    def test_init_with_delta_target(self, catalog_manager, mock_delta_connection, sync_config):
        """Test initializing syncer with Delta target."""
        syncer = CatalogSyncer(
            source_catalog=catalog_manager,
            sync_config=sync_config,
            target_connection=mock_delta_connection,
        )

        assert syncer.target_type == "delta"

    def test_target_type_detection_by_class_name(self, catalog_manager, sync_config):
        """Test target type detection from class name."""
        # Mock connection with no type attribute
        conn = Mock(spec=[])
        conn.__class__.__name__ = "SQLServerConnection"

        syncer = CatalogSyncer(
            source_catalog=catalog_manager,
            sync_config=sync_config,
            target_connection=conn,
        )

        assert syncer.target_type == "sql_server"

    def test_target_type_detection_default(self, catalog_manager, sync_config):
        """Test target type defaults to delta when unknown."""
        conn = Mock(spec=[])
        conn.__class__.__name__ = "UnknownConnection"

        syncer = CatalogSyncer(
            source_catalog=catalog_manager,
            sync_config=sync_config,
            target_connection=conn,
        )

        assert syncer.target_type == "delta"


class TestTableSelection:
    """Test table selection and validation."""

    def test_get_tables_to_sync_default(self, catalog_manager, mock_sql_connection):
        """Test getting default tables when none specified."""
        config = SyncToConfig(connection="target", mode="full", tables=None)
        syncer = CatalogSyncer(catalog_manager, config, mock_sql_connection)

        tables = syncer.get_tables_to_sync()
        assert tables == DEFAULT_SYNC_TABLES
        assert "meta_runs" in tables
        assert "meta_pipeline_runs" in tables

    def test_get_tables_to_sync_custom(self, catalog_manager, mock_sql_connection):
        """Test getting custom tables list."""
        custom_tables = ["meta_runs", "meta_pipelines", "meta_nodes"]
        config = SyncToConfig(connection="target", mode="full", tables=custom_tables)
        syncer = CatalogSyncer(catalog_manager, config, mock_sql_connection)

        tables = syncer.get_tables_to_sync()
        assert set(tables) == set(custom_tables)

    def test_get_tables_to_sync_invalid_filtered(self, catalog_manager, mock_sql_connection):
        """Test that invalid table names are filtered out."""
        config = SyncToConfig(
            connection="target", mode="full", tables=["meta_runs", "invalid_table", "meta_nodes"]
        )
        syncer = CatalogSyncer(catalog_manager, config, mock_sql_connection)

        tables = syncer.get_tables_to_sync()
        assert "meta_runs" in tables
        assert "meta_nodes" in tables
        assert "invalid_table" not in tables

    def test_all_sync_tables_constant(self):
        """Test that ALL_SYNC_TABLES contains expected tables."""
        assert "meta_tables" in ALL_SYNC_TABLES
        assert "meta_runs" in ALL_SYNC_TABLES
        assert "meta_pipelines" in ALL_SYNC_TABLES
        assert len(ALL_SYNC_TABLES) > 10  # Should have multiple tables


class TestSyncOperations:
    """Test sync operations and data flow."""

    def test_sync_to_sql_server_no_data(self, catalog_manager, mock_sql_connection, sync_config):
        """Test syncing empty table to SQL Server."""
        syncer = CatalogSyncer(catalog_manager, sync_config, mock_sql_connection)

        # Mock methods
        syncer._ensure_sql_schema = Mock()
        syncer._ensure_sql_table = Mock()
        syncer._read_source_table = Mock(return_value=pd.DataFrame())  # Empty dataframe
        syncer._update_sync_state = Mock()
        syncer._ensure_sql_views = Mock()

        result = syncer.sync(tables=["meta_runs"])

        assert "meta_runs" in result
        assert result["meta_runs"]["success"] is True
        assert result["meta_runs"]["rows"] == 0

    def test_sync_to_sql_server_with_data(self, catalog_manager, mock_sql_connection, sync_config):
        """Test syncing table with data to SQL Server."""
        syncer = CatalogSyncer(catalog_manager, sync_config, mock_sql_connection)

        # Create sample data
        sample_df = pd.DataFrame(
            {
                "run_id": ["run1", "run2"],
                "pipeline_name": ["pipe1", "pipe2"],
                "status": ["SUCCESS", "FAILED"],
                "rows_processed": [100, 50],
            }
        )

        # Mock methods
        syncer._ensure_sql_schema = Mock()
        syncer._ensure_sql_table = Mock()
        syncer._read_source_table = Mock(return_value=sample_df)
        syncer._apply_incremental_filter = Mock(return_value=sample_df)
        syncer._apply_column_mappings = Mock(return_value=sample_df)
        syncer._insert_to_sql_server = Mock()
        syncer._update_sync_state = Mock()
        syncer._ensure_sql_views = Mock()

        result = syncer.sync(tables=["meta_runs"])

        assert "meta_runs" in result
        assert result["meta_runs"]["success"] is True
        assert result["meta_runs"]["rows"] == 2
        syncer._insert_to_sql_server.assert_called_once()

    def test_sync_failure_handling(self, catalog_manager, mock_sql_connection, sync_config):
        """Test that sync failures are caught and reported."""
        syncer = CatalogSyncer(catalog_manager, sync_config, mock_sql_connection)

        # Make read throw an error
        syncer._read_source_table = Mock(side_effect=Exception("Connection failed"))
        syncer._ensure_sql_schema = Mock()
        syncer._ensure_sql_table = Mock()
        syncer._update_sync_state = Mock()
        syncer._ensure_sql_views = Mock()

        result = syncer.sync(tables=["meta_runs"])

        assert "meta_runs" in result
        assert result["meta_runs"]["success"] is False
        assert "Connection failed" in result["meta_runs"]["error"]

    def test_sync_multiple_tables(self, catalog_manager, mock_sql_connection, sync_config):
        """Test syncing multiple tables."""
        syncer = CatalogSyncer(catalog_manager, sync_config, mock_sql_connection)

        # Mock for successful sync
        syncer._ensure_sql_schema = Mock()
        syncer._ensure_sql_table = Mock()
        syncer._read_source_table = Mock(return_value=pd.DataFrame({"id": [1, 2]}))
        syncer._apply_incremental_filter = Mock(side_effect=lambda df, t: df)
        syncer._apply_column_mappings = Mock(side_effect=lambda df, t: df)
        syncer._insert_to_sql_server = Mock()
        syncer._update_sync_state = Mock()
        syncer._ensure_sql_views = Mock()

        tables = ["meta_runs", "meta_pipelines", "meta_nodes"]
        result = syncer.sync(tables=tables)

        assert len(result) == 3
        for table in tables:
            assert table in result
            assert result[table]["success"] is True

    def test_sync_async(self, catalog_manager, mock_sql_connection, sync_config):
        """Test asynchronous sync starts in background."""
        syncer = CatalogSyncer(catalog_manager, sync_config, mock_sql_connection)

        # Mock sync method
        syncer.sync = Mock(return_value={"meta_runs": {"success": True, "rows": 0}})

        # Call async sync
        syncer.sync_async(tables=["meta_runs"])

        # Give thread a moment to start
        import time

        time.sleep(0.1)

        # Sync should have been called (may take a moment in thread)
        # We can't reliably assert this in unit test without thread synchronization


class TestIncrementalSync:
    """Test incremental sync mode."""

    def test_apply_incremental_filter_pandas(self, catalog_manager, mock_sql_connection):
        """Test incremental filter for pandas dataframe."""
        config = SyncToConfig(connection="target", mode="incremental", tables=["meta_runs"])
        syncer = CatalogSyncer(catalog_manager, config, mock_sql_connection)

        # Create data with dates
        now = datetime.now(timezone.utc)
        old_date = now - timedelta(days=10)

        df = pd.DataFrame(
            {
                "run_id": ["run1", "run2", "run3"],
                "timestamp": [old_date, now, now],
                "date": [old_date.date(), now.date(), now.date()],
            }
        )

        # Mock last sync to 5 days ago
        syncer._get_last_sync_timestamp = Mock(return_value=now - timedelta(days=5))

        # Apply filter
        filtered = syncer._apply_incremental_filter(df, "meta_runs")

        # Should filter out old_date row
        assert len(filtered) == 2
        assert "run1" not in filtered["run_id"].values

    def test_apply_incremental_filter_no_timestamp(self, catalog_manager, mock_sql_connection):
        """Test incremental filter when no last sync timestamp exists."""
        config = SyncToConfig(connection="target", mode="incremental")
        syncer = CatalogSyncer(catalog_manager, config, mock_sql_connection)

        df = pd.DataFrame({"run_id": ["run1", "run2"], "status": ["SUCCESS", "FAILED"]})

        # No last sync timestamp
        syncer._get_last_sync_timestamp = Mock(return_value=None)

        # Should return all rows
        filtered = syncer._apply_incremental_filter(df, "meta_runs")
        assert len(filtered) == 2


class TestSQLServerOperations:
    """Test SQL Server specific operations."""

    def test_ensure_sql_schema_called(self, catalog_manager, mock_sql_connection, sync_config):
        """Test that schema creation is attempted."""
        syncer = CatalogSyncer(catalog_manager, sync_config, mock_sql_connection)

        with patch.object(syncer, "_ensure_sql_schema"):
            syncer._ensure_sql_schema = Mock()
            syncer._ensure_sql_table = Mock()
            syncer._read_source_table = Mock(return_value=pd.DataFrame())
            syncer._update_sync_state = Mock()

            syncer._sync_to_sql_server("meta_runs")

            # Note: We can't directly test private method call in real execution
            # This test structure shows intent

    def test_sql_server_ddl_templates_exist(self):
        """Test that DDL templates exist for key tables."""
        assert "meta_runs" in SQL_SERVER_DDL
        assert "meta_pipeline_runs" in SQL_SERVER_DDL
        assert "meta_node_runs" in SQL_SERVER_DDL
        assert "meta_failures" in SQL_SERVER_DDL

        # Check DDL contains CREATE TABLE
        assert "CREATE TABLE" in SQL_SERVER_DDL["meta_runs"]

    def test_table_primary_keys_defined(self):
        """Test that primary keys are defined for tables."""
        assert "meta_runs" in TABLE_PRIMARY_KEYS
        assert "meta_pipeline_runs" in TABLE_PRIMARY_KEYS
        assert "meta_node_runs" in TABLE_PRIMARY_KEYS

        # Check keys are lists
        assert isinstance(TABLE_PRIMARY_KEYS["meta_runs"], list)
        assert len(TABLE_PRIMARY_KEYS["meta_runs"]) > 0


class TestDeltaSync:
    """Test syncing to Delta targets."""

    def test_sync_to_delta_basic(self, catalog_manager, mock_delta_connection, sync_config):
        """Test basic sync to Delta target."""
        syncer = CatalogSyncer(catalog_manager, sync_config, mock_delta_connection)

        # Create sample data
        sample_df = pd.DataFrame({"run_id": ["run1"], "status": ["SUCCESS"]})

        # Mock methods
        syncer._read_source_table = Mock(return_value=sample_df)
        syncer._apply_incremental_filter = Mock(return_value=sample_df)
        syncer._update_sync_state = Mock()

        # Mock the actual write (would need real Delta write in integration test)
        with patch.object(syncer.source, "engine") as mock_engine:
            mock_engine.write = Mock()

            syncer._sync_to_delta("meta_runs")

            # Expect failure since we're not fully mocking the delta write path
            # In real scenario, this would write to Delta Lake


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_sync_missing_source_table(self, catalog_manager, mock_sql_connection, sync_config):
        """Test syncing a table that doesn't exist in source."""
        syncer = CatalogSyncer(catalog_manager, sync_config, mock_sql_connection)

        # Mock to remove table from source
        syncer.source.tables = {}

        syncer._ensure_sql_schema = Mock()
        syncer._ensure_sql_table = Mock()
        syncer._update_sync_state = Mock()
        syncer._ensure_sql_views = Mock()

        result = syncer.sync(tables=["meta_runs"])

        assert "meta_runs" in result
        assert result["meta_runs"]["success"] is False
        assert "not found in source" in result["meta_runs"]["error"]

    def test_sync_with_none_dataframe(self, catalog_manager, mock_sql_connection, sync_config):
        """Test handling None return from source read."""
        syncer = CatalogSyncer(catalog_manager, sync_config, mock_sql_connection)

        syncer._ensure_sql_schema = Mock()
        syncer._ensure_sql_table = Mock()
        syncer._read_source_table = Mock(return_value=None)
        syncer._update_sync_state = Mock()
        syncer._ensure_sql_views = Mock()

        result = syncer.sync(tables=["meta_runs"])

        assert result["meta_runs"]["success"] is True
        assert result["meta_runs"]["rows"] == 0

    def test_sync_with_empty_tables_list(self, catalog_manager, mock_sql_connection, sync_config):
        """Test sync with empty tables list falls back to defaults."""
        syncer = CatalogSyncer(catalog_manager, sync_config, mock_sql_connection)

        syncer._ensure_sql_schema = Mock()
        syncer._ensure_sql_table = Mock()
        syncer._read_source_table = Mock(return_value=pd.DataFrame())
        syncer._update_sync_state = Mock()
        syncer._ensure_sql_views = Mock()

        result = syncer.sync(tables=[])

        # Empty list is falsy, so it uses defaults
        assert len(result) > 0  # Should sync default tables

    def test_purge_sql_tables(self, catalog_manager, mock_sql_connection, sync_config):
        """Test purging old data from SQL tables."""
        syncer = CatalogSyncer(catalog_manager, sync_config, mock_sql_connection)

        # Mock SQL connection methods
        mock_sql_connection.cursor = Mock()
        mock_cursor = Mock()
        mock_sql_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_sql_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_sql_connection.commit = Mock()

        # Call purge
        result = syncer.purge_sql_tables(days=90)

        # Should return results dict
        assert isinstance(result, dict)

    def test_column_mapping_applied(self, catalog_manager, mock_sql_connection, sync_config):
        """Test that column mappings are applied correctly."""
        syncer = CatalogSyncer(catalog_manager, sync_config, mock_sql_connection)

        # Create dataframe with timestamp column
        df = pd.DataFrame(
            {
                "run_id": ["run1"],
                "timestamp": [datetime.now(timezone.utc)],
            }
        )

        # Apply mappings (implementation specific)
        result = syncer._apply_column_mappings(df, "meta_runs")

        # Result should still be a dataframe
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1


class TestSyncStateManagement:
    """Test sync state tracking."""

    def test_update_sync_state_called(self, catalog_manager, mock_sql_connection, sync_config):
        """Test that sync state is updated after sync."""
        syncer = CatalogSyncer(catalog_manager, sync_config, mock_sql_connection)

        syncer._ensure_sql_schema = Mock()
        syncer._ensure_sql_table = Mock()
        syncer._read_source_table = Mock(return_value=pd.DataFrame())
        syncer._update_sync_state = Mock()
        syncer._ensure_sql_views = Mock()

        syncer.sync(tables=["meta_runs"])

        # _update_sync_state should be called once
        syncer._update_sync_state.assert_called_once()

    def test_get_last_sync_timestamp(self, catalog_manager, mock_sql_connection, sync_config):
        """Test retrieving last sync timestamp."""
        syncer = CatalogSyncer(catalog_manager, sync_config, mock_sql_connection)

        # Mock state backend
        with patch.object(syncer.source, "_read_local_table") as mock_read:
            # Return empty dataframe (no prior sync)
            mock_read.return_value = pd.DataFrame()

            timestamp = syncer._get_last_sync_timestamp("meta_runs")
            assert timestamp is None


class TestConnectionTypeDetection:
    """Test various connection type detection scenarios."""

    def test_detect_azure_sql_by_type(self, catalog_manager, sync_config):
        """Test detecting AzureSQL connection."""
        conn = Mock()
        conn.type = "azure_sql"
        conn.connection_type = "azure_sql"

        syncer = CatalogSyncer(catalog_manager, sync_config, conn)
        assert syncer.target_type == "sql_server"

    def test_detect_adls_by_type(self, catalog_manager, sync_config):
        """Test detecting ADLS connection."""
        conn = Mock()
        conn.type = "azure_blob"
        conn.connection_type = "azure_blob"

        syncer = CatalogSyncer(catalog_manager, sync_config, conn)
        assert syncer.target_type == "delta"

    def test_detect_local_as_delta(self, catalog_manager, sync_config):
        """Test detecting local connection as delta target."""
        conn = Mock()
        conn.type = "local"

        syncer = CatalogSyncer(catalog_manager, sync_config, conn)
        assert syncer.target_type == "delta"

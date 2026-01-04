"""Tests for catalog path consistency across the framework.

This test module verifies that:
1. CatalogManager and StateBackend use consistent path naming
2. All meta_* tables are accessible via both catalog and state backend
3. HWM operations work correctly with catalog integration
"""

import pytest

from odibi.catalog import CatalogManager
from odibi.config import SystemConfig
from odibi.engine.pandas_engine import PandasEngine
from odibi.state import CatalogStateBackend, StateManager, create_state_backend


class MockProjectConfig:
    """Mock project config for testing create_state_backend."""

    def __init__(self, system_conn_name, connections):
        self.system = MockSystemConfig(system_conn_name)
        self.connections = connections


class MockSystemConfig:
    """Mock system config."""

    def __init__(self, connection):
        self.connection = connection
        self.path = "_odibi_system"


class MockLocalConnection:
    """Mock local connection."""

    def __init__(self, base_path):
        self.type = "local"
        self.base_path = base_path


class TestCatalogPathNaming:
    """Test that all catalog paths follow the meta_* naming convention."""

    @pytest.fixture
    def catalog_manager(self, tmp_path):
        """Create a catalog manager with a temp directory."""
        config = SystemConfig(connection="local", path="_odibi_system")
        base_path = str(tmp_path / "_odibi_system")
        engine = PandasEngine(config={})
        return CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)

    def test_all_tables_use_meta_prefix(self, catalog_manager):
        """Verify all tables in CatalogManager use meta_ prefix."""
        for table_name, table_path in catalog_manager.tables.items():
            assert table_name.startswith("meta_"), f"Table '{table_name}' should start with 'meta_'"
            assert f"/{table_name}" in table_path or table_path.endswith(
                table_name
            ), f"Path for '{table_name}' should end with the table name: {table_path}"

    def test_expected_tables_exist(self, catalog_manager):
        """Verify all 9 expected meta tables are defined."""
        expected_tables = [
            "meta_tables",
            "meta_runs",
            "meta_patterns",
            "meta_metrics",
            "meta_state",
            "meta_pipelines",
            "meta_nodes",
            "meta_schemas",
            "meta_lineage",
        ]
        for table in expected_tables:
            assert table in catalog_manager.tables, f"Missing table: {table}"


class TestStateBackendPathConsistency:
    """Test that StateBackend uses the same paths as CatalogManager."""

    @pytest.fixture
    def tmp_catalog_path(self, tmp_path):
        """Create temp directory for catalog."""
        return str(tmp_path / "_odibi_system")

    def test_create_state_backend_uses_meta_prefix(self, tmp_path):
        """Test that create_state_backend creates paths with meta_ prefix."""
        base_path = str(tmp_path)
        connections = {"local_conn": MockLocalConnection(base_path)}
        config = MockProjectConfig("local_conn", connections)

        backend = create_state_backend(config, project_root=str(tmp_path))

        assert isinstance(backend, CatalogStateBackend)
        assert (
            "meta_state" in backend.meta_state_path
        ), f"meta_state_path should contain 'meta_state': {backend.meta_state_path}"
        assert (
            "meta_runs" in backend.meta_runs_path
        ), f"meta_runs_path should contain 'meta_runs': {backend.meta_runs_path}"

    def test_state_backend_paths_match_catalog(self, tmp_catalog_path):
        """Test that StateBackend paths match CatalogManager paths."""
        config = SystemConfig(connection="local", path="_odibi_system")
        engine = PandasEngine(config={})
        catalog = CatalogManager(
            spark=None, config=config, base_path=tmp_catalog_path, engine=engine
        )
        catalog.bootstrap()

        backend = CatalogStateBackend(
            meta_state_path=catalog.tables["meta_state"],
            meta_runs_path=catalog.tables["meta_runs"],
        )

        assert backend.meta_state_path == catalog.tables["meta_state"]
        assert backend.meta_runs_path == catalog.tables["meta_runs"]


class TestHWMRoundTrip:
    """Test HWM set/get operations with catalog integration."""

    @pytest.fixture
    def catalog_and_backend(self, tmp_path):
        """Create catalog and state backend for testing."""
        base_path = str(tmp_path / "_odibi_system")
        config = SystemConfig(connection="local", path="_odibi_system")
        engine = PandasEngine(config={})
        catalog = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
        catalog.bootstrap()

        backend = CatalogStateBackend(
            meta_state_path=catalog.tables["meta_state"],
            meta_runs_path=catalog.tables["meta_runs"],
        )

        return catalog, backend

    def test_hwm_set_and_get(self, catalog_and_backend):
        """Test that HWM values can be set and retrieved."""
        catalog, backend = catalog_and_backend

        key = "test_node_hwm"
        value = {"timestamp": "2024-01-01T00:00:00", "count": 100}

        backend.set_hwm(key, value)
        retrieved = backend.get_hwm(key)

        assert retrieved == value, f"HWM mismatch: expected {value}, got {retrieved}"

    def test_hwm_update_existing(self, catalog_and_backend):
        """Test that HWM values can be updated."""
        catalog, backend = catalog_and_backend

        key = "update_test_hwm"
        initial_value = 100
        updated_value = 200

        backend.set_hwm(key, initial_value)
        assert backend.get_hwm(key) == initial_value

        backend.set_hwm(key, updated_value)
        assert backend.get_hwm(key) == updated_value

    def test_hwm_nonexistent_key(self, catalog_and_backend):
        """Test that getting non-existent HWM returns None."""
        catalog, backend = catalog_and_backend

        result = backend.get_hwm("nonexistent_key")
        assert result is None


class TestCatalogIntegration:
    """Integration tests for catalog and state backend working together."""

    @pytest.fixture
    def integrated_setup(self, tmp_path):
        """Set up integrated catalog and state manager."""
        base_path = str(tmp_path / "_odibi_system")
        config = SystemConfig(connection="local", path="_odibi_system")
        engine = PandasEngine(config={})

        catalog = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
        catalog.bootstrap()

        backend = CatalogStateBackend(
            meta_state_path=catalog.tables["meta_state"],
            meta_runs_path=catalog.tables["meta_runs"],
        )

        state_manager = StateManager(backend=backend)

        return catalog, state_manager

    def test_log_run_and_get_last_run(self, integrated_setup):
        """Test that runs logged by catalog can be read by state backend."""
        catalog, state_manager = integrated_setup

        pipeline_name = "test_pipeline"
        node_name = "test_node"
        run_id = "run_001"

        catalog.log_run(
            run_id=run_id,
            pipeline_name=pipeline_name,
            node_name=node_name,
            status="SUCCESS",
            rows_processed=100,
            duration_ms=500,
        )

        last_run = state_manager.get_last_run_info(pipeline_name, node_name)

        assert last_run is not None, "Should be able to retrieve last run info"
        assert last_run.get("success") is True, "Run should be marked as success"

    def test_state_manager_hwm_through_catalog_tables(self, integrated_setup):
        """Test that state manager HWM operations use catalog paths correctly."""
        catalog, state_manager = integrated_setup

        key = "integration_test_hwm"
        value = "2024-01-15T12:00:00"

        state_manager.set_hwm(key, value)
        retrieved = state_manager.get_hwm(key)

        assert retrieved == value

        df = catalog._read_local_table(catalog.tables["meta_state"])
        assert not df.empty, "meta_state table should have records"
        assert key in df["key"].values, f"Key '{key}' should be in meta_state table"

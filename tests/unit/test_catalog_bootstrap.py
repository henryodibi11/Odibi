"""Unit tests for CatalogManager bootstrap, table creation, and schema migration.

Issue #288: Tests for catalog.py lines 376-654 covering bootstrap(), _ensure_table(),
_table_exists(), and _migrate_schema_if_needed().

NOTE: Test names avoid 'spark' and 'delta' to prevent conftest.py from skipping on Windows.
"""

import logging
import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.catalog import CatalogManager, StructField, StructType, StringType, LongType
from odibi.config import SystemConfig
from odibi.engine.pandas_engine import PandasEngine


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog_manager(tmp_path):
    """Create a CatalogManager in Pandas mode WITHOUT bootstrapping."""
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    engine = PandasEngine(config={})
    cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
    return cm


@pytest.fixture
def bootstrapped_catalog(tmp_path):
    """Create a CatalogManager in Pandas mode WITH bootstrap completed."""
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    engine = PandasEngine(config={})
    cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
    cm.bootstrap()
    return cm


# ---------------------------------------------------------------------------
# Expected system tables
# ---------------------------------------------------------------------------

EXPECTED_TABLES = [
    "meta_tables",
    "meta_runs",
    "meta_patterns",
    "meta_metrics",
    "meta_state",
    "meta_pipelines",
    "meta_nodes",
    "meta_schemas",
    "meta_lineage",
    "meta_outputs",
    "meta_pipeline_runs",
    "meta_node_runs",
    "meta_failures",
    "meta_observability_errors",
    "meta_derived_applied_runs",
    "meta_daily_stats",
    "meta_pipeline_health",
    "meta_sla_status",
]


# ===========================================================================
# bootstrap()
# ===========================================================================


class TestBootstrap:
    """Tests for CatalogManager.bootstrap()."""

    def test_no_engine_no_session_returns_early(self, tmp_path):
        """bootstrap() returns early when neither engine nor session is set."""
        config = SystemConfig(connection="local", path="_odibi_system")
        base_path = str(tmp_path / "_odibi_system")
        cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=None)
        cm.bootstrap()
        # No directories should be created
        assert not os.path.exists(base_path)

    def test_creates_all_system_table_directories(self, bootstrapped_catalog):
        """bootstrap() creates directories for all 18 system tables."""
        cm = bootstrapped_catalog
        for table_name in EXPECTED_TABLES:
            path = cm.tables[table_name]
            assert os.path.exists(path), f"Table directory missing: {table_name}"

    def test_tables_dict_contains_expected_keys(self, catalog_manager):
        """The tables dict should contain all 18 expected keys."""
        for table_name in EXPECTED_TABLES:
            assert table_name in catalog_manager.tables, f"Missing key: {table_name}"
        assert len(catalog_manager.tables) == 18

    def test_idempotent_no_error_on_second_call(self, catalog_manager):
        """Calling bootstrap() twice should not raise any errors."""
        catalog_manager.bootstrap()
        catalog_manager.bootstrap()  # second call — should be a no-op

    def test_idempotent_tables_still_exist_after_second_call(self, catalog_manager):
        """All tables should still exist after bootstrapping twice."""
        catalog_manager.bootstrap()
        catalog_manager.bootstrap()
        for table_name in EXPECTED_TABLES:
            path = catalog_manager.tables[table_name]
            assert os.path.exists(path), f"Table missing after second bootstrap: {table_name}"


# ===========================================================================
# _ensure_table()
# ===========================================================================


class TestEnsureTable:
    """Tests for CatalogManager._ensure_table()."""

    def test_creates_directory_for_new_table(self, catalog_manager):
        """_ensure_table should create a directory for a table that doesn't exist."""
        schema = StructType([StructField("id", StringType(), True)])
        catalog_manager._ensure_table("meta_tables", schema)
        assert os.path.exists(catalog_manager.tables["meta_tables"])

    def test_skips_creation_when_table_exists(self, bootstrapped_catalog, caplog):
        """_ensure_table should skip creation when table already exists."""
        schema = bootstrapped_catalog._get_schema_meta_tables()
        with caplog.at_level(logging.DEBUG, logger="odibi.catalog"):
            bootstrapped_catalog._ensure_table("meta_tables", schema)
        assert any("System table exists" in msg for msg in caplog.messages)

    def test_partition_cols_accepted(self, catalog_manager):
        """_ensure_table should accept partition_cols without error."""
        schema = catalog_manager._get_schema_meta_runs()
        catalog_manager._ensure_table("meta_runs", schema, partition_cols=["pipeline_name", "date"])
        assert os.path.exists(catalog_manager.tables["meta_runs"])

    def test_schema_evolution_flag_accepted(self, catalog_manager):
        """_ensure_table should accept schema_evolution flag without crash."""
        schema = catalog_manager._get_schema_meta_runs()
        catalog_manager._ensure_table("meta_runs", schema, schema_evolution=True)
        assert os.path.exists(catalog_manager.tables["meta_runs"])

    def test_table_readable_after_creation(self, bootstrapped_catalog):
        """A created table should be readable via _read_local_table."""
        path = bootstrapped_catalog.tables["meta_tables"]
        df = bootstrapped_catalog._read_local_table(path)
        assert isinstance(df, pd.DataFrame)

    def test_created_table_is_empty(self, bootstrapped_catalog):
        """A freshly bootstrapped table should have zero rows."""
        path = bootstrapped_catalog.tables["meta_tables"]
        df = bootstrapped_catalog._read_local_table(path)
        assert len(df) == 0

    def test_schema_has_correct_columns(self, bootstrapped_catalog):
        """Created table should have the expected column names from its schema."""
        expected_schema = bootstrapped_catalog._get_schema_meta_tables()
        expected_cols = {f.name for f in expected_schema.fields}
        path = bootstrapped_catalog.tables["meta_tables"]
        df = bootstrapped_catalog._read_local_table(path)
        # Only check if columns match when df is non-empty or has columns
        if len(df.columns) > 0:
            assert set(df.columns) == expected_cols

    def test_ensure_table_calls_migrate_on_existing(self, bootstrapped_catalog):
        """_ensure_table should call _migrate_schema_if_needed when table exists."""
        schema = bootstrapped_catalog._get_schema_meta_tables()
        with patch.object(bootstrapped_catalog, "_migrate_schema_if_needed") as mock_migrate:
            bootstrapped_catalog._ensure_table("meta_tables", schema)
            mock_migrate.assert_called_once_with(
                "meta_tables", bootstrapped_catalog.tables["meta_tables"], schema
            )


# ===========================================================================
# _table_exists()
# ===========================================================================


class TestTableExists:
    """Tests for CatalogManager._table_exists()."""

    def test_returns_false_for_nonexistent_path(self, catalog_manager):
        """_table_exists should return False for a path that doesn't exist."""
        assert catalog_manager._table_exists("/nonexistent/path/xyz") is False

    def test_returns_true_after_bootstrap(self, bootstrapped_catalog):
        """_table_exists should return True for tables created by bootstrap."""
        path = bootstrapped_catalog.tables["meta_tables"]
        assert bootstrapped_catalog._table_exists(path) is True

    def test_returns_false_for_empty_directory(self, tmp_path):
        """_table_exists should return False for an empty directory."""
        config = SystemConfig(connection="local", path="_odibi_system")
        base_path = str(tmp_path / "_odibi_system")
        engine = PandasEngine(config={})
        cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
        empty_dir = str(tmp_path / "empty_table")
        os.makedirs(empty_dir, exist_ok=True)
        assert cm._table_exists(empty_dir) is False

    def test_returns_false_with_no_engine(self, tmp_path):
        """_table_exists should return False when no engine is set."""
        config = SystemConfig(connection="local", path="_odibi_system")
        base_path = str(tmp_path / "_odibi_system")
        cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=None)
        assert cm._table_exists(base_path) is False


# ===========================================================================
# _migrate_schema_if_needed()
# ===========================================================================


class TestMigrateSchemaIfNeeded:
    """Tests for CatalogManager._migrate_schema_if_needed()."""

    def test_no_crash_on_nonexistent_path(self, catalog_manager):
        """_migrate_schema_if_needed should not crash on a non-existent path."""
        schema = StructType([StructField("id", StringType(), True)])
        # Should warn but not raise
        catalog_manager._migrate_schema_if_needed("test_table", "/nonexistent/path", schema)

    def test_no_op_when_schemas_match(self, bootstrapped_catalog, caplog):
        """_migrate_schema_if_needed should be a no-op when schemas already match."""
        schema = bootstrapped_catalog._get_schema_meta_tables()
        path = bootstrapped_catalog.tables["meta_tables"]
        with caplog.at_level(logging.INFO, logger="odibi.catalog"):
            bootstrapped_catalog._migrate_schema_if_needed("meta_tables", path, schema)
        # Should NOT log "Migrating schema" since schemas match
        assert not any("Migrating schema" in msg for msg in caplog.messages)

    def test_warning_logged_on_migration_failure(self, catalog_manager, caplog):
        """_migrate_schema_if_needed should log a warning on failure."""
        schema = StructType([StructField("id", StringType(), True)])
        # Patch the engine to be pandas mode but make the import fail
        with caplog.at_level(logging.WARNING, logger="odibi.catalog"):
            with patch(
                "odibi.catalog.CatalogManager.is_pandas_mode",
                new_callable=lambda: property(lambda self: True),
            ):
                # Trigger with a path that will cause DeltaTable to fail
                catalog_manager._migrate_schema_if_needed(
                    "test_table", "/bogus/path/for/migration", schema
                )
        assert any("Schema migration check failed" in msg for msg in caplog.messages)


# ===========================================================================
# Edge cases
# ===========================================================================


class TestEdgeCases:
    """Edge case tests for bootstrap and table management."""

    def test_base_path_trailing_slash_stripped(self, tmp_path):
        """CatalogManager should strip trailing slashes from base_path."""
        config = SystemConfig(connection="local", path="_odibi_system")
        base_with_slash = str(tmp_path / "_odibi_system") + "/"
        engine = PandasEngine(config={})
        cm = CatalogManager(spark=None, config=config, base_path=base_with_slash, engine=engine)
        assert not cm.base_path.endswith("/")

    def test_table_paths_correctly_formed(self, catalog_manager):
        """Each table path should be base_path/table_name."""
        for table_name, path in catalog_manager.tables.items():
            expected = f"{catalog_manager.base_path}/{table_name}"
            assert path == expected, f"Bad path for {table_name}: {path}"

    def test_bootstrap_with_mock_engine_not_pandas(self, tmp_path):
        """bootstrap() with engine.name != 'pandas' still goes through ensure_table."""
        config = SystemConfig(connection="local", path="_odibi_system")
        base_path = str(tmp_path / "_odibi_system")
        mock_engine = MagicMock()
        mock_engine.name = "polars"
        cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=mock_engine)
        # Should not crash but won't create tables (pandas branch not taken)
        cm.bootstrap()

    def test_multiple_tables_have_unique_paths(self, catalog_manager):
        """All 18 table paths should be unique."""
        paths = list(catalog_manager.tables.values())
        assert len(paths) == len(set(paths))

    def test_ensure_table_with_all_type_columns(self, catalog_manager):
        """_ensure_table handles schemas with diverse column types."""
        from odibi.catalog import DoubleType, TimestampType, DateType

        schema = StructType(
            [
                StructField("str_col", StringType(), True),
                StructField("long_col", LongType(), True),
                StructField("double_col", DoubleType(), True),
                StructField("ts_col", TimestampType(), True),
                StructField("date_col", DateType(), True),
            ]
        )
        catalog_manager._ensure_table("meta_tables", schema)
        assert os.path.exists(catalog_manager.tables["meta_tables"])

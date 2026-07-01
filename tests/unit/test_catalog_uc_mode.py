"""Tests for CatalogManager Unity Catalog mode.

Tests the UC-specific code paths: table name resolution, _table_exists,
_ensure_table, _spark_read_table, _spark_write_append, _merge_target_ref.
"""

from unittest.mock import MagicMock


from odibi.config import SystemConfig
from odibi.connections.unity_catalog import UnityCatalogConnection


def _make_catalog_manager(uc=True, spark=None):
    """Create a CatalogManager with optional UC connection and mock Spark."""
    # Import here to avoid top-level pyspark issues
    from odibi.catalog import CatalogManager

    if uc:
        conn = UnityCatalogConnection(catalog="workspace", schema="odibi_logs")
    else:
        conn = MagicMock()
        conn.__class__ = type("LocalConnection", (), {})

    config = SystemConfig(connection="uc_metadata", path="_odibi_system")

    cm = CatalogManager(
        spark=spark,
        config=config,
        base_path="workspace.odibi_logs._odibi_system" if uc else "/tmp/_odibi_system",
        engine=None,
        connection=conn,
    )
    return cm


# ===================================================================
# UC mode detection
# ===================================================================


class TestUCModeDetection:
    def test_uc_connection_detected(self):
        cm = _make_catalog_manager(uc=True)
        assert cm._is_uc is True
        assert cm.is_unity_catalog_mode is True

    def test_non_uc_connection_not_detected(self):
        cm = _make_catalog_manager(uc=False)
        assert cm._is_uc is False
        assert cm.is_unity_catalog_mode is False

    def test_none_connection_not_detected(self):
        from odibi.catalog import CatalogManager

        config = SystemConfig(connection="local", path="_sys")
        cm = CatalogManager(
            spark=None,
            config=config,
            base_path="/tmp/_sys",
            engine=None,
            connection=None,
        )
        assert cm._is_uc is False


# ===================================================================
# Table name resolution
# ===================================================================


class TestUCTableNames:
    def test_uc_tables_are_fully_qualified(self):
        cm = _make_catalog_manager(uc=True)
        assert cm.tables["meta_tables"] == "workspace.odibi_logs.meta_tables"
        assert cm.tables["meta_runs"] == "workspace.odibi_logs.meta_runs"
        assert cm.tables["meta_pipelines"] == "workspace.odibi_logs.meta_pipelines"

    def test_uc_all_18_tables_qualified(self):
        cm = _make_catalog_manager(uc=True)
        assert len(cm.tables) == 18
        for key, value in cm.tables.items():
            assert value == f"workspace.odibi_logs.{key}"

    def test_non_uc_tables_are_file_paths(self):
        cm = _make_catalog_manager(uc=False)
        assert cm.tables["meta_tables"] == "/tmp/_odibi_system/meta_tables"
        assert cm.tables["meta_runs"] == "/tmp/_odibi_system/meta_runs"


# ===================================================================
# _table_exists (UC mode)
# ===================================================================


class TestUCTableExists:
    def test_uc_uses_catalog_tableExists(self):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        cm = _make_catalog_manager(uc=True, spark=mock_spark)

        assert cm._table_exists("workspace.odibi_logs.meta_tables") is True
        mock_spark.catalog.tableExists.assert_called_once_with("workspace.odibi_logs.meta_tables")

    def test_uc_table_not_exists(self):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        cm = _make_catalog_manager(uc=True, spark=mock_spark)

        assert cm._table_exists("workspace.odibi_logs.meta_tables") is False

    def test_uc_table_exists_exception(self):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.side_effect = Exception("fail")
        cm = _make_catalog_manager(uc=True, spark=mock_spark)

        assert cm._table_exists("workspace.odibi_logs.meta_tables") is False

    def test_non_uc_uses_delta_load(self):
        mock_spark = MagicMock()
        cm = _make_catalog_manager(uc=False, spark=mock_spark)

        cm._table_exists("/tmp/path")
        mock_spark.read.format("delta").load.assert_called_with("/tmp/path")


# ===================================================================
# _ensure_table (UC mode)
# ===================================================================


class TestUCEnsureTable:
    def test_uc_uses_saveAsTable(self):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        cm = _make_catalog_manager(uc=True, spark=mock_spark)

        # Need a schema
        schema = cm._get_schema_meta_tables()
        cm._ensure_table("meta_tables", schema)

        # Should call saveAsTable, not save
        writer = mock_spark.createDataFrame.return_value.write.format.return_value
        writer.saveAsTable.assert_called_once_with("workspace.odibi_logs.meta_tables")

    def test_uc_with_partition_cols(self):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        cm = _make_catalog_manager(uc=True, spark=mock_spark)

        schema = cm._get_schema_meta_runs()
        cm._ensure_table("meta_runs", schema, partition_cols=["pipeline_name", "date"])

        writer = mock_spark.createDataFrame.return_value.write.format.return_value
        writer.partitionBy.assert_called_once_with("pipeline_name", "date")
        writer.partitionBy.return_value.saveAsTable.assert_called_once()

    def test_uc_table_already_exists_skips(self):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        cm = _make_catalog_manager(uc=True, spark=mock_spark)

        schema = cm._get_schema_meta_tables()
        cm._ensure_table("meta_tables", schema)

        mock_spark.createDataFrame.assert_not_called()


# ===================================================================
# _spark_read_table
# ===================================================================


class TestSparkReadTable:
    def test_uc_uses_spark_table(self):
        mock_spark = MagicMock()
        cm = _make_catalog_manager(uc=True, spark=mock_spark)

        cm._spark_read_table("meta_pipelines")
        mock_spark.table.assert_called_once_with("workspace.odibi_logs.meta_pipelines")

    def test_non_uc_uses_delta_load(self):
        mock_spark = MagicMock()
        cm = _make_catalog_manager(uc=False, spark=mock_spark)

        cm._spark_read_table("meta_pipelines")
        mock_spark.read.format.assert_called_once_with("delta")
        mock_spark.read.format.return_value.load.assert_called_once_with(
            "/tmp/_odibi_system/meta_pipelines"
        )


# ===================================================================
# _spark_write_append
# ===================================================================


class TestSparkWriteAppend:
    def test_uc_uses_saveAsTable(self):
        mock_spark = MagicMock()
        cm = _make_catalog_manager(uc=True, spark=mock_spark)
        mock_df = MagicMock()

        cm._spark_write_append(mock_df, "meta_runs")
        mock_df.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
            "workspace.odibi_logs.meta_runs"
        )

    def test_non_uc_uses_save(self):
        mock_spark = MagicMock()
        cm = _make_catalog_manager(uc=False, spark=mock_spark)
        mock_df = MagicMock()

        cm._spark_write_append(mock_df, "meta_runs")
        mock_df.write.format.return_value.mode.return_value.save.assert_called_once_with(
            "/tmp/_odibi_system/meta_runs"
        )


# ===================================================================
# _merge_target_ref
# ===================================================================


class TestMergeTargetRef:
    def test_uc_returns_table_name(self):
        cm = _make_catalog_manager(uc=True)
        ref = cm._merge_target_ref("meta_pipelines")
        assert ref == "workspace.odibi_logs.meta_pipelines"

    def test_non_uc_returns_delta_path(self):
        cm = _make_catalog_manager(uc=False)
        ref = cm._merge_target_ref("meta_pipelines")
        assert ref == "delta.`/tmp/_odibi_system/meta_pipelines`"


# ===================================================================
# bootstrap (UC mode)
# ===================================================================


class TestUCBootstrap:
    def test_bootstrap_creates_all_tables(self):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        cm = _make_catalog_manager(uc=True, spark=mock_spark)

        cm.bootstrap()

        # Should have called createDataFrame 18 times (one per table)
        assert mock_spark.createDataFrame.call_count == 18
        assert cm._bootstrapped is True

    def test_bootstrap_skips_when_already_done(self):
        mock_spark = MagicMock()
        cm = _make_catalog_manager(uc=True, spark=mock_spark)
        cm._bootstrapped = True

        cm.bootstrap()
        mock_spark.catalog.tableExists.assert_not_called()

    def test_bootstrap_skips_existing_tables(self):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        cm = _make_catalog_manager(uc=True, spark=mock_spark)

        cm.bootstrap()
        mock_spark.createDataFrame.assert_not_called()

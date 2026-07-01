"""Tests for UnityCatalogConnection and factory function."""

from unittest.mock import MagicMock, patch

import pytest

from odibi.connections.unity_catalog import UnityCatalogConnection
from odibi.connections.factory import create_unity_catalog_connection


@pytest.fixture(autouse=True)
def _patch_logging_context():
    with patch("odibi.connections.factory.get_logging_context") as mock_ctx:
        mock_ctx.return_value = MagicMock()
        yield


@pytest.fixture(autouse=True)
def _patch_logger():
    with patch("odibi.connections.factory.logger") as mock_logger:
        mock_logger.register_secret = MagicMock()
        yield


# ===================================================================
# UnityCatalogConnection
# ===================================================================


class TestUnityCatalogConnection:
    def test_get_path_simple(self):
        conn = UnityCatalogConnection(catalog="workspace", schema="odibi_logs")
        assert conn.get_path("meta_tables") == "workspace.odibi_logs.meta_tables"

    def test_get_path_already_qualified(self):
        conn = UnityCatalogConnection(catalog="workspace", schema="odibi_logs")
        assert conn.get_path("a.b.c") == "a.b.c"

    def test_get_path_default_schema(self):
        conn = UnityCatalogConnection(catalog="main")
        assert conn.get_path("my_table") == "main.default.my_table"

    def test_pandas_storage_options_empty(self):
        conn = UnityCatalogConnection(catalog="main", schema="s")
        assert conn.pandas_storage_options() == {}

    def test_validate_no_spark_noop(self):
        """validate() does nothing when SparkSession is not available."""
        conn = UnityCatalogConnection(catalog="main", schema="s")
        mock_ss = MagicMock()
        mock_ss.getActiveSession.return_value = None
        with patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": MagicMock(SparkSession=mock_ss)}):
            conn.validate()  # should not raise

    def test_validate_creates_schema(self):
        conn = UnityCatalogConnection(catalog="ws", schema="logs")
        mock_spark = MagicMock()
        mock_ss = MagicMock()
        mock_ss.getActiveSession.return_value = mock_spark
        with patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": MagicMock(SparkSession=mock_ss)}):
            conn.validate()
            mock_spark.sql.assert_called_once_with(
                "CREATE SCHEMA IF NOT EXISTS ws.logs"
            )

    def test_validate_create_schema_false(self):
        conn = UnityCatalogConnection(
            catalog="ws", schema="logs", create_schema=False
        )
        conn.validate()  # should return immediately, no Spark import

    def test_validate_exception_swallowed(self):
        conn = UnityCatalogConnection(catalog="ws", schema="logs")
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("permission denied")
        mock_ss = MagicMock()
        mock_ss.getActiveSession.return_value = mock_spark
        with patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": MagicMock(SparkSession=mock_ss)}):
            conn.validate()  # should not raise

    def test_validate_import_error_swallowed(self):
        """validate() handles missing pyspark gracefully."""
        conn = UnityCatalogConnection(catalog="ws", schema="logs")
        # Remove pyspark from modules so import fails
        import sys
        orig = sys.modules.get("pyspark")
        orig_sql = sys.modules.get("pyspark.sql")
        sys.modules["pyspark"] = None
        sys.modules["pyspark.sql"] = None
        try:
            conn.validate()  # should not raise
        finally:
            if orig is not None:
                sys.modules["pyspark"] = orig
            else:
                sys.modules.pop("pyspark", None)
            if orig_sql is not None:
                sys.modules["pyspark.sql"] = orig_sql
            else:
                sys.modules.pop("pyspark.sql", None)

    def test_discover_catalog_no_spark(self):
        conn = UnityCatalogConnection(catalog="ws", schema="s")
        mock_ss = MagicMock()
        mock_ss.getActiveSession.return_value = None
        with patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": MagicMock(SparkSession=mock_ss)}):
            result = conn.discover_catalog()
            assert result["datasets"] == []
            assert "error" in result

    def test_discover_catalog_with_tables(self):
        conn = UnityCatalogConnection(catalog="ws", schema="logs")
        mock_spark = MagicMock()
        mock_row1 = MagicMock()
        mock_row1.__getitem__ = lambda self, k: "meta_tables"
        mock_row2 = MagicMock()
        mock_row2.__getitem__ = lambda self, k: "meta_runs"
        mock_spark.sql.return_value.collect.return_value = [mock_row1, mock_row2]
        mock_ss = MagicMock()
        mock_ss.getActiveSession.return_value = mock_spark

        with patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": MagicMock(SparkSession=mock_ss)}):
            result = conn.discover_catalog()
            assert len(result["datasets"]) == 2
            assert result["datasets"][0]["name"] == "meta_tables"
            assert result["datasets"][0]["full_name"] == "ws.logs.meta_tables"

    def test_discover_catalog_with_pattern(self):
        conn = UnityCatalogConnection(catalog="ws", schema="logs")
        mock_spark = MagicMock()
        mock_row1 = MagicMock()
        mock_row1.__getitem__ = lambda self, k: "meta_tables"
        mock_row2 = MagicMock()
        mock_row2.__getitem__ = lambda self, k: "other_table"
        mock_spark.sql.return_value.collect.return_value = [mock_row1, mock_row2]
        mock_ss = MagicMock()
        mock_ss.getActiveSession.return_value = mock_spark

        with patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": MagicMock(SparkSession=mock_ss)}):
            result = conn.discover_catalog(pattern="meta")
            assert len(result["datasets"]) == 1

    def test_discover_catalog_with_limit(self):
        conn = UnityCatalogConnection(catalog="ws", schema="logs")
        mock_spark = MagicMock()
        rows = []
        for i in range(5):
            r = MagicMock()
            r.__getitem__ = lambda self, k, i=i: f"table_{i}"
            rows.append(r)
        mock_spark.sql.return_value.collect.return_value = rows
        mock_ss = MagicMock()
        mock_ss.getActiveSession.return_value = mock_spark

        with patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": MagicMock(SparkSession=mock_ss)}):
            result = conn.discover_catalog(limit=2)
            assert len(result["datasets"]) == 2

    def test_discover_catalog_exception(self):
        conn = UnityCatalogConnection(catalog="ws", schema="logs")
        mock_ss = MagicMock()
        mock_ss.getActiveSession.side_effect = Exception("fail")
        with patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": MagicMock(SparkSession=mock_ss)}):
            result = conn.discover_catalog()
            assert result["datasets"] == []
            assert "error" in result

    def test_list_tables(self):
        conn = UnityCatalogConnection(catalog="ws", schema="logs")
        with patch.object(
            conn,
            "discover_catalog",
            return_value={"datasets": [{"name": "t1"}, {"name": "t2"}]},
        ):
            assert conn.list_tables() == ["t1", "t2"]

    def test_list_tables_empty(self):
        conn = UnityCatalogConnection(catalog="ws", schema="logs")
        with patch.object(
            conn, "discover_catalog", return_value={"datasets": []}
        ):
            assert conn.list_tables() == []

    def test_get_freshness_no_spark(self):
        conn = UnityCatalogConnection(catalog="ws", schema="logs")
        mock_ss = MagicMock()
        mock_ss.getActiveSession.return_value = None
        with patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": MagicMock(SparkSession=mock_ss)}):
            result = conn.get_freshness("meta_tables")
            assert "error" in result

    def test_get_freshness_with_history(self):
        conn = UnityCatalogConnection(catalog="ws", schema="logs")
        mock_spark = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, k: {
            "timestamp": "2026-07-01T10:00:00",
            "operation": "MERGE",
        }[k]
        mock_spark.sql.return_value.collect.return_value = [mock_row]
        mock_ss = MagicMock()
        mock_ss.getActiveSession.return_value = mock_spark

        with patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": MagicMock(SparkSession=mock_ss)}):
            result = conn.get_freshness("meta_tables")
            assert result["last_modified"] == "2026-07-01T10:00:00"
            assert result["operation"] == "MERGE"
            mock_spark.sql.assert_called_with(
                "DESCRIBE HISTORY ws.logs.meta_tables LIMIT 1"
            )

    def test_get_freshness_empty_history(self):
        conn = UnityCatalogConnection(catalog="ws", schema="logs")
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        mock_ss = MagicMock()
        mock_ss.getActiveSession.return_value = mock_spark

        with patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": MagicMock(SparkSession=mock_ss)}):
            result = conn.get_freshness("meta_tables")
            assert result["last_modified"] is None

    def test_get_freshness_exception(self):
        conn = UnityCatalogConnection(catalog="ws", schema="logs")
        mock_ss = MagicMock()
        mock_ss.getActiveSession.side_effect = Exception("boom")
        with patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": MagicMock(SparkSession=mock_ss)}):
            result = conn.get_freshness("meta_tables")
            assert "error" in result

    def test_is_base_connection_subclass(self):
        from odibi.connections.base import BaseConnection

        conn = UnityCatalogConnection(catalog="ws", schema="s")
        assert isinstance(conn, BaseConnection)


# ===================================================================
# create_unity_catalog_connection factory
# ===================================================================


class TestCreateUnityCatalogFactory:
    def test_basic(self):
        result = create_unity_catalog_connection(
            "uc1", {"catalog": "workspace", "schema": "odibi_logs"}
        )
        assert isinstance(result, UnityCatalogConnection)
        assert result.catalog == "workspace"
        assert result.schema == "odibi_logs"
        assert result.get_path("t") == "workspace.odibi_logs.t"

    def test_default_schema(self):
        result = create_unity_catalog_connection("uc2", {"catalog": "main"})
        assert result.schema == "default"

    def test_create_schema_flag(self):
        result = create_unity_catalog_connection(
            "uc3", {"catalog": "main", "create_schema": False}
        )
        assert result.create_schema is False

    def test_missing_catalog_raises(self):
        with pytest.raises(ValueError, match="missing 'catalog'"):
            create_unity_catalog_connection("uc4", {"schema": "s"})

    def test_missing_catalog_empty_string_raises(self):
        with pytest.raises(ValueError, match="missing 'catalog'"):
            create_unity_catalog_connection("uc5", {"catalog": ""})


# ===================================================================
# register_builtins includes unity_catalog
# ===================================================================


class TestRegisterBuiltinsIncludesUC:
    def test_unity_catalog_registered(self):
        with patch(
            "odibi.connections.factory.register_connection_factory"
        ) as mock_reg:
            from odibi.connections.factory import register_builtins

            register_builtins()
            registered_types = [
                call.args[0] for call in mock_reg.call_args_list
            ]
            assert "unity_catalog" in registered_types

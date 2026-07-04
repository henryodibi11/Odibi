"""Tests for Unity Catalog write path promotion in SparkEngine.

When a UC connection is used with ``path`` (not ``table``), the engine
must detect the dotted table name and route through ``saveAsTable()``
instead of ``.save()``.  Volume paths (starting with ``/``) must stay
as file-based writes.
"""

from unittest.mock import MagicMock

from odibi.connections.unity_catalog import UnityCatalogConnection


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_uc_connection(catalog="workspace", schema="sim_demo"):
    return UnityCatalogConnection(catalog=catalog, schema=schema, name="uc")


def _make_mock_spark_engine():
    """Return a mock SparkEngine with the real write method patched in."""
    from odibi.engine.spark_engine import SparkEngine

    engine = MagicMock(spec=SparkEngine)
    engine.spark = MagicMock()
    # Bind the real write method so the UC detection logic runs
    engine.write = SparkEngine.write.__get__(engine, SparkEngine)
    return engine


# ---------------------------------------------------------------------------
# UnityCatalogConnection.get_path unit tests
# ---------------------------------------------------------------------------


class TestUCGetPath:
    """Verify get_path resolves bare names, FQNs, and volume paths correctly."""

    def test_bare_name_qualified(self):
        conn = _make_uc_connection()
        assert conn.get_path("my_table") == "workspace.sim_demo.my_table"

    def test_already_qualified_unchanged(self):
        conn = _make_uc_connection()
        assert conn.get_path("other.schema.tbl") == "other.schema.tbl"

    def test_volume_path_unchanged(self):
        conn = _make_uc_connection()
        assert (
            conn.get_path("/Volumes/workspace/sim_demo/vol/data")
            == "/Volumes/workspace/sim_demo/vol/data"
        )


# ---------------------------------------------------------------------------
# Write routing integration tests (mock Spark, real UC detection)
# ---------------------------------------------------------------------------


class TestUCWriteRouting:
    """Ensure SparkEngine.write routes UC paths to saveAsTable."""

    def test_uc_path_promotes_to_saveAsTable(self):
        """path='sample_output' on a UC connection must call saveAsTable,
        not .save()."""
        conn = _make_uc_connection()
        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_df.rdd.getNumPartitions.return_value = 1
        mock_df.isStreaming = False

        engine = _make_mock_spark_engine()
        # _optimize_delta_write and _get_last_delta_commit_info are internal
        engine._optimize_delta_write = MagicMock()
        engine._get_last_delta_commit_info = MagicMock(return_value={"version": 1})

        engine.write(
            df=mock_df,
            connection=conn,
            format="delta",
            table=None,
            path="sample_output",
            mode="overwrite",
        )

        # Should call saveAsTable with the fully qualified name
        mock_writer.saveAsTable.assert_called_once_with("workspace.sim_demo.sample_output")
        # Should NOT call .save()
        mock_writer.save.assert_not_called()

    def test_uc_volume_path_stays_as_save(self):
        """Volume paths (starting with /) must NOT be promoted to saveAsTable."""
        conn = _make_uc_connection()
        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_df.rdd.getNumPartitions.return_value = 1
        mock_df.isStreaming = False

        engine = _make_mock_spark_engine()
        engine._optimize_delta_write = MagicMock()
        engine._get_last_delta_commit_info = MagicMock(return_value=None)

        engine.write(
            df=mock_df,
            connection=conn,
            format="delta",
            table=None,
            path="/Volumes/workspace/sim_demo/vol/output",
            mode="overwrite",
        )

        # Should call .save() with the volume path
        mock_writer.save.assert_called_once_with("/Volumes/workspace/sim_demo/vol/output")
        # Should NOT call saveAsTable
        mock_writer.saveAsTable.assert_not_called()

    def test_uc_fqn_path_promotes_to_saveAsTable(self):
        """Already-qualified name (catalog.schema.table) via path should also
        promote to saveAsTable."""
        conn = _make_uc_connection()
        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_df.rdd.getNumPartitions.return_value = 1
        mock_df.isStreaming = False

        engine = _make_mock_spark_engine()
        engine._optimize_delta_write = MagicMock()
        engine._get_last_delta_commit_info = MagicMock(return_value={"version": 1})

        engine.write(
            df=mock_df,
            connection=conn,
            format="delta",
            table=None,
            path="main.production.fact_orders",
            mode="overwrite",
        )

        mock_writer.saveAsTable.assert_called_once_with("main.production.fact_orders")
        mock_writer.save.assert_not_called()

    def test_non_uc_connection_path_uses_save(self):
        """Non-UC connections with path must still use .save()."""
        conn = MagicMock()
        conn.get_path.return_value = "/data/output/my_table"

        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_df.rdd.getNumPartitions.return_value = 1
        mock_df.isStreaming = False

        engine = _make_mock_spark_engine()
        engine._optimize_delta_write = MagicMock()
        engine._get_last_delta_commit_info = MagicMock(return_value=None)

        engine.write(
            df=mock_df,
            connection=conn,
            format="delta",
            table=None,
            path="my_table",
            mode="overwrite",
        )

        mock_writer.save.assert_called_once_with("/data/output/my_table")
        mock_writer.saveAsTable.assert_not_called()

    def test_uc_bare_table_qualified(self):
        """A bare table name (no dots) on a UC connection must be qualified
        via get_path so the configured schema is honoured (#327)."""
        conn = _make_uc_connection()
        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_df.rdd.getNumPartitions.return_value = 1
        mock_df.isStreaming = False

        engine = _make_mock_spark_engine()
        engine._optimize_delta_write = MagicMock()
        engine._get_last_delta_commit_info = MagicMock(return_value={"version": 1})

        engine.write(
            df=mock_df,
            connection=conn,
            format="delta",
            table="sim_iot_sensors",
            path=None,
            mode="overwrite",
        )

        mock_writer.saveAsTable.assert_called_once_with("workspace.sim_demo.sim_iot_sensors")
        mock_writer.save.assert_not_called()

    def test_uc_with_table_kwarg_unchanged(self):
        """When table= is already provided, the UC promotion should not run."""
        conn = _make_uc_connection()
        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_df.write = MagicMock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_df.rdd.getNumPartitions.return_value = 1
        mock_df.isStreaming = False

        engine = _make_mock_spark_engine()
        engine._optimize_delta_write = MagicMock()
        engine._get_last_delta_commit_info = MagicMock(return_value={"version": 1})

        engine.write(
            df=mock_df,
            connection=conn,
            format="delta",
            table="workspace.sim_demo.explicit_table",
            path=None,
            mode="overwrite",
        )

        mock_writer.saveAsTable.assert_called_once_with("workspace.sim_demo.explicit_table")
        mock_writer.save.assert_not_called()

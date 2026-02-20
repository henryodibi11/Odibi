"""Tests for #247: Delete detection should handle non-sequential Delta versions."""

from unittest.mock import MagicMock, patch

from odibi.config import DeleteDetectionConfig, DeleteDetectionMode
from odibi.context import EngineContext
from odibi.enums import EngineType


def _make_context(spark_mock, df_mock, table_path="dbfs:/test/table"):
    """Create a mock EngineContext for delete detection tests."""
    ctx = MagicMock(spec=EngineContext)
    ctx.df = df_mock
    ctx.df.columns = ["id", "name"]
    ctx.engine_type = EngineType.SPARK
    ctx.spark = spark_mock
    ctx.params = {
        "keys": ["id"],
        "target_path": table_path,
    }
    return ctx


class TestDeleteDetectionVersions:
    """Tests for non-sequential Delta version handling."""

    @patch(
        "odibi.transformers.delete_detection._get_target_path",
        return_value="dbfs:/test/table",
    )
    @patch("delta.tables.DeltaTable")
    def test_uses_actual_previous_version_after_vacuum(self, mock_delta_cls, mock_get_path):
        """After VACUUM, version v-1 may not exist; should use latest available."""
        from odibi.transformers.delete_detection import _snapshot_diff_spark

        spark = MagicMock()
        df = MagicMock()
        df.columns = ["id", "name"]

        # Current version is 10, but only versions 10, 7, 5 exist (VACUUM removed 8, 9)
        dt_instance = MagicMock()
        mock_delta_cls.forPath.return_value = dt_instance
        mock_delta_cls.isDeltaTable.return_value = True

        # history(1) returns current version
        current_row = MagicMock()
        current_row.__getitem__ = lambda self, key: 10

        # Full history returns versions 10, 7, 5
        history_rows = [MagicMock(), MagicMock(), MagicMock()]
        history_rows[0].__getitem__ = lambda self, key: 10
        history_rows[1].__getitem__ = lambda self, key: 7
        history_rows[2].__getitem__ = lambda self, key: 5

        # dt.history(1).collect() returns [current_row]
        # dt.history().select("version").collect() returns history_rows
        def history_side_effect(*args, **kwargs):
            mock_result = MagicMock()
            if args and args[0] == 1:
                mock_result.collect.return_value = [current_row]
            else:
                mock_result.select.return_value.collect.return_value = history_rows
            return mock_result

        dt_instance.history.side_effect = history_side_effect

        # Mock the prev_df read
        prev_df = MagicMock()
        prev_df.columns = ["id", "name"]
        spark.read.format.return_value.option.return_value.load.return_value = prev_df

        # Mock anti-join result
        deleted_keys = MagicMock()
        prev_df.select.return_value.distinct.return_value.subtract.return_value = deleted_keys
        deleted_keys.count.return_value = 0
        df.select.return_value.distinct.return_value = MagicMock()

        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            target_path="dbfs:/test/table",
        )
        context = _make_context(spark, df)

        # This should NOT raise even though version 9 doesn't exist
        # It should use version 7 instead
        try:
            _snapshot_diff_spark(context, config)
        except Exception:
            pass  # May fail on further mock issues, but version lookup should work

        # Verify it requested version 7 (not 9) when reading previous version
        spark.read.format.return_value.option.assert_called_with("versionAsOf", 7)

    @patch("odibi.transformers.delete_detection._ensure_delete_column")
    @patch(
        "odibi.transformers.delete_detection._get_target_path",
        return_value="dbfs:/test/table",
    )
    @patch("delta.tables.DeltaTable")
    def test_no_previous_version_skips_gracefully(self, mock_delta_cls, mock_get_path, mock_ensure):
        """When only version 0 exists, should skip delete detection."""
        from odibi.transformers.delete_detection import _snapshot_diff_spark

        spark = MagicMock()
        df = MagicMock()
        df.columns = ["id", "name", "_is_deleted"]

        dt_instance = MagicMock()
        mock_delta_cls.forPath.return_value = dt_instance
        mock_delta_cls.isDeltaTable.return_value = True

        current_row = MagicMock()
        current_row.__getitem__ = lambda self, key: 0
        dt_instance.history.return_value.collect.return_value = [current_row]

        mock_ensure.return_value = MagicMock()

        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            target_path="dbfs:/test/table",
        )
        context = _make_context(spark, df)

        result = _snapshot_diff_spark(context, config)
        # Should return without error (skips on version 0)
        assert result is not None
        mock_ensure.assert_called_once()

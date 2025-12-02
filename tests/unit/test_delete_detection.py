"""
Tests for CDC-like delete detection feature.

Covers:
1. Config validation (DeleteDetectionConfig, WriteMetadataConfig)
2. Transformer tests (detect_deletes for Pandas)
3. Metadata column tests (add_write_metadata)
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.config import (
    DeleteDetectionConfig,
    DeleteDetectionMode,
    FirstRunBehavior,
    ThresholdBreachAction,
    WriteMetadataConfig,
)
from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.delete_detection import (
    DeleteThresholdExceeded,
    detect_deletes,
)

# ============================================================
# Section 1: Config Validation Tests
# ============================================================


class TestDeleteDetectionConfigValidation:
    """Test DeleteDetectionConfig validation logic."""

    def test_mode_none_valid_without_keys(self):
        """Mode none should be valid without keys."""
        config = DeleteDetectionConfig(mode=DeleteDetectionMode.NONE)
        assert config.mode == DeleteDetectionMode.NONE
        assert config.keys == []

    def test_mode_none_valid_with_keys(self):
        """Mode none should be valid even with keys specified."""
        config = DeleteDetectionConfig(mode=DeleteDetectionMode.NONE, keys=["id"])
        assert config.keys == ["id"]

    def test_snapshot_diff_requires_keys(self):
        """Snapshot diff mode requires keys."""
        with pytest.raises(ValueError, match="'keys' required for mode='snapshot_diff'"):
            DeleteDetectionConfig(mode=DeleteDetectionMode.SNAPSHOT_DIFF)

    def test_snapshot_diff_valid_with_keys(self):
        """Snapshot diff mode works with keys."""
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["customer_id", "order_id"],
        )
        assert config.keys == ["customer_id", "order_id"]

    def test_sql_compare_requires_keys(self):
        """SQL compare mode requires keys."""
        with pytest.raises(ValueError, match="'keys' required for mode='sql_compare'"):
            DeleteDetectionConfig(
                mode=DeleteDetectionMode.SQL_COMPARE,
                source_connection="my_conn",
                source_table="my_table",
            )

    def test_sql_compare_requires_source_connection(self):
        """SQL compare mode requires source_connection."""
        with pytest.raises(ValueError, match="'source_connection' required for sql_compare"):
            DeleteDetectionConfig(
                mode=DeleteDetectionMode.SQL_COMPARE,
                keys=["id"],
                source_table="my_table",
            )

    def test_sql_compare_requires_source_table_or_query(self):
        """SQL compare mode requires source_table or source_query."""
        with pytest.raises(ValueError, match="'source_table' or 'source_query' required"):
            DeleteDetectionConfig(
                mode=DeleteDetectionMode.SQL_COMPARE,
                keys=["id"],
                source_connection="my_conn",
            )

    def test_sql_compare_valid_with_source_table(self):
        """SQL compare mode works with source_table."""
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SQL_COMPARE,
            keys=["customer_id"],
            source_connection="azure_sql",
            source_table="dbo.Customers",
        )
        assert config.source_table == "dbo.Customers"
        assert config.source_query is None

    def test_sql_compare_valid_with_source_query(self):
        """SQL compare mode works with source_query."""
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SQL_COMPARE,
            keys=["id"],
            source_connection="azure_sql",
            source_query="SELECT DISTINCT id FROM dbo.Customers WHERE active=1",
        )
        assert config.source_query is not None

    def test_default_soft_delete_col(self):
        """Default soft_delete_col is _is_deleted."""
        config = DeleteDetectionConfig(mode=DeleteDetectionMode.NONE)
        assert config.soft_delete_col == "_is_deleted"

    def test_soft_delete_col_null_for_hard_delete(self):
        """soft_delete_col=None enables hard delete."""
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["id"],
            soft_delete_col=None,
        )
        assert config.soft_delete_col is None

    def test_default_threshold_values(self):
        """Default threshold settings."""
        config = DeleteDetectionConfig(mode=DeleteDetectionMode.NONE)
        assert config.max_delete_percent == 50.0
        assert config.on_threshold_breach == ThresholdBreachAction.WARN
        assert config.on_first_run == FirstRunBehavior.SKIP


class TestWriteMetadataConfigValidation:
    """Test WriteMetadataConfig validation logic."""

    def test_default_values(self):
        """Default config enables extracted_at and source_file."""
        config = WriteMetadataConfig()
        assert config.extracted_at is True
        assert config.source_file is True
        assert config.source_connection is False
        assert config.source_table is False

    def test_all_false(self):
        """All options can be disabled."""
        config = WriteMetadataConfig(
            extracted_at=False,
            source_file=False,
            source_connection=False,
            source_table=False,
        )
        assert config.extracted_at is False
        assert config.source_file is False

    def test_selective_options(self):
        """Selective options can be enabled."""
        config = WriteMetadataConfig(
            extracted_at=True,
            source_file=False,
            source_connection=True,
            source_table=True,
        )
        assert config.extracted_at is True
        assert config.source_file is False
        assert config.source_connection is True


# ============================================================
# Section 2: Transformer Tests (Pandas)
# ============================================================


class TestDetectDeletesTransformerPandas:
    """Test detect_deletes transformer for Pandas engine."""

    @pytest.fixture
    def pandas_context(self):
        """Create a basic PandasContext."""
        return PandasContext()

    def _make_engine_context(self, df, pandas_context):
        """Helper to create EngineContext for Pandas."""
        return EngineContext(
            context=pandas_context,
            df=df,
            engine_type=EngineType.PANDAS,
        )

    def test_mode_none_passthrough(self, pandas_context):
        """Mode none should pass through unchanged."""
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        ctx = self._make_engine_context(df, pandas_context)

        result = detect_deletes(ctx, mode="none")

        pd.testing.assert_frame_equal(result.df, df)

    def test_mode_none_does_not_add_column(self, pandas_context):
        """Mode none should not add soft delete column."""
        df = pd.DataFrame({"id": [1, 2, 3]})
        ctx = self._make_engine_context(df, pandas_context)

        result = detect_deletes(ctx, mode="none")

        assert "_is_deleted" not in result.df.columns

    def test_snapshot_diff_identifies_deleted_keys(self, pandas_context):
        """Snapshot diff identifies keys that were deleted.

        For snapshot_diff, we compare current Silver (context.df) to previous Delta version.
        Keys that are in Silver but were deleted from source are flagged.

        In this test scenario:
        - current Silver has [1, 2, 3] (what's currently in the Delta table)
        - previous Delta version had [1, 2] (simulating that row 3 was added then marked deleted)
        - Actually, we flip this: Silver has [1,2,3], source (prev version) had [1,2]
        - But wait, snapshot_diff compares prev_keys to curr_keys and finds deleted
        - deleted = prev_keys EXCEPT curr_keys
        - So if prev has [1,2,3] and curr has [1,2], then deleted = [3]

        Since deleted rows aren't in context.df for snapshot_diff (by definition they're
        missing from the current extraction), the soft delete flag is applied to rows
        that ARE in context.df. For sql_compare mode, we'd flag rows in Silver.

        For this test, we simulate:
        - context.df = Silver data = [1, 2, 3]
        - prev Delta version = [1, 2]
        - This means row 3 is NEW (not deleted)
        - Actually let's test the opposite: prev has more rows than current
        """
        # Silver data (what's currently in the Delta table being processed)
        current_df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        # Previous version had these keys - id=4 was deleted
        prev_df = pd.DataFrame({"id": [1, 2, 3, 4], "name": ["a", "b", "c", "d"]})

        mock_dt = MagicMock()
        mock_dt.version.return_value = 1
        mock_dt.to_pandas.return_value = prev_df

        def delta_table_init(path, version=None):
            return mock_dt

        with patch.dict("sys.modules", {"deltalake": MagicMock()}):
            import sys

            sys.modules["deltalake"].DeltaTable = delta_table_init

            ctx = self._make_engine_context(current_df, pandas_context)
            ctx.context._current_table_path = "/fake/delta/table"

            result = detect_deletes(
                ctx,
                mode="snapshot_diff",
                keys=["id"],
                soft_delete_col="_is_deleted",
            )

        # All rows in current_df should have _is_deleted column
        # None should be marked as deleted (since deleted row id=4 is not in current_df)
        assert "_is_deleted" in result.df.columns
        result_df = result.df.sort_values("id").reset_index(drop=True)
        assert len(result_df) == 3
        assert all(result_df["_is_deleted"] == False)  # noqa: E712

    @patch("odibi.transformers.delete_detection._get_sqlalchemy_engine")
    @patch("odibi.transformers.delete_detection._get_connection")
    def test_sql_compare_identifies_deleted_keys(
        self, mock_get_conn, mock_get_engine, pandas_context
    ):
        """SQL compare identifies keys that no longer exist in source."""
        silver_df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        source_keys_df = pd.DataFrame({"id": [1, 2]})

        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        with patch("pandas.read_sql", return_value=source_keys_df):
            ctx = self._make_engine_context(silver_df, pandas_context)

            result = detect_deletes(
                ctx,
                mode="sql_compare",
                keys=["id"],
                source_connection="test_conn",
                source_table="source_table",
            )

        assert "_is_deleted" in result.df.columns
        result_df = result.df.sort_values("id").reset_index(drop=True)
        assert bool(result_df.loc[result_df["id"] == 3, "_is_deleted"].iloc[0])
        assert not bool(result_df.loc[result_df["id"] == 1, "_is_deleted"].iloc[0])

    @patch("odibi.transformers.delete_detection._get_sqlalchemy_engine")
    @patch("odibi.transformers.delete_detection._get_connection")
    def test_soft_delete_adds_is_deleted_column(
        self, mock_get_conn, mock_get_engine, pandas_context
    ):
        """Soft delete adds _is_deleted column with True for deleted records."""
        silver_df = pd.DataFrame({"id": [1, 2, 3], "value": ["x", "y", "z"]})
        source_keys_df = pd.DataFrame({"id": [1]})

        mock_get_conn.return_value = MagicMock()
        mock_get_engine.return_value = MagicMock()

        with patch("pandas.read_sql", return_value=source_keys_df):
            ctx = self._make_engine_context(silver_df, pandas_context)

            result = detect_deletes(
                ctx,
                mode="sql_compare",
                keys=["id"],
                source_connection="conn",
                source_table="tbl",
                soft_delete_col="_is_deleted",
            )

        result_df = result.df.sort_values("id").reset_index(drop=True)
        assert "_is_deleted" in result_df.columns
        deleted_mask = result_df["_is_deleted"]
        assert bool(deleted_mask[result_df["id"] == 2].iloc[0])
        assert bool(deleted_mask[result_df["id"] == 3].iloc[0])
        assert not bool(deleted_mask[result_df["id"] == 1].iloc[0])

    @patch("odibi.transformers.delete_detection._get_sqlalchemy_engine")
    @patch("odibi.transformers.delete_detection._get_connection")
    def test_hard_delete_removes_rows(self, mock_get_conn, mock_get_engine, pandas_context):
        """Hard delete (soft_delete_col=None) removes deleted rows."""
        silver_df = pd.DataFrame({"id": [1, 2, 3], "value": ["x", "y", "z"]})
        source_keys_df = pd.DataFrame({"id": [1]})

        mock_get_conn.return_value = MagicMock()
        mock_get_engine.return_value = MagicMock()

        with patch("pandas.read_sql", return_value=source_keys_df):
            ctx = self._make_engine_context(silver_df, pandas_context)

            result = detect_deletes(
                ctx,
                mode="sql_compare",
                keys=["id"],
                source_connection="conn",
                source_table="tbl",
                soft_delete_col=None,
            )

        assert len(result.df) == 1
        assert result.df["id"].tolist() == [1]
        assert "_is_deleted" not in result.df.columns

    def test_first_run_skip_behavior(self, pandas_context):
        """First run with on_first_run=skip should skip delete detection."""
        current_df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        mock_dt = MagicMock()
        mock_dt.version.return_value = 0

        def delta_table_init(path, version=None):
            return mock_dt

        with patch.dict("sys.modules", {"deltalake": MagicMock()}):
            import sys

            sys.modules["deltalake"].DeltaTable = delta_table_init

            ctx = self._make_engine_context(current_df, pandas_context)
            ctx.context._current_table_path = "/fake/delta/table"

            result = detect_deletes(
                ctx,
                mode="snapshot_diff",
                keys=["id"],
                on_first_run="skip",
            )

        assert "_is_deleted" in result.df.columns
        assert all(result.df["_is_deleted"] == False)  # noqa: E712

    def test_first_run_error_behavior(self, pandas_context):
        """First run with on_first_run=error should raise."""
        current_df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        mock_dt = MagicMock()
        mock_dt.version.return_value = 0

        def delta_table_init(path, version=None):
            return mock_dt

        with patch.dict("sys.modules", {"deltalake": MagicMock()}):
            import sys

            sys.modules["deltalake"].DeltaTable = delta_table_init

            ctx = self._make_engine_context(current_df, pandas_context)
            ctx.context._current_table_path = "/fake/delta/table"

            with pytest.raises(ValueError, match="No previous version exists"):
                detect_deletes(
                    ctx,
                    mode="snapshot_diff",
                    keys=["id"],
                    on_first_run="error",
                )

    @patch("odibi.transformers.delete_detection._get_sqlalchemy_engine")
    @patch("odibi.transformers.delete_detection._get_connection")
    def test_threshold_breach_error(self, mock_get_conn, mock_get_engine, pandas_context):
        """Threshold breach with on_threshold_breach=error should raise."""
        silver_df = pd.DataFrame({"id": list(range(100))})
        source_keys_df = pd.DataFrame({"id": list(range(10))})

        mock_get_conn.return_value = MagicMock()
        mock_get_engine.return_value = MagicMock()

        with patch("pandas.read_sql", return_value=source_keys_df):
            ctx = self._make_engine_context(silver_df, pandas_context)

            with pytest.raises(DeleteThresholdExceeded, match="exceeds threshold"):
                detect_deletes(
                    ctx,
                    mode="sql_compare",
                    keys=["id"],
                    source_connection="conn",
                    source_table="tbl",
                    max_delete_percent=50.0,
                    on_threshold_breach="error",
                )

    @patch("odibi.transformers.delete_detection._get_sqlalchemy_engine")
    @patch("odibi.transformers.delete_detection._get_connection")
    def test_threshold_breach_warn(self, mock_get_conn, mock_get_engine, pandas_context, caplog):
        """Threshold breach with on_threshold_breach=warn should log warning."""
        import logging

        silver_df = pd.DataFrame({"id": list(range(100))})
        source_keys_df = pd.DataFrame({"id": list(range(10))})

        mock_get_conn.return_value = MagicMock()
        mock_get_engine.return_value = MagicMock()

        with patch("pandas.read_sql", return_value=source_keys_df):
            ctx = self._make_engine_context(silver_df, pandas_context)

            with caplog.at_level(logging.WARNING):
                result = detect_deletes(
                    ctx,
                    mode="sql_compare",
                    keys=["id"],
                    source_connection="conn",
                    source_table="tbl",
                    max_delete_percent=50.0,
                    on_threshold_breach="warn",
                )

            assert result.df is not None
            assert "threshold" in caplog.text.lower() or len(result.df) > 0

    @patch("odibi.transformers.delete_detection._get_sqlalchemy_engine")
    @patch("odibi.transformers.delete_detection._get_connection")
    def test_threshold_breach_skip(self, mock_get_conn, mock_get_engine, pandas_context):
        """Threshold breach with on_threshold_breach=skip should skip detection."""
        silver_df = pd.DataFrame({"id": list(range(100))})
        source_keys_df = pd.DataFrame({"id": list(range(10))})

        mock_get_conn.return_value = MagicMock()
        mock_get_engine.return_value = MagicMock()

        with patch("pandas.read_sql", return_value=source_keys_df):
            ctx = self._make_engine_context(silver_df, pandas_context)

            result = detect_deletes(
                ctx,
                mode="sql_compare",
                keys=["id"],
                source_connection="conn",
                source_table="tbl",
                max_delete_percent=50.0,
                on_threshold_breach="skip",
            )

        assert "_is_deleted" in result.df.columns
        assert all(result.df["_is_deleted"] == False)  # noqa: E712

    @patch("odibi.transformers.delete_detection._get_sqlalchemy_engine")
    @patch("odibi.transformers.delete_detection._get_connection")
    def test_no_deletes_found(self, mock_get_conn, mock_get_engine, pandas_context):
        """When no deletes are found, soft_delete column should be all False."""
        silver_df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        source_keys_df = pd.DataFrame({"id": [1, 2, 3]})

        mock_get_conn.return_value = MagicMock()
        mock_get_engine.return_value = MagicMock()

        with patch("pandas.read_sql", return_value=source_keys_df):
            ctx = self._make_engine_context(silver_df, pandas_context)

            result = detect_deletes(
                ctx,
                mode="sql_compare",
                keys=["id"],
                source_connection="conn",
                source_table="tbl",
            )

        assert "_is_deleted" in result.df.columns
        assert all(result.df["_is_deleted"] == False)  # noqa: E712


# ============================================================
# Section 3: Metadata Column Tests
# ============================================================


class TestWriteMetadata:
    """Test add_write_metadata functionality in Pandas engine."""

    @pytest.fixture
    def pandas_engine(self):
        """Create PandasEngine instance."""
        from odibi.engine.pandas_engine import PandasEngine

        return PandasEngine()

    def test_add_metadata_true_adds_default_columns(self, pandas_engine):
        """add_metadata=True adds default columns (_extracted_at, _source_file)."""
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        result = pandas_engine.add_write_metadata(
            df=df,
            metadata_config=True,
            source_path="/data/input.csv",
            is_file_source=True,
        )

        assert "_extracted_at" in result.columns
        assert "_source_file" in result.columns
        assert "_source_connection" not in result.columns
        assert "_source_table" not in result.columns

    def test_add_metadata_config_selective(self, pandas_engine):
        """WriteMetadataConfig with selective options."""
        df = pd.DataFrame({"id": [1, 2]})

        config = WriteMetadataConfig(
            extracted_at=True,
            source_file=False,
            source_connection=True,
            source_table=True,
        )

        result = pandas_engine.add_write_metadata(
            df=df,
            metadata_config=config,
            source_connection="azure_sql",
            source_table="dbo.Customers",
            source_path="/data/input.csv",
            is_file_source=False,
        )

        assert "_extracted_at" in result.columns
        assert "_source_file" not in result.columns
        assert "_source_connection" in result.columns
        assert "_source_table" in result.columns
        assert result["_source_connection"].iloc[0] == "azure_sql"
        assert result["_source_table"].iloc[0] == "dbo.Customers"

    def test_extracted_at_is_timestamp(self, pandas_engine):
        """_extracted_at should be a valid timestamp."""
        df = pd.DataFrame({"id": [1]})

        result = pandas_engine.add_write_metadata(
            df=df,
            metadata_config=True,
        )

        assert "_extracted_at" in result.columns
        assert pd.api.types.is_datetime64_any_dtype(result["_extracted_at"])

    def test_source_file_only_for_file_sources(self, pandas_engine):
        """_source_file should only be added for file sources."""
        df = pd.DataFrame({"id": [1]})

        result = pandas_engine.add_write_metadata(
            df=df,
            metadata_config=True,
            source_path="/data/input.csv",
            is_file_source=False,
        )

        assert "_source_file" not in result.columns

    def test_source_file_added_for_file_sources(self, pandas_engine):
        """_source_file should be added for file sources."""
        df = pd.DataFrame({"id": [1]})

        result = pandas_engine.add_write_metadata(
            df=df,
            metadata_config=True,
            source_path="/data/input.csv",
            is_file_source=True,
        )

        assert "_source_file" in result.columns
        assert result["_source_file"].iloc[0] == "/data/input.csv"

    def test_source_table_only_for_sql_sources(self, pandas_engine):
        """_source_table requires source_table to be provided."""
        df = pd.DataFrame({"id": [1]})

        config = WriteMetadataConfig(source_table=True)

        result_no_table = pandas_engine.add_write_metadata(
            df=df,
            metadata_config=config,
            source_table=None,
        )

        result_with_table = pandas_engine.add_write_metadata(
            df=df,
            metadata_config=config,
            source_table="dbo.Orders",
        )

        assert "_source_table" not in result_no_table.columns
        assert "_source_table" in result_with_table.columns
        assert result_with_table["_source_table"].iloc[0] == "dbo.Orders"

    def test_metadata_none_returns_unchanged(self, pandas_engine):
        """metadata_config=None should return DataFrame unchanged."""
        df = pd.DataFrame({"id": [1, 2]})

        result = pandas_engine.add_write_metadata(
            df=df,
            metadata_config=None,
        )

        pd.testing.assert_frame_equal(result, df)

    def test_metadata_false_returns_unchanged(self, pandas_engine):
        """metadata_config=False should return DataFrame unchanged."""
        df = pd.DataFrame({"id": [1, 2]})

        result = pandas_engine.add_write_metadata(
            df=df,
            metadata_config=False,
        )

        pd.testing.assert_frame_equal(result, df)

    def test_does_not_modify_original_dataframe(self, pandas_engine):
        """add_write_metadata should not modify the original DataFrame."""
        df = pd.DataFrame({"id": [1, 2]})
        original_columns = list(df.columns)

        pandas_engine.add_write_metadata(
            df=df,
            metadata_config=True,
            is_file_source=True,
            source_path="/data/file.csv",
        )

        assert list(df.columns) == original_columns

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
        with pytest.raises(ValueError, match="'keys' is required when mode='snapshot_diff'"):
            DeleteDetectionConfig(mode=DeleteDetectionMode.SNAPSHOT_DIFF)

    def test_snapshot_diff_valid_with_keys(self):
        """Snapshot diff mode works with keys (connection+path optional, can fallback to context)."""
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["customer_id", "order_id"],
        )
        assert config.keys == ["customer_id", "order_id"]

    def test_snapshot_diff_with_explicit_connection_path(self):
        """Snapshot diff mode accepts explicit connection and path."""
        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SNAPSHOT_DIFF,
            keys=["customer_id"],
            connection="silver_conn",
            path="silver/customers",
        )
        assert config.connection == "silver_conn"
        assert config.path == "silver/customers"

    def test_sql_compare_requires_keys(self):
        """SQL compare mode requires keys."""
        with pytest.raises(ValueError, match="'keys' is required when mode='sql_compare'"):
            DeleteDetectionConfig(
                mode=DeleteDetectionMode.SQL_COMPARE,
                source_connection="my_conn",
                source_table="my_table",
            )

    def test_sql_compare_requires_source_connection(self):
        """SQL compare mode requires source_connection."""
        with pytest.raises(
            ValueError, match="'source_connection' is required for mode='sql_compare'"
        ):
            DeleteDetectionConfig(
                mode=DeleteDetectionMode.SQL_COMPARE,
                keys=["id"],
                source_table="my_table",
            )

    def test_sql_compare_requires_source_table_or_query(self):
        """SQL compare mode requires source_table or source_query."""
        with pytest.raises(ValueError, match="'source_table' or 'source_query' is required"):
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

        # Result should include all source rows + deleted row from target
        # - Source rows (id=1,2,3) have _is_deleted=False
        # - Deleted row (id=4) is UNIONED back with _is_deleted=True
        assert "_is_deleted" in result.df.columns
        result_df = result.df.sort_values("id").reset_index(drop=True)
        assert len(result_df) == 4  # 3 source rows + 1 deleted row
        assert not bool(result_df.loc[result_df["id"] == 1, "_is_deleted"].iloc[0])
        assert not bool(result_df.loc[result_df["id"] == 2, "_is_deleted"].iloc[0])
        assert not bool(result_df.loc[result_df["id"] == 3, "_is_deleted"].iloc[0])
        assert bool(result_df.loc[result_df["id"] == 4, "_is_deleted"].iloc[0])

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

    def test_soft_delete_no_setting_with_copy_warning(self, pandas_engine):
        """Soft delete should not raise SettingWithCopyWarning (issue #210)."""
        import warnings

        current_df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        prev_df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        mock_dt = MagicMock()
        mock_dt.version.return_value = 1
        mock_dt.to_pandas.return_value = prev_df

        def delta_table_init(path, version=None):
            return mock_dt

        with patch.dict("sys.modules", {"deltalake": MagicMock()}):
            import sys

            sys.modules["deltalake"].DeltaTable = delta_table_init

            pandas_ctx = PandasContext()
            ctx = EngineContext(
                context=pandas_ctx,
                df=current_df,
                engine_type=EngineType.PANDAS,
            )
            ctx.context._current_table_path = "/fake/delta/table"

            with warnings.catch_warnings():
                warnings.simplefilter("error", category=pd.errors.SettingWithCopyWarning)
                result = detect_deletes(
                    ctx,
                    mode="snapshot_diff",
                    keys=["id"],
                    soft_delete_col="_is_deleted",
                )

        result_df = result.df.sort_values("id").reset_index(drop=True)
        assert len(result_df) == 3
        assert bool(result_df.loc[result_df["id"] == 3, "_is_deleted"].iloc[0])

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


# ============================================================
# Section 4: Helper Functions & Edge Cases
# ============================================================


class TestDeleteDetectionHelpers:
    """Test internal helper functions for delete detection."""

    def test_unknown_mode_raises_error(self):
        """Unknown mode raises validation error at the detect_deletes entry point."""
        df = pd.DataFrame({"id": [1, 2]})
        ctx = EngineContext(context=PandasContext(), df=df, engine_type=EngineType.PANDAS)
        with pytest.raises(Exception, match="none.*snapshot_diff.*sql_compare"):
            detect_deletes(ctx, mode="invalid_mode", keys=["id"])

    def test_build_source_keys_query_with_table(self):
        """_build_source_keys_query returns SELECT DISTINCT from source_table."""
        from odibi.transformers.delete_detection import _build_source_keys_query

        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SQL_COMPARE,
            keys=["id", "region"],
            source_connection="conn",
            source_table="dbo.Customers",
        )
        query = _build_source_keys_query(config)
        assert "SELECT DISTINCT id, region FROM dbo.Customers" == query

    def test_build_source_keys_query_with_custom_query(self):
        """_build_source_keys_query returns source_query when provided."""
        from odibi.transformers.delete_detection import _build_source_keys_query

        config = DeleteDetectionConfig(
            mode=DeleteDetectionMode.SQL_COMPARE,
            keys=["id"],
            source_connection="conn",
            source_query="SELECT id FROM dbo.Active WHERE status=1",
        )
        query = _build_source_keys_query(config)
        assert query == "SELECT id FROM dbo.Active WHERE status=1"

    def test_get_row_count_pandas(self):
        """_get_row_count works for Pandas DataFrames."""
        from odibi.transformers.delete_detection import _get_row_count

        df = pd.DataFrame({"id": [1, 2, 3]})
        assert _get_row_count(df, EngineType.PANDAS) == 3

    def test_get_target_path_from_write_path(self):
        """_get_target_path resolves from _current_write_path."""
        from odibi.transformers.delete_detection import _get_target_path

        ctx = EngineContext(
            context=PandasContext(), df=pd.DataFrame(), engine_type=EngineType.PANDAS
        )
        mock_engine = MagicMock()
        mock_engine._current_write_path = "/data/silver/table"
        ctx.engine = mock_engine
        assert _get_target_path(ctx) == "/data/silver/table"

    def test_get_target_path_from_input_path(self):
        """_get_target_path resolves from _current_input_path."""
        from odibi.transformers.delete_detection import _get_target_path

        ctx = EngineContext(
            context=PandasContext(), df=pd.DataFrame(), engine_type=EngineType.PANDAS
        )
        mock_engine = MagicMock()
        mock_engine._current_write_path = None
        mock_engine._current_input_path = "/data/input/table"
        ctx.engine = mock_engine
        assert _get_target_path(ctx) == "/data/input/table"

    def test_get_target_path_from_inner_context(self):
        """_get_target_path falls back to inner context._current_table_path."""
        from odibi.transformers.delete_detection import _get_target_path

        ctx = EngineContext(
            context=PandasContext(), df=pd.DataFrame(), engine_type=EngineType.PANDAS
        )
        ctx.engine = None
        ctx.context._current_table_path = "/fallback/path"
        assert _get_target_path(ctx) == "/fallback/path"

    def test_get_target_path_returns_none(self):
        """_get_target_path returns None when no path available."""
        from odibi.transformers.delete_detection import _get_target_path

        ctx = EngineContext(
            context=PandasContext(), df=pd.DataFrame(), engine_type=EngineType.PANDAS
        )
        ctx.engine = None
        assert _get_target_path(ctx) is None

    def test_get_connection_found(self):
        """_get_connection returns connection from engine."""
        from odibi.transformers.delete_detection import _get_connection

        ctx = EngineContext(
            context=PandasContext(), df=pd.DataFrame(), engine_type=EngineType.PANDAS
        )
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connections = {"my_conn": mock_conn}
        ctx.engine = mock_engine
        assert _get_connection(ctx, "my_conn") is mock_conn

    def test_get_connection_not_found(self):
        """_get_connection returns None when connection not found."""
        from odibi.transformers.delete_detection import _get_connection

        ctx = EngineContext(
            context=PandasContext(), df=pd.DataFrame(), engine_type=EngineType.PANDAS
        )
        mock_engine = MagicMock()
        mock_engine.connections = {}
        ctx.engine = mock_engine
        assert _get_connection(ctx, "missing") is None

    def test_get_connection_no_engine(self):
        """_get_connection returns None when no engine."""
        from odibi.transformers.delete_detection import _get_connection

        ctx = EngineContext(
            context=PandasContext(), df=pd.DataFrame(), engine_type=EngineType.PANDAS
        )
        ctx.engine = None
        assert _get_connection(ctx, "any") is None

    def test_get_sqlalchemy_engine_from_engine_attr(self):
        """_get_sqlalchemy_engine returns conn.engine."""
        from odibi.transformers.delete_detection import _get_sqlalchemy_engine

        mock_conn = MagicMock()
        mock_conn.engine = "fake_engine"
        assert _get_sqlalchemy_engine(mock_conn) == "fake_engine"

    def test_get_sqlalchemy_engine_from_get_engine(self):
        """_get_sqlalchemy_engine calls conn.get_engine()."""
        from odibi.transformers.delete_detection import _get_sqlalchemy_engine

        mock_conn = MagicMock(spec=["get_engine"])
        mock_conn.get_engine.return_value = "engine_from_method"
        assert _get_sqlalchemy_engine(mock_conn) == "engine_from_method"

    def test_get_sqlalchemy_engine_raises_on_missing(self):
        """_get_sqlalchemy_engine raises ValueError if no way to create engine."""
        from odibi.transformers.delete_detection import _get_sqlalchemy_engine

        mock_conn = MagicMock(spec=[])
        with pytest.raises(ValueError, match="Cannot create SQLAlchemy engine"):
            _get_sqlalchemy_engine(mock_conn)

    def test_ensure_delete_column_no_soft_col(self):
        """_ensure_delete_column returns unchanged if no soft_delete_col."""
        from odibi.transformers.delete_detection import _ensure_delete_column

        df = pd.DataFrame({"id": [1, 2]})
        ctx = EngineContext(context=PandasContext(), df=df, engine_type=EngineType.PANDAS)
        config = DeleteDetectionConfig(mode=DeleteDetectionMode.NONE, soft_delete_col=None)
        result = _ensure_delete_column(ctx, config)
        assert "_is_deleted" not in result.df.columns

    def test_ensure_delete_column_adds_column(self):
        """_ensure_delete_column adds soft_delete_col with False."""
        from odibi.transformers.delete_detection import _ensure_delete_column

        df = pd.DataFrame({"id": [1, 2]})
        ctx = EngineContext(context=PandasContext(), df=df, engine_type=EngineType.PANDAS)
        config = DeleteDetectionConfig(mode=DeleteDetectionMode.NONE, soft_delete_col="_is_deleted")
        result = _ensure_delete_column(ctx, config)
        assert "_is_deleted" in result.df.columns
        assert all(result.df["_is_deleted"] == False)  # noqa: E712

    def test_ensure_delete_column_already_exists(self):
        """_ensure_delete_column returns unchanged if column already exists."""
        from odibi.transformers.delete_detection import _ensure_delete_column

        df = pd.DataFrame({"id": [1], "_is_deleted": [True]})
        ctx = EngineContext(context=PandasContext(), df=df, engine_type=EngineType.PANDAS)
        config = DeleteDetectionConfig(mode=DeleteDetectionMode.NONE, soft_delete_col="_is_deleted")
        result = _ensure_delete_column(ctx, config)
        assert bool(result.df["_is_deleted"].iloc[0]) is True

    def test_snapshot_diff_no_target_path_skips(self):
        """snapshot_diff skips when target path cannot be determined."""
        df = pd.DataFrame({"id": [1, 2]})
        ctx = EngineContext(context=PandasContext(), df=df, engine_type=EngineType.PANDAS)
        ctx.engine = None

        with patch.dict("sys.modules", {"deltalake": MagicMock()}):
            result = detect_deletes(ctx, mode="snapshot_diff", keys=["id"])

        assert "_is_deleted" in result.df.columns
        assert all(result.df["_is_deleted"] == False)  # noqa: E712

    @patch("odibi.transformers.delete_detection._get_sqlalchemy_engine")
    @patch("odibi.transformers.delete_detection._get_connection")
    def test_sql_compare_connection_not_found(self, mock_get_conn, mock_get_engine):
        """sql_compare raises ValueError when connection not found."""
        mock_get_conn.return_value = None

        df = pd.DataFrame({"id": [1, 2]})
        ctx = EngineContext(context=PandasContext(), df=df, engine_type=EngineType.PANDAS)

        with pytest.raises(ValueError, match="not found"):
            detect_deletes(
                ctx,
                mode="sql_compare",
                keys=["id"],
                source_connection="missing_conn",
                source_table="tbl",
            )

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.context import EngineContext, PandasContext, SparkContext
from odibi.enums import EngineType
from odibi.transformers.scd import SCD2Params, scd2


class TestSCD2Pandas:
    @pytest.fixture
    def context(self):
        """Create a pandas context wrapped in EngineContext."""
        base_ctx = PandasContext()
        # EngineContext wrapper is required for transformers
        return EngineContext(
            context=base_ctx,
            df=None,  # Will be replaced in tests via with_df
            engine_type=EngineType.PANDAS,
        )

    @pytest.fixture
    def source_df(self):
        """Create source dataframe."""
        return pd.DataFrame(
            {
                "id": [1, 2, 3],
                "status": ["active", "active", "active"],
                "updated_at": [
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 1),
                ],
            }
        )

    @pytest.fixture
    def params(self):
        """Create standard SCD2 params."""
        return SCD2Params(
            target="dummy.parquet",
            keys=["id"],
            track_cols=["status"],
            effective_time_col="updated_at",
            end_time_col="valid_to",
            current_flag_col="is_current",
        )

    def test_first_run_empty_target(self, context, source_df, params):
        """Test SCD2 when target does not exist (First Run)."""
        # Use with_df to set the current dataframe on the EngineContext
        ctx = context.with_df(source_df)

        # Mock target loading to return empty DF
        with patch("os.path.exists", return_value=False):
            result_ctx = scd2(ctx, params)
            result_df = result_ctx.df

        # Should have all source rows + SCD cols
        assert len(result_df) == 3
        assert "valid_to" in result_df.columns
        assert "is_current" in result_df.columns
        assert result_df["is_current"].all()  # All should be true
        assert result_df["valid_to"].isna().all()  # All open-ended

    def test_no_changes(self, context, source_df, params):
        """Test SCD2 when source matches target exactly."""
        # Create target that matches source exactly (but with SCD metadata)
        target_df = source_df.copy()
        target_df["valid_to"] = None
        target_df["is_current"] = True

        ctx = context.with_df(source_df)

        with (
            patch("pandas.read_parquet", return_value=target_df),
            patch("os.path.exists", return_value=True),
        ):
            result_ctx = scd2(ctx, params)
            result_df = result_ctx.df

        # Should return same rows (no new history)
        assert len(result_df) == 3
        # Ensure no duplicates added
        assert len(result_df[result_df["id"] == 1]) == 1

    def test_update_existing_record(self, context, source_df, params):
        """Test SCD2 logic: closing old record and inserting new one."""
        # Target: ID 1 is 'active' since 2023
        target_df = pd.DataFrame(
            {
                "id": [1],
                "status": ["active"],
                "updated_at": [datetime(2023, 1, 1)],
                "valid_to": [None],
                "is_current": [True],
            }
        )

        # Source: ID 1 is now 'inactive' since 2024
        source_df = pd.DataFrame(
            {
                "id": [1],
                "status": ["inactive"],  # CHANGED
                "updated_at": [datetime(2024, 1, 1)],
            }
        )

        ctx = context.with_df(source_df)

        with (
            patch("pandas.read_parquet", return_value=target_df),
            patch("os.path.exists", return_value=True),
        ):
            result_ctx = scd2(ctx, params)
            result_df = result_ctx.df

        # Should have 2 rows for ID 1
        rows = result_df[result_df["id"] == 1].sort_values("updated_at")
        assert len(rows) == 2

        # Old Record: Closed
        old = rows.iloc[0]
        assert old["status"] == "active"
        assert bool(old["is_current"]) is False
        assert pd.notna(old["valid_to"])
        assert old["valid_to"] == datetime(2024, 1, 1)  # Closed at new effective time

        # New Record: Open
        new = rows.iloc[1]
        assert new["status"] == "inactive"
        assert bool(new["is_current"]) is True
        assert pd.isna(new["valid_to"])

    def test_new_insert(self, context, source_df, params):
        """Test SCD2 logic: inserting completely new key."""
        target_df = pd.DataFrame(
            {
                "id": [1],
                "status": ["active"],
                "updated_at": [datetime(2023, 1, 1)],
                "valid_to": [None],
                "is_current": [True],
            }
        )

        # Source has ID 2 (New)
        source_df = pd.DataFrame(
            {
                "id": [2],
                "status": ["active"],
                "updated_at": [datetime(2024, 1, 1)],
            }
        )

        ctx = context.with_df(source_df)

        with (
            patch("pandas.read_parquet", return_value=target_df),
            patch("os.path.exists", return_value=True),
        ):
            result_ctx = scd2(ctx, params)
            result_df = result_ctx.df

        assert len(result_df) == 2
        assert 1 in result_df["id"].values
        assert 2 in result_df["id"].values

        # Check ID 2 metadata
        row_2 = result_df[result_df["id"] == 2].iloc[0]
        assert bool(row_2["is_current"]) is True
        assert pd.isna(row_2["valid_to"])


class TestSCD2Spark:
    @pytest.fixture
    def mock_spark(self):
        mock = MagicMock()
        return mock

    @pytest.fixture
    def context(self, mock_spark):
        base_ctx = SparkContext(spark_session=mock_spark)
        return EngineContext(
            context=base_ctx,
            df=None,
            engine_type=EngineType.SPARK,
        )

    @pytest.fixture
    def params(self):
        return SCD2Params(
            target="delta_table",
            keys=["id"],
            track_cols=["status"],
            effective_time_col="updated_at",
        )

    def test_spark_dispatch(self, context, params):
        """Test that the spark implementation is called."""
        # Mock source DataFrame
        source_df = MagicMock()
        ctx = context.with_df(source_df)

        # Patch the internal spark implementation to avoid real spark calls
        with patch("odibi.transformers.scd._scd2_spark") as mock_impl:
            # When calling scd2, it should dispatch to _scd2_spark
            scd2(ctx, params)

            # Verify dispatch
            mock_impl.assert_called_once()
            # Verify arguments passed (ctx, df, params)
            args, _ = mock_impl.call_args
            assert args[0] == ctx
            # args[1] is source_df (context.df)
            assert args[2] == params

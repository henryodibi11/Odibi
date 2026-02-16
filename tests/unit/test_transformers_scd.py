import logging

logging.getLogger("odibi").propagate = False

from datetime import datetime  # noqa: E402
from unittest.mock import MagicMock, patch  # noqa: E402

import pandas as pd  # noqa: E402
import pytest  # noqa: E402
from pydantic import ValidationError  # noqa: E402

from odibi.context import EngineContext, PandasContext, SparkContext  # noqa: E402
from odibi.enums import EngineType  # noqa: E402
from odibi.transformers.scd import SCD2Params, scd2  # noqa: E402


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
        with (
            patch("os.path.exists", return_value=False),
            patch("pandas.DataFrame.to_parquet"),
        ):
            result_ctx = scd2(ctx, params)
            result_df = result_ctx.df

        # Should have all source rows + SCD cols
        assert len(result_df) == 3
        assert "valid_from" in result_df.columns
        assert "valid_to" in result_df.columns
        assert "is_current" in result_df.columns
        assert "updated_at" not in result_df.columns
        assert result_df["is_current"].all()  # All should be true
        assert result_df["valid_to"].isna().all()  # All open-ended

    def test_end_col_dtype_is_datetime(self, context, source_df, params):
        """Test that end_col is created with datetime64 dtype, not object."""
        ctx = context.with_df(source_df)

        # Mock target loading to return empty DF
        with patch("os.path.exists", return_value=False):
            result_ctx = scd2(ctx, params)
            result_df = result_ctx.df

        # Verify end_col exists and has datetime64 dtype
        assert "valid_to" in result_df.columns
        assert pd.api.types.is_datetime64_any_dtype(result_df["valid_to"])
        # Also verify it's not object dtype (the bug we're fixing)
        assert result_df["valid_to"].dtype != "object"

    def test_no_changes(self, context, source_df, params):
        """Test SCD2 when source matches target exactly."""
        # Create target that matches source exactly (but with SCD metadata)
        # Target already has renamed column from previous run
        target_df = source_df.drop(columns=["updated_at"]).copy()
        target_df["valid_from"] = source_df["updated_at"]
        target_df["valid_to"] = None
        target_df["is_current"] = True

        ctx = context.with_df(source_df)

        with (
            patch("pandas.read_parquet", return_value=target_df),
            patch("os.path.exists", return_value=True),
            patch("pandas.DataFrame.to_parquet"),
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
                "valid_from": [datetime(2023, 1, 1)],
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
        params = params.model_copy(update={"target": "target.csv"})
        with (
            patch("os.path.exists", return_value=True),
            patch("pandas.read_csv", return_value=target_df),
            patch("pandas.DataFrame.to_csv"),
        ):
            result_ctx = scd2(ctx, params)
            result_df = result_ctx.df

        # Should have 2 rows for ID 1
        rows = result_df[result_df["id"] == 1].sort_values("valid_from")
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
                "valid_from": [datetime(2023, 1, 1)],
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
        params = params.model_copy(update={"target": "target.csv"})
        with (
            patch("os.path.exists", return_value=True),
            patch("pandas.read_csv", return_value=target_df),
            patch("pandas.DataFrame.to_csv"),
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

    def test_duckdb_composite_key_new_record_detection(self, context, params, tmp_path):
        """Bug #185: DuckDB path must check ALL keys for new record detection."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        composite_params = SCD2Params(
            target=str(tmp_path / "target.parquet"),
            keys=["region", "product_id"],
            track_cols=["price"],
            effective_time_col="updated_at",
            end_time_col="valid_to",
            current_flag_col="is_current",
        )

        # Existing target: region=US, product_id=100
        target_df = pd.DataFrame(
            {
                "region": ["US"],
                "product_id": [100],
                "price": [9.99],
                "updated_at": [datetime(2024, 1, 1)],
                "valid_to": [None],
                "is_current": [True],
            }
        )
        pq.write_table(pa.Table.from_pandas(target_df), composite_params.target)

        # Source: region=US, product_id=200 (new composite key, first key matches!)
        source_df = pd.DataFrame(
            {
                "region": ["US"],
                "product_id": [200],
                "price": [19.99],
                "updated_at": [datetime(2024, 2, 1)],
            }
        )
        ctx = context.with_df(source_df)

        try:
            import duckdb  # noqa: F401

            scd2(ctx, composite_params)
            # DuckDB path writes results to the parquet file
            result_df = pd.read_parquet(composite_params.target)
            # Should have 2 rows: original + new record
            assert len(result_df) == 2, f"Expected 2 rows, got {len(result_df)}"
            new_records = result_df[result_df["product_id"] == 200]
            assert len(new_records) == 1, "New composite key record should be detected"
            assert bool(new_records.iloc[0]["is_current"]) is True
        except ImportError:
            pytest.skip("DuckDB not installed")


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


class TestSCD2ParamsValidation:
    def test_params_valid_with_target(self):
        params = SCD2Params(
            target="silver.dim_customers",
            keys=["customer_id"],
            track_cols=["address"],
            effective_time_col="txn_date",
        )
        assert params.target == "silver.dim_customers"
        assert params.connection is None
        assert params.path is None

    def test_params_valid_with_connection_and_path(self):
        params = SCD2Params(
            connection="adls_prod",
            path="OEE/silver/dim_customers",
            keys=["customer_id"],
            track_cols=["address"],
            effective_time_col="txn_date",
        )
        assert params.connection == "adls_prod"
        assert params.path == "OEE/silver/dim_customers"
        assert params.target is None

    def test_params_invalid_no_target_no_connection(self):
        with pytest.raises(ValidationError, match="provide either 'target' OR"):
            SCD2Params(
                keys=["customer_id"],
                track_cols=["address"],
                effective_time_col="txn_date",
            )

    def test_params_invalid_both_target_and_connection(self):
        with pytest.raises(ValidationError, match="use 'target' OR 'connection'"):
            SCD2Params(
                target="silver.dim_customers",
                connection="adls_prod",
                path="OEE/silver/dim_customers",
                keys=["customer_id"],
                track_cols=["address"],
                effective_time_col="txn_date",
            )

    def test_params_invalid_connection_without_path(self):
        with pytest.raises(ValidationError, match="provide either 'target' OR"):
            SCD2Params(
                connection="adls_prod",
                keys=["customer_id"],
                track_cols=["address"],
                effective_time_col="txn_date",
            )

    def test_params_custom_start_time_col(self):
        params = SCD2Params(
            target="silver.dim_customers",
            keys=["customer_id"],
            track_cols=["address"],
            effective_time_col="txn_date",
            start_time_col="effective_from",
        )
        assert params.start_time_col == "effective_from"

    def test_params_default_start_time_col(self):
        params = SCD2Params(
            target="silver.dim_customers",
            keys=["customer_id"],
            track_cols=["address"],
            effective_time_col="txn_date",
        )
        assert params.start_time_col == "valid_from"


class TestSCD2PandasEdgeCases:
    @pytest.fixture
    def context(self):
        base_ctx = PandasContext()
        return EngineContext(
            context=base_ctx,
            df=None,
            engine_type=EngineType.PANDAS,
        )

    def test_soft_deletion_with_delete_col(self, context):
        target_df = pd.DataFrame(
            {
                "id": [1, 2],
                "status": ["active", "active"],
                "valid_from": [datetime(2023, 1, 1), datetime(2023, 1, 1)],
                "valid_to": [None, None],
                "is_current": [True, True],
            }
        )
        source_df = pd.DataFrame(
            {
                "id": [1, 2],
                "status": ["active", "active"],
                "is_deleted": [True, False],
                "updated_at": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            }
        )
        params = SCD2Params(
            target="dummy.parquet",
            keys=["id"],
            track_cols=["status", "is_deleted"],
            effective_time_col="updated_at",
            end_time_col="valid_to",
            current_flag_col="is_current",
            delete_col="is_deleted",
        )
        ctx = context.with_df(source_df)
        with (
            patch("pandas.read_parquet", return_value=target_df),
            patch("os.path.exists", return_value=True),
            patch("pandas.DataFrame.to_parquet"),
        ):
            result_ctx = scd2(ctx, params)
            result_df = result_ctx.df

        id1_rows = result_df[result_df["id"] == 1].sort_values("valid_from")
        assert len(id1_rows) == 2
        old = id1_rows.iloc[0]
        assert bool(old["is_current"]) is False
        assert pd.notna(old["valid_to"])

        new = id1_rows.iloc[1]
        assert bool(new["is_current"]) is True
        assert new["is_deleted"] is True

    def test_multiple_keys(self, context):
        target_df = pd.DataFrame(
            {
                "region": ["US", "EU"],
                "product_id": [100, 200],
                "price": [10.0, 20.0],
                "valid_from": [datetime(2023, 1, 1), datetime(2023, 1, 1)],
                "valid_to": [None, None],
                "is_current": [True, True],
            }
        )
        source_df = pd.DataFrame(
            {
                "region": ["US", "EU"],
                "product_id": [100, 200],
                "price": [15.0, 20.0],
                "updated_at": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            }
        )
        params = SCD2Params(
            target="dummy.parquet",
            keys=["region", "product_id"],
            track_cols=["price"],
            effective_time_col="updated_at",
            end_time_col="valid_to",
            current_flag_col="is_current",
        )
        ctx = context.with_df(source_df)
        with (
            patch("pandas.read_parquet", return_value=target_df),
            patch("os.path.exists", return_value=True),
            patch("pandas.DataFrame.to_parquet"),
        ):
            result_ctx = scd2(ctx, params)
            result_df = result_ctx.df

        us100 = result_df[
            (result_df["region"] == "US") & (result_df["product_id"] == 100)
        ].sort_values("valid_from")
        assert len(us100) == 2
        assert bool(us100.iloc[0]["is_current"]) is False
        assert bool(us100.iloc[1]["is_current"]) is True
        assert us100.iloc[1]["price"] == 15.0

        eu200 = result_df[(result_df["region"] == "EU") & (result_df["product_id"] == 200)]
        assert len(eu200) == 1
        assert bool(eu200.iloc[0]["is_current"]) is True

    def test_multiple_track_cols(self, context):
        target_df = pd.DataFrame(
            {
                "id": [1],
                "status": ["active"],
                "tier": ["gold"],
                "valid_from": [datetime(2023, 1, 1)],
                "valid_to": [None],
                "is_current": [True],
            }
        )
        source_df = pd.DataFrame(
            {
                "id": [1],
                "status": ["active"],
                "tier": ["platinum"],
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        params = SCD2Params(
            target="dummy.parquet",
            keys=["id"],
            track_cols=["status", "tier"],
            effective_time_col="updated_at",
            end_time_col="valid_to",
            current_flag_col="is_current",
        )
        ctx = context.with_df(source_df)
        with (
            patch("pandas.read_parquet", return_value=target_df),
            patch("os.path.exists", return_value=True),
            patch("pandas.DataFrame.to_parquet"),
        ):
            result_ctx = scd2(ctx, params)
            result_df = result_ctx.df

        rows = result_df[result_df["id"] == 1].sort_values("valid_from")
        assert len(rows) == 2
        assert rows.iloc[0]["tier"] == "gold"
        assert bool(rows.iloc[0]["is_current"]) is False
        assert rows.iloc[1]["tier"] == "platinum"
        assert bool(rows.iloc[1]["is_current"]) is True

    def test_unchanged_records_preserved(self, context):
        target_df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "status": ["active", "inactive", "active"],
                "valid_from": [
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 1),
                ],
                "valid_to": [None, None, None],
                "is_current": [True, True, True],
            }
        )
        source_df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "status": ["active", "inactive", "active"],
                "updated_at": [
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 1),
                ],
            }
        )
        params = SCD2Params(
            target="dummy.csv",
            keys=["id"],
            track_cols=["status"],
            effective_time_col="updated_at",
            end_time_col="valid_to",
            current_flag_col="is_current",
        )
        ctx = context.with_df(source_df)
        with (
            patch("pandas.read_csv", return_value=target_df),
            patch("os.path.exists", return_value=True),
            patch("pandas.DataFrame.to_csv"),
        ):
            result_ctx = scd2(ctx, params)
            result_df = result_ctx.df

        assert len(result_df) == 3
        for _, row in result_df.iterrows():
            assert bool(row["is_current"]) is True
            assert pd.isna(row["valid_to"])

    def test_connection_path_resolution(self, context):
        source_df = pd.DataFrame(
            {
                "id": [1],
                "status": ["active"],
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        params = SCD2Params(
            connection="adls_prod",
            path="OEE/silver/dim_customers",
            keys=["id"],
            track_cols=["status"],
            effective_time_col="updated_at",
            end_time_col="valid_to",
            current_flag_col="is_current",
        )

        mock_connection = MagicMock()
        mock_connection.get_path.return_value = "/resolved/path/dim_customers.parquet"

        mock_engine = MagicMock()
        mock_engine.connections = {"adls_prod": mock_connection}

        ctx = context.with_df(source_df)
        ctx.engine = mock_engine

        with (
            patch("os.path.exists", return_value=False),
            patch("pandas.DataFrame.to_parquet"),
        ):
            result_ctx = scd2(ctx, params)
            result_df = result_ctx.df

        mock_connection.get_path.assert_called_once_with("OEE/silver/dim_customers")
        assert len(result_df) == 1
        assert "valid_from" in result_df.columns
        assert "updated_at" not in result_df.columns
        assert bool(result_df.iloc[0]["is_current"]) is True


class TestSCD2MergeOptimizationParam:
    """Tests for the use_delta_merge parameter on SCD2Params."""

    def test_use_delta_merge_defaults_true(self):
        params = SCD2Params(
            target="test_table",
            keys=["id"],
            track_cols=["col"],
            effective_time_col="eff",
        )
        assert params.use_delta_merge is True

    def test_use_delta_merge_can_be_disabled(self):
        params = SCD2Params(
            target="test_table",
            keys=["id"],
            track_cols=["col"],
            effective_time_col="eff",
            use_delta_merge=False,
        )
        assert params.use_delta_merge is False


class TestSCD2MergeOptimization:
    """Tests for the optimized SCD2 merge path (mock-based, no real engine needed)."""

    @pytest.fixture
    def mock_spark(self):
        return MagicMock()

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
            target="silver.dim_customers",
            keys=["customer_id"],
            track_cols=["address", "tier"],
            effective_time_col="txn_date",
            use_delta_merge=True,
        )

    @pytest.fixture
    def params_disabled(self):
        return SCD2Params(
            target="silver.dim_customers",
            keys=["customer_id"],
            track_cols=["address", "tier"],
            effective_time_col="txn_date",
            use_delta_merge=False,
        )

    def test_merge_optimization_dispatched(self, context, params):
        """When use_delta_merge=True, _scd2_spark_delta_merge is called."""
        source_df = MagicMock()
        source_df.columns = ["customer_id", "address", "tier", "txn_date"]
        ctx = context.with_df(source_df)

        with patch("odibi.transformers.scd._scd2_spark_delta_merge") as mock_merge:
            mock_merge.return_value = ctx
            scd2(ctx, params)
            mock_merge.assert_called_once()

    def test_merge_optimization_not_dispatched_when_disabled(self, context, params_disabled):
        """When use_delta_merge=False, _scd2_spark_delta_merge is NOT called."""
        source_df = MagicMock()
        source_df.columns = ["customer_id", "address", "tier", "txn_date"]
        ctx = context.with_df(source_df)

        with patch("odibi.transformers.scd._scd2_spark_delta_merge") as mock_merge:
            # Patch _scd2_spark itself at the top level to avoid PySpark calls
            with patch("odibi.transformers.scd._scd2_spark") as mock_legacy:
                mock_legacy.return_value = ctx
                scd2(ctx, params_disabled)
                mock_merge.assert_not_called()

    def test_merge_optimization_fallback_on_error(self, context, params):
        """Merge optimization failure falls back to legacy overwrite path."""
        source_df = MagicMock()
        source_df.columns = ["customer_id", "address", "tier", "txn_date"]
        ctx = context.with_df(source_df)

        # Patch _scd2_spark_delta_merge to raise, _scd2_spark to handle it
        with patch("odibi.transformers.scd._scd2_spark") as mock_spark:
            # Simulate: delta merge fails, legacy succeeds
            mock_spark.return_value = ctx
            scd2(ctx, params)
            mock_spark.assert_called_once()

    def test_merge_optimization_returns_none_for_non_compatible(self, context, params):
        """_scd2_spark_delta_merge returns None when target is not a compatible format."""
        source_df = MagicMock()
        source_df.columns = ["customer_id", "address", "tier", "txn_date"]
        ctx = context.with_df(source_df)

        from odibi.transformers.scd import _scd2_spark_delta_merge

        mock_dt_class = MagicMock()
        mock_dt_class.forName.side_effect = Exception("not a table")
        mock_dt_class.isDeltaTable.return_value = False

        # Target exists (as non-Delta table) so spark.table succeeds
        context.spark.table.return_value = MagicMock()

        # Mock delta.tables at sys.modules level since it can't import on Windows
        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable = mock_dt_class
        with patch.dict("sys.modules", {"delta": MagicMock(), "delta.tables": mock_delta_tables}):
            result = _scd2_spark_delta_merge(ctx, source_df, params)
            assert result is None

    def test_merge_optimization_first_run_writes_directly(self, context, params):
        """First run (no target) writes directly to target and returns context."""
        source_df = MagicMock()
        source_df.columns = ["customer_id", "address", "tier", "txn_date"]
        source_df.withColumn.return_value = source_df
        source_df.withColumnRenamed.return_value = source_df
        mock_writer = MagicMock()
        source_df.write.format.return_value.mode.return_value = mock_writer
        ctx = context.with_df(source_df)

        from odibi.transformers.scd import _scd2_spark_delta_merge

        mock_dt_class = MagicMock()
        mock_dt_class.forName.side_effect = Exception("not found")
        mock_dt_class.isDeltaTable.return_value = False

        # Target doesn't exist at all
        context.spark.table.side_effect = Exception("not found")
        context.spark.read.format.return_value.load.side_effect = Exception("not found")

        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable = mock_dt_class
        mock_modules = {"delta": MagicMock(), "delta.tables": mock_delta_tables}
        with (
            patch.dict("sys.modules", mock_modules),
            patch("pyspark.sql.functions.lit") as mock_lit,
        ):
            mock_lit.return_value = MagicMock()
            result = _scd2_spark_delta_merge(ctx, source_df, params)
            assert result is not None
            source_df.withColumn.assert_called()
            # Verify eff_col renamed to start_col
            source_df.withColumnRenamed.assert_called_with("txn_date", "valid_from")
            # Verify it wrote directly to the target
            source_df.write.format.assert_called_once_with("delta")
            source_df.write.format.return_value.mode.assert_called_once_with("overwrite")
            mock_writer.saveAsTable.assert_called_once_with("silver.dim_customers")

    def test_merge_optimization_executes_merge(self, context, params):
        """Verify merge is executed with correct condition and update set."""
        source_df = MagicMock()
        source_df.columns = [
            "customer_id",
            "address",
            "tier",
            "txn_date",
            "valid_from",
            "valid_to",
            "is_current",
        ]
        source_df.withColumn.return_value = source_df
        source_df.drop.return_value = source_df
        ctx = context.with_df(source_df)

        from odibi.transformers.scd import _scd2_spark_delta_merge

        mock_delta_table = MagicMock()
        mock_merger = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merger
        mock_merger.whenMatchedUpdate.return_value = mock_merger
        mock_merger.whenNotMatchedInsert.return_value = mock_merger

        mock_dt_class = MagicMock()
        mock_dt_class.forName.return_value = mock_delta_table

        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable = mock_dt_class
        mock_modules = {"delta": MagicMock(), "delta.tables": mock_delta_tables}
        with (
            patch.dict("sys.modules", mock_modules),
            patch("pyspark.sql.functions.lit") as mock_lit,
            patch("pyspark.sql.functions.col") as mock_col,
        ):
            mock_lit.return_value = MagicMock()
            mock_col.return_value = MagicMock()
            result = _scd2_spark_delta_merge(ctx, source_df, params)

        # Verify MERGE was built and executed
        mock_delta_table.alias.assert_called_once_with("target")
        mock_merger.whenMatchedUpdate.assert_called_once()
        mock_merger.whenNotMatchedInsert.assert_called_once()
        mock_merger.execute.assert_called_once()
        assert result is not None

        # Verify merge condition includes is_current filter
        merge_call = mock_delta_table.alias.return_value.merge
        condition_arg = merge_call.call_args[0][1]
        assert "target.`is_current` = true" in condition_arg
        assert "target.`customer_id` = source.`customer_id`" in condition_arg

        # Verify update set only touches end_time and current_flag
        update_call = mock_merger.whenMatchedUpdate
        update_set = update_call.call_args[1]["set"]
        assert "`valid_to`" in update_set
        assert "`is_current`" in update_set
        assert update_set["`is_current`"] == "false"
        assert "txn_date" in update_set["`valid_to`"]

        # Verify change detection condition
        change_cond = update_call.call_args[1]["condition"]
        assert "target.`address` <=> source.`address`" in change_cond
        assert "target.`tier` <=> source.`tier`" in change_cond

    def test_merge_optimization_insert_has_start_col_not_eff_col(self, context, params):
        """Verify whenNotMatchedInsert includes start_col and excludes effective_time_col."""
        source_df = MagicMock()
        source_df.columns = [
            "customer_id",
            "address",
            "tier",
            "txn_date",
            "valid_from",
            "valid_to",
            "is_current",
        ]
        source_df.withColumn.return_value = source_df
        source_df.drop.return_value = source_df
        ctx = context.with_df(source_df)

        from odibi.transformers.scd import _scd2_spark_delta_merge

        mock_delta_table = MagicMock()
        mock_merger = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merger
        mock_merger.whenMatchedUpdate.return_value = mock_merger
        mock_merger.whenNotMatchedInsert.return_value = mock_merger

        mock_dt_class = MagicMock()
        mock_dt_class.forName.return_value = mock_delta_table

        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable = mock_dt_class
        mock_modules = {"delta": MagicMock(), "delta.tables": mock_delta_tables}
        with (
            patch.dict("sys.modules", mock_modules),
            patch("pyspark.sql.functions.lit") as mock_lit,
            patch("pyspark.sql.functions.col") as mock_col,
        ):
            mock_lit.return_value = MagicMock()
            mock_col.return_value = MagicMock()
            _scd2_spark_delta_merge(ctx, source_df, params)

        # Verify insert values exclude txn_date but include valid_from
        insert_call = mock_merger.whenNotMatchedInsert
        insert_values = insert_call.call_args[1]["values"]
        assert "`txn_date`" not in insert_values
        assert "`valid_from`" in insert_values
        assert "`customer_id`" in insert_values
        assert "`address`" in insert_values


class TestSCD2PandasBugs:
    """Regression tests for specific bug fixes."""

    @pytest.fixture
    def context(self):
        base_ctx = PandasContext()
        return EngineContext(
            context=base_ctx,
            df=None,
            engine_type=EngineType.PANDAS,
        )

    @pytest.fixture
    def params(self):
        return SCD2Params(
            target="dummy.parquet",
            keys=["id"],
            track_cols=["status"],
            effective_time_col="updated_at",
            end_time_col="valid_to",
            current_flag_col="is_current",
        )

    def test_duplicate_key_source_batch_no_cartesian(self, context, params):
        """Bug #184: duplicate keys in source batch must not cause Cartesian explosion."""
        params = SCD2Params(
            target="dummy.csv",
            keys=["id"],
            track_cols=["status"],
            effective_time_col="updated_at",
            end_time_col="valid_to",
            current_flag_col="is_current",
        )
        source_df = pd.DataFrame(
            {
                "id": [1, 1],
                "status": ["v2", "v3"],
                "updated_at": [datetime(2024, 2, 1), datetime(2024, 3, 1)],
            }
        )
        target_df = pd.DataFrame(
            {
                "id": [1],
                "status": ["v1"],
                "updated_at": [datetime(2024, 1, 1)],
                "valid_to": [None],
                "is_current": [True],
            }
        )
        ctx = context.with_df(source_df)
        with (
            patch("os.path.exists", return_value=True),
            patch("pandas.read_csv", return_value=target_df),
        ):
            result_ctx = scd2(ctx, params)
            result_df = result_ctx.df
            # Should have 3 rows: 1 closed original + 2 new versions, NOT a Cartesian product
            assert len(result_df) == 3, f"Expected 3 rows, got {len(result_df)}"
            # Exactly 1 closed row
            closed = result_df[result_df["is_current"] == False]  # noqa: E712
            assert len(closed) == 1

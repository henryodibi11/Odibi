"""Unit tests for odibi.transformers.scd — SCD2 transformer."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.scd import SCD2Params, _scd2_pandas, scd2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_context(df, engine=None):
    """Create a PandasContext-based EngineContext."""
    return EngineContext(
        context=PandasContext(),
        df=df,
        engine_type=EngineType.PANDAS,
        engine=engine,
    )


def _base_params(**overrides):
    """Return standard SCD2Params with sensible defaults, allowing overrides."""
    defaults = dict(
        target="test.parquet",
        keys=["id"],
        track_cols=["status"],
        effective_time_col="updated_at",
        end_time_col="valid_to",
        current_flag_col="is_current",
    )
    defaults.update(overrides)
    return SCD2Params(**defaults)


# ---------------------------------------------------------------------------
# SCD2Params validation
# ---------------------------------------------------------------------------
class TestSCD2ParamsValidation:
    """Pydantic model validation for SCD2Params."""

    def test_target_only_ok(self):
        p = _base_params(target="dim_customers.parquet")
        assert p.target == "dim_customers.parquet"
        assert p.connection is None
        assert p.path is None

    def test_connection_and_path_ok(self):
        p = SCD2Params(
            connection="adls_prod",
            path="silver/dim_customers",
            keys=["id"],
            track_cols=["status"],
            effective_time_col="updated_at",
        )
        assert p.connection == "adls_prod"
        assert p.path == "silver/dim_customers"
        assert p.target is None

    def test_both_target_and_connection_raises(self):
        with pytest.raises(ValueError, match="not both"):
            SCD2Params(
                target="dim.parquet",
                connection="adls_prod",
                path="silver/dim",
                keys=["id"],
                track_cols=["status"],
                effective_time_col="updated_at",
            )

    def test_neither_target_nor_connection_raises(self):
        with pytest.raises(ValueError, match="provide either"):
            SCD2Params(
                keys=["id"],
                track_cols=["status"],
                effective_time_col="updated_at",
            )


# ---------------------------------------------------------------------------
# scd2 entry-point
# ---------------------------------------------------------------------------
class TestSCD2EntryPoint:
    """Tests for the top-level ``scd2`` function."""

    def test_unsupported_engine_raises(self):
        ctx = EngineContext(
            context=PandasContext(),
            df=pd.DataFrame({"id": [1]}),
            engine_type="unknown",
        )
        with pytest.raises(ValueError, match="does not support engine type"):
            scd2(ctx, _base_params())

    def test_row_count_logged_after_completion(self):
        """Result context should contain the correct DataFrame."""
        src = pd.DataFrame(
            {
                "id": [1],
                "status": ["a"],
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        ctx = _make_context(src)
        with patch("os.path.exists", return_value=False):
            result = scd2(ctx, _base_params())
        assert result.df.shape[0] == 1

    def test_current_param_overrides_context_df(self):
        """When `current` is passed, it should be used instead of context.df."""
        dummy_ctx_df = pd.DataFrame({"id": [99]})
        real_src = pd.DataFrame(
            {
                "id": [1],
                "status": ["a"],
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        ctx = _make_context(dummy_ctx_df)
        with patch("os.path.exists", return_value=False):
            result = scd2(ctx, _base_params(), current=real_src)
        assert 1 in result.df["id"].values
        assert 99 not in result.df["id"].values


# ---------------------------------------------------------------------------
# _scd2_pandas — first-run (no target)
# ---------------------------------------------------------------------------
class TestSCD2PandasFirstRun:
    """First run when no target file exists."""

    def test_returns_source_with_scd_columns(self):
        src = pd.DataFrame(
            {
                "id": [1, 2],
                "status": ["a", "b"],
                "updated_at": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
            }
        )
        ctx = _make_context(src)
        params = _base_params()
        with patch("os.path.exists", return_value=False):
            result = _scd2_pandas(ctx, src.copy(), params)
        df = result.df
        assert len(df) == 2
        assert "valid_to" in df.columns
        assert "is_current" in df.columns
        assert df["is_current"].all()
        assert df["valid_to"].isna().all()


# ---------------------------------------------------------------------------
# _scd2_pandas — existing parquet target
# ---------------------------------------------------------------------------
class TestSCD2PandasExistingTarget:
    """Tests with a pre-existing parquet target file."""

    @pytest.fixture
    def target_df(self):
        return pd.DataFrame(
            {
                "id": [1, 2],
                "status": ["active", "gold"],
                "valid_from": [datetime(2023, 6, 1), datetime(2023, 6, 1)],
                "valid_to": [None, None],
                "is_current": [True, True],
            }
        )

    def test_new_insert_detected(self, target_df):
        """A key not in target should be inserted."""
        src = pd.DataFrame(
            {
                "id": [3],
                "status": ["new"],
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        ctx = _make_context(src)
        with (
            patch("os.path.exists", return_value=True),
            patch("pandas.read_csv", return_value=target_df),
            patch("pandas.DataFrame.to_csv"),
        ):
            result = _scd2_pandas(ctx, src.copy(), _base_params(target="target.csv"))
        df = result.df
        assert 3 in df["id"].values
        row3 = df[df["id"] == 3].iloc[0]
        assert bool(row3["is_current"]) is True
        assert pd.isna(row3["valid_to"])

    def test_unchanged_records_preserved(self, target_df):
        """Records with no change should not be duplicated."""
        src = pd.DataFrame(
            {
                "id": [1],
                "status": ["active"],  # same as target
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        ctx = _make_context(src)
        with (
            patch("os.path.exists", return_value=True),
            patch("pandas.read_csv", return_value=target_df),
            patch("pandas.DataFrame.to_csv"),
        ):
            result = _scd2_pandas(ctx, src.copy(), _base_params(target="target.csv"))
        df = result.df
        # id=1 appears once (unchanged), id=2 also once (from target)
        assert len(df[df["id"] == 1]) == 1
        assert len(df[df["id"] == 2]) == 1

    def test_changed_record_closes_old_inserts_new(self, target_df):
        """A changed tracked column should close old and insert new."""
        src = pd.DataFrame(
            {
                "id": [1],
                "status": ["inactive"],  # changed from 'active'
                "updated_at": [datetime(2024, 7, 1)],
            }
        )
        ctx = _make_context(src)
        with (
            patch("os.path.exists", return_value=True),
            patch("pandas.read_csv", return_value=target_df),
            patch("pandas.DataFrame.to_csv"),
        ):
            result = _scd2_pandas(ctx, src.copy(), _base_params(target="target.csv"))
        df = result.df
        rows_id1 = df[df["id"] == 1].sort_values("valid_from")
        assert len(rows_id1) == 2

        old = rows_id1.iloc[0]
        assert old["status"] == "active"
        assert bool(old["is_current"]) is False
        assert old["valid_to"] == datetime(2024, 7, 1)

        new = rows_id1.iloc[1]
        assert new["status"] == "inactive"
        assert bool(new["is_current"]) is True
        assert pd.isna(new["valid_to"])


# ---------------------------------------------------------------------------
# _scd2_pandas — delete_col handling
# ---------------------------------------------------------------------------
class TestSCD2PandasDeleteCol:
    """Tests for the delete_col parameter."""

    def test_delete_col_passed_through(self):
        """delete_col should be accepted without error."""
        params = _base_params(delete_col="is_deleted")
        assert params.delete_col == "is_deleted"

    def test_first_run_with_delete_col(self):
        """First run with delete_col should still add SCD columns."""
        src = pd.DataFrame(
            {
                "id": [1],
                "status": ["a"],
                "is_deleted": [False],
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        ctx = _make_context(src)
        params = _base_params(delete_col="is_deleted")
        with patch("os.path.exists", return_value=False):
            result = _scd2_pandas(ctx, src.copy(), params)
        df = result.df
        assert "is_current" in df.columns
        assert "valid_to" in df.columns


# ---------------------------------------------------------------------------
# _scd2_pandas — CSV target format
# ---------------------------------------------------------------------------
class TestSCD2PandasCSVTarget:
    """CSV target file format."""

    def test_csv_target_loaded(self):
        target_df = pd.DataFrame(
            {
                "id": [1],
                "status": ["active"],
                "updated_at": [datetime(2023, 1, 1)],
                "valid_to": [None],
                "is_current": [True],
            }
        )
        src = pd.DataFrame(
            {
                "id": [2],
                "status": ["new"],
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        ctx = _make_context(src)
        params = _base_params(target="data.csv")
        with (
            patch("os.path.exists", return_value=True),
            patch("pandas.read_csv", return_value=target_df),
        ):
            result = _scd2_pandas(ctx, src.copy(), params)
        df = result.df
        assert 1 in df["id"].values
        assert 2 in df["id"].values


# ---------------------------------------------------------------------------
# Connection resolution
# ---------------------------------------------------------------------------
class TestSCD2ConnectionResolution:
    """Tests for connection + path resolution via the scd2 entry point."""

    def test_connection_found_resolves_path(self, tmp_path):
        conn = MagicMock()
        resolved = str(tmp_path / "dim.parquet")
        conn.get_path.return_value = resolved
        engine = MagicMock()
        engine.connections = {"adls_prod": conn}

        src = pd.DataFrame(
            {
                "id": [1],
                "status": ["a"],
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        ctx = _make_context(src, engine=engine)
        params = SCD2Params(
            connection="adls_prod",
            path="silver/dim",
            keys=["id"],
            track_cols=["status"],
            effective_time_col="updated_at",
        )
        with patch("os.path.exists", return_value=False):
            result = scd2(ctx, params)
        conn.get_path.assert_called_once_with("silver/dim")
        assert result.df.shape[0] == 1

    def test_connection_not_found_raises(self):
        engine = MagicMock()
        engine.connections = {}

        src = pd.DataFrame({"id": [1]})
        ctx = _make_context(src, engine=engine)
        params = SCD2Params(
            connection="missing_conn",
            path="silver/dim",
            keys=["id"],
            track_cols=["status"],
            effective_time_col="updated_at",
        )
        with pytest.raises(ValueError, match="not found"):
            scd2(ctx, params)

    def test_connection_missing_get_path_raises(self):
        conn = object()  # no get_path method
        engine = MagicMock()
        engine.connections = {"bad_conn": conn}

        src = pd.DataFrame({"id": [1]})
        ctx = _make_context(src, engine=engine)
        params = SCD2Params(
            connection="bad_conn",
            path="silver/dim",
            keys=["id"],
            track_cols=["status"],
            effective_time_col="updated_at",
        )
        with pytest.raises(ValueError, match="does not support path resolution"):
            scd2(ctx, params)


# ---------------------------------------------------------------------------
# _scd2_pandas — DuckDB path
# ---------------------------------------------------------------------------
class TestSCD2PandasDuckDB:
    """DuckDB-accelerated path inside _scd2_pandas."""

    def test_duckdb_path_executes_sql(self, tmp_path):
        """When duckdb is available, target is .parquet, file exists → DuckDB SQL."""
        target_file = tmp_path / "dim.parquet"
        target_df = pd.DataFrame(
            {
                "id": [1],
                "status": ["active"],
                "updated_at": [datetime(2023, 1, 1)],
                "valid_to": [None],
                "is_current": [True],
            }
        )
        target_df.to_parquet(str(target_file))

        src = pd.DataFrame(
            {
                "id": [1],
                "status": ["inactive"],
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        ctx = _make_context(src)
        params = _base_params(target=str(target_file))

        mock_con = MagicMock()
        mock_duckdb = MagicMock()
        mock_duckdb.connect.return_value = mock_con

        with patch.dict("sys.modules", {"duckdb": mock_duckdb}):
            result = _scd2_pandas(ctx, src.copy(), params)

        mock_duckdb.connect.assert_called_once_with(database=":memory:")
        mock_con.register.assert_called_once()
        mock_con.execute.assert_called_once()
        mock_con.close.assert_called_once()
        # Returns source_df on DuckDB path
        assert result.df.shape[0] == 1

    def test_duckdb_failure_falls_back_to_pandas(self, tmp_path):
        """When DuckDB execution fails, falls back to Pandas."""
        target_file = tmp_path / "dim.parquet"
        target_df = pd.DataFrame(
            {
                "id": [1],
                "status": ["active"],
                "updated_at": [datetime(2023, 1, 1)],
                "valid_to": [None],
                "is_current": [True],
            }
        )
        target_df.to_parquet(str(target_file))

        src = pd.DataFrame(
            {
                "id": [1],
                "status": ["changed"],
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        ctx = _make_context(src)
        params = _base_params(target=str(target_file))

        mock_con = MagicMock()
        mock_con.execute.side_effect = RuntimeError("DuckDB boom")
        mock_duckdb = MagicMock()
        mock_duckdb.connect.return_value = mock_con

        with patch.dict("sys.modules", {"duckdb": mock_duckdb}):
            result = _scd2_pandas(ctx, src.copy(), params)

        df = result.df
        # Pandas fallback should produce 2 rows for id=1 (old closed + new)
        rows_id1 = df[df["id"] == 1]
        assert len(rows_id1) == 2


# ---------------------------------------------------------------------------
# _scd2_pandas — connection.path resolution inside _scd2_pandas
# ---------------------------------------------------------------------------
class TestSCD2PandasConnectionPathResolution:
    """Dotted target (e.g. 'conn.table') resolved via engine connections."""

    def test_dotted_target_resolved_via_engine(self):
        conn = MagicMock()
        conn.get_path.return_value = "nonexistent.parquet"
        engine = MagicMock()
        engine.connections = {"myconn": conn}

        src = pd.DataFrame(
            {
                "id": [1],
                "status": ["a"],
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        ctx = _make_context(src, engine=engine)
        params = _base_params(target="myconn.table")

        with patch("os.path.exists", return_value=False):
            result = _scd2_pandas(ctx, src.copy(), params)

        conn.get_path.assert_called_once_with("table")
        assert result.df.shape[0] == 1


# ---------------------------------------------------------------------------
# register_table
# ---------------------------------------------------------------------------
class TestSCD2RegisterTable:
    """Tests for register_table parameter."""

    def test_register_table_param_accepted(self):
        """register_table should be accepted as a valid parameter."""
        params = _base_params(register_table="silver.dim_customers")
        assert params.register_table == "silver.dim_customers"

    def test_register_table_default_is_none(self):
        """register_table should default to None."""
        params = _base_params()
        assert params.register_table is None

    def test_register_table_calls_spark_sql(self):
        """On Spark, register_table should issue CREATE TABLE IF NOT EXISTS."""
        mock_spark = MagicMock()
        mock_context = MagicMock()
        mock_context.spark = mock_spark
        ctx = EngineContext(
            context=mock_context,
            df=MagicMock(),
            engine_type=EngineType.SPARK,
        )

        params = SCD2Params(
            target="dbfs:/mnt/silver/dim_customers",
            keys=["id"],
            track_cols=["status"],
            effective_time_col="updated_at",
            register_table="silver.dim_customers",
        )

        with patch("odibi.transformers.scd._scd2_spark") as mock_impl:
            mock_impl.return_value = ctx
            scd2(ctx, params)

        mock_spark.sql.assert_called_once()
        call_sql = mock_spark.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS silver.dim_customers" in call_sql
        assert "USING DELTA" in call_sql

    def test_register_table_ignored_on_pandas(self):
        """On Pandas, register_table should be silently ignored."""
        src = pd.DataFrame(
            {
                "id": [1],
                "status": ["a"],
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        ctx = _make_context(src)
        params = _base_params(register_table="silver.dim_customers")
        with patch("os.path.exists", return_value=False):
            result = scd2(ctx, params)
        assert result.df.shape[0] == 1


# ---------------------------------------------------------------------------
# vacuum_hours parameter
# ---------------------------------------------------------------------------
class TestSCD2VacuumHours:
    """Tests for the vacuum_hours parameter on SCD2."""

    def test_param_default_none(self):
        p = _base_params()
        assert p.vacuum_hours is None

    def test_param_set(self):
        p = _base_params(vacuum_hours=168)
        assert p.vacuum_hours == 168

    def test_vacuum_skipped_on_pandas(self):
        """VACUUM is Spark-only; Pandas should complete without error."""
        src = pd.DataFrame(
            {
                "id": [1],
                "status": ["a"],
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        ctx = _make_context(src)
        params = _base_params(vacuum_hours=168)
        with patch("os.path.exists", return_value=False):
            result = scd2(ctx, params)
        assert result.df.shape[0] == 1

    def test_vacuum_runs_on_engine_with_sql(self):
        """Verify VACUUM SQL is called when vacuum_hours is set on a mock engine."""
        mock_spark = MagicMock()
        mock_engine = MagicMock()
        mock_engine.spark = mock_spark

        ctx = EngineContext(
            context=MagicMock(),
            df=pd.DataFrame(
                {
                    "id": [1],
                    "status": ["a"],
                    "updated_at": [datetime(2024, 1, 1)],
                }
            ),
            engine_type=EngineType.SPARK,
            engine=mock_engine,
        )
        params = _base_params(target="silver.dim_customers", vacuum_hours=168)

        with patch(
            "odibi.transformers.scd._scd2_spark",
            return_value=EngineContext(
                context=ctx.context,
                df=pd.DataFrame({"id": [1], "is_current": [True]}),
                engine_type=EngineType.SPARK,
                engine=mock_engine,
            ),
        ):
            scd2(ctx, params)

        vacuum_calls = [c for c in mock_spark.sql.call_args_list if "VACUUM" in str(c)]
        assert len(vacuum_calls) == 1
        assert "RETAIN 168 HOURS" in str(vacuum_calls[0])

    def test_vacuum_uses_path_syntax(self):
        """Path-based targets should use delta.`path` syntax."""
        mock_spark = MagicMock()
        mock_engine = MagicMock()
        mock_engine.spark = mock_spark

        ctx = EngineContext(
            context=MagicMock(),
            df=pd.DataFrame(
                {
                    "id": [1],
                    "status": ["a"],
                    "updated_at": [datetime(2024, 1, 1)],
                }
            ),
            engine_type=EngineType.SPARK,
            engine=mock_engine,
        )
        params = _base_params(target="/mnt/data/silver/dim", vacuum_hours=72)

        with patch(
            "odibi.transformers.scd._scd2_spark",
            return_value=EngineContext(
                context=ctx.context,
                df=pd.DataFrame({"id": [1]}),
                engine_type=EngineType.SPARK,
                engine=mock_engine,
            ),
        ):
            scd2(ctx, params)

        vacuum_calls = [c for c in mock_spark.sql.call_args_list if "VACUUM" in str(c)]
        assert len(vacuum_calls) == 1
        assert "delta.`/mnt/data/silver/dim`" in str(vacuum_calls[0])

    def test_vacuum_failure_is_warning_not_error(self):
        """VACUUM failure should log a warning, not crash."""
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = RuntimeError("VACUUM failed")
        mock_engine = MagicMock()
        mock_engine.spark = mock_spark

        ctx = EngineContext(
            context=MagicMock(),
            df=pd.DataFrame(
                {
                    "id": [1],
                    "status": ["a"],
                    "updated_at": [datetime(2024, 1, 1)],
                }
            ),
            engine_type=EngineType.SPARK,
            engine=mock_engine,
        )
        params = _base_params(target="silver.dim_customers", vacuum_hours=168)

        with patch(
            "odibi.transformers.scd._scd2_spark",
            return_value=EngineContext(
                context=ctx.context,
                df=pd.DataFrame({"id": [1]}),
                engine_type=EngineType.SPARK,
                engine=mock_engine,
            ),
        ):
            # Should not raise
            result = scd2(ctx, params)
        assert result.df is not None

    def test_vacuum_not_called_when_none(self):
        """No VACUUM when vacuum_hours is None (default)."""
        mock_spark = MagicMock()
        mock_engine = MagicMock()
        mock_engine.spark = mock_spark

        ctx = EngineContext(
            context=MagicMock(),
            df=pd.DataFrame(
                {
                    "id": [1],
                    "status": ["a"],
                    "updated_at": [datetime(2024, 1, 1)],
                }
            ),
            engine_type=EngineType.SPARK,
            engine=mock_engine,
        )
        params = _base_params(target="silver.dim_customers")  # vacuum_hours=None

        with patch(
            "odibi.transformers.scd._scd2_spark",
            return_value=EngineContext(
                context=ctx.context,
                df=pd.DataFrame({"id": [1]}),
                engine_type=EngineType.SPARK,
                engine=mock_engine,
            ),
        ):
            scd2(ctx, params)

        vacuum_calls = [c for c in mock_spark.sql.call_args_list if "VACUUM" in str(c)]
        assert len(vacuum_calls) == 0


# ---------------------------------------------------------------------------
# _safe_isna & has_changed float/NaN handling  (issue #248)
# ---------------------------------------------------------------------------
class TestSafeIsna:
    """Tests for _safe_isna helper."""

    def test_nan_returns_true(self):
        from odibi.transformers.scd import _safe_isna

        assert _safe_isna(float("nan")) is True

    def test_none_returns_true(self):
        from odibi.transformers.scd import _safe_isna

        assert _safe_isna(None) is True

    def test_nat_returns_true(self):
        from odibi.transformers.scd import _safe_isna

        assert _safe_isna(pd.NaT) is True

    def test_normal_value_returns_false(self):
        from odibi.transformers.scd import _safe_isna

        assert _safe_isna(42) is False
        assert _safe_isna("hello") is False
        assert _safe_isna(3.14) is False

    def test_list_returns_false(self):
        """pd.isna raises ValueError on lists; _safe_isna should not."""
        from odibi.transformers.scd import _safe_isna

        assert _safe_isna([1, 2, 3]) is False


class TestHasChangedFloatPrecision:
    """Regression tests: SCD2 Pandas change detection with floats and NaN."""

    @pytest.fixture
    def target_df(self):
        return pd.DataFrame(
            {
                "id": [1, 2, 3],
                "price": [19.99, 100.0, float("nan")],
                "valid_from": [datetime(2023, 1, 1)] * 3,
                "valid_to": [None] * 3,
                "is_current": [True] * 3,
            }
        )

    def test_float_precision_no_false_change(self, target_df, tmp_path):
        """Identical floats with tiny representation drift should NOT be flagged as changed."""
        target_file = tmp_path / "target.parquet"
        target_df.to_parquet(target_file, index=False)

        # Simulate float precision drift (e.g., from read-back)
        src = pd.DataFrame(
            {
                "id": [1, 2],
                "price": [19.99 + 1e-15, 100.0 - 1e-14],
                "updated_at": [datetime(2024, 1, 1)] * 2,
            }
        )
        ctx = _make_context(src)
        params = _base_params(target=str(target_file), track_cols=["price"], keys=["id"])
        result = _scd2_pandas(ctx, src.copy(), params)
        df = result.df
        # No old records should be closed (no changes detected)
        current_rows = df[df["is_current"] == True]  # noqa: E712
        assert len(current_rows) == 3  # original 3 still current

    def test_real_float_change_detected(self, target_df, tmp_path):
        """A genuine float value change should still be detected."""
        target_file = tmp_path / "target.parquet"
        target_df.to_parquet(target_file, index=False)

        src = pd.DataFrame(
            {
                "id": [1],
                "price": [29.99],  # genuinely different from 19.99
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        ctx = _make_context(src)
        params = _base_params(target=str(target_file), track_cols=["price"], keys=["id"])
        result = _scd2_pandas(ctx, src.copy(), params)
        df = result.df
        # id=1 should have 2 rows: old closed + new current
        id1_rows = df[df["id"] == 1]
        assert len(id1_rows) == 2
        assert id1_rows[id1_rows["is_current"] == True].iloc[0]["price"] == 29.99  # noqa: E712

    def test_both_nan_no_change(self, target_df, tmp_path):
        """NaN in both source and target should NOT be flagged as changed."""
        target_file = tmp_path / "target.parquet"
        target_df.to_parquet(target_file, index=False)

        src = pd.DataFrame(
            {
                "id": [3],
                "price": [float("nan")],  # same as target
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        ctx = _make_context(src)
        params = _base_params(target=str(target_file), track_cols=["price"], keys=["id"])
        result = _scd2_pandas(ctx, src.copy(), params)
        df = result.df
        # id=3 should still have only 1 current row (no change)
        id3_current = df[(df["id"] == 3) & (df["is_current"] == True)]  # noqa: E712
        assert len(id3_current) == 1

    def test_nan_to_value_is_change(self, target_df, tmp_path):
        """NaN → real value should be detected as a change."""
        target_file = tmp_path / "target.parquet"
        target_df.to_parquet(target_file, index=False)

        src = pd.DataFrame(
            {
                "id": [3],
                "price": [50.0],  # was NaN
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        ctx = _make_context(src)
        params = _base_params(target=str(target_file), track_cols=["price"], keys=["id"])
        result = _scd2_pandas(ctx, src.copy(), params)
        df = result.df
        id3_rows = df[df["id"] == 3]
        assert len(id3_rows) == 2  # old closed + new current

    def test_value_to_nan_is_change(self, target_df, tmp_path):
        """Real value → NaN should be detected as a change."""
        target_file = tmp_path / "target.parquet"
        target_df.to_parquet(target_file, index=False)

        src = pd.DataFrame(
            {
                "id": [1],
                "price": [float("nan")],  # was 19.99
                "updated_at": [datetime(2024, 1, 1)],
            }
        )
        ctx = _make_context(src)
        params = _base_params(target=str(target_file), track_cols=["price"], keys=["id"])
        result = _scd2_pandas(ctx, src.copy(), params)
        df = result.df
        id1_rows = df[df["id"] == 1]
        assert len(id1_rows) == 2  # old closed + new current

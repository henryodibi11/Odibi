"""Unit tests for delete_detection.py — Polars paths.

Tests the Polars branches added in T-012:
- _ensure_delete_column (Polars)
- _get_row_count (Polars)
- _apply_hard_delete (Polars)
- _apply_soft_delete (Polars, sql_compare mode)
- _apply_soft_delete (Polars, snapshot_diff mode with prev_df)
- _apply_deletes threshold logic with Polars
- detect_deletes entry point routing to Polars
- Cross-engine parity: Polars vs Pandas
"""

import pytest

pl = pytest.importorskip("polars")

import pandas as pd  # noqa: E402

from odibi.config import (  # noqa: E402
    DeleteDetectionConfig,
    DeleteDetectionMode,
    ThresholdBreachAction,
)
from odibi.context import EngineContext, PolarsContext  # noqa: E402
from odibi.engine.polars_engine import PolarsEngine  # noqa: E402
from odibi.enums import EngineType  # noqa: E402
from odibi.transformers.delete_detection import (  # noqa: E402
    DeleteThresholdExceeded,
    detect_deletes,
    _apply_deletes,
    _apply_soft_delete,
    _apply_hard_delete,
    _ensure_delete_column,
    _get_row_count,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_polars_ctx(df, engine=None):
    """Build a PolarsContext-backed EngineContext."""
    ctx = PolarsContext()
    eng = engine or PolarsEngine()
    return EngineContext(
        context=ctx,
        df=df,
        engine_type=EngineType.POLARS,
        sql_executor=None,
        engine=eng,
    )


def _cfg(**overrides):
    """Shortcut for DeleteDetectionConfig with sensible defaults."""
    defaults = dict(mode=DeleteDetectionMode.SNAPSHOT_DIFF, keys=["id"])
    defaults.update(overrides)
    return DeleteDetectionConfig(**defaults)


# =========================================================================
# _ensure_delete_column() — Polars
# =========================================================================


class TestEnsureDeleteColumnPolars:
    def test_adds_false_column(self):
        df = pl.DataFrame({"id": [1, 2, 3]})
        ctx = _make_polars_ctx(df)
        config = _cfg(soft_delete_col="_is_deleted")
        result = _ensure_delete_column(ctx, config)
        assert "_is_deleted" in result.df.columns
        assert result.df["_is_deleted"].to_list() == [False, False, False]

    def test_no_op_when_no_soft_delete_col(self):
        df = pl.DataFrame({"id": [1]})
        ctx = _make_polars_ctx(df)
        config = _cfg(soft_delete_col=None)
        result = _ensure_delete_column(ctx, config)
        assert "_is_deleted" not in result.df.columns

    def test_column_already_exists_no_change(self):
        df = pl.DataFrame({"id": [1], "_is_deleted": [True]})
        ctx = _make_polars_ctx(df)
        config = _cfg(soft_delete_col="_is_deleted")
        result = _ensure_delete_column(ctx, config)
        assert result.df["_is_deleted"].to_list() == [True]

    def test_lazyframe_collected(self):
        df = pl.DataFrame({"id": [1, 2]}).lazy()
        ctx = _make_polars_ctx(df)
        config = _cfg(soft_delete_col="_is_deleted")
        result = _ensure_delete_column(ctx, config)
        assert isinstance(result.df, pl.DataFrame)
        assert result.df["_is_deleted"].to_list() == [False, False]


# =========================================================================
# _get_row_count() — Polars
# =========================================================================


class TestGetRowCountPolars:
    def test_eager_dataframe(self):
        df = pl.DataFrame({"a": [1, 2, 3]})
        assert _get_row_count(df, EngineType.POLARS) == 3

    def test_empty_dataframe(self):
        df = pl.DataFrame({"a": []})
        assert _get_row_count(df, EngineType.POLARS) == 0

    def test_lazyframe(self):
        df = pl.DataFrame({"a": [1, 2, 3, 4]}).lazy()
        assert _get_row_count(df, EngineType.POLARS) == 4


# =========================================================================
# _apply_hard_delete() — Polars
# =========================================================================


class TestApplyHardDeletePolars:
    def test_removes_deleted_keys(self):
        df = pl.DataFrame({"id": [1, 2, 3, 4], "val": ["a", "b", "c", "d"]})
        deleted_keys = pl.DataFrame({"id": [2, 4]})
        ctx = _make_polars_ctx(df)
        config = _cfg(soft_delete_col=None)
        result = _apply_hard_delete(ctx, deleted_keys, config)
        assert result.df["id"].sort().to_list() == [1, 3]
        assert result.df.shape[0] == 2

    def test_no_deletes(self):
        df = pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        deleted_keys = pl.DataFrame({"id": []}).cast({"id": pl.Int64})
        ctx = _make_polars_ctx(df)
        config = _cfg(soft_delete_col=None)
        result = _apply_hard_delete(ctx, deleted_keys, config)
        assert result.df.shape[0] == 2

    def test_lazyframe_input(self):
        df = pl.DataFrame({"id": [1, 2, 3]}).lazy()
        deleted_keys = pl.DataFrame({"id": [2]})
        ctx = _make_polars_ctx(df)
        config = _cfg(soft_delete_col=None)
        result = _apply_hard_delete(ctx, deleted_keys, config)
        assert result.df["id"].sort().to_list() == [1, 3]

    def test_composite_keys(self):
        df = pl.DataFrame({"k1": [1, 1, 2, 2], "k2": ["a", "b", "a", "b"], "v": [10, 20, 30, 40]})
        deleted_keys = pl.DataFrame({"k1": [1, 2], "k2": ["b", "a"]})
        ctx = _make_polars_ctx(df)
        config = _cfg(keys=["k1", "k2"], soft_delete_col=None)
        result = _apply_hard_delete(ctx, deleted_keys, config)
        remaining = result.df.sort("k1", "k2")
        assert remaining["k1"].to_list() == [1, 2]
        assert remaining["k2"].to_list() == ["a", "b"]


# =========================================================================
# _apply_soft_delete() — Polars (sql_compare mode, no prev_df)
# =========================================================================


class TestApplySoftDeletePolarsNoPredf:
    def test_flags_deleted_rows(self):
        df = pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        deleted_keys = pl.DataFrame({"id": [2]})
        ctx = _make_polars_ctx(df)
        config = _cfg(soft_delete_col="_is_deleted")
        result = _apply_soft_delete(ctx, deleted_keys, config, prev_df=None)
        result_sorted = result.df.sort("id")
        assert result_sorted["_is_deleted"].to_list() == [False, True, False]
        assert result_sorted.shape[0] == 3

    def test_no_deleted_keys(self):
        df = pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        deleted_keys = pl.DataFrame({"id": []}).cast({"id": pl.Int64})
        ctx = _make_polars_ctx(df)
        config = _cfg(soft_delete_col="_is_deleted")
        result = _apply_soft_delete(ctx, deleted_keys, config, prev_df=None)
        assert result.df["_is_deleted"].to_list() == [False, False]


# =========================================================================
# _apply_soft_delete() — Polars (snapshot_diff mode, with prev_df)
# =========================================================================


class TestApplySoftDeletePolarsWithPrevDf:
    def test_unions_deleted_rows(self):
        # Current source has ids 1, 3 (id 2 was deleted)
        curr_df = pl.DataFrame({"id": [1, 3], "val": ["a", "c"]})
        # Previous version had ids 1, 2, 3
        prev_df = pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        deleted_keys = pl.DataFrame({"id": [2]})
        ctx = _make_polars_ctx(curr_df)
        config = _cfg(soft_delete_col="_is_deleted")
        result = _apply_soft_delete(ctx, deleted_keys, config, prev_df=prev_df)
        result_sorted = result.df.sort("id")
        # Should have 3 rows: 1 (not deleted), 2 (deleted from prev), 3 (not deleted)
        assert result_sorted.shape[0] == 3
        assert result_sorted["id"].to_list() == [1, 2, 3]
        assert result_sorted["_is_deleted"].to_list() == [False, True, False]

    def test_multiple_deletes(self):
        curr_df = pl.DataFrame({"id": [1], "val": ["a"]})
        prev_df = pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        deleted_keys = pl.DataFrame({"id": [2, 3]})
        ctx = _make_polars_ctx(curr_df)
        config = _cfg(soft_delete_col="_is_deleted")
        result = _apply_soft_delete(ctx, deleted_keys, config, prev_df=prev_df)
        result_sorted = result.df.sort("id")
        assert result_sorted.shape[0] == 3
        assert result_sorted["_is_deleted"].to_list() == [False, True, True]


# =========================================================================
# _apply_deletes() — threshold logic with Polars
# =========================================================================


class TestApplyDeletesThresholdPolars:
    def test_threshold_error(self):
        df = pl.DataFrame({"id": [1, 2, 3, 4], "val": ["a", "b", "c", "d"]})
        deleted_keys = pl.DataFrame({"id": [1, 2, 3]})  # 75% deletion
        ctx = _make_polars_ctx(df)
        config = _cfg(
            soft_delete_col=None,
            max_delete_percent=50.0,
            on_threshold_breach=ThresholdBreachAction.ERROR,
        )
        with pytest.raises(DeleteThresholdExceeded, match="exceeds threshold"):
            _apply_deletes(ctx, deleted_keys, config)

    def test_threshold_skip(self):
        df = pl.DataFrame({"id": [1, 2, 3, 4], "val": ["a", "b", "c", "d"]})
        deleted_keys = pl.DataFrame({"id": [1, 2, 3]})
        ctx = _make_polars_ctx(df)
        config = _cfg(
            soft_delete_col="_is_deleted",
            max_delete_percent=50.0,
            on_threshold_breach=ThresholdBreachAction.SKIP,
        )
        result = _apply_deletes(ctx, deleted_keys, config)
        # Should skip and add _is_deleted=False
        assert result.df["_is_deleted"].to_list() == [False, False, False, False]

    def test_threshold_warn_proceeds(self):
        df = pl.DataFrame({"id": [1, 2, 3, 4], "val": ["a", "b", "c", "d"]})
        deleted_keys = pl.DataFrame({"id": [1, 2, 3]})
        ctx = _make_polars_ctx(df)
        config = _cfg(
            soft_delete_col=None,
            max_delete_percent=50.0,
            on_threshold_breach=ThresholdBreachAction.WARN,
        )
        result = _apply_deletes(ctx, deleted_keys, config)
        # Warn proceeds with hard delete
        assert result.df.shape[0] == 1
        assert result.df["id"].to_list() == [4]


# =========================================================================
# detect_deletes() entry point — Polars routing
# =========================================================================


class TestDetectDeletesPolarsRouting:
    def test_mode_none_returns_unchanged(self):
        df = pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        ctx = _make_polars_ctx(df)
        config = _cfg(mode=DeleteDetectionMode.NONE)
        result = detect_deletes(ctx, config)
        assert result.df["id"].to_list() == [1, 2]

    def test_unknown_mode_raises(self):
        df = pl.DataFrame({"id": [1]})
        ctx = _make_polars_ctx(df)
        config = _cfg()
        config.mode = "totally_fake"
        with pytest.raises(ValueError, match="Unknown delete detection mode"):
            detect_deletes(ctx, config)


# =========================================================================
# Cross-engine parity: Polars vs Pandas
# =========================================================================


class TestCrossEngineParity:
    """Verify Polars and Pandas paths produce identical results."""

    def _make_pandas_ctx(self, df):
        from odibi.context import PandasContext
        from odibi.engine.pandas_engine import PandasEngine

        ctx = PandasContext()
        eng = PandasEngine()
        return EngineContext(
            context=ctx,
            df=df,
            engine_type=EngineType.PANDAS,
            sql_executor=None,
            engine=eng,
        )

    def test_hard_delete_parity(self):
        data = {"id": [1, 2, 3, 4, 5], "val": ["a", "b", "c", "d", "e"]}
        pl_df = pl.DataFrame(data)
        pd_df = pd.DataFrame(data)

        pl_deleted = pl.DataFrame({"id": [2, 4]})
        pd_deleted = pd.DataFrame({"id": [2, 4]})

        config = _cfg(soft_delete_col=None)

        pl_result = _apply_hard_delete(_make_polars_ctx(pl_df), pl_deleted, config)
        pd_result = _apply_hard_delete(self._make_pandas_ctx(pd_df), pd_deleted, config)

        pl_sorted = pl_result.df.sort("id").to_pandas().reset_index(drop=True)
        pd_sorted = pd_result.df.sort_values("id").reset_index(drop=True)
        pd.testing.assert_frame_equal(pl_sorted, pd_sorted, check_dtype=False)

    def test_soft_delete_sql_compare_parity(self):
        data = {"id": [1, 2, 3], "val": ["a", "b", "c"]}
        pl_df = pl.DataFrame(data)
        pd_df = pd.DataFrame(data)

        pl_deleted = pl.DataFrame({"id": [2]})
        pd_deleted = pd.DataFrame({"id": [2]})

        config = _cfg(soft_delete_col="_is_deleted")

        pl_result = _apply_soft_delete(_make_polars_ctx(pl_df), pl_deleted, config, prev_df=None)
        pd_result = _apply_soft_delete(
            self._make_pandas_ctx(pd_df), pd_deleted, config, prev_df=None
        )

        pl_sorted = pl_result.df.sort("id").to_pandas().reset_index(drop=True)
        pd_sorted = pd_result.df.sort_values("id").reset_index(drop=True)
        pd.testing.assert_frame_equal(
            pl_sorted[["id", "val", "_is_deleted"]],
            pd_sorted[["id", "val", "_is_deleted"]],
            check_dtype=False,
        )

    def test_soft_delete_snapshot_diff_parity(self):
        curr_data = {"id": [1, 3], "val": ["a", "c"]}
        prev_data = {"id": [1, 2, 3], "val": ["a", "b", "c"]}

        pl_curr = pl.DataFrame(curr_data)
        pd_curr = pd.DataFrame(curr_data)

        pl_prev = pl.DataFrame(prev_data)
        pd_prev = pd.DataFrame(prev_data)

        pl_deleted = pl.DataFrame({"id": [2]})
        pd_deleted = pd.DataFrame({"id": [2]})

        config = _cfg(soft_delete_col="_is_deleted")

        pl_result = _apply_soft_delete(
            _make_polars_ctx(pl_curr),
            pl_deleted,
            config,
            prev_df=pl_prev,
        )
        pd_result = _apply_soft_delete(
            self._make_pandas_ctx(pd_curr),
            pd_deleted,
            config,
            prev_df=pd_prev,
        )

        pl_sorted = pl_result.df.sort("id").to_pandas().reset_index(drop=True)
        pd_sorted = pd_result.df.sort_values("id").reset_index(drop=True)
        pd.testing.assert_frame_equal(
            pl_sorted[["id", "val", "_is_deleted"]],
            pd_sorted[["id", "val", "_is_deleted"]],
            check_dtype=False,
        )

    def test_ensure_delete_column_parity(self):
        pl_df = pl.DataFrame({"id": [1, 2]})
        pd_df = pd.DataFrame({"id": [1, 2]})
        config = _cfg(soft_delete_col="_is_deleted")

        pl_result = _ensure_delete_column(_make_polars_ctx(pl_df), config)
        pd_result = _ensure_delete_column(self._make_pandas_ctx(pd_df), config)

        assert pl_result.df["_is_deleted"].to_list() == pd_result.df["_is_deleted"].tolist()

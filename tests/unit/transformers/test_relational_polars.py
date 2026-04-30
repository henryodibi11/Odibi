"""Tests for relational transformers — Polars engine.

Tests pivot, unpivot (new Polars branches) and join, union, aggregate
(implicit Polars support via context.sql()) with cross-engine parity.

GitHub issue #212 — Polars parity gap fill.
"""

import pandas as pd
import polars as pl
import pytest

from odibi.context import EngineContext, PandasContext, PolarsContext
from odibi.engine.pandas_engine import PandasEngine
from odibi.engine.polars_engine import PolarsEngine
from odibi.enums import EngineType
from odibi.transformers.relational import (
    AggFunc,
    AggregateParams,
    JoinParams,
    PivotParams,
    UnionParams,
    UnpivotParams,
    aggregate,
    join,
    pivot,
    union,
    unpivot,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _polars_sql_executor(query, context):
    """Execute SQL on Polars DataFrames."""
    sql_ctx = pl.SQLContext()
    for name in context.list_names():
        df = context.get(name)
        if isinstance(df, pl.LazyFrame):
            sql_ctx.register(name, df)
        elif isinstance(df, pl.DataFrame):
            sql_ctx.register(name, df.lazy())
    return sql_ctx.execute(query).collect()


def _pandas_sql_executor(query, context):
    """Execute SQL on Pandas DataFrames via DuckDB."""
    import duckdb

    conn = duckdb.connect()
    for name in context.list_names():
        conn.register(name, context.get(name))
    return conn.execute(query).fetchdf()


def _make_polars_ctx(df, datasets=None):
    ctx = PolarsContext()
    if datasets:
        for name, d in datasets.items():
            ctx.register(name, d)
    return EngineContext(
        context=ctx,
        df=df,
        engine_type=EngineType.POLARS,
        engine=PolarsEngine(),
        sql_executor=_polars_sql_executor,
    )


def _make_pandas_ctx(df, datasets=None):
    ctx = PandasContext()
    if datasets:
        for name, d in datasets.items():
            ctx.register(name, d)
    return EngineContext(
        context=ctx,
        df=df,
        engine_type=EngineType.PANDAS,
        engine=PandasEngine(config={}),
        sql_executor=_pandas_sql_executor,
    )


# ---------------------------------------------------------------------------
# Pivot — new Polars branch
# ---------------------------------------------------------------------------


class TestPivotPolars:
    def test_basic_sum(self):
        df = pl.DataFrame(
            {"id": [1, 1, 2, 2], "cat": ["A", "B", "A", "B"], "val": [10, 20, 30, 40]}
        )
        result = pivot(
            _make_polars_ctx(df),
            PivotParams(group_by=["id"], pivot_col="cat", agg_col="val", agg_func="sum"),
        )
        assert result.df.shape == (2, 3)
        assert set(result.df.columns) == {"id", "A", "B"}
        row1 = result.df.filter(pl.col("id") == 1)
        assert row1["A"][0] == 10
        assert row1["B"][0] == 20

    def test_avg_maps_to_mean(self):
        df = pl.DataFrame(
            {"id": [1, 1, 2, 2], "cat": ["A", "A", "B", "B"], "val": [10.0, 30.0, 20.0, 40.0]}
        )
        result = pivot(
            _make_polars_ctx(df),
            PivotParams(group_by=["id"], pivot_col="cat", agg_col="val", agg_func="avg"),
        )
        row1 = result.df.filter(pl.col("id") == 1)
        assert row1["A"][0] == pytest.approx(20.0)

    def test_agg_max(self):
        df = pl.DataFrame(
            {"id": [1, 1, 2, 2], "cat": ["A", "A", "B", "B"], "val": [10, 30, 20, 40]}
        )
        result = pivot(
            _make_polars_ctx(df),
            PivotParams(group_by=["id"], pivot_col="cat", agg_col="val", agg_func="max"),
        )
        row1 = result.df.filter(pl.col("id") == 1)
        assert row1["A"][0] == 30

    def test_lazyframe_collected(self):
        lf = pl.DataFrame({"id": [1, 2], "cat": ["A", "B"], "val": [10, 20]}).lazy()
        result = pivot(
            _make_polars_ctx(lf),
            PivotParams(group_by=["id"], pivot_col="cat", agg_col="val"),
        )
        assert isinstance(result.df, (pl.DataFrame, pl.LazyFrame))
        assert result.df.shape[0] == 2

    def test_parity_with_pandas(self):
        data = {"id": [1, 1, 2, 2], "cat": ["A", "B", "A", "B"], "val": [10, 20, 30, 40]}
        params = PivotParams(group_by=["id"], pivot_col="cat", agg_col="val", agg_func="sum")
        pd_r = pivot(_make_pandas_ctx(pd.DataFrame(data)), params)
        pl_r = pivot(_make_polars_ctx(pl.DataFrame(data)), params)
        pd_df = pd_r.df.sort_values("id").reset_index(drop=True)
        pl_df = pl_r.df.to_pandas().sort_values("id").reset_index(drop=True)
        pd_df.columns = [str(c) for c in pd_df.columns]
        pl_df.columns = [str(c) for c in pl_df.columns]
        pd.testing.assert_frame_equal(pd_df, pl_df, check_dtype=False)


# ---------------------------------------------------------------------------
# Unpivot — new Polars branch
# ---------------------------------------------------------------------------


class TestUnpivotPolars:
    def test_basic(self):
        df = pl.DataFrame({"pid": [1, 2], "jan": [100, 200], "feb": [150, 250]})
        result = unpivot(
            _make_polars_ctx(df),
            UnpivotParams(
                id_cols=["pid"], value_vars=["jan", "feb"], var_name="month", value_name="sales"
            ),
        )
        assert result.df.shape == (4, 3)
        assert set(result.df.columns) == {"pid", "month", "sales"}

    def test_preserves_values(self):
        df = pl.DataFrame({"id": [1], "a": [10], "b": [20], "c": [30]})
        result = unpivot(
            _make_polars_ctx(df),
            UnpivotParams(
                id_cols=["id"], value_vars=["a", "b", "c"], var_name="key", value_name="val"
            ),
        )
        assert result.df.shape == (3, 3)
        values = sorted(result.df["val"].to_list())
        assert values == [10, 20, 30]

    def test_lazyframe_collected(self):
        lf = pl.DataFrame({"id": [1, 2], "x": [10, 20], "y": [30, 40]}).lazy()
        result = unpivot(
            _make_polars_ctx(lf),
            UnpivotParams(id_cols=["id"], value_vars=["x", "y"]),
        )
        assert result.df.shape == (4, 3)

    def test_parity_with_pandas(self):
        data = {"pid": [1, 2], "jan": [100, 200], "feb": [150, 250]}
        params = UnpivotParams(
            id_cols=["pid"], value_vars=["jan", "feb"], var_name="m", value_name="v"
        )
        pd_r = unpivot(_make_pandas_ctx(pd.DataFrame(data)), params)
        pl_r = unpivot(_make_polars_ctx(pl.DataFrame(data)), params)
        pd_df = pd_r.df.sort_values(["pid", "m"]).reset_index(drop=True)
        pl_df = pl_r.df.to_pandas().sort_values(["pid", "m"]).reset_index(drop=True)
        pd.testing.assert_frame_equal(pd_df, pl_df, check_dtype=False)


# ---------------------------------------------------------------------------
# Join — implicit Polars support via SQL fallback
# ---------------------------------------------------------------------------


class TestJoinPolars:
    def test_left_join(self):
        left = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        right = pl.DataFrame({"id": [1, 2, 4], "score": [100, 200, 400]})
        result = join(
            _make_polars_ctx(left, datasets={"scores": right}),
            JoinParams(right_dataset="scores", on=["id"], how="left"),
        )
        assert result.df.shape[0] == 3
        assert "score" in result.df.columns

    def test_inner_join(self):
        left = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        right = pl.DataFrame({"id": [1, 2, 4], "score": [100, 200, 400]})
        result = join(
            _make_polars_ctx(left, datasets={"scores": right}),
            JoinParams(right_dataset="scores", on=["id"], how="inner"),
        )
        assert result.df.shape[0] == 2


# ---------------------------------------------------------------------------
# Union — implicit Polars support via SQL
# ---------------------------------------------------------------------------


class TestUnionPolars:
    def test_basic_union(self):
        df_a = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        df_b = pl.DataFrame({"id": [3, 4], "name": ["c", "d"]})
        result = union(
            _make_polars_ctx(df_a, datasets={"other": df_b}),
            UnionParams(datasets=["other"]),
        )
        assert result.df.shape[0] == 4

    def test_union_preserves_all_rows(self):
        df_a = pl.DataFrame({"x": [1]})
        df_b = pl.DataFrame({"x": [1]})  # duplicate — should be kept
        result = union(
            _make_polars_ctx(df_a, datasets={"dup": df_b}),
            UnionParams(datasets=["dup"]),
        )
        assert result.df.shape[0] == 2


# ---------------------------------------------------------------------------
# Aggregate — implicit Polars support via SQL
# ---------------------------------------------------------------------------


class TestAggregatePolars:
    def test_sum(self):
        df = pl.DataFrame({"cat": ["A", "A", "B", "B"], "val": [10, 20, 30, 40]})
        result = aggregate(
            _make_polars_ctx(df),
            AggregateParams(group_by=["cat"], aggregations={"val": AggFunc.SUM}),
        )
        assert result.df.shape[0] == 2

    def test_parity_with_pandas(self):
        data = {"cat": ["A", "A", "B"], "val": [10, 20, 30]}
        params = AggregateParams(group_by=["cat"], aggregations={"val": AggFunc.SUM})
        pd_r = aggregate(_make_pandas_ctx(pd.DataFrame(data)), params)
        pl_r = aggregate(_make_polars_ctx(pl.DataFrame(data)), params)
        pd_df = pd_r.df.sort_values("cat").reset_index(drop=True)
        pl_df = pl_r.df.to_pandas().sort_values("cat").reset_index(drop=True)
        pd_df.columns = [str(c) for c in pd_df.columns]
        pl_df.columns = [str(c) for c in pl_df.columns]
        pd.testing.assert_frame_equal(pd_df, pl_df, check_dtype=False)

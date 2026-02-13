"""Unit tests for odibi/transformers/relational.py — Pandas paths."""

from typing import Optional

import pandas as pd
import pytest
from pydantic import ValidationError

from odibi.context import EngineContext, PandasContext
from odibi.engine.pandas_engine import PandasEngine
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


def _make_context(df: pd.DataFrame, extras: Optional[dict] = None) -> EngineContext:
    """Build a real EngineContext backed by PandasContext + DuckDB sql_executor."""
    ctx = PandasContext()
    ctx.register("df", df)
    if extras:
        for name, extra_df in extras.items():
            ctx.register(name, extra_df)
    engine = PandasEngine()
    return EngineContext(
        context=ctx,
        df=df,
        engine_type=EngineType.PANDAS,
        sql_executor=engine.execute_sql,
    )


# =========================================================================
# 1. JoinParams validation
# =========================================================================


class TestJoinParams:
    def test_coerce_string_to_list(self):
        p = JoinParams(right_dataset="r", on="col_a")
        assert p.on == ["col_a"]

    def test_list_passes_through(self):
        p = JoinParams(right_dataset="r", on=["x", "y"])
        assert p.on == ["x", "y"]

    def test_empty_list_raises(self):
        with pytest.raises(ValidationError, match="at least one join key"):
            JoinParams(right_dataset="r", on=[])


# =========================================================================
# 2. join — Pandas path
# =========================================================================


class TestJoinPandas:
    @pytest.fixture()
    def left(self):
        return pd.DataFrame({"id": [1, 2, 3], "val_l": ["a", "b", "c"]})

    @pytest.fixture()
    def right(self):
        return pd.DataFrame({"id": [2, 3, 4], "val_r": ["x", "y", "z"]})

    def test_anti_join(self, left, right):
        ctx = _make_context(left, extras={"right_ds": right})
        params = JoinParams(right_dataset="right_ds", on="id", how="anti")
        result = join(ctx, params)
        res = result.df.sort_values("id").reset_index(drop=True)
        assert list(res["id"]) == [1]
        assert "val_r" not in res.columns

    def test_semi_join(self, left, right):
        ctx = _make_context(left, extras={"right_ds": right})
        params = JoinParams(right_dataset="right_ds", on="id", how="semi")
        result = join(ctx, params)
        res = result.df.sort_values("id").reset_index(drop=True)
        assert list(res["id"]) == [2, 3]
        assert "val_r" not in res.columns

    def test_full_outer_join(self, left, right):
        ctx = _make_context(left, extras={"right_ds": right})
        params = JoinParams(right_dataset="right_ds", on="id", how="full")
        result = join(ctx, params)
        res = result.df.sort_values("id").reset_index(drop=True)
        assert set(res["id"]) == {1, 2, 3, 4}

    def test_prefix_collision(self, left):
        right = pd.DataFrame({"id": [2], "val_l": ["collision"]})
        ctx = _make_context(left, extras={"right_ds": right})
        params = JoinParams(right_dataset="right_ds", on="id", how="inner", prefix="rhs")
        result = join(ctx, params)
        assert "val_l_rhs" in result.df.columns

    def test_right_dataset_not_found(self, left):
        ctx = _make_context(left)
        params = JoinParams(right_dataset="missing", on="id")
        with pytest.raises(KeyError, match="not found"):
            join(ctx, params)


# =========================================================================
# 3. UnionParams validation
# =========================================================================


class TestUnionParams:
    def test_valid(self):
        p = UnionParams(datasets=["a", "b"])
        assert p.by_name is True

    def test_by_name_false(self):
        p = UnionParams(datasets=["x"], by_name=False)
        assert p.by_name is False


# =========================================================================
# 4. union — Pandas path
# =========================================================================


class TestUnionPandas:
    def test_union_by_name(self):
        df1 = pd.DataFrame({"a": [1], "b": [10]})
        df2 = pd.DataFrame({"b": [20], "a": [2]})  # swapped column order
        ctx = _make_context(df1, extras={"ds2": df2})
        params = UnionParams(datasets=["ds2"], by_name=True)
        result = union(ctx, params)
        res = result.df.sort_values("a").reset_index(drop=True)
        assert list(res["a"]) == [1, 2]
        assert list(res["b"]) == [10, 20]

    def test_union_by_position(self):
        df1 = pd.DataFrame({"x": [1], "y": [10]})
        df2 = pd.DataFrame({"x": [2], "y": [20]})
        ctx = _make_context(df1, extras={"ds2": df2})
        params = UnionParams(datasets=["ds2"], by_name=False)
        result = union(ctx, params)
        assert len(result.df) == 2

    def test_union_dedup_via_sql(self):
        """DuckDB UNION ALL keeps duplicates; verify row count."""
        df1 = pd.DataFrame({"a": [1, 2]})
        df2 = pd.DataFrame({"a": [2, 3]})
        ctx = _make_context(df1, extras={"ds2": df2})
        params = UnionParams(datasets=["ds2"])
        result = union(ctx, params)
        # UNION ALL → 4 rows (2+2), including duplicate id=2
        assert len(result.df) == 4

    def test_union_multiple_datasets(self):
        df1 = pd.DataFrame({"col": [1]})
        df2 = pd.DataFrame({"col": [2]})
        df3 = pd.DataFrame({"col": [3]})
        ctx = _make_context(df1, extras={"ds2": df2, "ds3": df3})
        params = UnionParams(datasets=["ds2", "ds3"])
        result = union(ctx, params)
        assert len(result.df) == 3


# =========================================================================
# 5. PivotParams validation
# =========================================================================


class TestPivotParams:
    def test_defaults(self):
        p = PivotParams(group_by=["g"], pivot_col="p", agg_col="v")
        assert p.agg_func == "sum"
        assert p.values is None

    def test_all_agg_funcs(self):
        for func in ("sum", "count", "avg", "max", "min", "first"):
            p = PivotParams(group_by=["g"], pivot_col="p", agg_col="v", agg_func=func)
            assert p.agg_func == func


# =========================================================================
# 6. pivot — Pandas path
# =========================================================================


class TestPivotPandas:
    def test_pivot_avg(self):
        df = pd.DataFrame(
            {
                "product": ["A", "A", "B", "B"],
                "month": ["Jan", "Feb", "Jan", "Feb"],
                "sales": [10, 20, 30, 40],
            }
        )
        ctx = _make_context(df)
        params = PivotParams(
            group_by=["product"], pivot_col="month", agg_col="sales", agg_func="avg"
        )
        result = pivot(ctx, params)
        res = result.df.sort_values("product").reset_index(drop=True)
        assert "Jan" in res.columns or ("month", "Jan") in (
            getattr(res.columns, "tolist", lambda: [])()
        )
        # Flatten multi-index columns if present
        if hasattr(res.columns, "levels"):
            res.columns = [c[1] if isinstance(c, tuple) else c for c in res.columns]
        assert len(res) == 2

    def test_pivot_unsupported_engine(self):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        ctx = _make_context(df)
        ctx.engine_type = EngineType.POLARS
        params = PivotParams(group_by=["a"], pivot_col="b", agg_col="c")
        with pytest.raises(ValueError, match="does not support engine type"):
            pivot(ctx, params)


# =========================================================================
# 7. UnpivotParams validation
# =========================================================================


class TestUnpivotParams:
    def test_defaults(self):
        p = UnpivotParams(id_cols=["id"], value_vars=["a", "b"])
        assert p.var_name == "variable"
        assert p.value_name == "value"

    def test_custom_names(self):
        p = UnpivotParams(id_cols=["id"], value_vars=["x"], var_name="metric", value_name="amt")
        assert p.var_name == "metric"
        assert p.value_name == "amt"


# =========================================================================
# 8. unpivot — Pandas path
# =========================================================================


class TestUnpivotPandas:
    def test_basic_unpivot(self):
        df = pd.DataFrame({"product_id": [1, 2], "jan_sales": [10, 20], "feb_sales": [30, 40]})
        ctx = _make_context(df)
        params = UnpivotParams(
            id_cols=["product_id"],
            value_vars=["jan_sales", "feb_sales"],
            var_name="month",
            value_name="sales",
        )
        result = unpivot(ctx, params)
        res = result.df
        assert len(res) == 4  # 2 products × 2 vars
        assert "month" in res.columns
        assert "sales" in res.columns
        assert set(res["month"]) == {"jan_sales", "feb_sales"}

    def test_unpivot_unsupported_engine(self):
        df = pd.DataFrame({"id": [1], "a": [2], "b": [3]})
        ctx = _make_context(df)
        ctx.engine_type = EngineType.POLARS
        params = UnpivotParams(id_cols=["id"], value_vars=["a", "b"])
        with pytest.raises(ValueError, match="does not support engine type"):
            unpivot(ctx, params)


# =========================================================================
# 9. AggregateParams validation
# =========================================================================


class TestAggregateParams:
    def test_valid(self):
        p = AggregateParams(
            group_by=["dept"],
            aggregations={"salary": AggFunc.SUM, "emp": AggFunc.COUNT},
        )
        assert p.aggregations["salary"] == AggFunc.SUM

    def test_string_coercion(self):
        p = AggregateParams(group_by=["dept"], aggregations={"salary": "avg"})
        assert p.aggregations["salary"] == AggFunc.AVG


# =========================================================================
# 10. aggregate — Pandas path (via DuckDB sql_executor)
# =========================================================================


class TestAggregatePandas:
    def test_group_by_sum_avg_count(self):
        df = pd.DataFrame(
            {
                "dept": ["A", "A", "B", "B"],
                "salary": [100, 200, 300, 400],
                "emp_id": [1, 2, 3, 4],
            }
        )
        ctx = _make_context(df)
        params = AggregateParams(
            group_by=["dept"],
            aggregations={
                "salary": AggFunc.SUM,
                "emp_id": AggFunc.COUNT,
            },
        )
        result = aggregate(ctx, params)
        res = result.df.sort_values("dept").reset_index(drop=True)
        assert len(res) == 2
        assert res.loc[res["dept"] == "A", "salary"].values[0] == 300
        assert res.loc[res["dept"] == "B", "salary"].values[0] == 700
        assert res.loc[res["dept"] == "A", "emp_id"].values[0] == 2

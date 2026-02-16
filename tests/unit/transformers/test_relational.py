import logging

import duckdb
import pandas as pd
import pytest
from pydantic import ValidationError

from odibi.context import EngineContext, PandasContext
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
    unpivot,
)

logging.getLogger("odibi").propagate = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_pandas_context(df, other_dfs=None, sql_executor=None):
    ctx = PandasContext()
    ctx.register("df", df)
    if other_dfs:
        for name, other_df in other_dfs.items():
            ctx.register(name, other_df)
    return EngineContext(
        context=ctx,
        df=df,
        engine_type=EngineType.PANDAS,
        sql_executor=sql_executor,
    )


def pandas_sql_executor(query, context):
    con = duckdb.connect(":memory:")
    for name in context.list_names():
        df = context.get(name)
        con.register(name, df)
    result = con.execute(query).fetchdf()
    con.close()
    return result


# ===========================================================================
# Pydantic Model Validation
# ===========================================================================


class TestJoinParamsValidation:
    def test_string_on_coerced_to_list(self):
        p = JoinParams(right_dataset="other", on="id")
        assert p.on == ["id"]

    def test_list_on_preserved(self):
        p = JoinParams(right_dataset="other", on=["a", "b"])
        assert p.on == ["a", "b"]

    def test_empty_on_raises(self):
        with pytest.raises(ValidationError, match="at least one join key"):
            JoinParams(right_dataset="other", on=[])

    def test_defaults(self):
        p = JoinParams(right_dataset="other", on="id")
        assert p.how == "left"
        assert p.prefix is None


class TestUnionParamsValidation:
    def test_basic(self):
        p = UnionParams(datasets=["a", "b"])
        assert p.datasets == ["a", "b"]
        assert p.by_name is True

    def test_datasets_required(self):
        with pytest.raises(ValidationError):
            UnionParams()


class TestPivotParamsValidation:
    def test_basic(self):
        p = PivotParams(group_by=["id"], pivot_col="month", agg_col="sales")
        assert p.agg_func == "sum"
        assert p.values is None

    def test_custom_agg_func(self):
        p = PivotParams(group_by=["id"], pivot_col="m", agg_col="v", agg_func="count")
        assert p.agg_func == "count"


class TestUnpivotParamsValidation:
    def test_defaults(self):
        p = UnpivotParams(id_cols=["id"], value_vars=["a", "b"])
        assert p.var_name == "variable"
        assert p.value_name == "value"

    def test_custom_names(self):
        p = UnpivotParams(id_cols=["id"], value_vars=["a"], var_name="metric", value_name="val")
        assert p.var_name == "metric"
        assert p.value_name == "val"


class TestAggFuncEnum:
    @pytest.mark.parametrize(
        "member,value",
        [
            (AggFunc.SUM, "sum"),
            (AggFunc.AVG, "avg"),
            (AggFunc.MIN, "min"),
            (AggFunc.MAX, "max"),
            (AggFunc.COUNT, "count"),
            (AggFunc.FIRST, "first"),
        ],
    )
    def test_values(self, member, value):
        assert member.value == value

    def test_member_count(self):
        assert len(AggFunc) == 6


class TestAggregateParamsValidation:
    def test_basic(self):
        p = AggregateParams(
            group_by=["dept"],
            aggregations={"salary": AggFunc.SUM, "age": AggFunc.AVG},
        )
        assert p.group_by == ["dept"]
        assert p.aggregations["salary"] == AggFunc.SUM

    def test_required_fields(self):
        with pytest.raises(ValidationError):
            AggregateParams()


# ===========================================================================
# Pandas Engine – join()
# ===========================================================================


class TestJoinPandas:
    @pytest.fixture
    def left_df(self):
        return pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})

    @pytest.fixture
    def right_df(self):
        return pd.DataFrame({"id": [2, 3, 4], "score": [10, 20, 30]})

    def test_inner_join(self, left_df, right_df):
        ctx = make_pandas_context(left_df, {"right": right_df})
        params = JoinParams(right_dataset="right", on="id", how="inner")
        result = join(ctx, params)
        assert result.df.shape[0] == 2
        assert set(result.df["id"]) == {2, 3}
        assert "score" in result.df.columns

    def test_left_join(self, left_df, right_df):
        ctx = make_pandas_context(left_df, {"right": right_df})
        params = JoinParams(right_dataset="right", on="id", how="left")
        result = join(ctx, params)
        assert result.df.shape[0] == 3
        assert set(result.df["id"]) == {1, 2, 3}

    def test_anti_join(self, left_df, right_df):
        ctx = make_pandas_context(left_df, {"right": right_df})
        params = JoinParams(right_dataset="right", on="id", how="anti")
        result = join(ctx, params)
        assert result.df.shape[0] == 1
        assert result.df["id"].iloc[0] == 1
        assert "_merge" not in result.df.columns

    def test_semi_join(self, left_df, right_df):
        ctx = make_pandas_context(left_df, {"right": right_df})
        params = JoinParams(right_dataset="right", on="id", how="semi")
        result = join(ctx, params)
        assert result.df.shape[0] == 2
        assert set(result.df["id"]) == {2, 3}
        assert "score" not in result.df.columns

    def test_join_with_prefix(self, left_df):
        right = pd.DataFrame({"id": [1, 2], "val": [100, 200]})
        ctx = make_pandas_context(left_df, {"right": right})
        params = JoinParams(right_dataset="right", on="id", how="inner", prefix="r")
        result = join(ctx, params)
        assert "val" in result.df.columns
        assert "val_r" in result.df.columns

    def test_missing_right_dataset_raises(self, left_df):
        ctx = make_pandas_context(left_df)
        params = JoinParams(right_dataset="nonexistent", on="id")
        with pytest.raises((ValueError, KeyError)):
            join(ctx, params)

    def test_default_suffix_uses_dataset_name(self, left_df):
        right = pd.DataFrame({"id": [1], "val": [99]})
        ctx = make_pandas_context(left_df, {"orders": right})
        params = JoinParams(right_dataset="orders", on="id", how="inner")
        result = join(ctx, params)
        assert "val_orders" in result.df.columns


# ===========================================================================
# Pandas Engine – unpivot()
# ===========================================================================


class TestUnpivotPandas:
    def test_basic_melt(self):
        df = pd.DataFrame({"product": ["A", "B"], "jan": [10, 20], "feb": [30, 40]})
        ctx = make_pandas_context(df)
        params = UnpivotParams(id_cols=["product"], value_vars=["jan", "feb"])
        result = unpivot(ctx, params)
        assert result.df.shape[0] == 4
        assert "variable" in result.df.columns
        assert "value" in result.df.columns

    def test_custom_var_and_value_names(self):
        df = pd.DataFrame({"id": [1], "x": [100], "y": [200]})
        ctx = make_pandas_context(df)
        params = UnpivotParams(
            id_cols=["id"], value_vars=["x", "y"], var_name="metric", value_name="measure"
        )
        result = unpivot(ctx, params)
        assert "metric" in result.df.columns
        assert "measure" in result.df.columns
        assert set(result.df["metric"]) == {"x", "y"}


# ===========================================================================
# Pandas Engine – pivot()
# ===========================================================================


class TestPivotPandas:
    def test_basic_pivot_sum(self):
        df = pd.DataFrame(
            {
                "region": ["N", "N", "S", "S"],
                "month": ["Jan", "Feb", "Jan", "Feb"],
                "sales": [10, 20, 30, 40],
            }
        )
        ctx = make_pandas_context(df)
        params = PivotParams(
            group_by=["region"], pivot_col="month", agg_col="sales", agg_func="sum"
        )
        result = pivot(ctx, params)
        assert result.df.shape[0] == 2
        cols = [str(c) for c in result.df.columns]
        assert "Jan" in cols or ("month", "Jan") in [
            (str(c[0]), str(c[1])) for c in result.df.columns
            if isinstance(c, tuple)
        ]

    def test_unsupported_engine_raises(self):
        df = pd.DataFrame({"a": [1], "b": ["x"], "c": [10]})
        pandas_ctx = PandasContext()
        pandas_ctx.register("df", df)
        ctx = EngineContext(
            context=pandas_ctx,
            df=df,
            engine_type=EngineType.POLARS,
        )
        params = PivotParams(group_by=["a"], pivot_col="b", agg_col="c")
        with pytest.raises(ValueError, match="does not support engine type"):
            pivot(ctx, params)


# ===========================================================================
# Aggregate (via DuckDB sql_executor)
# ===========================================================================


class TestAggregatePandas:
    def test_sum_aggregation(self):
        df = pd.DataFrame(
            {"dept": ["A", "A", "B", "B"], "salary": [100, 200, 300, 400]}
        )
        ctx = make_pandas_context(df, sql_executor=pandas_sql_executor)
        params = AggregateParams(
            group_by=["dept"], aggregations={"salary": AggFunc.SUM}
        )
        result = aggregate(ctx, params)
        result_df = result.df.sort_values("dept").reset_index(drop=True)
        assert result_df.loc[0, "salary"] == 300
        assert result_df.loc[1, "salary"] == 700

    def test_count_aggregation(self):
        df = pd.DataFrame(
            {"dept": ["A", "A", "B"], "emp_id": [1, 2, 3]}
        )
        ctx = make_pandas_context(df, sql_executor=pandas_sql_executor)
        params = AggregateParams(
            group_by=["dept"], aggregations={"emp_id": AggFunc.COUNT}
        )
        result = aggregate(ctx, params)
        result_df = result.df.sort_values("dept").reset_index(drop=True)
        assert result_df.loc[0, "emp_id"] == 2
        assert result_df.loc[1, "emp_id"] == 1

    def test_multiple_aggregations(self):
        df = pd.DataFrame(
            {"dept": ["A", "A", "B"], "salary": [10, 20, 30], "age": [25, 35, 45]}
        )
        ctx = make_pandas_context(df, sql_executor=pandas_sql_executor)
        params = AggregateParams(
            group_by=["dept"],
            aggregations={"salary": AggFunc.SUM, "age": AggFunc.AVG},
        )
        result = aggregate(ctx, params)
        result_df = result.df.sort_values("dept").reset_index(drop=True)
        assert result_df.loc[0, "salary"] == 30
        assert result_df.loc[0, "age"] == 30.0

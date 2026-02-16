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
    union,
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
            (str(c[0]), str(c[1])) for c in result.df.columns if isinstance(c, tuple)
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
        df = pd.DataFrame({"dept": ["A", "A", "B", "B"], "salary": [100, 200, 300, 400]})
        ctx = make_pandas_context(df, sql_executor=pandas_sql_executor)
        params = AggregateParams(group_by=["dept"], aggregations={"salary": AggFunc.SUM})
        result = aggregate(ctx, params)
        result_df = result.df.sort_values("dept").reset_index(drop=True)
        assert result_df.loc[0, "salary"] == 300
        assert result_df.loc[1, "salary"] == 700

    def test_count_aggregation(self):
        df = pd.DataFrame({"dept": ["A", "A", "B"], "emp_id": [1, 2, 3]})
        ctx = make_pandas_context(df, sql_executor=pandas_sql_executor)
        params = AggregateParams(group_by=["dept"], aggregations={"emp_id": AggFunc.COUNT})
        result = aggregate(ctx, params)
        result_df = result.df.sort_values("dept").reset_index(drop=True)
        assert result_df.loc[0, "emp_id"] == 2
        assert result_df.loc[1, "emp_id"] == 1

    def test_multiple_aggregations(self):
        df = pd.DataFrame({"dept": ["A", "A", "B"], "salary": [10, 20, 30], "age": [25, 35, 45]})
        ctx = make_pandas_context(df, sql_executor=pandas_sql_executor)
        params = AggregateParams(
            group_by=["dept"],
            aggregations={"salary": AggFunc.SUM, "age": AggFunc.AVG},
        )
        result = aggregate(ctx, params)
        result_df = result.df.sort_values("dept").reset_index(drop=True)
        assert result_df.loc[0, "salary"] == 30
        assert result_df.loc[0, "age"] == 30.0


# -------------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------------


@pytest.fixture
def left_df():
    """Left dataset for join tests."""
    return pd.DataFrame(
        {
            "customer_id": [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Charlie", "David"],
            "amount": [100, 200, 300, 400],
        }
    )


@pytest.fixture
def right_df():
    """Right dataset for join tests."""
    return pd.DataFrame(
        {
            "customer_id": [2, 3, 4, 5],
            "region": ["East", "West", "North", "South"],
            "amount": [20, 30, 40, 50],  # collision column
        }
    )


@pytest.fixture
def empty_df():
    """Empty DataFrame for edge case testing."""
    return pd.DataFrame(columns=["a", "b", "c"])


@pytest.fixture
def sales_2023():
    """Dataset for union tests."""
    return pd.DataFrame(
        {
            "product": ["A", "B"],
            "sales": [100, 200],
            "year": [2023, 2023],
        }
    )


@pytest.fixture
def sales_2024():
    """Dataset for union tests."""
    return pd.DataFrame(
        {
            "product": ["C", "D"],
            "sales": [300, 400],
            "year": [2024, 2024],
        }
    )


@pytest.fixture
def pivot_df():
    """Dataset for pivot tests."""
    return pd.DataFrame(
        {
            "product_id": ["P1", "P1", "P2", "P2"],
            "region": ["East", "West", "East", "West"],
            "month": ["Jan", "Jan", "Jan", "Jan"],
            "sales": [100, 150, 200, 250],
        }
    )


@pytest.fixture
def unpivot_df():
    """Dataset for unpivot tests."""
    return pd.DataFrame(
        {
            "product_id": ["P1", "P2"],
            "jan_sales": [100, 200],
            "feb_sales": [110, 210],
            "mar_sales": [120, 220],
        }
    )


@pytest.fixture
def aggregate_df():
    """Dataset for aggregation tests."""
    return pd.DataFrame(
        {
            "department": ["Sales", "Sales", "IT", "IT"],
            "region": ["East", "West", "East", "West"],
            "salary": [50000, 60000, 70000, 80000],
            "employee_id": [1, 2, 3, 4],
        }
    )


def create_context(df, extra_datasets=None):
    """Helper to create EngineContext with optional extra datasets."""
    pandas_ctx = PandasContext()
    pandas_ctx.register("df", df.copy())

    if extra_datasets:
        for name, dataset in extra_datasets.items():
            pandas_ctx.register(name, dataset.copy())

    # Create a simple SQL executor for testing using DuckDB
    def sql_executor(query, context):
        try:
            import duckdb

            conn = duckdb.connect(":memory:")
            # Register all dataframes using list_names()
            for name in context.list_names():
                conn.register(name, context.get(name))
            result = conn.execute(query).fetchdf()
            conn.close()
            return result
        except ImportError:
            # Fallback if duckdb not available (though it should be)
            raise NotImplementedError("DuckDB required for SQL execution in tests")

    return EngineContext(pandas_ctx, df.copy(), EngineType.PANDAS, sql_executor=sql_executor)


# -------------------------------------------------------------------------
# Tests for join
# -------------------------------------------------------------------------


def test_join_left(left_df, right_df):
    """Test basic left join."""
    context = create_context(left_df, {"customers": right_df})
    params = JoinParams(right_dataset="customers", on="customer_id", how="left")
    result_ctx = join(context, params)
    result = result_ctx.df

    # All left rows should be present
    assert len(result) == 4
    assert list(result["customer_id"]) == [1, 2, 3, 4]
    # Check that region is added
    assert "region" in result.columns
    # First row should have null region (no match)
    assert pd.isna(result.loc[result["customer_id"] == 1, "region"].iloc[0])


def test_join_inner(left_df, right_df):
    """Test inner join."""
    context = create_context(left_df, {"customers": right_df})
    params = JoinParams(right_dataset="customers", on="customer_id", how="inner")
    result_ctx = join(context, params)
    result = result_ctx.df

    # Only matching rows should be present
    assert len(result) == 3
    assert sorted(result["customer_id"].tolist()) == [2, 3, 4]


def test_join_right(left_df, right_df):
    """Test right join."""
    context = create_context(left_df, {"customers": right_df})
    params = JoinParams(right_dataset="customers", on="customer_id", how="right")
    result_ctx = join(context, params)
    result = result_ctx.df

    # All right rows should be present
    assert len(result) == 4
    assert sorted(result["customer_id"].tolist()) == [2, 3, 4, 5]
    # Check for null name in unmatched rows
    assert pd.isna(result.loc[result["customer_id"] == 5, "name"].iloc[0])


def test_join_full(left_df, right_df):
    """Test full outer join.

    Verifies that 'full' join type correctly maps to 'outer' for pandas compatibility.
    Full outer join should include all rows from both datasets.
    """
    context = create_context(left_df, {"customers": right_df})
    params = JoinParams(right_dataset="customers", on="customer_id", how="full")
    result_ctx = join(context, params)
    result = result_ctx.df

    # Full outer join should include all rows from both datasets
    # Left has: 1, 2, 3, 4
    # Right has: 2, 3, 4, 5
    # Result should have: 1, 2, 3, 4, 5
    assert len(result) == 5
    assert sorted(result["customer_id"].tolist()) == [1, 2, 3, 4, 5]

    # Check that row 1 (left only) has null region
    assert pd.isna(result.loc[result["customer_id"] == 1, "region"].iloc[0])

    # Check that row 5 (right only) has null name
    assert pd.isna(result.loc[result["customer_id"] == 5, "name"].iloc[0])

    # Check that rows 2, 3, 4 (both sides) have both name and region
    for cid in [2, 3, 4]:
        row = result.loc[result["customer_id"] == cid].iloc[0]
        assert pd.notna(row["name"])
        assert pd.notna(row["region"])


def test_join_with_prefix(left_df, right_df):
    """Test join with prefix to avoid column collisions."""
    context = create_context(left_df, {"customers": right_df})
    params = JoinParams(right_dataset="customers", on="customer_id", how="left", prefix="cust")
    result_ctx = join(context, params)
    result = result_ctx.df

    # Original amount column should be preserved
    assert "amount" in result.columns
    # Right amount column should have suffix format: amount_cust (pandas adds suffix at end)
    assert "amount_cust" in result.columns
    # Verify values
    assert result.loc[0, "amount"] == 100  # left value


def test_join_anti(left_df, right_df):
    """Test anti join (rows in left that don't match right)."""
    context = create_context(left_df, {"customers": right_df})
    params = JoinParams(right_dataset="customers", on="customer_id", how="anti")
    result_ctx = join(context, params)
    result = result_ctx.df

    # Only non-matching left rows
    assert len(result) == 1
    assert result["customer_id"].iloc[0] == 1
    # Right columns should not be present
    assert "region" not in result.columns


def test_join_semi(left_df, right_df):
    """Test semi join (rows in left that match right, no columns from right)."""
    context = create_context(left_df, {"customers": right_df})
    params = JoinParams(right_dataset="customers", on="customer_id", how="semi")
    result_ctx = join(context, params)
    result = result_ctx.df

    # Only matching left rows
    assert len(result) == 3
    assert sorted(result["customer_id"].tolist()) == [2, 3, 4]
    # Right columns should not be present
    assert "region" not in result.columns


def test_join_multiple_keys():
    """Test join on multiple columns."""
    left = pd.DataFrame(
        {
            "key1": [1, 2, 3],
            "key2": ["A", "B", "C"],
            "value": [10, 20, 30],
        }
    )
    right = pd.DataFrame(
        {
            "key1": [1, 2, 4],
            "key2": ["A", "B", "D"],
            "extra": [100, 200, 400],
        }
    )

    context = create_context(left, {"right_data": right})
    params = JoinParams(right_dataset="right_data", on=["key1", "key2"], how="inner")
    result_ctx = join(context, params)
    result = result_ctx.df

    # Only matching rows on both keys
    assert len(result) == 2
    assert sorted(result["value"].tolist()) == [10, 20]


def test_join_empty_left(empty_df, right_df):
    """Test join with empty left DataFrame."""
    # Create empty df with customer_id column to match right_df
    empty_left = pd.DataFrame(columns=["customer_id", "name", "amount"])
    context = create_context(empty_left, {"right_data": right_df})
    params = JoinParams(right_dataset="right_data", on="customer_id", how="left")

    # Should handle gracefully - pandas will return empty result
    result_ctx = join(context, params)
    assert result_ctx.df.empty


def test_join_empty_right(left_df, empty_df):
    """Test join with empty right DataFrame."""
    # Create empty right df with customer_id column
    empty_right = pd.DataFrame(columns=["customer_id", "region", "amount"])
    context = create_context(left_df, {"right_data": empty_right})
    params = JoinParams(right_dataset="right_data", on="customer_id", how="left")

    result_ctx = join(context, params)
    result = result_ctx.df

    # Left join with empty right should preserve all left rows
    assert len(result) == 4


def test_join_missing_dataset(left_df):
    """Test join with missing dataset raises error."""
    context = create_context(left_df)
    params = JoinParams(right_dataset="nonexistent", on="customer_id", how="left")

    # The actual code raises KeyError, not ValueError
    with pytest.raises(KeyError, match="not found in context"):
        join(context, params)


def test_join_null_keys():
    """Test join with null keys."""
    left = pd.DataFrame(
        {
            "id": [1, None, 3],
            "value": ["A", "B", "C"],
        }
    )
    right = pd.DataFrame(
        {
            "id": [1, 2, None],
            "extra": ["X", "Y", "Z"],
        }
    )

    context = create_context(left, {"right_data": right})
    params = JoinParams(right_dataset="right_data", on="id", how="inner")
    result_ctx = join(context, params)
    result = result_ctx.df

    # pandas inner join can keep some null keys in certain versions
    # At minimum we should have the matching id=1
    assert len(result) >= 1
    # Check that id=1 matches correctly
    assert result[result["id"] == 1.0]["value"].iloc[0] == "A"


def test_join_params_on_string():
    """Test JoinParams converts single string to list."""
    params = JoinParams(right_dataset="test", on="id")
    assert params.on == ["id"]


def test_join_params_on_list():
    """Test JoinParams preserves list."""
    params = JoinParams(right_dataset="test", on=["id", "name"])
    assert params.on == ["id", "name"]


def test_join_params_on_empty_raises():
    """Test JoinParams raises error for empty on parameter."""
    with pytest.raises(ValueError, match="at least one join key"):
        JoinParams(right_dataset="test", on=[])


# -------------------------------------------------------------------------
# Tests for union
# -------------------------------------------------------------------------


def test_union_basic(sales_2023, sales_2024):
    """Test basic union by name."""
    context = create_context(sales_2023, {"sales_2024": sales_2024})
    params = UnionParams(datasets=["sales_2024"], by_name=True)
    result_ctx = union(context, params)
    result = result_ctx.df

    # Should have rows from both datasets
    assert len(result) == 4
    assert sorted(result["product"].tolist()) == ["A", "B", "C", "D"]


def test_union_multiple_datasets():
    """Test union with multiple datasets."""
    df1 = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    df2 = pd.DataFrame({"a": [3, 4], "b": ["z", "w"]})
    df3 = pd.DataFrame({"a": [5, 6], "b": ["v", "u"]})

    context = create_context(df1, {"df2": df2, "df3": df3})
    params = UnionParams(datasets=["df2", "df3"], by_name=True)
    result_ctx = union(context, params)
    result = result_ctx.df

    assert len(result) == 6
    assert sorted(result["a"].tolist()) == [1, 2, 3, 4, 5, 6]


def test_union_by_position():
    """Test union by position (not by name)."""
    df1 = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    df2 = pd.DataFrame({"c": [3, 4], "d": ["z", "w"]})

    context = create_context(df1, {"df2": df2})
    params = UnionParams(datasets=["df2"], by_name=False)
    result_ctx = union(context, params)
    result = result_ctx.df

    # Union by position should succeed
    assert len(result) == 4


def test_union_empty_dataset(empty_df, sales_2023):
    """Test union with empty dataset."""
    context = create_context(empty_df, {"sales": sales_2023})
    params = UnionParams(datasets=["sales"], by_name=True)
    result_ctx = union(context, params)
    result = result_ctx.df

    # Should have rows from non-empty dataset
    assert len(result) == 2


def test_union_missing_dataset(sales_2023):
    """Test union with missing dataset raises error."""
    context = create_context(sales_2023)
    params = UnionParams(datasets=["nonexistent"], by_name=True)

    # The actual code raises KeyError, not ValueError
    with pytest.raises(KeyError, match="not found in context"):
        union(context, params)


def test_union_mismatched_columns():
    """Test union with mismatched columns (by_name=True should handle this)."""
    df1 = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    df2 = pd.DataFrame({"a": [3, 4], "c": ["z", "w"]})  # different column 'c'

    context = create_context(df1, {"df2": df2})
    params = UnionParams(datasets=["df2"], by_name=True)
    result_ctx = union(context, params)
    result = result_ctx.df

    # Should handle mismatched columns gracefully
    assert len(result) == 4
    assert "a" in result.columns


# -------------------------------------------------------------------------
# Tests for pivot
# -------------------------------------------------------------------------


def test_pivot_basic(pivot_df):
    """Test basic pivot operation."""
    context = create_context(pivot_df)
    params = PivotParams(
        group_by=["product_id", "region"],
        pivot_col="month",
        agg_col="sales",
        agg_func="sum",
    )
    result_ctx = pivot(context, params)
    result = result_ctx.df

    # Should have pivoted columns
    assert "Jan" in result.columns
    assert len(result) == 4  # 2 products x 2 regions


def test_pivot_count():
    """Test pivot with count aggregation."""
    df = pd.DataFrame(
        {
            "category": ["A", "A", "B", "B"],
            "item": ["X", "X", "Y", "Y"],
            "value": [1, 2, 3, 4],
        }
    )

    context = create_context(df)
    params = PivotParams(
        group_by=["category"],
        pivot_col="item",
        agg_col="value",
        agg_func="count",
    )
    result_ctx = pivot(context, params)
    result = result_ctx.df

    assert "X" in result.columns
    assert "Y" in result.columns
    assert len(result) == 2  # 2 categories


def test_pivot_avg():
    """Test pivot with average aggregation.

    This test verifies that 'avg' is correctly mapped to 'mean' for pandas compatibility.
    """
    df = pd.DataFrame(
        {
            "product": ["A", "A", "B", "B"],
            "month": ["Jan", "Feb", "Jan", "Feb"],
            "sales": [100, 150, 200, 250],
        }
    )

    context = create_context(df)
    params = PivotParams(
        group_by=["product"],
        pivot_col="month",
        agg_col="sales",
        agg_func="avg",
    )
    result_ctx = pivot(context, params)
    result = result_ctx.df

    assert len(result) == 2
    # Check that average values are correctly calculated
    # Product A: Jan=100, Feb=150
    # Product B: Jan=200, Feb=250
    assert result[result["product"] == "A"]["Jan"].values[0] == 100
    assert result[result["product"] == "A"]["Feb"].values[0] == 150
    assert result[result["product"] == "B"]["Jan"].values[0] == 200
    assert result[result["product"] == "B"]["Feb"].values[0] == 250


def test_pivot_avg_mean_equivalence():
    """Test that 'avg' and 'mean' produce identical results.

    This verifies that the mapping from 'avg' to 'mean' works correctly.
    """
    df = pd.DataFrame(
        {
            "product": ["A", "A", "A", "B", "B", "B"],
            "month": ["Jan", "Jan", "Feb", "Jan", "Jan", "Feb"],
            "sales": [100, 120, 150, 200, 220, 250],
        }
    )

    context = create_context(df)

    # Test with 'avg'
    params_avg = PivotParams(
        group_by=["product"],
        pivot_col="month",
        agg_col="sales",
        agg_func="avg",
    )
    result_avg_ctx = pivot(context, params_avg)
    result_avg = result_avg_ctx.df

    # Note: 'mean' is not currently in the Literal type for agg_func,
    # but we can test by directly calling pandas to verify our expectation
    # Instead, we'll verify avg produces mathematically correct means

    # Product A: Jan=(100+120)/2=110, Feb=150
    # Product B: Jan=(200+220)/2=210, Feb=250
    assert result_avg[result_avg["product"] == "A"]["Jan"].values[0] == 110
    assert result_avg[result_avg["product"] == "A"]["Feb"].values[0] == 150
    assert result_avg[result_avg["product"] == "B"]["Jan"].values[0] == 210
    assert result_avg[result_avg["product"] == "B"]["Feb"].values[0] == 250


def test_pivot_max():
    """Test pivot with max aggregation."""
    df = pd.DataFrame(
        {
            "product": ["A", "A", "B", "B"],
            "month": ["Jan", "Feb", "Jan", "Feb"],
            "sales": [100, 150, 200, 250],
        }
    )

    context = create_context(df)
    params = PivotParams(
        group_by=["product"],
        pivot_col="month",
        agg_col="sales",
        agg_func="max",
    )
    result_ctx = pivot(context, params)
    result = result_ctx.df

    assert len(result) == 2


def test_pivot_min():
    """Test pivot with min aggregation."""
    df = pd.DataFrame(
        {
            "product": ["A", "A", "B", "B"],
            "month": ["Jan", "Feb", "Jan", "Feb"],
            "sales": [100, 150, 200, 250],
        }
    )

    context = create_context(df)
    params = PivotParams(
        group_by=["product"],
        pivot_col="month",
        agg_col="sales",
        agg_func="min",
    )
    result_ctx = pivot(context, params)
    result = result_ctx.df

    assert len(result) == 2


def test_pivot_first():
    """Test pivot with first aggregation."""
    df = pd.DataFrame(
        {
            "product": ["A", "A", "B", "B"],
            "month": ["Jan", "Feb", "Jan", "Feb"],
            "sales": [100, 150, 200, 250],
        }
    )

    context = create_context(df)
    params = PivotParams(
        group_by=["product"],
        pivot_col="month",
        agg_col="sales",
        agg_func="first",
    )
    result_ctx = pivot(context, params)
    result = result_ctx.df

    assert len(result) == 2


def test_pivot_with_values():
    """Test pivot with explicit values list."""
    df = pd.DataFrame(
        {
            "product": ["A", "A", "B", "B"],
            "month": ["Jan", "Feb", "Jan", "Feb"],
            "sales": [100, 150, 200, 250],
        }
    )

    context = create_context(df)
    params = PivotParams(
        group_by=["product"],
        pivot_col="month",
        agg_col="sales",
        agg_func="sum",
        values=["Jan", "Feb"],
    )
    result_ctx = pivot(context, params)
    result = result_ctx.df

    assert "Jan" in result.columns
    assert "Feb" in result.columns


def test_pivot_empty(empty_df):
    """Test pivot with empty DataFrame."""
    context = create_context(empty_df)
    params = PivotParams(
        group_by=["a"],
        pivot_col="b",
        agg_col="c",
        agg_func="sum",
    )
    result_ctx = pivot(context, params)
    result = result_ctx.df

    assert result.empty


# -------------------------------------------------------------------------
# Tests for unpivot
# -------------------------------------------------------------------------


def test_unpivot_basic(unpivot_df):
    """Test basic unpivot operation."""
    context = create_context(unpivot_df)
    params = UnpivotParams(
        id_cols=["product_id"],
        value_vars=["jan_sales", "feb_sales", "mar_sales"],
        var_name="month",
        value_name="sales",
    )
    result_ctx = unpivot(context, params)
    result = result_ctx.df

    # Should have 2 products * 3 months = 6 rows
    assert len(result) == 6
    assert "month" in result.columns
    assert "sales" in result.columns
    assert sorted(result["product_id"].unique().tolist()) == ["P1", "P2"]


def test_unpivot_single_column():
    """Test unpivot with single value column."""
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "value": [10, 20],
        }
    )

    context = create_context(df)
    params = UnpivotParams(
        id_cols=["id"],
        value_vars=["value"],
        var_name="variable",
        value_name="val",
    )
    result_ctx = unpivot(context, params)
    result = result_ctx.df

    assert len(result) == 2
    assert "variable" in result.columns
    assert "val" in result.columns


def test_unpivot_multiple_id_cols():
    """Test unpivot with multiple ID columns."""
    df = pd.DataFrame(
        {
            "product": ["A", "B"],
            "region": ["East", "West"],
            "q1": [100, 200],
            "q2": [110, 210],
        }
    )

    context = create_context(df)
    params = UnpivotParams(
        id_cols=["product", "region"],
        value_vars=["q1", "q2"],
        var_name="quarter",
        value_name="sales",
    )
    result_ctx = unpivot(context, params)
    result = result_ctx.df

    assert len(result) == 4  # 2 products * 2 quarters
    assert "product" in result.columns
    assert "region" in result.columns


def test_unpivot_custom_names():
    """Test unpivot with custom variable and value names."""
    df = pd.DataFrame(
        {
            "id": [1],
            "col_a": [10],
            "col_b": [20],
        }
    )

    context = create_context(df)
    params = UnpivotParams(
        id_cols=["id"],
        value_vars=["col_a", "col_b"],
        var_name="custom_var",
        value_name="custom_val",
    )
    result_ctx = unpivot(context, params)
    result = result_ctx.df

    assert "custom_var" in result.columns
    assert "custom_val" in result.columns


def test_unpivot_empty(empty_df):
    """Test unpivot with empty DataFrame."""
    context = create_context(empty_df)
    params = UnpivotParams(
        id_cols=["a"],
        value_vars=["b", "c"],
        var_name="variable",
        value_name="value",
    )
    result_ctx = unpivot(context, params)
    result = result_ctx.df

    assert result.empty


# -------------------------------------------------------------------------
# Tests for aggregate
# -------------------------------------------------------------------------


def test_aggregate_basic(aggregate_df):
    """Test basic aggregation."""
    context = create_context(aggregate_df)
    params = AggregateParams(
        group_by=["department"],
        aggregations={"salary": AggFunc.SUM},
    )
    result_ctx = aggregate(context, params)
    result = result_ctx.df

    # Should have 2 departments
    assert len(result) == 2
    assert sorted(result["department"].tolist()) == ["IT", "Sales"]
    # Check sum values
    assert result.loc[result["department"] == "IT", "salary"].iloc[0] == 150000
    assert result.loc[result["department"] == "Sales", "salary"].iloc[0] == 110000


def test_aggregate_multiple_functions(aggregate_df):
    """Test aggregation with multiple functions."""
    context = create_context(aggregate_df)
    params = AggregateParams(
        group_by=["department"],
        aggregations={
            "salary": AggFunc.AVG,
            "employee_id": AggFunc.COUNT,
        },
    )
    result_ctx = aggregate(context, params)
    result = result_ctx.df

    assert len(result) == 2
    assert "salary" in result.columns
    assert "employee_id" in result.columns
    # Check counts
    assert result["employee_id"].iloc[0] == 2
    assert result["employee_id"].iloc[1] == 2


def test_aggregate_multiple_group_by(aggregate_df):
    """Test aggregation with multiple group by columns."""
    context = create_context(aggregate_df)
    params = AggregateParams(
        group_by=["department", "region"],
        aggregations={"salary": AggFunc.SUM},
    )
    result_ctx = aggregate(context, params)
    result = result_ctx.df

    # Should have 4 groups (2 departments x 2 regions)
    assert len(result) == 4


def test_aggregate_max(aggregate_df):
    """Test aggregation with max function."""
    context = create_context(aggregate_df)
    params = AggregateParams(
        group_by=["department"],
        aggregations={"salary": AggFunc.MAX},
    )
    result_ctx = aggregate(context, params)
    result = result_ctx.df

    assert result.loc[result["department"] == "IT", "salary"].iloc[0] == 80000
    assert result.loc[result["department"] == "Sales", "salary"].iloc[0] == 60000


def test_aggregate_min(aggregate_df):
    """Test aggregation with min function."""
    context = create_context(aggregate_df)
    params = AggregateParams(
        group_by=["department"],
        aggregations={"salary": AggFunc.MIN},
    )
    result_ctx = aggregate(context, params)
    result = result_ctx.df

    assert result.loc[result["department"] == "IT", "salary"].iloc[0] == 70000
    assert result.loc[result["department"] == "Sales", "salary"].iloc[0] == 50000


def test_aggregate_avg(aggregate_df):
    """Test aggregation with average function."""
    context = create_context(aggregate_df)
    params = AggregateParams(
        group_by=["department"],
        aggregations={"salary": AggFunc.AVG},
    )
    result_ctx = aggregate(context, params)
    result = result_ctx.df

    assert result.loc[result["department"] == "IT", "salary"].iloc[0] == 75000
    assert result.loc[result["department"] == "Sales", "salary"].iloc[0] == 55000


def test_aggregate_count(aggregate_df):
    """Test aggregation with count function."""
    context = create_context(aggregate_df)
    params = AggregateParams(
        group_by=["department"],
        aggregations={"employee_id": AggFunc.COUNT},
    )
    result_ctx = aggregate(context, params)
    result = result_ctx.df

    assert result["employee_id"].iloc[0] == 2
    assert result["employee_id"].iloc[1] == 2


def test_aggregate_first(aggregate_df):
    """Test aggregation with first function."""
    context = create_context(aggregate_df)
    params = AggregateParams(
        group_by=["department"],
        aggregations={"salary": AggFunc.FIRST},
    )
    result_ctx = aggregate(context, params)
    result = result_ctx.df

    # Should have one value per group
    assert len(result) == 2


def test_aggregate_empty(empty_df):
    """Test aggregation with empty DataFrame."""
    context = create_context(empty_df)
    params = AggregateParams(
        group_by=["a"],
        aggregations={"b": AggFunc.SUM},
    )
    result_ctx = aggregate(context, params)
    result = result_ctx.df

    assert result.empty


def test_aggregate_with_nulls():
    """Test aggregation with null values."""
    df = pd.DataFrame(
        {
            "category": ["A", "A", "B", "B"],
            "value": [10, None, 20, 30],
        }
    )

    context = create_context(df)
    params = AggregateParams(
        group_by=["category"],
        aggregations={"value": AggFunc.SUM},
    )
    result_ctx = aggregate(context, params)
    result = result_ctx.df

    # SQL SUM should ignore nulls
    assert len(result) == 2
    assert result.loc[result["category"] == "A", "value"].iloc[0] == 10
    assert result.loc[result["category"] == "B", "value"].iloc[0] == 50

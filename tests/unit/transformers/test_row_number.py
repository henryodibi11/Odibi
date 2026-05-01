"""Tests for the row_number transformer."""

import pandas as pd
import pytest

from odibi.context import EngineContext, PandasContext
from odibi.engine.pandas_engine import PandasEngine
from odibi.enums import EngineType
from odibi.transformers.sql_core import RowNumberParams, row_number


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------


def setup_context(df: pd.DataFrame) -> EngineContext:
    """Create EngineContext with DuckDB SQL executor for Pandas tests."""
    pandas_ctx = PandasContext()
    pandas_ctx.register("df", df.copy())
    engine = PandasEngine()
    return EngineContext(
        pandas_ctx,
        df.copy(),
        EngineType.PANDAS,
        sql_executor=engine.execute_sql,
        engine=engine,
    )


@pytest.fixture
def employees_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"],
            "department": ["Eng", "Eng", "Sales", "Sales", "Eng", "Sales"],
            "hire_date": pd.to_datetime(
                ["2020-01-15", "2019-06-01", "2021-03-10", "2018-11-20", "2022-07-01", "2020-09-15"]
            ),
            "salary": [90000, 85000, 70000, 75000, 95000, 72000],
        }
    )


# -------------------------------------------------------------------------
# Tests
# -------------------------------------------------------------------------


class TestRowNumberBasic:
    """Basic row_number with no partition or order."""

    def test_basic_no_partition_no_order(self, employees_df):
        ctx = setup_context(employees_df)
        params = RowNumberParams()
        result = row_number(ctx, params)
        df = result.df
        assert "row_num" in df.columns
        assert list(df["row_num"]) == [1, 2, 3, 4, 5, 6]
        # Original columns preserved
        assert "id" in df.columns
        assert "name" in df.columns

    def test_default_output_column_name(self, employees_df):
        ctx = setup_context(employees_df)
        params = RowNumberParams()
        result = row_number(ctx, params)
        assert "row_num" in result.df.columns

    def test_custom_output_column_name(self, employees_df):
        ctx = setup_context(employees_df)
        params = RowNumberParams(output="seq_id")
        result = row_number(ctx, params)
        assert "seq_id" in result.df.columns
        assert "row_num" not in result.df.columns


class TestRowNumberPartitionBy:
    """Row number with partition_by only."""

    def test_partition_by_department(self, employees_df):
        ctx = setup_context(employees_df)
        params = RowNumberParams(partition_by=["department"])
        result = row_number(ctx, params)
        df = result.df
        assert "row_num" in df.columns
        # Each department should restart numbering at 1
        eng_rows = df[df["department"] == "Eng"]["row_num"].tolist()
        sales_rows = df[df["department"] == "Sales"]["row_num"].tolist()
        assert sorted(eng_rows) == [1, 2, 3]
        assert sorted(sales_rows) == [1, 2, 3]

    def test_partition_by_multiple_columns(self, employees_df):
        ctx = setup_context(employees_df)
        # Partition by department — all rows are unique per partition
        params = RowNumberParams(partition_by=["department"], output="rn")
        result = row_number(ctx, params)
        assert max(result.df["rn"]) <= 3  # max 3 per partition


class TestRowNumberOrderBy:
    """Row number with order_by only."""

    def test_order_by_salary(self, employees_df):
        ctx = setup_context(employees_df)
        params = RowNumberParams(order_by=["salary"])
        result = row_number(ctx, params)
        df = result.df
        # Row 1 should have lowest salary
        row_1 = df[df["row_num"] == 1].iloc[0]
        assert row_1["salary"] == 70000  # Charlie (lowest)

    def test_order_by_descending(self, employees_df):
        ctx = setup_context(employees_df)
        params = RowNumberParams(order_by=["salary DESC"])
        result = row_number(ctx, params)
        df = result.df
        row_1 = df[df["row_num"] == 1].iloc[0]
        assert row_1["salary"] == 95000  # Eve (highest)


class TestRowNumberPartitionAndOrder:
    """Row number with both partition_by and order_by."""

    def test_partition_and_order(self, employees_df):
        ctx = setup_context(employees_df)
        params = RowNumberParams(
            partition_by=["department"],
            order_by=["salary DESC"],
            output="rank_in_dept",
        )
        result = row_number(ctx, params)
        df = result.df
        assert "rank_in_dept" in df.columns

        # In Eng (Alice 90k, Bob 85k, Eve 95k) -> Eve=1, Alice=2, Bob=3
        eng = df[df["department"] == "Eng"].sort_values("rank_in_dept")
        assert eng.iloc[0]["name"] == "Eve"
        assert eng.iloc[0]["rank_in_dept"] == 1
        assert eng.iloc[1]["name"] == "Alice"
        assert eng.iloc[2]["name"] == "Bob"

        # In Sales (Charlie 70k, David 75k, Frank 72k) -> David=1, Frank=2, Charlie=3
        sales = df[df["department"] == "Sales"].sort_values("rank_in_dept")
        assert sales.iloc[0]["name"] == "David"
        assert sales.iloc[0]["rank_in_dept"] == 1


class TestRowNumberEdgeCases:
    """Edge cases: empty DataFrame, single row, all same partition."""

    def test_empty_dataframe(self):
        df = pd.DataFrame({"a": pd.Series([], dtype="int64"), "b": pd.Series([], dtype="str")})
        ctx = setup_context(df)
        params = RowNumberParams()
        result = row_number(ctx, params)
        assert len(result.df) == 0
        assert "row_num" in result.df.columns

    def test_single_row(self):
        df = pd.DataFrame({"x": [42]})
        ctx = setup_context(df)
        params = RowNumberParams(output="rn")
        result = row_number(ctx, params)
        assert list(result.df["rn"]) == [1]

    def test_all_same_partition(self, employees_df):
        ctx = setup_context(employees_df)
        # Partition by a column where all values are different -> each row is partition of 1
        params = RowNumberParams(partition_by=["id"])
        result = row_number(ctx, params)
        # Every row should be row_num=1 within its own partition
        assert all(result.df["row_num"] == 1)


class TestRowNumberParams:
    """Param validation."""

    def test_defaults(self):
        params = RowNumberParams()
        assert params.output == "row_num"
        assert params.partition_by is None
        assert params.order_by is None

    def test_all_fields(self):
        params = RowNumberParams(
            output="rn",
            partition_by=["dept"],
            order_by=["salary DESC"],
        )
        assert params.output == "rn"
        assert params.partition_by == ["dept"]
        assert params.order_by == ["salary DESC"]


class TestRowNumberRegistration:
    """Verify row_number is registered in FunctionRegistry."""

    def test_registered(self):
        from odibi.transformers import register_standard_library
        from odibi.registry import FunctionRegistry

        # Reset for clean test
        FunctionRegistry._functions.clear()
        import odibi.transformers as _t

        _t._standard_library_registered = False

        register_standard_library()
        assert "row_number" in FunctionRegistry.list_functions()

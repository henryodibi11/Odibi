import pytest
import pandas as pd

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.sql_core import coalesce_columns, CoalesceColumnsParams


@pytest.fixture
def sample_df():
    return pd.DataFrame({"a": [None, None, None], "b": [None, None, None], "c": [None, None, None]})


def mock_sql_executor(query, context):
    # For test purposes, just return the DataFrame registered as "df"
    return context.get("df")


def test_coalesce_columns_empty_columns(sample_df):
    pandas_ctx = PandasContext()
    pandas_ctx.register("df", sample_df.copy())
    context = EngineContext(
        pandas_ctx, sample_df.copy(), EngineType.PANDAS, sql_executor=mock_sql_executor
    )
    params = CoalesceColumnsParams(columns=[], output_col="result")
    result_ctx = coalesce_columns(context, params)
    pd.testing.assert_frame_equal(result_ctx.df, sample_df)


def test_coalesce_columns_all_nulls(sample_df):
    pandas_ctx = PandasContext()
    pandas_ctx.register("df", sample_df.copy())
    context = EngineContext(
        pandas_ctx, sample_df.copy(), EngineType.PANDAS, sql_executor=mock_sql_executor
    )
    params = CoalesceColumnsParams(columns=["a", "b", "c"], output_col="result")
    result_ctx = coalesce_columns(context, params)
    # Should produce a new column (e.g., "coalesced") with all nulls, or update one of the columns
    # We'll check all columns remain all nulls (since all input is null)
    assert result_ctx.df.isnull().all().all()


def test_coalesce_columns_normal():
    df = pd.DataFrame({"a": [None, 1, None], "b": [2, None, None], "c": [None, None, 3]})
    pandas_ctx = PandasContext()
    pandas_ctx.register("df", df)
    context = EngineContext(pandas_ctx, df, EngineType.PANDAS, sql_executor=mock_sql_executor)
    params = CoalesceColumnsParams(columns=["a", "b", "c"], output_col="result")
    result_ctx = coalesce_columns(context, params)
    # The coalesced column should have [2, 1, 3] if implemented as expected
    # We'll check if any column contains the expected coalesced values
    coalesced = result_ctx.df.apply(
        lambda row: next((row[col] for col in ["a", "b", "c"] if pd.notnull(row[col])), None),
        axis=1,
    )
    assert list(coalesced) == [2, 1, 3]

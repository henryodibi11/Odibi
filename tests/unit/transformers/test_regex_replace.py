"""Tests for regex_replace transformer."""

import pandas as pd
import pytest

from odibi.context import EngineContext, create_context
from odibi.enums import EngineType
from odibi.transformers.advanced import RegexReplaceParams, regex_replace


def create_test_context(df: pd.DataFrame) -> EngineContext:
    """Create a test context with DuckDB SQL executor."""
    import duckdb

    ctx = create_context(engine="pandas")
    ctx.register("df", df)

    def sql_executor(sql: str, context) -> pd.DataFrame:
        conn = duckdb.connect()
        for name in context.list_names():
            conn.register(name, context.get(name))
        return conn.execute(sql).fetchdf()

    return EngineContext(ctx, df, EngineType.PANDAS, sql_executor=sql_executor)


@pytest.fixture
def empty_df():
    return pd.DataFrame({"a": pd.Series([], dtype=str)})


@pytest.fixture
def no_match_df():
    return pd.DataFrame({"a": ["hello", "world"]})


@pytest.fixture
def all_match_df():
    return pd.DataFrame({"a": ["foo", "foo", "foo"]})


@pytest.fixture
def normal_df():
    return pd.DataFrame({"a": ["foo", "bar", "foobar", "barfoo"]})


@pytest.fixture
def multi_col_df():
    return pd.DataFrame({"a": ["foo", "bar"], "b": [1, 2], "c": ["x", "y"]})


def test_regex_replace_empty(empty_df):
    """Test regex_replace with empty dataframe."""
    context = create_test_context(empty_df)
    params = RegexReplaceParams(column="a", pattern="foo", replacement="bar")
    result_ctx = regex_replace(context, params)
    assert len(result_ctx.df) == 0


def test_regex_replace_no_match(no_match_df):
    """Test regex_replace when pattern doesn't match any values."""
    context = create_test_context(no_match_df)
    params = RegexReplaceParams(column="a", pattern="foo", replacement="bar")
    result_ctx = regex_replace(context, params)
    assert result_ctx.df["a"].tolist() == ["hello", "world"]


def test_regex_replace_all_match(all_match_df):
    """Test regex_replace when all values match pattern."""
    context = create_test_context(all_match_df)
    params = RegexReplaceParams(column="a", pattern="foo", replacement="bar")
    result_ctx = regex_replace(context, params)
    assert result_ctx.df["a"].tolist() == ["bar", "bar", "bar"]


def test_regex_replace_normal(normal_df):
    """Test regex_replace with partial matches."""
    context = create_test_context(normal_df)
    params = RegexReplaceParams(column="a", pattern="foo", replacement="bar")
    result_ctx = regex_replace(context, params)
    assert result_ctx.df["a"].tolist() == ["bar", "bar", "barbar", "barbar"]


def test_regex_replace_preserves_other_columns(multi_col_df):
    """Test that regex_replace preserves other columns."""
    context = create_test_context(multi_col_df)
    params = RegexReplaceParams(column="a", pattern="foo", replacement="XXX")
    result_ctx = regex_replace(context, params)
    # Check replaced column
    assert result_ctx.df["a"].tolist() == ["XXX", "bar"]
    # Check other columns preserved
    assert result_ctx.df["b"].tolist() == [1, 2]
    assert result_ctx.df["c"].tolist() == ["x", "y"]

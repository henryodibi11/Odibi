import pytest
import pandas as pd

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.advanced import generate_surrogate_key, SurrogateKeyParams


def mock_sql_executor(query, context):
    # Simulate surrogate key generation for test purposes
    df = context.get("df").copy()
    # Parse output_col from the query (very basic, for test only)
    output_col = "sk"
    if "ROW_NUMBER()" in query or "DENSE_RANK()" in query or "HASH" in query or "sk" in query:
        # If all columns are null, output_col is all null
        if df.isnull().all().all():
            df[output_col] = None
        elif df.empty:
            df[output_col] = []
        else:
            # Assign a hash-based surrogate key for each row
            df[output_col] = df.apply(lambda row: hash(tuple(row)), axis=1)
    return df


@pytest.fixture
def empty_df():
    return pd.DataFrame(columns=["a", "b"])


@pytest.fixture
def nulls_df():
    return pd.DataFrame({"a": [None, None, None], "b": [None, None, None]})


@pytest.fixture
def duplicates_df():
    return pd.DataFrame({"a": [1, 1, 2, 2], "b": ["x", "x", "y", "y"]})


@pytest.fixture
def normal_df():
    return pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})


def setup_context(df):
    pandas_ctx = PandasContext()
    pandas_ctx.register("df", df.copy())
    return EngineContext(pandas_ctx, df.copy(), EngineType.PANDAS, sql_executor=mock_sql_executor)


def test_generate_surrogate_key_empty(empty_df):
    context = setup_context(empty_df)
    params = SurrogateKeyParams(columns=["a", "b"], output_col="sk")
    result_ctx = generate_surrogate_key(context, params)
    assert result_ctx.df.empty


def test_generate_surrogate_key_all_nulls(nulls_df):
    context = setup_context(nulls_df)
    params = SurrogateKeyParams(columns=["a", "b"], output_col="sk")
    result_ctx = generate_surrogate_key(context, params)
    assert result_ctx.df["sk"].isnull().all()


def test_generate_surrogate_key_duplicates(duplicates_df):
    context = setup_context(duplicates_df)
    params = SurrogateKeyParams(columns=["a", "b"], output_col="sk")
    result_ctx = generate_surrogate_key(context, params)
    # Should assign the same surrogate key to duplicate rows
    sk = result_ctx.df["sk"]
    assert sk.iloc[0] == sk.iloc[1]
    assert sk.iloc[2] == sk.iloc[3]


def test_generate_surrogate_key_normal(normal_df):
    context = setup_context(normal_df)
    params = SurrogateKeyParams(columns=["a", "b"], output_col="sk")
    result_ctx = generate_surrogate_key(context, params)
    # Should assign unique surrogate keys
    assert result_ctx.df["sk"].nunique() == 3

import pytest
import pandas as pd

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.advanced import hash_columns, HashParams


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {"email": ["a@b.com", "c@d.com"], "ssn": ["123-45-6789", "987-65-4321"], "other": [1, 2]}
    )


def test_hash_columns_pandas(sample_df):
    context = EngineContext(PandasContext(), sample_df, EngineType.PANDAS)
    params = HashParams(columns=["email", "ssn"])
    result_ctx = hash_columns(context, params)
    result = result_ctx.df
    # Columns should be replaced with hashes (not equal to original)
    assert not (result["email"] == sample_df["email"]).any()
    assert not (result["ssn"] == sample_df["ssn"]).any()
    # Other columns unchanged
    assert (result["other"] == sample_df["other"]).all()


def test_hash_columns_empty_columns(sample_df):
    context = EngineContext(PandasContext(), sample_df, EngineType.PANDAS)
    params = HashParams(columns=[])
    result_ctx = hash_columns(context, params)
    result = result_ctx.df
    # No columns should be changed
    pd.testing.assert_frame_equal(result, sample_df)


@pytest.mark.skipif("pyspark" not in globals(), reason="Spark not available in CI")
def test_hash_columns_spark(sample_df):
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        sdf = spark.createDataFrame(sample_df)
        context = EngineContext.from_spark(sdf)
        params = HashParams(columns=["email", "ssn"])
        result_ctx = hash_columns(context, params)
        result = result_ctx.to_spark().toPandas()
        # Columns should be replaced with hashes (not equal to original)
        assert not (result["email"] == sample_df["email"]).any()
        assert not (result["ssn"] == sample_df["ssn"]).any()
        # Other columns unchanged
        assert (result["other"] == sample_df["other"]).all()
    except ImportError:
        pytest.skip("pyspark not installed")

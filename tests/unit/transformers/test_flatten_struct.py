"""Tests for the flatten_struct transformer."""

import pandas as pd
import pytest

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkSession = None  # type: ignore[assignment]
    IntegerType = StringType = StructField = StructType = None  # type: ignore[assignment]

from odibi.context import EngineContext, PandasContext, SparkContext
from odibi.engine.pandas_engine import PandasEngine
from odibi.engine.spark_engine import SparkEngine
from odibi.enums import EngineType
from odibi.transformers.advanced import FlattenStructParams, flatten_struct


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


# -------------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------------


@pytest.fixture
def simple_struct_df() -> pd.DataFrame:
    """DataFrame with a single-level struct (dict) column."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "metadata": [
                {"owner": "Alice", "version": 1},
                {"owner": "Bob", "version": 2},
                {"owner": "Charlie", "version": 3},
            ],
        }
    )


@pytest.fixture
def nested_struct_df() -> pd.DataFrame:
    """DataFrame with a nested struct column (depth 2)."""
    return pd.DataFrame(
        {
            "id": [1, 2],
            "metadata": [
                {"owner": {"name": "Alice", "dept": "Eng"}, "version": 2},
                {"owner": {"name": "Bob", "dept": "Sales"}, "version": 5},
            ],
        }
    )


@pytest.fixture
def deeply_nested_df() -> pd.DataFrame:
    """DataFrame with 3 levels of nesting."""
    return pd.DataFrame(
        {
            "id": [1],
            "data": [
                {
                    "level1": {
                        "level2": {"level3_val": 42},
                        "flat_val": "hello",
                    },
                    "top_val": 99,
                }
            ],
        }
    )


# -------------------------------------------------------------------------
# Tests: Single-Level Struct
# -------------------------------------------------------------------------


class TestFlattenSingleLevel:
    def test_basic_flatten(self, simple_struct_df):
        ctx = setup_context(simple_struct_df)
        params = FlattenStructParams(column="metadata")
        result = flatten_struct(ctx, params)
        df = result.df
        assert "metadata_owner" in df.columns
        assert "metadata_version" in df.columns
        assert "metadata" not in df.columns  # drop_source=True by default
        assert list(df["metadata_owner"]) == ["Alice", "Bob", "Charlie"]
        assert list(df["metadata_version"]) == [1, 2, 3]

    def test_keep_source(self, simple_struct_df):
        ctx = setup_context(simple_struct_df)
        params = FlattenStructParams(column="metadata", drop_source=False)
        result = flatten_struct(ctx, params)
        assert "metadata" in result.df.columns
        assert "metadata_owner" in result.df.columns

    def test_custom_prefix(self, simple_struct_df):
        ctx = setup_context(simple_struct_df)
        params = FlattenStructParams(column="metadata", prefix="meta_")
        result = flatten_struct(ctx, params)
        assert "meta_owner" in result.df.columns
        assert "meta_version" in result.df.columns
        assert "metadata_owner" not in result.df.columns

    def test_custom_separator(self, simple_struct_df):
        ctx = setup_context(simple_struct_df)
        params = FlattenStructParams(column="metadata", separator="__")
        result = flatten_struct(ctx, params)
        assert "metadata__owner" in result.df.columns
        assert "metadata__version" in result.df.columns


# -------------------------------------------------------------------------
# Tests: Nested Struct (depth > 1)
# -------------------------------------------------------------------------


class TestFlattenNested:
    def test_depth_1_leaves_nested_as_dict(self, nested_struct_df):
        ctx = setup_context(nested_struct_df)
        params = FlattenStructParams(column="metadata", depth=1)
        result = flatten_struct(ctx, params)
        df = result.df
        # depth=1: owner stays as dict, version is extracted
        assert "metadata_version" in df.columns
        assert "metadata_owner" in df.columns
        assert list(df["metadata_version"]) == [2, 5]
        # owner should still be a dict at depth=1
        assert isinstance(df["metadata_owner"].iloc[0], dict)

    def test_depth_2_flattens_nested(self, nested_struct_df):
        ctx = setup_context(nested_struct_df)
        params = FlattenStructParams(column="metadata", depth=2)
        result = flatten_struct(ctx, params)
        df = result.df
        assert "metadata_owner_name" in df.columns
        assert "metadata_owner_dept" in df.columns
        assert "metadata_version" in df.columns
        assert list(df["metadata_owner_name"]) == ["Alice", "Bob"]
        assert list(df["metadata_owner_dept"]) == ["Eng", "Sales"]

    def test_depth_2_with_custom_prefix(self, nested_struct_df):
        ctx = setup_context(nested_struct_df)
        params = FlattenStructParams(column="metadata", prefix="m_", depth=2)
        result = flatten_struct(ctx, params)
        df = result.df
        assert "m_owner_name" in df.columns
        assert "m_owner_dept" in df.columns
        assert "m_version" in df.columns

    def test_depth_3_on_deeply_nested(self, deeply_nested_df):
        ctx = setup_context(deeply_nested_df)
        params = FlattenStructParams(column="data", depth=3)
        result = flatten_struct(ctx, params)
        df = result.df
        assert "data_level1_level2_level3_val" in df.columns
        assert "data_level1_flat_val" in df.columns
        assert "data_top_val" in df.columns
        assert df["data_level1_level2_level3_val"].iloc[0] == 42
        assert df["data_level1_flat_val"].iloc[0] == "hello"
        assert df["data_top_val"].iloc[0] == 99


# -------------------------------------------------------------------------
# Tests: Null and Edge Cases
# -------------------------------------------------------------------------


class TestFlattenEdgeCases:
    def test_null_struct_values(self):
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "info": [{"a": 1, "b": 2}, None, {"a": 3, "b": 4}],
            }
        )
        ctx = setup_context(df)
        params = FlattenStructParams(column="info")
        result = flatten_struct(ctx, params)
        rdf = result.df
        assert "info_a" in rdf.columns
        assert "info_b" in rdf.columns
        assert rdf["info_a"].iloc[0] == 1
        assert rdf["info_a"].iloc[2] == 3
        # Null row should produce NaN/None
        assert pd.isna(rdf["info_a"].iloc[1])

    def test_empty_dataframe(self):
        df = pd.DataFrame(
            {"id": pd.Series([], dtype="int64"), "data": pd.Series([], dtype="object")}
        )
        ctx = setup_context(df)
        params = FlattenStructParams(column="data")
        result = flatten_struct(ctx, params)
        assert len(result.df) == 0

    def test_single_row(self):
        df = pd.DataFrame({"id": [1], "info": [{"x": 42, "y": "hello"}]})
        ctx = setup_context(df)
        params = FlattenStructParams(column="info")
        result = flatten_struct(ctx, params)
        assert result.df["info_x"].iloc[0] == 42
        assert result.df["info_y"].iloc[0] == "hello"

    def test_all_null_structs(self):
        df = pd.DataFrame({"id": [1, 2], "info": [None, None]})
        ctx = setup_context(df)
        params = FlattenStructParams(column="info")
        result = flatten_struct(ctx, params)
        # Should not crash — produces empty expansion
        assert len(result.df) == 2


# -------------------------------------------------------------------------
# Tests: Params Validation
# -------------------------------------------------------------------------


class TestFlattenStructParams:
    def test_defaults(self):
        params = FlattenStructParams(column="metadata")
        assert params.column == "metadata"
        assert params.prefix is None
        assert params.depth == 1
        assert params.separator == "_"
        assert params.drop_source is True

    def test_all_fields(self):
        params = FlattenStructParams(
            column="data", prefix="d_", depth=3, separator="__", drop_source=False
        )
        assert params.prefix == "d_"
        assert params.depth == 3
        assert params.separator == "__"
        assert params.drop_source is False

    def test_missing_column_raises(self):
        with pytest.raises(Exception):
            FlattenStructParams()

    def test_depth_must_be_positive(self):
        with pytest.raises(Exception):
            FlattenStructParams(column="x", depth=0)


class TestFlattenStructRegistration:
    def test_registered(self):
        from odibi.transformers import register_standard_library
        from odibi.registry import FunctionRegistry

        FunctionRegistry._functions.clear()
        import odibi.transformers as _t

        _t._standard_library_registered = False

        register_standard_library()
        assert "flatten_struct" in FunctionRegistry.list_functions()


# =========================================================================
# Spark Tests — verify _flatten_struct_spark + _collect_struct_fields
# =========================================================================


@pytest.fixture(scope="module")
def spark_fixture():
    """Module-scoped SparkSession for Spark tests.

    On Databricks shared clusters (Spark Connect), getActiveSession() works
    but builder.getOrCreate() may fail. Falls back gracefully.
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        try:
            spark = SparkSession.builder.getOrCreate()
        except Exception:
            pytest.skip("SparkSession not available (CI or subprocess)")
    yield spark


def setup_spark_context(spark, sdf):
    """Create EngineContext wrapping a Spark DataFrame."""
    spark_ctx = SparkContext(spark)
    spark_ctx.register("df", sdf)
    engine = SparkEngine(spark_session=spark)
    return EngineContext(
        spark_ctx,
        sdf,
        EngineType.SPARK,
        sql_executor=engine.execute_sql,
        engine=engine,
    )


# ── Schema helpers ──────────────────────────────────────────────────────

if PYSPARK_AVAILABLE:
    SIMPLE_SCHEMA = StructType(
        [
            StructField("id", IntegerType()),
            StructField(
                "metadata",
                StructType(
                    [
                        StructField("owner", StringType()),
                        StructField("version", IntegerType()),
                    ]
                ),
            ),
        ]
    )

    NESTED_SCHEMA = StructType(
        [
            StructField("id", IntegerType()),
            StructField(
                "metadata",
                StructType(
                    [
                        StructField(
                            "owner",
                            StructType(
                                [
                                    StructField("name", StringType()),
                                    StructField("dept", StringType()),
                                ]
                            ),
                        ),
                        StructField("version", IntegerType()),
                    ]
                ),
            ),
        ]
    )
else:
    SIMPLE_SCHEMA = None  # type: ignore[assignment]
    NESTED_SCHEMA = None  # type: ignore[assignment]


pytestmark_spark = pytest.mark.skipif(
    not PYSPARK_AVAILABLE, reason="pyspark types not importable in this environment"
)


@pytestmark_spark
class TestFlattenStructSparkBasic:
    """Spark path: single-level struct flatten."""

    def test_basic_flatten(self, spark_fixture):
        spark = spark_fixture
        data = [(1, ("Alice", 1)), (2, ("Bob", 2)), (3, ("Charlie", 3))]
        sdf = spark.createDataFrame(data, SIMPLE_SCHEMA)
        ctx = setup_spark_context(spark, sdf)

        params = FlattenStructParams(column="metadata")
        result = flatten_struct(ctx, params)
        pdf = result.df.orderBy("id").toPandas()

        assert "metadata_owner" in pdf.columns
        assert "metadata_version" in pdf.columns
        assert "metadata" not in pdf.columns  # drop_source=True
        assert list(pdf["metadata_owner"]) == ["Alice", "Bob", "Charlie"]
        assert list(pdf["metadata_version"]) == [1, 2, 3]

    def test_keep_source(self, spark_fixture):
        spark = spark_fixture
        data = [(1, ("Alice", 1))]
        sdf = spark.createDataFrame(data, SIMPLE_SCHEMA)
        ctx = setup_spark_context(spark, sdf)

        params = FlattenStructParams(column="metadata", drop_source=False)
        result = flatten_struct(ctx, params)
        pdf = result.df.toPandas()

        assert "metadata" in pdf.columns
        assert "metadata_owner" in pdf.columns
        assert "metadata_version" in pdf.columns


@pytestmark_spark
class TestFlattenStructSparkNested:
    """Spark path: nested struct with depth control."""

    def test_depth_1_keeps_nested(self, spark_fixture):
        spark = spark_fixture
        data = [(1, (("Alice", "Eng"), 2))]
        sdf = spark.createDataFrame(data, NESTED_SCHEMA)
        ctx = setup_spark_context(spark, sdf)

        params = FlattenStructParams(column="metadata", depth=1)
        result = flatten_struct(ctx, params)
        pdf = result.df.toPandas()

        assert "metadata_owner" in pdf.columns
        assert "metadata_version" in pdf.columns
        assert pdf["metadata_version"].iloc[0] == 2
        # depth=1: owner stays as struct (Row object in Pandas)
        owner = pdf["metadata_owner"].iloc[0]
        assert hasattr(owner, "name") or isinstance(owner, dict)

    def test_depth_2_flattens_nested(self, spark_fixture):
        spark = spark_fixture
        data = [
            (1, (("Alice", "Eng"), 2)),
            (2, (("Bob", "Sales"), 5)),
        ]
        sdf = spark.createDataFrame(data, NESTED_SCHEMA)
        ctx = setup_spark_context(spark, sdf)

        params = FlattenStructParams(column="metadata", depth=2)
        result = flatten_struct(ctx, params)
        pdf = result.df.orderBy("id").toPandas()

        assert "metadata_owner_name" in pdf.columns
        assert "metadata_owner_dept" in pdf.columns
        assert "metadata_version" in pdf.columns
        assert list(pdf["metadata_owner_name"]) == ["Alice", "Bob"]
        assert list(pdf["metadata_owner_dept"]) == ["Eng", "Sales"]
        assert list(pdf["metadata_version"]) == [2, 5]


@pytestmark_spark
class TestFlattenStructSparkCustom:
    """Spark path: custom prefix, separator."""

    def test_custom_prefix(self, spark_fixture):
        spark = spark_fixture
        data = [(1, (("Alice", "Eng"), 2))]
        sdf = spark.createDataFrame(data, NESTED_SCHEMA)
        ctx = setup_spark_context(spark, sdf)

        params = FlattenStructParams(column="metadata", prefix="m_", depth=2)
        result = flatten_struct(ctx, params)
        pdf = result.df.toPandas()

        assert "m_owner_name" in pdf.columns
        assert "m_owner_dept" in pdf.columns
        assert "m_version" in pdf.columns

    def test_custom_separator(self, spark_fixture):
        spark = spark_fixture
        data = [(1, ("Alice", 1))]
        sdf = spark.createDataFrame(data, SIMPLE_SCHEMA)
        ctx = setup_spark_context(spark, sdf)

        params = FlattenStructParams(column="metadata", separator="__")
        result = flatten_struct(ctx, params)
        pdf = result.df.toPandas()

        assert "metadata__owner" in pdf.columns
        assert "metadata__version" in pdf.columns


@pytestmark_spark
class TestFlattenStructSparkSpecialChars:
    """Spark path: field names with special characters (backtick escaping)."""

    def test_special_char_field_names(self, spark_fixture):
        spark = spark_fixture
        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField(
                    "metadata",
                    StructType(
                        [
                            StructField("my field", StringType()),
                            StructField("cost-usd", IntegerType()),
                        ]
                    ),
                ),
            ]
        )
        data = [(1, ("Alice", 100)), (2, ("Bob", 200))]
        sdf = spark.createDataFrame(data, schema)
        ctx = setup_spark_context(spark, sdf)

        params = FlattenStructParams(column="metadata")
        result = flatten_struct(ctx, params)
        pdf = result.df.orderBy("id").toPandas()

        assert "metadata_my field" in pdf.columns
        assert "metadata_cost-usd" in pdf.columns
        assert list(pdf["metadata_my field"]) == ["Alice", "Bob"]
        assert list(pdf["metadata_cost-usd"]) == [100, 200]

    def test_nested_special_chars(self, spark_fixture):
        spark = spark_fixture
        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField(
                    "info",
                    StructType(
                        [
                            StructField(
                                "sub-data",
                                StructType(
                                    [
                                        StructField("val 1", IntegerType()),
                                    ]
                                ),
                            ),
                        ]
                    ),
                ),
            ]
        )
        data = [(1, (((42,),),))]
        sdf = spark.createDataFrame(data, schema)
        ctx = setup_spark_context(spark, sdf)

        params = FlattenStructParams(column="info", depth=2)
        result = flatten_struct(ctx, params)
        pdf = result.df.toPandas()

        assert "info_sub-data_val 1" in pdf.columns
        assert pdf["info_sub-data_val 1"].iloc[0] == 42

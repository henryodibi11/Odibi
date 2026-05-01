"""Tests for the apply_mapping transformer."""

import pandas as pd
import pytest

try:
    from pyspark.sql import SparkSession

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkSession = None  # type: ignore[assignment]

from odibi.context import EngineContext, PandasContext, SparkContext
from odibi.engine.pandas_engine import PandasEngine
from odibi.engine.spark_engine import SparkEngine
from odibi.enums import EngineType
from odibi.transformers.advanced import ApplyMappingParams, apply_mapping

pytestmark_spark = pytest.mark.skipif(
    not PYSPARK_AVAILABLE, reason="pyspark not importable in this environment"
)


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------


def setup_context(df: pd.DataFrame, extra_datasets: dict | None = None) -> EngineContext:
    """Create EngineContext with DuckDB SQL executor and optional extra datasets."""
    pandas_ctx = PandasContext()
    pandas_ctx.register("df", df.copy())

    if extra_datasets:
        for name, dataset in extra_datasets.items():
            pandas_ctx.register(name, dataset.copy())

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
def main_df():
    """Main DataFrame with status codes to map."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "status_code": ["A", "B", "C", "A", "D"],
            "value": [100, 200, 300, 400, 500],
        }
    )


@pytest.fixture
def mapping_df():
    """Mapping table: code -> description."""
    return pd.DataFrame(
        {
            "code": ["A", "B", "C"],
            "description": ["Active", "Blocked", "Closed"],
        }
    )


# -------------------------------------------------------------------------
# TestApplyMappingBasic
# -------------------------------------------------------------------------


class TestApplyMappingBasic:
    """Core mapping scenarios."""

    def test_all_values_matched(self, main_df, mapping_df):
        """All source values exist in mapping → no NULLs."""
        df = main_df[main_df["status_code"] != "D"].copy().reset_index(drop=True)
        ctx = setup_context(df, {"ref_codes": mapping_df})

        params = ApplyMappingParams(
            column="status_code",
            mapping_source="ref_codes",
            source_key="code",
            source_value="description",
            output="status_desc",
        )
        result = apply_mapping(ctx, params)
        pdf = result.df.sort_values("id").reset_index(drop=True)

        assert list(pdf["status_desc"]) == ["Active", "Blocked", "Closed", "Active"]
        assert "status_code" in pdf.columns  # original preserved
        assert list(pdf["value"]) == [100, 200, 300, 400]  # other cols untouched

    def test_unmatched_with_default(self, main_df, mapping_df):
        """Unmatched values get the default string."""
        ctx = setup_context(main_df, {"ref_codes": mapping_df})

        params = ApplyMappingParams(
            column="status_code",
            mapping_source="ref_codes",
            source_key="code",
            source_value="description",
            output="status_desc",
            default="Unknown",
        )
        result = apply_mapping(ctx, params)
        pdf = result.df.sort_values("id").reset_index(drop=True)

        assert list(pdf["status_desc"]) == [
            "Active",
            "Blocked",
            "Closed",
            "Active",
            "Unknown",
        ]

    def test_unmatched_without_default(self, main_df, mapping_df):
        """Unmatched values produce NULL when no default is set."""
        ctx = setup_context(main_df, {"ref_codes": mapping_df})

        params = ApplyMappingParams(
            column="status_code",
            mapping_source="ref_codes",
            source_key="code",
            source_value="description",
            output="status_desc",
        )
        result = apply_mapping(ctx, params)
        pdf = result.df.sort_values("id").reset_index(drop=True)

        assert pdf["status_desc"].iloc[0] == "Active"
        assert pd.isna(pdf["status_desc"].iloc[4])  # "D" not in mapping → NULL


# -------------------------------------------------------------------------
# TestApplyMappingOutput
# -------------------------------------------------------------------------


class TestApplyMappingOutput:
    """Output column behavior."""

    def test_custom_output_column(self, main_df, mapping_df):
        """Output column is added alongside the original."""
        ctx = setup_context(main_df, {"ref_codes": mapping_df})

        params = ApplyMappingParams(
            column="status_code",
            mapping_source="ref_codes",
            source_key="code",
            source_value="description",
            output="mapped_status",
        )
        result = apply_mapping(ctx, params)
        pdf = result.df

        assert "mapped_status" in pdf.columns
        assert "status_code" in pdf.columns  # original preserved
        assert "value" in pdf.columns

    def test_overwrite_column(self, main_df, mapping_df):
        """When output == column, the original column is replaced."""
        ctx = setup_context(main_df, {"ref_codes": mapping_df})

        params = ApplyMappingParams(
            column="status_code",
            mapping_source="ref_codes",
            source_key="code",
            source_value="description",
            output="status_code",  # overwrite
            default="Unknown",
        )
        result = apply_mapping(ctx, params)
        pdf = result.df.sort_values("id").reset_index(drop=True)

        assert list(pdf["status_code"]) == [
            "Active",
            "Blocked",
            "Closed",
            "Active",
            "Unknown",
        ]
        # Other columns untouched
        assert list(pdf["value"]) == [100, 200, 300, 400, 500]

    def test_default_output_overwrites(self, main_df, mapping_df):
        """When output is omitted, default behavior is overwrite."""
        ctx = setup_context(main_df, {"ref_codes": mapping_df})

        params = ApplyMappingParams(
            column="status_code",
            mapping_source="ref_codes",
            source_key="code",
            source_value="description",
            default="Unknown",
        )
        result = apply_mapping(ctx, params)
        pdf = result.df.sort_values("id").reset_index(drop=True)

        # output defaults to column name → overwrites
        assert list(pdf["status_code"]) == [
            "Active",
            "Blocked",
            "Closed",
            "Active",
            "Unknown",
        ]


# -------------------------------------------------------------------------
# TestApplyMappingEdgeCases
# -------------------------------------------------------------------------


class TestApplyMappingEdgeCases:
    """Edge cases and error handling."""

    def test_duplicate_mapping_keys(self):
        """Duplicate keys in mapping → first match (alphabetical) used, no row multiplication."""
        df = pd.DataFrame({"id": [1, 2], "code": ["X", "Y"]})
        mapping = pd.DataFrame(
            {
                "code": ["X", "X", "Y"],
                "label": ["First", "Second", "Yankee"],
            }
        )
        ctx = setup_context(df, {"ref_map": mapping})

        params = ApplyMappingParams(
            column="code",
            mapping_source="ref_map",
            source_key="code",
            source_value="label",
            output="label",
        )
        result = apply_mapping(ctx, params)
        pdf = result.df.sort_values("id").reset_index(drop=True)

        assert len(pdf) == 2, f"Row multiplication! Got {len(pdf)} rows instead of 2"
        assert pdf["label"].iloc[0] == "First"  # ROW_NUMBER picks first alphabetically
        assert pdf["label"].iloc[1] == "Yankee"

    def test_empty_main_dataframe(self, mapping_df):
        """Empty main DataFrame returns empty result."""
        df = pd.DataFrame(
            {"id": pd.Series([], dtype="int64"), "status_code": pd.Series([], dtype="str")}
        )
        ctx = setup_context(df, {"ref_codes": mapping_df})

        params = ApplyMappingParams(
            column="status_code",
            mapping_source="ref_codes",
            source_key="code",
            source_value="description",
            output="status_desc",
        )
        result = apply_mapping(ctx, params)
        assert len(result.df) == 0

    def test_missing_mapping_source(self, main_df):
        """Referencing a non-existent mapping source raises ValueError."""
        ctx = setup_context(main_df)

        params = ApplyMappingParams(
            column="status_code",
            mapping_source="nonexistent_table",
            source_key="code",
            source_value="description",
        )
        with pytest.raises(ValueError, match="not found in context"):
            apply_mapping(ctx, params)

    def test_single_row(self, mapping_df):
        """Single-row DataFrame maps correctly."""
        df = pd.DataFrame({"id": [1], "code": ["B"]})
        ctx = setup_context(df, {"ref": mapping_df})

        params = ApplyMappingParams(
            column="code",
            mapping_source="ref",
            source_key="code",
            source_value="description",
            output="desc",
        )
        result = apply_mapping(ctx, params)
        assert result.df["desc"].iloc[0] == "Blocked"


# -------------------------------------------------------------------------
# TestApplyMappingParams
# -------------------------------------------------------------------------


class TestApplyMappingParams:
    """Param model validation."""

    def test_defaults(self):
        p = ApplyMappingParams(column="c", mapping_source="m", source_key="k", source_value="v")
        assert p.output is None
        assert p.default is None

    def test_all_fields(self):
        p = ApplyMappingParams(
            column="c",
            mapping_source="m",
            source_key="k",
            source_value="v",
            output="out",
            default="N/A",
        )
        assert p.output == "out"
        assert p.default == "N/A"

    def test_missing_required_raises(self):
        with pytest.raises(Exception):
            ApplyMappingParams(column="c")  # missing required fields


# -------------------------------------------------------------------------
# TestApplyMappingRegistration
# -------------------------------------------------------------------------


class TestApplyMappingRegistration:
    """Verify transformer is registered in the function registry."""

    def test_registered(self):
        from odibi.transformers import register_standard_library
        from odibi.registry import FunctionRegistry

        register_standard_library()
        names = FunctionRegistry.list_functions()
        assert "apply_mapping" in names


# =========================================================================
# Spark Tests — verify apply_mapping SQL via Spark SQL engine
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


def setup_spark_context(spark, sdf, extra_datasets=None):
    """Create EngineContext wrapping a Spark DataFrame with optional extras."""
    spark_ctx = SparkContext(spark)
    spark_ctx.register("df", sdf)

    if extra_datasets:
        for name, dataset in extra_datasets.items():
            spark_ctx.register(name, dataset)

    engine = SparkEngine(spark_session=spark)
    return EngineContext(
        spark_ctx,
        sdf,
        EngineType.SPARK,
        sql_executor=engine.execute_sql,
        engine=engine,
    )


@pytestmark_spark
class TestApplyMappingSparkBasic:
    """Spark path: core mapping scenarios."""

    def test_all_values_matched(self, spark_fixture):
        spark = spark_fixture
        main_sdf = spark.createDataFrame(
            [(1, "A", 100), (2, "B", 200), (3, "C", 300)],
            ["id", "status_code", "value"],
        )
        mapping_sdf = spark.createDataFrame(
            [("A", "Active"), ("B", "Blocked"), ("C", "Closed")],
            ["code", "description"],
        )
        ctx = setup_spark_context(spark, main_sdf, {"ref_codes": mapping_sdf})

        params = ApplyMappingParams(
            column="status_code",
            mapping_source="ref_codes",
            source_key="code",
            source_value="description",
            output="status_desc",
        )
        result = apply_mapping(ctx, params)
        pdf = result.df.orderBy("id").toPandas()

        assert list(pdf["status_desc"]) == ["Active", "Blocked", "Closed"]
        assert "status_code" in pdf.columns
        assert list(pdf["value"]) == [100, 200, 300]

    def test_unmatched_with_default(self, spark_fixture):
        spark = spark_fixture
        main_sdf = spark.createDataFrame(
            [(1, "A"), (2, "B"), (3, "Z")],
            ["id", "code"],
        )
        mapping_sdf = spark.createDataFrame(
            [("A", "Alpha"), ("B", "Bravo")],
            ["code", "label"],
        )
        ctx = setup_spark_context(spark, main_sdf, {"ref": mapping_sdf})

        params = ApplyMappingParams(
            column="code",
            mapping_source="ref",
            source_key="code",
            source_value="label",
            output="label",
            default="Unknown",
        )
        result = apply_mapping(ctx, params)
        pdf = result.df.orderBy("id").toPandas()

        assert list(pdf["label"]) == ["Alpha", "Bravo", "Unknown"]

    def test_unmatched_without_default_is_null(self, spark_fixture):
        spark = spark_fixture
        main_sdf = spark.createDataFrame(
            [(1, "A"), (2, "Z")],
            ["id", "code"],
        )
        mapping_sdf = spark.createDataFrame(
            [("A", "Alpha")],
            ["code", "label"],
        )
        ctx = setup_spark_context(spark, main_sdf, {"ref": mapping_sdf})

        params = ApplyMappingParams(
            column="code",
            mapping_source="ref",
            source_key="code",
            source_value="label",
            output="label",
        )
        result = apply_mapping(ctx, params)
        pdf = result.df.orderBy("id").toPandas()

        assert pdf["label"].iloc[0] == "Alpha"
        assert pdf["label"].iloc[1] is None or pd.isna(pdf["label"].iloc[1])


@pytestmark_spark
class TestApplyMappingSparkOutput:
    """Spark path: output column behavior."""

    def test_overwrite_column(self, spark_fixture):
        spark = spark_fixture
        main_sdf = spark.createDataFrame(
            [(1, "A", 10), (2, "B", 20)],
            ["id", "code", "value"],
        )
        mapping_sdf = spark.createDataFrame(
            [("A", "Alpha"), ("B", "Bravo")],
            ["code", "label"],
        )
        ctx = setup_spark_context(spark, main_sdf, {"ref": mapping_sdf})

        params = ApplyMappingParams(
            column="code",
            mapping_source="ref",
            source_key="code",
            source_value="label",
            output="code",  # overwrite
        )
        result = apply_mapping(ctx, params)
        pdf = result.df.orderBy("id").toPandas()

        assert list(pdf["code"]) == ["Alpha", "Bravo"]
        assert list(pdf["value"]) == [10, 20]  # other cols untouched

    def test_custom_output_preserves_original(self, spark_fixture):
        spark = spark_fixture
        main_sdf = spark.createDataFrame(
            [(1, "A")],
            ["id", "code"],
        )
        mapping_sdf = spark.createDataFrame(
            [("A", "Alpha")],
            ["code", "label"],
        )
        ctx = setup_spark_context(spark, main_sdf, {"ref": mapping_sdf})

        params = ApplyMappingParams(
            column="code",
            mapping_source="ref",
            source_key="code",
            source_value="label",
            output="mapped",
        )
        result = apply_mapping(ctx, params)
        pdf = result.df.toPandas()

        assert "code" in pdf.columns  # original preserved
        assert "mapped" in pdf.columns
        assert pdf["mapped"].iloc[0] == "Alpha"


@pytestmark_spark
class TestApplyMappingSparkEdgeCases:
    """Spark path: duplicate keys and edge cases."""

    def test_duplicate_keys_no_row_multiplication(self, spark_fixture):
        spark = spark_fixture
        main_sdf = spark.createDataFrame(
            [(1, "X"), (2, "Y")],
            ["id", "code"],
        )
        mapping_sdf = spark.createDataFrame(
            [("X", "First"), ("X", "Second"), ("Y", "Yankee")],
            ["code", "label"],
        )
        ctx = setup_spark_context(spark, main_sdf, {"ref": mapping_sdf})

        params = ApplyMappingParams(
            column="code",
            mapping_source="ref",
            source_key="code",
            source_value="label",
            output="label",
        )
        result = apply_mapping(ctx, params)
        pdf = result.df.orderBy("id").toPandas()

        assert len(pdf) == 2, f"Row multiplication! Got {len(pdf)} rows"
        assert pdf["label"].iloc[0] == "First"  # alphabetical first
        assert pdf["label"].iloc[1] == "Yankee"

    def test_single_row(self, spark_fixture):
        spark = spark_fixture
        main_sdf = spark.createDataFrame([(1, "A")], ["id", "code"])
        mapping_sdf = spark.createDataFrame([("A", "Alpha")], ["code", "label"])
        ctx = setup_spark_context(spark, main_sdf, {"ref": mapping_sdf})

        params = ApplyMappingParams(
            column="code",
            mapping_source="ref",
            source_key="code",
            source_value="label",
            output="label",
        )
        result = apply_mapping(ctx, params)
        pdf = result.df.toPandas()

        assert pdf["label"].iloc[0] == "Alpha"

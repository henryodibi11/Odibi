"""Tests for the flatten_struct transformer."""

import pandas as pd
import pytest

from odibi.context import EngineContext, PandasContext
from odibi.engine.pandas_engine import PandasEngine
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

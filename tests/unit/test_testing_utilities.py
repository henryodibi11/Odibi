import os
from datetime import datetime

import pandas as pd
import pytest

from odibi.testing.assertions import (
    _get_columns,
    _to_pandas,
    assert_frame_equal,
    assert_schema_equal,
)
from odibi.testing.fixtures import generate_sample_data, temp_directory


class TestAssertFrameEqual:
    def test_identical_dataframes(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        assert_frame_equal(df, df.copy())

    def test_reordered_rows(self):
        df1 = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        df2 = pd.DataFrame({"a": [3, 1, 2], "b": [6.0, 4.0, 5.0]})
        assert_frame_equal(df1, df2)

    def test_different_values_raises(self):
        df1 = pd.DataFrame({"a": [1, 2, 3]})
        df2 = pd.DataFrame({"a": [1, 2, 99]})
        with pytest.raises(AssertionError):
            assert_frame_equal(df1, df2)

    def test_different_columns_raises(self):
        df1 = pd.DataFrame({"a": [1]})
        df2 = pd.DataFrame({"b": [1]})
        with pytest.raises((AssertionError, KeyError)):
            assert_frame_equal(df1, df2)

    def test_empty_dataframes(self):
        df1 = pd.DataFrame()
        df2 = pd.DataFrame()
        assert_frame_equal(df1, df2)

    def test_check_dtype_false(self):
        df1 = pd.DataFrame({"a": [1, 2, 3]})
        df2 = pd.DataFrame({"a": [1.0, 2.0, 3.0]})
        assert_frame_equal(df1, df2, check_dtype=False)

    def test_check_dtype_true_raises(self):
        df1 = pd.DataFrame({"a": pd.array([1, 2, 3], dtype="int64")})
        df2 = pd.DataFrame({"a": pd.array([1.0, 2.0, 3.0], dtype="float64")})
        with pytest.raises(AssertionError):
            assert_frame_equal(df1, df2, check_dtype=True)


class TestAssertSchemaEqual:
    def test_matching_schemas(self):
        df1 = pd.DataFrame({"x": [1], "y": ["a"]})
        df2 = pd.DataFrame({"y": ["b"], "x": [2]})
        assert_schema_equal(df1, df2)

    def test_mismatching_schemas_raises(self):
        df1 = pd.DataFrame({"x": [1], "y": [2]})
        df2 = pd.DataFrame({"x": [1], "z": [2]})
        with pytest.raises(AssertionError, match="Schema mismatch"):
            assert_schema_equal(df1, df2)

    def test_extra_column_raises(self):
        df1 = pd.DataFrame({"a": [1], "b": [2]})
        df2 = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        with pytest.raises(AssertionError, match="Schema mismatch"):
            assert_schema_equal(df1, df2)


class TestToPandas:
    def test_passthrough_pandas(self):
        df = pd.DataFrame({"col": [1, 2]})
        result = _to_pandas(df)
        assert result is df

    def test_non_dataframe_raises_typeerror(self):
        with pytest.raises(TypeError, match="Expected DataFrame"):
            _to_pandas("not a dataframe")

    def test_dict_raises_typeerror(self):
        with pytest.raises(TypeError, match="Expected DataFrame"):
            _to_pandas({"a": 1})


class TestGetColumns:
    def test_returns_column_list(self):
        df = pd.DataFrame({"alpha": [1], "beta": [2], "gamma": [3]})
        result = _get_columns(df)
        assert result == ["alpha", "beta", "gamma"]

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = _get_columns(df)
        assert result == []

    def test_single_column(self):
        df = pd.DataFrame({"only": [1]})
        result = _get_columns(df)
        assert result == ["only"]


class TestTempDirectory:
    def test_creates_directory(self):
        with temp_directory() as tmp:
            assert os.path.isdir(tmp)

    def test_cleans_up_on_exit(self):
        with temp_directory() as tmp:
            path = tmp
        assert not os.path.exists(path)

    def test_cleans_up_with_files(self):
        with temp_directory() as tmp:
            filepath = os.path.join(tmp, "test.txt")
            with open(filepath, "w") as f:
                f.write("hello")
            assert os.path.isfile(filepath)
            path = tmp
        assert not os.path.exists(path)

    def test_cleans_up_on_exception(self):
        path = None
        try:
            with temp_directory() as tmp:
                path = tmp
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        assert path is not None
        assert not os.path.exists(path)


class TestGenerateSampleData:
    def test_default_schema(self):
        df = generate_sample_data()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10
        assert set(df.columns) == {"id", "value", "category", "timestamp"}

    def test_default_dtypes(self):
        df = generate_sample_data(rows=5)
        assert pd.api.types.is_integer_dtype(df["id"])
        assert pd.api.types.is_float_dtype(df["value"])
        assert df["category"].dtype == object
        assert all(isinstance(v, datetime) for v in df["timestamp"])

    def test_custom_schema(self):
        schema = {"name": "str", "score": "float"}
        df = generate_sample_data(rows=3, schema=schema)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert set(df.columns) == {"name", "score"}

    def test_custom_row_count(self):
        df = generate_sample_data(rows=50)
        assert len(df) == 50

    def test_single_row(self):
        df = generate_sample_data(rows=1)
        assert len(df) == 1

    def test_int_column_values(self):
        df = generate_sample_data(rows=100, schema={"x": "int"})
        assert (df["x"] >= 0).all()
        assert (df["x"] < 1000).all()

    def test_float_column_values(self):
        df = generate_sample_data(rows=100, schema={"x": "float"})
        assert (df["x"] >= 0).all()
        assert (df["x"] < 100).all()

    def test_str_column_values(self):
        df = generate_sample_data(rows=5, schema={"label": "str"})
        expected = [f"val_{i}" for i in range(5)]
        assert list(df["label"]) == expected

    def test_date_column_values(self):
        df = generate_sample_data(rows=3, schema={"dt": "date"})
        assert len(df["dt"]) == 3
        assert all(isinstance(v, datetime) for v in df["dt"])

    def test_unknown_engine_raises(self):
        with pytest.raises(ValueError, match="Unknown engine type"):
            generate_sample_data(engine_type="polars")

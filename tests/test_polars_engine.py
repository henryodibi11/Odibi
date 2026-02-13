import builtins

import pytest

import pandas as pd

pl = pytest.importorskip("polars")

from odibi.context import PolarsContext  # noqa: E402
from odibi.engine.polars_engine import PolarsEngine  # noqa: E402


class MockConnection:
    def get_path(self, path):
        return path


class MockSqlConnection:
    """Mock SQL connection with read_table support."""

    def read_table(self, table_name: str, schema: str = "dbo") -> pd.DataFrame:
        return pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [100.0, 200.0, 300.0],
            }
        )


@pytest.fixture
def polars_engine():
    try:
        return PolarsEngine()
    except ImportError:
        pytest.skip("Polars not installed")


def test_polars_engine_end_to_end(tmp_path, polars_engine):
    # 1. Create dummy data
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "score": [85.0, 90.0, 78.0, 92.0, 88.0],
    }
    input_csv = tmp_path / "input.csv"
    pl.DataFrame(data).write_csv(input_csv)

    # 2. Read data (Lazy)
    connection = MockConnection()
    df = polars_engine.read(connection=connection, format="csv", path=str(input_csv))
    assert isinstance(df, pl.LazyFrame)

    # 3. Validate schema
    schema = polars_engine.get_schema(df)
    assert "id" in schema
    assert "name" in schema

    # 4. Count rows
    count = polars_engine.count_rows(df)
    assert count == 5

    # 5. Write data
    output_parquet = tmp_path / "output.parquet"
    polars_engine.write(df, connection=connection, format="parquet", path=str(output_parquet))

    assert output_parquet.exists()
    assert pl.scan_parquet(output_parquet).collect().shape == (5, 3)

    # 6. Profile nulls
    nulls = polars_engine.profile_nulls(df)
    assert nulls["id"] == 0.0


def test_polars_execute_sql(tmp_path, polars_engine):
    # Create data
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).lazy()

    # Use Real Context
    context = PolarsContext()
    context.register("my_table", df)

    # Execute SQL
    result_df = polars_engine.execute_sql("SELECT a, b FROM my_table WHERE a > 1", context)

    # Verify result
    assert isinstance(result_df, pl.LazyFrame)
    result = result_df.collect()
    assert result.shape == (2, 2)
    assert result["a"].to_list() == [2, 3]


def test_polars_operations(polars_engine):
    df = pl.DataFrame(
        {"group": ["A", "A", "B", "B"], "val": [1, 2, 3, 4], "id": [1, 2, 3, 4]}
    ).lazy()

    # Test sort
    res_sort = polars_engine.execute_operation("sort", {"by": "val", "ascending": False}, df)
    assert res_sort.collect()["val"].to_list() == [4, 3, 2, 1]

    # Test pivot (materializes)
    res_pivot = polars_engine.execute_operation(
        "pivot", {"group_by": ["group"], "pivot_column": "id", "value_column": "val"}, df
    )
    assert isinstance(res_pivot, pl.DataFrame)
    assert "1" in res_pivot.columns

    # Test fillna
    df_null = pl.DataFrame({"a": [1, None], "b": [None, 2]}).lazy()
    res_fill = polars_engine.execute_operation("fillna", {"value": 0}, df_null)
    assert res_fill.collect()["a"].to_list() == [1, 0]


def test_polars_harmonize_schema(polars_engine):
    from odibi.config import OnMissingColumns, OnNewColumns, SchemaMode, SchemaPolicyConfig

    df = pl.DataFrame({"a": [1], "b": [2]}).lazy()

    # Case 1: Enforce (Drop new 'b', Add missing 'c')
    target = {"a": "int", "c": "int"}
    policy = SchemaPolicyConfig(
        mode=SchemaMode.ENFORCE,
        on_missing_columns=OnMissingColumns.FILL_NULL,
        on_new_columns=OnNewColumns.IGNORE,
    )

    res = polars_engine.harmonize_schema(df, target, policy)
    res_df = res.collect()
    assert "b" not in res_df.columns
    assert "c" in res_df.columns
    assert res_df["c"][0] is None


def test_polars_anonymize(polars_engine):
    df = pl.DataFrame({"ssn": ["123-45-6789"], "name": ["Alice"]}).lazy()

    # Test Mask
    res_mask = polars_engine.anonymize(df, ["ssn"], "mask")
    masked = res_mask.collect()["ssn"][0]
    assert masked == "*******6789"

    # Test Hash
    res_hash = polars_engine.anonymize(df, ["name"], "hash", salt="salty")
    hashed = res_hash.collect()["name"][0]
    assert hashed != "Alice"
    assert len(hashed) == 64  # sha256 hex


class TestPolarsAzureSqlRead:
    """Tests for Polars Azure SQL / SQL Server read support."""

    def test_read_azure_sql_basic(self, polars_engine):
        """Test basic Azure SQL read returns LazyFrame."""
        connection = MockSqlConnection()
        df = polars_engine.read(
            connection=connection,
            format="azure_sql",
            table="Sales.Orders",
        )
        assert isinstance(df, pl.LazyFrame)
        result = df.collect()
        assert result.shape == (3, 3)
        assert result.columns == ["id", "name", "value"]

    def test_read_sql_server_format(self, polars_engine):
        """Test sql_server format alias works."""
        connection = MockSqlConnection()
        df = polars_engine.read(
            connection=connection,
            format="sql_server",
            table="dbo.Customers",
        )
        assert isinstance(df, pl.LazyFrame)
        assert df.collect().shape == (3, 3)

    def test_read_sql_format(self, polars_engine):
        """Test generic sql format works."""
        connection = MockSqlConnection()
        df = polars_engine.read(
            connection=connection,
            format="sql",
            path="Products",  # Can use path instead of table
        )
        assert isinstance(df, pl.LazyFrame)

    def test_read_azure_sql_with_schema(self, polars_engine):
        """Test schema.table parsing works correctly."""
        call_log = []

        class TrackingConnection:
            def read_table(self, table_name: str, schema: str = "dbo") -> pd.DataFrame:
                call_log.append({"table": table_name, "schema": schema})
                return pd.DataFrame({"a": [1]})

        connection = TrackingConnection()
        polars_engine.read(
            connection=connection,
            format="azure_sql",
            table="Sales.OrderItems",
        )
        assert call_log[0]["schema"] == "Sales"
        assert call_log[0]["table"] == "OrderItems"

    def test_read_azure_sql_default_schema(self, polars_engine):
        """Test default dbo schema when not specified."""
        call_log = []

        class TrackingConnection:
            def read_table(self, table_name: str, schema: str = "dbo") -> pd.DataFrame:
                call_log.append({"table": table_name, "schema": schema})
                return pd.DataFrame({"a": [1]})

        connection = TrackingConnection()
        polars_engine.read(
            connection=connection,
            format="azure_sql",
            table="Customers",  # No schema prefix
        )
        assert call_log[0]["schema"] == "dbo"
        assert call_log[0]["table"] == "Customers"

    def test_read_azure_sql_no_read_table_raises(self, polars_engine):
        """Test error when connection doesn't support read_table."""
        connection = MockConnection()  # No read_table method
        with pytest.raises(ValueError, match="does not support SQL operations"):
            polars_engine.read(
                connection=connection,
                format="azure_sql",
                table="SomeTable",
            )

    def test_read_azure_sql_no_table_raises(self, polars_engine):
        """Test error when neither table nor path provided."""
        connection = MockSqlConnection()
        with pytest.raises(ValueError, match="requires 'table' or 'path'"):
            polars_engine.read(
                connection=connection,
                format="azure_sql",
            )


class TestPolarsUtilityMethods:
    """Tests for utility methods in PolarsEngine."""

    def test_materialize_lazyframe(self, polars_engine):
        """Test materialize converts LazyFrame to DataFrame."""
        lazy_df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = polars_engine.materialize(lazy_df)
        assert isinstance(result, pl.DataFrame)
        assert result.shape == (3, 1)

    def test_materialize_dataframe_passthrough(self, polars_engine):
        """Test materialize passes through DataFrame unchanged."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = polars_engine.materialize(df)
        assert result is df

    def test_get_shape_dataframe(self, polars_engine):
        """Test get_shape returns correct shape for DataFrame."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        shape = polars_engine.get_shape(df)
        assert shape == (3, 2)

    def test_get_shape_lazyframe(self, polars_engine):
        """Test get_shape returns correct shape for LazyFrame."""
        lazy_df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).lazy()
        shape = polars_engine.get_shape(lazy_df)
        assert shape == (3, 2)

    def test_get_shape_empty_dataframe(self, polars_engine):
        """Test get_shape with empty DataFrame."""
        df = pl.DataFrame({"a": [], "b": []})
        shape = polars_engine.get_shape(df)
        assert shape == (0, 2)

    def test_count_nulls_dataframe(self, polars_engine):
        """Test count_nulls with DataFrame."""
        df = pl.DataFrame({"a": [1, None, 3], "b": [None, None, 6]})
        counts = polars_engine.count_nulls(df, ["a", "b"])
        assert counts["a"] == 1
        assert counts["b"] == 2

    def test_count_nulls_lazyframe(self, polars_engine):
        """Test count_nulls with LazyFrame."""
        lazy_df = pl.DataFrame({"a": [1, None, 3], "b": [None, None, 6]}).lazy()
        counts = polars_engine.count_nulls(lazy_df, ["a", "b"])
        assert counts["a"] == 1
        assert counts["b"] == 2

    def test_count_nulls_no_nulls(self, polars_engine):
        """Test count_nulls when no nulls present."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        counts = polars_engine.count_nulls(df, ["a", "b"])
        assert counts["a"] == 0
        assert counts["b"] == 0

    def test_get_sample_dataframe(self, polars_engine):
        """Test get_sample returns list of dicts from DataFrame."""
        df = pl.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
        sample = polars_engine.get_sample(df, n=3)
        assert len(sample) == 3
        assert sample[0] == {"a": 1, "b": 10}
        assert sample[2] == {"a": 3, "b": 30}

    def test_get_sample_lazyframe(self, polars_engine):
        """Test get_sample returns list of dicts from LazyFrame."""
        lazy_df = pl.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]}).lazy()
        sample = polars_engine.get_sample(lazy_df, n=2)
        assert len(sample) == 2
        assert sample[0] == {"a": 1, "b": 10}

    def test_get_sample_default_n(self, polars_engine):
        """Test get_sample with default n=10."""
        df = pl.DataFrame({"a": list(range(20))})
        sample = polars_engine.get_sample(df)
        assert len(sample) == 10


class TestPolarsValidation:
    """Tests for validation methods in PolarsEngine."""

    def test_validate_schema_required_columns_success(self, polars_engine):
        """Test validate_schema passes when all required columns present."""
        df = pl.DataFrame({"a": [1], "b": [2], "c": [3]})
        rules = {"required_columns": ["a", "b"]}
        failures = polars_engine.validate_schema(df, rules)
        assert len(failures) == 0

    def test_validate_schema_required_columns_missing(self, polars_engine):
        """Test validate_schema fails when required columns missing."""
        df = pl.DataFrame({"a": [1], "b": [2]})
        rules = {"required_columns": ["a", "b", "c"]}
        failures = polars_engine.validate_schema(df, rules)
        assert len(failures) == 1
        assert "Missing required columns" in failures[0]
        assert "c" in failures[0]

    def test_validate_schema_types_success(self, polars_engine):
        """Test validate_schema passes with correct types."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        # Polars string type is "String" - validation checks via substring match
        rules = {"types": {"a": "Int", "b": "String"}}
        failures = polars_engine.validate_schema(df, rules)
        assert len(failures) == 0

    def test_validate_schema_types_mismatch(self, polars_engine):
        """Test validate_schema fails with type mismatch."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        # Column 'a' is Int64, expecting String type should fail validation
        rules = {"types": {"a": "String"}}
        failures = polars_engine.validate_schema(df, rules)
        assert len(failures) == 1
        assert "has type" in failures[0]
        assert "expected" in failures[0]

    def test_validate_schema_type_for_missing_column(self, polars_engine):
        """Test validate_schema handles type check for missing column."""
        df = pl.DataFrame({"a": [1]})
        rules = {"types": {"b": "Int64"}}
        failures = polars_engine.validate_schema(df, rules)
        assert len(failures) == 1
        assert "not found for type validation" in failures[0]

    def test_validate_schema_with_lazyframe(self, polars_engine):
        """Test validate_schema works with LazyFrame."""
        lazy_df = pl.DataFrame({"a": [1], "b": [2]}).lazy()
        rules = {"required_columns": ["a"]}
        failures = polars_engine.validate_schema(lazy_df, rules)
        assert len(failures) == 0

    def test_validate_data_not_empty_pass(self, polars_engine):
        """Test validate_data not_empty check passes with data."""
        df = pl.DataFrame({"a": [1, 2]})

        class Config:
            not_empty = True

        failures = polars_engine.validate_data(df, Config())
        assert len(failures) == 0

    def test_validate_data_not_empty_fail(self, polars_engine):
        """Test validate_data not_empty check fails with empty DataFrame."""
        df = pl.DataFrame({"a": []})

        class Config:
            not_empty = True

        failures = polars_engine.validate_data(df, Config())
        assert len(failures) == 1
        assert "empty" in failures[0].lower()

    def test_validate_data_no_nulls_pass(self, polars_engine):
        """Test validate_data no_nulls check passes."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

        class Config:
            no_nulls = ["a", "b"]

        failures = polars_engine.validate_data(df, Config())
        assert len(failures) == 0

    def test_validate_data_no_nulls_fail(self, polars_engine):
        """Test validate_data no_nulls check fails."""
        df = pl.DataFrame({"a": [1, None, 3], "b": [4, 5, 6]})

        class Config:
            no_nulls = ["a"]

        failures = polars_engine.validate_data(df, Config())
        assert len(failures) == 1
        assert "null values" in failures[0]

    def test_validate_data_ranges_min_pass(self, polars_engine):
        """Test validate_data ranges min check passes."""
        df = pl.DataFrame({"score": [10, 20, 30]})

        class Config:
            ranges = {"score": {"min": 5}}

        failures = polars_engine.validate_data(df, Config())
        assert len(failures) == 0

    def test_validate_data_ranges_min_fail(self, polars_engine):
        """Test validate_data ranges min check fails."""
        df = pl.DataFrame({"score": [1, 20, 30]})

        class Config:
            ranges = {"score": {"min": 10}}

        failures = polars_engine.validate_data(df, Config())
        assert len(failures) == 1
        assert "< 10" in failures[0]

    def test_validate_data_ranges_max_fail(self, polars_engine):
        """Test validate_data ranges max check fails."""
        df = pl.DataFrame({"score": [10, 20, 150]})

        class Config:
            ranges = {"score": {"max": 100}}

        failures = polars_engine.validate_data(df, Config())
        assert len(failures) == 1
        assert "> 100" in failures[0]

    def test_validate_data_allowed_values_pass(self, polars_engine):
        """Test validate_data allowed_values check passes."""
        df = pl.DataFrame({"status": ["ACTIVE", "INACTIVE", "ACTIVE"]})

        class Config:
            allowed_values = {"status": ["ACTIVE", "INACTIVE"]}

        failures = polars_engine.validate_data(df, Config())
        assert len(failures) == 0

    def test_validate_data_allowed_values_fail(self, polars_engine):
        """Test validate_data allowed_values check fails."""
        df = pl.DataFrame({"status": ["ACTIVE", "INVALID", "ACTIVE"]})

        class Config:
            allowed_values = {"status": ["ACTIVE", "INACTIVE"]}

        failures = polars_engine.validate_data(df, Config())
        assert len(failures) == 1
        assert "invalid values" in failures[0]

    def test_validate_data_with_lazyframe(self, polars_engine):
        """Test validate_data works with LazyFrame."""
        lazy_df = pl.DataFrame({"a": [1, 2, 3]}).lazy()

        class Config:
            not_empty = True

        failures = polars_engine.validate_data(lazy_df, Config())
        assert len(failures) == 0


class TestPolarsTableManagement:
    """Tests for table management methods in PolarsEngine."""

    def test_table_exists_with_path(self, tmp_path, polars_engine):
        """Test table_exists returns True when path exists."""
        test_file = tmp_path / "test.parquet"
        pl.DataFrame({"a": [1]}).write_parquet(test_file)

        connection = MockConnection()
        connection.base_path = tmp_path

        class ConnWithGetPath:
            def get_path(self, p):
                return str(tmp_path / p)

        result = polars_engine.table_exists(ConnWithGetPath(), path="test.parquet")
        assert result is True

    def test_table_exists_path_not_found(self, tmp_path, polars_engine):
        """Test table_exists returns False when path doesn't exist."""

        class ConnWithGetPath:
            def get_path(self, p):
                return str(tmp_path / p)

        result = polars_engine.table_exists(ConnWithGetPath(), path="nonexistent.parquet")
        assert result is False

    def test_table_exists_no_path(self, polars_engine):
        """Test table_exists returns False when no path provided."""
        connection = MockConnection()
        result = polars_engine.table_exists(connection)
        assert result is False

    def test_get_table_schema_parquet(self, tmp_path, polars_engine):
        """Test get_table_schema returns schema from parquet file."""
        test_file = tmp_path / "test.parquet"
        pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]}).write_parquet(test_file)

        class ConnWithGetPath:
            def get_path(self, p):
                return str(tmp_path / p)

        schema = polars_engine.get_table_schema(
            ConnWithGetPath(), path="test.parquet", format="parquet"
        )
        assert schema is not None
        assert "id" in schema
        assert "name" in schema

    def test_get_table_schema_csv(self, tmp_path, polars_engine):
        """Test get_table_schema returns schema from CSV file."""
        test_file = tmp_path / "test.csv"
        pl.DataFrame({"id": [1, 2], "value": [10.5, 20.5]}).write_csv(test_file)

        class ConnWithGetPath:
            def get_path(self, p):
                return str(tmp_path / p)

        schema = polars_engine.get_table_schema(ConnWithGetPath(), path="test.csv", format="csv")
        assert schema is not None
        assert "id" in schema
        assert "value" in schema

    def test_get_table_schema_nonexistent_file(self, tmp_path, polars_engine):
        """Test get_table_schema returns None for nonexistent file."""

        class ConnWithGetPath:
            def get_path(self, p):
                return str(tmp_path / p)

        schema = polars_engine.get_table_schema(
            ConnWithGetPath(), path="nonexistent.parquet", format="parquet"
        )
        assert schema is None

    def test_get_source_files_dataframe(self, polars_engine):
        """Test get_source_files returns empty list for DataFrame."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        files = polars_engine.get_source_files(df)
        assert files == []

    def test_get_source_files_lazyframe(self, polars_engine):
        """Test get_source_files returns empty list for LazyFrame."""
        lazy_df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        files = polars_engine.get_source_files(lazy_df)
        assert files == []


class TestPolarsEdgeCases:
    """Tests for edge cases and error handling."""

    def test_operations_on_empty_dataframe(self, polars_engine):
        """Test operations work correctly with empty DataFrame."""
        df = pl.DataFrame({"a": [], "b": []})

        # Count rows
        count = polars_engine.count_rows(df)
        assert count == 0

        # Get schema
        schema = polars_engine.get_schema(df)
        assert "a" in schema
        assert "b" in schema

        # Profile nulls
        nulls = polars_engine.profile_nulls(df)
        assert nulls["a"] == 0.0
        assert nulls["b"] == 0.0

    def test_all_nulls_dataframe(self, polars_engine):
        """Test operations with DataFrame containing only nulls."""
        df = pl.DataFrame({"a": [None, None, None], "b": [None, None, None]})

        # Count nulls
        counts = polars_engine.count_nulls(df, ["a", "b"])
        assert counts["a"] == 3
        assert counts["b"] == 3

        # Profile nulls
        nulls = polars_engine.profile_nulls(df)
        assert nulls["a"] == 1.0
        assert nulls["b"] == 1.0

    def test_mixed_types_handling(self, polars_engine):
        """Test engine handles mixed numeric types correctly."""
        df = pl.DataFrame({"int_col": [1, 2, 3], "float_col": [1.5, 2.5, 3.5]})

        schema = polars_engine.get_schema(df)
        assert "int" in schema["int_col"].lower()
        assert "float" in schema["float_col"].lower()

        # Count rows
        assert polars_engine.count_rows(df) == 3

    def test_large_sample_request(self, polars_engine):
        """Test get_sample handles n larger than DataFrame size."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        sample = polars_engine.get_sample(df, n=100)
        # Should return all 3 rows, not fail
        assert len(sample) <= 3


class TestPolarsDeltaOperations:
    """Tests for Delta Lake specific operations."""

    @pytest.fixture
    def mock_deltalake_import_error(self):
        """Context manager to mock deltalake import failure."""
        import sys

        original_deltalake = sys.modules.get("deltalake")
        original_import = builtins.__import__

        # Remove deltalake from modules to simulate ImportError
        if "deltalake" in sys.modules:
            del sys.modules["deltalake"]

        # Mock builtins.__import__ to raise ImportError for deltalake
        def mock_import(name, *args, **kwargs):
            if name == "deltalake":
                raise ImportError("No module named 'deltalake'")
            return original_import(name, *args, **kwargs)

        builtins.__import__ = mock_import

        yield

        # Restore original import and deltalake module
        builtins.__import__ = original_import
        if original_deltalake:
            sys.modules["deltalake"] = original_deltalake

    def test_vacuum_delta_import_error_handling(self, polars_engine, mock_deltalake_import_error):
        """Test vacuum_delta raises proper error when deltalake not available."""
        connection = MockConnection()
        with pytest.raises(ImportError, match="Delta Lake support requires"):
            polars_engine.vacuum_delta(connection, path="test_path")

    def test_get_delta_history_import_error_handling(
        self, polars_engine, mock_deltalake_import_error
    ):
        """Test get_delta_history raises proper error when deltalake not available."""
        connection = MockConnection()
        with pytest.raises(ImportError, match="Delta Lake support requires"):
            polars_engine.get_delta_history(connection, path="test_path")

    def test_maintain_table_disabled(self, polars_engine):
        """Test maintain_table does nothing when config is disabled."""

        class FakeConfig:
            enabled = False

        connection = MockConnection()
        # Should not raise any errors, just return silently
        polars_engine.maintain_table(
            connection, format="delta", path="test_path", config=FakeConfig()
        )

    def test_maintain_table_non_delta_format(self, polars_engine):
        """Test maintain_table does nothing for non-Delta formats."""

        class FakeConfig:
            enabled = True

        connection = MockConnection()
        # Should not raise errors for non-delta formats
        polars_engine.maintain_table(
            connection, format="parquet", path="test_path", config=FakeConfig()
        )

    def test_maintain_table_no_path(self, polars_engine):
        """Test maintain_table does nothing when no path provided."""

        class FakeConfig:
            enabled = True

        connection = MockConnection()
        # Should return early without error
        polars_engine.maintain_table(connection, format="delta", config=FakeConfig())


class TestPolarsWriteSql:
    """Tests for SQL Server write functionality."""

    def test_write_sql_basic_insert(self, polars_engine):
        """Test _write_sql performs insert operation."""
        df = pl.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        # Track what was written
        written_data = []

        class MockSqlWriteConnection:
            def write_table(self, df, table_name, schema="dbo", if_exists="append", chunksize=1000):
                written_data.append(
                    {
                        "table": table_name,
                        "schema": schema,
                        "if_exists": if_exists,
                        "rows": len(df),
                    }
                )

        connection = MockSqlWriteConnection()
        polars_engine._write_sql(
            df, connection=connection, table="TestTable", mode="append", options={}
        )

        assert len(written_data) == 1
        assert written_data[0]["table"] == "TestTable"
        assert written_data[0]["schema"] == "dbo"
        assert written_data[0]["if_exists"] == "append"
        assert written_data[0]["rows"] == 3

    def test_write_sql_overwrite_mode(self, polars_engine):
        """Test _write_sql with overwrite mode."""
        df = pl.DataFrame({"id": [1]})
        written_data = []

        class MockSqlWriteConnection:
            def write_table(self, df, table_name, schema="dbo", if_exists="append", chunksize=1000):
                written_data.append({"if_exists": if_exists})

        connection = MockSqlWriteConnection()
        polars_engine._write_sql(
            df, connection=connection, table="TestTable", mode="overwrite", options={}
        )

        assert len(written_data) == 1
        assert written_data[0]["if_exists"] == "replace"

    def test_write_sql_fail_mode(self, polars_engine):
        """Test _write_sql with fail mode."""
        df = pl.DataFrame({"id": [1]})
        written_data = []

        class MockSqlWriteConnection:
            def write_table(self, df, table_name, schema="dbo", if_exists="append", chunksize=1000):
                written_data.append({"if_exists": if_exists})

        connection = MockSqlWriteConnection()
        polars_engine._write_sql(
            df, connection=connection, table="TestTable", mode="fail", options={}
        )

        assert len(written_data) == 1
        assert written_data[0]["if_exists"] == "fail"

    def test_write_sql_schema_table_parsing(self, polars_engine):
        """Test _write_sql correctly parses schema.table format."""
        df = pl.DataFrame({"id": [1]})
        written_data = []

        class MockSqlWriteConnection:
            def write_table(self, df, table_name, schema="dbo", if_exists="append", chunksize=1000):
                written_data.append({"table": table_name, "schema": schema})

        connection = MockSqlWriteConnection()
        polars_engine._write_sql(
            df, connection=connection, table="Sales.Orders", mode="append", options={}
        )

        assert len(written_data) == 1
        assert written_data[0]["schema"] == "Sales"
        assert written_data[0]["table"] == "Orders"

    def test_write_sql_default_dbo_schema(self, polars_engine):
        """Test _write_sql uses default dbo schema."""
        df = pl.DataFrame({"id": [1]})
        written_data = []

        class MockSqlWriteConnection:
            def write_table(self, df, table_name, schema="dbo", if_exists="append", chunksize=1000):
                written_data.append({"table": table_name, "schema": schema})

        connection = MockSqlWriteConnection()
        polars_engine._write_sql(
            df, connection=connection, table="Customers", mode="append", options={}
        )

        assert len(written_data) == 1
        assert written_data[0]["schema"] == "dbo"
        assert written_data[0]["table"] == "Customers"

    def test_write_sql_no_write_table_method(self, polars_engine):
        """Test _write_sql raises error when connection doesn't support write_table."""
        df = pl.DataFrame({"id": [1]})
        connection = MockConnection()  # No write_table method

        with pytest.raises(ValueError, match="does not support SQL operations"):
            polars_engine._write_sql(
                df, connection=connection, table="TestTable", mode="append", options={}
            )

    def test_write_sql_no_table_provided(self, polars_engine):
        """Test _write_sql raises error when no table provided."""
        df = pl.DataFrame({"id": [1]})

        class MockSqlWriteConnection:
            def write_table(self, df, table_name, schema="dbo", if_exists="append", chunksize=1000):
                pass

        connection = MockSqlWriteConnection()

        with pytest.raises(ValueError, match="table' parameter is required"):
            polars_engine._write_sql(
                df, connection=connection, table=None, mode="append", options={}
            )

    def test_write_sql_with_chunksize_option(self, polars_engine):
        """Test _write_sql respects chunksize option."""
        df = pl.DataFrame({"id": [1, 2, 3, 4, 5]})
        written_data = []

        class MockSqlWriteConnection:
            def write_table(self, df, table_name, schema="dbo", if_exists="append", chunksize=1000):
                written_data.append({"chunksize": chunksize})

        connection = MockSqlWriteConnection()
        polars_engine._write_sql(
            df, connection=connection, table="TestTable", mode="append", options={"chunksize": 500}
        )

        assert len(written_data) == 1
        assert written_data[0]["chunksize"] == 500

    def test_write_sql_materializes_lazyframe(self, polars_engine):
        """Test _write_sql materializes LazyFrame before writing."""
        lazy_df = pl.DataFrame({"id": [1, 2, 3]}).lazy()
        written_data = []

        class MockSqlWriteConnection:
            def write_table(self, df, table_name, schema="dbo", if_exists="append", chunksize=1000):
                written_data.append({"rows": len(df)})

        connection = MockSqlWriteConnection()
        polars_engine._write_sql(
            lazy_df, connection=connection, table="TestTable", mode="append", options={}
        )

        assert len(written_data) == 1
        assert written_data[0]["rows"] == 3


class TestPolarsAddWriteMetadata:
    """Test add_write_metadata method for PolarsEngine."""

    def test_add_write_metadata_all_defaults(self, polars_engine):
        """add_write_metadata with True adds enabled metadata."""
        df = pl.DataFrame({"a": [1, 2]})
        result = polars_engine.add_write_metadata(
            df,
            metadata_config=True,
            source_connection="my_conn",
            source_table="my_table",
            source_path="/path/to/file.csv",
            is_file_source=True,
        )

        # Default WriteMetadataConfig enables: extracted_at, source_file
        # but NOT source_connection or source_table
        assert "_extracted_at" in result.columns
        assert "_source_file" in result.columns
        # These are False by default
        assert "_source_connection" not in result.columns
        assert "_source_table" not in result.columns
        assert all(result["_source_file"] == "/path/to/file.csv")

    def test_add_write_metadata_file_only_for_file_source(self, polars_engine):
        """add_write_metadata only adds source_file for file sources."""
        from odibi.config import WriteMetadataConfig

        df = pl.DataFrame({"a": [1]})
        config = WriteMetadataConfig(
            extracted_at=False, source_file=True, source_connection=False, source_table=False
        )

        # File source
        result1 = polars_engine.add_write_metadata(
            df, config, source_path="/file.csv", is_file_source=True
        )
        assert "_source_file" in result1.columns

        # SQL source
        result2 = polars_engine.add_write_metadata(
            df, config, source_path="/file.csv", is_file_source=False
        )
        assert "_source_file" not in result2.columns

    def test_add_write_metadata_selective_config(self, polars_engine):
        """add_write_metadata respects config selections."""
        from odibi.config import WriteMetadataConfig

        df = pl.DataFrame({"a": [1]})
        config = WriteMetadataConfig(
            extracted_at=True, source_file=False, source_connection=True, source_table=False
        )

        result = polars_engine.add_write_metadata(
            df,
            config,
            source_connection="conn1",
            source_table="table1",
            source_path="/file.csv",
            is_file_source=True,
        )

        assert "_extracted_at" in result.columns
        assert "_source_connection" in result.columns
        assert "_source_file" not in result.columns
        assert "_source_table" not in result.columns

    def test_add_write_metadata_none_returns_unchanged(self, polars_engine):
        """add_write_metadata with None returns DataFrame unchanged."""
        df = pl.DataFrame({"a": [1, 2]})
        result = polars_engine.add_write_metadata(df, metadata_config=None)

        # Check that columns are the same
        assert result.columns == df.columns
        assert "_extracted_at" not in result.columns

    def test_add_write_metadata_all_metadata_types(self, polars_engine):
        """add_write_metadata can add all metadata types."""
        from odibi.config import WriteMetadataConfig

        df = pl.DataFrame({"a": [1]})
        config = WriteMetadataConfig(
            extracted_at=True, source_file=True, source_connection=True, source_table=True
        )

        result = polars_engine.add_write_metadata(
            df,
            config,
            source_connection="conn1",
            source_table="table1",
            source_path="/file.csv",
            is_file_source=True,
        )

        assert "_extracted_at" in result.columns
        assert "_source_file" in result.columns
        assert "_source_connection" in result.columns
        assert "_source_table" in result.columns
        assert all(result["_source_file"] == "/file.csv")
        assert all(result["_source_connection"] == "conn1")
        assert all(result["_source_table"] == "table1")

    def test_add_write_metadata_with_lazyframe(self, polars_engine):
        """add_write_metadata works with LazyFrame."""
        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = polars_engine.add_write_metadata(
            df,
            metadata_config=True,
            source_path="/path/to/file.csv",
            is_file_source=True,
        )

        # Result should still be lazy
        assert isinstance(result, pl.LazyFrame)

        # Collect and verify
        materialized = result.collect()
        assert "_extracted_at" in materialized.columns
        assert "_source_file" in materialized.columns

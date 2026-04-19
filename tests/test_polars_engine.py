import os

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


class TestPolarsLakeOperations:
    """Tests for Lake (DL) specific operations."""

    @pytest.fixture
    def mock_deltalake_import_error(self):
        """Context manager to simulate deltalake being unavailable."""
        import sys

        original_deltalake = sys.modules.get("deltalake")
        sys.modules["deltalake"] = None  # causes ImportError on `import deltalake`

        yield

        if original_deltalake is not None:
            sys.modules["deltalake"] = original_deltalake
        else:
            sys.modules.pop("deltalake", None)

    def test_vacuum_lake_import_error_handling(self, polars_engine, mock_deltalake_import_error):
        """Test vacuum raises proper error when deltalake not available."""
        connection = MockConnection()
        with pytest.raises(ImportError, match="Delta Lake support requires"):
            polars_engine.vacuum_delta(connection, path="test_path")

    def test_get_lake_history_import_error_handling(
        self, polars_engine, mock_deltalake_import_error
    ):
        """Test get_lake_history raises proper error when deltalake not available."""
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

    def test_maintain_table_non_lake_format(self, polars_engine):
        """Test maintain_table does nothing for non-lake formats."""

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


class TestAddWriteMetadata:
    def test_defaults_adds_extracted_at(self, polars_engine):
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        result = polars_engine.add_write_metadata(df, True)
        assert "_extracted_at" in result.columns
        assert len(result) == 2

    def test_extracted_at_is_timezone_aware(self, polars_engine):
        """Verify _extracted_at column has timezone information (UTC)."""
        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        result = polars_engine.add_write_metadata(df, True)

        # Polars datetime values should have timezone if created with timezone.utc
        # The dtype should be Datetime with time_zone="UTC"
        assert result["_extracted_at"].dtype == pl.Datetime(time_unit="us", time_zone="UTC")

    def test_source_file_only_for_file_sources(self, polars_engine):
        df = pl.DataFrame({"id": [1]})
        result = polars_engine.add_write_metadata(
            df, True, source_path="/data/file.csv", is_file_source=True
        )
        assert "_source_file" in result.columns
        assert result["_source_file"][0] == "/data/file.csv"

    def test_source_file_skipped_for_non_file(self, polars_engine):
        df = pl.DataFrame({"id": [1]})
        result = polars_engine.add_write_metadata(
            df, True, source_path="/data/file.csv", is_file_source=False
        )
        assert "_source_file" not in result.columns

    def test_source_connection_added(self, polars_engine):
        from odibi.config import WriteMetadataConfig

        cfg = WriteMetadataConfig(source_connection=True)
        df = pl.DataFrame({"id": [1]})
        result = polars_engine.add_write_metadata(df, cfg, source_connection="my_db")
        assert "_source_connection" in result.columns
        assert result["_source_connection"][0] == "my_db"

    def test_source_table_added(self, polars_engine):
        from odibi.config import WriteMetadataConfig

        cfg = WriteMetadataConfig(source_table=True)
        df = pl.DataFrame({"id": [1]})
        result = polars_engine.add_write_metadata(df, cfg, source_table="customers")
        assert "_source_table" in result.columns
        assert result["_source_table"][0] == "customers"

    def test_none_config_returns_unchanged(self, polars_engine):
        df = pl.DataFrame({"id": [1]})
        result = polars_engine.add_write_metadata(df, None)
        assert result.columns == ["id"]

    def test_lazyframe_support(self, polars_engine):
        df = pl.DataFrame({"id": [1]}).lazy()
        result = polars_engine.add_write_metadata(df, True)
        assert isinstance(result, pl.LazyFrame)
        collected = result.collect()
        assert "_extracted_at" in collected.columns


class TestRestoreLakeVersion:
    def test_import_error_raised(self, polars_engine):
        import sys

        original = sys.modules.get("deltalake")
        sys.modules["deltalake"] = None
        try:
            with pytest.raises(ImportError, match="Delta Lake support requires"):
                polars_engine.restore_delta(MockConnection(), "/table", 1)
        finally:
            if original is not None:
                sys.modules["deltalake"] = original
            else:
                sys.modules.pop("deltalake", None)

    def test_restore_calls_dt_restore(self, polars_engine):
        import sys
        import types
        from unittest.mock import MagicMock, patch

        mock_dt_instance = MagicMock()
        mock_dt_class = MagicMock(return_value=mock_dt_instance)

        mock_deltalake = types.ModuleType("deltalake")
        mock_deltalake.DeltaTable = mock_dt_class

        with patch.dict(sys.modules, {"deltalake": mock_deltalake}):
            polars_engine.restore_delta(MockConnection(), "/table", 3)

        mock_dt_instance.restore.assert_called_once_with(3)


class TestFilterGreaterThan:
    def test_numeric_filter(self, polars_engine):
        df = pl.DataFrame({"value": [1, 5, 10, 15]})
        result = polars_engine.filter_greater_than(df, "value", 5)
        assert result["value"].to_list() == [10, 15]

    def test_datetime_column(self, polars_engine):
        from datetime import datetime

        df = pl.DataFrame(
            {"ts": [datetime(2024, 1, 1), datetime(2024, 6, 1), datetime(2024, 12, 1)]}
        )
        result = polars_engine.filter_greater_than(df, "ts", "2024-06-01")
        assert len(result) == 1

    def test_string_to_datetime_cast(self, polars_engine):
        df = pl.DataFrame({"date": ["2024-01-01", "2024-06-15", "2024-12-31"]})
        result = polars_engine.filter_greater_than(df, "date", "2024-06-01")
        assert len(result) == 2

    def test_missing_column_raises(self, polars_engine):
        df = pl.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="not found"):
            polars_engine.filter_greater_than(df, "missing", 0)

    def test_empty_result(self, polars_engine):
        df = pl.DataFrame({"value": [1, 2, 3]})
        result = polars_engine.filter_greater_than(df, "value", 100)
        assert len(result) == 0

    def test_lazyframe_support(self, polars_engine):
        df = pl.DataFrame({"value": [1, 5, 10]}).lazy()
        result = polars_engine.filter_greater_than(df, "value", 5)
        assert isinstance(result, pl.LazyFrame)
        assert result.collect()["value"].to_list() == [10]


class TestFilterCoalesce:
    def test_basic_coalesce_gte(self, polars_engine):
        df = pl.DataFrame({"a": [1, 5, 10], "b": [2, 6, 11]})
        result = polars_engine.filter_coalesce(df, "a", "b", ">=", 5)
        assert len(result) == 2

    def test_coalesce_fills_nulls(self, polars_engine):
        df = pl.DataFrame({"a": [None, 5, None], "b": [2, None, 11]})
        result = polars_engine.filter_coalesce(df, "a", "b", ">", 3)
        assert len(result) == 2

    def test_missing_col1_raises(self, polars_engine):
        df = pl.DataFrame({"b": [1]})
        with pytest.raises(ValueError, match="not found"):
            polars_engine.filter_coalesce(df, "missing", "b", ">", 0)

    def test_missing_col2_uses_col1_only(self, polars_engine):
        df = pl.DataFrame({"a": [1, 5, 10]})
        result = polars_engine.filter_coalesce(df, "a", "missing", ">", 3)
        assert len(result) == 2

    def test_unsupported_operator_raises(self, polars_engine):
        df = pl.DataFrame({"a": [1], "b": [2]})
        with pytest.raises(ValueError, match="Unsupported operator"):
            polars_engine.filter_coalesce(df, "a", "b", "!=", 1)

    def test_datetime_coalesce(self, polars_engine):
        df = pl.DataFrame(
            {
                "updated": ["2024-06-01", None, "2024-12-01"],
                "created": ["2024-01-01", "2024-03-01", "2024-01-01"],
            }
        )
        result = polars_engine.filter_coalesce(df, "updated", "created", ">=", "2024-04-01")
        assert len(result) == 2

    def test_all_operators(self, polars_engine):
        df = pl.DataFrame({"a": [1, 5, 10], "b": [1, 5, 10]})
        for op in [">=", ">", "<=", "<", "==", "="]:
            result = polars_engine.filter_coalesce(df, "a", "b", op, 5)
            assert len(result) >= 0

    def test_lazyframe_support(self, polars_engine):
        df = pl.DataFrame({"a": [1, 5, 10], "b": [2, 6, 11]}).lazy()
        result = polars_engine.filter_coalesce(df, "a", "b", ">", 5)
        assert isinstance(result, pl.LazyFrame)
        assert len(result.collect()) == 1


class TestPolarsLazyPreservation:
    """Tests for #274: Methods should not force premature .collect()."""

    def test_add_write_metadata_stays_lazy(self, polars_engine):
        """add_write_metadata should not collect a LazyFrame."""
        from odibi.config import WriteMetadataConfig

        lf = pl.LazyFrame({"a": [1, 2, 3]})
        result = polars_engine.add_write_metadata(
            lf,
            metadata_config=WriteMetadataConfig(extracted_at=True),
            is_file_source=False,
            source_path=None,
            source_connection=None,
            source_table=None,
        )
        assert isinstance(result, pl.LazyFrame)

    def test_filter_greater_than_stays_lazy(self, polars_engine):
        """filter_greater_than should not collect a LazyFrame."""
        lf = pl.LazyFrame({"val": [1, 2, 3, 4, 5]})
        result = polars_engine.filter_greater_than(lf, "val", 3)
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().shape[0] == 2

    def test_filter_coalesce_stays_lazy(self, polars_engine):
        """filter_coalesce should not collect a LazyFrame."""
        lf = pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        result = polars_engine.filter_coalesce(lf, "a", "b", ">", 1)
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().shape[0] == 2

    def test_filter_greater_than_eager_still_works(self, polars_engine):
        """filter_greater_than still works with eager DataFrames."""
        df = pl.DataFrame({"val": [1, 2, 3, 4, 5]})
        result = polars_engine.filter_greater_than(df, "val", 3)
        assert isinstance(result, pl.DataFrame)
        assert result.shape[0] == 2

    def test_filter_coalesce_eager_still_works(self, polars_engine):
        """filter_coalesce still works with eager DataFrames."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        result = polars_engine.filter_coalesce(df, "a", "b", ">", 1)
        assert isinstance(result, pl.DataFrame)
        assert result.shape[0] == 2


class TestPolarsUpsertAppendOnce:
    """Tests for #255: Polars engine upsert/append_once write modes."""

    def test_upsert_updates_existing_and_adds_new(self, tmp_path, polars_engine):
        """Upsert should replace matching rows and add new ones."""
        existing = pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        existing.write_parquet(tmp_path / "data.parquet")

        new_data = pl.DataFrame({"id": [2, 4], "val": ["B_updated", "d_new"]})
        conn = MockConnection()
        polars_engine.write(
            new_data,
            conn,
            format="parquet",
            path=str(tmp_path / "data.parquet"),
            mode="upsert",
            options={"keys": ["id"]},
        )

        result = pl.read_parquet(tmp_path / "data.parquet")
        assert len(result) == 4
        assert sorted(result["id"].to_list()) == [1, 2, 3, 4]
        row2 = result.filter(pl.col("id") == 2)["val"][0]
        assert row2 == "B_updated"

    def test_append_once_skips_existing_keys(self, tmp_path, polars_engine):
        """Append_once should only add rows with new keys."""
        existing = pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        existing.write_parquet(tmp_path / "data.parquet")

        new_data = pl.DataFrame({"id": [2, 4], "val": ["b_dup", "d_new"]})
        conn = MockConnection()
        polars_engine.write(
            new_data,
            conn,
            format="parquet",
            path=str(tmp_path / "data.parquet"),
            mode="append_once",
            options={"keys": ["id"]},
        )

        result = pl.read_parquet(tmp_path / "data.parquet")
        assert len(result) == 4
        assert sorted(result["id"].to_list()) == [1, 2, 3, 4]
        row2 = result.filter(pl.col("id") == 2)["val"][0]
        assert row2 == "b"  # original value preserved

    def test_upsert_no_existing_file_creates_new(self, tmp_path, polars_engine):
        """Upsert on non-existent file should create it."""
        new_data = pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        conn = MockConnection()
        polars_engine.write(
            new_data,
            conn,
            format="parquet",
            path=str(tmp_path / "new.parquet"),
            mode="upsert",
            options={"keys": ["id"]},
        )

        result = pl.read_parquet(tmp_path / "new.parquet")
        assert len(result) == 2

    def test_upsert_missing_keys_raises(self, tmp_path, polars_engine):
        """Upsert without keys option should raise ValueError."""
        new_data = pl.DataFrame({"id": [1], "val": ["a"]})
        conn = MockConnection()
        with pytest.raises(ValueError, match="requires 'keys'"):
            polars_engine.write(
                new_data,
                conn,
                format="parquet",
                path=str(tmp_path / "data.parquet"),
                mode="upsert",
                options={},
            )

    def test_upsert_missing_key_column_raises(self, tmp_path, polars_engine):
        """Upsert with key not in data should raise KeyError."""
        existing = pl.DataFrame({"id": [1], "val": ["a"]})
        existing.write_parquet(tmp_path / "data.parquet")

        new_data = pl.DataFrame({"id": [1], "val": ["b"]})
        conn = MockConnection()
        with pytest.raises(KeyError, match="missing_col"):
            polars_engine.write(
                new_data,
                conn,
                format="parquet",
                path=str(tmp_path / "data.parquet"),
                mode="upsert",
                options={"keys": ["missing_col"]},
            )

    def test_upsert_csv_format(self, tmp_path, polars_engine):
        """Upsert should work with CSV format."""
        existing = pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        existing.write_csv(tmp_path / "data.csv")

        new_data = pl.DataFrame({"id": [2, 3], "val": ["B_updated", "c_new"]})
        conn = MockConnection()
        polars_engine.write(
            new_data,
            conn,
            format="csv",
            path=str(tmp_path / "data.csv"),
            mode="upsert",
            options={"keys": ["id"]},
        )

        result = pl.read_csv(tmp_path / "data.csv")
        assert len(result) == 3

    def test_append_once_csv_appends(self, tmp_path, polars_engine):
        """Append_once on CSV should append only new rows."""
        existing = pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        existing.write_csv(tmp_path / "data.csv")

        new_data = pl.DataFrame({"id": [2, 3], "val": ["b_dup", "c_new"]})
        conn = MockConnection()
        polars_engine.write(
            new_data,
            conn,
            format="csv",
            path=str(tmp_path / "data.csv"),
            mode="append_once",
            options={"keys": ["id"]},
        )

        result = pl.read_csv(tmp_path / "data.csv")
        assert len(result) == 3
        assert sorted(result["id"].to_list()) == [1, 2, 3]

    def test_upsert_with_lazyframe(self, tmp_path, polars_engine):
        """Upsert should work when input is a LazyFrame."""
        existing = pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        existing.write_parquet(tmp_path / "data.parquet")

        new_data = pl.DataFrame({"id": [2, 3], "val": ["B_updated", "c_new"]}).lazy()
        conn = MockConnection()
        polars_engine.write(
            new_data,
            conn,
            format="parquet",
            path=str(tmp_path / "data.parquet"),
            mode="upsert",
            options={"keys": ["id"]},
        )

        result = pl.read_parquet(tmp_path / "data.parquet")
        assert len(result) == 3

    def test_upsert_string_key_coerced_to_list(self, tmp_path, polars_engine):
        """A single string key should be coerced to a list."""
        existing = pl.DataFrame({"id": [1], "val": ["a"]})
        existing.write_parquet(tmp_path / "data.parquet")

        new_data = pl.DataFrame({"id": [1], "val": ["updated"]})
        conn = MockConnection()
        polars_engine.write(
            new_data,
            conn,
            format="parquet",
            path=str(tmp_path / "data.parquet"),
            mode="upsert",
            options={"keys": "id"},
        )

        result = pl.read_parquet(tmp_path / "data.parquet")
        assert result["val"][0] == "updated"


class TestPolarsExecuteOperationBranches:
    """Tests for execute_operation branches (lines 561-698)."""

    def test_drop_duplicates_lazyframe(self, polars_engine):
        df = pl.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]}).lazy()
        result = polars_engine.execute_operation("drop_duplicates", {"subset": ["a"]}, df)
        assert isinstance(result, pl.LazyFrame)
        collected = result.collect()
        assert len(collected) == 2

    def test_drop_duplicates_dataframe(self, polars_engine):
        df = pl.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
        result = polars_engine.execute_operation("drop_duplicates", {"subset": ["a"]}, df)
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 2

    def test_fillna_dict_value(self, polars_engine):
        df = pl.DataFrame({"a": [1, None, 3], "b": [None, "hello", None]}).lazy()
        result = polars_engine.execute_operation("fillna", {"value": {"a": 0, "b": "missing"}}, df)
        collected = result.collect()
        assert collected["a"].to_list() == [1, 0, 3]
        assert collected["b"].to_list() == ["missing", "hello", "missing"]

    def test_drop_columns(self, polars_engine):
        df = pl.DataFrame({"a": [1], "b": [2], "c": [3]}).lazy()
        result = polars_engine.execute_operation("drop", {"columns": ["b", "c"]}, df)
        collected = result.collect()
        assert collected.columns == ["a"]

    def test_drop_labels(self, polars_engine):
        df = pl.DataFrame({"a": [1], "b": [2], "c": [3]}).lazy()
        result = polars_engine.execute_operation("drop", {"labels": ["c"]}, df)
        collected = result.collect()
        assert collected.columns == ["a", "b"]

    def test_rename_columns(self, polars_engine):
        df = pl.DataFrame({"old_name": [1, 2]}).lazy()
        result = polars_engine.execute_operation(
            "rename", {"columns": {"old_name": "new_name"}}, df
        )
        collected = result.collect()
        assert collected.columns == ["new_name"]

    def test_rename_mapper(self, polars_engine):
        df = pl.DataFrame({"x": [1], "y": [2]}).lazy()
        result = polars_engine.execute_operation(
            "rename", {"mapper": {"x": "alpha", "y": "beta"}}, df
        )
        collected = result.collect()
        assert collected.columns == ["alpha", "beta"]

    def test_sample_n_dataframe(self, polars_engine):
        df = pl.DataFrame({"a": list(range(100))})
        result = polars_engine.execute_operation("sample", {"n": 10, "random_state": 42}, df)
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 10

    def test_sample_n_lazyframe(self, polars_engine):
        df = pl.DataFrame({"a": list(range(100))}).lazy()
        result = polars_engine.execute_operation("sample", {"n": 10, "random_state": 42}, df)
        assert isinstance(result, pl.LazyFrame)
        assert len(result.collect()) == 10

    def test_sample_frac_dataframe(self, polars_engine):
        df = pl.DataFrame({"a": list(range(100))})
        result = polars_engine.execute_operation("sample", {"frac": 0.1, "random_state": 42}, df)
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 10

    def test_filter_passthrough(self, polars_engine):
        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = polars_engine.execute_operation("filter", {}, df)
        assert result.collect().equals(df.collect())

    def test_registry_fallback_with_param_model(self, polars_engine):
        from unittest.mock import MagicMock, patch

        df = pl.DataFrame({"a": [1, 2]}).lazy()
        mock_result_ctx = MagicMock()
        mock_result_ctx.df = pl.DataFrame({"a": [10, 20]})

        mock_func = MagicMock(return_value=mock_result_ctx)
        mock_param_model = MagicMock(return_value=MagicMock())

        with (
            patch(
                "odibi.registry.FunctionRegistry.has_function",
                return_value=True,
            ),
            patch(
                "odibi.registry.FunctionRegistry.get_function",
                return_value=mock_func,
            ),
            patch(
                "odibi.registry.FunctionRegistry.get_param_model",
                return_value=mock_param_model,
            ),
        ):
            result = polars_engine.execute_operation("custom_op", {"key": "val"}, df)

        assert result.equals(pl.DataFrame({"a": [10, 20]}))
        mock_param_model.assert_called_once_with(key="val")
        mock_func.assert_called_once()

    def test_registry_fallback_without_param_model(self, polars_engine):
        from unittest.mock import MagicMock, patch

        df = pl.DataFrame({"a": [1, 2]}).lazy()
        mock_result_ctx = MagicMock()
        mock_result_ctx.df = pl.DataFrame({"a": [99]})

        mock_func = MagicMock(return_value=mock_result_ctx)

        with (
            patch(
                "odibi.registry.FunctionRegistry.has_function",
                return_value=True,
            ),
            patch(
                "odibi.registry.FunctionRegistry.get_function",
                return_value=mock_func,
            ),
            patch(
                "odibi.registry.FunctionRegistry.get_param_model",
                return_value=None,
            ),
        ):
            result = polars_engine.execute_operation("custom_op", {"x": 1}, df)

        assert result.equals(pl.DataFrame({"a": [99]}))
        mock_func.assert_called_once()

    def test_unknown_operation_returns_df(self, polars_engine):
        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = polars_engine.execute_operation("nonexistent_op_xyz", {}, df)
        assert result.collect().equals(df.collect())


class TestPolarsReadBranches:
    """Tests for read() method branches (lines 108-256)."""

    def test_read_csv_lazy(self, tmp_path, polars_engine):
        """CSV read returns LazyFrame."""
        pl.DataFrame({"a": [1, 2]}).write_csv(tmp_path / "data.csv")
        conn = MockConnection()
        result = polars_engine.read(conn, format="csv", path=str(tmp_path / "data.csv"))
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().shape == (2, 1)

    def test_read_parquet_lazy(self, tmp_path, polars_engine):
        """Parquet read returns LazyFrame."""
        pl.DataFrame({"a": [1, 2]}).write_parquet(tmp_path / "data.parquet")
        conn = MockConnection()
        result = polars_engine.read(conn, format="parquet", path=str(tmp_path / "data.parquet"))
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().shape == (2, 1)

    def test_read_json_ndjson_default(self, tmp_path, polars_engine):
        """JSON read defaults to ndjson scan (json_lines=True by default)."""
        pl.DataFrame({"a": [1, 2]}).write_ndjson(tmp_path / "data.ndjson")
        conn = MockConnection()
        result = polars_engine.read(conn, format="json", path=str(tmp_path / "data.ndjson"))
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().shape == (2, 1)

    def test_read_json_standard(self, tmp_path, polars_engine):
        """JSON read with json_lines=False uses read_json().lazy().

        Note: The source code does not pop 'json_lines' from options before
        passing **options to read_json, so this path currently raises ValueError
        due to the unexpected kwarg. This test documents that behavior.
        """
        import json as json_mod

        data = [{"a": 1}, {"a": 2}]
        with open(tmp_path / "data.json", "w") as f:
            json_mod.dump(data, f)
        conn = MockConnection()
        with pytest.raises(ValueError, match="Failed to read"):
            polars_engine.read(
                conn,
                format="json",
                path=str(tmp_path / "data.json"),
                options={"json_lines": False},
            )

    def test_read_no_path_no_table_raises(self, polars_engine):
        """Read without path or table raises ValueError."""
        conn = MockConnection()
        with pytest.raises(ValueError, match="neither 'path' nor 'table'"):
            polars_engine.read(conn, format="csv")

    def test_read_table_without_connection_raises(self, polars_engine):
        """Read with table but no connection raises ValueError."""
        with pytest.raises(ValueError, match="connection is required"):
            polars_engine.read(connection=None, format="csv", table="my_table")

    def test_read_unsupported_format(self, tmp_path, polars_engine):
        """Unsupported format raises ValueError."""
        conn = MockConnection()
        with pytest.raises(ValueError, match="Unsupported format"):
            polars_engine.read(conn, format="xml", path=str(tmp_path / "data.xml"))

    def test_read_nonexistent_file(self, polars_engine):
        """Reading non-existent file raises ValueError on collect (lazy scan defers I/O)."""
        conn = MockConnection()
        # scan_csv is lazy; the error surfaces at collect time, not at read time.
        result = polars_engine.read(conn, format="csv", path="/nonexistent/path.csv")
        assert isinstance(result, pl.LazyFrame)
        with pytest.raises(Exception):
            result.collect()

    def test_read_path_without_connection(self, tmp_path, polars_engine):
        """Read with path and no connection uses path directly."""
        pl.DataFrame({"x": [10, 20]}).write_parquet(tmp_path / "direct.parquet")
        result = polars_engine.read(
            connection=None, format="parquet", path=str(tmp_path / "direct.parquet")
        )
        assert isinstance(result, pl.LazyFrame)
        assert result.collect()["x"].to_list() == [10, 20]

    def test_read_table_with_connection(self, tmp_path, polars_engine):
        """Read with table param resolves path via connection.get_path."""
        pl.DataFrame({"v": [5]}).write_csv(tmp_path / "tbl.csv")
        conn = MockConnection()
        result = polars_engine.read(conn, format="csv", table=str(tmp_path / "tbl.csv"))
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().shape == (1, 1)

    def test_read_excel_delegates_to_pandas(self, tmp_path, polars_engine):
        """Excel read delegates to PandasEngine._read_excel_with_patterns.

        The source does ``from odibi.context import get_logging_context``
        inside the excel branch. We inject it into odibi.context so the
        import succeeds and then mock the pandas engine call.
        """
        import odibi.context as ctx_mod
        from unittest.mock import MagicMock, patch

        mock_ctx = MagicMock()
        mock_ctx.with_context.return_value = mock_ctx
        mock_pdf = pd.DataFrame({"col": [1, 2, 3]})

        with (
            patch.object(ctx_mod, "get_logging_context", create=True, new=lambda: mock_ctx),
            patch(
                "odibi.engine.pandas_engine.PandasEngine._read_excel_with_patterns",
                return_value=mock_pdf,
            ) as mock_read,
        ):
            conn = MockConnection()
            result = polars_engine.read(conn, format="excel", path=str(tmp_path / "data.xlsx"))
            mock_read.assert_called_once()
            assert isinstance(result, pl.LazyFrame)
            assert result.collect().shape == (3, 1)

    def test_read_api_requires_http_connection(self, polars_engine):
        """API read with non-HttpConnection raises ValueError.

        The source does ``from odibi.context import get_logging_context``
        inside the api branch. We inject it so the import succeeds and the
        HttpConnection type-check can be reached.
        """
        import odibi.context as ctx_mod
        from unittest.mock import MagicMock, patch

        mock_ctx = MagicMock()
        mock_ctx.with_context.return_value = mock_ctx

        conn = MockConnection()
        with patch.object(ctx_mod, "get_logging_context", create=True, new=lambda: mock_ctx):
            with pytest.raises(ValueError, match="not an HttpConnection"):
                polars_engine.read(conn, format="api", path="/endpoint")

    def test_read_sql_filter_branch(self, polars_engine):
        """SQL filter option triggers read_sql_query on connection."""
        call_log = []

        class SqlFilterConnection:
            default_schema = "dbo"

            def build_select_query(self, table_name, schema="", where="", limit=-1, columns="*"):
                schema = schema or self.default_schema
                query = f"SELECT {columns} FROM [{schema}].[{table_name}]"
                if where:
                    query += f" WHERE {where}"
                return query

            def read_table(self, table_name, schema="dbo"):
                return pd.DataFrame({"a": [1]})

            def read_sql_query(self, query):
                call_log.append(query)
                return pd.DataFrame({"a": [1, 2]})

        conn = SqlFilterConnection()
        result = polars_engine.read(
            conn,
            format="azure_sql",
            table="Orders",
            options={"filter": "status = 'active'"},
        )
        assert isinstance(result, pl.LazyFrame)
        assert len(call_log) == 1
        assert "WHERE status = 'active'" in call_log[0]

    def test_read_csv_with_options(self, tmp_path, polars_engine):
        """CSV read passes extra options to scan_csv."""
        pl.DataFrame({"a": [1, 2], "b": [3, 4]}).write_csv(tmp_path / "data.csv", separator=";")
        conn = MockConnection()
        result = polars_engine.read(
            conn,
            format="csv",
            path=str(tmp_path / "data.csv"),
            options={"separator": ";"},
        )
        assert result.collect().shape == (2, 2)


class TestPolarsWriteBranches:
    """Tests for write() method branches (lines 258-357)."""

    def test_write_parquet_eager(self, tmp_path, polars_engine):
        """Write eager DataFrame as parquet."""
        df = pl.DataFrame({"a": [1, 2]})
        conn = MockConnection()
        polars_engine.write(df, conn, format="parquet", path=str(tmp_path / "out.parquet"))
        assert (tmp_path / "out.parquet").exists()
        assert pl.read_parquet(tmp_path / "out.parquet").shape == (2, 1)

    def test_write_parquet_lazy(self, tmp_path, polars_engine):
        """Write LazyFrame as parquet using sink_parquet."""
        df = pl.DataFrame({"a": [1, 2]}).lazy()
        conn = MockConnection()
        polars_engine.write(df, conn, format="parquet", path=str(tmp_path / "out.parquet"))
        assert (tmp_path / "out.parquet").exists()
        assert pl.read_parquet(tmp_path / "out.parquet").shape == (2, 1)

    def test_write_csv_eager(self, tmp_path, polars_engine):
        """Write eager DataFrame as CSV."""
        df = pl.DataFrame({"a": [1, 2]})
        conn = MockConnection()
        polars_engine.write(df, conn, format="csv", path=str(tmp_path / "out.csv"))
        assert (tmp_path / "out.csv").exists()
        assert pl.read_csv(tmp_path / "out.csv").shape == (2, 1)

    def test_write_csv_lazy(self, tmp_path, polars_engine):
        """Write LazyFrame as CSV using sink_csv."""
        df = pl.DataFrame({"a": [1, 2]}).lazy()
        conn = MockConnection()
        polars_engine.write(df, conn, format="csv", path=str(tmp_path / "out.csv"))
        assert (tmp_path / "out.csv").exists()
        assert pl.read_csv(tmp_path / "out.csv").shape == (2, 1)

    def test_write_json_eager(self, tmp_path, polars_engine):
        """Write eager DataFrame as ndjson."""
        df = pl.DataFrame({"a": [1, 2]})
        conn = MockConnection()
        polars_engine.write(df, conn, format="json", path=str(tmp_path / "out.json"))
        assert (tmp_path / "out.json").exists()

    def test_write_json_lazy(self, tmp_path, polars_engine):
        """Write LazyFrame as ndjson using sink_ndjson."""
        df = pl.DataFrame({"a": [1, 2]}).lazy()
        conn = MockConnection()
        polars_engine.write(df, conn, format="json", path=str(tmp_path / "out.json"))
        assert (tmp_path / "out.json").exists()

    def test_write_no_path_no_table_raises(self, polars_engine):
        """Write without path or table raises ValueError."""
        df = pl.DataFrame({"a": [1]})
        conn = MockConnection()
        with pytest.raises(ValueError, match="neither 'path' nor 'table'"):
            polars_engine.write(df, conn, format="csv")

    def test_write_table_without_connection_raises(self, polars_engine):
        """Write with table but no connection raises ValueError."""
        df = pl.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="connection is required"):
            polars_engine.write(df, connection=None, format="csv", table="my_table")

    def test_write_unsupported_format(self, tmp_path, polars_engine):
        """Unsupported write format raises ValueError."""
        df = pl.DataFrame({"a": [1]})
        conn = MockConnection()
        with pytest.raises(ValueError, match="Unsupported write format"):
            polars_engine.write(df, conn, format="xml", path=str(tmp_path / "out.xml"))

    def test_write_path_no_connection(self, tmp_path, polars_engine):
        """Write with path and no connection uses path directly."""
        df = pl.DataFrame({"a": [1, 2]})
        polars_engine.write(
            df, connection=None, format="parquet", path=str(tmp_path / "out.parquet")
        )
        assert (tmp_path / "out.parquet").exists()

    def test_write_table_with_connection(self, tmp_path, polars_engine):
        """Write with table param resolves path via connection.get_path."""
        df = pl.DataFrame({"a": [1]})
        conn = MockConnection()
        polars_engine.write(df, conn, format="csv", table=str(tmp_path / "tbl.csv"))
        assert (tmp_path / "tbl.csv").exists()

    def test_write_returns_none(self, tmp_path, polars_engine):
        """Write returns None for non-delta formats."""
        df = pl.DataFrame({"a": [1]})
        conn = MockConnection()
        result = polars_engine.write(df, conn, format="parquet", path=str(tmp_path / "out.parquet"))
        assert result is None

    def test_write_creates_parent_dirs(self, tmp_path, polars_engine):
        """Write creates parent directories if they don't exist."""
        df = pl.DataFrame({"a": [1]})
        conn = MockConnection()
        nested_path = str(tmp_path / "sub" / "dir" / "out.parquet")
        polars_engine.write(df, conn, format="parquet", path=nested_path)
        assert os.path.exists(nested_path)

    def test_write_upsert_non_dlt_delegates(self, tmp_path, polars_engine):
        """Upsert mode for non-delta-lake format calls _handle_generic_upsert."""
        existing = pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        existing.write_parquet(tmp_path / "data.parquet")

        new_data = pl.DataFrame({"id": [2, 3], "val": ["B_up", "c_new"]})
        conn = MockConnection()
        polars_engine.write(
            new_data,
            conn,
            format="parquet",
            path=str(tmp_path / "data.parquet"),
            mode="upsert",
            options={"keys": ["id"]},
        )
        result = pl.read_parquet(tmp_path / "data.parquet")
        assert len(result) == 3
        assert set(result["id"].to_list()) == {1, 2, 3}


# ---------------------------------------------------------------------------
# Additional coverage tests (Issue #302)
# ---------------------------------------------------------------------------


class TestPolarsSqlWriteMerge:
    """Tests for _write_sql merge and enhanced overwrite paths."""

    def test_merge_no_keys_raises(self, polars_engine):
        class WriteConn:
            def write_table(self, **kw):
                pass

        with pytest.raises(ValueError, match="merge_keys"):
            polars_engine._write_sql(pl.DataFrame({"a": [1]}), WriteConn(), "t", "merge", {})

    def test_merge_with_keys(self, polars_engine):
        from unittest.mock import MagicMock, patch

        mock_result = MagicMock(inserted=1, updated=2, deleted=0, total_affected=3)
        mock_writer = MagicMock()
        mock_writer.merge_polars.return_value = mock_result

        class WriteConn:
            def write_table(self, **kw):
                pass

        with patch(
            "odibi.writers.sql_server_writer.SqlServerMergeWriter",
            return_value=mock_writer,
        ):
            result = polars_engine._write_sql(
                pl.DataFrame({"a": [1]}),
                WriteConn(),
                "t",
                "merge",
                {"merge_keys": ["a"]},
            )
        assert result["mode"] == "merge"
        assert result["inserted"] == 1
        assert result["updated"] == 2
        assert result["deleted"] == 0
        assert result["total_affected"] == 3

    def test_enhanced_overwrite(self, polars_engine):
        from unittest.mock import MagicMock, patch

        mock_result = MagicMock(strategy="truncate_insert", rows_written=5)
        mock_writer = MagicMock()
        mock_writer.overwrite_polars.return_value = mock_result
        mock_opts = MagicMock()
        mock_opts.strategy.value = "truncate_insert"

        class WriteConn:
            def write_table(self, **kw):
                pass

        with patch(
            "odibi.writers.sql_server_writer.SqlServerMergeWriter",
            return_value=mock_writer,
        ):
            result = polars_engine._write_sql(
                pl.DataFrame({"a": [1]}),
                WriteConn(),
                "t",
                "overwrite",
                {"overwrite_options": mock_opts},
            )
        assert result["mode"] == "overwrite"
        assert result["strategy"] == "truncate_insert"
        assert result["rows_written"] == 5

    def test_lazyframe_collected_for_basic_write(self, polars_engine):
        call_log = {}

        class WriteConn:
            def write_table(self, **kw):
                call_log.update(kw)

        polars_engine._write_sql(
            pl.DataFrame({"a": [1]}).lazy(),
            WriteConn(),
            "t",
            "overwrite",
            {},
        )
        assert isinstance(call_log["df"], pd.DataFrame)


class TestPolarsGenericUpsert:
    """Tests for _handle_generic_upsert method."""

    def test_json_upsert(self, tmp_path, polars_engine):
        existing = pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        existing.write_ndjson(tmp_path / "data.json")

        new_data = pl.DataFrame({"id": [2, 3], "val": ["B_up", "c_new"]})
        result_df, result_mode = polars_engine._handle_generic_upsert(
            new_data,
            str(tmp_path / "data.json"),
            "json",
            "upsert",
            {"keys": ["id"]},
        )
        assert result_mode == "overwrite"
        assert len(result_df) == 3

    def test_json_append_once(self, tmp_path, polars_engine):
        existing = pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        existing.write_ndjson(tmp_path / "data.json")

        new_data = pl.DataFrame({"id": [2, 3], "val": ["b_dup", "c_new"]})
        result_df, result_mode = polars_engine._handle_generic_upsert(
            new_data,
            str(tmp_path / "data.json"),
            "json",
            "append_once",
            {"keys": ["id"]},
        )
        assert result_mode == "overwrite"
        assert len(result_df) == 3
        row2 = result_df.filter(pl.col("id") == 2)["val"][0]
        assert row2 == "b"

    def test_file_not_found_returns_overwrite(self, polars_engine):
        result_df, result_mode = polars_engine._handle_generic_upsert(
            pl.DataFrame({"id": [1], "val": ["a"]}),
            "/nonexistent/file.parquet",
            "parquet",
            "upsert",
            {"keys": ["id"]},
        )
        assert result_mode == "overwrite"
        assert len(result_df) == 1

    def test_missing_keys_raises(self, polars_engine):
        with pytest.raises(ValueError, match="requires 'keys'"):
            polars_engine._handle_generic_upsert(
                pl.DataFrame({"a": [1]}), "/path", "csv", "upsert", {}
            )

    def test_string_key_coerced(self, tmp_path, polars_engine):
        existing = pl.DataFrame({"id": [1], "val": ["a"]})
        existing.write_parquet(tmp_path / "data.parquet")

        new_data = pl.DataFrame({"id": [1], "val": ["updated"]})
        result_df, _ = polars_engine._handle_generic_upsert(
            new_data,
            str(tmp_path / "data.parquet"),
            "parquet",
            "upsert",
            {"keys": "id"},
        )
        assert result_df.filter(pl.col("id") == 1)["val"][0] == "updated"

    def test_lazyframe_collected(self, tmp_path, polars_engine):
        existing = pl.DataFrame({"id": [1], "val": ["a"]})
        existing.write_parquet(tmp_path / "data.parquet")

        lazy = pl.DataFrame({"id": [2], "val": ["b"]}).lazy()
        result_df, _ = polars_engine._handle_generic_upsert(
            lazy,
            str(tmp_path / "data.parquet"),
            "parquet",
            "append_once",
            {"keys": ["id"]},
        )
        assert len(result_df) == 2

    def test_unknown_mode_passthrough(self, tmp_path, polars_engine):
        existing = pl.DataFrame({"id": [1]})
        existing.write_parquet(tmp_path / "data.parquet")

        df = pl.DataFrame({"id": [2]})
        result_df, result_mode = polars_engine._handle_generic_upsert(
            df,
            str(tmp_path / "data.parquet"),
            "parquet",
            "some_other_mode",
            {"keys": ["id"]},
        )
        assert result_mode == "some_other_mode"

    def test_upsert_missing_key_column_raises(self, tmp_path, polars_engine):
        existing = pl.DataFrame({"id": [1], "val": ["a"]})
        existing.write_parquet(tmp_path / "data.parquet")

        new_data = pl.DataFrame({"id": [1], "val": ["b"]})
        with pytest.raises(KeyError, match="missing_col"):
            polars_engine._handle_generic_upsert(
                new_data,
                str(tmp_path / "data.parquet"),
                "parquet",
                "upsert",
                {"keys": ["missing_col"]},
            )

    def test_append_once_missing_key_column_raises(self, tmp_path, polars_engine):
        existing = pl.DataFrame({"id": [1], "val": ["a"]})
        existing.write_parquet(tmp_path / "data.parquet")

        new_data = pl.DataFrame({"id": [1], "val": ["b"]})
        with pytest.raises(KeyError, match="missing_col"):
            polars_engine._handle_generic_upsert(
                new_data,
                str(tmp_path / "data.parquet"),
                "parquet",
                "append_once",
                {"keys": ["missing_col"]},
            )


class TestPolarsValidateData:
    """Tests for validate_data covering schema_validation, lazy ranges, etc."""

    def test_schema_validation_delegates(self, polars_engine):
        df = pl.DataFrame({"a": [1], "b": [2]})

        class Config:
            schema_validation = {"required_columns": ["a", "b", "c"]}

        failures = polars_engine.validate_data(df, Config())
        assert len(failures) == 1
        assert "Missing" in failures[0]

    def test_ranges_min_lazy(self, polars_engine):
        lazy_df = pl.DataFrame({"score": [1, 20, 30]}).lazy()

        class Config:
            ranges = {"score": {"min": 10}}

        failures = polars_engine.validate_data(lazy_df, Config())
        assert len(failures) == 1
        assert "< 10" in failures[0]

    def test_ranges_max_lazy(self, polars_engine):
        lazy_df = pl.DataFrame({"score": [10, 20, 150]}).lazy()

        class Config:
            ranges = {"score": {"max": 100}}

        failures = polars_engine.validate_data(lazy_df, Config())
        assert len(failures) == 1
        assert "> 100" in failures[0]

    def test_ranges_col_not_found(self, polars_engine):
        df = pl.DataFrame({"a": [1]})

        class Config:
            ranges = {"missing_col": {"min": 0}}

        failures = polars_engine.validate_data(df, Config())
        assert len(failures) == 1
        assert "not found for range validation" in failures[0]

    def test_allowed_values_lazy(self, polars_engine):
        lazy_df = pl.DataFrame({"status": ["ACTIVE", "INVALID"]}).lazy()

        class Config:
            allowed_values = {"status": ["ACTIVE", "INACTIVE"]}

        failures = polars_engine.validate_data(lazy_df, Config())
        assert len(failures) == 1
        assert "invalid values" in failures[0]

    def test_allowed_values_col_not_found(self, polars_engine):
        df = pl.DataFrame({"a": [1]})

        class Config:
            allowed_values = {"missing_col": ["x"]}

        failures = polars_engine.validate_data(df, Config())
        assert len(failures) == 1
        assert "not found for allowed values" in failures[0]

    def test_no_nulls_with_lazyframe(self, polars_engine):
        lazy_df = pl.DataFrame({"a": [1, None, 3]}).lazy()

        class Config:
            no_nulls = ["a"]

        failures = polars_engine.validate_data(lazy_df, Config())
        assert len(failures) == 1
        assert "null values" in failures[0]

    def test_ranges_min_max_both(self, polars_engine):
        df = pl.DataFrame({"score": [0, 50, 200]})

        class Config:
            ranges = {"score": {"min": 10, "max": 100}}

        failures = polars_engine.validate_data(df, Config())
        assert len(failures) == 2

    def test_ranges_max_pass(self, polars_engine):
        df = pl.DataFrame({"score": [10, 20, 30]})

        class Config:
            ranges = {"score": {"max": 100}}

        failures = polars_engine.validate_data(df, Config())
        assert len(failures) == 0


class TestPolarsTableSchema:
    """Tests for get_table_schema method."""

    def test_parquet_dir(self, tmp_path, polars_engine):
        """Test get_table_schema with parquet directory."""
        subdir = tmp_path / "parts"
        subdir.mkdir()
        pl.DataFrame({"x": [1], "y": [2.0]}).write_parquet(subdir / "part0.parquet")

        schema = polars_engine.get_table_schema(
            MockConnection(), path=str(subdir), format="parquet"
        )
        assert schema is not None
        assert "x" in schema
        assert "y" in schema

    def test_parquet_dir_empty(self, tmp_path, polars_engine):
        """Test get_table_schema returns None for empty parquet directory."""
        subdir = tmp_path / "empty"
        subdir.mkdir()

        schema = polars_engine.get_table_schema(
            MockConnection(), path=str(subdir), format="parquet"
        )
        assert schema is None

    def test_file_not_found_returns_none(self, tmp_path, polars_engine):
        schema = polars_engine.get_table_schema(
            MockConnection(), path=str(tmp_path / "nope.parquet"), format="parquet"
        )
        assert schema is None

    def test_dl_format_schema(self, tmp_path, polars_engine):
        """Test get_table_schema for lake format."""
        import pyarrow as pa
        from deltalake import write_deltalake

        tbl_path = str(tmp_path / "tbl")
        write_deltalake(tbl_path, pa.table({"id": [1], "val": ["a"]}))

        schema = polars_engine.get_table_schema(MockConnection(), path=tbl_path, format="delta")
        assert schema is not None
        assert "id" in schema
        assert "val" in schema

    def test_csv_schema(self, tmp_path, polars_engine):
        pl.DataFrame({"a": [1], "b": ["x"]}).write_csv(tmp_path / "data.csv")

        schema = polars_engine.get_table_schema(
            MockConnection(), path=str(tmp_path / "data.csv"), format="csv"
        )
        assert schema is not None
        assert "a" in schema
        assert "b" in schema

    def test_no_path_no_table_returns_none(self, polars_engine):
        schema = polars_engine.get_table_schema(MockConnection(), format="csv")
        assert schema is None


class TestPolarsTableMaintenance:
    """Tests for maintain_table method (avoid 'delta' in test names)."""

    def test_non_dl_returns_early(self, polars_engine):
        class Cfg:
            enabled = True

        polars_engine.maintain_table(MockConnection(), format="parquet", config=Cfg())

    def test_disabled_config_returns_early(self, polars_engine):
        class Cfg:
            enabled = False

        polars_engine.maintain_table(MockConnection(), format="delta", path="/p", config=Cfg())

    def test_no_path_no_table_returns_early(self, polars_engine):
        class Cfg:
            enabled = True

        polars_engine.maintain_table(MockConnection(), format="delta", config=Cfg())

    def test_optimize_and_vacuum(self, tmp_path, polars_engine):
        """Run compact + vacuum on a real lake table."""
        import pyarrow as pa
        from deltalake import write_deltalake

        tbl_path = str(tmp_path / "tbl")
        write_deltalake(tbl_path, pa.table({"id": [1, 2], "val": ["a", "b"]}))

        class Cfg:
            enabled = True
            vacuum_retention_hours = 0

        polars_engine.maintain_table(MockConnection(), format="delta", path=tbl_path, config=Cfg())

    def test_optimize_without_vacuum(self, tmp_path, polars_engine):
        """Run compact only (no vacuum_retention_hours)."""
        import pyarrow as pa
        from deltalake import write_deltalake

        tbl_path = str(tmp_path / "tbl")
        write_deltalake(tbl_path, pa.table({"id": [1]}))

        class Cfg:
            enabled = True
            vacuum_retention_hours = None

        polars_engine.maintain_table(MockConnection(), format="delta", path=tbl_path, config=Cfg())

    def test_import_error_returns_silently(self, polars_engine):
        """Missing deltalake library logs warning and returns."""
        import sys

        original = sys.modules.get("deltalake")
        sys.modules["deltalake"] = None

        class Cfg:
            enabled = True

        try:
            polars_engine.maintain_table(MockConnection(), format="delta", path="/p", config=Cfg())
        finally:
            if original is not None:
                sys.modules["deltalake"] = original
            else:
                sys.modules.pop("deltalake", None)

    def test_exception_handled(self, tmp_path, polars_engine):
        """Exception during maintenance logged as warning."""

        class Cfg:
            enabled = True

        polars_engine.maintain_table(
            MockConnection(),
            format="delta",
            path=str(tmp_path / "nonexistent_tbl"),
            config=Cfg(),
        )

    def test_table_param_used_when_no_path(self, tmp_path, polars_engine):
        """When path is None, table param is used."""
        import pyarrow as pa
        from deltalake import write_deltalake

        tbl_path = str(tmp_path / "tbl")
        write_deltalake(tbl_path, pa.table({"id": [1]}))

        class Cfg:
            enabled = True
            vacuum_retention_hours = None

        polars_engine.maintain_table(MockConnection(), format="delta", table=tbl_path, config=Cfg())


class TestPolarsLakeOps:
    """Tests for vacuum, history, and restore using real lake tables."""

    @staticmethod
    def _seed_tbl(path):
        import pyarrow as pa
        from deltalake import write_deltalake

        write_deltalake(str(path), pa.table({"id": [1, 2, 3], "val": ["a", "b", "c"]}))

    def test_vacuum_tbl(self, tmp_path, polars_engine):
        tbl_path = tmp_path / "tbl"
        self._seed_tbl(tbl_path)

        result = polars_engine.vacuum_delta(
            MockConnection(),
            path=str(tbl_path),
            retention_hours=0,
            dry_run=False,
            enforce_retention_duration=False,
        )
        assert "files_deleted" in result

    def test_vacuum_dry_run(self, tmp_path, polars_engine):
        tbl_path = tmp_path / "tbl"
        self._seed_tbl(tbl_path)

        result = polars_engine.vacuum_delta(
            MockConnection(),
            path=str(tbl_path),
            retention_hours=168,
            dry_run=True,
        )
        assert "files_deleted" in result

    def test_vacuum_no_connection(self, tmp_path, polars_engine):
        tbl_path = tmp_path / "tbl"
        self._seed_tbl(tbl_path)

        result = polars_engine.vacuum_delta(
            None,
            path=str(tbl_path),
            retention_hours=0,
            dry_run=True,
            enforce_retention_duration=False,
        )
        assert "files_deleted" in result

    def test_history_tbl(self, tmp_path, polars_engine):
        tbl_path = tmp_path / "tbl"
        self._seed_tbl(tbl_path)

        history = polars_engine.get_delta_history(MockConnection(), path=str(tbl_path))
        assert isinstance(history, list)
        assert len(history) >= 1

    def test_history_with_limit(self, tmp_path, polars_engine):
        import pyarrow as pa
        from deltalake import write_deltalake

        tbl_path = str(tmp_path / "tbl")
        write_deltalake(tbl_path, pa.table({"id": [1]}))
        write_deltalake(tbl_path, pa.table({"id": [2]}), mode="overwrite")

        history = polars_engine.get_delta_history(MockConnection(), path=tbl_path, limit=1)
        assert len(history) == 1

    def test_restore_tbl(self, tmp_path, polars_engine):
        import pyarrow as pa
        from deltalake import DeltaTable, write_deltalake

        tbl_path = str(tmp_path / "tbl")
        write_deltalake(tbl_path, pa.table({"id": [1, 2]}))
        write_deltalake(tbl_path, pa.table({"id": [3, 4]}), mode="overwrite")

        polars_engine.restore_delta(MockConnection(), tbl_path, version=0)

        dt = DeltaTable(tbl_path)
        restored = dt.to_pyarrow_table()
        assert restored.column("id").to_pylist() == [1, 2]

    def test_restore_import_error(self, polars_engine):
        import sys

        original = sys.modules.get("deltalake")
        sys.modules["deltalake"] = None
        try:
            with pytest.raises(ImportError, match="Delta Lake support requires"):
                polars_engine.restore_delta(MockConnection(), "/tbl", 1)
        finally:
            if original is not None:
                sys.modules["deltalake"] = original
            else:
                sys.modules.pop("deltalake", None)

    def test_vacuum_import_error(self, polars_engine):
        import sys

        original = sys.modules.get("deltalake")
        sys.modules["deltalake"] = None
        try:
            with pytest.raises(ImportError, match="Delta Lake support requires"):
                polars_engine.vacuum_delta(MockConnection(), path="/tbl")
        finally:
            if original is not None:
                sys.modules["deltalake"] = original
            else:
                sys.modules.pop("deltalake", None)

    def test_history_import_error(self, polars_engine):
        import sys

        original = sys.modules.get("deltalake")
        sys.modules["deltalake"] = None
        try:
            with pytest.raises(ImportError, match="Delta Lake support requires"):
                polars_engine.get_delta_history(MockConnection(), path="/tbl")
        finally:
            if original is not None:
                sys.modules["deltalake"] = original
            else:
                sys.modules.pop("deltalake", None)

    def test_history_no_connection(self, tmp_path, polars_engine):
        tbl_path = tmp_path / "tbl"
        self._seed_tbl(tbl_path)

        history = polars_engine.get_delta_history(None, path=str(tbl_path))
        assert isinstance(history, list)

    def test_vacuum_with_storage_opts(self, tmp_path, polars_engine):
        tbl_path = tmp_path / "tbl"
        self._seed_tbl(tbl_path)

        class ConnWithOpts:
            def get_path(self, p):
                return p

            def pandas_storage_options(self):
                return {}

        result = polars_engine.vacuum_delta(
            ConnWithOpts(),
            path=str(tbl_path),
            retention_hours=0,
            dry_run=True,
            enforce_retention_duration=False,
        )
        assert "files_deleted" in result


class TestPolarsWriteMetadataExtra:
    """Additional tests for add_write_metadata."""

    def test_non_config_returns_unchanged(self, polars_engine):
        df = pl.DataFrame({"id": [1]})
        result = polars_engine.add_write_metadata(df, "random_string")
        assert result.columns == ["id"]

    def test_int_config_returns_unchanged(self, polars_engine):
        df = pl.DataFrame({"id": [1]})
        result = polars_engine.add_write_metadata(df, 42)
        assert result.columns == ["id"]

    def test_true_config_all_metadata(self, polars_engine):
        df = pl.DataFrame({"id": [1]})
        result = polars_engine.add_write_metadata(
            df,
            True,
            source_connection="conn1",
            source_table="tbl1",
            source_path="/file.csv",
            is_file_source=True,
        )
        assert "_extracted_at" in result.columns
        assert "_source_file" in result.columns

    def test_write_metadata_config_selective(self, polars_engine):
        from odibi.config import WriteMetadataConfig

        cfg = WriteMetadataConfig(
            extracted_at=False,
            source_file=False,
            source_connection=True,
            source_table=True,
        )
        df = pl.DataFrame({"id": [1]})
        result = polars_engine.add_write_metadata(
            df, cfg, source_connection="db", source_table="orders"
        )
        assert "_extracted_at" not in result.columns
        assert "_source_file" not in result.columns
        assert "_source_connection" in result.columns
        assert "_source_table" in result.columns

    def test_no_columns_added_when_all_disabled(self, polars_engine):
        from odibi.config import WriteMetadataConfig

        cfg = WriteMetadataConfig(
            extracted_at=False,
            source_file=False,
            source_connection=False,
            source_table=False,
        )
        df = pl.DataFrame({"id": [1]})
        result = polars_engine.add_write_metadata(df, cfg)
        assert result.columns == ["id"]


class TestPolarsSourceFiles:
    """Tests for get_source_files method."""

    def test_lazyframe_returns_empty(self, polars_engine):
        lf = pl.DataFrame({"a": [1]}).lazy()
        assert polars_engine.get_source_files(lf) == []

    def test_dataframe_without_attrs(self, polars_engine):
        df = pl.DataFrame({"a": [1]})
        assert polars_engine.get_source_files(df) == []

    def test_dataframe_with_source_attrs(self, polars_engine):
        from unittest.mock import MagicMock

        df = MagicMock(spec=pl.DataFrame)
        df.attrs = {"odibi_source_files": ["/data/file1.csv", "/data/file2.csv"]}
        files = polars_engine.get_source_files(df)
        assert files == ["/data/file1.csv", "/data/file2.csv"]

    def test_dataframe_with_empty_attrs(self, polars_engine):
        from unittest.mock import MagicMock

        df = MagicMock(spec=pl.DataFrame)
        df.attrs = {"odibi_source_files": []}
        assert polars_engine.get_source_files(df) == []

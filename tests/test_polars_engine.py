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

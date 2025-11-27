import polars as pl
import pytest

from odibi.context import PolarsContext
from odibi.engine.polars_engine import PolarsEngine


class MockConnection:
    def get_path(self, path):
        return path


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

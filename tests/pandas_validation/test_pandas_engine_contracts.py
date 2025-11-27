import pandas as pd
import pytest

from odibi.engine.pandas_engine import LazyDataset, PandasEngine

# pytestmark = pytest.mark.skip(reason="LazyDataset support temporarily disabled")


@pytest.fixture
def engine():
    return PandasEngine()


@pytest.fixture
def lazy_df(tmp_path):
    # Create a dummy CSV
    csv_path = tmp_path / "test.csv"
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    df.to_csv(csv_path, index=False)

    return LazyDataset(path=str(csv_path), format="csv", options={})


def test_anonymize_lazy(engine, lazy_df):
    """Test anonymize works with LazyDataset."""
    # This was fixed in v1, should pass
    res = engine.anonymize(lazy_df, columns=["col2"], method="redact")
    assert isinstance(res, pd.DataFrame)
    assert res["col2"].iloc[0] == "[REDACTED]"


def test_harmonize_schema_lazy(engine, lazy_df):
    """Test harmonize_schema works with LazyDataset."""
    from odibi.config import OnMissingColumns, OnNewColumns, SchemaMode, SchemaPolicyConfig

    policy = SchemaPolicyConfig(
        mode=SchemaMode.ENFORCE,
        on_missing_columns=OnMissingColumns.FAIL,  # or FAIL, doesn't matter
        on_new_columns=OnNewColumns.IGNORE,
    )
    target_schema = {"col1": "int64"}

    # This is expected to fail if materialize() is missing
    try:
        res = engine.harmonize_schema(lazy_df, target_schema, policy)
        assert isinstance(res, pd.DataFrame)
    except AttributeError as e:
        pytest.fail(f"harmonize_schema failed with LazyDataset: {e}")


def test_execute_operation_lazy(engine, lazy_df):
    """Test execute_operation works with LazyDataset."""
    # This is expected to fail if materialize() is missing
    try:
        res = engine.execute_operation("drop", {"columns": ["col2"]}, lazy_df)
        assert isinstance(res, pd.DataFrame)
        assert "col2" not in res.columns
    except Exception as e:
        pytest.fail(f"execute_operation failed with LazyDataset: {e}")


def test_validate_schema_lazy(engine, lazy_df):
    """Test validate_schema works with LazyDataset."""
    schema_rules = {"types": {"col1": "int64"}}
    try:
        res = engine.validate_schema(lazy_df, schema_rules)
        assert isinstance(res, list)
    except AttributeError as e:
        pytest.fail(f"validate_schema failed with LazyDataset: {e}")


def test_profile_nulls_lazy(engine, lazy_df):
    """Test profile_nulls works with LazyDataset."""
    try:
        res = engine.profile_nulls(lazy_df)
        assert isinstance(res, dict)
    except AttributeError as e:
        pytest.fail(f"profile_nulls failed with LazyDataset: {e}")


def test_duplicate_methods(engine):
    """Check for duplicate method definitions (static check logic)."""
    # This is hard to test at runtime because the last one wins.
    # But we can verify if count_nulls exists and works.
    res = engine.count_nulls(pd.DataFrame({"a": [1, None]}), ["a"])
    assert res["a"] == 1

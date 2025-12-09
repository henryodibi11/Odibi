import pandas as pd

from odibi.context import EngineContext, create_context
from odibi.enums import EngineType
from odibi.transformers.sql_core import (
    AddPrefixParams,
    AddSuffixParams,
    CoalesceColumnsParams,
    DropColumnsParams,
    NormalizeColumnNamesParams,
    RenameColumnsParams,
    ReplaceValuesParams,
    SelectColumnsParams,
    TrimWhitespaceParams,
    add_prefix,
    add_suffix,
    coalesce_columns,
    drop_columns,
    normalize_column_names,
    rename_columns,
    replace_values,
    select_columns,
    trim_whitespace,
)


# -------------------------------------------------------------------------
# Helper
# -------------------------------------------------------------------------


def create_pandas_context(df: pd.DataFrame) -> EngineContext:
    """Creates a Pandas EngineContext with DuckDB SQL executor."""
    ctx = create_context(engine="pandas")

    def mock_sql_executor(sql: str, context) -> pd.DataFrame:
        import duckdb

        for name in context.list_names():
            locals()[name] = context.get(name)

        return duckdb.query(sql).to_df()

    return EngineContext(ctx, df, EngineType.PANDAS, sql_executor=mock_sql_executor)


# -------------------------------------------------------------------------
# Select Columns Tests
# -------------------------------------------------------------------------


def test_select_columns():
    """Test selecting specific columns."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "city": ["NYC", "LA", "SF"],
        }
    )

    ctx = create_pandas_context(df)
    params = SelectColumnsParams(columns=["id", "name"])

    res_ctx = select_columns(ctx, params)
    res_df = res_ctx.df

    assert list(res_df.columns) == ["id", "name"]
    assert len(res_df) == 3


# -------------------------------------------------------------------------
# Drop Columns Tests
# -------------------------------------------------------------------------


def test_drop_columns():
    """Test dropping specific columns."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "_internal": ["x", "y", "z"],
            "_temp": [1, 2, 3],
        }
    )

    ctx = create_pandas_context(df)
    params = DropColumnsParams(columns=["_internal", "_temp"])

    res_ctx = drop_columns(ctx, params)
    res_df = res_ctx.df

    assert list(res_df.columns) == ["id", "name"]
    assert len(res_df) == 3


# -------------------------------------------------------------------------
# Rename Columns Tests
# -------------------------------------------------------------------------


def test_rename_columns():
    """Test renaming columns via mapping."""
    df = pd.DataFrame(
        {
            "customer_id": [1, 2, 3],
            "order_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "total_amount": [100, 200, 300],
        }
    )

    ctx = create_pandas_context(df)
    params = RenameColumnsParams(mapping={"customer_id": "cust_id", "total_amount": "amount"})

    res_ctx = rename_columns(ctx, params)
    res_df = res_ctx.df

    assert list(res_df.columns) == ["cust_id", "order_date", "amount"]
    assert res_df["cust_id"].tolist() == [1, 2, 3]


# -------------------------------------------------------------------------
# Add Prefix Tests
# -------------------------------------------------------------------------


def test_add_prefix_all_columns():
    """Test adding prefix to all columns."""
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "age": [25, 30]})

    ctx = create_pandas_context(df)
    params = AddPrefixParams(prefix="src_")

    res_ctx = add_prefix(ctx, params)
    res_df = res_ctx.df

    assert list(res_df.columns) == ["src_id", "src_name", "src_age"]


def test_add_prefix_specific_columns():
    """Test adding prefix to specific columns only."""
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "age": [25, 30]})

    ctx = create_pandas_context(df)
    params = AddPrefixParams(prefix="raw_", columns=["name", "age"])

    res_ctx = add_prefix(ctx, params)
    res_df = res_ctx.df

    assert list(res_df.columns) == ["id", "raw_name", "raw_age"]


def test_add_prefix_with_exclude():
    """Test adding prefix with exclusions."""
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "age": [25, 30]})

    ctx = create_pandas_context(df)
    params = AddPrefixParams(prefix="src_", exclude=["id"])

    res_ctx = add_prefix(ctx, params)
    res_df = res_ctx.df

    assert list(res_df.columns) == ["id", "src_name", "src_age"]


# -------------------------------------------------------------------------
# Add Suffix Tests
# -------------------------------------------------------------------------


def test_add_suffix_all_columns():
    """Test adding suffix to all columns."""
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "age": [25, 30]})

    ctx = create_pandas_context(df)
    params = AddSuffixParams(suffix="_raw")

    res_ctx = add_suffix(ctx, params)
    res_df = res_ctx.df

    assert list(res_df.columns) == ["id_raw", "name_raw", "age_raw"]


def test_add_suffix_specific_columns():
    """Test adding suffix to specific columns only."""
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "age": [25, 30]})

    ctx = create_pandas_context(df)
    params = AddSuffixParams(suffix="_v2", columns=["name", "age"])

    res_ctx = add_suffix(ctx, params)
    res_df = res_ctx.df

    assert list(res_df.columns) == ["id", "name_v2", "age_v2"]


# -------------------------------------------------------------------------
# Normalize Column Names Tests
# -------------------------------------------------------------------------


def test_normalize_column_names_snake_case():
    """Test converting column names to snake_case."""
    df = pd.DataFrame(
        {
            "Customer ID": [1, 2],
            "First Name": ["Alice", "Bob"],
            "last-name": ["Smith", "Jones"],
        }
    )

    ctx = create_pandas_context(df)
    params = NormalizeColumnNamesParams(style="snake_case", lowercase=True)

    res_ctx = normalize_column_names(ctx, params)
    res_df = res_ctx.df

    assert "customer_id" in res_df.columns
    assert "first_name" in res_df.columns
    assert "last_name" in res_df.columns


def test_normalize_column_names_removes_special():
    """Test removing special characters from column names."""
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4], "col3": [5, 6]})

    ctx = create_pandas_context(df)
    params = NormalizeColumnNamesParams(remove_special=True)

    res_ctx = normalize_column_names(ctx, params)
    res_df = res_ctx.df

    assert "col1" in res_df.columns
    assert "col2" in res_df.columns
    assert "col3" in res_df.columns


# -------------------------------------------------------------------------
# Coalesce Columns Tests
# -------------------------------------------------------------------------


def test_coalesce_columns():
    """Test coalescing columns (first non-null)."""
    df = pd.DataFrame(
        {
            "mobile": [None, "555-1234", None],
            "work": ["555-9999", None, None],
            "home": ["555-0000", "555-0001", "555-0002"],
        }
    )

    ctx = create_pandas_context(df)
    params = CoalesceColumnsParams(columns=["mobile", "work", "home"], output_col="primary_phone")

    res_ctx = coalesce_columns(ctx, params)
    res_df = res_ctx.df

    assert "primary_phone" in res_df.columns
    assert res_df["primary_phone"].tolist() == ["555-9999", "555-1234", "555-0002"]


def test_coalesce_columns_drop_source():
    """Test coalescing columns with drop_source=True."""
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "mobile": [None, "555-1234"],
            "work": ["555-9999", None],
        }
    )

    ctx = create_pandas_context(df)
    params = CoalesceColumnsParams(columns=["mobile", "work"], output_col="phone", drop_source=True)

    res_ctx = coalesce_columns(ctx, params)
    res_df = res_ctx.df

    assert "phone" in res_df.columns
    assert "mobile" not in res_df.columns
    assert "work" not in res_df.columns
    assert "id" in res_df.columns


# -------------------------------------------------------------------------
# Replace Values Tests
# -------------------------------------------------------------------------


def test_replace_values():
    """Test replacing values in columns."""
    df = pd.DataFrame(
        {"status": ["Active", "N/A", "Unknown", "Active"], "code": ["US", "UK", "US", "CA"]}
    )

    ctx = create_pandas_context(df)
    params = ReplaceValuesParams(columns=["status"], mapping={"N/A": None, "Unknown": None})

    res_ctx = replace_values(ctx, params)
    res_df = res_ctx.df

    assert res_df["status"].tolist() == ["Active", None, None, "Active"]


def test_replace_values_code_mapping():
    """Test replacing code values."""
    df = pd.DataFrame({"country": ["US", "UK", "CA", "US"]})

    ctx = create_pandas_context(df)
    params = ReplaceValuesParams(
        columns=["country"], mapping={"US": "USA", "UK": "GBR", "CA": "CAN"}
    )

    res_ctx = replace_values(ctx, params)
    res_df = res_ctx.df

    assert res_df["country"].tolist() == ["USA", "GBR", "CAN", "USA"]


# -------------------------------------------------------------------------
# Trim Whitespace Tests
# -------------------------------------------------------------------------


def test_trim_whitespace():
    """Test trimming whitespace from columns."""
    df = pd.DataFrame({"name": ["  Alice  ", " Bob", "Charlie "], "city": [" NYC ", "LA", " SF"]})

    ctx = create_pandas_context(df)
    params = TrimWhitespaceParams(columns=["name", "city"])

    res_ctx = trim_whitespace(ctx, params)
    res_df = res_ctx.df

    assert res_df["name"].tolist() == ["Alice", "Bob", "Charlie"]
    assert res_df["city"].tolist() == ["NYC", "LA", "SF"]


def test_trim_whitespace_all_columns():
    """Test trimming whitespace from all columns when none specified."""
    df = pd.DataFrame({"a": ["  x  ", " y"], "b": ["z ", "  w"]})

    ctx = create_pandas_context(df)
    params = TrimWhitespaceParams()

    res_ctx = trim_whitespace(ctx, params)
    res_df = res_ctx.df

    assert res_df["a"].tolist() == ["x", "y"]
    assert res_df["b"].tolist() == ["z", "w"]

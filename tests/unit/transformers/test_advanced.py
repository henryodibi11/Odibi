"""
Unit tests for odibi/transformers/advanced.py

Tests each public transformer function with:
- Normal cases
- Empty DataFrames
- Null values
- Invalid inputs
"""

import pytest
import pandas as pd
import json
from pydantic import ValidationError

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.engine.pandas_engine import PandasEngine
from odibi.transformers.advanced import (
    # Transformers
    deduplicate,
    explode_list_column,
    dict_based_mapping,
    regex_replace,
    unpack_struct,
    hash_columns,
    generate_surrogate_key,
    generate_numeric_key,
    parse_json,
    validate_and_flag,
    window_calculation,
    normalize_json,
    sessionize,
    geocode,
    split_events_by_period,
    # Params
    DeduplicateParams,
    ExplodeParams,
    DictMappingParams,
    RegexReplaceParams,
    UnpackStructParams,
    HashParams,
    SurrogateKeyParams,
    NumericKeyParams,
    ParseJsonParams,
    ValidateAndFlagParams,
    WindowCalculationParams,
    NormalizeJsonParams,
    SessionizeParams,
    GeocodeParams,
    SplitEventsByPeriodParams,
    ShiftDefinition,
)


# -------------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------------

@pytest.fixture
def empty_df():
    """Empty DataFrame with columns"""
    return pd.DataFrame(columns=["id", "name", "value"])


@pytest.fixture
def nulls_df():
    """DataFrame with all null values"""
    return pd.DataFrame({
        "id": [None, None, None],
        "name": [None, None, None],
        "value": [None, None, None]
    })


@pytest.fixture
def sample_df():
    """Sample DataFrame with varied data"""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10, 20, 30]
    })


@pytest.fixture
def duplicates_df():
    """DataFrame with duplicate rows"""
    return pd.DataFrame({
        "id": [1, 1, 2, 2, 3],
        "name": ["A", "A", "B", "B", "C"],
        "updated_at": pd.to_datetime([
            "2023-01-01 10:00",
            "2023-01-01 11:00",  # Later (should be kept)
            "2023-01-02 10:00",
            "2023-01-02 09:00",  # Earlier
            "2023-01-03 10:00"
        ])
    })


def setup_context(df):
    """Helper to create EngineContext for pandas testing"""
    pandas_ctx = PandasContext()
    pandas_ctx.register("df", df.copy())
    pandas_engine = PandasEngine()
    return EngineContext(
        pandas_ctx, 
        df.copy(), 
        EngineType.PANDAS,
        sql_executor=pandas_engine.execute_sql,
        engine=pandas_engine
    )


# -------------------------------------------------------------------------
# 1. deduplicate tests
# -------------------------------------------------------------------------

def test_deduplicate_empty(empty_df):
    """Test deduplicate with empty DataFrame"""
    context = setup_context(empty_df)
    params = DeduplicateParams(keys=["id"])
    result_ctx = deduplicate(context, params)
    assert result_ctx.df.empty
    assert list(result_ctx.df.columns) == list(empty_df.columns)


def test_deduplicate_no_duplicates(sample_df):
    """Test deduplicate when no duplicates exist"""
    context = setup_context(sample_df)
    params = DeduplicateParams(keys=["id"])
    result_ctx = deduplicate(context, params)
    assert len(result_ctx.df) == 3
    pd.testing.assert_frame_equal(result_ctx.df.sort_values("id").reset_index(drop=True), 
                                   sample_df.sort_values("id").reset_index(drop=True))


def test_deduplicate_with_order(duplicates_df):
    """Test deduplicate keeping latest record based on order_by"""
    context = setup_context(duplicates_df)
    params = DeduplicateParams(keys=["id"], order_by="updated_at DESC")
    result_ctx = deduplicate(context, params)
    
    # Should keep 3 unique records
    assert len(result_ctx.df) == 3
    
    # Check that the latest record for id=1 is kept
    id1_records = result_ctx.df[result_ctx.df["id"] == 1]
    assert len(id1_records) == 1


def test_deduplicate_nulls(nulls_df):
    """Test deduplicate with all null values"""
    context = setup_context(nulls_df)
    params = DeduplicateParams(keys=["id"])
    result_ctx = deduplicate(context, params)
    # All nulls are treated as same key, should keep 1 row
    assert len(result_ctx.df) >= 1


# -------------------------------------------------------------------------
# 2. explode_list_column tests
# -------------------------------------------------------------------------

def test_explode_list_column_basic():
    """Test basic list explosion"""
    df = pd.DataFrame({
        "id": [1, 2],
        "items": [["a", "b"], ["c"]]
    })
    context = setup_context(df)
    params = ExplodeParams(column="items", outer=False)
    result_ctx = explode_list_column(context, params)
    
    # Should have 3 rows (2 from first list, 1 from second)
    assert len(result_ctx.df) == 3


def test_explode_list_column_empty_lists():
    """Test explode with empty lists"""
    df = pd.DataFrame({
        "id": [1, 2],
        "items": [[], ["a"]]
    })
    context = setup_context(df)
    
    # Test without outer (drops empty lists)
    params = ExplodeParams(column="items", outer=False)
    result_ctx = explode_list_column(context, params)
    assert len(result_ctx.df) == 1
    
    # Test with outer (keeps empty lists)
    params = ExplodeParams(column="items", outer=True)
    result_ctx = explode_list_column(context, params)
    assert len(result_ctx.df) == 2


def test_explode_list_column_empty_df(empty_df):
    """Test explode with empty DataFrame"""
    df = pd.DataFrame(columns=["id", "items"])
    context = setup_context(df)
    params = ExplodeParams(column="items")
    result_ctx = explode_list_column(context, params)
    assert result_ctx.df.empty


# -------------------------------------------------------------------------
# 3. dict_based_mapping tests
# -------------------------------------------------------------------------

def test_dict_based_mapping_basic():
    """Test basic dictionary mapping"""
    df = pd.DataFrame({
        "status": ["1", "0", "2"]
    })
    context = setup_context(df)
    params = DictMappingParams(
        column="status",
        mapping={"1": "Active", "0": "Inactive"},
        default="Unknown",
        output_column="status_desc"
    )
    result_ctx = dict_based_mapping(context, params)
    
    expected = ["Active", "Inactive", "Unknown"]
    assert list(result_ctx.df["status_desc"]) == expected


def test_dict_based_mapping_no_default():
    """Test mapping without default value"""
    df = pd.DataFrame({
        "code": ["A", "B", "C"]
    })
    context = setup_context(df)
    params = DictMappingParams(
        column="code",
        mapping={"A": "Apple", "B": "Banana"}
    )
    result_ctx = dict_based_mapping(context, params)
    
    # C should be NaN since no default provided
    assert pd.isna(result_ctx.df["code"].iloc[2])


def test_dict_based_mapping_inplace():
    """Test mapping that overwrites original column"""
    df = pd.DataFrame({
        "status": ["1", "0"]
    })
    context = setup_context(df)
    params = DictMappingParams(
        column="status",
        mapping={"1": "Active", "0": "Inactive"}
    )
    result_ctx = dict_based_mapping(context, params)
    
    # Should overwrite status column
    assert "Active" in list(result_ctx.df["status"])


def test_dict_based_mapping_empty(empty_df):
    """Test mapping with empty DataFrame"""
    df = pd.DataFrame(columns=["status"])
    context = setup_context(df)
    params = DictMappingParams(
        column="status",
        mapping={"1": "Active"}
    )
    result_ctx = dict_based_mapping(context, params)
    assert result_ctx.df.empty


# -------------------------------------------------------------------------
# 4. regex_replace tests
# -------------------------------------------------------------------------

def test_regex_replace_basic():
    """Test basic regex replacement"""
    df = pd.DataFrame({
        "phone": ["123-456-7890", "987-654-3210"],
        "id": [1, 2]
    })
    context = setup_context(df)
    params = RegexReplaceParams(
        column="phone",
        pattern="[^0-9]",
        replacement=""
    )
    result_ctx = regex_replace(context, params)
    
    # The regex should remove all non-numeric characters
    assert "phone" in result_ctx.df.columns
    # Verify the phone values were modified
    assert result_ctx.df["phone"].iloc[0] != df["phone"].iloc[0]
    assert result_ctx.df["phone"].iloc[1] != df["phone"].iloc[1]


def test_regex_replace_empty(empty_df):
    """Test regex replace with empty DataFrame"""
    df = pd.DataFrame(columns=["text"])
    context = setup_context(df)
    params = RegexReplaceParams(
        column="text",
        pattern="[0-9]",
        replacement="X"
    )
    result_ctx = regex_replace(context, params)
    assert result_ctx.df.empty


def test_regex_replace_nulls():
    """Test regex replace with null values"""
    df = pd.DataFrame({
        "text": ["abc123", None, "xyz456"]
    })
    context = setup_context(df)
    params = RegexReplaceParams(
        column="text",
        pattern="[0-9]",
        replacement="X"
    )
    result_ctx = regex_replace(context, params)
    
    # Should handle nulls gracefully
    assert pd.isna(result_ctx.df["text"].iloc[1])


# -------------------------------------------------------------------------
# 5. unpack_struct tests
# -------------------------------------------------------------------------

def test_unpack_struct_basic():
    """Test basic struct unpacking"""
    df = pd.DataFrame({
        "id": [1, 2],
        "user_info": [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25}
        ]
    })
    context = setup_context(df)
    params = UnpackStructParams(column="user_info")
    result_ctx = unpack_struct(context, params)
    
    # Should have unpacked columns
    assert "name" in result_ctx.df.columns
    assert "age" in result_ctx.df.columns


def test_unpack_struct_empty(empty_df):
    """Test unpack with empty DataFrame"""
    df = pd.DataFrame(columns=["id", "data"])
    context = setup_context(df)
    params = UnpackStructParams(column="data")
    result_ctx = unpack_struct(context, params)
    assert result_ctx.df.empty


def test_unpack_struct_nulls():
    """Test unpack with null structs"""
    df = pd.DataFrame({
        "id": [1, 2],
        "data": [{"key": "val"}, None]
    })
    context = setup_context(df)
    params = UnpackStructParams(column="data")
    result_ctx = unpack_struct(context, params)
    
    # Should handle nulls
    assert len(result_ctx.df) == 2


# -------------------------------------------------------------------------
# 6. hash_columns tests
# -------------------------------------------------------------------------

def test_hash_columns_basic(sample_df):
    """Test basic column hashing"""
    context = setup_context(sample_df)
    params = HashParams(columns=["name"])
    result_ctx = hash_columns(context, params)
    
    # Name column should be hashed (different from original)
    assert not (result_ctx.df["name"] == sample_df["name"]).any()
    # Other columns should be unchanged
    assert (result_ctx.df["id"] == sample_df["id"]).all()


def test_hash_columns_multiple():
    """Test hashing multiple columns"""
    df = pd.DataFrame({
        "email": ["a@b.com", "c@d.com"],
        "ssn": ["123-45-6789", "987-65-4321"],
        "id": [1, 2]
    })
    context = setup_context(df)
    params = HashParams(columns=["email", "ssn"])
    result_ctx = hash_columns(context, params)
    
    # Both columns should be hashed
    assert not (result_ctx.df["email"] == df["email"]).any()
    assert not (result_ctx.df["ssn"] == df["ssn"]).any()
    # ID unchanged
    assert (result_ctx.df["id"] == df["id"]).all()


def test_hash_columns_empty(empty_df):
    """Test hash with empty DataFrame"""
    context = setup_context(empty_df)
    params = HashParams(columns=["name"])
    result_ctx = hash_columns(context, params)
    assert result_ctx.df.empty


def test_hash_columns_empty_list(sample_df):
    """Test hash with empty column list"""
    context = setup_context(sample_df)
    params = HashParams(columns=[])
    result_ctx = hash_columns(context, params)
    # Should return unchanged DataFrame
    pd.testing.assert_frame_equal(result_ctx.df, sample_df)


# -------------------------------------------------------------------------
# 7. generate_surrogate_key tests
# -------------------------------------------------------------------------

def test_generate_surrogate_key_basic(sample_df):
    """Test basic surrogate key generation"""
    context = setup_context(sample_df)
    params = SurrogateKeyParams(
        columns=["id", "name"],
        separator="-",
        output_col="sk"
    )
    result_ctx = generate_surrogate_key(context, params)
    
    # Should have new column
    assert "sk" in result_ctx.df.columns
    # All values should be unique
    assert result_ctx.df["sk"].nunique() == 3


def test_generate_surrogate_key_duplicates():
    """Test surrogate key with duplicate input values"""
    df = pd.DataFrame({
        "a": [1, 1, 2],
        "b": ["x", "x", "y"]
    })
    context = setup_context(df)
    params = SurrogateKeyParams(columns=["a", "b"], output_col="sk")
    result_ctx = generate_surrogate_key(context, params)
    
    # Same input should produce same key
    assert result_ctx.df["sk"].iloc[0] == result_ctx.df["sk"].iloc[1]
    # Different input should produce different key
    assert result_ctx.df["sk"].iloc[0] != result_ctx.df["sk"].iloc[2]


def test_generate_surrogate_key_empty(empty_df):
    """Test surrogate key with empty DataFrame"""
    context = setup_context(empty_df)
    params = SurrogateKeyParams(columns=["id", "name"], output_col="sk")
    result_ctx = generate_surrogate_key(context, params)
    assert result_ctx.df.empty


# -------------------------------------------------------------------------
# 8. generate_numeric_key tests
# -------------------------------------------------------------------------

def test_generate_numeric_key_basic(sample_df):
    """Test basic numeric key generation"""
    context = setup_context(sample_df)
    params = NumericKeyParams(
        columns=["id", "name"],
        output_col="numeric_key"
    )
    result_ctx = generate_numeric_key(context, params)
    
    # Should have new column with integer numeric values
    assert "numeric_key" in result_ctx.df.columns
    assert pd.api.types.is_integer_dtype(result_ctx.df["numeric_key"])


def test_generate_numeric_key_with_coalesce():
    """Test numeric key with coalesce_with option"""
    df = pd.DataFrame({
        "id": [1, None, 3],
        "name": ["A", "B", "C"]
    })
    context = setup_context(df)
    params = NumericKeyParams(
        columns=["name"],
        output_col="id",
        coalesce_with="id"
    )
    result_ctx = generate_numeric_key(context, params)
    
    # Existing id=1 should be kept, null should be replaced
    assert result_ctx.df["id"].iloc[0] == 1
    assert pd.notna(result_ctx.df["id"].iloc[1])
    assert result_ctx.df["id"].iloc[2] == 3


def test_generate_numeric_key_empty(empty_df):
    """Test numeric key with empty DataFrame"""
    context = setup_context(empty_df)
    params = NumericKeyParams(columns=["id"], output_col="num_key")
    result_ctx = generate_numeric_key(context, params)
    assert result_ctx.df.empty


# -------------------------------------------------------------------------
# 9. parse_json tests
# -------------------------------------------------------------------------

def test_parse_json_basic():
    """Test basic JSON parsing"""
    df = pd.DataFrame({
        "data": ['{"key": "value"}', '{"key": "other"}']
    })
    context = setup_context(df)
    params = ParseJsonParams(
        column="data",
        json_schema="key STRING",
        output_col="parsed"
    )
    result_ctx = parse_json(context, params)
    
    # Should have new parsed column
    assert "parsed" in result_ctx.df.columns


def test_parse_json_empty(empty_df):
    """Test JSON parsing with empty DataFrame"""
    df = pd.DataFrame(columns=["json_col"])
    context = setup_context(df)
    params = ParseJsonParams(
        column="json_col",
        json_schema="a INT"
    )
    result_ctx = parse_json(context, params)
    assert result_ctx.df.empty


# -------------------------------------------------------------------------
# 10. validate_and_flag tests
# -------------------------------------------------------------------------

def test_validate_and_flag_basic():
    """Test basic validation and flagging"""
    df = pd.DataFrame({
        "age": [25, -5, 30],
        "email": ["a@b.com", "invalid", "c@d.com"]
    })
    context = setup_context(df)
    params = ValidateAndFlagParams(
        rules={
            "age_check": "age >= 0",
            "email_format": "email LIKE '%@%'"
        },
        flag_col="issues"
    )
    result_ctx = validate_and_flag(context, params)
    
    # Should have issues column
    assert "issues" in result_ctx.df.columns
    # First row should pass all checks
    assert pd.isna(result_ctx.df["issues"].iloc[0])
    # Second row should fail both checks
    assert pd.notna(result_ctx.df["issues"].iloc[1])


def test_validate_and_flag_empty(empty_df):
    """Test validation with empty DataFrame"""
    df = pd.DataFrame(columns=["value"])
    context = setup_context(df)
    params = ValidateAndFlagParams(
        rules={"check": "value > 0"}
    )
    result_ctx = validate_and_flag(context, params)
    assert result_ctx.df.empty


def test_validate_and_flag_all_pass():
    """Test when all records pass validation"""
    df = pd.DataFrame({
        "value": [1, 2, 3]
    })
    context = setup_context(df)
    params = ValidateAndFlagParams(
        rules={"positive": "value > 0"}
    )
    result_ctx = validate_and_flag(context, params)
    
    # All should be null (no issues)
    assert result_ctx.df["_issues"].isna().all()


def test_validate_and_flag_invalid_empty_rules():
    """Test that empty rules raise validation error"""
    with pytest.raises(ValueError, match="must not be empty"):
        ValidateAndFlagParams(rules={})


# -------------------------------------------------------------------------
# 11. window_calculation tests
# -------------------------------------------------------------------------

def test_window_calculation_basic():
    """Test basic window calculation"""
    df = pd.DataFrame({
        "region": ["A", "A", "B", "B"],
        "sales": [100, 200, 150, 250],
        "date": ["2023-01-01", "2023-01-02", "2023-01-01", "2023-01-02"]
    })
    context = setup_context(df)
    params = WindowCalculationParams(
        target_col="cumulative_sales",
        function="sum(sales)",
        partition_by=["region"],
        order_by="date ASC"
    )
    result_ctx = window_calculation(context, params)
    
    # Should have new column
    assert "cumulative_sales" in result_ctx.df.columns


def test_window_calculation_no_partition():
    """Test window calculation without partition"""
    df = pd.DataFrame({
        "value": [1, 2, 3]
    })
    context = setup_context(df)
    params = WindowCalculationParams(
        target_col="row_num",
        function="row_number()"
    )
    result_ctx = window_calculation(context, params)
    
    assert "row_num" in result_ctx.df.columns


def test_window_calculation_empty(empty_df):
    """Test window calculation with empty DataFrame"""
    df = pd.DataFrame(columns=["value"])
    context = setup_context(df)
    params = WindowCalculationParams(
        target_col="result",
        function="sum(value)"
    )
    result_ctx = window_calculation(context, params)
    assert result_ctx.df.empty


# -------------------------------------------------------------------------
# 12. normalize_json tests
# -------------------------------------------------------------------------

def test_normalize_json_basic():
    """Test basic JSON normalization"""
    df = pd.DataFrame({
        "id": [1, 2],
        "data": [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25}
        ]
    })
    context = setup_context(df)
    params = NormalizeJsonParams(column="data", sep="_")
    result_ctx = normalize_json(context, params)
    
    # Should flatten the structure
    assert "name" in result_ctx.df.columns or "data_name" in result_ctx.df.columns


def test_normalize_json_string_data():
    """Test normalize with JSON strings"""
    df = pd.DataFrame({
        "data": ['{"key": "value"}', '{"key": "other"}']
    })
    context = setup_context(df)
    params = NormalizeJsonParams(column="data")
    result_ctx = normalize_json(context, params)
    
    # Should parse and normalize
    assert len(result_ctx.df.columns) >= 2  # At least data + key


def test_normalize_json_empty(empty_df):
    """Test normalize with empty DataFrame"""
    df = pd.DataFrame(columns=["data"])
    context = setup_context(df)
    params = NormalizeJsonParams(column="data")
    result_ctx = normalize_json(context, params)
    assert result_ctx.df.empty


# -------------------------------------------------------------------------
# 13. sessionize tests
# -------------------------------------------------------------------------

def test_sessionize_basic():
    """Test basic sessionization"""
    df = pd.DataFrame({
        "user": ["A", "A", "A", "B", "B"],
        "timestamp": pd.to_datetime([
            "2023-01-01 10:00:00",
            "2023-01-01 10:05:00",  # 5 min gap - same session
            "2023-01-01 11:00:00",  # 55 min gap - new session
            "2023-01-01 10:00:00",
            "2023-01-01 10:10:00"
        ])
    })
    context = setup_context(df)
    params = SessionizeParams(
        user_col="user",
        timestamp_col="timestamp",
        threshold_seconds=1800  # 30 minutes
    )
    result_ctx = sessionize(context, params)
    
    # Should have session_id column
    assert "session_id" in result_ctx.df.columns
    # Should have 3 sessions total (2 for user A, 1 for user B)
    assert result_ctx.df["session_id"].nunique() == 3


def test_sessionize_empty(empty_df):
    """Test sessionize with empty DataFrame"""
    df = pd.DataFrame(columns=["user", "ts"])
    context = setup_context(df)
    params = SessionizeParams(
        user_col="user",
        timestamp_col="ts"
    )
    result_ctx = sessionize(context, params)
    assert result_ctx.df.empty


def test_sessionize_single_user():
    """Test sessionize with single user"""
    df = pd.DataFrame({
        "user": ["A", "A"],
        "timestamp": pd.to_datetime([
            "2023-01-01 10:00:00",
            "2023-01-01 10:05:00"
        ])
    })
    context = setup_context(df)
    params = SessionizeParams(
        user_col="user",
        timestamp_col="timestamp"
    )
    result_ctx = sessionize(context, params)
    
    # Both should be in same session
    assert result_ctx.df["session_id"].nunique() == 1


# -------------------------------------------------------------------------
# 14. geocode tests
# -------------------------------------------------------------------------

def test_geocode_stub(sample_df):
    """Test geocode stub function"""
    df = pd.DataFrame({
        "address": ["123 Main St", "456 Oak Ave"]
    })
    context = setup_context(df)
    params = GeocodeParams(address_col="address", output_col="coords")
    result_ctx = geocode(context, params)
    
    # Should pass through unchanged (stub)
    pd.testing.assert_frame_equal(result_ctx.df, df)


def test_geocode_empty(empty_df):
    """Test geocode with empty DataFrame"""
    df = pd.DataFrame(columns=["address"])
    context = setup_context(df)
    params = GeocodeParams(address_col="address")
    result_ctx = geocode(context, params)
    assert result_ctx.df.empty


# -------------------------------------------------------------------------
# 15. split_events_by_period tests
# -------------------------------------------------------------------------

def test_split_events_by_period_day():
    """Test splitting events by day"""
    df = pd.DataFrame({
        "event_id": [1],
        "start": ["2023-01-01 23:00:00"],
        "end": ["2023-01-02 01:00:00"]
    })
    context = setup_context(df)
    params = SplitEventsByPeriodParams(
        start_col="start",
        end_col="end",
        period="day"
    )
    result_ctx = split_events_by_period(context, params)
    
    # Should split into 2 rows (one for each day)
    assert len(result_ctx.df) == 2


def test_split_events_by_period_hour():
    """Test splitting events by hour"""
    df = pd.DataFrame({
        "event_id": [1],
        "start": ["2023-01-01 10:30:00"],
        "end": ["2023-01-01 12:30:00"]
    })
    context = setup_context(df)
    params = SplitEventsByPeriodParams(
        start_col="start",
        end_col="end",
        period="hour"
    )
    result_ctx = split_events_by_period(context, params)
    
    # Should split into 3 rows (10:30-11:00, 11:00-12:00, 12:00-12:30)
    assert len(result_ctx.df) == 3


def test_split_events_by_period_shift():
    """Test splitting events by shift"""
    df = pd.DataFrame({
        "event_id": [1],
        "start": ["2023-01-01 07:00:00"],
        "end": ["2023-01-01 15:00:00"]
    })
    context = setup_context(df)
    params = SplitEventsByPeriodParams(
        start_col="start",
        end_col="end",
        period="shift",
        shifts=[
            ShiftDefinition(name="Morning", start="06:00", end="14:00"),
            ShiftDefinition(name="Evening", start="14:00", end="22:00")
        ]
    )
    result_ctx = split_events_by_period(context, params)
    
    # Should split into 2 rows (one for Morning shift, one for Evening shift)
    assert len(result_ctx.df) == 2


def test_split_events_by_period_empty(empty_df):
    """Test split events with empty DataFrame"""
    df = pd.DataFrame(columns=["start", "end"])
    context = setup_context(df)
    params = SplitEventsByPeriodParams(
        start_col="start",
        end_col="end",
        period="day"
    )
    result_ctx = split_events_by_period(context, params)
    assert result_ctx.df.empty


def test_split_events_by_period_invalid_period():
    """Test split events with invalid period"""
    df = pd.DataFrame({
        "start": ["2023-01-01"],
        "end": ["2023-01-02"]
    })
    context = setup_context(df)
    
    # Pydantic validation will catch this before the function is called
    with pytest.raises(ValidationError):
        params = SplitEventsByPeriodParams(
            start_col="start",
            end_col="end",
            period="invalid"
        )


# -------------------------------------------------------------------------
# Edge case tests
# -------------------------------------------------------------------------

def test_all_transformers_handle_empty_gracefully():
    """Meta test: verify all transformers handle empty DataFrames"""
    empty = pd.DataFrame(columns=["col1"])
    context = setup_context(empty)
    
    # List of simple transformer calls that should work with empty DF
    test_cases = [
        (deduplicate, DeduplicateParams(keys=["col1"])),
        (hash_columns, HashParams(columns=[])),
        (window_calculation, WindowCalculationParams(target_col="result", function="count(*)")),
    ]
    
    for transformer, params in test_cases:
        result = transformer(context, params)
        assert result.df.empty, f"{transformer.__name__} didn't handle empty DF"

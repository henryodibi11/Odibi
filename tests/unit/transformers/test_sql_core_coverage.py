import pytest
import pandas as pd

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.engine.pandas_engine import PandasEngine
from odibi.transformers.sql_core import (
    ExtractDateParams,
    extract_date_parts,
    NormalizeSchemaParams,
    normalize_schema,
    SortParams,
    sort,
    LimitParams,
    limit,
    SampleParams,
    sample,
    DistinctParams,
    distinct,
    FillNullsParams,
    fill_nulls,
    SplitPartParams,
    split_part,
    DateAddParams,
    date_add,
    DateTruncParams,
    date_trunc,
    DateDiffParams,
    date_diff,
    CaseWhenCase,
    CaseWhenParams,
    case_when,
    ConvertTimezoneParams,
    convert_timezone,
    ConcatColumnsParams,
    concat_columns,
    NormalizeColumnNamesParams,
    normalize_column_names,
    CoalesceColumnsParams,
    coalesce_columns,
)


# -------------------------------------------------------------------------
# Helper
# -------------------------------------------------------------------------


def setup_context(df):
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
# Test extract_date_parts
# -------------------------------------------------------------------------


class TestExtractDateParts:
    @pytest.fixture
    def date_df(self):
        return pd.DataFrame(
            {"created_at": pd.to_datetime(["2024-03-15 10:30:00", "2023-12-01 08:00:00"])}
        )

    def test_all_parts_default_prefix(self, date_df):
        ctx = setup_context(date_df)
        params = ExtractDateParams(source_col="created_at", parts=["year", "month", "day", "hour"])
        result = extract_date_parts(ctx, params).df

        assert result.iloc[0]["created_at_year"] == 2024
        assert result.iloc[0]["created_at_month"] == 3
        assert result.iloc[0]["created_at_day"] == 15
        assert result.iloc[0]["created_at_hour"] == 10

    def test_custom_prefix(self, date_df):
        ctx = setup_context(date_df)
        params = ExtractDateParams(
            source_col="created_at", prefix="created", parts=["year", "month"]
        )
        result = extract_date_parts(ctx, params).df

        assert "created_year" in result.columns
        assert "created_month" in result.columns
        assert result.iloc[1]["created_year"] == 2023
        assert result.iloc[1]["created_month"] == 12


# -------------------------------------------------------------------------
# Test normalize_schema
# -------------------------------------------------------------------------


class TestNormalizeSchema:
    @pytest.fixture
    def schema_df(self):
        return pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})

    def test_rename_only(self, schema_df):
        ctx = setup_context(schema_df)
        params = NormalizeSchemaParams(rename={"a": "alpha"})
        result = normalize_schema(ctx, params).df

        assert "alpha" in result.columns
        assert "a" not in result.columns
        assert list(result["alpha"]) == [1, 2]

    def test_drop_only(self, schema_df):
        ctx = setup_context(schema_df)
        params = NormalizeSchemaParams(drop=["c"])
        result = normalize_schema(ctx, params).df

        assert "c" not in result.columns
        assert list(result.columns) == ["a", "b"]

    def test_select_order_only(self, schema_df):
        ctx = setup_context(schema_df)
        params = NormalizeSchemaParams(select_order=["c", "a", "b"])
        result = normalize_schema(ctx, params).df

        assert list(result.columns) == ["c", "a", "b"]

    def test_rename_and_select_order(self, schema_df):
        ctx = setup_context(schema_df)
        params = NormalizeSchemaParams(rename={"a": "alpha"}, select_order=["c", "alpha", "b"])
        result = normalize_schema(ctx, params).df

        assert list(result.columns) == ["c", "alpha", "b"]
        assert list(result["alpha"]) == [1, 2]

    def test_combined_rename_drop_order(self, schema_df):
        ctx = setup_context(schema_df)
        params = NormalizeSchemaParams(
            rename={"a": "alpha"}, drop=["c"], select_order=["b", "alpha"]
        )
        result = normalize_schema(ctx, params).df

        assert list(result.columns) == ["b", "alpha"]


# -------------------------------------------------------------------------
# Test sort
# -------------------------------------------------------------------------


class TestSort:
    @pytest.fixture
    def sort_df(self):
        return pd.DataFrame({"name": ["Charlie", "Alice", "Bob"], "age": [30, 25, 25]})

    def test_ascending(self, sort_df):
        ctx = setup_context(sort_df)
        params = SortParams(by="name", ascending=True)
        result = sort(ctx, params).df

        assert list(result["name"]) == ["Alice", "Bob", "Charlie"]

    def test_descending(self, sort_df):
        ctx = setup_context(sort_df)
        params = SortParams(by="age", ascending=False)
        result = sort(ctx, params).df

        assert result.iloc[0]["age"] == 30

    def test_multi_column(self, sort_df):
        ctx = setup_context(sort_df)
        params = SortParams(by=["age", "name"], ascending=True)
        result = sort(ctx, params).df

        assert result.iloc[0]["name"] == "Alice"
        assert result.iloc[1]["name"] == "Bob"


# -------------------------------------------------------------------------
# Test limit
# -------------------------------------------------------------------------


class TestLimit:
    @pytest.fixture
    def limit_df(self):
        return pd.DataFrame({"id": range(10)})

    def test_basic_limit(self, limit_df):
        ctx = setup_context(limit_df)
        params = LimitParams(n=3)
        result = limit(ctx, params).df

        assert len(result) == 3

    def test_limit_with_offset(self, limit_df):
        ctx = setup_context(limit_df)
        params = LimitParams(n=3, offset=5)
        result = limit(ctx, params).df

        assert len(result) == 3
        assert list(result["id"]) == [5, 6, 7]


# -------------------------------------------------------------------------
# Test sample
# -------------------------------------------------------------------------


class TestSample:
    def test_basic_sample(self):
        df = pd.DataFrame({"id": range(100)})
        ctx = setup_context(df)
        params = SampleParams(fraction=0.5)
        result = sample(ctx, params).df

        # Probabilistic — just check we got some rows but not all
        assert 0 < len(result) < 100


# -------------------------------------------------------------------------
# Test distinct
# -------------------------------------------------------------------------


class TestDistinct:
    @pytest.fixture
    def dup_df(self):
        return pd.DataFrame({"a": [1, 1, 2, 2], "b": ["x", "x", "y", "z"]})

    def test_all_columns(self, dup_df):
        ctx = setup_context(dup_df)
        params = DistinctParams()
        result = distinct(ctx, params).df

        assert len(result) == 3  # (1,x), (2,y), (2,z)

    def test_specific_columns(self, dup_df):
        ctx = setup_context(dup_df)
        params = DistinctParams(columns=["a"])
        result = distinct(ctx, params).df

        assert len(result) == 2
        assert set(result["a"]) == {1, 2}


# -------------------------------------------------------------------------
# Test fill_nulls
# -------------------------------------------------------------------------


class TestFillNulls:
    def test_string_fill(self):
        df = pd.DataFrame({"status": [None, "active", None]})
        ctx = setup_context(df)
        params = FillNullsParams(values={"status": "unknown"})
        result = fill_nulls(ctx, params).df

        assert result.iloc[0]["status"] == "unknown"
        assert result.iloc[1]["status"] == "active"
        assert result.iloc[2]["status"] == "unknown"

    def test_numeric_fill(self):
        df = pd.DataFrame({"count": [None, 5, None]})
        ctx = setup_context(df)
        params = FillNullsParams(values={"count": 0})
        result = fill_nulls(ctx, params).df

        assert result.iloc[0]["count"] == 0
        assert result.iloc[1]["count"] == 5

    def test_boolean_fill(self):
        df = pd.DataFrame({"flag": [None, True, None]})
        ctx = setup_context(df)
        params = FillNullsParams(values={"flag": False})
        result = fill_nulls(ctx, params).df

        assert result.iloc[0]["flag"] is False or result.iloc[0]["flag"] == False  # noqa: E712
        assert result.iloc[1]["flag"] is True or result.iloc[1]["flag"] == True  # noqa: E712


# -------------------------------------------------------------------------
# Test split_part (DuckDB path)
# -------------------------------------------------------------------------


class TestSplitPart:
    def test_email_split(self):
        df = pd.DataFrame({"email": ["alice@example.com", "bob@test.org"]})
        ctx = setup_context(df)
        params = SplitPartParams(col="email", delimiter="@", index=2)
        result = split_part(ctx, params).df

        assert "email_part_2" in result.columns
        assert result.iloc[0]["email_part_2"] == "example.com"
        assert result.iloc[1]["email_part_2"] == "test.org"


# -------------------------------------------------------------------------
# Test date_add
# -------------------------------------------------------------------------


class TestDateAdd:
    def test_add_days(self):
        df = pd.DataFrame({"created_at": pd.to_datetime(["2024-01-01", "2024-06-15"])})
        ctx = setup_context(df)
        params = DateAddParams(col="created_at", value=7, unit="day")
        result = date_add(ctx, params).df

        assert "created_at_future" in result.columns
        assert result.iloc[0]["created_at_future"] == pd.Timestamp("2024-01-08")


# -------------------------------------------------------------------------
# Test date_trunc
# -------------------------------------------------------------------------


class TestDateTrunc:
    def test_truncate_to_month(self):
        df = pd.DataFrame({"ts": pd.to_datetime(["2024-03-15 10:30:00", "2024-07-22 14:45:00"])})
        ctx = setup_context(df)
        params = DateTruncParams(col="ts", unit="month")
        result = date_trunc(ctx, params).df

        assert "ts_trunc" in result.columns
        assert result.iloc[0]["ts_trunc"] == pd.Timestamp("2024-03-01")
        assert result.iloc[1]["ts_trunc"] == pd.Timestamp("2024-07-01")


# -------------------------------------------------------------------------
# Test date_diff (DuckDB path)
# -------------------------------------------------------------------------


class TestDateDiff:
    @pytest.fixture
    def diff_df(self):
        return pd.DataFrame(
            {
                "start_ts": pd.to_datetime(["2024-01-01 00:00:00", "2024-06-01 12:00:00"]),
                "end_ts": pd.to_datetime(["2024-01-10 00:00:00", "2024-06-01 18:00:00"]),
            }
        )

    def test_day_diff(self, diff_df):
        ctx = setup_context(diff_df)
        params = DateDiffParams(start_col="start_ts", end_col="end_ts", unit="day")
        result = date_diff(ctx, params).df

        assert "diff_day" in result.columns
        assert result.iloc[0]["diff_day"] == 9

    def test_hour_diff(self, diff_df):
        ctx = setup_context(diff_df)
        params = DateDiffParams(start_col="start_ts", end_col="end_ts", unit="hour")
        result = date_diff(ctx, params).df

        assert "diff_hour" in result.columns
        assert result.iloc[1]["diff_hour"] == pytest.approx(6.0, rel=0.01)


# -------------------------------------------------------------------------
# Test case_when
# -------------------------------------------------------------------------


class TestCaseWhen:
    def test_basic_branching(self):
        df = pd.DataFrame({"age": [15, 30, 70]})
        ctx = setup_context(df)
        params = CaseWhenParams(
            output_col="category",
            default="'Adult'",
            cases=[
                CaseWhenCase(condition="age < 18", value="'Minor'"),
                CaseWhenCase(condition="age > 65", value="'Senior'"),
            ],
        )
        result = case_when(ctx, params).df

        assert result.iloc[0]["category"] == "Minor"
        assert result.iloc[1]["category"] == "Adult"
        assert result.iloc[2]["category"] == "Senior"


# -------------------------------------------------------------------------
# Test convert_timezone (DuckDB path)
# -------------------------------------------------------------------------


class TestConvertTimezone:
    def test_utc_to_eastern(self):
        df = pd.DataFrame({"utc_time": pd.to_datetime(["2024-06-15 12:00:00"])})
        ctx = setup_context(df)
        params = ConvertTimezoneParams(
            col="utc_time", source_tz="UTC", target_tz="America/New_York"
        )
        result = convert_timezone(ctx, params).df

        assert "utc_time_converted" in result.columns
        converted = pd.Timestamp(result.iloc[0]["utc_time_converted"])
        assert converted.hour == 8  # UTC-4 in June (EDT)

    def test_custom_output_col(self):
        df = pd.DataFrame({"utc_time": pd.to_datetime(["2024-01-15 12:00:00"])})
        ctx = setup_context(df)
        params = ConvertTimezoneParams(
            col="utc_time", source_tz="UTC", target_tz="America/New_York", output_col="eastern"
        )
        result = convert_timezone(ctx, params).df

        assert "eastern" in result.columns


# -------------------------------------------------------------------------
# Test concat_columns
# -------------------------------------------------------------------------


class TestConcatColumns:
    def test_basic_concat(self):
        df = pd.DataFrame({"first": ["Alice", "Bob"], "last": ["Smith", "Jones"]})
        ctx = setup_context(df)
        params = ConcatColumnsParams(columns=["first", "last"], separator=" ", output_col="full")
        result = concat_columns(ctx, params).df

        assert "full" in result.columns
        assert result.iloc[0]["full"] == "Alice Smith"
        assert result.iloc[1]["full"] == "Bob Jones"


# -------------------------------------------------------------------------
# Test normalize_column_names
# -------------------------------------------------------------------------


class TestNormalizeColumnNames:
    def test_snake_case_conversion(self):
        df = pd.DataFrame({"First Name": [1], "Last-Name": [2], "order.date": [3]})
        ctx = setup_context(df)
        params = NormalizeColumnNamesParams()
        result = normalize_column_names(ctx, params).df

        assert "first_name" in result.columns
        assert "last_name" in result.columns
        assert "order_date" in result.columns

    def test_special_char_removal(self):
        df = pd.DataFrame({"price ($)": [100], "count #": [5]})
        ctx = setup_context(df)
        params = NormalizeColumnNamesParams()
        result = normalize_column_names(ctx, params).df

        for col in result.columns:
            assert col.isidentifier() or col.replace("_", "").isalnum()

    def test_lowercase(self):
        df = pd.DataFrame({"STATUS": [1], "Name": [2]})
        ctx = setup_context(df)
        params = NormalizeColumnNamesParams(style="none", lowercase=True, remove_special=False)
        result = normalize_column_names(ctx, params).df

        assert "status" in result.columns
        assert "name" in result.columns


# -------------------------------------------------------------------------
# Test coalesce_columns with drop_source=True (EXCLUDE path)
# -------------------------------------------------------------------------


class TestCoalesceColumnsDropSource:
    def test_drop_source_pandas(self):
        df = pd.DataFrame({"a": [None, 1], "b": [2, None], "c": [10, 20]})
        ctx = setup_context(df)
        params = CoalesceColumnsParams(columns=["a", "b"], output_col="result", drop_source=True)
        result = coalesce_columns(ctx, params).df

        assert "result" in result.columns
        assert "a" not in result.columns
        assert "b" not in result.columns
        assert "c" in result.columns
        assert result.iloc[0]["result"] == 2
        assert result.iloc[1]["result"] == 1

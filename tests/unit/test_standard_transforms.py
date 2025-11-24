import pytest
import pandas as pd
from odibi.context import EngineContext, create_context
from odibi.enums import EngineType
from odibi.transformers import sql_core, relational, advanced

# -------------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------------


@pytest.fixture
def pandas_context():
    """Creates a Pandas EngineContext with sample data."""
    # Sample Data
    data = {
        "id": [1, 2, 3, 4, 1],  # duplicate id=1
        "name": [" Alice ", "Bob", "Charlie", "David", "Alice"],
        "age": [25, 30, 35, 40, 25],
        "dept": ["HR", "IT", "HR", "IT", "HR"],
        "joined_at": pd.to_datetime(
            ["2023-01-01", "2023-02-15", "2023-03-10", "2023-04-05", "2023-01-01"]
        ),
        "tags": [["a", "b"], ["c"], ["d", "e"], [], ["a", "b"]],
    }
    df = pd.DataFrame(data)

    # Create context
    ctx = create_context(engine="pandas")

    # Define mock SQL executor
    def mock_sql_executor(sql: str, context) -> pd.DataFrame:
        import duckdb

        # Register all DFs from context
        for name in context.list_names():
            # DuckDB can't query Context directly, we need to extract DFs
            # and perhaps register them in DuckDB connection?
            # Simplest: Let DuckDB look at local variables if we extract them
            locals()[name] = context.get(name)

        # Execute
        return duckdb.query(sql).to_df()

    # Initialize EngineContext manually

    return EngineContext(ctx, df, EngineType.PANDAS, sql_executor=mock_sql_executor)


# -------------------------------------------------------------------------
# SQL Core Tests
# -------------------------------------------------------------------------


def test_filter_rows(pandas_context):
    params = sql_core.FilterRowsParams(condition="age > 30")
    result_ctx = sql_core.filter_rows(pandas_context, params)
    res = result_ctx.df

    assert len(res) == 2
    assert 35 in res["age"].values
    assert 40 in res["age"].values
    assert 25 not in res["age"].values


def test_derive_columns(pandas_context):
    params = sql_core.DeriveColumnsParams(
        derivations={"age_plus_one": "age + 1", "is_hr": "CASE WHEN dept = 'HR' THEN 1 ELSE 0 END"}
    )
    result_ctx = sql_core.derive_columns(pandas_context, params)
    res = result_ctx.df

    assert "age_plus_one" in res.columns
    assert res.iloc[0]["age_plus_one"] == 26
    assert res.iloc[0]["is_hr"] == 1
    assert res.iloc[1]["is_hr"] == 0


def test_clean_text(pandas_context):
    # " Alice " -> "alice"
    params = sql_core.CleanTextParams(columns=["name"], trim=True, case="lower")
    result_ctx = sql_core.clean_text(pandas_context, params)
    res = result_ctx.df

    assert res.iloc[0]["name"] == "alice"
    assert res.iloc[1]["name"] == "bob"


def test_extract_date_parts(pandas_context):
    params = sql_core.ExtractDateParams(source_col="joined_at", parts=["year", "month"])
    result_ctx = sql_core.extract_date_parts(pandas_context, params)
    res = result_ctx.df

    assert "joined_at_year" in res.columns
    assert "joined_at_month" in res.columns
    assert res.iloc[0]["joined_at_year"] == 2023
    assert res.iloc[1]["joined_at_month"] == 2


def test_normalize_schema(pandas_context):
    params = sql_core.NormalizeSchemaParams(
        rename={"id": "user_id"}, drop=["tags"], select_order=["user_id", "name"]
    )
    result_ctx = sql_core.normalize_schema(pandas_context, params)
    res = result_ctx.df

    assert list(res.columns) == ["user_id", "name"]
    assert "tags" not in res.columns


# -------------------------------------------------------------------------
# Relational Tests
# -------------------------------------------------------------------------


def test_join_pandas(pandas_context):
    # Create another DF for joining
    salaries = pd.DataFrame({"emp_id": [1, 2], "salary": [50000, 60000]})
    pandas_context.context.register("salaries", salaries)

    params = relational.JoinParams(
        right_dataset="salaries",
        on="id",  # Requires renaming right side if column names differ, or mapping
        # Wait, 'on' implies same name. Let's rename 'emp_id' to 'id' in sample
    )

    # Fix sample for join
    salaries_fixed = pd.DataFrame({"id": [1, 2], "salary": [50000, 60000]})
    pandas_context.context.register("salaries_fixed", salaries_fixed)

    params = relational.JoinParams(right_dataset="salaries_fixed", on="id", how="left")

    result_ctx = relational.join(pandas_context, params)
    res = result_ctx.df

    assert "salary" in res.columns
    # ID 1 should have salary, ID 3 (Charlie) should be NaN (Left Join)
    row_charlie = res[res["name"].str.contains("Charlie")]
    assert pd.isna(row_charlie.iloc[0]["salary"])

    row_alice = res[res["name"].str.contains("Alice")].iloc[0]
    assert row_alice["salary"] == 50000


def test_pivot_pandas(pandas_context):
    # Pivot: Dept -> Sum(Age)
    # Data: HR (25, 35, 25), IT (30, 40)

    params = relational.PivotParams(
        group_by=["dept"],
        pivot_col="name",  # Too many names, let's pivot something simpler
        # Let's pivot 'dept' on 'age'? No.
        # Let's create dummy year col
        agg_col="age",
        agg_func="sum",
    )

    # Pivot is usually: Index=Year, Columns=Dept, Values=Sales
    # Here: Index=id?

    # Let's construct specific DF for pivot test
    pivot_data = pd.DataFrame(
        {
            "year": [2020, 2020, 2021, 2021],
            "region": ["North", "South", "North", "South"],
            "sales": [100, 200, 150, 250],
        }
    )
    # Need EngineContext wrapper
    from odibi.enums import EngineType

    ctx = create_context("pandas")  # Just to satisfy constructor
    ctx = EngineContext(ctx, pivot_data, EngineType.PANDAS)

    params = relational.PivotParams(
        group_by=["year"], pivot_col="region", agg_col="sales", agg_func="sum"
    )

    result_ctx = relational.pivot(ctx, params)
    res = result_ctx.df

    assert "North" in res.columns
    assert "South" in res.columns
    assert res[res["year"] == 2020]["North"].values[0] == 100


# -------------------------------------------------------------------------
# Advanced Tests
# -------------------------------------------------------------------------


def test_deduplicate(pandas_context):
    # Alice (id=1) is duplicated
    params = advanced.DeduplicateParams(keys=["id"], order_by="joined_at DESC")

    result_ctx = advanced.deduplicate(pandas_context, params)
    res = result_ctx.df

    # Should be 4 unique IDs (1, 2, 3, 4)
    assert len(res) == 4
    assert res["id"].is_unique


def test_explode(pandas_context):
    # tags: [a,b], [c], [d,e], [], [a,b]
    params = advanced.ExplodeParams(column="tags")

    result_ctx = advanced.explode_list_column(pandas_context, params)
    res = result_ctx.df

    # Rows: 2 + 1 + 2 + 0 + 2 = 7 rows
    assert len(res) == 7
    assert "tags" in res.columns


def test_dict_mapping(pandas_context):
    mapping = {"HR": "Human Resources", "IT": "Tech"}
    params = advanced.DictMappingParams(column="dept", mapping=mapping, output_column="dept_full")

    result_ctx = advanced.dict_based_mapping(pandas_context, params)
    res = result_ctx.df

    assert "dept_full" in res.columns
    assert res.iloc[0]["dept_full"] == "Human Resources"
    assert res.iloc[1]["dept_full"] == "Tech"


def test_hash_columns(pandas_context):
    params = advanced.HashParams(columns=["name"], algorithm="md5")

    result_ctx = advanced.hash_columns(pandas_context, params)
    res = result_ctx.df

    # " Alice " hashed
    val = res.iloc[0]["name"]
    assert len(val) == 32  # MD5 length
    assert val != " Alice "


# -------------------------------------------------------------------------
# Additional Core Tests
# -------------------------------------------------------------------------


def test_sort(pandas_context):
    params = sql_core.SortParams(by="age", ascending=False)
    result = sql_core.sort(pandas_context, params).df
    assert result.iloc[0]["age"] == 40
    assert result.iloc[-1]["age"] == 25


def test_limit(pandas_context):
    params = sql_core.LimitParams(n=2)
    result = sql_core.limit(pandas_context, params).df
    assert len(result) == 2


def test_distinct(pandas_context):
    # Original has 5 rows.
    # ID 1 appears twice, but "name" differs (" Alice " vs "Alice").
    # So DISTINCT * should return 5 rows.
    params = sql_core.DistinctParams()
    result = sql_core.distinct(pandas_context, params).df
    assert len(result) == 5

    # Distinct on ID (should merge the two ID=1 rows if we only select ID)
    params_id = sql_core.DistinctParams(columns=["id"])
    result_id = sql_core.distinct(pandas_context, params_id).df
    assert len(result_id) == 4

    # Distinct on dept
    params_dept = sql_core.DistinctParams(columns=["dept"])
    result_dept = sql_core.distinct(pandas_context, params_dept).df
    assert len(result_dept) == 2  # HR and IT


def test_aggregate(pandas_context):
    params = relational.AggregateParams(
        group_by=["dept"], aggregations={"age": "avg", "id": "count"}
    )
    result = relational.aggregate(pandas_context, params).df

    assert len(result) == 2
    row_hr = result[result["dept"] == "HR"].iloc[0]
    # HR ages: 25, 35, 25. Avg = 28.33
    assert abs(row_hr["age"] - 28.33) < 0.1
    assert row_hr["id"] == 3


def test_join_explicit_projection(pandas_context):
    # Test the new join logic with collisions
    # Left: id, name, age, dept
    # Right: id, salary, dept (collision on dept)

    right_df = pd.DataFrame(
        {
            "id": [1, 2],
            "salary": [50000, 60000],
            "dept": ["HR_Right", "IT_Right"],  # Different values to verify
        }
    )
    pandas_context.context.register("salaries", right_df)

    params = relational.JoinParams(right_dataset="salaries", on="id", how="left")

    result = relational.join(pandas_context, params).df

    assert "salary" in result.columns
    # Check collision handling
    # Should have 'dept' (from left) and 'dept_salaries' (from right - default fallback)
    assert "dept" in result.columns
    assert "dept_salaries" in result.columns

    row = result[result["id"] == 1].iloc[0]
    assert row["dept"] == "HR"
    assert row["dept_salaries"] == "HR_Right"


# -------------------------------------------------------------------------
# Extended SQL Core Tests
# -------------------------------------------------------------------------


def test_split_part(pandas_context):
    # " Alice " -> " Alice " (no delimiter match logic check)
    # Let's use a new string column for split test
    df = pandas_context.df.copy()
    df["email"] = [
        "alice@example.com",
        "bob@corp.org",
        "charlie@test.net",
        "david@web.com",
        "alice@example.com",
    ]
    pandas_context.df = df

    params = sql_core.SplitPartParams(col="email", delimiter="@", index=1)
    result = sql_core.split_part(pandas_context, params).df

    assert "email_part_1" in result.columns
    assert result.iloc[0]["email_part_1"] == "alice"
    assert result.iloc[1]["email_part_1"] == "bob"


def test_date_add(pandas_context):
    params = sql_core.DateAddParams(col="joined_at", value=5, unit="day")
    result = sql_core.date_add(pandas_context, params).df

    # 2023-01-01 + 5 days = 2023-01-06
    assert "joined_at_future" in result.columns
    assert str(result.iloc[0]["joined_at_future"].date()) == "2023-01-06"


def test_date_trunc(pandas_context):
    params = sql_core.DateTruncParams(col="joined_at", unit="month")
    result = sql_core.date_trunc(pandas_context, params).df

    # 2023-02-15 -> 2023-02-01
    assert "joined_at_trunc" in result.columns
    assert str(result.iloc[1]["joined_at_trunc"].date()) == "2023-02-01"


def test_date_diff(pandas_context):
    # Diff between joined_at and now? Or add a column.
    df = pandas_context.df.copy()
    df["end_date"] = df["joined_at"] + pd.Timedelta(days=10)
    pandas_context.df = df

    params = sql_core.DateDiffParams(start_col="joined_at", end_col="end_date", unit="day")
    result = sql_core.date_diff(pandas_context, params).df

    assert "diff_day" in result.columns
    assert result.iloc[0]["diff_day"] == 10


def test_case_when(pandas_context):
    params = sql_core.CaseWhenParams(
        cases=[
            {"condition": "age < 30", "value": "'Junior'"},
            {"condition": "age >= 30", "value": "'Senior'"},
        ],
        default="'Unknown'",
        output_col="level",
    )
    result = sql_core.case_when(pandas_context, params).df

    assert "level" in result.columns
    assert result.iloc[0]["level"] == "Junior"  # Age 25
    assert result.iloc[1]["level"] == "Senior"  # Age 30


def test_convert_timezone(pandas_context):
    # We have joined_at in naive UTC (2023-01-01).
    # Let's convert to 'America/New_York'.
    # 2023-01-01 00:00 UTC is 2022-12-31 19:00 EST.

    params = sql_core.ConvertTimezoneParams(
        col="joined_at", source_tz="UTC", target_tz="America/New_York", output_col="joined_at_ny"
    )

    result = sql_core.convert_timezone(pandas_context, params).df

    assert "joined_at_ny" in result.columns
    val = pd.to_datetime(result.iloc[0]["joined_at_ny"])

    # Check hour (should be 19:00 previous day)
    # Note: DuckDB/Spark might return timezone aware or naive timestamp depending on driver.
    # Pandas typically handles it.
    # If naive, it should just be 19:00.
    assert val.hour == 19
    assert val.year == 2022


# -------------------------------------------------------------------------
# Extended Advanced Tests
# -------------------------------------------------------------------------


def test_validate_and_flag(pandas_context):
    # Valid: age > 20. Invalid: age > 100 (fails no one here), age < 26 (Alice fails rule 'mature')
    # Let's say:
    # Rule 1: "valid_age": "age >= 18" (All pass)
    # Rule 2: "not_hr": "dept != 'HR'" (Alice fails)

    params = advanced.ValidateAndFlagParams(
        rules={"valid_age": "age >= 18", "not_hr": "dept != 'HR'"}, flag_col="validation_issues"
    )

    result = advanced.validate_and_flag(pandas_context, params).df

    assert "validation_issues" in result.columns
    # Alice (HR) should fail 'not_hr'
    issues = result.iloc[0]["validation_issues"]
    assert "not_hr" in str(issues)
    assert "valid_age" not in str(issues)  # Passed

    # Bob (IT) should pass both?
    # Bob age 30 >= 18 (Pass), dept IT != HR (Pass)
    # So issues should be NULL or None or empty
    bob_issues = result.iloc[1]["validation_issues"]
    assert pd.isna(bob_issues) or bob_issues == ""


def test_window_calculation(pandas_context):
    # Rank by age within dept
    params = advanced.WindowCalculationParams(
        target_col="age_rank", function="rank()", partition_by=["dept"], order_by="age DESC"
    )

    result = advanced.window_calculation(pandas_context, params).df

    assert "age_rank" in result.columns
    # IT: Bob (30), David (40). Rank: David 1, Bob 2
    it_rows = result[result["dept"] == "IT"].sort_values("age", ascending=False)
    assert it_rows.iloc[0]["age_rank"] == 1  # David
    assert it_rows.iloc[1]["age_rank"] == 2  # Bob

# Add project root to path to import odibi
import os
import sys

import duckdb
import pandas as pd

sys.path.append(os.getcwd())

from odibi.context import EngineContext, create_context
from odibi.enums import EngineType
from odibi.transformers import advanced, relational, sql_core

# -------------------------------------------------------------------------
# Setup
# -------------------------------------------------------------------------

# Setup persistent DuckDB connection with JSON extension
try:
    con = duckdb.connect()
    con.install_extension("json")
    con.load_extension("json")
except Exception as e:
    print(f"Warning: Could not load DuckDB JSON extension: {e}")


def get_pandas_context(df: pd.DataFrame) -> EngineContext:
    """Creates a Pandas EngineContext with a DuckDB SQL executor."""

    # Create base context
    ctx = create_context(engine="pandas")

    # Define SQL executor
    def sql_executor(sql: str, context) -> pd.DataFrame:
        # Register all DFs from context into DuckDB

        # We extract 'df' from context (registered by EngineContext.sql)
        try:
            current_df = context.get("df")
            con.register("df", current_df)
        except Exception:
            pass

        for name in context.list_names():
            con.register(name, context.get(name))

        # Execute
        return con.query(sql).to_df()

    return EngineContext(ctx, df, EngineType.PANDAS, sql_executor=sql_executor)


def run_test(name: str, func):
    """Helper to run a test and print status."""
    try:
        func()
        print(f"[PASS] {name}")
    except Exception as e:
        print(f"[FAIL] {name} FAILED: {e}")
        import traceback

        traceback.print_exc()


# -------------------------------------------------------------------------
# Data
# -------------------------------------------------------------------------


def create_sample_data():
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 1],
            "name": [" Alice ", "Bob", "Charlie", "David", "Alice"],
            "age": [25, 30, 35, 40, 25],
            "dept": ["HR", "IT", "HR", "IT", "HR"],
            "joined_at": pd.to_datetime(
                ["2023-01-01", "2023-02-15", "2023-03-10", "2023-04-05", "2023-01-01"]
            ),
            "salary": [50000, 60000, 55000, 70000, 50000],
            "tags": [["a", "b"], ["c"], ["d", "e"], [], ["a", "b"]],
            "meta": [
                '{"role": "admin"}',
                '{"role": "user"}',
                "{}",
                '{"role": "lead"}',
                '{"role": "admin"}',
            ],
        }
    )


# -------------------------------------------------------------------------
# Tests
# -------------------------------------------------------------------------


def test_sql_core():
    print("\n--- SQL Core Transformers ---")
    df = create_sample_data()
    ctx = get_pandas_context(df)

    # 1. Filter
    def t_filter():
        res = sql_core.filter_rows(ctx, sql_core.FilterRowsParams(condition="age > 30")).df
        assert len(res) == 2

    # 2. Derive
    def t_derive():
        res = sql_core.derive_columns(
            ctx, sql_core.DeriveColumnsParams(derivations={"age_next_year": "age + 1"})
        ).df
        assert res.iloc[0]["age_next_year"] == 26

    # 3. Clean Text
    def t_clean():
        res = sql_core.clean_text(
            ctx, sql_core.CleanTextParams(columns=["name"], trim=True, case="lower")
        ).df
        assert res.iloc[0]["name"] == "alice"

    # 4. Split Part (New)
    def t_split():
        # Add email col
        df_email = df.copy()
        df_email["email"] = ["alice@corp.com"] * 5
        ctx_email = get_pandas_context(df_email)
        res = sql_core.split_part(
            ctx_email, sql_core.SplitPartParams(col="email", delimiter="@", index=1)
        ).df
        assert res.iloc[0]["email_part_1"] == "alice"

    # 5. Case When (New)
    def t_case():
        res = sql_core.case_when(
            ctx,
            sql_core.CaseWhenParams(
                cases=[{"condition": "age < 30", "value": "'Young'"}],
                default="'Old'",
                output_col="category",
            ),
        ).df
        assert res.iloc[0]["category"] == "Young"
        assert res.iloc[1]["category"] == "Old"

    # 6. Concat Columns (New)
    def t_concat():
        res = sql_core.concat_columns(
            ctx,
            sql_core.ConcatColumnsParams(
                columns=["dept", "name"], separator="-", output_col="full_id"
            ),
        ).df
        # Note: name has spaces " Alice "
        assert "HR- Alice " in res.iloc[0]["full_id"]

    # 7. Date Functions (New)
    def t_dates():
        # Date Add
        res = sql_core.date_add(
            ctx, sql_core.DateAddParams(col="joined_at", value=1, unit="year")
        ).df
        assert str(res.iloc[0]["joined_at_future"].year) == "2024"

        # Date Trunc
        res = sql_core.date_trunc(ctx, sql_core.DateTruncParams(col="joined_at", unit="month")).df
        # 2023-02-15 -> 2023-02-01
        assert res.iloc[1]["joined_at_trunc"].day == 1

        # Date Diff
        # Diff between row 0 and row 1? No, diff is between cols.
        # Let's derive a future date first
        res["end_date"] = res["joined_at"] + pd.Timedelta(days=10)
        ctx_dates = get_pandas_context(res)
        res_diff = sql_core.date_diff(
            ctx_dates,
            sql_core.DateDiffParams(start_col="joined_at", end_col="end_date", unit="day"),
        ).df
        assert res_diff.iloc[0]["diff_day"] == 10

    # 8. Timezone (New)
    def t_timezone():
        # UTC -> NY
        res = sql_core.convert_timezone(
            ctx,
            sql_core.ConvertTimezoneParams(
                col="joined_at", source_tz="UTC", target_tz="America/New_York", output_col="ny_time"
            ),
        ).df
        # 2023-01-01 00:00 UTC -> 2022-12-31 19:00 EST
        assert res.iloc[0]["ny_time"].hour == 19

    # Run
    run_test("filter_rows", t_filter)
    run_test("derive_columns", t_derive)
    run_test("clean_text", t_clean)
    run_test("split_part", t_split)
    run_test("case_when", t_case)
    run_test("concat_columns", t_concat)
    run_test("date_functions", t_dates)
    run_test("convert_timezone", t_timezone)


def test_relational():
    print("\n--- Relational Transformers ---")
    df = create_sample_data()
    ctx = get_pandas_context(df)

    # Register a second table
    df2 = pd.DataFrame({"id": [1, 2], "bonus": [1000, 2000]})
    ctx.context.register("bonuses", df2)

    # 1. Join
    def t_join():
        res = relational.join(
            ctx, relational.JoinParams(right_dataset="bonuses", on="id", how="left")
        ).df
        assert "bonus" in res.columns
        assert res.iloc[0]["bonus"] == 1000.0

    # 2. Aggregate
    def t_agg():
        res = relational.aggregate(
            ctx, relational.AggregateParams(group_by=["dept"], aggregations={"salary": "avg"})
        ).df
        # HR: 50k, 55k, 50k -> 51666.66
        hr_sal = res[res["dept"] == "HR"].iloc[0]["salary"]
        assert 51000 < hr_sal < 52000

    # 3. Pivot
    def t_pivot():
        # Pivot dept vs avg salary?
        # Need simple data
        df_p = pd.DataFrame(
            {"year": [2020, 2020, 2021], "cat": ["A", "B", "A"], "val": [10, 20, 30]}
        )
        ctx_p = get_pandas_context(df_p)
        res = relational.pivot(
            ctx_p,
            relational.PivotParams(
                group_by=["year"], pivot_col="cat", agg_col="val", agg_func="sum"
            ),
        ).df
        assert "A" in res.columns and "B" in res.columns

    run_test("join", t_join)
    run_test("aggregate", t_agg)
    run_test("pivot", t_pivot)


def test_advanced():
    print("\n--- Advanced Transformers ---")
    df = create_sample_data()
    ctx = get_pandas_context(df)

    # 1. Deduplicate
    def t_dedup():
        res = advanced.deduplicate(ctx, advanced.DeduplicateParams(keys=["id"])).df
        assert len(res) == 4  # 5 rows, 1 dupe

    # 2. Validate & Flag (New)
    def t_validate():
        res = advanced.validate_and_flag(
            ctx,
            advanced.ValidateAndFlagParams(
                rules={"rich": "salary > 60000", "young": "age < 30"}, flag_col="issues"
            ),
        ).df
        # Alice (50k, 25): Fails rich, Passes young -> issues="rich"
        # Bob (60k, 30): Fails rich (not >), Fails young (not <) -> issues="rich, young"
        # Wait, verify logic: "condition must be true for valid".
        # If condition fails, it is flagged.

        alice = res.iloc[0]  # age 25, sal 50000
        # rich (50k > 60k) -> False -> Flag "rich"
        # young (25 < 30) -> True -> Pass
        assert "rich" in alice["issues"]
        assert "young" not in alice["issues"]

    # 3. Window (New)
    def t_window():
        res = advanced.window_calculation(
            ctx,
            advanced.WindowCalculationParams(
                target_col="rank_sal",
                function="rank()",
                partition_by=["dept"],
                order_by="salary DESC",
            ),
        ).df
        # IT: David 70k (1), Bob 60k (2)
        david = res[res["name"].str.contains("David")].iloc[0]
        assert david["rank_sal"] == 1

    # 4. Surrogate Key (New)
    def t_surrogate():
        res = advanced.generate_surrogate_key(
            ctx, advanced.SurrogateKeyParams(columns=["dept", "id"], separator="_")
        ).df
        assert len(res.iloc[0]["surrogate_key"]) == 32

    # 5. Parse JSON (New)
    def t_json():
        # 'meta' col has json
        res = advanced.parse_json(
            ctx, advanced.ParseJsonParams(column="meta", json_schema="role STRING")
        ).df
        # DuckDB json_parse returns a JSON object type, pandas sees it as object/string representation
        # or standard dict if converted?
        # DuckDB to_df() usually converts JSON types to String? Or Objects?
        # Let's inspect
        val = res.iloc[0]["meta_parsed"]
        # It might be a string '{"role": "admin"}'
        assert val is not None

    # 6. Unpack Struct (Uses Pandas impl)
    def t_unpack():
        # Only works if column is dict/struct.
        # If parse_json returned a string (in duckdb conversion), this might fail in Pandas.
        # Let's manually create a struct col for this test
        df_struct = df.copy()
        df_struct["info"] = [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}]
        ctx_struct = get_pandas_context(df_struct)

        res = advanced.unpack_struct(ctx_struct, advanced.UnpackStructParams(column="info")).df
        assert "a" in res.columns

    run_test("deduplicate", t_dedup)
    run_test("validate_and_flag", t_validate)
    run_test("window_calculation", t_window)
    run_test("generate_surrogate_key", t_surrogate)
    run_test("parse_json", t_json)
    run_test("unpack_struct", t_unpack)


if __name__ == "__main__":
    print("Starting Comprehensive Standard Library Test (Pandas Engine)...")
    test_sql_core()
    test_relational()
    test_advanced()
    print("\nAll Tests Completed.")

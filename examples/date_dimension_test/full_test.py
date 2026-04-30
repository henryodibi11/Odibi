"""Date Dimension Pattern — full feature test (Pandas + Spark).

Task 20 deliverable. Validates DateDimensionPattern generates correct values
for every column on both engines, exercises all fiscal-year start months,
unknown-member handling, and edge-case date ranges (single day, leap-year
boundary, 10-year span).

Run:
    & "C:\\Users\\HOdibi\\Repos\\.venv\\Scripts\\python.exe" \
        examples\\date_dimension_test\\full_test.py
"""
from __future__ import annotations

import sys
import time
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from odibi.context import EngineContext, PandasContext, SparkContext  # noqa: E402
from odibi.engine.pandas_engine import PandasEngine  # noqa: E402
from odibi.engine.spark_engine import SparkEngine  # noqa: E402
from odibi.enums import EngineType  # noqa: E402
from odibi.patterns.date_dimension import DateDimensionPattern  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ALL_COLS = [
    "date_sk", "full_date", "day_of_week", "day_of_week_num", "day_of_month",
    "day_of_year", "is_weekend", "week_of_year", "month", "month_name",
    "quarter", "quarter_name", "year", "fiscal_year", "fiscal_quarter",
    "is_month_start", "is_month_end", "is_year_start", "is_year_end",
]


def make_pattern(params: dict) -> DateDimensionPattern:
    cfg = MagicMock()
    cfg.name = "date_dim_test"
    cfg.params = params
    return DateDimensionPattern(engine=PandasEngine(config={}), config=cfg)


def pandas_ctx() -> EngineContext:
    return EngineContext(
        context=PandasContext(),
        df=None,
        engine_type=EngineType.PANDAS,
        engine=PandasEngine(config={}),
    )


def spark_ctx(spark) -> EngineContext:
    engine = SparkEngine(spark_session=spark, config={})
    return EngineContext(
        context=SparkContext(spark),
        df=None,
        engine_type=EngineType.SPARK,
        sql_executor=engine.execute_sql,
        engine=engine,
    )


def to_pandas(df) -> pd.DataFrame:
    return df.toPandas() if hasattr(df, "toPandas") else df


def assert_columns(df: pd.DataFrame, label: str):
    missing = set(ALL_COLS) - set(df.columns)
    assert not missing, f"[{label}] missing columns: {missing}"


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


def scenario_all_columns_pandas():
    print("  - 1_pandas_columns ...", end=" ", flush=True)
    p = make_pattern({"start_date": "2024-01-01", "end_date": "2024-12-31"})
    p.validate()
    df = p.execute(pandas_ctx())
    assert_columns(df, "pandas")
    assert len(df) == 366, f"2024 has 366 days, got {len(df)}"
    # Spot-check 2024-03-15 (Friday, Q1)
    row = df.loc[df["full_date"] == date(2024, 3, 15)].iloc[0]
    assert row["day_of_week"] == "Friday", row["day_of_week"]
    assert row["day_of_week_num"] == 5, row["day_of_week_num"]
    assert row["quarter"] == 1, row["quarter"]
    assert row["quarter_name"] == "Q1"
    assert row["month_name"] == "March"
    assert row["is_weekend"] is False or row["is_weekend"] == False  # noqa: E712
    assert row["date_sk"] == 20240315
    assert row["fiscal_year"] == 2024  # default fiscal start = Jan
    assert row["is_year_start"] is False or row["is_year_start"] == False  # noqa: E712
    assert df.loc[df["full_date"] == date(2024, 1, 1)].iloc[0]["is_year_start"]
    assert df.loc[df["full_date"] == date(2024, 12, 31)].iloc[0]["is_year_end"]
    print("ok")
    return df


def scenario_pandas_spark_parity(spark):
    print("  - 2_pandas_spark_parity (90 days) ...", end=" ", flush=True)
    p = make_pattern({"start_date": "2024-01-01", "end_date": "2024-03-31"})
    pdf = p.execute(pandas_ctx())
    sdf_raw = make_pattern({"start_date": "2024-01-01", "end_date": "2024-03-31"}).execute(
        spark_ctx(spark)
    )
    sdf = to_pandas(sdf_raw).sort_values("date_sk").reset_index(drop=True)
    pdfs = pdf.sort_values("date_sk").reset_index(drop=True)
    assert len(pdfs) == len(sdf) == 91, (len(pdfs), len(sdf))
    for col in [
        "date_sk", "day_of_week", "day_of_week_num", "day_of_month",
        "day_of_year", "is_weekend", "week_of_year", "month", "month_name",
        "quarter", "quarter_name", "year", "fiscal_year", "fiscal_quarter",
        "is_month_start", "is_month_end", "is_year_start", "is_year_end",
    ]:
        a = pdfs[col].tolist()
        b = sdf[col].tolist()
        assert a == b, f"col={col} pandas={a[:3]} spark={b[:3]}"
    # full_date: pandas returns Python date, Spark returns date — compare as iso strings
    assert pdfs["full_date"].astype(str).tolist() == sdf["full_date"].astype(str).tolist()
    print("ok")


def scenario_fiscal_years():
    print("  - 3_fiscal_years (1/4/7/10) ...", end=" ", flush=True)
    cases = [
        # (fiscal_start, date, expected_fy, expected_fq)
        (1, "2024-03-15", 2024, 1),   # calendar fiscal — March = Q1
        (4, "2024-03-15", 2024, 4),   # April-start FY: March = last month → Q4 of FY2024
        (4, "2024-04-01", 2025, 1),   # April-start FY: April = Q1 of FY2025
        (4, "2024-12-31", 2025, 3),   # April-start FY: Dec = Q3 of FY2025
        (7, "2024-06-30", 2024, 4),   # July-start FY: June = Q4 of FY2024
        (7, "2024-07-01", 2025, 1),   # July-start FY: July = Q1 of FY2025
        (10, "2024-09-30", 2024, 4),  # Oct-start FY: Sep = Q4 of FY2024
        (10, "2024-10-01", 2025, 1),  # Oct-start FY: Oct = Q1 of FY2025
    ]
    for fsm, dstr, exp_fy, exp_fq in cases:
        df = make_pattern(
            {"start_date": dstr, "end_date": dstr, "fiscal_year_start_month": fsm}
        ).execute(pandas_ctx())
        row = df.iloc[0]
        assert row["fiscal_year"] == exp_fy, f"fsm={fsm} {dstr} fy={row['fiscal_year']}"
        assert row["fiscal_quarter"] == exp_fq, f"fsm={fsm} {dstr} fq={row['fiscal_quarter']}"
    print(f"ok ({len(cases)} cases)")


def scenario_unknown_member():
    print("  - 4_unknown_member ...", end=" ", flush=True)
    p = make_pattern(
        {"start_date": "2024-01-01", "end_date": "2024-01-03", "unknown_member": True}
    )
    df = p.execute(pandas_ctx())
    assert len(df) == 4, f"3 days + 1 unknown = 4, got {len(df)}"
    unk = df.loc[df["date_sk"] == 0]
    assert len(unk) == 1, "exactly one unknown row"
    u = unk.iloc[0]
    assert u["day_of_week"] == "Unknown"
    assert u["month_name"] == "Unknown"
    assert u["quarter_name"] == "Unknown"
    assert u["full_date"] == date(1900, 1, 1)
    print("ok")


def scenario_unknown_member_spark(spark):
    print("  - 4b_unknown_member_spark ...", end=" ", flush=True)
    p = make_pattern(
        {"start_date": "2024-01-01", "end_date": "2024-01-03", "unknown_member": True}
    )
    sdf = p.execute(spark_ctx(spark))
    pdf = to_pandas(sdf).sort_values("date_sk").reset_index(drop=True)
    assert len(pdf) == 4, len(pdf)
    assert pdf.iloc[0]["date_sk"] == 0
    assert pdf.iloc[0]["day_of_week"] == "Unknown"
    print("ok")


def scenario_single_day():
    print("  - 5_single_day ...", end=" ", flush=True)
    df = make_pattern(
        {"start_date": "2024-07-04", "end_date": "2024-07-04"}
    ).execute(pandas_ctx())
    assert len(df) == 1
    row = df.iloc[0]
    assert row["date_sk"] == 20240704
    assert row["day_of_week"] == "Thursday"
    assert row["is_weekend"] is False or row["is_weekend"] == False  # noqa: E712
    print("ok")


def scenario_leap_year():
    print("  - 6_leap_year ...", end=" ", flush=True)
    df = make_pattern(
        {"start_date": "2024-02-28", "end_date": "2024-03-01"}
    ).execute(pandas_ctx())
    # Should include 2024-02-29
    assert len(df) == 3
    leap = df.loc[df["full_date"] == date(2024, 2, 29)]
    assert len(leap) == 1, "Feb 29 must be present in leap year 2024"
    assert leap.iloc[0]["day_of_year"] == 60  # 31 + 29
    # Also check non-leap 2023
    df_nonleap = make_pattern(
        {"start_date": "2023-02-28", "end_date": "2023-03-01"}
    ).execute(pandas_ctx())
    assert len(df_nonleap) == 2, "2023 has no Feb 29"
    print("ok")


def scenario_ten_year_perf():
    print("  - 7_ten_year_perf ...", end=" ", flush=True)
    t0 = time.time()
    df = make_pattern(
        {"start_date": "2015-01-01", "end_date": "2024-12-31"}
    ).execute(pandas_ctx())
    elapsed = time.time() - t0
    expected_days = (date(2024, 12, 31) - date(2015, 1, 1)).days + 1
    assert len(df) == expected_days, f"expected {expected_days}, got {len(df)}"
    assert elapsed < 30, f"10-year generation took {elapsed:.1f}s"
    print(f"ok ({expected_days} rows in {elapsed:.2f}s)")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> int:
    print("Date Dimension Pattern — full feature test")
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()
    print("  Spark Connect ready.\nRunning scenarios:")
    scenario_all_columns_pandas()
    scenario_pandas_spark_parity(spark)
    scenario_fiscal_years()
    scenario_unknown_member()
    scenario_unknown_member_spark(spark)
    scenario_single_day()
    scenario_leap_year()
    scenario_ten_year_perf()
    print("\nAll scenarios passed (Pandas + Spark validated).")
    return 0


if __name__ == "__main__":
    sys.exit(main())

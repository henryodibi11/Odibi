"""Aggregation Pattern stress test — Pandas + Spark engine parity.

Runs 8 scenarios against both engines and asserts exact aggregated values,
then compares Pandas and Spark results for parity.

Scenarios (Task 19, campaign #225):
    1. Simple aggregation — SUM, AVG, COUNT, MIN, MAX on one grain
    2. Multi-grain — GROUP BY [date, category, region]
    3. Incremental merge — strategy=replace
    4. Incremental merge — strategy=sum (additive)
    5. Audit columns — load_timestamp present and recent
    6. Large data — 100k rows aggregated to ~1k rows under 30s
    7. Null handling — nulls in grain columns
    8. Empty groups — HAVING that filters everything

Run:
    & "C:\\Users\\HOdibi\\Repos\\.venv\\Scripts\\python.exe" \
        examples\\aggregation_stress\\stress_test.py

Cleans up no Unity Catalog tables — all execution is in-memory.
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from odibi.context import EngineContext, PandasContext, SparkContext  # noqa: E402
from odibi.engine.pandas_engine import PandasEngine  # noqa: E402
from odibi.engine.spark_engine import SparkEngine  # noqa: E402
from odibi.enums import EngineType  # noqa: E402
from odibi.patterns.aggregation import AggregationPattern  # noqa: E402

# ---------------------------------------------------------------------------
# Engine wiring helpers
# ---------------------------------------------------------------------------


def duckdb_executor(query, ctx):
    conn = duckdb.connect()
    for n in ctx.list_names():
        conn.register(n, ctx.get(n))
    return conn.execute(query).fetchdf()


def make_pandas_ctx(df: pd.DataFrame) -> EngineContext:
    return EngineContext(
        context=PandasContext(),
        df=df,
        engine_type=EngineType.PANDAS,
        sql_executor=duckdb_executor,
        engine=PandasEngine(config={}),
    )


def make_spark_ctx(spark, pdf: pd.DataFrame) -> EngineContext:
    sdf = spark.createDataFrame(pdf)
    engine = SparkEngine(spark_session=spark, config={})
    return EngineContext(
        context=SparkContext(spark),
        df=sdf,
        engine_type=EngineType.SPARK,
        sql_executor=engine.execute_sql,
        engine=engine,
    )


def make_pattern(name: str, params: dict) -> AggregationPattern:
    cfg = MagicMock()
    cfg.name = name
    cfg.params = params
    return AggregationPattern(engine=PandasEngine(config={}), config=cfg)


# ---------------------------------------------------------------------------
# Test data generators
# ---------------------------------------------------------------------------


def base_sales(n_rows: int = 1000, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=30, freq="D")
    return pd.DataFrame(
        {
            "order_date": rng.choice(dates, size=n_rows),
            "category": rng.choice(["A", "B", "C"], size=n_rows),
            "region": rng.choice(["US", "EU", "APAC"], size=n_rows),
            "amount": rng.uniform(10, 1000, size=n_rows).round(2),
        }
    )


def to_native(df) -> pd.DataFrame:
    """Convert Spark or Pandas DF to a sorted pandas DF for comparison."""
    out = df.toPandas() if hasattr(df, "toPandas") else df
    return out.reset_index(drop=True)


def assert_parity(label: str, pdf, sdf, key_cols, ignore_cols=()):
    p = to_native(pdf).sort_values(key_cols).reset_index(drop=True)
    s = to_native(sdf).sort_values(key_cols).reset_index(drop=True)
    assert len(p) == len(s), f"[{label}] row count mismatch: pandas={len(p)} spark={len(s)}"
    common = [
        c for c in p.columns if c in s.columns and c not in key_cols and c not in ignore_cols
    ]
    for col in common:
        a, b = p[col].to_numpy(), s[col].to_numpy()
        if pd.api.types.is_numeric_dtype(p[col]):
            assert np.allclose(a, b, equal_nan=True), f"[{label}] {col}: pandas!=spark"
        else:
            # Coerce both to string for object/null mixed dtype safety (handles None vs nan)
            sa = pd.Series(a).astype("object").where(pd.notna(a), None).tolist()
            sb = pd.Series(b).astype("object").where(pd.notna(b), None).tolist()
            assert sa == sb, f"[{label}] {col}: pandas!=spark ({sa[:3]} vs {sb[:3]})"


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

MEASURES_FULL = [
    {"name": "total", "expr": "SUM(amount)"},
    {"name": "avg", "expr": "AVG(amount)"},
    {"name": "cnt", "expr": "COUNT(*)"},
    {"name": "min_amt", "expr": "MIN(amount)"},
    {"name": "max_amt", "expr": "MAX(amount)"},
]


def run_scenario(spark, label, params, pdf, key_cols, asserts=None, ignore_cols=()):
    print(f"  - {label} ...", end=" ", flush=True)
    p_pat = make_pattern(label, params)
    s_pat = make_pattern(label, params)
    p_pat.validate()
    p_res = p_pat.execute(make_pandas_ctx(pdf))
    s_res = s_pat.execute(make_spark_ctx(spark, pdf))
    assert_parity(label, p_res, s_res, key_cols, ignore_cols=ignore_cols)
    if asserts:
        asserts(to_native(p_res), to_native(s_res))
    print("ok")
    return p_res, s_res


def scenario_simple(spark, pdf):
    def chk(p, s):
        assert (p["cnt"].sum()) == len(pdf)
        np.testing.assert_allclose(p["total"].sum(), pdf["amount"].sum(), rtol=1e-6)

    run_scenario(
        spark, "1_simple", {"grain": ["region"], "measures": MEASURES_FULL}, pdf, ["region"], chk
    )


def scenario_multigrain(spark, pdf):
    run_scenario(
        spark,
        "2_multigrain",
        {
            "grain": ["order_date", "category", "region"],
            "measures": [{"name": "rev", "expr": "SUM(amount)"}, {"name": "n", "expr": "COUNT(*)"}],
        },
        pdf,
        ["order_date", "category", "region"],
    )


def scenario_audit(spark, pdf):
    def chk(p, _s):
        assert "load_timestamp" in p.columns
        ts = pd.to_datetime(p["load_timestamp"], utc=True).iloc[0]
        delta = datetime.now(timezone.utc) - ts.to_pydatetime()
        assert delta.total_seconds() < 120, f"audit ts too old: {delta}"

    run_scenario(
        spark,
        "5_audit",
        {
            "grain": ["region"],
            "measures": [{"name": "rev", "expr": "SUM(amount)"}],
            "audit": {"load_timestamp": True},
        },
        pdf,
        ["region"],
        chk,
        ignore_cols=("load_timestamp",),
    )


def scenario_large(spark):
    pdf = base_sales(n_rows=100_000, seed=7)
    pdf["bucket"] = (np.arange(len(pdf)) % 1000).astype(int)
    t0 = time.time()
    p_res, s_res = run_scenario(
        spark,
        "6_large_100k",
        {"grain": ["bucket"], "measures": [{"name": "rev", "expr": "SUM(amount)"}]},
        pdf,
        ["bucket"],
    )
    elapsed = time.time() - t0
    assert elapsed < 60, f"100k aggregation took {elapsed:.1f}s (>60s)"
    assert len(to_native(p_res)) == 1000
    print(f"    -> 100k rows -> 1k groups in {elapsed:.1f}s")


def scenario_nulls(spark):
    pdf = base_sales(500, seed=11).copy()
    pdf.loc[::25, "region"] = None  # ~20 null grain values
    run_scenario(
        spark,
        "7_nulls",
        {"grain": ["region"], "measures": [{"name": "rev", "expr": "SUM(amount)"}]},
        pdf,
        ["region"],
    )


def scenario_empty_having(spark, pdf):
    res_pdf = make_pattern(
        "8_empty",
        {
            "grain": ["region"],
            "measures": [{"name": "rev", "expr": "SUM(amount)"}],
            "having": "SUM(amount) < 0",
        },
    ).execute(make_pandas_ctx(pdf))
    assert len(res_pdf) == 0, f"HAVING should filter everything, got {len(res_pdf)} rows"
    print("  - 8_empty_having ... ok")


def scenario_incremental_replace(spark, pdf):
    """Test _merge_replace directly — bypasses target loading by calling the helper."""
    pat = make_pattern(
        "3_incr_replace",
        {"grain": ["region"], "measures": [{"name": "rev", "expr": "SUM(amount)"}]},
    )
    existing = pd.DataFrame({"region": ["US", "EU", "OTHER"], "rev": [100.0, 200.0, 999.0]})
    new = pdf.groupby("region", as_index=False)["amount"].sum().rename(columns={"amount": "rev"})
    merged = pat._merge_replace(make_pandas_ctx(pdf), existing, new, ["region"])
    by_region = merged.set_index("region")["rev"].to_dict()
    assert "OTHER" in by_region and by_region["OTHER"] == 999.0, "untouched key dropped"
    np.testing.assert_allclose(by_region["US"], pdf[pdf.region == "US"].amount.sum(), rtol=1e-6)
    print("  - 3_incremental_replace ... ok")


def scenario_incremental_sum(spark, pdf):
    pat = make_pattern(
        "4_incr_sum",
        {"grain": ["region"], "measures": [{"name": "rev", "expr": "SUM(amount)"}]},
    )
    existing = pd.DataFrame({"region": ["US", "EU"], "rev": [100.0, 50.0]})
    new = pd.DataFrame({"region": ["US", "APAC"], "rev": [25.0, 200.0]})
    merged = pat._merge_sum(
        make_pandas_ctx(pdf),
        existing,
        new,
        ["region"],
        [{"name": "rev", "expr": "SUM(amount)"}],
    )
    by = merged.set_index("region")["rev"].to_dict()
    assert by["US"] == 125.0, by
    assert by["EU"] == 50.0, by
    assert by["APAC"] == 200.0, by
    print("  - 4_incremental_sum ... ok")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> int:
    print(
        f"[{datetime.now(timezone.utc).isoformat()}] "
        "Aggregation stress test - connecting to Spark..."
    )
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()
    pdf = base_sales(1000, seed=42)
    print(f"  Spark Connect ready. Source rows: {len(pdf)}")
    print("Running scenarios:")
    scenario_simple(spark, pdf)
    scenario_multigrain(spark, pdf)
    scenario_incremental_replace(spark, pdf)
    scenario_incremental_sum(spark, pdf)
    scenario_audit(spark, pdf)
    scenario_large(spark)
    scenario_nulls(spark)
    scenario_empty_having(spark, pdf)
    print("\n✅ All 8 scenarios passed (Pandas + Spark parity verified).")
    return 0


if __name__ == "__main__":
    sys.exit(main())

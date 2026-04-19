"""Tests for odibi.patterns.aggregation — Pandas paths only."""

import duckdb
import pandas as pd
import pytest

from odibi.config import NodeConfig
from odibi.context import EngineContext, PandasContext
from odibi.engine.pandas_engine import PandasEngine
from odibi.enums import EngineType
from odibi.patterns.aggregation import AggregationPattern


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_engine():
    return PandasEngine(config={})


def _make_pattern(params: dict, bypass_validation=False) -> AggregationPattern:
    engine = _make_engine()
    if bypass_validation:
        # Build a minimal mock config to bypass NodeConfig's Pydantic validation
        # which checks required params at construction time
        from unittest.mock import MagicMock

        config = MagicMock()
        config.name = "test_agg"
        config.params = params
        return AggregationPattern(engine=engine, config=config)
    config = NodeConfig(name="test_agg", transformer="aggregation", params=params)
    return AggregationPattern(engine=engine, config=config)


def _sql_executor(query, context):
    """DuckDB SQL executor that registers context DataFrames."""
    conn = duckdb.connect()
    for name in context.list_names():
        df = context.get(name)
        conn.register(name, df)
    return conn.execute(query).fetchdf()


def _make_context(df: pd.DataFrame) -> EngineContext:
    ctx = PandasContext()
    engine = _make_engine()
    return EngineContext(
        context=ctx,
        df=df,
        engine_type=EngineType.PANDAS,
        sql_executor=_sql_executor,
        engine=engine,
    )


# ---------------------------------------------------------------------------
# validate() tests
# ---------------------------------------------------------------------------


class TestValidate:
    def test_valid_config(self):
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
            }
        )
        p.validate()  # should not raise

    def test_missing_grain_raises(self):
        p = _make_pattern(
            {"measures": [{"name": "total", "expr": "SUM(amount)"}]}, bypass_validation=True
        )
        with pytest.raises(ValueError, match="grain"):
            p.validate()

    def test_empty_grain_raises(self):
        p = _make_pattern(
            {
                "grain": [],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
            },
            bypass_validation=True,
        )
        with pytest.raises(ValueError, match="grain"):
            p.validate()

    def test_none_grain_raises(self):
        p = _make_pattern(
            {
                "grain": None,
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
            },
            bypass_validation=True,
        )
        with pytest.raises(ValueError, match="grain"):
            p.validate()

    def test_missing_measures_raises(self):
        p = _make_pattern({"grain": ["region"]}, bypass_validation=True)
        with pytest.raises(ValueError, match="measures"):
            p.validate()

    def test_empty_measures_raises(self):
        p = _make_pattern({"grain": ["region"], "measures": []}, bypass_validation=True)
        with pytest.raises(ValueError, match="measures"):
            p.validate()

    def test_measure_not_dict_raises(self):
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": ["bad_measure"],
            },
            bypass_validation=True,
        )
        with pytest.raises(ValueError, match="must be a dict"):
            p.validate()

    def test_measure_missing_name_raises(self):
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"expr": "SUM(amount)"}],
            },
            bypass_validation=True,
        )
        with pytest.raises(ValueError, match="missing 'name'"):
            p.validate()

    def test_measure_missing_expr_raises(self):
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total"}],
            },
            bypass_validation=True,
        )
        with pytest.raises(ValueError, match="missing 'expr'"):
            p.validate()

    def test_valid_incremental_config(self):
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
                "incremental": {"timestamp_column": "ts", "merge_strategy": "sum"},
            }
        )
        p.validate()

    def test_incremental_default_merge_strategy(self):
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
                "incremental": {"timestamp_column": "ts"},
            }
        )
        p.validate()  # defaults to "replace"

    def test_incremental_missing_timestamp_column_raises(self):
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
                "incremental": {"merge_strategy": "sum"},
            }
        )
        with pytest.raises(ValueError, match="timestamp_column"):
            p.validate()

    def test_invalid_merge_strategy_raises(self):
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
                "incremental": {"timestamp_column": "ts", "merge_strategy": "invalid"},
            }
        )
        with pytest.raises(ValueError, match="merge_strategy"):
            p.validate()

    def test_all_valid_merge_strategies(self):
        for strategy in ("replace", "sum", "min", "max"):
            p = _make_pattern(
                {
                    "grain": ["region"],
                    "measures": [{"name": "total", "expr": "SUM(amount)"}],
                    "incremental": {"timestamp_column": "ts", "merge_strategy": strategy},
                }
            )
            p.validate()


# ---------------------------------------------------------------------------
# _aggregate_pandas() tests
# ---------------------------------------------------------------------------


class TestAggregatePandas:
    def test_basic_sum(self):
        df = pd.DataFrame(
            {
                "region": ["A", "A", "B", "B"],
                "amount": [10, 20, 30, 40],
            }
        )
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        assert len(result) == 2
        a_row = result[result["region"] == "A"]
        assert a_row["total"].iloc[0] == 30

    def test_multiple_measures(self):
        df = pd.DataFrame(
            {
                "region": ["A", "A", "B"],
                "amount": [10, 20, 30],
            }
        )
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [
                    {"name": "total", "expr": "SUM(amount)"},
                    {"name": "cnt", "expr": "COUNT(*)"},
                    {"name": "avg_amt", "expr": "AVG(amount)"},
                ],
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        assert len(result) == 2
        a_row = result[result["region"] == "A"]
        assert a_row["total"].iloc[0] == 30
        assert a_row["cnt"].iloc[0] == 2
        assert a_row["avg_amt"].iloc[0] == 15.0

    def test_multiple_grain_columns(self):
        df = pd.DataFrame(
            {
                "region": ["A", "A", "A", "B"],
                "product": ["X", "X", "Y", "X"],
                "amount": [10, 20, 30, 40],
            }
        )
        p = _make_pattern(
            {
                "grain": ["region", "product"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        assert len(result) == 3
        ax_row = result[(result["region"] == "A") & (result["product"] == "X")]
        assert ax_row["total"].iloc[0] == 30

    def test_having_clause(self):
        df = pd.DataFrame(
            {
                "region": ["A", "A", "B"],
                "amount": [10, 20, 30],
            }
        )
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [
                    {"name": "total", "expr": "SUM(amount)"},
                    {"name": "cnt", "expr": "COUNT(*)"},
                ],
                "having": "COUNT(*) > 1",
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        # Only A has count > 1
        assert len(result) == 1
        assert result["region"].iloc[0] == "A"

    def test_empty_source(self):
        df = pd.DataFrame(
            {"region": pd.Series([], dtype="str"), "amount": pd.Series([], dtype="float")}
        )
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Incremental merge tests
# ---------------------------------------------------------------------------


class TestIncrementalMerge:
    def _write_existing(self, tmp_path, existing_df):
        path = str(tmp_path / "existing.parquet")
        existing_df.to_parquet(path, index=False)
        return path

    def test_no_existing_target_returns_new(self, tmp_path):
        """When target doesn't exist, returns new agg as-is."""
        target_path = str(tmp_path / "nonexistent.parquet")
        df = pd.DataFrame(
            {
                "region": ["A", "A", "B"],
                "amount": [10, 20, 30],
            }
        )
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
                "incremental": {"timestamp_column": "ts", "merge_strategy": "replace"},
                "target": target_path,
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        assert len(result) == 2

    def test_replace_strategy(self, tmp_path):
        existing = pd.DataFrame(
            {
                "region": ["A", "B"],
                "total": [100, 200],
            }
        )
        target_path = self._write_existing(tmp_path, existing)

        df = pd.DataFrame(
            {
                "region": ["A", "A"],
                "amount": [10, 20],
            }
        )
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
                "incremental": {"timestamp_column": "ts", "merge_strategy": "replace"},
                "target": target_path,
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        # A replaced with 30, B unchanged at 200
        assert len(result) == 2
        a_row = result[result["region"] == "A"]
        b_row = result[result["region"] == "B"]
        assert a_row["total"].iloc[0] == 30
        assert b_row["total"].iloc[0] == 200

    def test_sum_strategy(self, tmp_path):
        existing = pd.DataFrame(
            {
                "region": ["A", "B"],
                "total": [100, 200],
            }
        )
        target_path = self._write_existing(tmp_path, existing)

        df = pd.DataFrame(
            {
                "region": ["A", "A"],
                "amount": [10, 20],
            }
        )
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
                "incremental": {"timestamp_column": "ts", "merge_strategy": "sum"},
                "target": target_path,
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        a_row = result[result["region"] == "A"]
        b_row = result[result["region"] == "B"]
        assert a_row["total"].iloc[0] == 130  # 100 + 30
        assert b_row["total"].iloc[0] == 200  # 200 + 0

    def test_min_strategy(self, tmp_path):
        existing = pd.DataFrame(
            {
                "region": ["A", "B"],
                "total": [100, 200],
            }
        )
        target_path = self._write_existing(tmp_path, existing)

        df = pd.DataFrame(
            {
                "region": ["A"],
                "amount": [50],
            }
        )
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
                "incremental": {"timestamp_column": "ts", "merge_strategy": "min"},
                "target": target_path,
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        a_row = result[result["region"] == "A"]
        assert a_row["total"].iloc[0] == 50  # min(100, 50)

    def test_max_strategy(self, tmp_path):
        existing = pd.DataFrame(
            {
                "region": ["A", "B"],
                "total": [100, 200],
            }
        )
        target_path = self._write_existing(tmp_path, existing)

        df = pd.DataFrame(
            {
                "region": ["A"],
                "amount": [50],
            }
        )
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
                "incremental": {"timestamp_column": "ts", "merge_strategy": "max"},
                "target": target_path,
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        a_row = result[result["region"] == "A"]
        assert a_row["total"].iloc[0] == 100  # max(100, 50)

    def test_non_overlapping_keys(self, tmp_path):
        existing = pd.DataFrame(
            {
                "region": ["A"],
                "total": [100],
            }
        )
        target_path = self._write_existing(tmp_path, existing)

        df = pd.DataFrame(
            {
                "region": ["B", "B"],
                "amount": [10, 20],
            }
        )
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
                "incremental": {"timestamp_column": "ts", "merge_strategy": "replace"},
                "target": target_path,
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        assert len(result) == 2
        a_row = result[result["region"] == "A"]
        b_row = result[result["region"] == "B"]
        assert a_row["total"].iloc[0] == 100
        assert b_row["total"].iloc[0] == 30

    def test_sum_multiple_measures(self, tmp_path):
        existing = pd.DataFrame(
            {
                "region": ["A"],
                "total": [100],
                "cnt": [5],
            }
        )
        target_path = self._write_existing(tmp_path, existing)

        df = pd.DataFrame(
            {
                "region": ["A", "A"],
                "amount": [10, 20],
            }
        )
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [
                    {"name": "total", "expr": "SUM(amount)"},
                    {"name": "cnt", "expr": "COUNT(*)"},
                ],
                "incremental": {"timestamp_column": "ts", "merge_strategy": "sum"},
                "target": target_path,
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        a_row = result[result["region"] == "A"]
        assert a_row["total"].iloc[0] == 130  # 100 + 30
        assert a_row["cnt"].iloc[0] == 7  # 5 + 2


# ---------------------------------------------------------------------------
# Audit column tests
# ---------------------------------------------------------------------------


class TestAuditColumns:
    def test_load_timestamp_added(self):
        df = pd.DataFrame(
            {
                "region": ["A", "B"],
                "amount": [10, 20],
            }
        )
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
                "audit": {"load_timestamp": True},
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        assert "load_timestamp" in result.columns

    def test_source_system_added(self):
        df = pd.DataFrame(
            {
                "region": ["A", "B"],
                "amount": [10, 20],
            }
        )
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
                "audit": {"source_system": "erp"},
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        assert "source_system" in result.columns
        assert result["source_system"].iloc[0] == "erp"

    def test_both_audit_columns(self):
        df = pd.DataFrame(
            {
                "region": ["A"],
                "amount": [10],
            }
        )
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
                "audit": {"load_timestamp": True, "source_system": "pos"},
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        assert "load_timestamp" in result.columns
        assert "source_system" in result.columns

    def test_no_audit_config(self):
        df = pd.DataFrame(
            {
                "region": ["A"],
                "amount": [10],
            }
        )
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        assert "load_timestamp" not in result.columns
        assert "source_system" not in result.columns


# ---------------------------------------------------------------------------
# End-to-end tests
# ---------------------------------------------------------------------------


class TestEndToEnd:
    def test_aggregate_only(self):
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01", "2024-01-02"],
                "product": ["A", "B", "A"],
                "amount": [100, 200, 150],
            }
        )
        p = _make_pattern(
            {
                "grain": ["date", "product"],
                "measures": [
                    {"name": "total_amount", "expr": "SUM(amount)"},
                    {"name": "txn_count", "expr": "COUNT(*)"},
                ],
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        assert len(result) == 3

    def test_full_pipeline_with_incremental_and_audit(self, tmp_path):
        existing = pd.DataFrame(
            {
                "region": ["A"],
                "total": [50],
            }
        )
        target_path = str(tmp_path / "target.parquet")
        existing.to_parquet(target_path, index=False)

        df = pd.DataFrame(
            {
                "region": ["A", "B"],
                "amount": [10, 20],
            }
        )
        p = _make_pattern(
            {
                "grain": ["region"],
                "measures": [{"name": "total", "expr": "SUM(amount)"}],
                "incremental": {"timestamp_column": "ts", "merge_strategy": "sum"},
                "target": target_path,
                "audit": {"load_timestamp": True, "source_system": "test"},
            }
        )
        ctx = _make_context(df)
        result = p.execute(ctx)
        assert "load_timestamp" in result.columns
        assert "source_system" in result.columns
        a_row = result[result["region"] == "A"]
        assert a_row["total"].iloc[0] == 60  # 50 + 10

"""Tests for odibi.validation.engine.Validator — Pandas and Polars paths."""

import pandas as pd
import pytest

pl = pytest.importorskip("polars")
from datetime import datetime, timedelta, timezone

from odibi.validation.engine import Validator
from odibi.config import (
    ValidationConfig,
    ContractSeverity,
    NotNullTest,
    UniqueTest,
    AcceptedValuesTest,
    RowCountTest,
    RangeTest,
    RegexMatchTest,
    CustomSQLTest,
    SchemaContract,
    FreshnessContract,
)


@pytest.fixture
def validator():
    return Validator()


# ── Pandas NOT_NULL ──────────────────────────────────────────────


def test_pandas_not_null_pass(validator):
    df = pd.DataFrame({"a": [1, 2, 3]})
    cfg = ValidationConfig(tests=[NotNullTest(columns=["a"])])
    assert validator.validate(df, cfg) == []


def test_pandas_not_null_fail(validator):
    df = pd.DataFrame({"a": [1, None, 3]})
    cfg = ValidationConfig(tests=[NotNullTest(columns=["a"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "NULLs" in errors[0]


def test_pandas_not_null_multiple_columns(validator):
    df = pd.DataFrame({"a": [1, None, 3], "b": [None, 2, 3]})
    cfg = ValidationConfig(tests=[NotNullTest(columns=["a", "b"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 2


def test_pandas_not_null_missing_column(validator):
    df = pd.DataFrame({"a": [1, 2, 3]})
    cfg = ValidationConfig(tests=[NotNullTest(columns=["missing"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "not found" in errors[0]


def test_pandas_not_null_warn_severity(validator):
    df = pd.DataFrame({"a": [1, None, 3]})
    cfg = ValidationConfig(tests=[NotNullTest(columns=["a"], on_fail=ContractSeverity.WARN)])
    errors = validator.validate(df, cfg)
    assert errors == []


def test_pandas_not_null_fail_fast(validator):
    df = pd.DataFrame({"a": [None], "b": [None]})
    cfg = ValidationConfig(tests=[NotNullTest(columns=["a", "b"])], fail_fast=True)
    errors = validator.validate(df, cfg)
    assert len(errors) == 1


# ── Pandas UNIQUE ────────────────────────────────────────────────


def test_pandas_unique_pass(validator):
    df = pd.DataFrame({"id": [1, 2, 3]})
    cfg = ValidationConfig(tests=[UniqueTest(columns=["id"])])
    assert validator.validate(df, cfg) == []


def test_pandas_unique_fail(validator):
    df = pd.DataFrame({"id": [1, 1, 3]})
    cfg = ValidationConfig(tests=[UniqueTest(columns=["id"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "not unique" in errors[0]


def test_pandas_unique_multi_column(validator):
    df = pd.DataFrame({"a": [1, 1, 2], "b": [10, 10, 20]})
    cfg = ValidationConfig(tests=[UniqueTest(columns=["a", "b"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1


def test_pandas_unique_missing_column(validator):
    df = pd.DataFrame({"a": [1, 2]})
    cfg = ValidationConfig(tests=[UniqueTest(columns=["a", "missing"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "not found" in errors[0]


# ── Pandas ACCEPTED_VALUES ───────────────────────────────────────


def test_pandas_accepted_values_pass(validator):
    df = pd.DataFrame({"status": ["A", "B", "A"]})
    cfg = ValidationConfig(tests=[AcceptedValuesTest(column="status", values=["A", "B"])])
    assert validator.validate(df, cfg) == []


def test_pandas_accepted_values_fail(validator):
    df = pd.DataFrame({"status": ["A", "X", "B"]})
    cfg = ValidationConfig(tests=[AcceptedValuesTest(column="status", values=["A", "B"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "invalid values" in errors[0]


def test_pandas_accepted_values_missing_column(validator):
    df = pd.DataFrame({"a": [1]})
    cfg = ValidationConfig(tests=[AcceptedValuesTest(column="missing", values=["A"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "not found" in errors[0]


# ── Pandas ROW_COUNT ─────────────────────────────────────────────


def test_pandas_row_count_pass(validator):
    df = pd.DataFrame({"a": range(10)})
    cfg = ValidationConfig(tests=[RowCountTest(min=5, max=20)])
    assert validator.validate(df, cfg) == []


def test_pandas_row_count_below_min(validator):
    df = pd.DataFrame({"a": [1, 2]})
    cfg = ValidationConfig(tests=[RowCountTest(min=5)])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "< min" in errors[0]


def test_pandas_row_count_above_max(validator):
    df = pd.DataFrame({"a": range(100)})
    cfg = ValidationConfig(tests=[RowCountTest(max=50)])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "> max" in errors[0]


# ── Pandas RANGE ─────────────────────────────────────────────────


def test_pandas_range_pass(validator):
    df = pd.DataFrame({"val": [10, 20, 30]})
    cfg = ValidationConfig(tests=[RangeTest(column="val", min=0, max=50)])
    assert validator.validate(df, cfg) == []


def test_pandas_range_below_min(validator):
    df = pd.DataFrame({"val": [-5, 10, 20]})
    cfg = ValidationConfig(tests=[RangeTest(column="val", min=0)])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "out of range" in errors[0]


def test_pandas_range_above_max(validator):
    df = pd.DataFrame({"val": [10, 200]})
    cfg = ValidationConfig(tests=[RangeTest(column="val", max=100)])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "out of range" in errors[0]


def test_pandas_range_missing_column(validator):
    df = pd.DataFrame({"a": [1]})
    cfg = ValidationConfig(tests=[RangeTest(column="missing", min=0)])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "not found" in errors[0]


# ── Pandas REGEX_MATCH ───────────────────────────────────────────


def test_pandas_regex_pass(validator):
    df = pd.DataFrame({"email": ["a@b.com", "c@d.com"]})
    cfg = ValidationConfig(tests=[RegexMatchTest(column="email", pattern=r".*@.*\.com")])
    assert validator.validate(df, cfg) == []


def test_pandas_regex_fail(validator):
    df = pd.DataFrame({"code": ["ABC", "123", "XY"]})
    cfg = ValidationConfig(tests=[RegexMatchTest(column="code", pattern=r"^[0-9]+$")])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "does not match" in errors[0]


def test_pandas_regex_missing_column(validator):
    df = pd.DataFrame({"a": [1]})
    cfg = ValidationConfig(tests=[RegexMatchTest(column="missing", pattern=r".*")])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "not found" in errors[0]


# ── Pandas CUSTOM_SQL ────────────────────────────────────────────


def test_pandas_custom_sql_pass(validator):
    df = pd.DataFrame({"a": [1, 2, 3]})
    cfg = ValidationConfig(tests=[CustomSQLTest(condition="a > 0")])
    assert validator.validate(df, cfg) == []


def test_pandas_custom_sql_fail(validator):
    df = pd.DataFrame({"a": [1, -1, 3]})
    cfg = ValidationConfig(tests=[CustomSQLTest(condition="a > 0")])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "invalid rows" in errors[0]


def test_pandas_custom_sql_error(validator):
    df = pd.DataFrame({"a": [1]})
    cfg = ValidationConfig(tests=[CustomSQLTest(condition="nonexistent_col > 0")])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "Failed to execute" in errors[0]


# ── Pandas SCHEMA ────────────────────────────────────────────────


def test_pandas_schema_strict_pass(validator):
    df = pd.DataFrame({"a": [1], "b": [2]})
    ctx = {"columns": {"a": "int", "b": "int"}}
    cfg = ValidationConfig(tests=[SchemaContract(strict=True)])
    assert validator.validate(df, cfg, context=ctx) == []


def test_pandas_schema_strict_fail(validator):
    df = pd.DataFrame({"a": [1], "b": [2], "extra": [3]})
    ctx = {"columns": {"a": "int", "b": "int"}}
    cfg = ValidationConfig(tests=[SchemaContract(strict=True)])
    errors = validator.validate(df, cfg, context=ctx)
    assert len(errors) == 1
    assert "Schema mismatch" in errors[0]


def test_pandas_schema_non_strict_pass(validator):
    df = pd.DataFrame({"a": [1], "b": [2], "extra": [3]})
    ctx = {"columns": {"a": "int", "b": "int"}}
    cfg = ValidationConfig(tests=[SchemaContract(strict=False)])
    assert validator.validate(df, cfg, context=ctx) == []


def test_pandas_schema_non_strict_fail(validator):
    df = pd.DataFrame({"a": [1]})
    ctx = {"columns": {"a": "int", "b": "int"}}
    cfg = ValidationConfig(tests=[SchemaContract(strict=False)])
    errors = validator.validate(df, cfg, context=ctx)
    assert len(errors) == 1
    assert "Missing columns" in errors[0]


def test_pandas_schema_no_context(validator):
    df = pd.DataFrame({"a": [1]})
    cfg = ValidationConfig(tests=[SchemaContract()])
    assert validator.validate(df, cfg) == []


# ── Pandas FRESHNESS ─────────────────────────────────────────────


def test_pandas_freshness_fresh_data(validator):
    now = datetime.now()
    df = pd.DataFrame({"updated_at": [now - timedelta(minutes=5)]})
    cfg = ValidationConfig(tests=[FreshnessContract(max_age="1h")])
    assert validator.validate(df, cfg) == []


def test_pandas_freshness_stale_data(validator):
    old = datetime.now() - timedelta(days=10)
    df = pd.DataFrame({"updated_at": [old]})
    cfg = ValidationConfig(tests=[FreshnessContract(max_age="1d")])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "too old" in errors[0]


def test_pandas_freshness_missing_column(validator):
    df = pd.DataFrame({"a": [1]})
    cfg = ValidationConfig(tests=[FreshnessContract(max_age="1h")])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "not found" in errors[0]


def test_pandas_freshness_minutes_suffix(validator):
    old = datetime.now() - timedelta(hours=2)
    df = pd.DataFrame({"updated_at": [old]})
    cfg = ValidationConfig(tests=[FreshnessContract(max_age="30m")])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "too old" in errors[0]


def test_pandas_freshness_days_suffix(validator):
    old = datetime.now() - timedelta(days=10)
    df = pd.DataFrame({"updated_at": [old]})
    cfg = ValidationConfig(tests=[FreshnessContract(max_age="7d")])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1


def test_pandas_freshness_string_timestamp(validator):
    """Freshness with string column that can be parsed to datetime."""
    old = (datetime.now() - timedelta(days=10)).isoformat()
    df = pd.DataFrame({"updated_at": [old]})
    cfg = ValidationConfig(tests=[FreshnessContract(max_age="1d")])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "too old" in errors[0]


# ── Pandas edge cases ────────────────────────────────────────────


def test_pandas_empty_dataframe(validator):
    df = pd.DataFrame({"a": pd.Series([], dtype="float64")})
    cfg = ValidationConfig(tests=[NotNullTest(columns=["a"])])
    assert validator.validate(df, cfg) == []


def test_pandas_multiple_failing_tests(validator):
    df = pd.DataFrame({"a": [None], "b": [1]})
    cfg = ValidationConfig(
        tests=[
            NotNullTest(columns=["a"]),
            RowCountTest(min=10),
        ]
    )
    errors = validator.validate(df, cfg)
    assert len(errors) == 2


def test_pandas_fail_fast_stops_early(validator):
    df = pd.DataFrame({"a": range(3)})
    cfg = ValidationConfig(
        tests=[
            RowCountTest(min=100),
            RowCountTest(max=1),
        ],
        fail_fast=True,
    )
    errors = validator.validate(df, cfg)
    assert len(errors) == 1


def test_pandas_warn_severity_no_error(validator):
    df = pd.DataFrame({"a": [1, 1, 2]})
    cfg = ValidationConfig(tests=[UniqueTest(columns=["a"], on_fail=ContractSeverity.WARN)])
    errors = validator.validate(df, cfg)
    assert errors == []


# ── Polars NOT_NULL ──────────────────────────────────────────────


def test_polars_not_null_pass(validator):
    df = pl.DataFrame({"a": [1, 2, 3]})
    cfg = ValidationConfig(tests=[NotNullTest(columns=["a"])])
    assert validator.validate(df, cfg) == []


def test_polars_not_null_fail(validator):
    df = pl.DataFrame({"a": [1, None, 3]})
    cfg = ValidationConfig(tests=[NotNullTest(columns=["a"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "NULLs" in errors[0]


def test_polars_not_null_lazyframe(validator):
    df = pl.LazyFrame({"a": [1, None, 3]})
    cfg = ValidationConfig(tests=[NotNullTest(columns=["a"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "NULLs" in errors[0]


def test_polars_not_null_lazyframe_pass(validator):
    df = pl.LazyFrame({"a": [1, 2, 3]})
    cfg = ValidationConfig(tests=[NotNullTest(columns=["a"])])
    assert validator.validate(df, cfg) == []


# ── Polars UNIQUE ────────────────────────────────────────────────


def test_polars_unique_pass(validator):
    df = pl.DataFrame({"id": [1, 2, 3]})
    cfg = ValidationConfig(tests=[UniqueTest(columns=["id"])])
    assert validator.validate(df, cfg) == []


def test_polars_unique_fail(validator):
    df = pl.DataFrame({"id": [1, 1, 3]})
    cfg = ValidationConfig(tests=[UniqueTest(columns=["id"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "not unique" in errors[0]


def test_polars_unique_lazy(validator):
    df = pl.LazyFrame({"id": [1, 1, 3]})
    cfg = ValidationConfig(tests=[UniqueTest(columns=["id"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1


def test_polars_unique_missing_columns(validator):
    df = pl.DataFrame({"a": [1, 2]})
    cfg = ValidationConfig(tests=[UniqueTest(columns=["a", "missing"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "not found" in errors[0]


# ── Polars ACCEPTED_VALUES ───────────────────────────────────────


def test_polars_accepted_values_pass(validator):
    df = pl.DataFrame({"status": ["A", "B"]})
    cfg = ValidationConfig(tests=[AcceptedValuesTest(column="status", values=["A", "B"])])
    assert validator.validate(df, cfg) == []


def test_polars_accepted_values_fail(validator):
    df = pl.DataFrame({"status": ["A", "X"]})
    cfg = ValidationConfig(tests=[AcceptedValuesTest(column="status", values=["A", "B"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "invalid values" in errors[0]


def test_polars_accepted_values_lazy(validator):
    df = pl.LazyFrame({"status": ["A", "X"]})
    cfg = ValidationConfig(tests=[AcceptedValuesTest(column="status", values=["A", "B"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1


def test_polars_accepted_values_missing_column(validator):
    df = pl.DataFrame({"a": [1]})
    cfg = ValidationConfig(tests=[AcceptedValuesTest(column="missing", values=["A"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "not found" in errors[0]


# ── Polars ROW_COUNT ─────────────────────────────────────────────


def test_polars_row_count_pass(validator):
    df = pl.DataFrame({"a": list(range(10))})
    cfg = ValidationConfig(tests=[RowCountTest(min=5, max=20)])
    assert validator.validate(df, cfg) == []


def test_polars_row_count_below_min(validator):
    df = pl.DataFrame({"a": [1, 2]})
    cfg = ValidationConfig(tests=[RowCountTest(min=5)])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "< min" in errors[0]


def test_polars_row_count_above_max(validator):
    df = pl.DataFrame({"a": list(range(100))})
    cfg = ValidationConfig(tests=[RowCountTest(max=50)])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "> max" in errors[0]


def test_polars_row_count_lazy(validator):
    df = pl.LazyFrame({"a": [1, 2]})
    cfg = ValidationConfig(tests=[RowCountTest(min=5)])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1


# ── Polars RANGE ─────────────────────────────────────────────────


def test_polars_range_pass(validator):
    df = pl.DataFrame({"val": [10, 20, 30]})
    cfg = ValidationConfig(tests=[RangeTest(column="val", min=0, max=50)])
    assert validator.validate(df, cfg) == []


def test_polars_range_fail(validator):
    df = pl.DataFrame({"val": [-5, 10, 200]})
    cfg = ValidationConfig(tests=[RangeTest(column="val", min=0, max=100)])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "out of range" in errors[0]


def test_polars_range_lazy(validator):
    df = pl.LazyFrame({"val": [-5, 10]})
    cfg = ValidationConfig(tests=[RangeTest(column="val", min=0)])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1


def test_polars_range_missing_column(validator):
    df = pl.DataFrame({"a": [1]})
    cfg = ValidationConfig(tests=[RangeTest(column="missing", min=0)])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "not found" in errors[0]


# ── Polars REGEX_MATCH ───────────────────────────────────────────


def test_polars_regex_pass(validator):
    df = pl.DataFrame({"code": ["ABC", "DEF"]})
    cfg = ValidationConfig(tests=[RegexMatchTest(column="code", pattern=r"^[A-Z]+$")])
    assert validator.validate(df, cfg) == []


def test_polars_regex_fail(validator):
    df = pl.DataFrame({"code": ["ABC", "123"]})
    cfg = ValidationConfig(tests=[RegexMatchTest(column="code", pattern=r"^[A-Z]+$")])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "does not match" in errors[0]


def test_polars_regex_lazy(validator):
    df = pl.LazyFrame({"code": ["ABC", "123"]})
    cfg = ValidationConfig(tests=[RegexMatchTest(column="code", pattern=r"^[A-Z]+$")])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1


def test_polars_regex_missing_column(validator):
    df = pl.DataFrame({"a": ["x"]})
    cfg = ValidationConfig(tests=[RegexMatchTest(column="missing", pattern=r".*")])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "not found" in errors[0]


# ── Polars CUSTOM_SQL ────────────────────────────────────────────


def test_polars_custom_sql_skipped(validator):
    """CUSTOM_SQL is not supported in Polars; should be skipped."""
    df = pl.DataFrame({"a": [1, -1]})
    cfg = ValidationConfig(tests=[CustomSQLTest(condition="a > 0")])
    errors = validator.validate(df, cfg)
    assert errors == []


# ── Polars SCHEMA ────────────────────────────────────────────────


def test_polars_schema_strict_pass(validator):
    df = pl.DataFrame({"a": [1], "b": [2]})
    ctx = {"columns": {"a": "int", "b": "int"}}
    cfg = ValidationConfig(tests=[SchemaContract(strict=True)])
    assert validator.validate(df, cfg, context=ctx) == []


def test_polars_schema_strict_fail(validator):
    df = pl.DataFrame({"a": [1], "b": [2], "extra": [3]})
    ctx = {"columns": {"a": "int", "b": "int"}}
    cfg = ValidationConfig(tests=[SchemaContract(strict=True)])
    errors = validator.validate(df, cfg, context=ctx)
    assert len(errors) == 1
    assert "Schema mismatch" in errors[0]


def test_polars_schema_non_strict_pass(validator):
    df = pl.DataFrame({"a": [1], "b": [2], "extra": [3]})
    ctx = {"columns": {"a": "int", "b": "int"}}
    cfg = ValidationConfig(tests=[SchemaContract(strict=False)])
    assert validator.validate(df, cfg, context=ctx) == []


# ── Polars FRESHNESS ─────────────────────────────────────────────


def test_polars_freshness_fresh(validator):
    now = datetime.now(timezone.utc)
    df = pl.DataFrame({"updated_at": [now - timedelta(minutes=5)]})
    cfg = ValidationConfig(tests=[FreshnessContract(max_age="1h")])
    assert validator.validate(df, cfg) == []


def test_polars_freshness_stale(validator):
    old = datetime.now() - timedelta(days=10)
    df = pl.DataFrame({"updated_at": [old]})
    cfg = ValidationConfig(tests=[FreshnessContract(max_age="1d")])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "too old" in errors[0]


def test_polars_freshness_missing_column(validator):
    df = pl.DataFrame({"a": [1]})
    cfg = ValidationConfig(tests=[FreshnessContract(max_age="1h")])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
    assert "not found" in errors[0]


def test_polars_freshness_lazy(validator):
    old = datetime.now() - timedelta(days=10)
    df = pl.LazyFrame({"updated_at": [old]})
    cfg = ValidationConfig(tests=[FreshnessContract(max_age="1d")])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1


# ── Polars edge cases ────────────────────────────────────────────


def test_polars_empty_dataframe(validator):
    df = pl.DataFrame({"a": pl.Series([], dtype=pl.Int64)})
    cfg = ValidationConfig(tests=[NotNullTest(columns=["a"])])
    assert validator.validate(df, cfg) == []


def test_polars_fail_fast(validator):
    df = pl.DataFrame({"a": list(range(3))})
    cfg = ValidationConfig(
        tests=[
            RowCountTest(min=100),
            RowCountTest(max=1),
        ],
        fail_fast=True,
    )
    errors = validator.validate(df, cfg)
    assert len(errors) == 1


def test_polars_warn_severity(validator):
    df = pl.DataFrame({"id": [1, 1, 2]})
    cfg = ValidationConfig(tests=[UniqueTest(columns=["id"], on_fail=ContractSeverity.WARN)])
    assert validator.validate(df, cfg) == []

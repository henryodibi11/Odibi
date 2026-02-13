"""
Extended unit tests for odibi.validation.engine.Validator.

Covers:
- _handle_failure severity logic (WARN, FAIL, QUARANTINE)
- _validate_pandas: FRESHNESS, REGEX_MATCH, CUSTOM_SQL, NOT_NULL, fail_fast, SCHEMA
- _validate_polars: lazy/eager for FRESHNESS, SCHEMA, ROW_COUNT, NOT_NULL,
                    UNIQUE, ACCEPTED_VALUES, RANGE, REGEX_MATCH, fail_fast
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pandas as pd
import polars as pl
import pytest

from odibi.config import (
    AcceptedValuesTest,
    ContractSeverity,
    CustomSQLTest,
    FreshnessContract,
    NotNullTest,
    RangeTest,
    RegexMatchTest,
    RowCountTest,
    SchemaContract,
    UniqueTest,
    ValidationConfig,
)
from odibi.validation.engine import Validator


@pytest.fixture()
def validator():
    return Validator()


# ──────────────────────────────────────────────
# _handle_failure severity tests
# ──────────────────────────────────────────────


class TestHandleFailure:
    def test_warn_returns_none(self, validator):
        test = SimpleNamespace(on_fail=ContractSeverity.WARN, type="test")
        result = validator._handle_failure("some warning", test)
        assert result is None

    def test_fail_returns_message(self, validator):
        test = SimpleNamespace(on_fail=ContractSeverity.FAIL, type="test")
        result = validator._handle_failure("bad data", test)
        assert result == "bad data"

    def test_quarantine_returns_message(self, validator):
        test = SimpleNamespace(on_fail=ContractSeverity.QUARANTINE, type="test")
        result = validator._handle_failure("quarantine row", test)
        assert result == "quarantine row"

    def test_default_severity_is_fail(self, validator):
        test = SimpleNamespace(type="test")
        result = validator._handle_failure("default fail", test)
        assert result == "default fail"


# ──────────────────────────────────────────────
# _validate_pandas tests
# ──────────────────────────────────────────────


class TestValidatePandasFreshness:
    def test_freshness_datetime_col_fresh(self, validator):
        now = datetime.now(timezone.utc)
        ts = pd.Timestamp(now - timedelta(hours=1))
        df = pd.DataFrame({"updated_at": [ts]})
        config = ValidationConfig(tests=[FreshnessContract(column="updated_at", max_age="24h")])
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_freshness_datetime_col_stale(self, validator):
        old = datetime.now(timezone.utc) - timedelta(days=5)
        ts = pd.Timestamp(old)
        df = pd.DataFrame({"updated_at": [ts]})
        config = ValidationConfig(tests=[FreshnessContract(column="updated_at", max_age="24h")])
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "Data too old" in failures[0]

    def test_freshness_string_col_auto_convert_tz_naive(self, validator):
        """String col auto-converts to tz-naive Timestamp; subtraction with
        tz-aware now() raises TypeError which is uncaught, so no freshness
        failure is produced (max_ts stays as NaT-equivalent path)."""
        old = datetime.now(timezone.utc) - timedelta(days=5)
        ts_str = old.strftime("%Y-%m-%d %H:%M:%S")
        df = pd.DataFrame({"updated_at": [ts_str]})
        config = ValidationConfig(tests=[FreshnessContract(column="updated_at", max_age="24h")])
        # tz-naive Timestamp vs tz-aware now() → TypeError bubbles up
        with pytest.raises(TypeError, match="tz-naive and tz-aware"):
            validator.validate(df, config)

    def test_freshness_missing_column(self, validator):
        df = pd.DataFrame({"other_col": [1, 2, 3]})
        config = ValidationConfig(tests=[FreshnessContract(column="updated_at", max_age="24h")])
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "not found" in failures[0]

    def test_freshness_max_age_days(self, validator):
        old = datetime.now(timezone.utc) - timedelta(days=10)
        ts = pd.Timestamp(old)
        df = pd.DataFrame({"ts": [ts]})
        config = ValidationConfig(tests=[FreshnessContract(column="ts", max_age="7d")])
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "Data too old" in failures[0]

    def test_freshness_max_age_minutes(self, validator):
        old = datetime.now(timezone.utc) - timedelta(minutes=120)
        ts = pd.Timestamp(old)
        df = pd.DataFrame({"ts": [ts]})
        config = ValidationConfig(tests=[FreshnessContract(column="ts", max_age="60m")])
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "Data too old" in failures[0]


class TestValidatePandasRegex:
    def test_regex_match(self, validator):
        df = pd.DataFrame({"code": ["AB-123", "CD-456"]})
        config = ValidationConfig(
            tests=[RegexMatchTest(column="code", pattern=r"^[A-Z]{2}-\d{3}$")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_regex_mismatch(self, validator):
        df = pd.DataFrame({"code": ["AB-123", "bad"]})
        config = ValidationConfig(
            tests=[RegexMatchTest(column="code", pattern=r"^[A-Z]{2}-\d{3}$")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "does not match pattern" in failures[0]

    def test_regex_empty_after_dropna(self, validator):
        df = pd.DataFrame({"code": [None, None]})
        config = ValidationConfig(tests=[RegexMatchTest(column="code", pattern=r"^[A-Z]+$")])
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_regex_missing_column(self, validator):
        df = pd.DataFrame({"other": [1]})
        config = ValidationConfig(tests=[RegexMatchTest(column="code", pattern=r"^[A-Z]+$")])
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "not found" in failures[0]


class TestValidatePandasCustomSQL:
    def test_custom_sql_valid(self, validator):
        df = pd.DataFrame({"amount": [10, 20, 30]})
        config = ValidationConfig(tests=[CustomSQLTest(condition="amount > 0")])
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_custom_sql_invalid_rows(self, validator):
        df = pd.DataFrame({"amount": [10, -5, 30]})
        config = ValidationConfig(tests=[CustomSQLTest(condition="amount > 0")])
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "Custom check" in failures[0]

    def test_custom_sql_bad_expression(self, validator):
        df = pd.DataFrame({"amount": [10]})
        config = ValidationConfig(tests=[CustomSQLTest(condition="INVALID SYNTAX @@@")])
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "Failed to execute" in failures[0]


class TestValidatePandasNotNull:
    def test_not_null_missing_column(self, validator):
        df = pd.DataFrame({"a": [1, 2]})
        config = ValidationConfig(tests=[NotNullTest(columns=["missing_col"])])
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "not found" in failures[0]


class TestValidatePandasFailFast:
    def test_fail_fast_exits_after_first(self, validator):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        config = ValidationConfig(
            fail_fast=True,
            tests=[
                RowCountTest(min=100),
                RangeTest(column="a", min=10, max=20),
            ],
        )
        failures = validator.validate(df, config)
        assert len(failures) == 1

    def test_no_fail_fast_collects_all(self, validator):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        config = ValidationConfig(
            fail_fast=False,
            tests=[
                RowCountTest(min=100),
                RangeTest(column="a", min=10, max=20),
            ],
        )
        failures = validator.validate(df, config)
        assert len(failures) == 2


class TestValidatePandasSchema:
    def test_schema_strict_match(self, validator):
        df = pd.DataFrame({"a": [1], "b": [2]})
        config = ValidationConfig(tests=[SchemaContract(strict=True)])
        context = {"columns": {"a": "int", "b": "int"}}
        failures = validator.validate(df, config, context)
        assert len(failures) == 0

    def test_schema_strict_mismatch(self, validator):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        config = ValidationConfig(tests=[SchemaContract(strict=True)])
        context = {"columns": {"a": "int", "b": "int"}}
        failures = validator.validate(df, config, context)
        assert len(failures) == 1
        assert "Schema mismatch" in failures[0]

    def test_schema_non_strict_ok_with_extra(self, validator):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        config = ValidationConfig(tests=[SchemaContract(strict=False)])
        context = {"columns": {"a": "int", "b": "int"}}
        failures = validator.validate(df, config, context)
        assert len(failures) == 0

    def test_schema_non_strict_missing(self, validator):
        df = pd.DataFrame({"a": [1]})
        config = ValidationConfig(tests=[SchemaContract(strict=False)])
        context = {"columns": {"a": "int", "b": "int"}}
        failures = validator.validate(df, config, context)
        assert len(failures) == 1
        assert "Missing columns" in failures[0]


# ──────────────────────────────────────────────
# _validate_polars tests
# ──────────────────────────────────────────────


def _polars_df():
    """Build a simple Polars eager DataFrame."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Charlie", None],
            "age": [25, 30, 150, 20],
            "code": ["AB-1", "CD-2", "bad", "EF-3"],
            "status": ["active", "inactive", "pending", "deleted"],
        }
    )


@pytest.fixture(params=["eager", "lazy"], ids=["eager", "lazy"])
def polars_df(request):
    """Yield both eager and lazy Polars DataFrames."""
    df = _polars_df()
    if request.param == "lazy":
        return df.lazy()
    return df


class TestValidatePolarsSchema:
    def test_strict_match(self, validator, polars_df):
        config = ValidationConfig(tests=[SchemaContract(strict=True)])
        context = {
            "columns": {"id": "int", "name": "str", "age": "int", "code": "str", "status": "str"}
        }
        failures = validator.validate(polars_df, config, context)
        assert len(failures) == 0

    def test_strict_mismatch(self, validator, polars_df):
        config = ValidationConfig(tests=[SchemaContract(strict=True)])
        context = {"columns": {"id": "int", "name": "str"}}
        failures = validator.validate(polars_df, config, context)
        assert len(failures) == 1
        assert "Schema mismatch" in failures[0]

    def test_non_strict_ok(self, validator, polars_df):
        config = ValidationConfig(tests=[SchemaContract(strict=False)])
        context = {"columns": {"id": "int"}}
        failures = validator.validate(polars_df, config, context)
        assert len(failures) == 0

    def test_non_strict_missing(self, validator, polars_df):
        config = ValidationConfig(tests=[SchemaContract(strict=False)])
        context = {"columns": {"id": "int", "missing_col": "str"}}
        failures = validator.validate(polars_df, config, context)
        assert len(failures) == 1
        assert "Missing columns" in failures[0]


class TestValidatePolarsRowCount:
    def test_min_pass(self, validator, polars_df):
        config = ValidationConfig(tests=[RowCountTest(min=1)])
        failures = validator.validate(polars_df, config)
        assert len(failures) == 0

    def test_min_fail(self, validator, polars_df):
        config = ValidationConfig(tests=[RowCountTest(min=100)])
        failures = validator.validate(polars_df, config)
        assert len(failures) == 1
        assert "< min" in failures[0]

    def test_max_pass(self, validator, polars_df):
        config = ValidationConfig(tests=[RowCountTest(max=100)])
        failures = validator.validate(polars_df, config)
        assert len(failures) == 0

    def test_max_fail(self, validator, polars_df):
        config = ValidationConfig(tests=[RowCountTest(max=2)])
        failures = validator.validate(polars_df, config)
        assert len(failures) == 1
        assert "> max" in failures[0]


class TestValidatePolarsNotNull:
    def test_no_nulls(self, validator, polars_df):
        config = ValidationConfig(tests=[NotNullTest(columns=["id"])])
        failures = validator.validate(polars_df, config)
        assert len(failures) == 0

    def test_with_nulls(self, validator, polars_df):
        config = ValidationConfig(tests=[NotNullTest(columns=["name"])])
        failures = validator.validate(polars_df, config)
        assert len(failures) == 1
        assert "NULLs" in failures[0]


class TestValidatePolarsUnique:
    def test_unique_pass(self, validator, polars_df):
        config = ValidationConfig(tests=[UniqueTest(columns=["id"])])
        failures = validator.validate(polars_df, config)
        assert len(failures) == 0

    def test_unique_fail(self, validator):
        df = pl.DataFrame({"x": [1, 1, 2]})
        config = ValidationConfig(tests=[UniqueTest(columns=["x"])])
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "not unique" in failures[0]

    def test_unique_fail_lazy(self, validator):
        df = pl.DataFrame({"x": [1, 1, 2]}).lazy()
        config = ValidationConfig(tests=[UniqueTest(columns=["x"])])
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "not unique" in failures[0]


class TestValidatePolarsAcceptedValues:
    def test_accepted_pass(self, validator, polars_df):
        config = ValidationConfig(
            tests=[
                AcceptedValuesTest(
                    column="status",
                    values=["active", "inactive", "pending", "deleted"],
                )
            ]
        )
        failures = validator.validate(polars_df, config)
        assert len(failures) == 0

    def test_accepted_fail(self, validator, polars_df):
        config = ValidationConfig(
            tests=[
                AcceptedValuesTest(
                    column="status",
                    values=["active", "inactive"],
                )
            ]
        )
        failures = validator.validate(polars_df, config)
        assert len(failures) == 1
        assert "invalid values" in failures[0]


class TestValidatePolarsRange:
    def test_range_pass(self, validator, polars_df):
        config = ValidationConfig(tests=[RangeTest(column="age", min=0, max=200)])
        failures = validator.validate(polars_df, config)
        assert len(failures) == 0

    def test_range_fail(self, validator, polars_df):
        config = ValidationConfig(tests=[RangeTest(column="age", min=0, max=100)])
        failures = validator.validate(polars_df, config)
        assert len(failures) == 1
        assert "out of range" in failures[0]


class TestValidatePolarsRegex:
    def test_regex_pass(self, validator):
        df = pl.DataFrame({"code": ["AB-1", "CD-2"]})
        config = ValidationConfig(tests=[RegexMatchTest(column="code", pattern=r"^[A-Z]{2}-\d$")])
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_regex_fail_eager(self, validator):
        df = pl.DataFrame({"code": ["AB-1", "bad"]})
        config = ValidationConfig(tests=[RegexMatchTest(column="code", pattern=r"^[A-Z]{2}-\d$")])
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "does not match pattern" in failures[0]

    def test_regex_fail_lazy(self, validator):
        df = pl.DataFrame({"code": ["AB-1", "bad"]}).lazy()
        config = ValidationConfig(tests=[RegexMatchTest(column="code", pattern=r"^[A-Z]{2}-\d$")])
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "does not match pattern" in failures[0]

    def test_regex_missing_column(self, validator, polars_df):
        config = ValidationConfig(tests=[RegexMatchTest(column="nonexistent", pattern=r".*")])
        failures = validator.validate(polars_df, config)
        assert len(failures) == 1
        assert "not found" in failures[0]


class TestValidatePolarsFreshness:
    def test_freshness_eager_fresh(self, validator):
        now = datetime.now(timezone.utc)
        df = pl.DataFrame({"ts": [now - timedelta(hours=1)]})
        config = ValidationConfig(tests=[FreshnessContract(column="ts", max_age="24h")])
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_freshness_eager_stale(self, validator):
        old = datetime.now(timezone.utc) - timedelta(days=5)
        df = pl.DataFrame({"ts": [old]})
        config = ValidationConfig(tests=[FreshnessContract(column="ts", max_age="24h")])
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "Data too old" in failures[0]

    def test_freshness_lazy_fresh(self, validator):
        now = datetime.now(timezone.utc)
        df = pl.DataFrame({"ts": [now - timedelta(hours=1)]}).lazy()
        config = ValidationConfig(tests=[FreshnessContract(column="ts", max_age="24h")])
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_freshness_lazy_stale(self, validator):
        old = datetime.now(timezone.utc) - timedelta(days=5)
        df = pl.DataFrame({"ts": [old]}).lazy()
        config = ValidationConfig(tests=[FreshnessContract(column="ts", max_age="24h")])
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "Data too old" in failures[0]

    def test_freshness_missing_column(self, validator, polars_df):
        config = ValidationConfig(tests=[FreshnessContract(column="nonexistent", max_age="24h")])
        failures = validator.validate(polars_df, config)
        assert len(failures) == 1
        assert "not found" in failures[0]


class TestValidatePolarsFailFast:
    def test_fail_fast_exits_early(self, validator, polars_df):
        config = ValidationConfig(
            fail_fast=True,
            tests=[
                RowCountTest(min=100),
                RangeTest(column="age", min=10, max=20),
            ],
        )
        failures = validator.validate(polars_df, config)
        assert len(failures) == 1

    def test_no_fail_fast_collects_all(self, validator, polars_df):
        config = ValidationConfig(
            fail_fast=False,
            tests=[
                RowCountTest(min=100),
                RangeTest(column="age", min=10, max=20),
            ],
        )
        failures = validator.validate(polars_df, config)
        assert len(failures) == 2


class TestValidatePolarsNotNullFailFast:
    def test_fail_fast_on_not_null(self, validator):
        df = pl.DataFrame({"a": [None, None], "b": [None, None]})
        config = ValidationConfig(
            fail_fast=True,
            tests=[NotNullTest(columns=["a", "b"])],
        )
        failures = validator.validate(df, config)
        assert len(failures) == 1

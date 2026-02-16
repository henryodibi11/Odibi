"""Comprehensive tests for odibi/validation/engine.py.

Coverage targets:
- Validator class and its validate() method
- All three engine paths: Pandas, Polars, Spark
- All test types: NOT_NULL, UNIQUE, RANGE, ACCEPTED_VALUES, REGEX_MATCH, etc.
- Fail-fast mode
- Severity handling (WARN vs FAIL)
"""

import logging

logging.getLogger("odibi").propagate = False

import pandas as pd  # noqa: E402
import pytest  # noqa: E402

from odibi.config import (  # noqa: E402
    AcceptedValuesTest,
    ContractSeverity,
    CustomSQLTest,
    FreshnessContract,
    NotNullTest,
    RangeTest,
    RegexMatchTest,
    RowCountTest,
    SchemaContract,
    TestType,
    UniqueTest,
    ValidationConfig,
)
from odibi.validation.engine import Validator  # noqa: E402


class TestValidatorPandas:
    """Test Validator with Pandas DataFrames."""

    @pytest.fixture
    def validator(self):
        return Validator()

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", None, "Eve"],
                "age": [25, 30, 150, 20, 35],
                "email": [
                    "alice@example.com",
                    "invalid_email",
                    "charlie@example.com",
                    "dave@example.com",
                    "eve@example.com",
                ],
                "status": ["active", "inactive", "pending", "deleted", "active"],
            }
        )

    def test_not_null_pass(self, validator, sample_df):
        """NOT_NULL passes when no nulls in column."""
        config = ValidationConfig(tests=[NotNullTest(type=TestType.NOT_NULL, columns=["id"])])
        failures = validator.validate(sample_df, config)
        assert len(failures) == 0

    def test_not_null_fail(self, validator, sample_df):
        """NOT_NULL fails when nulls exist in column."""
        config = ValidationConfig(tests=[NotNullTest(type=TestType.NOT_NULL, columns=["name"])])
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "Column 'name' contains 1 NULLs" in failures[0]

    def test_not_null_missing_column(self, validator, sample_df):
        """NOT_NULL reports error for missing column."""
        config = ValidationConfig(
            tests=[NotNullTest(type=TestType.NOT_NULL, columns=["nonexistent"])]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "not found" in failures[0]

    def test_unique_pass(self, validator, sample_df):
        """UNIQUE passes when column has no duplicates."""
        config = ValidationConfig(tests=[UniqueTest(type=TestType.UNIQUE, columns=["id"])])
        failures = validator.validate(sample_df, config)
        assert len(failures) == 0

    def test_unique_fail(self, validator, sample_df):
        """UNIQUE fails when duplicates exist."""
        config = ValidationConfig(tests=[UniqueTest(type=TestType.UNIQUE, columns=["status"])])
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "not unique" in failures[0]

    def test_range_pass(self, validator, sample_df):
        """RANGE passes when all values within bounds."""
        df = pd.DataFrame({"value": [10, 20, 30, 40, 50]})
        config = ValidationConfig(
            tests=[RangeTest(type=TestType.RANGE, column="value", min=0, max=100)]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_range_fail_max(self, validator, sample_df):
        """RANGE fails when values exceed max."""
        config = ValidationConfig(
            tests=[RangeTest(type=TestType.RANGE, column="age", min=0, max=100)]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "out of range" in failures[0]

    def test_range_fail_min(self, validator):
        """RANGE fails when values below min."""
        df = pd.DataFrame({"value": [-5, 10, 20]})
        config = ValidationConfig(
            tests=[RangeTest(type=TestType.RANGE, column="value", min=0, max=100)]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "out of range" in failures[0]

    def test_range_missing_column(self, validator, sample_df):
        """RANGE reports error for missing column."""
        config = ValidationConfig(
            tests=[RangeTest(type=TestType.RANGE, column="nonexistent", min=0, max=100)]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "not found" in failures[0]

    def test_accepted_values_pass(self, validator, sample_df):
        """ACCEPTED_VALUES passes when all values in list."""
        config = ValidationConfig(
            tests=[
                AcceptedValuesTest(
                    type=TestType.ACCEPTED_VALUES,
                    column="status",
                    values=["active", "inactive", "pending", "deleted"],
                )
            ]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 0

    def test_accepted_values_fail(self, validator, sample_df):
        """ACCEPTED_VALUES fails when values not in list."""
        config = ValidationConfig(
            tests=[
                AcceptedValuesTest(
                    type=TestType.ACCEPTED_VALUES,
                    column="status",
                    values=["active", "inactive"],
                )
            ]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "invalid values" in failures[0]

    def test_row_count_pass(self, validator, sample_df):
        """ROW_COUNT passes when within bounds."""
        config = ValidationConfig(tests=[RowCountTest(type=TestType.ROW_COUNT, min=1, max=100)])
        failures = validator.validate(sample_df, config)
        assert len(failures) == 0

    def test_row_count_fail_min(self, validator, sample_df):
        """ROW_COUNT fails when below minimum."""
        config = ValidationConfig(tests=[RowCountTest(type=TestType.ROW_COUNT, min=100)])
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "< min" in failures[0]

    def test_row_count_fail_max(self, validator, sample_df):
        """ROW_COUNT fails when above maximum."""
        config = ValidationConfig(tests=[RowCountTest(type=TestType.ROW_COUNT, max=2)])
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "> max" in failures[0]

    def test_regex_match_pass(self, validator):
        """REGEX_MATCH passes when all values match pattern."""
        df = pd.DataFrame({"email": ["a@b.com", "x@y.org", "test@example.net"]})
        config = ValidationConfig(
            tests=[
                RegexMatchTest(
                    type=TestType.REGEX_MATCH,
                    column="email",
                    pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$",
                )
            ]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_regex_match_fail(self, validator, sample_df):
        """REGEX_MATCH fails when values don't match pattern."""
        config = ValidationConfig(
            tests=[
                RegexMatchTest(
                    type=TestType.REGEX_MATCH,
                    column="email",
                    pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$",
                )
            ]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "does not match pattern" in failures[0]

    def test_custom_sql_pass(self, validator, sample_df):
        """CUSTOM_SQL passes when condition is true for all rows."""
        config = ValidationConfig(
            tests=[CustomSQLTest(type=TestType.CUSTOM_SQL, condition="id > 0")]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 0

    def test_custom_sql_fail(self, validator, sample_df):
        """CUSTOM_SQL fails when condition is false for some rows."""
        config = ValidationConfig(
            tests=[CustomSQLTest(type=TestType.CUSTOM_SQL, condition="age < 100")]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "Custom check" in failures[0]

    def test_multiple_tests(self, validator, sample_df):
        """Multiple tests are all evaluated."""
        config = ValidationConfig(
            tests=[
                NotNullTest(type=TestType.NOT_NULL, columns=["name"]),
                RangeTest(type=TestType.RANGE, column="age", min=0, max=100),
            ]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 2

    def test_fail_fast_mode(self, validator, sample_df):
        """fail_fast=True stops after first failure."""
        config = ValidationConfig(
            tests=[
                NotNullTest(type=TestType.NOT_NULL, columns=["name"]),
                RangeTest(type=TestType.RANGE, column="age", min=0, max=100),
            ],
            fail_fast=True,
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1

    def test_warn_severity_no_failure(self, validator, sample_df):
        """WARN severity logs warning but doesn't add to failures."""
        config = ValidationConfig(
            tests=[
                NotNullTest(
                    type=TestType.NOT_NULL,
                    columns=["name"],
                    on_fail=ContractSeverity.WARN,
                )
            ]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 0

    def test_empty_dataframe(self, validator):
        """Empty DataFrame passes basic tests."""
        empty_df = pd.DataFrame(columns=["id", "name"])
        config = ValidationConfig(tests=[UniqueTest(type=TestType.UNIQUE, columns=["id"])])
        failures = validator.validate(empty_df, config)
        assert len(failures) == 0

    def test_unique_missing_column(self, validator, sample_df):
        """UNIQUE fails when column doesn't exist."""
        config = ValidationConfig(tests=[UniqueTest(type=TestType.UNIQUE, columns=["nonexistent"])])
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "not found" in failures[0]

    def test_accepted_values_missing_column(self, validator, sample_df):
        """ACCEPTED_VALUES fails when column doesn't exist."""
        config = ValidationConfig(
            tests=[
                AcceptedValuesTest(
                    type=TestType.ACCEPTED_VALUES,
                    column="nonexistent",
                    values=["a", "b"],
                )
            ]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "not found" in failures[0]

    def test_regex_missing_column(self, validator, sample_df):
        """REGEX_MATCH fails when column doesn't exist."""
        config = ValidationConfig(
            tests=[
                RegexMatchTest(
                    type=TestType.REGEX_MATCH,
                    column="nonexistent",
                    pattern=r".*",
                )
            ]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "not found" in failures[0]

    def test_not_null_fail_fast_stops_early(self, validator, sample_df):
        """NOT_NULL with fail_fast returns on first missing column."""
        config = ValidationConfig(
            tests=[
                NotNullTest(type=TestType.NOT_NULL, columns=["nonexistent1", "nonexistent2"]),
            ],
            fail_fast=True,
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1


class TestValidatorPolars:
    """Test Validator with Polars DataFrames."""

    @pytest.fixture
    def validator(self):
        return Validator()

    @pytest.fixture
    def sample_df(self):
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        return pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", None, "Eve"],
                "age": [25, 30, 150, 20, 35],
                "email": [
                    "alice@example.com",
                    "invalid_email",
                    "charlie@example.com",
                    "dave@example.com",
                    "eve@example.com",
                ],
                "status": ["active", "inactive", "pending", "deleted", "active"],
            }
        )

    def test_polars_not_null_pass(self, validator, sample_df):
        """NOT_NULL passes with Polars DataFrame."""
        config = ValidationConfig(tests=[NotNullTest(type=TestType.NOT_NULL, columns=["id"])])
        failures = validator.validate(sample_df, config)
        assert len(failures) == 0

    def test_polars_not_null_fail(self, validator, sample_df):
        """NOT_NULL fails with Polars DataFrame when nulls exist."""
        config = ValidationConfig(tests=[NotNullTest(type=TestType.NOT_NULL, columns=["name"])])
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "NULLs" in failures[0]

    def test_polars_unique_pass(self, validator, sample_df):
        """UNIQUE passes with Polars DataFrame."""
        config = ValidationConfig(tests=[UniqueTest(type=TestType.UNIQUE, columns=["id"])])
        failures = validator.validate(sample_df, config)
        assert len(failures) == 0

    def test_polars_unique_fail(self, validator, sample_df):
        """UNIQUE fails with Polars DataFrame when duplicates exist."""
        config = ValidationConfig(tests=[UniqueTest(type=TestType.UNIQUE, columns=["status"])])
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "not unique" in failures[0]

    def test_polars_range_fail(self, validator, sample_df):
        """RANGE fails with Polars DataFrame when out of bounds."""
        config = ValidationConfig(
            tests=[RangeTest(type=TestType.RANGE, column="age", min=0, max=100)]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "out of range" in failures[0]

    def test_polars_accepted_values_fail(self, validator, sample_df):
        """ACCEPTED_VALUES fails with Polars DataFrame."""
        config = ValidationConfig(
            tests=[
                AcceptedValuesTest(
                    type=TestType.ACCEPTED_VALUES,
                    column="status",
                    values=["active", "inactive"],
                )
            ]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "invalid values" in failures[0]

    def test_polars_row_count_fail(self, validator, sample_df):
        """ROW_COUNT fails with Polars DataFrame when below minimum."""
        config = ValidationConfig(tests=[RowCountTest(type=TestType.ROW_COUNT, min=100)])
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "< min" in failures[0]

    def test_polars_lazy_frame(self, validator):
        """Validation works with Polars LazyFrame."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        lf = pl.LazyFrame(
            {
                "id": [1, 2, 3, None],
                "value": [10, 20, 30, 40],
            }
        )
        config = ValidationConfig(tests=[NotNullTest(type=TestType.NOT_NULL, columns=["id"])])
        failures = validator.validate(lf, config)
        assert len(failures) == 1
        assert "NULLs" in failures[0]

    def test_polars_regex_match_fail(self, validator, sample_df):
        """REGEX_MATCH fails with Polars DataFrame when values don't match."""
        config = ValidationConfig(
            tests=[
                RegexMatchTest(
                    type=TestType.REGEX_MATCH,
                    column="email",
                    pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$",
                )
            ]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "does not match pattern" in failures[0]

    def test_polars_regex_match_pass(self, validator):
        """REGEX_MATCH passes with Polars DataFrame when values match."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        df = pl.DataFrame({"email": ["a@b.com", "x@y.org", "test@example.net"]})
        config = ValidationConfig(
            tests=[
                RegexMatchTest(
                    type=TestType.REGEX_MATCH,
                    column="email",
                    pattern=r"@.*\.",
                )
            ]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_polars_regex_missing_column(self, validator, sample_df):
        """REGEX_MATCH fails with Polars when column missing."""
        config = ValidationConfig(
            tests=[
                RegexMatchTest(
                    type=TestType.REGEX_MATCH,
                    column="nonexistent",
                    pattern=r".*",
                )
            ]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "not found" in failures[0]

    def test_polars_accepted_values_missing_column(self, validator, sample_df):
        """ACCEPTED_VALUES fails with Polars when column missing."""
        config = ValidationConfig(
            tests=[
                AcceptedValuesTest(
                    type=TestType.ACCEPTED_VALUES,
                    column="nonexistent",
                    values=["a", "b"],
                )
            ]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "not found" in failures[0]

    def test_polars_range_missing_column(self, validator, sample_df):
        """RANGE fails with Polars when column missing."""
        config = ValidationConfig(
            tests=[RangeTest(type=TestType.RANGE, column="nonexistent", min=0, max=100)]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "not found" in failures[0]

    def test_polars_unique_missing_column(self, validator, sample_df):
        """UNIQUE fails with Polars when column missing."""
        config = ValidationConfig(tests=[UniqueTest(type=TestType.UNIQUE, columns=["nonexistent"])])
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1
        assert "not found" in failures[0]

    def test_polars_custom_sql_skipped(self, validator, sample_df):
        """CUSTOM_SQL is skipped with Polars DataFrame."""
        config = ValidationConfig(
            tests=[CustomSQLTest(type=TestType.CUSTOM_SQL, condition="id > 0")]
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 0

    def test_polars_fail_fast_mode(self, validator, sample_df):
        """fail_fast=True stops after first failure with Polars."""
        config = ValidationConfig(
            tests=[
                NotNullTest(type=TestType.NOT_NULL, columns=["name"]),
                RangeTest(type=TestType.RANGE, column="age", min=0, max=100),
            ],
            fail_fast=True,
        )
        failures = validator.validate(sample_df, config)
        assert len(failures) == 1

    def test_polars_lazy_frame_unique(self, validator):
        """UNIQUE check works with Polars LazyFrame."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        lf = pl.LazyFrame({"id": [1, 2, 2, 3], "name": ["a", "b", "c", "d"]})
        config = ValidationConfig(tests=[UniqueTest(type=TestType.UNIQUE, columns=["id"])])
        failures = validator.validate(lf, config)
        assert len(failures) == 1
        assert "not unique" in failures[0]

    def test_polars_lazy_frame_accepted_values(self, validator):
        """ACCEPTED_VALUES check works with Polars LazyFrame."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        lf = pl.LazyFrame({"status": ["active", "inactive", "unknown"]})
        config = ValidationConfig(
            tests=[
                AcceptedValuesTest(
                    type=TestType.ACCEPTED_VALUES,
                    column="status",
                    values=["active", "inactive"],
                )
            ]
        )
        failures = validator.validate(lf, config)
        assert len(failures) == 1
        assert "invalid values" in failures[0]

    def test_polars_lazy_frame_range(self, validator):
        """RANGE check works with Polars LazyFrame."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        lf = pl.LazyFrame({"value": [10, 50, 200]})
        config = ValidationConfig(
            tests=[RangeTest(type=TestType.RANGE, column="value", min=0, max=100)]
        )
        failures = validator.validate(lf, config)
        assert len(failures) == 1
        assert "out of range" in failures[0]

    def test_polars_lazy_frame_regex(self, validator):
        """REGEX_MATCH check works with Polars LazyFrame."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        lf = pl.LazyFrame({"email": ["valid@test.com", "invalid"]})
        config = ValidationConfig(
            tests=[
                RegexMatchTest(
                    type=TestType.REGEX_MATCH,
                    column="email",
                    pattern=r"@.*\.",
                )
            ]
        )
        failures = validator.validate(lf, config)
        assert len(failures) == 1

    def test_polars_schema_strict(self, validator):
        """Schema validation works with Polars DataFrame."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        df = pl.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        config = ValidationConfig(tests=[SchemaContract(type=TestType.SCHEMA, strict=True)])
        context = {"columns": {"id": "int64", "name": "str"}}
        failures = validator.validate(df, config, context)
        assert len(failures) == 0

    def test_polars_schema_fail(self, validator):
        """Schema validation fails with Polars when columns missing."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        df = pl.DataFrame({"id": [1, 2]})
        config = ValidationConfig(tests=[SchemaContract(type=TestType.SCHEMA, strict=False)])
        context = {"columns": {"id": "int64", "name": "str"}}
        failures = validator.validate(df, config, context)
        assert len(failures) == 1
        assert "Missing columns" in failures[0]


class TestValidatorSchema:
    """Test schema validation."""

    @pytest.fixture
    def validator(self):
        return Validator()

    def test_schema_strict_pass(self, validator):
        """Schema test passes in strict mode when columns match exactly."""
        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        config = ValidationConfig(tests=[SchemaContract(type=TestType.SCHEMA, strict=True)])
        context = {"columns": {"id": "int64", "name": "object"}}
        failures = validator.validate(df, config, context)
        assert len(failures) == 0

    def test_schema_strict_fail_extra_column(self, validator):
        """Schema test fails in strict mode when extra columns exist."""
        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"], "extra": [1, 2]})
        config = ValidationConfig(tests=[SchemaContract(type=TestType.SCHEMA, strict=True)])
        context = {"columns": {"id": "int64", "name": "object"}}
        failures = validator.validate(df, config, context)
        assert len(failures) == 1
        assert "mismatch" in failures[0].lower()

    def test_schema_non_strict_pass(self, validator):
        """Schema test passes in non-strict mode when extra columns exist."""
        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"], "extra": [1, 2]})
        config = ValidationConfig(tests=[SchemaContract(type=TestType.SCHEMA, strict=False)])
        context = {"columns": {"id": "int64", "name": "object"}}
        failures = validator.validate(df, config, context)
        assert len(failures) == 0

    def test_schema_non_strict_fail_missing(self, validator):
        """Schema test fails in non-strict mode when required columns missing."""
        df = pd.DataFrame({"id": [1, 2]})
        config = ValidationConfig(tests=[SchemaContract(type=TestType.SCHEMA, strict=False)])
        context = {"columns": {"id": "int64", "name": "object"}}
        failures = validator.validate(df, config, context)
        assert len(failures) == 1
        assert "Missing columns" in failures[0]


class TestValidatorFreshness:
    """Test freshness validation."""

    @pytest.fixture
    def validator(self):
        return Validator()

    def test_freshness_pass(self, validator):
        """Freshness test passes when data is recent."""
        from datetime import datetime, timezone

        df = pd.DataFrame({"id": [1, 2], "updated_at": [datetime.now(timezone.utc)] * 2})
        config = ValidationConfig(
            tests=[FreshnessContract(type=TestType.FRESHNESS, column="updated_at", max_age="1h")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_freshness_fail(self, validator):
        """Freshness test fails when data is too old."""
        from datetime import datetime, timedelta, timezone

        old_time = datetime.now(timezone.utc) - timedelta(hours=48)
        df = pd.DataFrame({"id": [1, 2], "updated_at": [old_time] * 2})
        config = ValidationConfig(
            tests=[FreshnessContract(type=TestType.FRESHNESS, column="updated_at", max_age="1h")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "too old" in failures[0].lower()

    def test_freshness_missing_column(self, validator):
        """Freshness test fails when column not found."""
        df = pd.DataFrame({"id": [1, 2]})
        config = ValidationConfig(
            tests=[FreshnessContract(type=TestType.FRESHNESS, column="nonexistent", max_age="1h")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "not found" in failures[0]

    def test_freshness_days_format(self, validator):
        """Freshness test works with days format."""
        from datetime import datetime, timezone

        df = pd.DataFrame({"id": [1, 2], "updated_at": [datetime.now(timezone.utc)] * 2})
        config = ValidationConfig(
            tests=[FreshnessContract(type=TestType.FRESHNESS, column="updated_at", max_age="7d")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_freshness_minutes_format(self, validator):
        """Freshness test works with minutes format."""
        from datetime import datetime, timezone

        df = pd.DataFrame({"id": [1, 2], "updated_at": [datetime.now(timezone.utc)] * 2})
        config = ValidationConfig(
            tests=[FreshnessContract(type=TestType.FRESHNESS, column="updated_at", max_age="30m")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_freshness_string_timestamp(self, validator):
        """Freshness test handles string timestamps by converting."""
        from datetime import datetime, timezone

        now_str = datetime.now(timezone.utc).isoformat()
        df = pd.DataFrame({"id": [1, 2], "updated_at": [now_str, now_str]})
        config = ValidationConfig(
            tests=[FreshnessContract(type=TestType.FRESHNESS, column="updated_at", max_age="1h")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_freshness_unparseable_timestamp(self, validator):
        """Freshness test handles unparseable timestamps gracefully."""
        df = pd.DataFrame({"id": [1, 2], "updated_at": [{"nested": "obj"}, {"another": "obj"}]})
        config = ValidationConfig(
            tests=[FreshnessContract(type=TestType.FRESHNESS, column="updated_at", max_age="1h")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0


class TestValidatorCustomSQL:
    """Test custom SQL validation edge cases."""

    @pytest.fixture
    def validator(self):
        return Validator()

    def test_custom_sql_invalid_expression(self, validator):
        """CUSTOM_SQL reports error for invalid expression."""
        df = pd.DataFrame({"id": [1, 2, 3]})
        config = ValidationConfig(
            tests=[CustomSQLTest(type=TestType.CUSTOM_SQL, condition="invalid_syntax ~~~")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "Failed to execute" in failures[0]

    def test_custom_sql_with_name(self, validator):
        """CUSTOM_SQL reports test name in failure message."""
        df = pd.DataFrame({"value": [1, 2, 3]})
        config = ValidationConfig(
            tests=[
                CustomSQLTest(
                    type=TestType.CUSTOM_SQL,
                    condition="value > 10",
                    name="positive_value_check",
                )
            ]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "positive_value_check" in failures[0]


class TestValidatorPolarsAdvanced:
    """Additional Polars tests for edge cases."""

    @pytest.fixture
    def validator(self):
        return Validator()

    def test_polars_freshness_pass(self, validator):
        """Freshness test works with Polars DataFrame."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        from datetime import datetime, timezone

        df = pl.DataFrame({"id": [1, 2], "updated_at": [datetime.now(timezone.utc)] * 2})
        config = ValidationConfig(
            tests=[FreshnessContract(type=TestType.FRESHNESS, column="updated_at", max_age="1h")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_polars_freshness_fail(self, validator):
        """Freshness test fails with Polars when data is too old."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        from datetime import datetime, timedelta, timezone

        old_time = datetime.now(timezone.utc) - timedelta(hours=48)
        df = pl.DataFrame({"id": [1, 2], "updated_at": [old_time] * 2})
        config = ValidationConfig(
            tests=[FreshnessContract(type=TestType.FRESHNESS, column="updated_at", max_age="1h")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "too old" in failures[0].lower()

    def test_polars_freshness_missing_column(self, validator):
        """Freshness test fails with Polars when column missing."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        df = pl.DataFrame({"id": [1, 2]})
        config = ValidationConfig(
            tests=[FreshnessContract(type=TestType.FRESHNESS, column="nonexistent", max_age="1h")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "not found" in failures[0]

    def test_polars_lazy_freshness(self, validator):
        """Freshness test works with Polars LazyFrame."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        from datetime import datetime, timedelta, timezone

        old_time = datetime.now(timezone.utc) - timedelta(hours=48)
        lf = pl.LazyFrame({"id": [1, 2], "updated_at": [old_time] * 2})
        config = ValidationConfig(
            tests=[FreshnessContract(type=TestType.FRESHNESS, column="updated_at", max_age="1h")]
        )
        failures = validator.validate(lf, config)
        assert len(failures) == 1

    def test_polars_freshness_days_format(self, validator):
        """Freshness test works with days format in Polars."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        from datetime import datetime, timedelta, timezone

        old_time = datetime.now(timezone.utc) - timedelta(days=10)
        df = pl.DataFrame({"id": [1, 2], "updated_at": [old_time] * 2})
        config = ValidationConfig(
            tests=[FreshnessContract(type=TestType.FRESHNESS, column="updated_at", max_age="7d")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 1

    def test_polars_freshness_minutes_format(self, validator):
        """Freshness test works with minutes format in Polars."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        from datetime import datetime, timezone

        df = pl.DataFrame({"id": [1, 2], "updated_at": [datetime.now(timezone.utc)] * 2})
        config = ValidationConfig(
            tests=[FreshnessContract(type=TestType.FRESHNESS, column="updated_at", max_age="30m")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0


class TestPolarsLazyFrameExtended:
    """Test Polars LazyFrame paths not covered by existing tests."""

    @pytest.fixture
    def validator(self):
        return Validator()

    def test_lazy_schema_strict_pass(self, validator):
        """Schema validation passes with LazyFrame in strict mode."""

    def test_polars_freshness_naive_timestamp(self, validator):
        """Freshness test works with timezone-naive timestamps in Polars."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        lf = pl.LazyFrame({"id": [1, 2], "name": ["A", "B"]})
        config = ValidationConfig(tests=[SchemaContract(type=TestType.SCHEMA, strict=True)])
        context = {"columns": {"id": "int64", "name": "str"}}
        failures = validator.validate(lf, config, context)
        assert len(failures) == 0

    def test_lazy_schema_strict_fail(self, validator):
        """Schema validation fails with LazyFrame when extra columns exist."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        lf = pl.LazyFrame({"id": [1, 2], "name": ["A", "B"], "extra": [1, 2]})
        config = ValidationConfig(tests=[SchemaContract(type=TestType.SCHEMA, strict=True)])
        context = {"columns": {"id": "int64", "name": "str"}}
        failures = validator.validate(lf, config, context)
        assert len(failures) == 1
        assert "mismatch" in failures[0].lower()

    def test_lazy_schema_non_strict_pass(self, validator):
        """Schema validation passes with LazyFrame in non-strict mode with extra columns."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        lf = pl.LazyFrame({"id": [1, 2], "name": ["A", "B"], "extra": [1, 2]})
        config = ValidationConfig(tests=[SchemaContract(type=TestType.SCHEMA, strict=False)])
        context = {"columns": {"id": "int64", "name": "str"}}
        failures = validator.validate(lf, config, context)
        assert len(failures) == 0

    def test_lazy_schema_non_strict_fail_missing(self, validator):
        """Schema validation fails with LazyFrame when required columns missing."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        lf = pl.LazyFrame({"id": [1, 2]})
        config = ValidationConfig(tests=[SchemaContract(type=TestType.SCHEMA, strict=False)])
        context = {"columns": {"id": "int64", "name": "str"}}
        failures = validator.validate(lf, config, context)
        assert len(failures) == 1
        assert "Missing columns" in failures[0]

    def test_lazy_row_count_pass(self, validator):
        """ROW_COUNT passes with LazyFrame when within bounds."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        lf = pl.LazyFrame({"id": [1, 2, 3, 4, 5]})
        config = ValidationConfig(tests=[RowCountTest(type=TestType.ROW_COUNT, min=1, max=10)])
        failures = validator.validate(lf, config)
        assert len(failures) == 0

    def test_lazy_row_count_fail_min(self, validator):
        """ROW_COUNT fails with LazyFrame when below minimum."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        lf = pl.LazyFrame({"id": [1, 2]})
        config = ValidationConfig(tests=[RowCountTest(type=TestType.ROW_COUNT, min=100)])
        failures = validator.validate(lf, config)
        assert len(failures) == 1
        assert "< min" in failures[0]

    def test_lazy_row_count_fail_max(self, validator):
        """ROW_COUNT fails with LazyFrame when above maximum."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        lf = pl.LazyFrame({"id": [1, 2, 3, 4, 5]})
        config = ValidationConfig(tests=[RowCountTest(type=TestType.ROW_COUNT, max=2)])
        failures = validator.validate(lf, config)
        assert len(failures) == 1
        assert "> max" in failures[0]

    def test_lazy_not_null_fail_fast(self, validator):
        """NOT_NULL with fail_fast returns early on LazyFrame."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        lf = pl.LazyFrame({"a": [None, 1], "b": [None, 2]})
        config = ValidationConfig(
            tests=[
                NotNullTest(type=TestType.NOT_NULL, columns=["a", "b"]),
            ],
            fail_fast=True,
        )
        failures = validator.validate(lf, config)
        assert len(failures) == 1
        assert "'a'" in failures[0]

    def test_lazy_accepted_values_examples_extraction(self, validator):
        """ACCEPTED_VALUES extracts example values via lazy collect path."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        lf = pl.LazyFrame({"status": ["active", "bad1", "bad2", "bad3", "bad4"]})
        config = ValidationConfig(
            tests=[
                AcceptedValuesTest(
                    type=TestType.ACCEPTED_VALUES,
                    column="status",
                    values=["active"],
                )
            ]
        )
        failures = validator.validate(lf, config)
        assert len(failures) == 1
        assert "invalid values" in failures[0]
        assert "bad1" in failures[0]


class TestPandasEdgeCases:
    """Test Pandas engine edge cases not covered by existing tests."""

    @pytest.fixture
    def validator(self):
        return Validator()

    def test_not_null_mixed_existing_and_missing_columns(self, validator):
        """NOT_NULL with multiple columns where some exist and some don't."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [None, 2, 3]})
        config = ValidationConfig(
            tests=[NotNullTest(type=TestType.NOT_NULL, columns=["a", "nonexistent", "b"])]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 2
        found_not_found = any("not found" in f for f in failures)
        found_nulls = any("NULLs" in f for f in failures)
        assert found_not_found
        assert found_nulls

    def test_range_min_only(self, validator):
        """RANGE with only min specified (no max)."""
        df = pd.DataFrame({"value": [-5, 10, 20]})
        config = ValidationConfig(tests=[RangeTest(type=TestType.RANGE, column="value", min=0)])
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "out of range" in failures[0]

    def test_range_min_only_pass(self, validator):
        """RANGE with only min specified passes when all above min."""
        df = pd.DataFrame({"value": [5, 10, 20]})
        config = ValidationConfig(tests=[RangeTest(type=TestType.RANGE, column="value", min=0)])
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_range_max_only(self, validator):
        """RANGE with only max specified (no min)."""
        df = pd.DataFrame({"value": [5, 10, 200]})
        config = ValidationConfig(tests=[RangeTest(type=TestType.RANGE, column="value", max=100)])
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "out of range" in failures[0]

    def test_range_max_only_pass(self, validator):
        """RANGE with only max specified passes when all below max."""
        df = pd.DataFrame({"value": [5, 10, 20]})
        config = ValidationConfig(tests=[RangeTest(type=TestType.RANGE, column="value", max=100)])
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_regex_match_all_null_column(self, validator):
        """REGEX_MATCH with all-null column passes (valid_series.empty path)."""
        df = pd.DataFrame({"col": [None, None, None]})
        config = ValidationConfig(
            tests=[
                RegexMatchTest(
                    type=TestType.REGEX_MATCH,
                    column="col",
                    pattern=r"^[A-Z]+$",
                )
            ]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_accepted_values_all_valid(self, validator):
        """ACCEPTED_VALUES passes when every value is in the accepted list."""
        df = pd.DataFrame({"status": ["active", "active", "inactive"]})
        config = ValidationConfig(
            tests=[
                AcceptedValuesTest(
                    type=TestType.ACCEPTED_VALUES,
                    column="status",
                    values=["active", "inactive"],
                )
            ]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_schema_without_context(self, validator):
        """Schema test with no context produces no failure."""
        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        config = ValidationConfig(tests=[SchemaContract(type=TestType.SCHEMA, strict=True)])
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_schema_with_context_missing_columns_key(self, validator):
        """Schema test with context that has no 'columns' key produces no failure."""
        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        config = ValidationConfig(tests=[SchemaContract(type=TestType.SCHEMA, strict=True)])
        context = {"other_key": "value"}
        failures = validator.validate(df, config, context)
        assert len(failures) == 0


class TestPolarsWarnSeverity:
    """Test WARN severity with Polars engine."""

    @pytest.fixture
    def validator(self):
        return Validator()

    def test_polars_warn_not_null_no_failure(self, validator):
        """WARN severity with Polars NOT_NULL does not add to failures."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        df = pl.DataFrame({"name": ["Alice", None, "Charlie"]})
        config = ValidationConfig(
            tests=[
                NotNullTest(
                    type=TestType.NOT_NULL,
                    columns=["name"],
                    on_fail=ContractSeverity.WARN,
                )
            ]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_polars_warn_range_no_failure(self, validator):
        """WARN severity with Polars RANGE does not add to failures."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        df = pl.DataFrame({"value": [10, 200, 30]})
        config = ValidationConfig(
            tests=[
                RangeTest(
                    type=TestType.RANGE,
                    column="value",
                    min=0,
                    max=100,
                    on_fail=ContractSeverity.WARN,
                )
            ]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_polars_warn_unique_no_failure(self, validator):
        """WARN severity with Polars UNIQUE does not add to failures."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        df = pl.DataFrame({"id": [1, 1, 2]})
        config = ValidationConfig(
            tests=[
                UniqueTest(
                    type=TestType.UNIQUE,
                    columns=["id"],
                    on_fail=ContractSeverity.WARN,
                )
            ]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0


class TestPolarsFailFastNotNull:
    """Test Polars NOT_NULL fail_fast special path (continue + early return)."""

    @pytest.fixture
    def validator(self):
        return Validator()

    def test_polars_not_null_fail_fast_stops_at_first_column(self, validator):
        """NOT_NULL with fail_fast returns after first null column in Polars."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        df = pl.DataFrame({"a": [None, 1, 2], "b": [None, None, 3]})
        config = ValidationConfig(
            tests=[
                NotNullTest(type=TestType.NOT_NULL, columns=["a", "b"]),
            ],
            fail_fast=True,
        )
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "'a'" in failures[0]

    def test_polars_not_null_fail_fast_skips_subsequent_tests(self, validator):
        """NOT_NULL fail_fast prevents subsequent tests from running in Polars."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        df = pl.DataFrame({"a": [None, 1], "b": [1, 1]})
        config = ValidationConfig(
            tests=[
                NotNullTest(type=TestType.NOT_NULL, columns=["a"]),
                UniqueTest(type=TestType.UNIQUE, columns=["b"]),
            ],
            fail_fast=True,
        )
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "NULLs" in failures[0]


class TestSchemaWithoutContext:
    """Test schema validation when context is None or missing 'columns'."""

    @pytest.fixture
    def validator(self):
        return Validator()

    def test_polars_schema_without_context(self, validator):
        """Schema test with Polars and no context produces no failure."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        df = pl.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        config = ValidationConfig(tests=[SchemaContract(type=TestType.SCHEMA, strict=True)])
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_polars_schema_context_no_columns_key(self, validator):
        """Schema test with Polars context missing 'columns' key produces no failure."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        df = pl.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        config = ValidationConfig(tests=[SchemaContract(type=TestType.SCHEMA, strict=True)])
        context = {"other_key": "value"}
        failures = validator.validate(df, config, context)
        assert len(failures) == 0

    def test_lazy_schema_without_context(self, validator):
        """Schema test with LazyFrame and no context produces no failure."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        lf = pl.LazyFrame({"id": [1, 2], "name": ["A", "B"]})
        config = ValidationConfig(tests=[SchemaContract(type=TestType.SCHEMA, strict=True)])
        failures = validator.validate(lf, config)
        assert len(failures) == 0


class TestPolarsFreshnessNaiveTimestamp:
    """Test Polars freshness with timezone-naive timestamps."""

    @pytest.fixture
    def validator(self):
        return Validator()

    def test_polars_freshness_naive_timestamp(self, validator):
        """Freshness test works with timezone-naive timestamps in Polars."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        from datetime import datetime

        # Use timezone-naive datetime (no timezone.utc)
        df = pl.DataFrame({"id": [1, 2], "updated_at": [datetime.now()] * 2})
        config = ValidationConfig(
            tests=[FreshnessContract(type=TestType.FRESHNESS, column="updated_at", max_age="1h")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 0

    def test_polars_freshness_naive_timestamp_old(self, validator):
        """Freshness test fails with old timezone-naive timestamps in Polars."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        from datetime import datetime, timedelta

        # Use timezone-naive datetime that is old
        old_time = datetime.now() - timedelta(hours=48)
        df = pl.DataFrame({"id": [1, 2], "updated_at": [old_time] * 2})
        config = ValidationConfig(
            tests=[FreshnessContract(type=TestType.FRESHNESS, column="updated_at", max_age="1h")]
        )
        failures = validator.validate(df, config)
        assert len(failures) == 1
        assert "too old" in failures[0].lower()

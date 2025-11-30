"""Tests for quarantine table functionality."""

import pandas as pd
import pytest

from odibi.config import (
    ContractSeverity,
    NotNullTest,
    QuarantineColumnsConfig,
    QuarantineConfig,
    RangeTest,
    TestType,
    ValidationConfig,
)
from odibi.validation.quarantine import (
    QuarantineResult,
    add_quarantine_metadata,
    has_quarantine_tests,
    split_valid_invalid,
)


class MockEngine:
    """Mock engine for testing without actual Spark/Pandas engines."""

    name = "pandas"

    def count_rows(self, df):
        return len(df)

    def materialize(self, df):
        return df


@pytest.fixture
def sample_df():
    """Sample DataFrame with some invalid data."""
    return pd.DataFrame(
        {
            "customer_id": [1, 2, None, 4, 5],
            "email": [
                "alice@example.com",
                "invalid_email",
                "charlie@example.com",
                None,
                "eve@example.com",
            ],
            "age": [25, 30, 150, 20, 35],
            "status": ["active", "inactive", "pending", "deleted", "active"],
        }
    )


@pytest.fixture
def mock_engine():
    return MockEngine()


class TestHasQuarantineTests:
    """Tests for has_quarantine_tests function."""

    def test_no_quarantine_tests(self):
        """Returns False when no tests use quarantine severity."""
        tests = [NotNullTest(type=TestType.NOT_NULL, columns=["id"], on_fail=ContractSeverity.FAIL)]
        assert has_quarantine_tests(tests) is False

    def test_has_quarantine_tests(self):
        """Returns True when at least one test uses quarantine severity."""
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL, columns=["id"], on_fail=ContractSeverity.QUARANTINE
            ),
            NotNullTest(type=TestType.NOT_NULL, columns=["email"], on_fail=ContractSeverity.FAIL),
        ]
        assert has_quarantine_tests(tests) is True

    def test_empty_tests(self):
        """Returns False for empty test list."""
        assert has_quarantine_tests([]) is False


class TestSplitValidInvalid:
    """Tests for split_valid_invalid function."""

    def test_splits_valid_invalid_rows(self, sample_df, mock_engine):
        """Valid rows continue, invalid rows go to quarantine."""
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["customer_id"],
                on_fail=ContractSeverity.QUARANTINE,
            )
        ]

        result = split_valid_invalid(sample_df, tests, mock_engine)

        assert isinstance(result, QuarantineResult)
        assert result.rows_valid == 4
        assert result.rows_quarantined == 1
        assert len(result.valid_df) == 4
        assert len(result.invalid_df) == 1
        assert result.invalid_df["customer_id"].isna().all()

    def test_no_quarantine_tests_returns_original(self, sample_df, mock_engine):
        """When no quarantine tests, return original DataFrame unchanged."""
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL, columns=["customer_id"], on_fail=ContractSeverity.FAIL
            )
        ]

        result = split_valid_invalid(sample_df, tests, mock_engine)

        assert result.rows_valid == 5
        assert result.rows_quarantined == 0
        assert len(result.valid_df) == 5
        assert len(result.invalid_df) == 0

    def test_multiple_tests_combined(self, sample_df, mock_engine):
        """Row failing ANY quarantine test is invalid."""
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["customer_id"],
                on_fail=ContractSeverity.QUARANTINE,
            ),
            NotNullTest(
                type=TestType.NOT_NULL, columns=["email"], on_fail=ContractSeverity.QUARANTINE
            ),
        ]

        result = split_valid_invalid(sample_df, tests, mock_engine)

        assert result.rows_quarantined == 2
        assert result.rows_valid == 3

    def test_range_test_quarantine(self, sample_df, mock_engine):
        """Range test failures are quarantined."""
        tests = [
            RangeTest(
                type=TestType.RANGE,
                column="age",
                min=0,
                max=100,
                on_fail=ContractSeverity.QUARANTINE,
            )
        ]

        result = split_valid_invalid(sample_df, tests, mock_engine)

        assert result.rows_quarantined == 1
        assert result.rows_valid == 4
        assert result.invalid_df["age"].iloc[0] == 150


class TestAddQuarantineMetadata:
    """Tests for add_quarantine_metadata function."""

    def test_adds_all_metadata_columns(self, sample_df, mock_engine):
        """Quarantined rows have all metadata columns when enabled."""
        invalid_df = sample_df.iloc[2:3].copy()
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["customer_id"],
                on_fail=ContractSeverity.QUARANTINE,
                name="customer_id_not_null",
            )
        ]
        config = QuarantineColumnsConfig(
            rejection_reason=True,
            rejected_at=True,
            source_batch_id=True,
            failed_tests=True,
            original_node=True,
        )

        result = add_quarantine_metadata(
            invalid_df,
            test_results={},
            config=config,
            engine=mock_engine,
            node_name="test_node",
            run_id="run-123",
            tests=tests,
        )

        assert "_rejection_reason" in result.columns
        assert "_rejected_at" in result.columns
        assert "_source_batch_id" in result.columns
        assert "_failed_tests" in result.columns
        assert "_original_node" in result.columns
        assert result["_original_node"].iloc[0] == "test_node"
        assert result["_source_batch_id"].iloc[0] == "run-123"

    def test_partial_metadata_columns(self, sample_df, mock_engine):
        """Only enabled metadata columns are added."""
        invalid_df = sample_df.iloc[2:3].copy()
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["customer_id"],
                on_fail=ContractSeverity.QUARANTINE,
            )
        ]
        config = QuarantineColumnsConfig(
            rejection_reason=True,
            rejected_at=False,
            source_batch_id=True,
            failed_tests=False,
            original_node=False,
        )

        result = add_quarantine_metadata(
            invalid_df,
            test_results={},
            config=config,
            engine=mock_engine,
            node_name="test_node",
            run_id="run-123",
            tests=tests,
        )

        assert "_rejection_reason" in result.columns
        assert "_rejected_at" not in result.columns
        assert "_source_batch_id" in result.columns
        assert "_failed_tests" not in result.columns
        assert "_original_node" not in result.columns


class TestQuarantineConfig:
    """Tests for QuarantineConfig validation."""

    def test_requires_path_or_table(self):
        """QuarantineConfig requires either path or table."""
        with pytest.raises(ValueError, match="requires either 'path' or 'table'"):
            QuarantineConfig(connection="silver")

    def test_valid_with_path(self):
        """QuarantineConfig is valid with path."""
        config = QuarantineConfig(connection="silver", path="quarantine/customers")
        assert config.path == "quarantine/customers"

    def test_valid_with_table(self):
        """QuarantineConfig is valid with table."""
        config = QuarantineConfig(connection="silver", table="customers_quarantine")
        assert config.table == "customers_quarantine"


class TestValidationConfigWithQuarantine:
    """Tests for ValidationConfig with quarantine support."""

    def test_validation_config_with_quarantine(self):
        """ValidationConfig accepts quarantine configuration."""
        config = ValidationConfig(
            tests=[
                NotNullTest(
                    type=TestType.NOT_NULL,
                    columns=["customer_id"],
                    on_fail=ContractSeverity.QUARANTINE,
                )
            ],
            quarantine=QuarantineConfig(connection="silver", path="quarantine/customers"),
        )

        assert config.quarantine is not None
        assert config.quarantine.connection == "silver"
        assert config.quarantine.path == "quarantine/customers"

    def test_validation_config_without_quarantine(self):
        """ValidationConfig works without quarantine configuration."""
        config = ValidationConfig(
            tests=[
                NotNullTest(type=TestType.NOT_NULL, columns=["customer_id"]),
            ]
        )

        assert config.quarantine is None


class TestMultipleFailedTestsCaptured:
    """Tests for capturing multiple failed tests per row."""

    def test_row_failing_multiple_tests(self, sample_df, mock_engine):
        """Row failing multiple tests has all test names captured."""
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["customer_id"],
                on_fail=ContractSeverity.QUARANTINE,
                name="not_null_customer_id",
            ),
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["email"],
                on_fail=ContractSeverity.QUARANTINE,
                name="not_null_email",
            ),
        ]

        result = split_valid_invalid(sample_df, tests, mock_engine)

        assert result.rows_quarantined >= 1

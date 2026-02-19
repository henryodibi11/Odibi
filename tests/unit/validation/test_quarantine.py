"""Comprehensive tests for odibi/validation/quarantine.py.

Coverage targets:
- split_valid_invalid function
- add_quarantine_metadata function
- write_quarantine function
- _evaluate_test_mask for all test types
- _apply_sampling function
- Engine-specific paths (Pandas, Polars, Spark)
"""

import pandas as pd
import pytest

from odibi.config import (
    AcceptedValuesTest,
    ContractSeverity,
    CustomSQLTest,
    NotNullTest,
    QuarantineColumnsConfig,
    QuarantineConfig,
    RangeTest,
    RegexMatchTest,
    TestType,
    UniqueTest,
)
from odibi.validation.quarantine import (
    QuarantineResult,
    _apply_sampling,
    _evaluate_test_mask,
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


class MockConnection:
    """Mock connection for testing write operations."""

    def __init__(self, base_path="/tmp/test"):
        self.base_path = base_path

    def get_path(self, path):
        return f"{self.base_path}/{path}"


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

    def test_all_quarantine_tests(self):
        """Returns True when all tests use quarantine severity."""
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL, columns=["id"], on_fail=ContractSeverity.QUARANTINE
            ),
            RangeTest(
                type=TestType.RANGE,
                column="age",
                min=0,
                max=100,
                on_fail=ContractSeverity.QUARANTINE,
            ),
        ]
        assert has_quarantine_tests(tests) is True

    def test_warn_severity_not_quarantine(self):
        """Returns False when only WARN severity is used."""
        tests = [NotNullTest(type=TestType.NOT_NULL, columns=["id"], on_fail=ContractSeverity.WARN)]
        assert has_quarantine_tests(tests) is False


class TestEvaluateTestMask:
    """Tests for _evaluate_test_mask function."""

    def test_not_null_mask_pandas(self, sample_df):
        """NOT_NULL mask correctly identifies null rows."""
        test = NotNullTest(
            type=TestType.NOT_NULL,
            columns=["customer_id"],
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(sample_df, test, is_spark=False, is_polars=False)
        assert mask.sum() == 4
        assert not mask.iloc[2]

    def test_not_null_mask_multiple_columns(self, sample_df):
        """NOT_NULL mask with multiple columns uses AND logic."""
        test = NotNullTest(
            type=TestType.NOT_NULL,
            columns=["customer_id", "email"],
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(sample_df, test, is_spark=False, is_polars=False)
        assert mask.sum() == 3

    def test_range_mask_pandas(self, sample_df):
        """RANGE mask correctly identifies out-of-range rows."""
        test = RangeTest(
            type=TestType.RANGE,
            column="age",
            min=0,
            max=100,
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(sample_df, test, is_spark=False, is_polars=False)
        assert mask.sum() == 4
        assert not mask.iloc[2]

    def test_range_mask_min_only(self):
        """RANGE mask works with only min specified."""
        df = pd.DataFrame({"value": [-5, 0, 10, 20]})
        test = RangeTest(
            type=TestType.RANGE,
            column="value",
            min=0,
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
        assert mask.sum() == 3
        assert not mask.iloc[0]

    def test_range_mask_max_only(self):
        """RANGE mask works with only max specified."""
        df = pd.DataFrame({"value": [5, 10, 100, 200]})
        test = RangeTest(
            type=TestType.RANGE,
            column="value",
            max=100,
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
        assert mask.sum() == 3
        assert not mask.iloc[3]

    def test_accepted_values_mask_pandas(self, sample_df):
        """ACCEPTED_VALUES mask correctly identifies invalid values."""
        test = AcceptedValuesTest(
            type=TestType.ACCEPTED_VALUES,
            column="status",
            values=["active", "inactive", "pending"],
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(sample_df, test, is_spark=False, is_polars=False)
        assert mask.sum() == 4
        assert not mask.iloc[3]

    def test_regex_mask_pandas(self, sample_df):
        """REGEX_MATCH mask correctly identifies non-matching values."""
        test = RegexMatchTest(
            type=TestType.REGEX_MATCH,
            column="email",
            pattern=r".*@.*\..*",
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(sample_df, test, is_spark=False, is_polars=False)
        assert not mask.iloc[1]

    def test_missing_column_returns_true(self, sample_df):
        """Missing column returns all-True mask (no rows quarantined)."""
        test = RangeTest(
            type=TestType.RANGE,
            column="nonexistent",
            min=0,
            max=100,
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(sample_df, test, is_spark=False, is_polars=False)
        assert mask.all()


class TestEvaluateTestMaskPolars:
    """Tests for _evaluate_test_mask with Polars DataFrames."""

    @pytest.fixture
    def polars_df(self):
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        return pl.DataFrame(
            {
                "customer_id": [1, 2, None, 4, 5],
                "age": [25, 30, 150, 20, 35],
                "status": ["active", "inactive", "pending", "deleted", "active"],
            }
        )

    def test_not_null_mask_polars(self, polars_df):
        """NOT_NULL mask works with Polars DataFrame."""

        test = NotNullTest(
            type=TestType.NOT_NULL,
            columns=["customer_id"],
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask_expr = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = polars_df.select(mask_expr.alias("mask"))["mask"].to_list()
        assert result.count(True) == 4
        assert result[2] is False

    def test_range_mask_polars(self, polars_df):
        """RANGE mask works with Polars DataFrame."""

        test = RangeTest(
            type=TestType.RANGE,
            column="age",
            min=0,
            max=100,
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask_expr = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = polars_df.select(mask_expr.alias("mask"))["mask"].to_list()
        assert result.count(True) == 4
        assert result[2] is False

    def test_accepted_values_mask_polars(self, polars_df):
        """ACCEPTED_VALUES mask works with Polars DataFrame."""

        test = AcceptedValuesTest(
            type=TestType.ACCEPTED_VALUES,
            column="status",
            values=["active", "inactive", "pending"],
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask_expr = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = polars_df.select(mask_expr.alias("mask"))["mask"].to_list()
        assert result.count(True) == 4
        assert result[3] is False

    def test_regex_mask_polars(self, polars_df):
        """REGEX_MATCH mask works with Polars DataFrame."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        df = pl.DataFrame(
            {
                "email": ["a@b.com", "invalid", "x@y.org", None, "z@w.net"],
            }
        )
        test = RegexMatchTest(
            type=TestType.REGEX_MATCH,
            column="email",
            pattern=r"@.*\.",
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask_expr = _evaluate_test_mask(df, test, is_spark=False, is_polars=True)
        result = df.select(mask_expr.alias("mask"))["mask"].to_list()
        assert result.count(False) == 1
        assert result[1] is False

    def test_range_missing_column_polars(self, polars_df):
        """RANGE mask returns all-True when column is missing in Polars."""
        test = RangeTest(
            type=TestType.RANGE,
            column="nonexistent",
            min=0,
            max=100,
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask_expr = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = polars_df.select(mask_expr.alias("mask"))["mask"].to_list()
        assert all(result)

    def test_accepted_values_missing_column_polars(self, polars_df):
        """ACCEPTED_VALUES mask returns all-True when column is missing in Polars."""
        test = AcceptedValuesTest(
            type=TestType.ACCEPTED_VALUES,
            column="nonexistent",
            values=["a", "b"],
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask_expr = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = polars_df.select(mask_expr.alias("mask"))["mask"].to_list()
        assert all(result)

    def test_regex_missing_column_polars(self, polars_df):
        """REGEX_MATCH mask returns all-True when column is missing in Polars."""
        test = RegexMatchTest(
            type=TestType.REGEX_MATCH,
            column="nonexistent",
            pattern=r".*",
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask_expr = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = polars_df.select(mask_expr.alias("mask"))["mask"].to_list()
        assert all(result)

    def test_default_mask_polars(self, polars_df):
        """Unknown test type returns all-True mask in Polars."""
        test = UniqueTest(
            type=TestType.UNIQUE,
            columns=["customer_id"],
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask_expr = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = polars_df.select(mask_expr.alias("mask"))["mask"].to_list()
        assert all(result)


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

    def test_all_rows_valid(self, mock_engine):
        """All rows pass when no violations."""
        df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
        tests = [
            NotNullTest(type=TestType.NOT_NULL, columns=["id"], on_fail=ContractSeverity.QUARANTINE)
        ]

        result = split_valid_invalid(df, tests, mock_engine)

        assert result.rows_valid == 3
        assert result.rows_quarantined == 0

    def test_all_rows_invalid(self, mock_engine):
        """All rows quarantined when all fail."""
        df = pd.DataFrame({"id": [None, None, None]})
        tests = [
            NotNullTest(type=TestType.NOT_NULL, columns=["id"], on_fail=ContractSeverity.QUARANTINE)
        ]

        result = split_valid_invalid(df, tests, mock_engine)

        assert result.rows_valid == 0
        assert result.rows_quarantined == 3

    def test_empty_dataframe(self, mock_engine):
        """Empty DataFrame returns empty results."""
        df = pd.DataFrame(columns=["id", "value"])
        tests = [
            NotNullTest(type=TestType.NOT_NULL, columns=["id"], on_fail=ContractSeverity.QUARANTINE)
        ]

        result = split_valid_invalid(df, tests, mock_engine)

        assert result.rows_valid == 0
        assert result.rows_quarantined == 0


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

    def test_no_metadata_columns(self, sample_df, mock_engine):
        """No metadata columns added when all disabled."""
        invalid_df = sample_df.iloc[2:3].copy()
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["customer_id"],
                on_fail=ContractSeverity.QUARANTINE,
            )
        ]
        config = QuarantineColumnsConfig(
            rejection_reason=False,
            rejected_at=False,
            source_batch_id=False,
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

        original_cols = set(invalid_df.columns)
        result_cols = set(result.columns)
        assert original_cols == result_cols


class TestApplySampling:
    """Tests for _apply_sampling function."""

    def test_no_sampling_returns_original(self):
        """No sampling config returns original DataFrame."""
        df = pd.DataFrame({"id": range(100)})
        config = QuarantineConfig(connection="test", path="test/path")
        result = _apply_sampling(df, config, is_spark=False, is_polars=False)
        assert len(result) == 100

    def test_max_rows_limits_output(self):
        """max_rows limits the number of rows."""
        df = pd.DataFrame({"id": range(100)})
        config = QuarantineConfig(connection="test", path="test/path", max_rows=10)
        result = _apply_sampling(df, config, is_spark=False, is_polars=False)
        assert len(result) == 10

    def test_sample_fraction_reduces_rows(self):
        """sample_fraction reduces rows proportionally."""
        df = pd.DataFrame({"id": range(1000)})
        config = QuarantineConfig(connection="test", path="test/path", sample_fraction=0.1)
        result = _apply_sampling(df, config, is_spark=False, is_polars=False)
        assert len(result) < 200

    def test_sample_fraction_and_max_rows(self):
        """Both sample_fraction and max_rows applied together."""
        df = pd.DataFrame({"id": range(1000)})
        config = QuarantineConfig(
            connection="test", path="test/path", sample_fraction=0.5, max_rows=10
        )
        result = _apply_sampling(df, config, is_spark=False, is_polars=False)
        assert len(result) <= 10


class TestApplySamplingPolars:
    """Tests for _apply_sampling with Polars DataFrames."""

    @pytest.fixture
    def polars_df(self):
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")
        return pl.DataFrame({"id": range(100)})

    def test_max_rows_polars(self, polars_df):
        """max_rows works with Polars DataFrame."""
        config = QuarantineConfig(connection="test", path="test/path", max_rows=10)
        result = _apply_sampling(polars_df, config, is_spark=False, is_polars=True)
        assert len(result) == 10

    def test_sample_fraction_polars(self, polars_df):
        """sample_fraction works with Polars DataFrame."""
        config = QuarantineConfig(connection="test", path="test/path", sample_fraction=0.1)
        result = _apply_sampling(polars_df, config, is_spark=False, is_polars=True)
        assert len(result) < 50


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

    def test_default_values(self):
        """QuarantineConfig has sensible defaults."""
        config = QuarantineConfig(connection="silver", path="test")
        assert config.max_rows is None
        assert config.sample_fraction is None


class TestSplitValidInvalidPolars:
    """Tests for split_valid_invalid with Polars DataFrames."""

    @pytest.fixture
    def polars_df(self):
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")
        return pl.DataFrame(
            {
                "customer_id": [1, 2, None, 4, 5],
                "age": [25, 30, 150, 20, 35],
                "status": ["active", "inactive", "pending", "deleted", "active"],
            }
        )

    def test_polars_split_valid_invalid(self, polars_df, mock_engine):
        """Polars DataFrame is split correctly."""
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["customer_id"],
                on_fail=ContractSeverity.QUARANTINE,
            )
        ]
        result = split_valid_invalid(polars_df, tests, mock_engine)
        assert result.rows_valid == 4
        assert result.rows_quarantined == 1

    def test_polars_split_no_quarantine_tests(self, polars_df, mock_engine):
        """Polars with no quarantine tests returns original."""
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL, columns=["customer_id"], on_fail=ContractSeverity.FAIL
            )
        ]
        result = split_valid_invalid(polars_df, tests, mock_engine)
        assert result.rows_valid == 5
        assert result.rows_quarantined == 0

    def test_polars_split_multiple_tests(self, polars_df, mock_engine):
        """Polars DataFrame with multiple quarantine tests.

        Row 3 (index 2) has customer_id=NULL AND age=150, so it fails both tests.
        But it's still just one row that gets quarantined.
        """
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["customer_id"],
                on_fail=ContractSeverity.QUARANTINE,
            ),
            RangeTest(
                type=TestType.RANGE,
                column="age",
                min=0,
                max=100,
                on_fail=ContractSeverity.QUARANTINE,
            ),
        ]
        result = split_valid_invalid(polars_df, tests, mock_engine)
        assert result.rows_quarantined == 1
        assert result.rows_valid == 4


class TestAddQuarantineMetadataPolars:
    """Tests for add_quarantine_metadata with Polars DataFrames."""

    @pytest.fixture
    def polars_invalid_df(self):
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")
        return pl.DataFrame({"customer_id": [None], "age": [150]})

    def test_polars_adds_metadata_columns(self, polars_invalid_df, mock_engine):
        """Polars DataFrame gets metadata columns added."""
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
            polars_invalid_df,
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


class TestWriteQuarantine:
    """Tests for write_quarantine function."""

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "customer_id": [None, None],
                "age": [150, 200],
                "_rejection_reason": ["test", "test"],
            }
        )

    def test_write_quarantine_empty_df(self, mock_engine):
        """Empty DataFrame returns early without writing."""
        from odibi.validation.quarantine import write_quarantine

        empty_df = pd.DataFrame(columns=["id", "value"])
        config = QuarantineConfig(connection="test", path="quarantine/test")
        connections = {"test": MockConnection()}

        result = write_quarantine(empty_df, config, mock_engine, connections)

        assert result["rows_quarantined"] == 0
        assert result["write_info"] is None

    def test_write_quarantine_missing_connection(self, sample_df, mock_engine):
        """Missing connection raises ValueError."""
        from odibi.validation.quarantine import write_quarantine

        config = QuarantineConfig(connection="missing", path="quarantine/test")
        connections = {"test": MockConnection()}

        with pytest.raises(ValueError, match="not found"):
            write_quarantine(sample_df, config, mock_engine, connections)

    def test_write_quarantine_success(self, sample_df):
        """Successful write returns correct metadata."""
        from odibi.validation.quarantine import write_quarantine

        class MockWriteEngine:
            name = "pandas"

            def write(self, df, **kwargs):
                return {"success": True}

        config = QuarantineConfig(connection="test", path="quarantine/test")
        connections = {"test": MockConnection()}

        result = write_quarantine(sample_df, config, MockWriteEngine(), connections)

        assert result["rows_quarantined"] == 2
        assert result["quarantine_path"] == "quarantine/test"
        assert result["write_info"] == {"success": True}

    def test_write_quarantine_with_table(self, sample_df):
        """Write with table instead of path."""
        from odibi.validation.quarantine import write_quarantine

        class MockWriteEngine:
            name = "pandas"

            def write(self, df, **kwargs):
                return {"success": True, "format": kwargs.get("format")}

        config = QuarantineConfig(connection="test", table="quarantine_table")
        connections = {"test": MockConnection()}

        result = write_quarantine(sample_df, config, MockWriteEngine(), connections)

        assert result["quarantine_path"] == "quarantine_table"

    def test_write_quarantine_with_sampling(self):
        """Write with sampling limits rows."""
        from odibi.validation.quarantine import write_quarantine

        large_df = pd.DataFrame(
            {
                "id": range(100),
                "_rejection_reason": ["test"] * 100,
            }
        )

        class MockWriteEngine:
            name = "pandas"
            written_rows = 0

            def write(self, df, **kwargs):
                self.written_rows = len(df)
                return {"success": True}

        engine = MockWriteEngine()
        config = QuarantineConfig(connection="test", path="quarantine/test", max_rows=10)
        connections = {"test": MockConnection()}

        result = write_quarantine(large_df, config, engine, connections)

        assert result["rows_quarantined"] <= 10

    def test_write_quarantine_error_handling(self, sample_df):
        """Write errors are raised properly."""
        from odibi.validation.quarantine import write_quarantine

        class FailingEngine:
            name = "pandas"

            def write(self, df, **kwargs):
                raise RuntimeError("Write failed")

        config = QuarantineConfig(connection="test", path="quarantine/test")
        connections = {"test": MockConnection()}

        with pytest.raises(RuntimeError, match="Write failed"):
            write_quarantine(sample_df, config, FailingEngine(), connections)


class TestWriteQuarantinePolars:
    """Tests for write_quarantine with Polars DataFrames."""

    @pytest.fixture
    def polars_df(self):
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")
        return pl.DataFrame(
            {
                "customer_id": [None, None],
                "age": [150, 200],
                "_rejection_reason": ["test", "test"],
            }
        )

    def test_polars_write_quarantine(self, polars_df):
        """Polars DataFrame is written correctly."""
        from odibi.validation.quarantine import write_quarantine

        class MockWriteEngine:
            name = "polars"

            def write(self, df, **kwargs):
                return {"success": True}

        config = QuarantineConfig(connection="test", path="quarantine/test")
        connections = {"test": MockConnection()}

        result = write_quarantine(polars_df, config, MockWriteEngine(), connections)

        assert result["rows_quarantined"] == 2

    def test_polars_write_empty(self):
        """Empty Polars DataFrame returns early."""
        from odibi.validation.quarantine import write_quarantine

        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        empty_df = pl.DataFrame({"id": [], "value": []})

        class MockWriteEngine:
            name = "polars"

        config = QuarantineConfig(connection="test", path="quarantine/test")
        connections = {"test": MockConnection()}

        result = write_quarantine(empty_df, config, MockWriteEngine(), connections)

        assert result["rows_quarantined"] == 0


class TestEvaluateTestMaskPandasExtended:
    """Additional Pandas-path tests for _evaluate_test_mask to cover remaining branches."""

    def test_unique_test_detects_duplicates(self):
        """UNIQUE test type marks duplicate rows as failing."""
        df = pd.DataFrame({"id": [1, 2, 2, 3]})
        test = UniqueTest(
            type=TestType.UNIQUE,
            columns=["id"],
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
        assert len(mask) == 4
        assert mask.iloc[0] is True or mask.iloc[0] == True  # noqa: E712
        assert mask.iloc[1] == False  # noqa: E712  # duplicate
        assert mask.iloc[2] == False  # noqa: E712  # duplicate
        assert mask.iloc[3] is True or mask.iloc[3] == True  # noqa: E712

    def test_unique_test_all_unique(self):
        """UNIQUE test passes when all values are unique."""
        df = pd.DataFrame({"id": [1, 2, 3, 4]})
        test = UniqueTest(
            type=TestType.UNIQUE,
            columns=["id"],
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
        assert mask.all()
        assert len(mask) == 4

    def test_custom_sql_valid_condition(self):
        """CUSTOM_SQL mask filters rows using df.query()."""
        df = pd.DataFrame({"amount": [10, -5, 20, 0, 15]})
        test = CustomSQLTest(
            type=TestType.CUSTOM_SQL,
            condition="amount > 0",
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
        assert mask.sum() == 3
        assert not mask.iloc[1]
        assert not mask.iloc[3]

    def test_custom_sql_invalid_condition_returns_all_true(self):
        """CUSTOM_SQL with bad condition falls back to all-True mask."""
        df = pd.DataFrame({"amount": [10, 20, 30]})
        test = CustomSQLTest(
            type=TestType.CUSTOM_SQL,
            condition="INVALID SYNTAX !!!",
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
        assert mask.all()

    def test_regex_match_missing_column_returns_all_true(self):
        """REGEX_MATCH with missing column returns all-True mask."""
        df = pd.DataFrame({"other_col": ["a", "b", "c"]})
        test = RegexMatchTest(
            type=TestType.REGEX_MATCH,
            column="nonexistent",
            pattern=r".*",
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
        assert mask.all()

    def test_accepted_values_missing_column_returns_all_true(self):
        """ACCEPTED_VALUES with missing column returns all-True mask."""
        df = pd.DataFrame({"other_col": ["a", "b", "c"]})
        test = AcceptedValuesTest(
            type=TestType.ACCEPTED_VALUES,
            column="nonexistent",
            values=["x", "y"],
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
        assert mask.all()

    def test_not_null_all_columns_missing(self):
        """NOT_NULL with all columns missing returns all-True mask."""
        df = pd.DataFrame({"other_col": [1, 2, 3]})
        test = NotNullTest(
            type=TestType.NOT_NULL,
            columns=["missing_a", "missing_b"],
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
        assert mask.all()
        assert len(mask) == 3


class TestSplitValidInvalidExtended:
    """Additional split_valid_invalid tests for uncovered Pandas branches."""

    @pytest.fixture
    def mock_engine(self):
        return MockEngine()

    def test_duplicate_test_names_are_deduplicated(self, mock_engine):
        """Tests with the same name get a suffix to avoid key collision."""
        df = pd.DataFrame({"id": [1, None, 3], "value": [10, 20, None]})
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["id"],
                on_fail=ContractSeverity.QUARANTINE,
                name="null_check",
            ),
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["value"],
                on_fail=ContractSeverity.QUARANTINE,
                name="null_check",
            ),
        ]
        result = split_valid_invalid(df, tests, mock_engine)
        assert result.rows_quarantined == 2
        assert result.rows_valid == 1
        test_names = list(result.test_results.keys())
        assert len(test_names) == 2
        assert test_names[0] != test_names[1]

    def test_test_results_populated_with_counts(self, mock_engine):
        """test_results dict contains pass_count and fail_count per test."""
        df = pd.DataFrame({"id": [1, None, 3, None, 5]})
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["id"],
                on_fail=ContractSeverity.QUARANTINE,
                name="id_not_null",
            )
        ]
        result = split_valid_invalid(df, tests, mock_engine)
        assert "id_not_null" in result.test_results
        assert result.test_results["id_not_null"]["pass_count"] == 3
        assert result.test_results["id_not_null"]["fail_count"] == 2

    def test_mixed_quarantine_and_non_quarantine_tests(self, mock_engine):
        """Only QUARANTINE-severity tests affect the split."""
        df = pd.DataFrame({"id": [1, None, 3], "value": [None, 20, 30]})
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["id"],
                on_fail=ContractSeverity.QUARANTINE,
            ),
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["value"],
                on_fail=ContractSeverity.FAIL,
            ),
        ]
        result = split_valid_invalid(df, tests, mock_engine)
        assert result.rows_quarantined == 1
        assert result.rows_valid == 2

    def test_custom_sql_quarantine_integration(self, mock_engine):
        """CUSTOM_SQL test works end-to-end through split_valid_invalid."""
        df = pd.DataFrame({"price": [10, -5, 20, -1, 15]})
        tests = [
            CustomSQLTest(
                type=TestType.CUSTOM_SQL,
                condition="price > 0",
                on_fail=ContractSeverity.QUARANTINE,
                name="positive_price",
            )
        ]
        result = split_valid_invalid(df, tests, mock_engine)
        assert result.rows_quarantined == 2
        assert result.rows_valid == 3
        assert all(result.invalid_df["price"] <= 0)


class TestAddQuarantineMetadataExtended:
    """Additional add_quarantine_metadata tests for Pandas path."""

    @pytest.fixture
    def mock_engine(self):
        return MockEngine()

    def test_metadata_with_unnamed_tests(self, mock_engine):
        """Tests without names use type value as fallback."""
        invalid_df = pd.DataFrame({"id": [None]})
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["id"],
                on_fail=ContractSeverity.QUARANTINE,
            )
        ]
        config = QuarantineColumnsConfig(
            rejection_reason=True,
            rejected_at=False,
            source_batch_id=False,
            failed_tests=True,
            original_node=False,
        )
        result = add_quarantine_metadata(
            invalid_df,
            test_results={},
            config=config,
            engine=mock_engine,
            node_name="node",
            run_id="run-1",
            tests=tests,
        )
        assert "not_null" in result["_failed_tests"].iloc[0]

    def test_metadata_only_original_node(self, mock_engine):
        """Only original_node column is added when others are disabled."""
        invalid_df = pd.DataFrame({"id": [None]})
        tests = [
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["id"],
                on_fail=ContractSeverity.QUARANTINE,
            )
        ]
        config = QuarantineColumnsConfig(
            rejection_reason=False,
            rejected_at=False,
            source_batch_id=False,
            failed_tests=False,
            original_node=True,
        )
        result = add_quarantine_metadata(
            invalid_df,
            test_results={},
            config=config,
            engine=mock_engine,
            node_name="my_node",
            run_id="run-1",
            tests=tests,
        )
        assert "_original_node" in result.columns
        assert result["_original_node"].iloc[0] == "my_node"
        assert "_rejection_reason" not in result.columns
        assert "_rejected_at" not in result.columns
        assert "_source_batch_id" not in result.columns
        assert "_failed_tests" not in result.columns

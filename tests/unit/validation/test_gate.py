"""Comprehensive tests for odibi/validation/gate.py.

Coverage targets:
- evaluate_gate function
- _check_row_count function
- _get_previous_row_count function
- GateResult dataclass
- All gate failure scenarios
- Engine-specific paths
"""

import pandas as pd
import pytest

from odibi.config import (
    GateConfig,
    GateOnFail,
    GateThreshold,
    RowCountGate,
)
from odibi.validation.gate import (
    GateResult,
    _check_row_count,
    _get_previous_row_count,
    evaluate_gate,
)


class MockEngine:
    """Mock engine for testing without actual Spark/Pandas engines."""

    name = "pandas"

    def count_rows(self, df):
        return len(df)

    def materialize(self, df):
        return df


class MockCatalog:
    """Mock catalog manager for testing historical row counts."""

    def __init__(self, previous_row_count=None, use_query=False):
        self._previous_count = previous_row_count
        self._use_query = use_query

    def get_last_run_metrics(self, node_name):
        if not self._use_query and self._previous_count is not None:
            return {"rows_processed": self._previous_count}
        return None

    def query(self, table, filter=None, order_by=None, limit=None):
        if self._use_query and self._previous_count is not None:
            return [{"rows_processed": self._previous_count}]
        return []


class MockCatalogNoMethods:
    """Mock catalog without expected methods."""

    pass


@pytest.fixture
def sample_df():
    """Sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "value": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        }
    )


@pytest.fixture
def mock_engine():
    return MockEngine()


class TestEvaluateGate:
    """Tests for evaluate_gate function."""

    def test_gate_passes_when_above_threshold(self, sample_df, mock_engine):
        """Gate passes when pass rate exceeds threshold."""
        test_results = {"not_null": [True] * 10}
        gate_config = GateConfig(require_pass_rate=0.95)

        result = evaluate_gate(
            sample_df,
            test_results,
            gate_config,
            mock_engine,
        )

        assert result.passed is True
        assert result.pass_rate == 1.0
        assert result.total_rows == 10
        assert result.passed_rows == 10
        assert result.failed_rows == 0

    def test_gate_fails_when_below_threshold(self, sample_df, mock_engine):
        """Gate fails when pass rate is below threshold."""
        test_results = {
            "not_null": [True, True, True, True, True, False, False, False, False, False]
        }
        gate_config = GateConfig(require_pass_rate=0.95)

        result = evaluate_gate(
            sample_df,
            test_results,
            gate_config,
            mock_engine,
        )

        assert result.passed is False
        assert result.pass_rate == 0.5
        assert result.failed_rows == 5
        assert len(result.failure_reasons) > 0

    def test_gate_passes_at_exact_threshold(self, sample_df, mock_engine):
        """Gate passes when pass rate exactly equals threshold."""
        test_results = {"test": [True] * 9 + [False]}
        gate_config = GateConfig(require_pass_rate=0.9)

        result = evaluate_gate(
            sample_df,
            test_results,
            gate_config,
            mock_engine,
        )

        assert result.passed is True
        assert result.pass_rate == 0.9

    def test_per_test_thresholds(self, sample_df, mock_engine):
        """Per-test thresholds override global threshold."""
        test_results = {
            "not_null": [True] * 8 + [False] * 2,
            "unique": [True] * 10,
        }
        gate_config = GateConfig(
            require_pass_rate=0.5,
            thresholds=[
                GateThreshold(test="not_null", min_pass_rate=0.99),
            ],
        )

        result = evaluate_gate(
            sample_df,
            test_results,
            gate_config,
            mock_engine,
        )

        assert result.passed is False
        assert "not_null" in result.failure_reasons[0]

    def test_per_test_threshold_pass(self, sample_df, mock_engine):
        """Per-test threshold passes when met."""
        test_results = {
            "not_null": [True] * 10,
            "unique": [True] * 9 + [False],
        }
        gate_config = GateConfig(
            require_pass_rate=0.5,
            thresholds=[
                GateThreshold(test="not_null", min_pass_rate=1.0),
            ],
        )

        result = evaluate_gate(
            sample_df,
            test_results,
            gate_config,
            mock_engine,
        )

        assert result.passed is True

    def test_multiple_per_test_thresholds(self, sample_df, mock_engine):
        """Multiple per-test thresholds are all evaluated."""
        test_results = {
            "not_null": [True] * 8 + [False] * 2,
            "unique": [True] * 7 + [False] * 3,
        }
        gate_config = GateConfig(
            require_pass_rate=0.5,
            thresholds=[
                GateThreshold(test="not_null", min_pass_rate=0.9),
                GateThreshold(test="unique", min_pass_rate=0.9),
            ],
        )

        result = evaluate_gate(
            sample_df,
            test_results,
            gate_config,
            mock_engine,
        )

        assert result.passed is False
        assert len(result.failure_reasons) == 2

    def test_empty_dataset_passes(self, mock_engine):
        """Empty dataset passes by default."""
        empty_df = pd.DataFrame(columns=["id", "value"])
        gate_config = GateConfig(require_pass_rate=0.95)

        result = evaluate_gate(
            empty_df,
            {},
            gate_config,
            mock_engine,
        )

        assert result.passed is True
        assert result.total_rows == 0

    def test_no_test_results(self, sample_df, mock_engine):
        """Gate passes with no test results (all rows considered passed)."""
        gate_config = GateConfig(require_pass_rate=0.95)

        result = evaluate_gate(
            sample_df,
            {},
            gate_config,
            mock_engine,
        )

        assert result.passed is True
        assert result.pass_rate == 1.0

    def test_details_contain_per_test_rates(self, sample_df, mock_engine):
        """Details include per-test pass rates."""
        test_results = {
            "not_null": [True] * 8 + [False] * 2,
        }
        gate_config = GateConfig(
            require_pass_rate=0.5,
            thresholds=[
                GateThreshold(test="not_null", min_pass_rate=0.7),
            ],
        )

        result = evaluate_gate(
            sample_df,
            test_results,
            gate_config,
            mock_engine,
        )

        assert "per_test_rates" in result.details
        assert "not_null" in result.details["per_test_rates"]
        assert result.details["per_test_rates"]["not_null"] == 0.8


class TestEvaluateGateRowCount:
    """Tests for evaluate_gate with row count checks."""

    def test_row_count_min_violation(self, mock_engine):
        """Gate fails when row count is below minimum."""
        small_df = pd.DataFrame({"id": [1, 2, 3]})
        gate_config = GateConfig(
            require_pass_rate=0.95,
            row_count=RowCountGate(min=100),
        )

        result = evaluate_gate(
            small_df,
            {},
            gate_config,
            mock_engine,
        )

        assert result.passed is False
        assert "< minimum" in result.failure_reasons[0]

    def test_row_count_max_violation(self, mock_engine):
        """Gate fails when row count exceeds maximum."""
        large_df = pd.DataFrame({"id": range(1000)})
        gate_config = GateConfig(
            require_pass_rate=0.95,
            row_count=RowCountGate(max=100),
        )

        result = evaluate_gate(
            large_df,
            {},
            gate_config,
            mock_engine,
        )

        assert result.passed is False
        assert "> maximum" in result.failure_reasons[0]

    def test_row_count_within_bounds(self, sample_df, mock_engine):
        """Gate passes when row count is within bounds."""
        gate_config = GateConfig(
            require_pass_rate=0.95,
            row_count=RowCountGate(min=5, max=20),
        )

        result = evaluate_gate(
            sample_df,
            {},
            gate_config,
            mock_engine,
        )

        assert result.passed is True

    def test_row_count_change_threshold(self, sample_df, mock_engine):
        """Gate fails when row count changes beyond threshold."""
        catalog = MockCatalog(previous_row_count=100)
        gate_config = GateConfig(
            require_pass_rate=0.95,
            row_count=RowCountGate(change_threshold=0.5),
        )

        result = evaluate_gate(
            sample_df,
            {},
            gate_config,
            mock_engine,
            catalog=catalog,
            node_name="test_node",
        )

        assert result.passed is False
        assert "changed" in result.failure_reasons[0]

    def test_row_count_change_within_threshold(self, sample_df, mock_engine):
        """Gate passes when row count change is within threshold."""
        catalog = MockCatalog(previous_row_count=9)
        gate_config = GateConfig(
            require_pass_rate=0.95,
            row_count=RowCountGate(change_threshold=0.5),
        )

        result = evaluate_gate(
            sample_df,
            {},
            gate_config,
            mock_engine,
            catalog=catalog,
            node_name="test_node",
        )

        assert result.passed is True

    def test_row_count_no_catalog(self, sample_df, mock_engine):
        """Gate passes change check when no catalog provided."""
        gate_config = GateConfig(
            require_pass_rate=0.95,
            row_count=RowCountGate(change_threshold=0.5),
        )

        result = evaluate_gate(
            sample_df,
            {},
            gate_config,
            mock_engine,
            catalog=None,
            node_name="test_node",
        )

        assert result.passed is True

    def test_row_count_details_included(self, sample_df, mock_engine):
        """Row count check details are included in result."""
        gate_config = GateConfig(
            require_pass_rate=0.95,
            row_count=RowCountGate(min=5, max=20),
        )

        result = evaluate_gate(
            sample_df,
            {},
            gate_config,
            mock_engine,
        )

        assert "row_count_check" in result.details
        assert result.details["row_count_check"]["passed"] is True
        assert result.details["row_count_check"]["current_count"] == 10


class TestCheckRowCount:
    """Tests for _check_row_count function."""

    def test_min_violation(self):
        """Returns failure when below minimum."""
        config = RowCountGate(min=100)
        result = _check_row_count(10, config, None, None)
        assert result["passed"] is False
        assert "< minimum" in result["reason"]

    def test_max_violation(self):
        """Returns failure when above maximum."""
        config = RowCountGate(max=100)
        result = _check_row_count(200, config, None, None)
        assert result["passed"] is False
        assert "> maximum" in result["reason"]

    def test_within_bounds(self):
        """Returns success when within bounds."""
        config = RowCountGate(min=5, max=100)
        result = _check_row_count(50, config, None, None)
        assert result["passed"] is True

    def test_change_threshold_exceeded(self):
        """Returns failure when change exceeds threshold."""
        config = RowCountGate(change_threshold=0.1)
        catalog = MockCatalog(previous_row_count=100)
        result = _check_row_count(50, config, catalog, "test_node")
        assert result["passed"] is False
        assert "changed" in result["reason"]
        assert result["change_percent"] == 0.5

    def test_change_threshold_within(self):
        """Returns success when change is within threshold."""
        config = RowCountGate(change_threshold=0.2)
        catalog = MockCatalog(previous_row_count=100)
        result = _check_row_count(90, config, catalog, "test_node")
        assert result["passed"] is True

    def test_no_previous_count(self):
        """Returns success when no previous count available."""
        config = RowCountGate(change_threshold=0.1)
        catalog = MockCatalog(previous_row_count=None)
        result = _check_row_count(100, config, catalog, "test_node")
        assert result["passed"] is True

    def test_previous_count_zero(self):
        """Returns success when previous count is zero (avoid division by zero)."""
        config = RowCountGate(change_threshold=0.1)
        catalog = MockCatalog(previous_row_count=0)
        result = _check_row_count(100, config, catalog, "test_node")
        assert result["passed"] is True


class TestGetPreviousRowCount:
    """Tests for _get_previous_row_count function."""

    def test_get_from_metrics(self):
        """Gets row count from get_last_run_metrics."""
        catalog = MockCatalog(previous_row_count=500)
        result = _get_previous_row_count(catalog, "test_node")
        assert result == 500

    def test_get_from_query(self):
        """Gets row count from query fallback."""
        catalog = MockCatalog(previous_row_count=300, use_query=True)
        result = _get_previous_row_count(catalog, "test_node")
        assert result == 300

    def test_no_metrics_available(self):
        """Returns None when no metrics available."""
        catalog = MockCatalog(previous_row_count=None)
        result = _get_previous_row_count(catalog, "test_node")
        assert result is None

    def test_catalog_without_methods(self):
        """Returns None when catalog has no expected methods."""
        catalog = MockCatalogNoMethods()
        result = _get_previous_row_count(catalog, "test_node")
        assert result is None


class TestGateResult:
    """Tests for GateResult dataclass."""

    def test_gate_result_attributes(self):
        """GateResult has all expected attributes."""
        result = GateResult(
            passed=True,
            pass_rate=0.95,
            total_rows=100,
            passed_rows=95,
            failed_rows=5,
        )

        assert result.passed is True
        assert result.pass_rate == 0.95
        assert result.total_rows == 100
        assert result.passed_rows == 95
        assert result.failed_rows == 5
        assert result.action == GateOnFail.ABORT

    def test_gate_result_with_custom_action(self):
        """GateResult accepts custom action."""
        result = GateResult(
            passed=False,
            pass_rate=0.5,
            total_rows=100,
            passed_rows=50,
            failed_rows=50,
            action=GateOnFail.WARN_AND_WRITE,
        )

        assert result.action == GateOnFail.WARN_AND_WRITE

    def test_gate_result_with_details(self):
        """GateResult includes details dict."""
        result = GateResult(
            passed=True,
            pass_rate=1.0,
            total_rows=100,
            passed_rows=100,
            failed_rows=0,
            details={"per_test_rates": {"not_null": 1.0}},
        )

        assert "per_test_rates" in result.details

    def test_gate_result_with_failure_reasons(self):
        """GateResult includes failure reasons list."""
        result = GateResult(
            passed=False,
            pass_rate=0.5,
            total_rows=100,
            passed_rows=50,
            failed_rows=50,
            failure_reasons=["Pass rate 50% < required 95%"],
        )

        assert len(result.failure_reasons) == 1


class TestGateConfig:
    """Tests for GateConfig validation."""

    def test_default_values(self):
        """GateConfig has sensible defaults."""
        config = GateConfig()

        assert config.require_pass_rate == 0.95
        assert config.on_fail == GateOnFail.ABORT
        assert config.thresholds == []
        assert config.row_count is None

    def test_custom_values(self):
        """GateConfig accepts custom values."""
        config = GateConfig(
            require_pass_rate=0.99,
            on_fail=GateOnFail.WARN_AND_WRITE,
            thresholds=[
                GateThreshold(test="not_null", min_pass_rate=1.0),
            ],
            row_count=RowCountGate(min=100, max=10000),
        )

        assert config.require_pass_rate == 0.99
        assert config.on_fail == GateOnFail.WARN_AND_WRITE
        assert len(config.thresholds) == 1
        assert config.row_count.min == 100


class TestCheckRowCountExceptionHandling:
    """Tests for exception handling in _check_row_count."""

    def test_catalog_exception_is_handled(self):
        """Exception from catalog is caught and logged."""

        class FailingCatalog:
            def get_last_run_metrics(self, node_name):
                raise RuntimeError("Database connection failed")

        config = RowCountGate(change_threshold=0.1)
        result = _check_row_count(100, config, FailingCatalog(), "test_node")
        assert result["passed"] is True

    def test_query_exception_is_handled(self):
        """Exception from catalog.query is caught."""

        class FailingQueryCatalog:
            def get_last_run_metrics(self, node_name):
                return None

            def query(self, table, **kwargs):
                raise RuntimeError("Query failed")

        config = RowCountGate(change_threshold=0.1)
        result = _check_row_count(100, config, FailingQueryCatalog(), "test_node")
        assert result["passed"] is True

    def test_change_calculation_exception_handled(self):
        """Exception during change calculation is caught (lines 213-214)."""

        class BadValueCatalog:
            call_count = 0

            def get_last_run_metrics(self, node_name):
                self.call_count += 1
                if self.call_count > 1:
                    raise RuntimeError("Connection dropped")
                return {"rows_processed": "not_a_number"}

        config = RowCountGate(change_threshold=0.1)
        result = _check_row_count(100, config, BadValueCatalog(), "test_node")
        assert result["passed"] is True


class TestGetPreviousRowCountExceptions:
    """Tests for exception handling in _get_previous_row_count."""

    def test_exception_during_metrics_lookup(self):
        """Exception returns None gracefully."""

        class ExceptionCatalog:
            def get_last_run_metrics(self, node_name):
                raise ValueError("Unexpected error")

        result = _get_previous_row_count(ExceptionCatalog(), "test_node")
        assert result is None

    def test_exception_during_query(self):
        """Exception during query returns None."""

        class QueryExceptionCatalog:
            def get_last_run_metrics(self, node_name):
                return None

            def query(self, table, **kwargs):
                raise ConnectionError("Network error")

        result = _get_previous_row_count(QueryExceptionCatalog(), "test_node")
        assert result is None


class TestEvaluateGateEngineDetection:
    """Tests for engine detection in evaluate_gate."""

    def test_engine_without_count_rows(self, sample_df):
        """Engine without count_rows falls back to len()."""

        class SimpleEngine:
            pass

        test_results = {"test": [True] * 10}
        gate_config = GateConfig(require_pass_rate=0.95)

        result = evaluate_gate(sample_df, test_results, gate_config, SimpleEngine())

        assert result.total_rows == 10

    def test_engine_with_count_rows_method(self, sample_df):
        """Engine with count_rows uses that method."""

        class CountRowsEngine:
            def count_rows(self, df):
                return 999

        test_results = {"test": [True] * 10}
        gate_config = GateConfig(require_pass_rate=0.95)

        result = evaluate_gate(sample_df, test_results, gate_config, CountRowsEngine())

        assert result.total_rows == 999


class TestGateOnFailActions:
    """Tests for different gate failure actions."""

    def test_abort_action(self, sample_df, mock_engine):
        """Gate with ABORT action returns correct action."""
        test_results = {"test": [True] * 5 + [False] * 5}
        gate_config = GateConfig(
            require_pass_rate=0.95,
            on_fail=GateOnFail.ABORT,
        )

        result = evaluate_gate(sample_df, test_results, gate_config, mock_engine)

        assert result.passed is False
        assert result.action == GateOnFail.ABORT

    def test_warn_and_write_action(self, sample_df, mock_engine):
        """Gate with WARN_AND_WRITE action returns correct action."""
        test_results = {"test": [True] * 5 + [False] * 5}
        gate_config = GateConfig(
            require_pass_rate=0.95,
            on_fail=GateOnFail.WARN_AND_WRITE,
        )

        result = evaluate_gate(sample_df, test_results, gate_config, mock_engine)

        assert result.passed is False
        assert result.action == GateOnFail.WARN_AND_WRITE

    def test_write_valid_only_action(self, sample_df, mock_engine):
        """Gate with WRITE_VALID_ONLY action returns correct action."""
        test_results = {"test": [True] * 5 + [False] * 5}
        gate_config = GateConfig(
            require_pass_rate=0.95,
            on_fail=GateOnFail.WRITE_VALID_ONLY,
        )

        result = evaluate_gate(sample_df, test_results, gate_config, mock_engine)

        assert result.passed is False
        assert result.action == GateOnFail.WRITE_VALID_ONLY

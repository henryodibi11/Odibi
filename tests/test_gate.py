"""Tests for quality gate functionality."""

import pandas as pd
import pytest

from odibi.config import (
    GateConfig,
    GateOnFail,
    GateThreshold,
    RowCountGate,
    ValidationConfig,
)
from odibi.exceptions import GateFailedError
from odibi.validation.gate import GateResult, evaluate_gate


class MockEngine:
    """Mock engine for testing without actual Spark/Pandas engines."""

    name = "pandas"

    def count_rows(self, df):
        return len(df)

    def materialize(self, df):
        return df


class MockCatalog:
    """Mock catalog manager for testing historical row counts."""

    def __init__(self, previous_row_count=None):
        self._previous_count = previous_row_count

    def get_last_run_metrics(self, node_name):
        if self._previous_count is not None:
            return {"rows_processed": self._previous_count}
        return None


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


class TestGateConfig:
    """Tests for GateConfig validation."""

    def test_default_values(self):
        """GateConfig has sensible defaults."""
        config = GateConfig()

        assert config.require_pass_rate == 0.95
        assert config.on_fail == GateOnFail.ABORT
        assert config.thresholds == []
        assert config.row_count is None

    def test_custom_thresholds(self):
        """GateConfig accepts custom thresholds."""
        config = GateConfig(
            require_pass_rate=0.99,
            on_fail=GateOnFail.WARN_AND_WRITE,
            thresholds=[
                GateThreshold(test="not_null", min_pass_rate=1.0),
                GateThreshold(test="unique", min_pass_rate=0.99),
            ],
        )

        assert config.require_pass_rate == 0.99
        assert config.on_fail == GateOnFail.WARN_AND_WRITE
        assert len(config.thresholds) == 2

    def test_row_count_config(self):
        """GateConfig accepts row_count configuration."""
        config = GateConfig(
            row_count=RowCountGate(min=100, max=10000, change_threshold=0.5),
        )

        assert config.row_count.min == 100
        assert config.row_count.max == 10000
        assert config.row_count.change_threshold == 0.5


class TestValidationConfigWithGate:
    """Tests for ValidationConfig with gate support."""

    def test_validation_config_with_gate(self):
        """ValidationConfig accepts gate configuration."""
        config = ValidationConfig(
            tests=[],
            gate=GateConfig(require_pass_rate=0.99),
        )

        assert config.gate is not None
        assert config.gate.require_pass_rate == 0.99

    def test_validation_config_without_gate(self):
        """ValidationConfig works without gate configuration."""
        config = ValidationConfig(tests=[])

        assert config.gate is None


class TestGateFailedError:
    """Tests for GateFailedError exception."""

    def test_error_formatting(self):
        """GateFailedError formats message correctly."""
        error = GateFailedError(
            node_name="test_node",
            pass_rate=0.8,
            required_rate=0.95,
            failed_rows=200,
            total_rows=1000,
            failure_reasons=["Pass rate 80.0% < required 95.0%"],
        )

        error_str = str(error)
        assert "test_node" in error_str
        assert "80.0%" in error_str
        assert "95.0%" in error_str
        assert "200" in error_str

    def test_error_attributes(self):
        """GateFailedError has correct attributes."""
        error = GateFailedError(
            node_name="test_node",
            pass_rate=0.8,
            required_rate=0.95,
            failed_rows=200,
            total_rows=1000,
        )

        assert error.node_name == "test_node"
        assert error.pass_rate == 0.8
        assert error.required_rate == 0.95
        assert error.failed_rows == 200
        assert error.total_rows == 1000


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

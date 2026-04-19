"""
Comprehensive coverage tests for odibi/validation/gate.py.

Focuses on edge cases, MagicMock-based catalog/engine mocking,
and scenarios not covered by test_gate.py.
"""

import pandas as pd
import pytest
from unittest.mock import MagicMock

from odibi.config import (
    GateConfig,
    GateOnFail,
    GateThreshold,
    RowCountGate,
)
from odibi.validation.gate import (
    _check_row_count,
    _get_previous_row_count,
    evaluate_gate,
)


@pytest.fixture
def df_10():
    """10-row DataFrame."""
    return pd.DataFrame({"id": range(10), "val": range(10)})


@pytest.fixture
def engine():
    """MagicMock engine without spark attribute."""
    eng = MagicMock()
    del eng.spark  # ensure is_spark branch is False
    eng.count_rows = MagicMock(side_effect=lambda df: len(df))
    return eng


# ---------------------------------------------------------------------------
# evaluate_gate
# ---------------------------------------------------------------------------


class TestEvaluateGateAllPass:
    """Gate passes when every row passes every test."""

    def test_all_rows_pass(self, df_10, engine):
        results = {"not_null": [True] * 10, "range": [True] * 10}
        gate = GateConfig(require_pass_rate=0.95)

        out = evaluate_gate(df_10, results, gate, engine)

        assert out.passed is True
        assert out.pass_rate == 1.0
        assert out.total_rows == 10
        assert out.passed_rows == 10
        assert out.failed_rows == 0


class TestEvaluateGateBelowRequirePassRate:
    """Gate fails when overall pass rate is below require_pass_rate."""

    def test_below_threshold_gives_failure_reason(self, df_10, engine):
        results = {"chk": [True] * 4 + [False] * 6}
        gate = GateConfig(require_pass_rate=0.95)

        out = evaluate_gate(df_10, results, gate, engine)

        assert out.passed is False
        assert out.pass_rate == 0.4
        assert any("Overall pass rate" in r for r in out.failure_reasons)


class TestEvaluateGateEmptyDataFrame:
    """Empty DataFrame yields a passing gate with zero counts."""

    def test_empty_df(self, engine):
        df = pd.DataFrame(columns=["a"])
        gate = GateConfig(require_pass_rate=0.99)

        out = evaluate_gate(df, {}, gate, engine)

        assert out.passed is True
        assert out.pass_rate == 1.0
        assert out.total_rows == 0
        assert out.details.get("message") == "Empty dataset - gate passed by default"


class TestEvaluateGatePerTestThresholds:
    """Per-test threshold failures."""

    def test_one_test_below_threshold_fails(self, df_10, engine):
        results = {
            "not_null": [True] * 10,
            "range_check": [True] * 7 + [False] * 3,
        }
        gate = GateConfig(
            require_pass_rate=0.5,
            thresholds=[GateThreshold(test="range_check", min_pass_rate=0.9)],
        )

        out = evaluate_gate(df_10, results, gate, engine)

        assert out.passed is False
        assert any("range_check" in r for r in out.failure_reasons)
        assert out.details["per_test_rates"]["range_check"] == 0.7

    def test_all_tests_above_threshold_passes(self, df_10, engine):
        results = {
            "not_null": [True] * 10,
            "unique": [True] * 9 + [False],
        }
        gate = GateConfig(
            require_pass_rate=0.5,
            thresholds=[
                GateThreshold(test="not_null", min_pass_rate=0.95),
                GateThreshold(test="unique", min_pass_rate=0.8),
            ],
        )

        out = evaluate_gate(df_10, results, gate, engine)

        assert out.passed is True


class TestEvaluateGateNoValidationResults:
    """No validation_results means all rows are considered passed."""

    def test_none_dict(self, df_10, engine):
        gate = GateConfig(require_pass_rate=0.95)

        out = evaluate_gate(df_10, {}, gate, engine)

        assert out.passed is True
        assert out.pass_rate == 1.0
        assert out.passed_rows == 10


class TestEvaluateGateRowCountMin:
    """Row count gate with min violation."""

    def test_row_count_min_fails(self, engine):
        df = pd.DataFrame({"id": [1, 2]})
        gate = GateConfig(
            require_pass_rate=0.0,
            row_count=RowCountGate(min=10),
        )

        out = evaluate_gate(df, {}, gate, engine)

        assert out.passed is False
        assert any("minimum" in r for r in out.failure_reasons)


class TestEvaluateGateRowCountMax:
    """Row count gate with max violation."""

    def test_row_count_max_fails(self, engine):
        df = pd.DataFrame({"id": range(50)})
        gate = GateConfig(
            require_pass_rate=0.0,
            row_count=RowCountGate(max=10),
        )

        out = evaluate_gate(df, {}, gate, engine)

        assert out.passed is False
        assert any("maximum" in r for r in out.failure_reasons)


class TestEvaluateGateRowCountChangeThreshold:
    """Row count gate with change_threshold using mock catalog."""

    def test_change_exceeds_threshold(self, df_10, engine):
        catalog = MagicMock()
        catalog.get_last_run_metrics.return_value = {"rows_processed": 100}
        gate = GateConfig(
            require_pass_rate=0.0,
            row_count=RowCountGate(change_threshold=0.1),
        )

        out = evaluate_gate(df_10, {}, gate, engine, catalog=catalog, node_name="n1")

        assert out.passed is False
        assert any("changed" in r for r in out.failure_reasons)


class TestEvaluateGateOnFail:
    """on_fail value is propagated into the result."""

    @pytest.mark.parametrize(
        "on_fail",
        [GateOnFail.ABORT, GateOnFail.WARN_AND_WRITE, GateOnFail.WRITE_VALID_ONLY],
    )
    def test_on_fail_propagated(self, df_10, engine, on_fail):
        gate = GateConfig(require_pass_rate=0.99, on_fail=on_fail)

        out = evaluate_gate(df_10, {"t": [False] * 10}, gate, engine)

        assert out.action == on_fail


class TestEvaluateGateMultipleFailures:
    """Multiple failure reasons are accumulated."""

    def test_overall_and_per_test_and_row_count(self, engine):
        df = pd.DataFrame({"id": [1, 2]})
        results = {"chk": [True, False]}
        gate = GateConfig(
            require_pass_rate=0.99,
            thresholds=[GateThreshold(test="chk", min_pass_rate=0.99)],
            row_count=RowCountGate(min=100),
        )

        out = evaluate_gate(df, results, gate, engine)

        assert out.passed is False
        assert len(out.failure_reasons) == 3


class TestEvaluateGateResultFields:
    """GateResult fields are fully populated."""

    def test_fields_populated(self, df_10, engine):
        results = {"t": [True] * 8 + [False] * 2}
        gate = GateConfig(
            require_pass_rate=0.5,
            on_fail=GateOnFail.WARN_AND_WRITE,
            row_count=RowCountGate(min=1, max=100),
        )

        out = evaluate_gate(df_10, results, gate, engine)

        assert out.passed is True
        assert out.pass_rate == 0.8
        assert out.total_rows == 10
        assert out.passed_rows == 8
        assert out.failed_rows == 2
        assert out.action == GateOnFail.WARN_AND_WRITE
        assert isinstance(out.failure_reasons, list)
        assert "overall_pass_rate" in out.details
        assert "row_count_check" in out.details
        assert out.details["row_count_check"]["passed"] is True


class TestEvaluateGateMultipleTestsCombined:
    """Combined pass mask across multiple tests."""

    def test_intersection_of_tests(self, engine):
        df = pd.DataFrame({"id": range(4)})
        results = {
            "a": [True, True, False, True],
            "b": [True, False, True, True],
        }
        gate = GateConfig(require_pass_rate=0.5)

        out = evaluate_gate(df, results, gate, engine)

        # Combined: [True, False, False, True] → 2/4 = 0.5
        assert out.pass_rate == 0.5
        assert out.passed_rows == 2
        assert out.passed is True


class TestEvaluateGateTestResultLengthMismatch:
    """Test results with wrong length are skipped in combined mask."""

    def test_mismatched_length_ignored(self, engine):
        df = pd.DataFrame({"id": range(4)})
        results = {
            "good": [True, True, False, True],
            "bad_length": [True, False],  # len != total_rows → skipped
        }
        gate = GateConfig(require_pass_rate=0.5)

        out = evaluate_gate(df, results, gate, engine)

        # Only "good" participates → 3/4 = 0.75
        assert out.passed_rows == 3
        assert out.pass_rate == 0.75


class TestEvaluateGateEngineWithoutCountRows:
    """Engine without count_rows falls back to len()."""

    def test_fallback_to_len(self, df_10):
        eng = MagicMock(spec=[])  # no methods at all
        gate = GateConfig(require_pass_rate=0.0)

        out = evaluate_gate(df_10, {}, gate, eng)

        assert out.total_rows == 10


# ---------------------------------------------------------------------------
# _check_row_count
# ---------------------------------------------------------------------------


class TestCheckRowCountBelowMin:
    def test_below_min(self):
        cfg = RowCountGate(min=50)
        result = _check_row_count(10, cfg, None, None)
        assert result["passed"] is False
        assert "< minimum" in result["reason"]


class TestCheckRowCountAboveMax:
    def test_above_max(self):
        cfg = RowCountGate(max=50)
        result = _check_row_count(100, cfg, None, None)
        assert result["passed"] is False
        assert "> maximum" in result["reason"]


class TestCheckRowCountWithinRange:
    def test_within_range(self):
        cfg = RowCountGate(min=5, max=100)
        result = _check_row_count(50, cfg, None, None)
        assert result["passed"] is True
        assert result["reason"] == ""


class TestCheckRowCountChangeExceeded:
    def test_change_exceeded(self):
        catalog = MagicMock()
        catalog.get_last_run_metrics.return_value = {"rows_processed": 100}
        cfg = RowCountGate(change_threshold=0.1)

        result = _check_row_count(50, cfg, catalog, "node_x")

        assert result["passed"] is False
        assert result["change_percent"] == 0.5
        assert "changed" in result["reason"]


class TestCheckRowCountChangeNotExceeded:
    def test_change_within(self):
        catalog = MagicMock()
        catalog.get_last_run_metrics.return_value = {"rows_processed": 100}
        cfg = RowCountGate(change_threshold=0.5)

        result = _check_row_count(95, cfg, catalog, "node_x")

        assert result["passed"] is True


class TestCheckRowCountNoCatalog:
    def test_no_catalog_skips_historical(self):
        cfg = RowCountGate(change_threshold=0.1)
        result = _check_row_count(50, cfg, None, "node_x")
        assert result["passed"] is True


class TestCheckRowCountNoNodeName:
    def test_no_node_name_skips_historical(self):
        catalog = MagicMock()
        cfg = RowCountGate(change_threshold=0.1)
        result = _check_row_count(50, cfg, catalog, None)
        assert result["passed"] is True


class TestCheckRowCountCatalogQueryFailure:
    def test_catalog_exception_still_passes(self):
        catalog = MagicMock()
        catalog.get_last_run_metrics.side_effect = RuntimeError("db down")
        cfg = RowCountGate(change_threshold=0.1)

        result = _check_row_count(50, cfg, catalog, "node_x")

        assert result["passed"] is True


class TestCheckRowCountNoPreviousCount:
    def test_no_previous_count_passes(self):
        catalog = MagicMock()
        catalog.get_last_run_metrics.return_value = None
        # Also make query return empty to exercise both paths
        catalog.query.return_value = []
        cfg = RowCountGate(change_threshold=0.1)

        result = _check_row_count(50, cfg, catalog, "node_x")

        assert result["passed"] is True


# ---------------------------------------------------------------------------
# _get_previous_row_count
# ---------------------------------------------------------------------------


class TestGetPreviousRowCountFromMetrics:
    def test_returns_rows_processed(self):
        catalog = MagicMock()
        catalog.get_last_run_metrics.return_value = {"rows_processed": 250}

        assert _get_previous_row_count(catalog, "n") == 250


class TestGetPreviousRowCountFromQuery:
    def test_falls_back_to_query(self):
        catalog = MagicMock()
        catalog.get_last_run_metrics.return_value = None
        catalog.query.return_value = [{"rows_processed": 300}]

        assert _get_previous_row_count(catalog, "n") == 300


class TestGetPreviousRowCountNoMethods:
    def test_no_methods_returns_none(self):
        catalog = MagicMock(spec=[])  # no get_last_run_metrics / query

        assert _get_previous_row_count(catalog, "n") is None


class TestGetPreviousRowCountException:
    def test_exception_returns_none(self):
        catalog = MagicMock()
        catalog.get_last_run_metrics.side_effect = Exception("boom")

        assert _get_previous_row_count(catalog, "n") is None


class TestGetPreviousRowCountEmptyQueryResults:
    def test_empty_query_returns_none(self):
        catalog = MagicMock()
        catalog.get_last_run_metrics.return_value = None
        catalog.query.return_value = []

        assert _get_previous_row_count(catalog, "n") is None


class TestGetPreviousRowCountMetricsWithoutKey:
    def test_metrics_missing_key_falls_to_query(self):
        catalog = MagicMock()
        catalog.get_last_run_metrics.return_value = {"other_metric": 42}
        catalog.query.return_value = [{"rows_processed": 999}]

        assert _get_previous_row_count(catalog, "n") == 999

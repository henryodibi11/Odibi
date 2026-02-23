"""Unit tests for NodeExecutor validation phase: quarantine, gates, and standard validation.

Covers issue #282 — lines 1905-2119 of node.py.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.config import (
    GateConfig,
    GateOnFail,
    NodeConfig,
    NotNullTest,
    QuarantineColumnsConfig,
    QuarantineConfig,
    ValidationAction,
    ValidationConfig,
    VolumeDropTest,
)
from odibi.exceptions import GateFailedError, ValidationError
from odibi.node import NodeExecutor


@pytest.fixture
def mock_context():
    return MagicMock()


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.name = "pandas"
    engine.materialize.side_effect = lambda df: df
    engine.count_rows.side_effect = lambda df: len(df) if df is not None else None
    return engine


@pytest.fixture
def connections():
    return {"src": MagicMock(), "dst": MagicMock()}


def _make_executor(mock_context, mock_engine, connections, catalog_manager=None):
    return NodeExecutor(
        mock_context,
        mock_engine,
        connections,
        catalog_manager=catalog_manager,
    )


def _make_config(validation_config, name="test_node"):
    return NodeConfig(
        name=name,
        read={"connection": "src", "format": "csv", "path": "input.csv"},
        write={"connection": "dst", "format": "csv", "path": "output.csv"},
        validation=validation_config,
    )


# ============================================================
# _execute_validation (standard FAIL/WARN modes)
# ============================================================


class TestExecuteValidationFailMode:
    """Test _execute_validation with mode=FAIL raises on failures."""

    def test_fail_mode_raises_on_validation_failure(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            ValidationConfig(
                mode=ValidationAction.FAIL,
                tests=[NotNullTest(type="not_null", columns=["id"])],
            )
        )
        df = pd.DataFrame({"id": [1, None, 3]})

        with pytest.raises(ValidationError):
            executor._execute_validation(config, df)

    def test_fail_mode_passes_clean_data(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            ValidationConfig(
                mode=ValidationAction.FAIL,
                tests=[NotNullTest(type="not_null", columns=["id"])],
            )
        )
        df = pd.DataFrame({"id": [1, 2, 3]})

        # Should not raise
        executor._execute_validation(config, df)


class TestExecuteValidationWarnMode:
    """Test _execute_validation with mode=WARN logs but doesn't raise."""

    def test_warn_mode_logs_warning_on_failure(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            ValidationConfig(
                mode=ValidationAction.WARN,
                tests=[NotNullTest(type="not_null", columns=["id"])],
            )
        )
        df = pd.DataFrame({"id": [1, None, 3]})

        # Should not raise
        executor._execute_validation(config, df)

        # Should have recorded warnings
        assert len(executor._validation_warnings) > 0
        assert len(executor._execution_steps) > 0

    def test_warn_mode_no_warnings_on_clean_data(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            ValidationConfig(
                mode=ValidationAction.WARN,
                tests=[NotNullTest(type="not_null", columns=["id"])],
            )
        )
        df = pd.DataFrame({"id": [1, 2, 3]})

        executor._execute_validation(config, df)

        assert len(executor._validation_warnings) == 0


# ============================================================
# _execute_validation_phase (no validation / None df)
# ============================================================


class TestValidationPhaseSkips:
    """Test that validation phase returns early when appropriate."""

    def test_returns_df_when_no_validation_config(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = NodeConfig(
            name="test_node",
            read={"connection": "src", "format": "csv", "path": "input.csv"},
        )
        df = pd.DataFrame({"id": [1, 2]})

        result = executor._execute_validation_phase(config, df)
        pd.testing.assert_frame_equal(result, df)

    def test_returns_none_when_df_is_none(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            ValidationConfig(
                tests=[NotNullTest(type="not_null", columns=["id"])],
            )
        )

        result = executor._execute_validation_phase(config, None)
        assert result is None


# ============================================================
# Volume drop validation
# ============================================================


class TestVolumeDrop:
    """Test volume_drop validation within _execute_validation_phase."""

    def test_volume_drop_raises_when_threshold_exceeded(
        self, mock_context, mock_engine, connections
    ):
        catalog = MagicMock()
        catalog.get_average_volume.return_value = 100
        executor = _make_executor(mock_context, mock_engine, connections, catalog)

        config = _make_config(
            ValidationConfig(
                tests=[VolumeDropTest(type="volume_drop", threshold=0.3, lookback_days=7)],
            )
        )
        # 50 rows vs avg 100 = 50% drop, threshold is 30%
        df = pd.DataFrame({"id": range(50)})

        with pytest.raises(ValidationError, match="Volume dropped"):
            executor._execute_validation_phase(config, df)

    def test_volume_drop_passes_within_threshold(self, mock_context, mock_engine, connections):
        catalog = MagicMock()
        catalog.get_average_volume.return_value = 100
        executor = _make_executor(mock_context, mock_engine, connections, catalog)

        config = _make_config(
            ValidationConfig(
                tests=[VolumeDropTest(type="volume_drop", threshold=0.5, lookback_days=7)],
            )
        )
        # 80 rows vs avg 100 = 20% drop, threshold is 50%
        df = pd.DataFrame({"id": range(80)})

        result = executor._execute_validation_phase(config, df)
        assert result is not None

    def test_volume_drop_skipped_without_catalog(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)

        config = _make_config(
            ValidationConfig(
                tests=[VolumeDropTest(type="volume_drop", threshold=0.3, lookback_days=7)],
            )
        )
        df = pd.DataFrame({"id": range(10)})

        # Should not raise — no catalog_manager
        result = executor._execute_validation_phase(config, df)
        assert result is not None

    def test_volume_drop_skipped_when_no_historical_data(
        self, mock_context, mock_engine, connections
    ):
        catalog = MagicMock()
        catalog.get_average_volume.return_value = None
        executor = _make_executor(mock_context, mock_engine, connections, catalog)

        config = _make_config(
            ValidationConfig(
                tests=[VolumeDropTest(type="volume_drop", threshold=0.3, lookback_days=7)],
            )
        )
        df = pd.DataFrame({"id": range(10)})

        result = executor._execute_validation_phase(config, df)
        assert result is not None

    def test_volume_drop_skipped_when_avg_is_zero(self, mock_context, mock_engine, connections):
        catalog = MagicMock()
        catalog.get_average_volume.return_value = 0
        executor = _make_executor(mock_context, mock_engine, connections, catalog)

        config = _make_config(
            ValidationConfig(
                tests=[VolumeDropTest(type="volume_drop", threshold=0.3, lookback_days=7)],
            )
        )
        df = pd.DataFrame({"id": range(10)})

        result = executor._execute_validation_phase(config, df)
        assert result is not None


# ============================================================
# Quarantine
# ============================================================


class TestQuarantine:
    """Test quarantine split and write within _execute_validation_phase."""

    def test_quarantine_splits_valid_and_invalid_rows(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)

        config = _make_config(
            ValidationConfig(
                tests=[NotNullTest(type="not_null", columns=["id"], on_fail="quarantine")],
                quarantine=QuarantineConfig(
                    connection="dst",
                    path="quarantine_table",
                    add_columns=QuarantineColumnsConfig(),
                ),
            )
        )

        df = pd.DataFrame({"id": [1, None, 3], "val": ["a", "b", "c"]})

        with (
            patch("odibi.validation.quarantine.split_valid_invalid") as mock_split,
            patch("odibi.validation.quarantine.add_quarantine_metadata") as mock_add_meta,
            patch("odibi.validation.quarantine.write_quarantine") as mock_write_q,
            patch("odibi.validation.quarantine.has_quarantine_tests", return_value=True),
        ):
            # Set up quarantine result
            mock_result = MagicMock()
            mock_result.rows_quarantined = 1
            mock_result.valid_df = pd.DataFrame({"id": [1, 3], "val": ["a", "c"]})
            mock_result.invalid_df = pd.DataFrame({"id": [None], "val": ["b"]})
            mock_result.test_results = {"not_null_id": [True, False, True]}
            mock_split.return_value = mock_result
            mock_add_meta.return_value = mock_result.invalid_df

            result = executor._execute_validation_phase(config, df)

        # Valid rows returned
        assert len(result) == 2
        # Quarantine write was called
        mock_write_q.assert_called_once()
        # Execution steps logged
        assert any("Quarantined" in s for s in executor._execution_steps)

    def test_quarantine_no_invalid_rows_skips_write(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)

        config = _make_config(
            ValidationConfig(
                tests=[NotNullTest(type="not_null", columns=["id"], on_fail="quarantine")],
                quarantine=QuarantineConfig(
                    connection="dst",
                    path="quarantine_table",
                    add_columns=QuarantineColumnsConfig(),
                ),
            )
        )

        df = pd.DataFrame({"id": [1, 2, 3]})

        with (
            patch("odibi.validation.quarantine.split_valid_invalid") as mock_split,
            patch("odibi.validation.quarantine.write_quarantine") as mock_write_q,
            patch("odibi.validation.quarantine.has_quarantine_tests", return_value=True),
        ):
            mock_result = MagicMock()
            mock_result.rows_quarantined = 0
            mock_result.valid_df = df
            mock_result.test_results = {}
            mock_split.return_value = mock_result

            result = executor._execute_validation_phase(config, df)

        assert len(result) == 3
        mock_write_q.assert_not_called()


# ============================================================
# Quality Gates (_check_gate)
# ============================================================


class TestCheckGateAbort:
    """Test _check_gate with on_fail=ABORT."""

    def test_gate_abort_raises_on_failure(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            ValidationConfig(
                tests=[NotNullTest(type="not_null", columns=["id"])],
                gate=GateConfig(require_pass_rate=0.95, on_fail=GateOnFail.ABORT),
            )
        )
        df = pd.DataFrame({"id": [1, 2, 3]})
        gate_config = config.validation.gate

        with patch("odibi.validation.gate.evaluate_gate") as mock_eval:
            mock_gate_result = MagicMock()
            mock_gate_result.passed = False
            mock_gate_result.pass_rate = 0.80
            mock_gate_result.action = GateOnFail.ABORT
            mock_gate_result.failed_rows = 1
            mock_gate_result.total_rows = 5
            mock_gate_result.failure_reasons = ["not_null failed"]
            mock_eval.return_value = mock_gate_result

            with pytest.raises(GateFailedError):
                executor._check_gate(config, df, {}, gate_config)

    def test_gate_passes_records_step(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            ValidationConfig(
                tests=[NotNullTest(type="not_null", columns=["id"])],
                gate=GateConfig(require_pass_rate=0.95, on_fail=GateOnFail.ABORT),
            )
        )
        df = pd.DataFrame({"id": [1, 2, 3]})
        gate_config = config.validation.gate

        with patch("odibi.validation.gate.evaluate_gate") as mock_eval:
            mock_gate_result = MagicMock()
            mock_gate_result.passed = True
            mock_gate_result.pass_rate = 1.0
            mock_eval.return_value = mock_gate_result

            result = executor._check_gate(config, df, {}, gate_config)

        pd.testing.assert_frame_equal(result, df)
        assert any("Gate passed" in s for s in executor._execution_steps)


class TestCheckGateWarnAndWrite:
    """Test _check_gate with on_fail=WARN_AND_WRITE."""

    def test_warn_and_write_returns_all_rows(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            ValidationConfig(
                tests=[NotNullTest(type="not_null", columns=["id"])],
                gate=GateConfig(require_pass_rate=0.95, on_fail=GateOnFail.WARN_AND_WRITE),
            )
        )
        df = pd.DataFrame({"id": [1, None, 3]})
        gate_config = config.validation.gate

        with patch("odibi.validation.gate.evaluate_gate") as mock_eval:
            mock_gate_result = MagicMock()
            mock_gate_result.passed = False
            mock_gate_result.pass_rate = 0.67
            mock_gate_result.action = GateOnFail.WARN_AND_WRITE
            mock_gate_result.failure_reasons = ["not_null failed on id"]
            mock_eval.return_value = mock_gate_result

            result = executor._check_gate(config, df, {}, gate_config)

        # All rows returned (including invalid)
        assert len(result) == 3
        # Warnings recorded
        assert len(executor._validation_warnings) > 0

    def test_warn_and_write_records_execution_step(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            ValidationConfig(
                tests=[NotNullTest(type="not_null", columns=["id"])],
                gate=GateConfig(require_pass_rate=0.95, on_fail=GateOnFail.WARN_AND_WRITE),
            )
        )
        df = pd.DataFrame({"id": [1, 2]})
        gate_config = config.validation.gate

        with patch("odibi.validation.gate.evaluate_gate") as mock_eval:
            mock_gate_result = MagicMock()
            mock_gate_result.passed = False
            mock_gate_result.pass_rate = 0.50
            mock_gate_result.action = GateOnFail.WARN_AND_WRITE
            mock_gate_result.failure_reasons = ["test_fail"]
            mock_eval.return_value = mock_gate_result

            executor._check_gate(config, df, {}, gate_config)

        assert any("Gate failed" in s for s in executor._execution_steps)


class TestCheckGateWriteValidOnly:
    """Test _check_gate with on_fail=WRITE_VALID_ONLY."""

    def test_write_valid_only_returns_df_and_logs_step(
        self, mock_context, mock_engine, connections
    ):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            ValidationConfig(
                tests=[NotNullTest(type="not_null", columns=["id"])],
                gate=GateConfig(require_pass_rate=0.95, on_fail=GateOnFail.WRITE_VALID_ONLY),
            )
        )
        df = pd.DataFrame({"id": [1, 2, 3]})
        gate_config = config.validation.gate

        with patch("odibi.validation.gate.evaluate_gate") as mock_eval:
            mock_gate_result = MagicMock()
            mock_gate_result.passed = False
            mock_gate_result.pass_rate = 0.80
            mock_gate_result.action = GateOnFail.WRITE_VALID_ONLY
            mock_gate_result.passed_rows = 4
            mock_gate_result.total_rows = 5
            mock_eval.return_value = mock_gate_result

            result = executor._check_gate(config, df, {}, gate_config)

        assert result is not None
        assert any("Writing only valid rows" in s for s in executor._execution_steps)


# ============================================================
# End-to-end validation phase integration
# ============================================================


class TestValidationPhaseEndToEnd:
    """Test full _execute_validation_phase flow with gate + standard validation."""

    def test_validation_phase_with_gate_abort(self, mock_context, mock_engine, connections):
        """Gate abort should raise even if standard validation passes."""
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            ValidationConfig(
                mode=ValidationAction.FAIL,
                tests=[NotNullTest(type="not_null", columns=["id"])],
                gate=GateConfig(require_pass_rate=0.99, on_fail=GateOnFail.ABORT),
            )
        )
        df = pd.DataFrame({"id": [1, 2, 3]})

        with patch("odibi.validation.gate.evaluate_gate") as mock_eval:
            mock_gate_result = MagicMock()
            mock_gate_result.passed = False
            mock_gate_result.pass_rate = 0.90
            mock_gate_result.action = GateOnFail.ABORT
            mock_gate_result.failed_rows = 1
            mock_gate_result.total_rows = 10
            mock_gate_result.failure_reasons = ["threshold not met"]
            mock_eval.return_value = mock_gate_result

            with pytest.raises(GateFailedError):
                executor._execute_validation_phase(config, df)

    def test_validation_phase_no_gate_passes_cleanly(self, mock_context, mock_engine, connections):
        """No gate configured — just standard validation."""
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            ValidationConfig(
                mode=ValidationAction.FAIL,
                tests=[NotNullTest(type="not_null", columns=["id"])],
            )
        )
        df = pd.DataFrame({"id": [1, 2, 3]})

        result = executor._execute_validation_phase(config, df)
        pd.testing.assert_frame_equal(result, df)

    def test_validation_phase_standard_fail_raises_before_gate(
        self, mock_context, mock_engine, connections
    ):
        """Standard validation failure should raise even when gate is configured."""
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            ValidationConfig(
                mode=ValidationAction.FAIL,
                tests=[NotNullTest(type="not_null", columns=["id"])],
                gate=GateConfig(require_pass_rate=0.50, on_fail=GateOnFail.WARN_AND_WRITE),
            )
        )
        df = pd.DataFrame({"id": [1, None, 3]})

        with pytest.raises(ValidationError):
            executor._execute_validation_phase(config, df)

"""Unit tests for node.py core classes - targeting uncovered lines."""

import logging
from unittest.mock import Mock, patch

import pytest

from odibi.node import NodeResult, PhaseTimer, _override_log_level


class TestPhaseTimer:
    def test_phase_timing(self):
        timer = PhaseTimer()
        with timer.phase("read"):
            pass  # instant
        assert "read" in timer.summary()
        assert timer.get("read") >= 0

    def test_record_manual(self):
        timer = PhaseTimer()
        timer.record("transform", 1.234)
        assert timer.get("transform") == pytest.approx(1.234)

    def test_get_missing_phase(self):
        timer = PhaseTimer()
        assert timer.get("nonexistent") == 0

    def test_summary(self):
        timer = PhaseTimer()
        timer.record("a", 1.2345)
        timer.record("b", 2.3456)
        s = timer.summary()
        assert s["a"] == 1.234  # rounded to 3dp
        assert s["b"] == 2.346

    def test_summary_ms(self):
        timer = PhaseTimer()
        timer.record("load", 1.5)
        ms = timer.summary_ms()
        assert ms["load"] == 1500.0

    def test_accumulates_same_phase(self):
        timer = PhaseTimer()
        timer.record("x", 1.0)
        timer.record("x", 2.0)
        assert timer.get("x") == 3.0


class TestNodeResult:
    def test_basic_creation(self):
        result = NodeResult(node_name="test_node", success=True, duration=1.5)
        assert result.node_name == "test_node"
        assert result.success is True
        assert result.duration == 1.5
        assert result.rows_processed is None
        assert result.error is None

    def test_with_error(self):
        err = ValueError("something broke")
        result = NodeResult(node_name="fail_node", success=False, duration=0.1, error=err)
        assert result.success is False
        assert isinstance(result.error, ValueError)

    def test_with_metadata(self):
        result = NodeResult(
            node_name="n",
            success=True,
            duration=0.5,
            rows_processed=100,
            rows_read=100,
            rows_written=95,
            metadata={"format": "parquet"},
        )
        assert result.rows_processed == 100
        assert result.metadata["format"] == "parquet"


class TestOverrideLogLevel:
    def test_no_level_yields(self):
        with _override_log_level(None):
            pass  # should not raise

    def test_override_and_restore(self):
        mock_logger = Mock()
        mock_logger.level = logging.INFO
        mock_inner_logger = Mock()
        mock_logger.logger = mock_inner_logger

        with patch("odibi.utils.logging.logger", mock_logger):
            with _override_log_level("DEBUG"):
                assert mock_logger.level == logging.DEBUG
                mock_inner_logger.setLevel.assert_called_with(logging.DEBUG)

            # Restored
            assert mock_logger.level == logging.INFO

    def test_override_invalid_level_uses_original(self):
        mock_logger = Mock()
        mock_logger.level = logging.WARNING
        mock_inner_logger = Mock()
        mock_logger.logger = mock_inner_logger

        with patch("odibi.utils.logging.logger", mock_logger):
            with _override_log_level("NONEXISTENT"):
                # getattr returns original level for invalid name
                assert mock_logger.level == logging.WARNING

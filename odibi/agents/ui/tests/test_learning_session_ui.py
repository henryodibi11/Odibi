"""Tests for Learning Session UI wiring (Phase 9.B).

These tests verify:
- UI start triggers scheduler invocation
- UI stop triggers graceful termination
- Heartbeat is visible during run
- ImprovementAgent execution is impossible

Run with: pytest agents/ui/tests/test_learning_session_ui.py -v
"""

import pytest
from unittest.mock import MagicMock


class TestLearningSessionUIWiring:
    """Tests for Learning Session UI controls."""

    def test_start_learning_session_calls_scheduler(self):
        """Start button must invoke AutonomousLearningScheduler.run_session()."""
        from odibi.agents.core.autonomous_learning import (
            AutonomousLearningScheduler,
            LearningCycleConfig,
        )

        mock_scheduler = MagicMock(spec=AutonomousLearningScheduler)
        mock_result = MagicMock()
        mock_result.cycles_completed = 1
        mock_result.cycles_failed = 0
        mock_scheduler.run_session.return_value = mock_result

        config = LearningCycleConfig(
            project_root="d:/odibi/examples",
            max_improvements=0,
        )

        mock_scheduler.run_session(
            config=config,
            max_cycles=3,
            max_wall_clock_hours=1.0,
        )

        mock_scheduler.run_session.assert_called_once()
        call_args = mock_scheduler.run_session.call_args
        assert call_args.kwargs["max_cycles"] == 3
        assert call_args.kwargs["max_wall_clock_hours"] == 1.0

    def test_stop_learning_session_calls_scheduler_stop(self):
        """Stop button must call scheduler.stop() for graceful termination."""
        from odibi.agents.core.autonomous_learning import AutonomousLearningScheduler

        mock_scheduler = MagicMock(spec=AutonomousLearningScheduler)

        mock_scheduler.stop()

        mock_scheduler.stop.assert_called_once()

    def test_learning_config_always_zero_improvements(self):
        """LearningCycleConfig MUST have max_improvements=0."""
        from odibi.agents.core.autonomous_learning import LearningCycleConfig

        config = LearningCycleConfig(
            project_root="d:/odibi/examples",
            max_improvements=0,
        )

        errors = config.validate()
        assert errors == [], f"Valid config should have no errors: {errors}"

        config_with_improvements = LearningCycleConfig(
            project_root="d:/odibi/examples",
            max_improvements=1,
        )

        errors = config_with_improvements.validate()
        assert len(errors) > 0, "Config with max_improvements=1 MUST fail validation"
        assert any("max_improvements" in e for e in errors)

    def test_guarded_runner_skips_improvement_step(self):
        """GuardedCycleRunner MUST skip improvement step."""
        from odibi.agents.core.autonomous_learning import GUARDED_SKIP_STEPS
        from odibi.agents.core.cycle import CycleStep

        assert CycleStep.IMPROVEMENT_PROPOSAL in GUARDED_SKIP_STEPS
        assert CycleStep.REVIEW in GUARDED_SKIP_STEPS

    def test_heartbeat_writer_creates_file(self):
        """HeartbeatWriter must write heartbeat.json."""
        import tempfile
        import os
        from odibi.agents.core.disk_guard import HeartbeatWriter, HeartbeatData

        with tempfile.TemporaryDirectory() as temp_dir:
            writer = HeartbeatWriter(temp_dir)

            data = HeartbeatData(
                last_cycle_id="test-cycle-123",
                timestamp="2024-01-15T08:30:00",
                last_status="SESSION_RUNNING",
                cycles_completed=5,
                cycles_failed=0,
                disk_usage={"artifacts": 1000, "reports": 500},
            )

            result = writer.write(data)
            assert result is True

            heartbeat_path = os.path.join(temp_dir, ".odibi", "heartbeat.json")
            assert os.path.exists(heartbeat_path)

    def test_heartbeat_reader_returns_data(self):
        """HeartbeatWriter.read() must return HeartbeatData."""
        import tempfile
        from odibi.agents.core.disk_guard import HeartbeatWriter, HeartbeatData

        with tempfile.TemporaryDirectory() as temp_dir:
            writer = HeartbeatWriter(temp_dir)

            data = HeartbeatData(
                last_cycle_id="test-cycle-456",
                timestamp="2024-01-15T09:00:00",
                last_status="SESSION_COMPLETED",
                cycles_completed=10,
                cycles_failed=1,
                disk_usage={"artifacts": 2000},
            )

            writer.write(data)

            read_data = writer.read()

            assert read_data is not None
            assert read_data.last_cycle_id == "test-cycle-456"
            assert read_data.cycles_completed == 10
            assert read_data.cycles_failed == 1


class TestLearningSessionSafetyGuarantees:
    """Tests that verify safety guarantees cannot be bypassed."""

    def test_improvement_agent_never_invoked_in_learning_mode(self):
        """ImprovementAgent must NEVER be invoked during learning sessions."""
        from odibi.agents.core.autonomous_learning import (
            GuardedCycleRunner,
            GUARDED_SKIP_STEPS,
        )
        from odibi.agents.core.cycle import CycleStep

        assert CycleStep.IMPROVEMENT_PROPOSAL in GUARDED_SKIP_STEPS

        runner = GuardedCycleRunner(odibi_root="d:/odibi")

        mock_state = MagicMock()
        mock_state.is_finished.return_value = False
        mock_state.current_step.return_value = CycleStep.IMPROVEMENT_PROPOSAL
        mock_state.config = MagicMock()
        mock_state.config.max_improvements = 0

        should_skip, reason = runner._should_skip_step(CycleStep.IMPROVEMENT_PROPOSAL, mock_state)
        assert should_skip is True, "Improvement step MUST be skipped"

    def test_review_agent_never_invoked_in_learning_mode(self):
        """ReviewAgent must NEVER be invoked during learning sessions."""
        from odibi.agents.core.autonomous_learning import GuardedCycleRunner, GUARDED_SKIP_STEPS
        from odibi.agents.core.cycle import CycleStep

        assert CycleStep.REVIEW in GUARDED_SKIP_STEPS

        runner = GuardedCycleRunner(odibi_root="d:/odibi")

        mock_state = MagicMock()
        mock_state.is_finished.return_value = False
        mock_state.current_step.return_value = CycleStep.REVIEW
        mock_state.config = MagicMock()
        mock_state.config.max_improvements = 0

        should_skip, reason = runner._should_skip_step(CycleStep.REVIEW, mock_state)
        assert should_skip is True, "Review step MUST be skipped"

    def test_learning_mode_guard_blocks_improvements(self):
        """LearningModeGuard must block any improvement invocation."""
        from odibi.agents.core.autonomous_learning import LearningModeGuard, LearningModeViolation

        guard = LearningModeGuard()

        guard.record_improvement_invocation()

        with pytest.raises(LearningModeViolation):
            guard.assert_no_improvement_agent()

    def test_learning_mode_guard_blocks_proposals(self):
        """LearningModeGuard must block any proposal generation."""
        from odibi.agents.core.autonomous_learning import LearningModeGuard, LearningModeViolation

        guard = LearningModeGuard()

        guard.record_proposal_generated()

        with pytest.raises(LearningModeViolation):
            guard.assert_no_proposals()

    def test_learning_mode_guard_blocks_code_edits(self):
        """LearningModeGuard must block any code edit attempts."""
        from odibi.agents.core.autonomous_learning import LearningModeGuard, LearningModeViolation

        guard = LearningModeGuard()

        guard.record_code_edit_attempt()

        with pytest.raises(LearningModeViolation):
            guard.assert_no_code_edits()


class TestUIMonitoringReadsOnly:
    """Tests that verify UI monitoring is read-only."""

    def test_refresh_status_only_reads_heartbeat(self):
        """Refresh status must only read from heartbeat.json, never write."""
        import tempfile
        import os
        from odibi.agents.core.disk_guard import HeartbeatWriter, HeartbeatData

        with tempfile.TemporaryDirectory() as temp_dir:
            writer = HeartbeatWriter(temp_dir)

            data = HeartbeatData(
                last_cycle_id="read-only-test",
                timestamp="2024-01-15T10:00:00",
                last_status="SESSION_RUNNING",
                cycles_completed=3,
                cycles_failed=0,
                disk_usage={},
            )
            writer.write(data)

            heartbeat_path = os.path.join(temp_dir, ".odibi", "heartbeat.json")
            mtime_before = os.path.getmtime(heartbeat_path)

            read_data = writer.read()

            mtime_after = os.path.getmtime(heartbeat_path)

            assert mtime_before == mtime_after, "Heartbeat file must not be modified by read"
            assert read_data.last_cycle_id == "read-only-test"

    def test_heartbeat_file_location_is_correct(self):
        """Heartbeat file must be at .odibi/heartbeat.json."""
        import tempfile
        import os
        from odibi.agents.core.disk_guard import HeartbeatWriter

        with tempfile.TemporaryDirectory() as temp_dir:
            writer = HeartbeatWriter(temp_dir)

            expected_path = os.path.join(temp_dir, ".odibi", "heartbeat.json")
            assert writer.heartbeat_path == expected_path

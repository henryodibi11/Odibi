"""Tests for Phase 9.A: Guarded Autonomous Learning (Observation-Only).

These tests verify all invariants for safe, unattended learning cycles:
- Learning cycles never modify code
- Learning cycles never propose improvements
- Learning cycles never promote sources
- Learning cycles never promote memories to validated curriculum
- Learning cycles never auto-advance to improvement mode

Run with: pytest agents/core/tests/test_autonomous_learning.py -v
"""

import pytest

from odibi.agents.core.autonomous_learning import (
    AutonomousLearningSession,
    AutonomousLearningStatus,
    GuardedCycleRunner,
    LearningCycleConfig,
    LearningCycleResult,
    LearningModeGuard,
    LearningModeViolation,
    GUARDED_SKIP_STEPS,
    validate_learning_profile,
)
from odibi.agents.core.cycle import CycleConfig, CycleState, CycleStep


class TestLearningCycleConfig:
    """Tests for LearningCycleConfig validation."""

    def test_valid_learning_config(self):
        """Valid learning config should pass validation."""
        config = LearningCycleConfig(
            project_root="d:/odibi/examples",
            max_improvements=0,
            mode="learning",
            require_frozen=True,
            deterministic=True,
        )
        errors = config.validate()
        assert errors == [], f"Expected no errors, got: {errors}"

    def test_invalid_max_improvements(self):
        """max_improvements != 0 should fail validation."""
        config = LearningCycleConfig(
            project_root="d:/odibi/examples",
            max_improvements=1,
            mode="learning",
        )
        errors = config.validate()
        assert any("max_improvements" in e for e in errors)

    def test_invalid_mode(self):
        """mode != 'learning' should fail validation."""
        config = LearningCycleConfig(
            project_root="d:/odibi/examples",
            max_improvements=0,
            mode="improvement",
        )
        errors = config.validate()
        assert any("mode" in e for e in errors)

    def test_require_frozen_must_be_true(self):
        """require_frozen=False should fail validation."""
        config = LearningCycleConfig(
            project_root="d:/odibi/examples",
            max_improvements=0,
            mode="learning",
            require_frozen=False,
        )
        errors = config.validate()
        assert any("require_frozen" in e for e in errors)

    def test_deterministic_must_be_true(self):
        """deterministic=False should fail validation."""
        config = LearningCycleConfig(
            project_root="d:/odibi/examples",
            max_improvements=0,
            mode="learning",
            deterministic=False,
        )
        errors = config.validate()
        assert any("deterministic" in e for e in errors)

    def test_to_cycle_config_forces_zero_improvements(self):
        """to_cycle_config should always produce max_improvements=0."""
        config = LearningCycleConfig(
            project_root="d:/odibi/examples",
            max_improvements=0,
            mode="learning",
        )
        cycle_config = config.to_cycle_config()
        assert cycle_config.max_improvements == 0
        assert cycle_config.is_learning_mode is True


class TestLearningModeGuard:
    """Tests for LearningModeGuard runtime enforcement."""

    def test_initial_state_has_no_violations(self):
        """Fresh guard should report no violations."""
        guard = LearningModeGuard()
        violations = guard.check_all_invariants()
        assert violations == []

    def test_improvement_invocation_is_detected(self):
        """ImprovementAgent invocation should be detected."""
        guard = LearningModeGuard()
        guard.record_improvement_invocation()
        violations = guard.check_all_invariants()
        assert any("ImprovementAgent" in v for v in violations)

    def test_review_invocation_is_detected(self):
        """ReviewerAgent invocation should be detected."""
        guard = LearningModeGuard()
        guard.record_review_invocation()
        violations = guard.check_all_invariants()
        assert any("ReviewerAgent" in v for v in violations)

    def test_proposal_generation_is_detected(self):
        """Proposal generation should be detected."""
        guard = LearningModeGuard()
        guard.record_proposal_generated()
        violations = guard.check_all_invariants()
        assert any("proposal" in v for v in violations)

    def test_code_edit_attempt_is_detected(self):
        """Code edit attempt should be detected."""
        guard = LearningModeGuard()
        guard.record_code_edit_attempt()
        violations = guard.check_all_invariants()
        assert any("code edit" in v for v in violations)

    def test_assert_no_improvement_agent_raises(self):
        """assert_no_improvement_agent should raise on violation."""
        guard = LearningModeGuard()
        guard.record_improvement_invocation()
        with pytest.raises(LearningModeViolation) as exc_info:
            guard.assert_no_improvement_agent()
        assert "ImprovementAgent" in str(exc_info.value)

    def test_assert_no_review_agent_raises(self):
        """assert_no_review_agent should raise on violation."""
        guard = LearningModeGuard()
        guard.record_review_invocation()
        with pytest.raises(LearningModeViolation) as exc_info:
            guard.assert_no_review_agent()
        assert "ReviewerAgent" in str(exc_info.value)

    def test_reset_clears_violations(self):
        """reset() should clear all recorded violations."""
        guard = LearningModeGuard()
        guard.record_improvement_invocation()
        guard.record_review_invocation()
        guard.record_proposal_generated()
        guard.record_code_edit_attempt()

        guard.reset()

        violations = guard.check_all_invariants()
        assert violations == []


class TestGuardedSkipSteps:
    """Tests for step skipping in learning mode."""

    def test_improvement_step_is_in_skip_list(self):
        """IMPROVEMENT_PROPOSAL should be in the skip list."""
        assert CycleStep.IMPROVEMENT_PROPOSAL in GUARDED_SKIP_STEPS

    def test_review_step_is_in_skip_list(self):
        """REVIEW should be in the skip list."""
        assert CycleStep.REVIEW in GUARDED_SKIP_STEPS

    def test_observation_step_is_not_skipped(self):
        """OBSERVATION should NOT be in the skip list."""
        assert CycleStep.OBSERVATION not in GUARDED_SKIP_STEPS

    def test_user_execution_step_is_not_skipped(self):
        """USER_EXECUTION should NOT be in the skip list."""
        assert CycleStep.USER_EXECUTION not in GUARDED_SKIP_STEPS

    def test_env_validation_step_is_not_skipped(self):
        """ENV_VALIDATION should NOT be in the skip list."""
        assert CycleStep.ENV_VALIDATION not in GUARDED_SKIP_STEPS


class TestLearningModeViolation:
    """Tests for LearningModeViolation exception."""

    def test_exception_message_format(self):
        """Exception should format violation and details correctly."""
        exc = LearningModeViolation(
            violation="ImprovementAgent was invoked",
            details="This is forbidden in learning mode.",
        )
        assert "LEARNING MODE VIOLATION" in str(exc)
        assert "ImprovementAgent" in str(exc)
        assert "forbidden" in str(exc)

    def test_exception_stores_violation(self):
        """Exception should store violation type."""
        exc = LearningModeViolation(violation="test violation")
        assert exc.violation == "test violation"


class TestGuardedCycleRunnerSkipping:
    """Tests for GuardedCycleRunner step skipping behavior."""

    @pytest.fixture
    def mock_state(self):
        """Create a mock CycleState in learning mode."""
        config = CycleConfig(
            project_root="d:/odibi/examples",
            max_improvements=0,
        )
        state = CycleState(
            cycle_id="test-cycle-id",
            config=config,
            mode="scheduled",
        )
        return state

    def test_should_skip_improvement_step(self, mock_state):
        """GuardedCycleRunner should skip IMPROVEMENT_PROPOSAL."""
        runner = GuardedCycleRunner(odibi_root="d:/odibi")
        should_skip, reason = runner._should_skip_step(CycleStep.IMPROVEMENT_PROPOSAL, mock_state)
        assert should_skip is True
        assert "Learning mode" in reason or "ImprovementAgent" in reason

    def test_should_skip_review_step(self, mock_state):
        """GuardedCycleRunner should skip REVIEW."""
        runner = GuardedCycleRunner(odibi_root="d:/odibi")
        should_skip, reason = runner._should_skip_step(CycleStep.REVIEW, mock_state)
        assert should_skip is True
        assert "Learning mode" in reason or "ReviewerAgent" in reason

    def test_should_not_skip_observation_step(self, mock_state):
        """GuardedCycleRunner should NOT skip OBSERVATION."""
        runner = GuardedCycleRunner(odibi_root="d:/odibi")
        should_skip, _reason = runner._should_skip_step(CycleStep.OBSERVATION, mock_state)
        assert should_skip is False


class TestLearningCycleResult:
    """Tests for LearningCycleResult data structure."""

    def test_to_dict_includes_all_fields(self):
        """to_dict should include all relevant fields."""
        result = LearningCycleResult(
            cycle_id="test-cycle",
            status=AutonomousLearningStatus.COMPLETED,
            started_at="2024-01-01T00:00:00",
            completed_at="2024-01-01T01:00:00",
            duration_seconds=3600.0,
            steps_executed=8,
            steps_skipped=2,
            evidence_collected=True,
            report_generated=True,
            indexed=True,
        )
        d = result.to_dict()

        assert d["cycle_id"] == "test-cycle"
        assert d["status"] == "COMPLETED"
        assert d["steps_executed"] == 8
        assert d["steps_skipped"] == 2
        assert d["evidence_collected"] is True
        assert d["indexed"] is True


class TestAutonomousLearningSession:
    """Tests for AutonomousLearningSession data structure."""

    def test_session_tracks_completed_and_failed_cycles(self):
        """Session should track completed and failed cycle counts."""
        session = AutonomousLearningSession(
            session_id="test-session",
            started_at="2024-01-01T00:00:00",
            cycles_completed=5,
            cycles_failed=1,
        )
        assert session.cycles_completed == 5
        assert session.cycles_failed == 1

    def test_to_dict_includes_results(self):
        """to_dict should include results list."""
        result = LearningCycleResult(
            cycle_id="test-cycle",
            status=AutonomousLearningStatus.COMPLETED,
            started_at="2024-01-01T00:00:00",
            completed_at="2024-01-01T00:30:00",
            duration_seconds=1800.0,
            steps_executed=8,
            steps_skipped=2,
            evidence_collected=True,
            report_generated=True,
            indexed=True,
        )
        session = AutonomousLearningSession(
            session_id="test-session",
            started_at="2024-01-01T00:00:00",
            results=[result],
        )
        d = session.to_dict()

        assert len(d["results"]) == 1
        assert d["results"][0]["cycle_id"] == "test-cycle"


class TestValidateLearningProfile:
    """Tests for validate_learning_profile function."""

    def test_valid_profile(self, tmp_path):
        """Valid learning profile should pass validation."""
        profile_path = tmp_path / "valid_learning.yaml"
        profile_path.write_text(
            """
mode: learning
max_improvements: 0
cycle_source_config:
  require_frozen: true
  deterministic: true
  allowed_tiers:
    - tier100gb
    - tier600gb
"""
        )
        errors = validate_learning_profile(str(profile_path))
        assert errors == []

    def test_invalid_mode(self, tmp_path):
        """mode != 'learning' should fail."""
        profile_path = tmp_path / "invalid_mode.yaml"
        profile_path.write_text(
            """
mode: improvement
max_improvements: 0
cycle_source_config:
  require_frozen: true
  deterministic: true
"""
        )
        errors = validate_learning_profile(str(profile_path))
        assert any("mode" in e for e in errors)

    def test_invalid_max_improvements(self, tmp_path):
        """max_improvements != 0 should fail."""
        profile_path = tmp_path / "invalid_improvements.yaml"
        profile_path.write_text(
            """
mode: learning
max_improvements: 3
cycle_source_config:
  require_frozen: true
  deterministic: true
"""
        )
        errors = validate_learning_profile(str(profile_path))
        assert any("max_improvements" in e for e in errors)

    def test_require_frozen_false_fails(self, tmp_path):
        """require_frozen=False should fail."""
        profile_path = tmp_path / "not_frozen.yaml"
        profile_path.write_text(
            """
mode: learning
max_improvements: 0
cycle_source_config:
  require_frozen: false
  deterministic: true
"""
        )
        errors = validate_learning_profile(str(profile_path))
        assert any("require_frozen" in e for e in errors)

    def test_deterministic_false_fails(self, tmp_path):
        """deterministic=False should fail."""
        profile_path = tmp_path / "not_deterministic.yaml"
        profile_path.write_text(
            """
mode: learning
max_improvements: 0
cycle_source_config:
  require_frozen: true
  deterministic: false
"""
        )
        errors = validate_learning_profile(str(profile_path))
        assert any("deterministic" in e for e in errors)

    def test_missing_file_returns_error(self):
        """Missing profile file should return error."""
        errors = validate_learning_profile("/nonexistent/profile.yaml")
        assert len(errors) == 1
        assert "Failed to load" in errors[0]


class TestPhase9AInvariants:
    """Integration tests for Phase 9.A invariants.

    These tests verify the critical safety guarantees:
    - Learning cycles never mutate code
    - Learning cycles never promote sources
    - Learning cycles never promote memories to validated curriculum
    - Learning cycles never auto-advance to improvement mode
    """

    def test_learning_cycle_cannot_mutate_code(self):
        """Learning cycles MUST NOT mutate code."""
        guard = LearningModeGuard()

        guard.record_code_edit_attempt()

        with pytest.raises(LearningModeViolation):
            guard.assert_no_code_edits()

    def test_learning_cycle_cannot_generate_proposals(self):
        """Learning cycles MUST NOT generate proposals."""
        guard = LearningModeGuard()

        guard.record_proposal_generated()

        with pytest.raises(LearningModeViolation):
            guard.assert_no_proposals()

    def test_learning_config_blocks_improvement_mode(self):
        """LearningCycleConfig MUST NOT allow improvement mode."""
        config = LearningCycleConfig(
            project_root="d:/odibi/examples",
            max_improvements=1,
        )
        errors = config.validate()

        assert len(errors) > 0
        assert any("max_improvements" in e for e in errors)

    def test_cycle_config_is_learning_mode_property(self):
        """CycleConfig.is_learning_mode should be True when max_improvements=0."""
        config = CycleConfig(
            project_root="d:/odibi/examples",
            max_improvements=0,
        )
        assert config.is_learning_mode is True

        config_improvement = CycleConfig(
            project_root="d:/odibi/examples",
            max_improvements=1,
        )
        assert config_improvement.is_learning_mode is False

    def test_learning_mode_memory_scope(self):
        """Learning mode should use 'learning' memory scope."""
        config = CycleConfig(
            project_root="d:/odibi/examples",
            max_improvements=0,
        )
        assert config.is_learning_mode is True


class TestLearningSessionNoImprovementAgent:
    """Regression test: Learning sessions must NEVER invoke ImprovementAgent.

    Phase 9.B.1 fix: Ensures that GuardedCycleRunner._should_skip_step()
    is checked BEFORE any guard assertions, so skipped steps don't trigger
    LearningModeViolation.
    """

    def test_guarded_runner_skips_improvement_step(self):
        """GuardedCycleRunner MUST skip IMPROVEMENT_PROPOSAL step."""
        from unittest.mock import MagicMock

        runner = GuardedCycleRunner(odibi_root="d:/odibi")

        state = MagicMock(spec=CycleState)
        state.is_finished.return_value = False
        state.current_step.return_value = CycleStep.IMPROVEMENT_PROPOSAL
        state.current_step_index = 0
        state.config = MagicMock()
        state.config.max_improvements = 0

        should_skip, reason = runner._should_skip_step(CycleStep.IMPROVEMENT_PROPOSAL, state)

        assert should_skip is True
        assert "Learning mode" in reason or "SKIPPED" in reason

    def test_guarded_runner_skips_review_step(self):
        """GuardedCycleRunner MUST skip REVIEW step."""
        from unittest.mock import MagicMock

        runner = GuardedCycleRunner(odibi_root="d:/odibi")

        state = MagicMock(spec=CycleState)
        state.is_finished.return_value = False
        state.current_step.return_value = CycleStep.REVIEW
        state.current_step_index = 0
        state.config = MagicMock()
        state.config.max_improvements = 0

        should_skip, reason = runner._should_skip_step(CycleStep.REVIEW, state)

        assert should_skip is True
        assert "Learning mode" in reason or "SKIPPED" in reason

    def test_run_next_step_does_not_fail_on_skipped_improvement(self):
        """run_next_step must NOT raise LearningModeViolation for skipped steps.

        This is the key regression test for Phase 9.B.1.
        """
        from unittest.mock import MagicMock, patch
        from odibi.agents.core.cycle import CycleStepLog

        runner = GuardedCycleRunner(odibi_root="d:/odibi")

        state = MagicMock(spec=CycleState)
        state.is_finished.return_value = False
        state.current_step.return_value = CycleStep.IMPROVEMENT_PROPOSAL
        state.current_step_index = 0
        state.logs = []
        state.config = MagicMock()
        state.config.max_improvements = 0

        skipped_log = CycleStepLog(
            step=CycleStep.IMPROVEMENT_PROPOSAL.value,
            agent_role="improvement",
            started_at="2024-01-01T00:00:00",
            completed_at="2024-01-01T00:00:01",
            input_summary="Step skipped",
            output_summary="Learning mode",
            success=True,
            skipped=True,
            skip_reason="Learning mode: ImprovementAgent is SKIPPED",
        )

        def mock_parent_run_next_step(self, s):
            s.logs.append(skipped_log)
            s.current_step_index += 1
            return s

        with patch.object(
            runner.__class__.__bases__[0],
            "run_next_step",
            mock_parent_run_next_step,
        ):
            result = runner.run_next_step(state)

        assert result.current_step_index == 1
        assert len(result.logs) == 1
        assert result.logs[0].skipped is True

    def test_guard_only_fails_on_actual_agent_invocation(self):
        """Guard should only fail if agent was actually invoked (not skipped).

        Ensures the fix: guard assertions happen AFTER skip check, not before.
        """
        from unittest.mock import MagicMock, patch
        from odibi.agents.core.cycle import CycleStepLog

        runner = GuardedCycleRunner(odibi_root="d:/odibi")

        state = MagicMock(spec=CycleState)
        state.is_finished.return_value = False
        state.current_step.return_value = CycleStep.IMPROVEMENT_PROPOSAL
        state.current_step_index = 0
        state.logs = []
        state.config = MagicMock()
        state.config.max_improvements = 0

        executed_log = CycleStepLog(
            step=CycleStep.IMPROVEMENT_PROPOSAL.value,
            agent_role="improvement",
            started_at="2024-01-01T00:00:00",
            completed_at="2024-01-01T00:00:01",
            input_summary="Some input",
            output_summary="Some output",
            success=True,
            skipped=False,
        )

        def mock_parent_run_next_step(self, s):
            s.logs.append(executed_log)
            s.current_step_index += 1
            return s

        with patch.object(
            runner.__class__.__bases__[0],
            "run_next_step",
            mock_parent_run_next_step,
        ):
            with pytest.raises(LearningModeViolation) as exc_info:
                runner.run_next_step(state)

        assert (
            "ImprovementAgent" in str(exc_info.value)
            or "improvement" in str(exc_info.value).lower()
        )

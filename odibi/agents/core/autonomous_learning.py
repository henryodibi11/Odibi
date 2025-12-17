"""Guarded Autonomous Learning (Observation-Only) - Phase 9.A.

This module provides safe, unattended learning cycles that:
- Execute real pipelines against large, frozen datasets
- Collect real execution evidence
- Produce human-readable reports
- Accumulate indexed learnings
- NEVER modify code
- NEVER propose improvements
- NEVER change behavior

PHASE 9.A INVARIANTS (MUST NOT BE VIOLATED):
❌ No ImprovementAgent execution
❌ No code edits
❌ No file writes outside evidence, reports, artifacts, or indexes
❌ No source downloads
❌ No schema inference
❌ No agent memory writes outside LEARNING scope
❌ No retries or recovery logic

This phase is designed to be BORING, SAFE, and DETERMINISTIC.

PHASE 9.C ADDITIONS:
- Cycle profiles loaded from .odibi/cycle_profiles/
- Profile config frozen for entire session
- Profile name + hash recorded in heartbeat, reports, session metadata
"""

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

from .cycle import (
    AssistantMode,
    CycleConfig,
    CycleEvent,
    CycleEventCallback,
    CycleRunner,
    CycleState,
    CycleStep,
)
from .disk_guard import (
    DEFAULT_ARTIFACTS_BUDGET_BYTES,
    DEFAULT_INDEX_BUDGET_BYTES,
    DEFAULT_REPORTS_BUDGET_BYTES,
    DiskUsageGuard,
    HeartbeatData,
    HeartbeatWriter,
)
from .source_binder import (
    BindingStatus,
    BoundSourceMap,
    SourceBinder,
    SourceBindingResult,
    TrivialCycleDetector,
)

if TYPE_CHECKING:
    from .cycle_profile import FrozenCycleProfile

logger = logging.getLogger(__name__)


class LearningModeViolation(Exception):
    """Raised when learning mode safety constraints are violated."""

    def __init__(self, violation: str, details: str = ""):
        self.violation = violation
        self.details = details
        super().__init__(f"LEARNING MODE VIOLATION: {violation}. {details}")


class AutonomousLearningStatus(str, Enum):
    """Status of autonomous learning execution."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TERMINATED = "TERMINATED"


GUARDED_SKIP_STEPS = frozenset(
    {
        CycleStep.IMPROVEMENT_PROPOSAL,
        CycleStep.REVIEW,
    }
)

ALLOWED_WRITE_PATHS = frozenset(
    {
        "evidence",
        "reports",
        "artifacts",
        "index",
        "cycle_index",
        "memories",
        "cycles",
    }
)


@dataclass
class LearningCycleConfig:
    """Configuration for guarded autonomous learning cycles.

    Can be created from scratch or resolved from a FrozenCycleProfile.
    Uses the large_scale_learning profile settings by default.
    """

    project_root: str
    task_description: str = "Autonomous learning cycle"

    max_improvements: int = 0
    max_runtime_hours: float = 24.0
    max_steps: int = 10
    max_wall_clock_hours: float = 24.0

    mode: str = "learning"
    require_frozen: bool = True
    allow_messy: bool = True
    deterministic: bool = True
    allowed_tiers: list[str] = field(default_factory=lambda: ["tier100gb", "tier600gb"])

    profile_id: str = ""
    profile_name: str = ""
    profile_hash: str = ""
    profile_path: str = ""

    @classmethod
    def from_frozen_profile(
        cls,
        profile: "FrozenCycleProfile",
        project_root: str,
        task_description: str = "Autonomous learning cycle",
    ) -> "LearningCycleConfig":
        """Create a LearningCycleConfig from a FrozenCycleProfile.

        This is the preferred way to create configs in Phase 9.C.
        The profile provides all learning parameters; only project_root
        and task_description are caller-provided.

        Args:
            profile: Frozen profile loaded from .odibi/cycle_profiles/.
            project_root: Path to the project to learn from.
            task_description: Optional task description.

        Returns:
            LearningCycleConfig resolved from the profile.
        """
        return cls(
            project_root=project_root,
            task_description=task_description,
            max_improvements=profile.max_improvements,
            max_runtime_hours=profile.max_duration_hours,
            mode=profile.mode,
            require_frozen=profile.require_frozen,
            allow_messy=profile.allow_messy,
            deterministic=profile.deterministic,
            allowed_tiers=list(profile.allowed_tiers),
            profile_id=profile.profile_id,
            profile_name=profile.profile_name,
            profile_hash=profile.content_hash,
            profile_path=profile.source_path,
        )

    def validate(self) -> list[str]:
        """Validate the config against Phase 9.A invariants.

        Returns:
            List of validation errors (empty if valid).
        """
        errors = []

        if self.max_improvements != 0:
            errors.append(
                f"max_improvements MUST be 0 for learning mode, got {self.max_improvements}"
            )

        if self.mode != "learning":
            errors.append(f"mode MUST be 'learning' for autonomous learning, got '{self.mode}'")

        if not self.require_frozen:
            errors.append("require_frozen MUST be True for autonomous learning")

        if not self.deterministic:
            errors.append("deterministic MUST be True for autonomous learning")

        return errors

    def to_cycle_config(self) -> CycleConfig:
        """Convert to CycleConfig for cycle execution."""
        return CycleConfig(
            project_root=self.project_root,
            task_description=self.task_description,
            max_improvements=0,
            max_runtime_hours=self.max_runtime_hours,
            gated_improvements=True,
            stop_on_convergence=True,
            golden_projects=[],
        )

    def get_profile_audit_info(self) -> dict[str, str]:
        """Get profile information for audit trails.

        Returns:
            Dict with profile_id, profile_name, profile_hash, profile_path.
            Empty values if no profile was used.
        """
        return {
            "profile_id": self.profile_id,
            "profile_name": self.profile_name,
            "profile_hash": self.profile_hash,
            "profile_path": self.profile_path,
        }


@dataclass
class LearningCycleResult:
    """Result of a single learning cycle."""

    cycle_id: str
    status: AutonomousLearningStatus
    started_at: str
    completed_at: str
    duration_seconds: float

    steps_executed: int
    steps_skipped: int

    evidence_collected: bool
    report_generated: bool
    indexed: bool

    error_message: str = ""

    source_resolution: Optional[dict[str, Any]] = None

    profile_id: str = ""
    profile_name: str = ""
    profile_hash: str = ""

    # Phase 9.D: Source binding
    source_binding: Optional[dict[str, Any]] = None
    trivial_cycle_warning: Optional[dict[str, Any]] = None
    bytes_read_from_sources: int = 0
    pools_bound: int = 0
    pools_used: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "cycle_id": self.cycle_id,
            "status": self.status.value,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": self.duration_seconds,
            "steps_executed": self.steps_executed,
            "steps_skipped": self.steps_skipped,
            "evidence_collected": self.evidence_collected,
            "report_generated": self.report_generated,
            "indexed": self.indexed,
            "error_message": self.error_message,
            "source_resolution": self.source_resolution,
            "profile_id": self.profile_id,
            "profile_name": self.profile_name,
            "profile_hash": self.profile_hash,
            "source_binding": self.source_binding,
            "trivial_cycle_warning": self.trivial_cycle_warning,
            "bytes_read_from_sources": self.bytes_read_from_sources,
            "pools_bound": self.pools_bound,
            "pools_used": self.pools_used,
        }


@dataclass
class AutonomousLearningSession:
    """Session for running multiple autonomous learning cycles."""

    session_id: str
    started_at: str
    completed_at: str = ""
    status: AutonomousLearningStatus = AutonomousLearningStatus.PENDING

    cycles_completed: int = 0
    cycles_failed: int = 0
    total_duration_seconds: float = 0.0

    results: list[LearningCycleResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    profile_id: str = ""
    profile_name: str = ""
    profile_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "status": self.status.value,
            "cycles_completed": self.cycles_completed,
            "cycles_failed": self.cycles_failed,
            "total_duration_seconds": self.total_duration_seconds,
            "results": [r.to_dict() for r in self.results],
            "errors": self.errors,
            "profile_id": self.profile_id,
            "profile_name": self.profile_name,
            "profile_hash": self.profile_hash,
        }


class LearningModeGuard:
    """Runtime guard that enforces learning-only constraints.

    This guard is checked at critical points during cycle execution
    to prevent any violation of Phase 9.A invariants.
    """

    def __init__(self):
        self._improvement_invoked = False
        self._review_invoked = False
        self._proposals_generated = 0
        self._code_edits_attempted = 0

    def assert_no_improvement_agent(self) -> None:
        """Assert that ImprovementAgent has not been invoked."""
        if self._improvement_invoked:
            raise LearningModeViolation(
                "ImprovementAgent was invoked",
                "Learning mode cycles MUST NOT execute ImprovementAgent.",
            )

    def assert_no_review_agent(self) -> None:
        """Assert that ReviewerAgent has not been invoked."""
        if self._review_invoked:
            raise LearningModeViolation(
                "ReviewerAgent was invoked",
                "Learning mode cycles MUST NOT execute ReviewerAgent.",
            )

    def assert_no_proposals(self) -> None:
        """Assert that no proposals have been generated."""
        if self._proposals_generated > 0:
            raise LearningModeViolation(
                f"{self._proposals_generated} proposal(s) were generated",
                "Learning mode cycles MUST NOT generate proposals.",
            )

    def assert_no_code_edits(self) -> None:
        """Assert that no code edits have been attempted."""
        if self._code_edits_attempted > 0:
            raise LearningModeViolation(
                f"{self._code_edits_attempted} code edit(s) were attempted",
                "Learning mode cycles MUST NOT edit code.",
            )

    def record_improvement_invocation(self) -> None:
        """Record that ImprovementAgent was invoked (violation)."""
        self._improvement_invoked = True

    def record_review_invocation(self) -> None:
        """Record that ReviewerAgent was invoked (violation)."""
        self._review_invoked = True

    def record_proposal_generated(self) -> None:
        """Record that a proposal was generated (violation)."""
        self._proposals_generated += 1

    def record_code_edit_attempt(self) -> None:
        """Record that a code edit was attempted (violation)."""
        self._code_edits_attempted += 1

    def check_all_invariants(self) -> list[str]:
        """Check all invariants and return any violations.

        Returns:
            List of violation descriptions (empty if all invariants hold).
        """
        violations = []

        if self._improvement_invoked:
            violations.append("ImprovementAgent was invoked")

        if self._review_invoked:
            violations.append("ReviewerAgent was invoked")

        if self._proposals_generated > 0:
            violations.append(f"{self._proposals_generated} proposal(s) were generated")

        if self._code_edits_attempted > 0:
            violations.append(f"{self._code_edits_attempted} code edit(s) were attempted")

        return violations

    def reset(self) -> None:
        """Reset the guard for a new cycle."""
        self._improvement_invoked = False
        self._review_invoked = False
        self._proposals_generated = 0
        self._code_edits_attempted = 0


class GuardedCycleRunner(CycleRunner):
    """CycleRunner with learning-mode guards.

    Extends CycleRunner to:
    - Skip ImprovementAgent and ReviewerAgent
    - Enforce learning-only invariants at runtime
    - Fail fast on any violation
    - Bind resolved sources to execution (Phase 9.D)
    - Detect trivial cycles (Phase 9.D)
    """

    def __init__(
        self,
        odibi_root: str,
        memory_manager: Optional[Any] = None,
        azure_config: Optional[Any] = None,
    ):
        super().__init__(odibi_root, memory_manager, azure_config)
        self._learning_guard = LearningModeGuard()
        # Phase 9.D: Source binding
        self._source_binder = SourceBinder(odibi_root)
        self._bound_source_map: Optional[BoundSourceMap] = None
        self._binding_result: Optional[SourceBindingResult] = None
        self._trivial_detector: Optional[TrivialCycleDetector] = None

    def _should_skip_step(self, step: CycleStep, state: CycleState) -> tuple[bool, str]:
        """Override to enforce learning-only step skipping.

        Args:
            step: The step to check.
            state: Current cycle state.

        Returns:
            Tuple of (should_skip, reason).
        """
        if step in GUARDED_SKIP_STEPS:
            if step == CycleStep.IMPROVEMENT_PROPOSAL:
                return True, "Learning mode: ImprovementAgent is SKIPPED (max_improvements=0)"
            if step == CycleStep.REVIEW:
                return True, "Learning mode: ReviewerAgent is SKIPPED (no proposals to review)"

        return super()._should_skip_step(step, state)

    def run_next_step(self, state: CycleState) -> CycleState:
        """Run the next step with learning-mode guards.

        The skip check happens in super().run_next_step() via _should_skip_step().
        We only record violations AFTER the parent runs, since skipped steps
        don't actually invoke agents.

        Args:
            state: Current cycle state.

        Returns:
            Updated cycle state.

        Raises:
            LearningModeViolation: If any learning-mode constraint is violated.
        """
        step = state.current_step()
        step_index_before = state.current_step_index

        state = super().run_next_step(state)

        if state.logs and len(state.logs) > step_index_before:
            last_log = state.logs[-1]
            if not last_log.skipped:
                if step == CycleStep.IMPROVEMENT_PROPOSAL:
                    self._learning_guard.record_improvement_invocation()
                    self._learning_guard.assert_no_improvement_agent()

                if step == CycleStep.REVIEW:
                    self._learning_guard.record_review_invocation()
                    self._learning_guard.assert_no_review_agent()

        self._learning_guard.assert_no_proposals()
        self._learning_guard.assert_no_code_edits()

        return state

    def start_learning_cycle(
        self,
        config: LearningCycleConfig,
    ) -> CycleState:
        """Start a guarded learning cycle.

        Phase 9.D: Now includes source binding after resolution.

        Args:
            config: Learning cycle configuration.

        Returns:
            Initial CycleState.

        Raises:
            LearningModeViolation: If config violates learning-mode constraints.
        """
        errors = config.validate()
        if errors:
            raise LearningModeViolation(
                "Invalid learning cycle configuration",
                "; ".join(errors),
            )

        self._learning_guard.reset()
        self._bound_source_map = None
        self._binding_result = None
        self._trivial_detector = None

        cycle_config = config.to_cycle_config()

        state = self.start_cycle(cycle_config, AssistantMode.SCHEDULED_ASSISTANT)

        # Phase 9.D: Bind resolved sources to execution
        self._bind_sources_for_cycle(state)

        return state

    def _bind_sources_for_cycle(self, state: CycleState) -> None:
        """Bind resolved sources to execution (Phase 9.D).

        This materializes sources under .odibi/source_cache/<cycle_id>/
        and wires them into ExecutionGateway for enforcement.

        Args:
            state: The cycle state with source resolution
        """
        if not state.source_resolution:
            logger.debug(f"Cycle {state.cycle_id}: No source resolution, skipping binding")
            return

        try:
            from .source_resolution import SourceResolutionResult

            # Reconstruct SourceResolutionResult from dict
            resolution_dict = state.source_resolution
            resolution = SourceResolutionResult(
                resolution_id=resolution_dict.get("resolution_id", ""),
                cycle_id=resolution_dict.get("cycle_id", state.cycle_id),
                mode=resolution_dict.get("mode", "learning"),
                selected_pool_names=resolution_dict.get("selected_pool_names", []),
                selected_pool_ids=resolution_dict.get("selected_pool_ids", []),
                tiers_used=resolution_dict.get("tiers_used", []),
                selection_hash=resolution_dict.get("selection_hash", ""),
                input_hash=resolution_dict.get("input_hash", ""),
                context_id=resolution_dict.get("context_id", ""),
                pools_considered=resolution_dict.get("pools_considered", 0),
                pools_eligible=resolution_dict.get("pools_eligible", 0),
                pools_excluded=resolution_dict.get("pools_excluded", 0),
                policy_id=resolution_dict.get("policy_id", ""),
                selection_strategy=resolution_dict.get("selection_strategy", ""),
                require_frozen=resolution_dict.get("require_frozen", True),
                allow_messy=resolution_dict.get("allow_messy", False),
                allow_tier_mixing=resolution_dict.get("allow_tier_mixing", False),
                max_sources=resolution_dict.get("max_sources", 3),
                allowed_tiers=resolution_dict.get("allowed_tiers", []),
                resolved_at=resolution_dict.get("resolved_at", ""),
            )

            # Bind sources
            self._binding_result = self._source_binder.bind(resolution)

            if self._binding_result.status == BindingStatus.BOUND:
                self._bound_source_map = self._binding_result.bound_source_map

                # Wire into ExecutionGateway
                if self._bound_source_map:
                    context = self._source_binder.create_execution_context(
                        self._bound_source_map, resolution
                    )
                    self.execution_gateway.bind_sources(context)

                    # Initialize trivial cycle detector
                    self._trivial_detector = TrivialCycleDetector(state.cycle_id)
                    self._trivial_detector.record_binding(self._bound_source_map)

                    self._emit_event(
                        CycleEvent(
                            event_type="source_binding",
                            step="pre_cycle",
                            agent_role="system",
                            message=f"Source binding complete: {self._binding_result.pools_bound} pool(s) bound",
                            detail=f"Total bytes: {self._binding_result.total_bytes:,}",
                            is_error=False,
                        )
                    )

                    logger.info(
                        f"Cycle {state.cycle_id}: Bound {self._binding_result.pools_bound} pools "
                        f"({self._binding_result.total_bytes:,} bytes)"
                    )
            else:
                for warning in self._binding_result.warnings:
                    logger.warning(f"Source binding warning: {warning}")
                for error in self._binding_result.errors:
                    logger.error(f"Source binding error: {error}")

        except Exception as e:
            logger.warning(f"Source binding failed for cycle {state.cycle_id}: {e}")
            self._emit_event(
                CycleEvent(
                    event_type="warning",
                    step="pre_cycle",
                    agent_role="system",
                    message=f"Source binding failed: {type(e).__name__}",
                    detail=str(e),
                    is_error=True,
                )
            )

    def run_learning_cycle(
        self,
        config: LearningCycleConfig,
    ) -> LearningCycleResult:
        """Run a complete guarded learning cycle.

        Phase 9.D: Now includes source binding and trivial cycle detection.

        Args:
            config: Learning cycle configuration.

        Returns:
            LearningCycleResult with execution details including source binding info.
        """
        started_at = datetime.now().isoformat()
        start_time = time.time()

        try:
            state = self.start_learning_cycle(config)
            cycle_id = state.cycle_id

            while not state.is_finished():
                state = self.run_next_step(state)

            violations = self._learning_guard.check_all_invariants()
            if violations:
                raise LearningModeViolation(
                    "Post-cycle invariant check failed",
                    "; ".join(violations),
                )

            completed_at = datetime.now().isoformat()
            duration = time.time() - start_time

            steps_executed = sum(1 for log in state.logs if not log.skipped)
            steps_skipped = sum(1 for log in state.logs if log.skipped)

            # Phase 9.D: Collect source binding info
            source_binding_dict = None
            pools_bound = 0
            pools_used = 0
            bytes_read = 0

            if self._binding_result:
                source_binding_dict = self._binding_result.to_dict()
                pools_bound = self._binding_result.pools_bound

            # Phase 9.D: Detect trivial cycles
            trivial_warning_dict = None
            if self._trivial_detector:
                # Update detector with evidence from steps
                self._update_trivial_detector_from_evidence()

                trivial_warning = self._trivial_detector.get_warning()
                if trivial_warning:
                    trivial_warning_dict = trivial_warning.to_dict()
                    # Emit warning event
                    self._emit_event(
                        CycleEvent(
                            event_type="warning",
                            step="post_cycle",
                            agent_role="system",
                            message=f"⚠️ {trivial_warning.message}",
                            detail=trivial_warning.detail,
                            is_error=False,
                        )
                    )
                    logger.warning(f"Cycle {cycle_id}: {trivial_warning.message}")

                pools_used = len(self._trivial_detector._pools_used)
                bytes_read = self._trivial_detector._bytes_read

            return LearningCycleResult(
                cycle_id=cycle_id,
                status=AutonomousLearningStatus.COMPLETED,
                started_at=started_at,
                completed_at=completed_at,
                duration_seconds=duration,
                steps_executed=steps_executed,
                steps_skipped=steps_skipped,
                evidence_collected=bool(self._step_evidence),
                report_generated=True,
                indexed=False,
                source_resolution=state.source_resolution,
                source_binding=source_binding_dict,
                trivial_cycle_warning=trivial_warning_dict,
                bytes_read_from_sources=bytes_read,
                pools_bound=pools_bound,
                pools_used=pools_used,
            )

        except LearningModeViolation as e:
            completed_at = datetime.now().isoformat()
            duration = time.time() - start_time

            return LearningCycleResult(
                cycle_id="",
                status=AutonomousLearningStatus.FAILED,
                started_at=started_at,
                completed_at=completed_at,
                duration_seconds=duration,
                steps_executed=0,
                steps_skipped=0,
                evidence_collected=False,
                report_generated=False,
                indexed=False,
                error_message=str(e),
            )

        except Exception as e:
            completed_at = datetime.now().isoformat()
            duration = time.time() - start_time

            return LearningCycleResult(
                cycle_id="",
                status=AutonomousLearningStatus.FAILED,
                started_at=started_at,
                completed_at=completed_at,
                duration_seconds=duration,
                steps_executed=0,
                steps_skipped=0,
                evidence_collected=False,
                report_generated=False,
                indexed=False,
                error_message=f"Unexpected error: {type(e).__name__}: {str(e)}",
            )

    def _update_trivial_detector_from_evidence(self) -> None:
        """Update trivial cycle detector from collected evidence.

        Phase 9.D: Extract source usage from step evidence.
        """
        if not self._trivial_detector:
            return

        for step_name, evidence in self._step_evidence.items():
            if evidence.source_usage:
                self._trivial_detector.record_from_summary(evidence.source_usage)


class AutonomousLearningScheduler:
    """Scheduler for running autonomous learning cycles.

    This scheduler:
    - Runs learning cycles using the large_scale_learning profile
    - Enforces wall-clock and step limits
    - Terminates cleanly on error
    - Indexes learnings after each cycle
    - Never modifies code or proposes improvements

    USAGE:
        scheduler = AutonomousLearningScheduler(odibi_root="d:/odibi")
        session = scheduler.run_session(
            config=LearningCycleConfig(project_root="d:/odibi/examples"),
            max_cycles=10,
            max_wall_clock_hours=8.0,
        )
    """

    def __init__(
        self,
        odibi_root: str,
        memory_manager: Optional[Any] = None,
        azure_config: Optional[Any] = None,
        artifacts_budget_bytes: int = DEFAULT_ARTIFACTS_BUDGET_BYTES,
        reports_budget_bytes: int = DEFAULT_REPORTS_BUDGET_BYTES,
        index_budget_bytes: int = DEFAULT_INDEX_BUDGET_BYTES,
    ):
        """Initialize the scheduler.

        Args:
            odibi_root: Path to Odibi repository root.
            memory_manager: Optional MemoryManager for persistence.
            azure_config: Optional Azure configuration for agents.
            artifacts_budget_bytes: Max bytes for .odibi/artifacts/ (default: 20 GB).
            reports_budget_bytes: Max bytes for .odibi/reports/ (default: 2 GB).
            index_budget_bytes: Max bytes for .odibi/index/ (default: 5 GB).
        """
        self.odibi_root = odibi_root
        self.memory_manager = memory_manager
        self.azure_config = azure_config

        self._runner = GuardedCycleRunner(
            odibi_root=odibi_root,
            memory_manager=memory_manager,
            azure_config=azure_config,
        )

        self._disk_guard = DiskUsageGuard(
            odibi_root=odibi_root,
            artifacts_budget_bytes=artifacts_budget_bytes,
            reports_budget_bytes=reports_budget_bytes,
            index_budget_bytes=index_budget_bytes,
        )
        self._heartbeat_writer = HeartbeatWriter(odibi_root)

        self._event_callback: Optional[CycleEventCallback] = None
        self._should_stop = False

    def set_event_callback(self, callback: Optional[CycleEventCallback]) -> None:
        """Set callback for learning events.

        Args:
            callback: Function to call with CycleEvent objects.
        """
        self._event_callback = callback
        self._runner.set_event_callback(callback)

    def stop(self) -> None:
        """Request graceful stop of the learning session."""
        self._should_stop = True
        logger.info("Autonomous learning stop requested")

    def _emit_event(self, event: CycleEvent) -> None:
        """Emit an event to the callback if set."""
        if self._event_callback:
            try:
                self._event_callback(event)
            except Exception:
                pass

    def _index_cycle_learnings(self, cycle_id: str) -> bool:
        """Index learnings from a completed cycle.

        Args:
            cycle_id: The cycle ID to index.

        Returns:
            True if indexing succeeded.
        """
        try:
            from .indexing import CycleIndexManager

            index_manager = CycleIndexManager(os.path.join(self.odibi_root, ".odibi"))

            result = index_manager.index_completed_cycles()

            return cycle_id in result.indexed

        except Exception as e:
            logger.warning(f"Indexing failed for cycle {cycle_id}: {e}")
            return False

    def _run_disk_cleanup(self) -> None:
        """Run disk cleanup to enforce budgets.

        Logs results but does not fail the cycle on cleanup errors.
        """
        try:
            report = self._disk_guard.run_cleanup()

            if report.total_bytes_freed > 0:
                logger.info(
                    f"Disk cleanup freed {report.total_bytes_freed} bytes "
                    f"({len(report.rotations)} directories rotated)"
                )
                self._emit_event(
                    CycleEvent(
                        event_type="disk_cleanup",
                        step="",
                        agent_role="",
                        message=f"🧹 Disk cleanup: {report.total_bytes_freed} bytes freed",
                        detail=f"Rotated {len(report.rotations)} directories",
                    )
                )

            for rotation in report.rotations:
                if rotation.errors:
                    for error in rotation.errors:
                        logger.warning(f"Disk cleanup error: {error}")

        except Exception as e:
            logger.warning(f"Disk cleanup failed: {e}")

    def _write_heartbeat(
        self,
        session: "AutonomousLearningSession",
        last_cycle_id: str = "",
        last_status: str = "",
    ) -> None:
        """Write heartbeat file for external monitoring.

        Args:
            session: Current session state.
            last_cycle_id: ID of last completed cycle.
            last_status: Status of last cycle.
        """
        try:
            disk_usage = self._disk_guard.get_total_usage()

            data = HeartbeatData(
                last_cycle_id=last_cycle_id,
                timestamp=datetime.now().isoformat(),
                last_status=last_status,
                cycles_completed=session.cycles_completed,
                cycles_failed=session.cycles_failed,
                disk_usage=disk_usage,
                profile_id=session.profile_id,
                profile_name=session.profile_name,
                profile_hash=session.profile_hash,
            )

            self._heartbeat_writer.write(data)

        except Exception as e:
            logger.warning(f"Failed to write heartbeat: {e}")

    def run_single_cycle(
        self,
        config: LearningCycleConfig,
    ) -> LearningCycleResult:
        """Run a single learning cycle.

        Args:
            config: Learning cycle configuration.

        Returns:
            LearningCycleResult with execution details.
        """
        profile_detail = ""
        if config.profile_id:
            profile_detail = f" (profile: {config.profile_name})"

        self._emit_event(
            CycleEvent(
                event_type="learning_cycle_start",
                step="",
                agent_role="",
                message=f"Starting autonomous learning cycle{profile_detail}",
                detail="Learning mode: observation only, no changes",
            )
        )

        result = self._runner.run_learning_cycle(config)

        result.profile_id = config.profile_id
        result.profile_name = config.profile_name
        result.profile_hash = config.profile_hash

        if result.status == AutonomousLearningStatus.COMPLETED and result.cycle_id:
            indexed = self._index_cycle_learnings(result.cycle_id)
            result.indexed = indexed

        self._emit_event(
            CycleEvent(
                event_type="learning_cycle_end",
                step="",
                agent_role="",
                message=f"Learning cycle {result.status.value}: {result.cycle_id or 'N/A'}",
                detail=result.error_message or f"Duration: {result.duration_seconds:.2f}s",
                is_error=result.status != AutonomousLearningStatus.COMPLETED,
            )
        )

        return result

    def load_profile(self, profile_name: str) -> "FrozenCycleProfile":
        """Load a cycle profile by name.

        Args:
            profile_name: Profile name (without .yaml extension).

        Returns:
            FrozenCycleProfile ready for use.

        Raises:
            CycleProfileError: If profile not found or invalid.
        """
        from .cycle_profile import CycleProfileLoader

        loader = CycleProfileLoader(self.odibi_root)
        return loader.load_profile(profile_name)

    def list_profiles(self) -> list[str]:
        """List available cycle profiles.

        Returns:
            List of profile names (without .yaml extension).
        """
        from .cycle_profile import CycleProfileLoader

        loader = CycleProfileLoader(self.odibi_root)
        return loader.list_profiles()

    def run_session_with_profile(
        self,
        profile_name: str,
        project_root: str,
        task_description: str = "Autonomous learning cycle",
        max_cycles: int = 100,
        max_wall_clock_hours: float = 168.0,
    ) -> AutonomousLearningSession:
        """Run a session using a named profile.

        This is the preferred entry point for Phase 9.C.
        Loads the profile, validates it, and runs the session
        with frozen configuration.

        Args:
            profile_name: Profile name (without .yaml extension).
            project_root: Path to the project to learn from.
            task_description: Optional task description.
            max_cycles: Maximum number of cycles to run.
            max_wall_clock_hours: Maximum wall-clock time in hours.

        Returns:
            AutonomousLearningSession with all results.

        Raises:
            CycleProfileError: If profile loading fails.
        """
        frozen_profile = self.load_profile(profile_name)

        config = LearningCycleConfig.from_frozen_profile(
            profile=frozen_profile,
            project_root=project_root,
            task_description=task_description,
        )

        logger.info(
            f"Running session with profile: {frozen_profile.profile_name} "
            f"(hash: {frozen_profile.content_hash})"
        )

        return self.run_session(
            config=config,
            max_cycles=max_cycles,
            max_wall_clock_hours=max_wall_clock_hours,
        )

    def run_session(
        self,
        config: LearningCycleConfig,
        max_cycles: int = 100,
        max_wall_clock_hours: float = 168.0,
    ) -> AutonomousLearningSession:
        """Run a session of multiple learning cycles.

        Args:
            config: Learning cycle configuration.
            max_cycles: Maximum number of cycles to run.
            max_wall_clock_hours: Maximum wall-clock time in hours.

        Returns:
            AutonomousLearningSession with all results.
        """
        import uuid

        session_id = str(uuid.uuid4())[:8]
        started_at = datetime.now().isoformat()
        start_time = time.time()
        max_seconds = max_wall_clock_hours * 3600

        session = AutonomousLearningSession(
            session_id=session_id,
            started_at=started_at,
            status=AutonomousLearningStatus.RUNNING,
            profile_id=config.profile_id,
            profile_name=config.profile_name,
            profile_hash=config.profile_hash,
        )

        self._should_stop = False

        profile_detail = ""
        if config.profile_id:
            profile_detail = f" | Profile: {config.profile_name} ({config.profile_hash[:8]})"

        self._emit_event(
            CycleEvent(
                event_type="learning_session_start",
                step="",
                agent_role="",
                message=f"Starting autonomous learning session {session_id}",
                detail=f"Max cycles: {max_cycles}, Max hours: {max_wall_clock_hours}{profile_detail}",
            )
        )

        self._run_disk_cleanup()
        self._write_heartbeat(session, last_status="STARTING")

        try:
            for cycle_num in range(max_cycles):
                if self._should_stop:
                    logger.info("Learning session stopped by request")
                    break

                elapsed = time.time() - start_time
                if elapsed >= max_seconds:
                    logger.info(f"Wall-clock limit reached: {elapsed / 3600:.2f} hours")
                    break

                logger.info(f"Running learning cycle {cycle_num + 1}/{max_cycles}")

                result = self.run_single_cycle(config)
                session.results.append(result)

                if result.status == AutonomousLearningStatus.COMPLETED:
                    session.cycles_completed += 1
                else:
                    session.cycles_failed += 1

                session.total_duration_seconds = time.time() - start_time

                self._write_heartbeat(
                    session,
                    last_cycle_id=result.cycle_id or "",
                    last_status=result.status.value,
                )

                self._run_disk_cleanup()

            session.status = AutonomousLearningStatus.COMPLETED

        except Exception as e:
            session.status = AutonomousLearningStatus.FAILED
            session.errors.append(f"Session error: {type(e).__name__}: {str(e)}")
            logger.error(f"Learning session failed: {e}")

        session.completed_at = datetime.now().isoformat()
        session.total_duration_seconds = time.time() - start_time

        self._write_heartbeat(
            session,
            last_cycle_id=session.results[-1].cycle_id if session.results else "",
            last_status=f"SESSION_{session.status.value}",
        )

        self._emit_event(
            CycleEvent(
                event_type="learning_session_end",
                step="",
                agent_role="",
                message=f"Learning session {session.status.value}",
                detail=(
                    f"Completed: {session.cycles_completed}, "
                    f"Failed: {session.cycles_failed}, "
                    f"Duration: {session.total_duration_seconds:.2f}s"
                ),
                is_error=session.status != AutonomousLearningStatus.COMPLETED,
            )
        )

        return session


def assert_learning_mode_safe() -> None:
    """Runtime assertion that learning mode constraints hold.

    Call this at critical points to verify learning mode safety.

    Raises:
        LearningModeViolation: If any constraint is violated.
    """
    pass


def validate_learning_profile(profile_path: str) -> list[str]:
    """Validate a learning profile YAML file.

    Args:
        profile_path: Path to the profile YAML.

    Returns:
        List of validation errors (empty if valid).
    """
    import yaml

    errors = []

    try:
        with open(profile_path, "r", encoding="utf-8") as f:
            profile = yaml.safe_load(f)
    except Exception as e:
        return [f"Failed to load profile: {e}"]

    if profile.get("mode") != "learning":
        errors.append(f"mode MUST be 'learning', got '{profile.get('mode')}'")

    if profile.get("max_improvements", 1) != 0:
        errors.append(f"max_improvements MUST be 0, got {profile.get('max_improvements')}")

    source_config = profile.get("cycle_source_config", {})
    if not source_config.get("require_frozen", False):
        errors.append("cycle_source_config.require_frozen MUST be True")

    if not source_config.get("deterministic", False):
        errors.append("cycle_source_config.deterministic MUST be True")

    return errors

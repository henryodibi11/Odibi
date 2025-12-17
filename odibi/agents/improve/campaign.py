"""CampaignRunner: Orchestrates autonomous improvement campaigns.

Runs multi-cycle improvement campaigns that:
- Create sandboxes for experimentation
- Run validation gates (tests, lint, golden projects)
- Use LLM brain to fix gate failures
- Promote passing changes to Master
- Track progress and lessons learned
- Stop on configured conditions (max_cycles, max_hours, convergence)
"""

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from odibi.agents.improve.brain import ImprovementBrain
    from odibi.agents.improve.events import BrainEvent, EventEmitter

from odibi.agents.improve.config import CampaignConfig
from odibi.agents.improve.environment import ImprovementEnvironment
from odibi.agents.improve.memory import CampaignMemory
from odibi.agents.improve.results import CycleResult, GateCheckResult
from odibi.agents.improve.runner import OdibiPipelineRunner
from odibi.agents.improve.sandbox import SandboxInfo
from odibi.agents.improve.status import StatusTracker

logger = logging.getLogger(__name__)


class CampaignError(Exception):
    """Base exception for campaign operations."""

    pass


@dataclass
class CampaignResult:
    """Final result of a campaign run."""

    campaign_id: str
    status: str
    stop_reason: str
    started_at: datetime
    completed_at: datetime
    cycles_completed: int
    improvements_promoted: int
    improvements_rejected: int
    elapsed_hours: float
    cycles: list[CycleResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """Check if campaign had at least one promotion."""
        return self.improvements_promoted > 0


class CampaignRunner:
    """Run autonomous improvement campaigns.

    Orchestrates the improvement loop:
    1. Create sandbox from Master
    2. Run validation gates
    3. If all gates pass, promote to Master
    4. Record lessons learned
    5. Repeat until stop condition

    Attributes:
        environment: The improvement environment.
        campaign_config: Campaign-specific configuration.
        memory: Persistent campaign memory.
        status: Status tracker for reporting.
    """

    def __init__(
        self,
        environment: ImprovementEnvironment,
        campaign_config: CampaignConfig,
        on_brain_event: Optional[Callable[["BrainEvent"], None]] = None,
        event_emitter: Optional["EventEmitter"] = None,
    ) -> None:
        """Initialize campaign runner.

        Args:
            environment: The improvement environment (must be initialized).
            campaign_config: Campaign-specific configuration.
            on_brain_event: Optional callback for real-time brain events.
            event_emitter: Optional shared event emitter for brain events.
        """
        self._env = environment
        self._config = campaign_config
        self._on_brain_event = on_brain_event
        self._event_emitter = event_emitter
        self._memory = CampaignMemory(environment.config.memory_path)
        self._status = StatusTracker(
            environment.config.status_file_path,
            environment.config.reports_path,
        )
        self._campaign_id: Optional[str] = None
        self._started_at: Optional[datetime] = None
        self._cycles: list[CycleResult] = []
        self._brain: Optional["ImprovementBrain"] = None

        if campaign_config.enable_llm_improvements:
            self._init_brain()

    def _init_brain(self) -> None:
        """Initialize the LLM improvement brain."""
        try:
            from odibi.agents.improve.brain import create_improvement_brain

            self._brain = create_improvement_brain(
                model=self._config.llm_model,
                endpoint=self._config.llm_endpoint,
                api_key=self._config.llm_api_key,
                max_attempts=self._config.max_improvement_attempts_per_cycle,
                on_event=self._on_brain_event,
                event_emitter=self._event_emitter,
                enable_streaming=True,
            )
            logger.info(f"LLM brain initialized with model {self._config.llm_model}")
        except Exception as e:
            logger.warning(f"Failed to initialize LLM brain: {e}")
            self._brain = None

    @property
    def environment(self) -> ImprovementEnvironment:
        """The improvement environment."""
        return self._env

    @property
    def campaign_config(self) -> CampaignConfig:
        """Campaign configuration."""
        return self._config

    @property
    def memory(self) -> CampaignMemory:
        """Campaign memory."""
        return self._memory

    @property
    def status(self) -> StatusTracker:
        """Status tracker."""
        return self._status

    def _generate_campaign_id(self) -> str:
        """Generate unique campaign ID."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        short_uuid = str(uuid.uuid4())[:8]
        return f"campaign_{timestamp}_{short_uuid}"

    def elapsed_hours(self) -> float:
        """Get elapsed time in hours since campaign start."""
        if self._started_at is None:
            return 0.0
        return (datetime.now() - self._started_at).total_seconds() / 3600

    def run(self) -> CampaignResult:
        """Run improvement campaign until stop condition.

        Executes cycles until one of:
        - max_cycles is reached
        - max_hours is exceeded
        - convergence (N cycles without learning)

        Returns:
            CampaignResult with final status.
        """
        self._campaign_id = self._generate_campaign_id()
        self._started_at = datetime.now()
        self._cycles = []

        logger.info(f"Starting campaign: {self._campaign_id}")
        logger.info(f"  Max cycles: {self._config.max_cycles}")
        logger.info(f"  Max hours: {self._config.max_hours}")
        logger.info(f"  Convergence threshold: {self._config.convergence_threshold}")

        self._status.start_campaign(self._campaign_id)

        stop_reason = "MAX_CYCLES_REACHED"

        try:
            for cycle_num in range(self._config.max_cycles):
                # Check time budget
                if self.elapsed_hours() > self._config.max_hours:
                    stop_reason = "TIME_BUDGET_EXHAUSTED"
                    logger.info(f"Time budget exhausted ({self._config.max_hours}h)")
                    break

                # Check convergence
                if self.is_converged():
                    stop_reason = "CONVERGED"
                    logger.info(
                        f"Converged after {self._config.convergence_threshold} "
                        "cycles without learning"
                    )
                    break

                # Run one cycle
                result = self.run_cycle(cycle_num)
                self._cycles.append(result)

                # Record to memory and update status
                self._memory.record(result)
                self._status.update(result)

        except Exception as e:
            logger.error(f"Campaign error: {e}")
            stop_reason = f"ERROR: {e}"

        return self._finalize(stop_reason)

    def _finalize(self, stop_reason: str) -> CampaignResult:
        """Finalize campaign and generate result."""
        completed_at = datetime.now()
        elapsed = self.elapsed_hours()

        promoted = sum(1 for c in self._cycles if c.promoted)
        rejected = sum(1 for c in self._cycles if not c.promoted)

        self._status.finish_campaign(stop_reason)

        # Save final report
        try:
            self._status.save_report()
        except Exception as e:
            logger.warning(f"Failed to save report: {e}")

        logger.info(f"Campaign completed: {stop_reason}")
        logger.info(f"  Cycles: {len(self._cycles)}")
        logger.info(f"  Promoted: {promoted}")
        logger.info(f"  Rejected: {rejected}")
        logger.info(f"  Elapsed: {elapsed:.2f}h")

        return CampaignResult(
            campaign_id=self._campaign_id or "unknown",
            status="COMPLETED",
            stop_reason=stop_reason,
            started_at=self._started_at or datetime.now(),
            completed_at=completed_at,
            cycles_completed=len(self._cycles),
            improvements_promoted=promoted,
            improvements_rejected=rejected,
            elapsed_hours=elapsed,
            cycles=self._cycles,
        )

    def run_cycle(self, cycle_num: int) -> CycleResult:
        """Run one improvement cycle.

        Creates sandbox, runs gates, uses LLM brain if gates fail,
        promotes if passing, cleans up.

        Args:
            cycle_num: Zero-based cycle number.

        Returns:
            CycleResult with cycle outcome.
        """
        # Include campaign_id in cycle_id for uniqueness across campaigns
        campaign_short = (self._campaign_id or "unknown")[-12:]  # Last 12 chars
        cycle_id = f"{campaign_short}_cycle_{cycle_num:03d}"
        logger.info(f"Starting {cycle_id}")

        result = CycleResult(
            cycle_id=cycle_id,
            sandbox_path=Path("."),  # Will be updated
            started_at=datetime.now(),
        )

        # Query memory for things to avoid
        avoid_issues = self._memory.get_failed_approaches()
        if avoid_issues:
            logger.debug(f"Avoiding {len(avoid_issues)} known failed approaches")

        # Create sandbox
        sandbox = self._env.create_sandbox(cycle_id)
        result.sandbox_path = sandbox.sandbox_path
        improvement_attempts = 0

        try:
            # Run gate checks
            gate_result = self.check_gates(sandbox)

            # If gates fail and LLM improvements are enabled, try to fix
            if not gate_result.all_passed and self._brain is not None:
                logger.info(f"{cycle_id}: Gates failed, attempting LLM improvement")
                gate_result, improvement_attempts, files_changed = self._brain.improve_sandbox(
                    sandbox=sandbox,
                    gate_result=gate_result,
                    avoid_issues=avoid_issues,
                    campaign_goal=self._config.goal,
                    run_gates=lambda s: self.check_gates(s),
                )
                result.files_modified = files_changed
                logger.info(
                    f"{cycle_id}: LLM made {improvement_attempts} attempt(s), "
                    f"modified {len(files_changed)} file(s), "
                    f"gates {'passed' if gate_result.all_passed else 'still failing'}"
                )

            result.gate_result = gate_result

            # Extract results from gate check
            if gate_result.test_result:
                result.test_result = gate_result.test_result
                result.tests_passed = gate_result.test_result.passed
                result.tests_failed = gate_result.test_result.failed

            if gate_result.lint_result:
                result.lint_result = gate_result.lint_result
                result.lint_errors = gate_result.lint_result.error_count

            result.golden_results = gate_result.golden_results
            result.golden_passed = sum(1 for g in gate_result.golden_results if g.passed)
            result.golden_failed = sum(1 for g in gate_result.golden_results if not g.passed)

            if gate_result.all_passed:
                # All gates passed - promote to master
                logger.info(f"{cycle_id}: All gates passed, promoting to Master")

                # Snapshot before promotion
                self._env.snapshot_master(cycle_id)

                # Promote
                self._env.promote_to_master(sandbox)

                lesson = (
                    f"Gates passed after {improvement_attempts} LLM attempts"
                    if improvement_attempts > 0
                    else "Gates passed, changes promoted"
                )
                result.mark_complete(
                    promoted=True,
                    learning=True,
                    lesson=lesson,
                )
            else:
                # Gates failed - reject
                reasons = gate_result.failure_reasons
                reason = "; ".join(reasons) if reasons else "Gate check failed"
                if improvement_attempts > 0:
                    reason = f"LLM tried {improvement_attempts} times but: {reason}"
                logger.info(f"{cycle_id}: Gates failed - {reason}")

                result.mark_complete(
                    promoted=False,
                    rejection_reason=reason,
                    learning=len(reasons) > 0,  # Learned if we know what failed
                    lesson=reason,
                )

        except Exception as e:
            logger.error(f"{cycle_id} error: {e}")
            result.mark_complete(
                promoted=False,
                rejection_reason=str(e),
                learning=False,
            )

        finally:
            # Always destroy sandbox
            try:
                self._env.destroy_sandbox(sandbox)
            except Exception as e:
                logger.warning(f"Failed to destroy sandbox: {e}")

        return result

    def check_gates(self, sandbox: SandboxInfo) -> GateCheckResult:
        """Run all promotion gates on a sandbox.

        Args:
            sandbox: The sandbox to validate.

        Returns:
            GateCheckResult with all gate outcomes.
        """
        # Create runner with WSL support if configured
        python_cmd = self._config.wsl_python if self._config.use_wsl else "python"
        runner = OdibiPipelineRunner(
            sandbox.sandbox_path,
            python_cmd=python_cmd,
            use_wsl=self._config.use_wsl,
            wsl_distro=self._config.wsl_distro,
            wsl_env=self._config.wsl_env,
            wsl_shell_init=self._config.wsl_shell_init,
        )
        failure_reasons = []

        # Gate 1: Tests (with ignore patterns for non-odibi tests)
        test_result = None
        tests_passed = True
        if self._config.require_tests_pass:
            # Build --ignore args for agent tests
            extra_args = []
            for pattern in self._config.test_ignore_patterns:
                extra_args.append(f"--ignore={pattern}")

            test_result = runner.run_tests(
                test_path=self._config.test_path,
                extra_args=extra_args if extra_args else None,
            )
            tests_passed = test_result.success
            if not tests_passed:
                if test_result.failed > 0 or test_result.errors > 0:
                    failure_reasons.append(
                        f"Tests failed: {test_result.failed} failed, {test_result.errors} errors"
                    )
                elif test_result.exit_code != 0:
                    failure_reasons.append(
                        f"Tests crashed: exit code {test_result.exit_code} (collection or import error)"
                    )
                else:
                    failure_reasons.append("Tests failed for unknown reason")

        # Gate 2: Lint (only odibi package, not agents)
        lint_result = None
        lint_passed = True
        if self._config.require_lint_clean:
            lint_result = runner.run_lint(path=self._config.lint_path)
            lint_passed = lint_result.success
            if not lint_passed:
                failure_reasons.append(f"Lint errors: {lint_result.error_count}")

        # Gate 3: Validation (placeholder - no specific files to validate)
        validate_passed = True
        validation_results = []

        # Gate 4: Golden projects
        golden_results = []
        golden_passed = True
        if self._config.require_golden_pass:
            golden_results = runner.run_golden_projects()
            failed_golden = [g for g in golden_results if not g.passed]
            golden_passed = len(failed_golden) == 0
            if not golden_passed:
                names = [g.config_name for g in failed_golden]
                failure_reasons.append(f"Golden projects failed: {', '.join(names)}")

        all_passed = tests_passed and lint_passed and validate_passed and golden_passed

        return GateCheckResult(
            tests_passed=tests_passed,
            lint_passed=lint_passed,
            validate_passed=validate_passed,
            golden_passed=golden_passed,
            all_passed=all_passed,
            test_result=test_result,
            lint_result=lint_result,
            validation_results=validation_results,
            golden_results=golden_results,
            failure_reasons=failure_reasons,
        )

    def is_converged(self) -> bool:
        """Check if campaign has converged (stopped learning).

        Convergence occurs when the last N cycles all had no learning,
        where N is the convergence_threshold.

        Returns:
            True if converged.
        """
        threshold = self._config.convergence_threshold

        if len(self._cycles) < threshold:
            return False

        recent = self._cycles[-threshold:]
        return all(not c.learning for c in recent)


def create_campaign_runner(
    environment_root: Path,
    campaign_config: Optional[CampaignConfig] = None,
    on_brain_event: Optional[Callable[["BrainEvent"], None]] = None,
    event_emitter: Optional["EventEmitter"] = None,
) -> CampaignRunner:
    """Create a CampaignRunner from an existing environment.

    Args:
        environment_root: Path to initialized environment.
        campaign_config: Optional campaign config (uses defaults if None).
        on_brain_event: Optional callback for real-time brain events.
        event_emitter: Optional shared event emitter.

    Returns:
        Configured CampaignRunner.

    Raises:
        CampaignError: If environment is not initialized.
    """
    from odibi.agents.improve.environment import load_environment

    try:
        env = load_environment(environment_root)
    except Exception as e:
        raise CampaignError(f"Failed to load environment: {e}") from e

    if campaign_config is None:
        campaign_config = CampaignConfig()

    return CampaignRunner(
        env,
        campaign_config,
        on_brain_event=on_brain_event,
        event_emitter=event_emitter,
    )

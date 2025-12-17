"""Cycle execution model for the Odibi AI Agent Suite.

A Cycle is:
- Finite (10 ordered steps)
- Deterministic (fixed step order and agent mapping)
- Logged (structured logs for each step)
- Interruptible (can pause and resume)
- Has explicit stop conditions

This module provides the Scheduled Assistant mode capability while
maintaining the assistant metaphor.

ENFORCEMENT (Phase 0):
- Wall-clock timeout is MECHANICALLY enforced (not advisory)
- Review gating is MECHANICALLY enforced (APPROVED keyword required)
- Cycle termination is GUARANTEED via max_steps OR timeout OR interrupt
"""

import hashlib
import json
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional, Union

import logging

from .agent_base import AgentContext, AgentRegistry, AgentResponse, AgentRole
from .disk_guard import ReportErrorWriter
from .enforcement import (
    CycleExitStatus,
    CycleTerminationEnforcer,
    ReviewDecision,
    ReviewGateEnforcer,
    ReviewGatingError,
)
from .evidence import ArtifactWriter, ExecutionEvidence
from .execution import ExecutionGateway
from .observation_integrity import (
    ObservationIntegrityError,
    validate_observer_output_integrity,
)
from .reports import CycleReportGenerator
from .source_selection import CycleMode

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GoldenProjectConfig:
    """A single golden project declaration.

    Golden projects are immutable verification targets.
    They are never modified by agents during cycles.
    """

    name: str
    """Human-readable name for logging and display."""

    path: str
    """Path to the pipeline config file.

    May be:
    - Absolute: D:/projects/sales_etl/pipeline.yaml
    - Relative: sales_etl/pipeline.yaml (resolved against workspace_root)
    - Simple filename: scale_join.odibi.yaml (auto-resolved to learning harness)
    """

    description: Optional[str] = None
    """Optional description of what this golden project tests."""

    def resolve_path(self, workspace_root: str) -> str:
        """Resolve path to absolute, using workspace_root for relative paths.

        Resolution order:
        1. If path is absolute, use as-is
        2. If path exists relative to workspace_root, use that
        3. If path exists in .odibi/learning_harness/, use that (auto-prefix)
        4. Fall back to workspace_root + path
        """
        p = Path(self.path)
        if p.is_absolute():
            return str(p)

        # Try relative to workspace_root first
        workspace_resolved = Path(workspace_root) / p
        if workspace_resolved.exists():
            return str(workspace_resolved)

        # Auto-resolve to learning harness if it's just a filename
        harness_path = Path(workspace_root) / ".odibi" / "learning_harness" / p
        if harness_path.exists():
            return str(harness_path)

        # Fall back to workspace_root + path (will fail at runtime if not found)
        return str(workspace_resolved)


@dataclass
class GoldenProjectResult:
    """Result of running a single golden project."""

    name: str
    path: str
    status: str  # "PASSED" | "FAILED" | "ERROR" | "SKIPPED"
    exit_code: Optional[int] = None
    output_summary: str = ""
    error_message: str = ""
    execution_time_seconds: float = 0.0

    @property
    def is_regression(self) -> bool:
        return self.status in ("FAILED", "ERROR")


@dataclass
class RegressionCheckResult:
    """Aggregate result of all golden project checks."""

    results: list[GoldenProjectResult] = field(default_factory=list)
    total_count: int = 0
    passed_count: int = 0
    failed_count: int = 0
    regression_detected: bool = False

    def __post_init__(self):
        self.total_count = len(self.results)
        self.passed_count = sum(1 for r in self.results if r.status == "PASSED")
        self.failed_count = sum(1 for r in self.results if r.is_regression)
        self.regression_detected = self.failed_count > 0


def parse_golden_projects_input(
    input_value: Union[str, list], workspace_root: str = ""
) -> list[GoldenProjectConfig]:
    """Parse UI input into GoldenProjectConfig list.

    Supports:
    - Simple string: "path1.yaml, path2.yaml"
    - Named string: "name:path, name:path"
    - List of strings: ["path1.yaml", "path2.yaml"]
    - List of dicts: [{"name": "n", "path": "p"}, ...]
    - List of GoldenProjectConfig: pass-through

    Args:
        input_value: Raw input from UI or config.
        workspace_root: Base path for relative path resolution.

    Returns:
        List of GoldenProjectConfig objects.
    """
    if not input_value:
        return []

    if isinstance(input_value, str):
        if not input_value.strip():
            return []

        projects = []
        for item in input_value.split(","):
            item = item.strip()
            if not item:
                continue

            if ":" in item and len(item) > 2 and item[1] != ":":
                name, path = item.split(":", 1)
                name = name.strip()
                path = path.strip()
            else:
                path = item
                name = Path(item).stem

            projects.append(GoldenProjectConfig(name=name, path=path))

        return projects

    if isinstance(input_value, list):
        projects = []
        for item in input_value:
            if isinstance(item, GoldenProjectConfig):
                projects.append(item)
            elif isinstance(item, dict):
                projects.append(
                    GoldenProjectConfig(
                        name=item.get("name", Path(item.get("path", "")).stem),
                        path=item.get("path", ""),
                        description=item.get("description"),
                    )
                )
            elif isinstance(item, str):
                projects.append(GoldenProjectConfig(name=Path(item).stem, path=item))
        return projects

    return []


@dataclass
class CycleEvent:
    """Event emitted during cycle execution for UI updates."""

    event_type: str  # step_start, step_end, step_skip, agent_thinking, agent_response, cycle_end
    step: str
    agent_role: str
    message: str
    detail: str = ""
    is_error: bool = False


CycleEventCallback = Callable[[CycleEvent], None]


class AssistantMode(str, Enum):
    """Assistant operating modes."""

    INTERACTIVE = "interactive"
    GUIDED_EXECUTION = "guided_execution"
    SCHEDULED_ASSISTANT = "scheduled"


class CycleStep(str, Enum):
    """Ordered steps in a cycle."""

    ENV_VALIDATION = "env_validation"
    PROJECT_SELECTION = "project_selection"
    USER_EXECUTION = "user_execution"
    OBSERVATION = "observation"
    IMPROVEMENT_PROPOSAL = "improvement"
    REVIEW = "review"
    REGRESSION_CHECKS = "regression_checks"
    MEMORY_PERSISTENCE = "memory_persistence"
    SUMMARY = "summary"
    EXIT = "exit"


CYCLE_ORDER = [
    CycleStep.ENV_VALIDATION,
    CycleStep.PROJECT_SELECTION,
    CycleStep.USER_EXECUTION,
    CycleStep.OBSERVATION,
    CycleStep.IMPROVEMENT_PROPOSAL,
    CycleStep.REVIEW,
    CycleStep.REGRESSION_CHECKS,
    CycleStep.MEMORY_PERSISTENCE,
    CycleStep.SUMMARY,
    CycleStep.EXIT,
]


STEP_AGENT_MAPPING: dict[CycleStep, AgentRole] = {
    CycleStep.ENV_VALIDATION: AgentRole.ENVIRONMENT,
    CycleStep.PROJECT_SELECTION: AgentRole.PROJECT,
    CycleStep.USER_EXECUTION: AgentRole.USER,
    CycleStep.OBSERVATION: AgentRole.OBSERVER,
    CycleStep.IMPROVEMENT_PROPOSAL: AgentRole.IMPROVEMENT,
    CycleStep.REVIEW: AgentRole.REVIEWER,
    CycleStep.REGRESSION_CHECKS: AgentRole.REGRESSION_GUARD,
    CycleStep.MEMORY_PERSISTENCE: AgentRole.CURRICULUM,
    CycleStep.SUMMARY: AgentRole.CONVERGENCE,
    CycleStep.EXIT: AgentRole.CONVERGENCE,
}


@dataclass
class CycleConfig:
    """Configuration for a cycle run."""

    project_root: str
    task_description: str = "Scheduled maintenance cycle"
    max_improvements: int = 3
    max_runtime_hours: float = 8.0
    gated_improvements: bool = True
    stop_on_convergence: bool = True
    golden_projects: list[GoldenProjectConfig] = field(default_factory=list)

    @property
    def is_learning_mode(self) -> bool:
        """Check if this is a learning/observation-only cycle.

        Learning mode is active when max_improvements == 0.
        In learning mode:
        - ProjectAgent is skipped (uses user-provided project_root)
        - Memory writes go to learning scope (non-validated)
        - No source code changes can occur
        """
        return self.max_improvements == 0

    @property
    def is_improvement_mode(self) -> bool:
        """Check if this is an improvement cycle (can make changes)."""
        return self.max_improvements > 0

    def validate_golden_projects_for_mode(self) -> list[str]:
        """Return validation warnings for golden project configuration.

        Returns:
            List of warning messages (empty if valid).
        """
        warnings = []

        if self.is_improvement_mode and not self.golden_projects:
            warnings.append(
                "⚠️ IMPROVEMENT MODE with no golden projects configured. "
                "Regressions cannot be detected. Consider adding golden projects."
            )

        return warnings

    def get_golden_project_names(self) -> list[str]:
        """Get list of golden project names for display."""
        return [gp.name for gp in self.golden_projects]


@dataclass
class CycleStepLog:
    """Log entry for a single cycle step."""

    step: str
    agent_role: str
    started_at: str
    completed_at: str
    input_summary: str
    output_summary: str
    success: bool
    skipped: bool = False
    skip_reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CycleState:
    """State of a cycle execution."""

    cycle_id: str
    config: CycleConfig
    mode: str = AssistantMode.GUIDED_EXECUTION.value
    current_step_index: int = 0
    steps: list[str] = field(default_factory=list)
    logs: list[CycleStepLog] = field(default_factory=list)
    started_at: str = ""
    completed_at: str = ""
    completed: bool = False
    interrupted: bool = False
    interrupt_reason: str = ""
    improvements_approved: int = 0
    improvements_rejected: int = 0
    regressions_detected: int = 0
    convergence_reached: bool = False
    summary: str = ""
    exit_status: str = ""
    pending_proposal_hash: str = ""
    review_approved: bool = False
    golden_project_results: list[dict] = field(default_factory=list)
    source_resolution: Optional[dict[str, Any]] = None

    def __post_init__(self):
        if not self.steps:
            self.steps = [s.value for s in CYCLE_ORDER]
        if not self.started_at:
            self.started_at = datetime.now().isoformat()

    def current_step(self) -> Optional[CycleStep]:
        """Get the current step."""
        if self.current_step_index >= len(self.steps):
            return None
        return CycleStep(self.steps[self.current_step_index])

    def is_finished(self) -> bool:
        """Check if cycle is finished."""
        return self.completed or self.interrupted

    def get_final_status(self) -> str:
        """Return explicit final execution status.

        Returns exactly one of:
        - "SUCCESS": Cycle completed without errors or regressions
        - "FAILURE": Cycle interrupted or had critical errors
        - "PARTIAL (<reason>)": Cycle completed but with issues
        """
        if self.interrupted:
            return "FAILURE"
        if not self.completed:
            return "FAILURE"
        if self.regressions_detected > 0:
            return f"PARTIAL ({self.regressions_detected} golden project(s) failed)"
        if self.improvements_rejected > 0 and self.improvements_approved == 0:
            return "PARTIAL (all improvements rejected)"
        return "SUCCESS"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        config_dict = asdict(self.config)
        config_dict["golden_projects"] = [asdict(gp) for gp in self.config.golden_projects]
        data["config"] = config_dict
        data["logs"] = [asdict(log) for log in self.logs]
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CycleState":
        """Create from dictionary."""
        config_data = data.pop("config", {})
        logs_data = data.pop("logs", [])

        golden_projects_data = config_data.pop("golden_projects", [])
        golden_projects = [
            GoldenProjectConfig(**gp) if isinstance(gp, dict) else gp for gp in golden_projects_data
        ]

        config = CycleConfig(**config_data, golden_projects=golden_projects)
        logs = [CycleStepLog(**log) for log in logs_data]

        state = cls(config=config, **data)
        state.logs = logs
        return state


class CycleStore:
    """Persistent storage for cycle state."""

    def __init__(self, base_path: str):
        """Initialize cycle store.

        Args:
            base_path: Base path for storing cycle state files.
        """
        self.base_path = os.path.join(base_path, "cycles")
        os.makedirs(self.base_path, exist_ok=True)

    def save(self, state: CycleState) -> None:
        """Save cycle state to disk.

        Args:
            state: The cycle state to save.
        """
        path = os.path.join(self.base_path, f"{state.cycle_id}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state.to_dict(), f, indent=2)

    def load(self, cycle_id: str) -> CycleState:
        """Load cycle state from disk.

        Args:
            cycle_id: The cycle ID to load.

        Returns:
            The loaded CycleState.

        Raises:
            FileNotFoundError: If cycle state file doesn't exist.
        """
        path = os.path.join(self.base_path, f"{cycle_id}.json")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return CycleState.from_dict(data)

    def list_cycles(self, limit: int = 20) -> list[dict[str, Any]]:
        """List recent cycles.

        Args:
            limit: Maximum number of cycles to return.

        Returns:
            List of cycle summaries.
        """
        cycles = []
        for filename in os.listdir(self.base_path):
            if filename.endswith(".json"):
                path = os.path.join(self.base_path, filename)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    cycles.append(
                        {
                            "cycle_id": data.get("cycle_id"),
                            "started_at": data.get("started_at"),
                            "completed_at": data.get("completed_at"),
                            "completed": data.get("completed"),
                            "interrupted": data.get("interrupted"),
                            "steps_completed": data.get("current_step_index", 0),
                        }
                    )
                except Exception:
                    pass

        cycles.sort(key=lambda x: x.get("started_at", ""), reverse=True)
        return cycles[:limit]

    def get_latest(self) -> Optional[CycleState]:
        """Get the most recent cycle.

        Returns:
            The latest CycleState or None.
        """
        cycles = self.list_cycles(limit=1)
        if cycles:
            return self.load(cycles[0]["cycle_id"])
        return None


class CycleRunner:
    """Runs cycles step-by-step with deterministic execution.

    The CycleRunner orchestrates the 10-step cycle:
    1. Environment validation
    2. Project selection
    3. User-style execution (Odibi usage)
    4. Observation and classification
    5. Improvement proposal (optional, gated)
    6. Review and approval
    7. Regression checks
    8. Memory persistence
    9. Summary generation
    10. Exit

    The assistant MUST stop after a cycle completes.

    ENFORCEMENT (Phase 0):
    - Wall-clock timeout enforced via CycleTerminationEnforcer
    - Review gating enforced via ReviewGateEnforcer
    - Cycle termination GUARANTEED (max_steps OR timeout OR interrupt)
    """

    MAX_STEPS = 10

    def __init__(
        self,
        odibi_root: str,
        memory_manager: Optional[Any] = None,
        azure_config: Optional[Any] = None,
    ):
        """Initialize cycle runner.

        Args:
            odibi_root: Path to Odibi repository root.
            memory_manager: Optional MemoryManager for persistence.
            azure_config: Optional Azure configuration for agents.
        """
        self.odibi_root = odibi_root
        self.memory_manager = memory_manager
        self.azure_config = azure_config
        self.execution_gateway = ExecutionGateway(odibi_root)
        self.cycle_store = CycleStore(os.path.join(odibi_root, ".odibi", "memories"))
        self._report_generator = CycleReportGenerator(os.path.join(odibi_root, ".odibi"))
        self._artifact_writer = ArtifactWriter(os.path.join(odibi_root, ".odibi"))
        self._report_error_writer = ReportErrorWriter(odibi_root)
        self._termination_enforcer: Optional[CycleTerminationEnforcer] = None
        self._review_gate: Optional[ReviewGateEnforcer] = None
        self._event_callback: Optional[CycleEventCallback] = None
        self._step_evidence: dict[str, ExecutionEvidence] = {}

    def set_event_callback(self, callback: Optional[CycleEventCallback]) -> None:
        """Set callback for cycle events (for UI updates).

        Args:
            callback: Function to call with CycleEvent objects.
        """
        self._event_callback = callback

    def _emit_event(self, event: CycleEvent) -> None:
        """Emit an event to the callback if set."""
        if self._event_callback:
            try:
                self._event_callback(event)
            except Exception:
                pass  # Don't let callback errors break the cycle

    def start_cycle(
        self,
        config: CycleConfig,
        mode: AssistantMode = AssistantMode.GUIDED_EXECUTION,
        source_config: Optional[Any] = None,
    ) -> CycleState:
        """Start a new cycle.

        Phase 8.B: Now includes source resolution at cycle start.

        Args:
            config: Cycle configuration.
            mode: Assistant mode (guided or scheduled).
            source_config: Optional CycleSourceConfig for source resolution.

        Returns:
            Initial CycleState with source resolution metadata.
        """
        warnings = config.validate_golden_projects_for_mode()
        for warning in warnings:
            self._emit_event(
                CycleEvent(
                    event_type="warning",
                    step="pre_cycle",
                    agent_role="system",
                    message=warning,
                    is_error=False,
                )
            )

        max_runtime_seconds = config.max_runtime_hours * 3600
        self._termination_enforcer = CycleTerminationEnforcer(
            max_steps=self.MAX_STEPS,
            max_runtime_seconds=max_runtime_seconds,
        )
        self._termination_enforcer.start()

        self._review_gate = ReviewGateEnforcer()

        cycle_id = str(uuid.uuid4())

        # Phase 8.B: Resolve sources at cycle start (deterministic, read-only)
        source_resolution_dict = self._resolve_sources(
            cycle_id=cycle_id,
            mode=mode,
            source_config=source_config,
        )

        state = CycleState(
            cycle_id=cycle_id,
            config=config,
            mode=mode.value,
            source_resolution=source_resolution_dict,
        )

        # Emit source resolution event for visibility
        if source_resolution_dict:
            pool_count = len(source_resolution_dict.get("selected_pool_names", []))
            self._emit_event(
                CycleEvent(
                    event_type="source_resolution",
                    step="pre_cycle",
                    agent_role="system",
                    message=f"Source resolution complete: {pool_count} pool(s) selected",
                    detail=source_resolution_dict.get("selection_hash", ""),
                    is_error=False,
                )
            )

        self.cycle_store.save(state)
        return state

    def _resolve_sources(
        self,
        cycle_id: str,
        mode: AssistantMode,
        source_config: Optional[Any] = None,
    ) -> Optional[dict[str, Any]]:
        """Resolve sources at cycle start (Phase 8.B).

        This is READ-ONLY and DETERMINISTIC:
        - NO pipeline execution
        - NO data downloads
        - NO ExecutionGateway changes
        - Same inputs → Same outputs

        Args:
            cycle_id: The cycle identifier
            mode: The assistant mode
            source_config: Optional CycleSourceConfig

        Returns:
            Source resolution dict or None if resolution not available
        """
        try:
            from .source_resolution import SourceResolver, SourceResolutionError

            # Map AssistantMode to CycleMode
            cycle_mode = self._mode_to_cycle_mode(mode)

            resolver = SourceResolver(self.odibi_root)
            result = resolver.resolve(
                cycle_id=cycle_id,
                mode=cycle_mode,
                source_config=source_config,
            )

            return result.to_dict()

        except ImportError:
            # source_resolution module not available (graceful degradation)
            return None
        except SourceResolutionError as e:
            # Log but don't fail the cycle (source resolution is optional in 8.B)
            self._emit_event(
                CycleEvent(
                    event_type="warning",
                    step="pre_cycle",
                    agent_role="system",
                    message=f"Source resolution failed: {e.message}",
                    detail=str(e.details),
                    is_error=True,
                )
            )
            return None
        except Exception as e:
            # Unexpected error - log and continue
            self._emit_event(
                CycleEvent(
                    event_type="warning",
                    step="pre_cycle",
                    agent_role="system",
                    message=f"Source resolution error: {str(e)}",
                    is_error=True,
                )
            )
            return None

    def _mode_to_cycle_mode(self, mode: AssistantMode) -> CycleMode:
        """Convert AssistantMode to CycleMode.

        Args:
            mode: The assistant mode

        Returns:
            Corresponding CycleMode
        """
        if mode == AssistantMode.SCHEDULED_ASSISTANT:
            return CycleMode.SCHEDULED
        elif mode == AssistantMode.GUIDED_EXECUTION:
            # Guided execution can be learning or improvement based on config
            return CycleMode.LEARNING
        else:
            return CycleMode.LEARNING

    def run_next_step(self, state: CycleState) -> CycleState:
        """Run the next step in the cycle.

        ENFORCEMENT: Checks termination conditions BEFORE each step.

        Args:
            state: Current cycle state.

        Returns:
            Updated cycle state.
        """
        if state.is_finished():
            return state

        if self._termination_enforcer:
            term_status = self._termination_enforcer.check_termination()
            if term_status is not None:
                state.interrupted = True
                state.interrupt_reason = f"Termination enforced: {term_status.value}"
                state.exit_status = term_status.value
                state.completed_at = datetime.now().isoformat()
                self._emit_event(
                    CycleEvent(
                        event_type="cycle_end",
                        step="",
                        agent_role="",
                        message=f"Cycle terminated: {term_status.value}",
                        is_error=True,
                    )
                )
                self.cycle_store.save(state)
                return state

        step = state.current_step()
        if step is None:
            state.completed = True
            state.completed_at = datetime.now().isoformat()
            state.exit_status = CycleExitStatus.COMPLETED.value
            self.cycle_store.save(state)
            return state

        agent_role = STEP_AGENT_MAPPING.get(step)
        if not agent_role:
            state.interrupted = True
            state.interrupt_reason = f"No agent mapping for step: {step.value}"
            state.exit_status = CycleExitStatus.ERROR.value
            self.cycle_store.save(state)
            return state

        agent = AgentRegistry.get(agent_role)
        started_at = datetime.now().isoformat()

        if not agent:
            log = CycleStepLog(
                step=step.value,
                agent_role=agent_role.value,
                started_at=started_at,
                completed_at=datetime.now().isoformat(),
                input_summary="Agent not registered",
                output_summary=f"No agent registered for role: {agent_role.value}",
                success=False,
                skipped=True,
                skip_reason="Agent not available",
            )
            state.logs.append(log)
            state.current_step_index += 1
            if self._termination_enforcer:
                self._termination_enforcer.record_step()
            self.cycle_store.save(state)
            return state

        should_skip, skip_reason = self._should_skip_step(step, state)
        if should_skip:
            self._emit_event(
                CycleEvent(
                    event_type="step_skip",
                    step=step.value,
                    agent_role=agent_role.value,
                    message=f"⏭️ Skipping {step.value}",
                    detail=skip_reason,
                )
            )
            log = CycleStepLog(
                step=step.value,
                agent_role=agent_role.value,
                started_at=started_at,
                completed_at=datetime.now().isoformat(),
                input_summary="Step skipped",
                output_summary=skip_reason,
                success=True,
                skipped=True,
                skip_reason=skip_reason,
            )
            state.logs.append(log)
            state.current_step_index += 1
            if self._termination_enforcer:
                self._termination_enforcer.record_step()
            self.cycle_store.save(state)
            return state

        self._emit_event(
            CycleEvent(
                event_type="step_start",
                step=step.value,
                agent_role=agent_role.value,
                message=f"▶️ Running {step.value}",
                detail=f"Agent: {agent_role.value}",
            )
        )

        context = self._build_context(step, state)

        self._emit_event(
            CycleEvent(
                event_type="agent_thinking",
                step=step.value,
                agent_role=agent_role.value,
                message=f"🤔 {agent_role.value} is thinking...",
                detail=context.query,  # Full input prompt
            )
        )

        try:
            response = agent.process(context)
            success = True
            output_summary = response.content[:2000]
            self._emit_event(
                CycleEvent(
                    event_type="agent_response",
                    step=step.value,
                    agent_role=agent_role.value,
                    message=f"✅ {agent_role.value} completed",
                    detail=output_summary,  # Full output
                )
            )
        except Exception as e:
            import traceback

            success = False
            error_detail = f"{type(e).__name__}: {str(e)}"
            output_summary = f"Error: {error_detail}"
            response = None
            self._emit_event(
                CycleEvent(
                    event_type="agent_response",
                    step=step.value,
                    agent_role=agent_role.value,
                    message=f"❌ {agent_role.value} failed: {error_detail}",
                    detail=traceback.format_exc()[:1000],
                    is_error=True,
                )
            )

        log = CycleStepLog(
            step=step.value,
            agent_role=agent_role.value,
            started_at=started_at,
            completed_at=datetime.now().isoformat(),
            input_summary=context.query[:500],
            output_summary=output_summary,
            success=success,
            metadata=response.metadata if response else {},
        )
        state.logs.append(log)

        self._post_process_step(step, state, response)

        state.current_step_index += 1
        if self._termination_enforcer:
            self._termination_enforcer.record_step()

        if step == CycleStep.EXIT or state.current_step_index >= len(state.steps):
            state.completed = True
            state.completed_at = datetime.now().isoformat()
            state.exit_status = CycleExitStatus.COMPLETED.value

            final_status = state.get_final_status()
            status_icon = (
                "✅" if final_status == "SUCCESS" else ("⚠️" if "PARTIAL" in final_status else "❌")
            )
            detail_parts = [
                f"Final Status: **{final_status}**",
                f"Steps: {state.current_step_index}",
                f"Improvements: {state.improvements_approved} approved, {state.improvements_rejected} rejected",
            ]
            if state.regressions_detected > 0:
                detail_parts.append(
                    f"Regressions: {state.regressions_detected} golden project(s) failed"
                )

            self._emit_event(
                CycleEvent(
                    event_type="cycle_end",
                    step=step.value,
                    agent_role="",
                    message=f"{status_icon} Cycle completed: {final_status}",
                    detail="\n".join(detail_parts),
                    is_error=final_status != "SUCCESS",
                )
            )

            try:
                for step_name, evidence in self._step_evidence.items():
                    self._report_generator.set_execution_evidence(step_name, evidence)
                report_path = self._report_generator.write_report(state)
                self._emit_event(
                    CycleEvent(
                        event_type="report_written",
                        step=step.value,
                        agent_role="",
                        message=f"📄 Report written: {report_path}",
                    )
                )
            except Exception as e:
                logger.warning(
                    f"Report generation failed for cycle {state.cycle_id}: {type(e).__name__}: {e}"
                )
                error_path = self._report_error_writer.write_error(
                    cycle_id=state.cycle_id,
                    error=e,
                    context="Report generation during cycle completion",
                )
                self._emit_event(
                    CycleEvent(
                        event_type="report_error",
                        step=step.value,
                        agent_role="",
                        message=f"⚠️ Report generation failed: {type(e).__name__}",
                        detail=f"Error artifact: {error_path}" if error_path else str(e),
                        is_error=True,
                    )
                )

        self.cycle_store.save(state)
        return state

    def run_full_cycle(
        self,
        config: CycleConfig,
        mode: AssistantMode = AssistantMode.GUIDED_EXECUTION,
    ) -> CycleState:
        """Run a complete cycle from start to finish.

        Args:
            config: Cycle configuration.
            mode: Assistant mode.

        Returns:
            Final CycleState.
        """
        state = self.start_cycle(config, mode)
        while not state.is_finished():
            state = self.run_next_step(state)
        return state

    def resume_cycle(self, cycle_id: str) -> CycleState:
        """Resume an interrupted or paused cycle.

        Args:
            cycle_id: The cycle ID to resume.

        Returns:
            Current CycleState.
        """
        state = self.cycle_store.load(cycle_id)
        if state.interrupted:
            state.interrupted = False
            state.interrupt_reason = ""
        return state

    def interrupt_cycle(self, state: CycleState, reason: str = "User interrupted") -> CycleState:
        """Interrupt a running cycle.

        Args:
            state: Current cycle state.
            reason: Reason for interruption.

        Returns:
            Updated CycleState.
        """
        if self._termination_enforcer:
            self._termination_enforcer.interrupt()

        state.interrupted = True
        state.interrupt_reason = reason
        state.exit_status = CycleExitStatus.INTERRUPTED.value
        state.completed_at = datetime.now().isoformat()
        self.cycle_store.save(state)
        return state

    def _should_skip_step(self, step: CycleStep, state: CycleState) -> tuple[bool, str]:
        """Determine if a step should be skipped.

        Args:
            step: The step to check.
            state: Current cycle state.

        Returns:
            Tuple of (should_skip, reason).
        """
        # Learning mode: Skip ProjectAgent - use user-provided project_root
        if step == CycleStep.PROJECT_SELECTION:
            if state.config.is_learning_mode:
                return (
                    True,
                    f"Learning mode: using user-provided project '{state.config.project_root}'",
                )

        if step == CycleStep.IMPROVEMENT_PROPOSAL:
            observer_log = self._get_step_log(state, CycleStep.OBSERVATION)
            if not observer_log or not observer_log.success:
                return True, "No valid observation to improve upon"

            if state.improvements_approved >= state.config.max_improvements:
                return True, f"Max improvements ({state.config.max_improvements}) reached"

        if step == CycleStep.REVIEW:
            improvement_log = self._get_step_log(state, CycleStep.IMPROVEMENT_PROPOSAL)
            if not improvement_log or not improvement_log.success or improvement_log.skipped:
                return True, "No improvement proposal to review"

        if step == CycleStep.REGRESSION_CHECKS:
            if not state.config.golden_projects:
                return True, "No golden projects configured for regression checks"

        return False, ""

    def _build_context(self, step: CycleStep, state: CycleState) -> AgentContext:
        """Build the agent context for a step.

        Handles automatic data flow wiring between agents:
        - ImprovementAgent receives Observer output automatically
        - ReviewerAgent receives Improvement proposal automatically

        Args:
            step: The current step.
            state: Current cycle state.

        Returns:
            AgentContext for the agent.
        """
        query = self._build_step_query(step, state)

        metadata = {
            "execution_gateway": self.execution_gateway,
            "cycle_config": state.config,
            "previous_logs": [asdict(log) for log in state.logs],
            "is_learning_mode": state.config.is_learning_mode,
            "memory_scope": "learning" if state.config.is_learning_mode else "validated",
        }

        if step == CycleStep.OBSERVATION:
            user_exec_evidence = self._step_evidence.get(CycleStep.USER_EXECUTION.value)
            env_evidence = self._step_evidence.get(CycleStep.ENV_VALIDATION.value)
            if user_exec_evidence:
                metadata["execution_evidence"] = user_exec_evidence.to_dict()
            if env_evidence:
                metadata["env_validation_evidence"] = env_evidence.to_dict()

        if step == CycleStep.IMPROVEMENT_PROPOSAL:
            observer_log = self._get_step_log(state, CycleStep.OBSERVATION)
            if observer_log and observer_log.metadata:
                observer_output = observer_log.metadata.get("observer_output")
                if observer_output:
                    metadata["observer_output"] = observer_output

        if step == CycleStep.REVIEW:
            improvement_log = self._get_step_log(state, CycleStep.IMPROVEMENT_PROPOSAL)
            if improvement_log and improvement_log.metadata:
                proposal = improvement_log.metadata.get("proposal")
                validation_errors = improvement_log.metadata.get("validation_errors", [])
                if proposal:
                    metadata["proposal"] = proposal
                    metadata["validation_errors"] = validation_errors

        return AgentContext(
            query=query,
            odibi_root=self.odibi_root,
            assistant_mode=state.mode,
            cycle_id=state.cycle_id,
            cycle_step=step.value,
            metadata=metadata,
            event_callback=self._event_callback,
        )

    def _build_step_query(self, step: CycleStep, state: CycleState) -> str:
        """Build the query for a specific step.

        Args:
            step: The current step.
            state: Current cycle state.

        Returns:
            Query string for the agent.
        """
        config = state.config

        queries = {
            CycleStep.ENV_VALIDATION: (
                "Validate the execution environment:\n"
                "1. Check WSL (Ubuntu) is available\n"
                "2. Verify Java and Spark are installed\n"
                "3. Confirm Python virtual environment is active\n"
                "4. Validate Odibi is importable\n"
                "Report any issues found."
            ),
            CycleStep.PROJECT_SELECTION: (
                f"Select a project to work on from: {config.project_root}\n"
                f"Task: {config.task_description}\n"
                "Consider coverage gaps and curriculum progression.\n"
                "Output the selected project path and rationale."
            ),
            CycleStep.USER_EXECUTION: (
                "Execute Odibi pipelines as a user would:\n"
                "1. Read existing YAML configurations\n"
                "2. Run pipelines using the execution gateway\n"
                "3. Observe any errors or warnings\n"
                "Report execution results."
            ),
            CycleStep.OBSERVATION: (
                "Analyze the execution results:\n"
                "1. Classify any failures or pain points\n"
                "2. Identify patterns in errors\n"
                "3. Note UX friction points\n"
                "Output structured observations (no fixes, just observations)."
            ),
            CycleStep.IMPROVEMENT_PROPOSAL: (
                "Based on observations, propose improvements:\n"
                "1. Generate specific code changes\n"
                "2. Explain the rationale\n"
                "3. Estimate impact\n"
                "This proposal will be reviewed before approval."
            ),
            CycleStep.REVIEW: (
                "Review the proposed improvement:\n"
                "1. Is this change valuable?\n"
                "2. Is it safe?\n"
                "3. Does it follow Odibi patterns?\n"
                "Output: APPROVED or REJECTED with explanation."
            ),
            CycleStep.REGRESSION_CHECKS: (
                f"Run regression checks against golden projects: {config.golden_projects}\n"
                "1. Execute each golden project\n"
                "2. Compare results to expected outcomes\n"
                "3. Report any regressions detected."
            ),
            CycleStep.MEMORY_PERSISTENCE: (
                f"{'[LEARNING MODE] ' if config.is_learning_mode else ''}"
                "Persist learnings from this cycle:\n"
                "1. Validated improvements\n"
                "2. Patterns discovered\n"
                "3. Anti-patterns to avoid\n"
                + (
                    "NOTE: This is a learning-only cycle (max_improvements=0).\n"
                    "Memories are stored in LEARNING scope (non-validated, ephemeral).\n"
                    "These observations will NOT be merged into durable curriculum.\n"
                    if config.is_learning_mode
                    else "Store in appropriate memory categories."
                )
            ),
            CycleStep.SUMMARY: (
                "Generate a cycle summary:\n"
                "1. Projects attempted\n"
                "2. Improvements approved/rejected\n"
                "3. Regressions detected\n"
                "4. Convergence status\n"
                "This will be shown in the morning summary."
            ),
            CycleStep.EXIT: (
                "Cycle complete. Generate final status and recommendations for next cycle."
            ),
        }

        return queries.get(step, f"Execute step: {step.value}")

    def _post_process_step(
        self,
        step: CycleStep,
        state: CycleState,
        response: Optional[AgentResponse],
    ) -> None:
        """Post-process a completed step.

        ENFORCEMENT: Review gating is mechanically enforced here.
        EVIDENCE (Phase 6): Captures and persists execution evidence.

        Args:
            step: The completed step.
            state: Current cycle state.
            response: Agent response (may be None on error).
        """
        if not response:
            return

        content = response.content.upper() if response.content else ""

        if step in (CycleStep.ENV_VALIDATION, CycleStep.USER_EXECUTION):
            evidence = response.metadata.get("execution_evidence")
            if evidence and isinstance(evidence, ExecutionEvidence):
                self._step_evidence[step.value] = evidence
                try:
                    artifacts = self._artifact_writer.write_execution_artifacts(
                        cycle_id=state.cycle_id,
                        step_name=step.value,
                        evidence=evidence,
                    )
                    evidence.artifacts = artifacts
                except Exception:
                    pass

        # Phase 9.F: Observation Integrity Check
        # Validate that observer reported issues for any failed pipelines
        if step == CycleStep.OBSERVATION:
            user_exec_evidence = self._step_evidence.get(CycleStep.USER_EXECUTION.value)
            if user_exec_evidence and response.metadata:
                try:
                    # Convert ExecutionEvidence to dict for validation
                    evidence_dict = user_exec_evidence.to_dict()

                    # Validate observer output integrity
                    integrity_check = validate_observer_output_integrity(
                        observer_metadata=response.metadata,
                        execution_evidence=evidence_dict,
                        fail_fast=False,  # Log warning, don't crash cycle
                    )

                    if not integrity_check.is_valid:
                        # Log the integrity violation
                        self._emit_event(
                            CycleEvent(
                                event_type="integrity_violation",
                                step=step.value,
                                agent_role="observer",
                                message=(
                                    f"⚠️ Observer-Convergence Inconsistency: "
                                    f"{integrity_check.pipeline_failure_count} pipeline(s) failed, "
                                    f"but observer reported {integrity_check.observer_issue_count} issue(s)"
                                ),
                                detail=integrity_check.error_message or "",
                                is_error=True,
                            )
                        )
                        logger.warning(
                            "Phase 9.F integrity violation: %s",
                            integrity_check.error_message,
                        )
                except ObservationIntegrityError as e:
                    # If fail_fast was True (which it's not by default),
                    # this would interrupt the cycle
                    state.interrupted = True
                    state.interrupt_reason = f"Observation integrity violation: {e}"
                    state.exit_status = CycleExitStatus.ERROR.value
                    self._emit_event(
                        CycleEvent(
                            event_type="error",
                            step=step.value,
                            agent_role="observer",
                            message=f"❌ Observation integrity check failed: {e}",
                            is_error=True,
                        )
                    )
                except Exception as e:
                    # Don't fail the cycle on validation errors, just log
                    logger.warning("Phase 9.F integrity check failed with error: %s", e)

        if step == CycleStep.IMPROVEMENT_PROPOSAL and self._review_gate:
            proposal_hash = hashlib.sha256(response.content.encode()).hexdigest()[:16]
            self._review_gate.register_proposal(response.content)
            state.pending_proposal_hash = proposal_hash
            state.review_approved = False

        if step == CycleStep.REVIEW:
            if self._review_gate and state.pending_proposal_hash:
                try:
                    decision = ReviewDecision.parse_from_response(
                        response.content, state.pending_proposal_hash
                    )
                    self._review_gate.register_decision(decision)
                    state.review_approved = decision.approved

                    if decision.approved:
                        state.improvements_approved += 1
                    else:
                        state.improvements_rejected += 1
                except ReviewGatingError:
                    state.improvements_rejected += 1
                    state.review_approved = False
            else:
                if "APPROVED" in content:
                    state.improvements_approved += 1
                elif "REJECTED" in content:
                    state.improvements_rejected += 1

        if step == CycleStep.REGRESSION_CHECKS:
            if self._review_gate and state.pending_proposal_hash:
                if not self._review_gate.is_approved:
                    state.pending_proposal_hash = ""
                    state.review_approved = False
                self._review_gate.clear()

            metadata = response.metadata or {}
            regressions = metadata.get("regressions_detected", 0)
            if regressions > 0:
                state.regressions_detected = regressions
            elif "regressions_detected" not in metadata:
                if (
                    "REGRESSIONS_DETECTED" in content.upper()
                    and "NO REGRESSIONS" not in content.upper()
                ):
                    state.regressions_detected += 1

            state.golden_project_results = metadata.get("results", [])

            results = metadata.get("results", [])
            if results:
                passed = [r for r in results if r.get("status") == "PASSED"]
                failed = [r for r in results if r.get("status") in ("FAILED", "ERROR")]
                summary_parts = []
                for r in results:
                    status_icon = "✅" if r.get("status") == "PASSED" else "❌"
                    summary_parts.append(
                        f"{status_icon} {r.get('name', 'unknown')}: {r.get('status')}"
                    )
                summary_msg = "\n".join(summary_parts)

                if failed:
                    self._emit_event(
                        CycleEvent(
                            event_type="warning",
                            step=step.value,
                            agent_role="regression_guard",
                            message=f"🚨 REGRESSION DETECTED: {len(failed)} golden project(s) failed",
                            detail=summary_msg,
                            is_error=True,
                        )
                    )
                else:
                    self._emit_event(
                        CycleEvent(
                            event_type="step_end",
                            step=step.value,
                            agent_role="regression_guard",
                            message=f"✅ All {len(passed)} golden project(s) passed",
                            detail=summary_msg,
                        )
                    )

        if step == CycleStep.SUMMARY:
            state.summary = response.content
            if "CONVERGENCE" in content and "REACHED" in content:
                state.convergence_reached = True

    def _get_step_log(self, state: CycleState, step: CycleStep) -> Optional[CycleStepLog]:
        """Get the log for a specific step.

        Args:
            state: Current cycle state.
            step: The step to find.

        Returns:
            CycleStepLog or None.
        """
        for log in state.logs:
            if log.step == step.value:
                return log
        return None

    def get_morning_summary(self, days: int = 1) -> dict[str, Any]:
        """Generate a morning summary of recent cycles.

        Args:
            days: Number of days to include.

        Returns:
            Summary dictionary.
        """
        cycles = self.cycle_store.list_cycles(limit=50)
        cutoff = datetime.now().isoformat()[:10]

        recent = []
        for c in cycles:
            started = c.get("started_at", "")[:10]
            if started >= cutoff:
                recent.append(c)

        total_cycles = len(recent)
        completed = sum(1 for c in recent if c.get("completed"))
        interrupted = sum(1 for c in recent if c.get("interrupted"))

        summary = {
            "period": f"Last {days} day(s)",
            "cycles_completed": completed,
            "cycles_interrupted": interrupted,
            "total_cycles": total_cycles,
            "cycles": recent,
        }

        if recent:
            latest = self.cycle_store.load(recent[0]["cycle_id"])
            summary["latest_summary"] = latest.summary
            summary["improvements_approved"] = latest.improvements_approved
            summary["improvements_rejected"] = latest.improvements_rejected
            summary["regressions_detected"] = latest.regressions_detected
            summary["convergence_reached"] = latest.convergence_reached

        return summary

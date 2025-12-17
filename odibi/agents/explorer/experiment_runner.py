"""ExperimentRunner: Executes experiments in sandboxed environments.

TRUST BOUNDARY:
- Runs ONLY within sandbox clones (never on trusted codebase)
- All outputs are recorded, not acted upon
- Produces candidate diffs for human review
- Has NO authority to apply changes anywhere
"""

import hashlib
import logging
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

from odibi.agents.explorer.clone_manager import RepoCloneManager, SandboxInfo
from odibi.agents.explorer.diff_capture import (
    DiffCaptureError,
    DiffPathViolation,
    DiffResult,
    capture_diff,
    create_baseline_snapshot,
)
from odibi.agents.explorer.path_validation import (
    PathValidationError,
    validate_sandbox_path,
)

logger = logging.getLogger(__name__)


class ExperimentStatus(str, Enum):
    """Status of an experiment run."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    ABORTED = "aborted"


class ExperimentTimeoutError(Exception):
    """Raised when experiment exceeds wall-clock timeout.

    This is a hard failure, not a warning.
    """

    def __init__(self, experiment_id: str, timeout_seconds: int, command: str):
        self.experiment_id = experiment_id
        self.timeout_seconds = timeout_seconds
        self.command = command
        super().__init__(
            f"EXPERIMENT TIMEOUT: {experiment_id} exceeded {timeout_seconds}s "
            f"wall-clock limit while running: {command[:100]}"
        )


class SandboxNotFoundError(Exception):
    """Raised when sandbox is not found or not active."""

    def __init__(self, sandbox_id: str):
        self.sandbox_id = sandbox_id
        super().__init__(f"Sandbox not found or not active: {sandbox_id}")


class SandboxPathViolation(Exception):
    """Raised when execution path is not inside a valid sandbox."""

    def __init__(self, path: Path, reason: str):
        self.path = path
        self.reason = reason
        super().__init__(f"SANDBOX PATH VIOLATION: Cannot execute in {path}. {reason}")


@dataclass(frozen=True)
class ExperimentResult:
    """Immutable record of an experiment outcome.

    Attributes:
        experiment_id: Unique identifier for this experiment.
        sandbox_id: ID of the sandbox where experiment ran.
        status: Final status of the experiment.
        started_at: ISO timestamp when experiment started.
        ended_at: ISO timestamp when experiment ended.
        diff_path: Path to generated diff file (always set when capture_diff=True).
        diff_content: The captured diff content.
        diff_is_empty: Whether the diff represents no changes.
        diff_hash: Hash of diff content for repeatability verification.
        stdout: Captured standard output.
        stderr: Captured standard error.
        exit_code: Process exit code (if applicable).
        metadata: Additional experiment metadata.
    """

    experiment_id: str
    sandbox_id: str
    status: ExperimentStatus
    started_at: str
    ended_at: str
    diff_path: Optional[Path] = None
    diff_content: str = ""
    diff_is_empty: bool = True
    diff_hash: str = ""
    stdout: str = ""
    stderr: str = ""
    exit_code: Optional[int] = None
    metadata: tuple = field(default_factory=tuple)

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "experiment_id": self.experiment_id,
            "sandbox_id": self.sandbox_id,
            "status": self.status.value,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "diff_path": str(self.diff_path) if self.diff_path else None,
            "diff_content": self.diff_content,
            "diff_is_empty": self.diff_is_empty,
            "diff_hash": self.diff_hash,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "exit_code": self.exit_code,
            "metadata": dict(self.metadata),
        }


@dataclass
class ExperimentSpec:
    """Specification for an experiment to run.

    Attributes:
        name: Human-readable experiment name.
        description: What this experiment attempts to do.
        commands: List of shell commands to execute (in order).
        timeout_seconds: Maximum time to allow for experiment.
        capture_diff: Whether to capture diff after commands.
    """

    name: str
    description: str
    commands: list[str] = field(default_factory=list)
    timeout_seconds: int = 300
    capture_diff: bool = True


@dataclass
class CommandResult:
    """Result of a single command execution."""

    command: str
    stdout: str
    stderr: str
    exit_code: int
    timed_out: bool = False


class ExperimentRunner:
    """Runs experiments in sandboxed repository clones.

    This component:
    - Executes experiment specs within sandbox boundaries
    - Captures all outputs (stdout, stderr, diffs)
    - Never applies changes to trusted codebase
    - Records everything for human review

    Attributes:
        clone_manager: RepoCloneManager for sandbox operations.
    """

    def __init__(self, clone_manager: RepoCloneManager) -> None:
        """Initialize the experiment runner.

        Args:
            clone_manager: Manager for creating/accessing sandboxes.
        """
        self._clone_manager = clone_manager
        self._results: list[ExperimentResult] = []
        self._baseline_snapshots: dict[str, dict[str, str]] = {}

    @property
    def clone_manager(self) -> RepoCloneManager:
        """Read-only access to clone manager."""
        return self._clone_manager

    def create_baseline(self, sandbox_id: str) -> None:
        """Create a baseline snapshot for filesystem diff.

        Call this before running experiments if sandbox is not a git repo.

        Args:
            sandbox_id: ID of the sandbox to snapshot.

        Raises:
            SandboxNotFoundError: If sandbox doesn't exist.
        """
        sandbox_info = self._clone_manager.get_sandbox(sandbox_id)
        if sandbox_info is None:
            raise SandboxNotFoundError(sandbox_id)

        snapshot = create_baseline_snapshot(sandbox_info.sandbox_path)
        self._baseline_snapshots[sandbox_id] = snapshot
        logger.info(f"Created baseline snapshot for {sandbox_id}: {len(snapshot)} files")

    def run_experiment(
        self,
        spec: ExperimentSpec,
        sandbox_id: str,
    ) -> ExperimentResult:
        """Execute an experiment in a sandbox.

        Args:
            spec: ExperimentSpec describing what to run.
            sandbox_id: ID of sandbox to run in.

        Returns:
            ExperimentResult with outcome details including diff.

        Raises:
            SandboxNotFoundError: If sandbox_id is invalid or inactive.
            SandboxPathViolation: If sandbox path fails validation.
            ExperimentTimeoutError: If any command exceeds timeout.
        """
        sandbox_info = self._clone_manager.get_sandbox(sandbox_id)
        if sandbox_info is None:
            raise SandboxNotFoundError(sandbox_id)

        self._validate_sandbox_for_execution(sandbox_info)

        experiment_id = self._generate_experiment_id(spec, sandbox_id)
        started_at = datetime.now().isoformat()

        all_stdout: list[str] = []
        all_stderr: list[str] = []
        final_exit_code: Optional[int] = None
        status = ExperimentStatus.COMPLETED

        try:
            for command in spec.commands:
                result = self._run_command(
                    command=command,
                    working_dir=sandbox_info.sandbox_path,
                    timeout_seconds=spec.timeout_seconds,
                    experiment_id=experiment_id,
                )

                all_stdout.append(result.stdout)
                all_stderr.append(result.stderr)
                final_exit_code = result.exit_code

                if result.timed_out:
                    status = ExperimentStatus.TIMEOUT
                    raise ExperimentTimeoutError(
                        experiment_id=experiment_id,
                        timeout_seconds=spec.timeout_seconds,
                        command=command,
                    )

                if result.exit_code != 0:
                    status = ExperimentStatus.FAILED
                    break

        except ExperimentTimeoutError:
            raise
        except Exception as e:
            logger.error(f"Experiment {experiment_id} failed: {e}")
            status = ExperimentStatus.FAILED
            all_stderr.append(f"Execution error: {e}")

        diff_path: Optional[Path] = None
        diff_content: str = ""
        diff_is_empty: bool = True
        diff_hash: str = ""

        if spec.capture_diff:
            try:
                diff_result = self._capture_experiment_diff(
                    sandbox_info=sandbox_info,
                    experiment_id=experiment_id,
                )
                diff_path = diff_result.diff_path
                diff_content = diff_result.diff_content
                diff_is_empty = diff_result.is_empty
                diff_hash = diff_result.diff_hash
            except (DiffCaptureError, DiffPathViolation) as e:
                logger.warning(f"Diff capture failed for {experiment_id}: {e}")
                all_stderr.append(f"Diff capture error: {e}")

        ended_at = datetime.now().isoformat()

        experiment_result = ExperimentResult(
            experiment_id=experiment_id,
            sandbox_id=sandbox_id,
            status=status,
            started_at=started_at,
            ended_at=ended_at,
            diff_path=diff_path,
            diff_content=diff_content,
            diff_is_empty=diff_is_empty,
            diff_hash=diff_hash,
            stdout="\n".join(all_stdout),
            stderr="\n".join(all_stderr),
            exit_code=final_exit_code,
            metadata=tuple([("spec_name", spec.name), ("spec_description", spec.description)]),
        )

        self._results.append(experiment_result)
        logger.info(
            f"Experiment {experiment_id} completed with status={status.value}, "
            f"exit_code={final_exit_code}, diff_is_empty={diff_is_empty}"
        )

        return experiment_result

    def _capture_experiment_diff(
        self,
        sandbox_info: SandboxInfo,
        experiment_id: str,
    ) -> DiffResult:
        """Capture diff for an experiment.

        Args:
            sandbox_info: Sandbox where experiment ran.
            experiment_id: ID of the experiment.

        Returns:
            DiffResult with captured diff.
        """
        baseline = self._baseline_snapshots.get(sandbox_info.sandbox_id)

        return capture_diff(
            sandbox_path=sandbox_info.sandbox_path,
            sandbox_root=self._clone_manager.sandbox_root,
            trusted_repo_root=self._clone_manager.trusted_repo_root,
            experiment_id=experiment_id,
            baseline_snapshot=baseline,
        )

    def _validate_sandbox_for_execution(self, sandbox_info: SandboxInfo) -> None:
        """Validate that sandbox is safe for execution.

        Args:
            sandbox_info: Sandbox to validate.

        Raises:
            SandboxPathViolation: If sandbox path fails validation.
        """
        sandbox_path = sandbox_info.sandbox_path

        if not sandbox_path.exists():
            raise SandboxPathViolation(
                sandbox_path,
                "Sandbox directory does not exist",
            )

        if not sandbox_path.is_dir():
            raise SandboxPathViolation(
                sandbox_path,
                "Sandbox path is not a directory",
            )

        try:
            validate_sandbox_path(
                sandbox_path,
                self._clone_manager.sandbox_root,
                self._clone_manager.trusted_repo_root,
            )
        except PathValidationError as e:
            raise SandboxPathViolation(sandbox_path, str(e))

    def _run_command(
        self,
        command: str,
        working_dir: Path,
        timeout_seconds: int,
        experiment_id: str,
    ) -> CommandResult:
        """Run a single command in the sandbox.

        Args:
            command: Shell command to execute.
            working_dir: Directory to run command in.
            timeout_seconds: Maximum wall-clock time.
            experiment_id: ID for logging.

        Returns:
            CommandResult with captured output.
        """
        logger.debug(f"Running command in {working_dir}: {command}")

        try:
            process = subprocess.run(
                command,
                shell=True,
                cwd=str(working_dir),
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )

            return CommandResult(
                command=command,
                stdout=process.stdout,
                stderr=process.stderr,
                exit_code=process.returncode,
                timed_out=False,
            )

        except subprocess.TimeoutExpired as e:
            stdout_content = ""
            stderr_content = ""
            if e.stdout is not None:
                stdout_content = e.stdout if isinstance(e.stdout, str) else e.stdout.decode()
            if e.stderr is not None:
                stderr_content = e.stderr if isinstance(e.stderr, str) else e.stderr.decode()

            return CommandResult(
                command=command,
                stdout=stdout_content,
                stderr=stderr_content,
                exit_code=-1,
                timed_out=True,
            )

    def get_results(self) -> list[ExperimentResult]:
        """Get all experiment results from this runner.

        Returns:
            List of ExperimentResult (oldest first).
        """
        return list(self._results)

    def get_result(self, experiment_id: str) -> Optional[ExperimentResult]:
        """Get a specific experiment result by ID.

        Args:
            experiment_id: ID of the experiment to look up.

        Returns:
            ExperimentResult if found, None otherwise.
        """
        for result in self._results:
            if result.experiment_id == experiment_id:
                return result
        return None

    def extract_diff(self, sandbox_id: str) -> DiffResult:
        """Extract the current diff from a sandbox.

        This can be called at any time to capture the current state
        of changes in a sandbox, independent of any experiment.

        Args:
            sandbox_id: ID of the sandbox to get diff from.

        Returns:
            DiffResult with the captured diff.

        Raises:
            SandboxNotFoundError: If sandbox doesn't exist.
            DiffCaptureError: If diff capture fails.
        """
        sandbox_info = self._clone_manager.get_sandbox(sandbox_id)
        if sandbox_info is None:
            raise SandboxNotFoundError(sandbox_id)

        self._validate_sandbox_for_execution(sandbox_info)

        diff_id = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        baseline = self._baseline_snapshots.get(sandbox_id)

        return capture_diff(
            sandbox_path=sandbox_info.sandbox_path,
            sandbox_root=self._clone_manager.sandbox_root,
            trusted_repo_root=self._clone_manager.trusted_repo_root,
            experiment_id=diff_id,
            baseline_snapshot=baseline,
        )

    @staticmethod
    def _generate_experiment_id(spec: ExperimentSpec, sandbox_id: str) -> str:
        """Generate a unique experiment ID.

        Args:
            spec: Experiment specification.
            sandbox_id: Sandbox ID.

        Returns:
            Unique experiment ID string.
        """
        timestamp = datetime.now().isoformat()
        content = f"{spec.name}:{sandbox_id}:{timestamp}"
        hash_suffix = hashlib.sha256(content.encode()).hexdigest()[:12]
        return f"exp_{hash_suffix}"

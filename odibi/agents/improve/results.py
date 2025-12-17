"""Result dataclasses for improvement cycle tracking.

Provides structured data for capturing pipeline, test, lint, and validation
results to support promotion decision-making.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional


class ExecutionStatus(str, Enum):
    """Status of an execution operation."""

    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    TIMEOUT = "timeout"
    ERROR = "error"


@dataclass
class CommandResult:
    """Result from running a shell command."""

    command: str
    exit_code: int
    stdout: str
    stderr: str
    duration: float
    timed_out: bool = False

    @property
    def success(self) -> bool:
        """Check if command succeeded."""
        return self.exit_code == 0 and not self.timed_out


@dataclass
class TestResult:
    """Result from running pytest."""

    passed: int
    failed: int
    skipped: int
    errors: int
    duration: float
    exit_code: int
    output: str
    failed_tests: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """Check if all tests passed."""
        return self.failed == 0 and self.errors == 0 and self.exit_code == 0

    @property
    def total(self) -> int:
        """Total tests run."""
        return self.passed + self.failed + self.skipped + self.errors


@dataclass
class LintResult:
    """Result from running ruff lint."""

    error_count: int
    warning_count: int
    fixable_count: int
    exit_code: int
    output: str
    duration: float
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """Check if lint passed (no errors)."""
        return self.error_count == 0 and self.exit_code == 0


@dataclass
class ValidationResult:
    """Result from running odibi validate."""

    valid: bool
    config_path: str
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    exit_code: int = 0
    output: str = ""
    duration: float = 0.0


@dataclass
class NodeResult:
    """Result for a single pipeline node execution."""

    node_name: str
    operation: str
    status: ExecutionStatus
    duration: float
    rows_in: Optional[int] = None
    rows_out: Optional[int] = None
    error_message: Optional[str] = None
    error_type: Optional[str] = None


@dataclass
class PipelineResult:
    """Result from running an odibi pipeline."""

    config_path: str
    pipeline_name: str
    status: ExecutionStatus
    duration: float
    total_nodes: int
    completed_nodes: int
    failed_nodes: int
    skipped_nodes: int
    nodes: list[NodeResult] = field(default_factory=list)
    story_path: Optional[Path] = None
    exit_code: int = 0
    output: str = ""
    error_message: Optional[str] = None

    @property
    def success(self) -> bool:
        """Check if pipeline succeeded."""
        return self.failed_nodes == 0 and self.status == ExecutionStatus.SUCCESS

    @property
    def success_rate(self) -> float:
        """Calculate node success rate."""
        if self.total_nodes == 0:
            return 0.0
        return (self.completed_nodes / self.total_nodes) * 100


@dataclass
class GoldenResult:
    """Result from running a golden/learning harness config."""

    config_name: str
    config_path: str
    pipeline_result: Optional[PipelineResult] = None
    passed: bool = False
    error_message: Optional[str] = None
    duration: float = 0.0


@dataclass
class GateCheckResult:
    """Result of checking all promotion gates."""

    tests_passed: bool
    lint_passed: bool
    validate_passed: bool
    golden_passed: bool
    all_passed: bool
    test_result: Optional[TestResult] = None
    lint_result: Optional[LintResult] = None
    validation_results: list[ValidationResult] = field(default_factory=list)
    golden_results: list[GoldenResult] = field(default_factory=list)
    failure_reasons: list[str] = field(default_factory=list)


@dataclass
class CycleResult:
    """Complete results from one improvement cycle.

    Captures everything that happened during an improvement cycle,
    used for decision-making and memory recording.
    """

    cycle_id: str
    sandbox_path: Path
    started_at: datetime
    completed_at: Optional[datetime] = None

    # Pipeline results
    pipelines_run: int = 0
    pipelines_passed: int = 0
    pipelines_failed: int = 0
    pipeline_results: list[PipelineResult] = field(default_factory=list)

    # Test results
    tests_passed: int = 0
    tests_failed: int = 0
    test_result: Optional[TestResult] = None

    # Lint results
    lint_errors: int = 0
    lint_result: Optional[LintResult] = None

    # Golden project results
    golden_passed: int = 0
    golden_failed: int = 0
    golden_results: list[GoldenResult] = field(default_factory=list)

    # Gate check
    gate_result: Optional[GateCheckResult] = None

    # Changes made
    files_modified: list[str] = field(default_factory=list)
    diff: str = ""

    # Decision
    promoted: bool = False
    rejection_reason: Optional[str] = None

    # Learning
    learning: bool = False
    lesson: Optional[str] = None

    @property
    def duration(self) -> float:
        """Calculate cycle duration in seconds."""
        if self.completed_at is None:
            return (datetime.now() - self.started_at).total_seconds()
        return (self.completed_at - self.started_at).total_seconds()

    @property
    def all_gates_passed(self) -> bool:
        """Check if all gates passed."""
        if self.gate_result:
            return self.gate_result.all_passed
        return False

    def mark_complete(
        self,
        promoted: bool = False,
        rejection_reason: Optional[str] = None,
        learning: bool = False,
        lesson: Optional[str] = None,
    ) -> None:
        """Mark cycle as complete with final status."""
        self.completed_at = datetime.now()
        self.promoted = promoted
        self.rejection_reason = rejection_reason
        self.learning = learning
        self.lesson = lesson

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "cycle_id": self.cycle_id,
            "sandbox_path": str(self.sandbox_path),
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration": self.duration,
            "pipelines_run": self.pipelines_run,
            "pipelines_passed": self.pipelines_passed,
            "pipelines_failed": self.pipelines_failed,
            "tests_passed": self.tests_passed,
            "tests_failed": self.tests_failed,
            "lint_errors": self.lint_errors,
            "golden_passed": self.golden_passed,
            "golden_failed": self.golden_failed,
            "files_modified": self.files_modified,
            "promoted": self.promoted,
            "rejection_reason": self.rejection_reason,
            "learning": self.learning,
            "lesson": self.lesson,
        }

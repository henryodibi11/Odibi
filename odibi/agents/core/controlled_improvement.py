"""Controlled Improvement Activation - Phase 9.G.

This module provides controlled, single-shot improvement capabilities that:
- Allow exactly ONE improvement per cycle
- Scope improvements to specific files only
- Enforce minimal, deterministic diffs
- Provide before/after snapshots for rollback
- Validate that unrelated pipelines remain stable

PHASE 9.G INVARIANTS:
- max_improvements = 1 (single-shot)
- Only allowed_files can be modified
- All other pipelines must remain unchanged
- Rollback on any regression or validation failure
- Deterministic: same inputs → identical diff

This phase proves the system can fix ONE real problem safely
before scaling to multi-file improvements.
"""

import hashlib
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from .autonomous_learning import (
    GuardedCycleRunner,
    LearningCycleConfig,
)
from .cycle import (
    AgentContext,
    CycleConfig,
    CycleState,
    CycleStep,
    GoldenProjectConfig,
)
from .schemas import ImprovementProposal

logger = logging.getLogger(__name__)


class ImprovementScopeViolation(Exception):
    """Raised when an improvement attempts to modify out-of-scope files."""

    def __init__(self, violation: str, attempted_files: list[str], allowed_files: list[str]):
        self.violation = violation
        self.attempted_files = attempted_files
        self.allowed_files = allowed_files
        super().__init__(
            f"IMPROVEMENT SCOPE VIOLATION: {violation}. "
            f"Attempted: {attempted_files}, Allowed: {allowed_files}"
        )


class LearningHarnessViolation(Exception):
    """Raised when attempting to use a learning harness file as an improvement target.

    Learning harness files under .odibi/learning_harness/ are system validation
    fixtures and cannot be modified by ImprovementAgent. They are:
    - Executable (for running stress scenarios)
    - Observable (for monitoring system behavior)
    - Usable for regression checks

    But NEVER modifiable as improvement targets.
    """

    def __init__(self, file_path: str):
        self.file_path = file_path
        super().__init__(
            f"LEARNING HARNESS VIOLATION: '{file_path}' is a system validation fixture "
            f"under .odibi/learning_harness/ and cannot be modified. "
            f"Learning harness files are for regression checks only, not improvement targets."
        )


# Path patterns that are protected from improvement (normalized to forward slashes)
PROTECTED_HARNESS_PATTERNS = [
    ".odibi/learning_harness/",
    "odibi/learning_harness/",  # Also catch without the dot prefix
]


def is_learning_harness_path(file_path: str) -> bool:
    """Check if a file path is within the protected learning harness directory.

    Args:
        file_path: The file path to check (absolute or relative).

    Returns:
        True if the path is inside .odibi/learning_harness/ or similar, False otherwise.
    """
    normalized = str(file_path).lower().replace("\\", "/")
    return any(pattern in normalized for pattern in PROTECTED_HARNESS_PATTERNS)


class ImprovementRejectionReason(str, Enum):
    """Reasons for rejecting an improvement."""

    SCOPE_VIOLATION = "SCOPE_VIOLATION"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    REGRESSION_DETECTED = "REGRESSION_DETECTED"
    NON_MINIMAL_DIFF = "NON_MINIMAL_DIFF"
    NON_DETERMINISTIC = "NON_DETERMINISTIC"
    UNRELATED_CHANGES = "UNRELATED_CHANGES"


@dataclass
class ImprovementScope:
    """Defines the scope of allowed improvements.

    This enforces that improvements can ONLY modify specific files.
    Any attempt to modify files outside this scope will be rejected.
    """

    allowed_files: list[str]
    """List of file paths (absolute or relative to project) that CAN be modified."""

    allowed_fields: list[str] = field(default_factory=list)
    """Optional: Specific YAML fields that can be modified (e.g., 'pipelines.0.nodes.2.params')."""

    max_changes_per_file: int = 1
    """Maximum number of discrete changes per file."""

    max_lines_changed: int = 10
    """Maximum number of lines that can change across all files."""

    description: str = ""
    """Human-readable description of why this scope exists."""

    def __post_init__(self):
        self._validate_no_harness_files()
        self._normalized_files = [self._normalize_path(f) for f in self.allowed_files]

    def _validate_no_harness_files(self) -> None:
        """Validate that no allowed files are in the learning harness directory.

        Raises:
            LearningHarnessViolation: If any file is in .odibi/learning_harness/
        """
        for file_path in self.allowed_files:
            if is_learning_harness_path(file_path):
                raise LearningHarnessViolation(file_path)

    def _normalize_path(self, path: str) -> str:
        """Normalize a path for comparison."""
        return str(Path(path)).lower().replace("\\", "/")

    def _get_filename(self, path: str) -> str:
        """Get just the filename from a path."""
        return Path(path).name.lower()

    def is_file_allowed(self, file_path: str) -> bool:
        """Check if a file is within the allowed scope.

        Matches if:
        - Exact path match (after normalization)
        - Filename matches an allowed filename
        - Allowed path is contained in the file path
        """
        normalized = self._normalize_path(file_path)
        file_name = self._get_filename(file_path)

        for allowed in self.allowed_files:
            allowed_normalized = self._normalize_path(allowed)
            allowed_filename = self._get_filename(allowed)

            if normalized == allowed_normalized:
                return True
            if normalized.endswith("/" + allowed_normalized):
                return True
            if file_name == allowed_filename:
                return True
            if allowed_normalized in normalized:
                return True

        return False

    def validate_proposal(self, proposal: ImprovementProposal) -> tuple[bool, list[str]]:
        """Validate that a proposal is within scope.

        Args:
            proposal: The improvement proposal to validate.

        Returns:
            Tuple of (is_valid, list of violations).
        """
        violations = []

        for change in proposal.changes:
            if not self.is_file_allowed(change.file):
                violations.append(f"File '{change.file}' is not in allowed scope")

        if len(proposal.changes) > len(self.allowed_files) * self.max_changes_per_file:
            violations.append(
                f"Too many changes: {len(proposal.changes)} "
                f"(max {len(self.allowed_files) * self.max_changes_per_file})"
            )

        total_lines = sum(
            len(change.after.splitlines()) - len(change.before.splitlines())
            for change in proposal.changes
        )
        if abs(total_lines) > self.max_lines_changed:
            violations.append(
                f"Too many lines changed: {abs(total_lines)} (max {self.max_lines_changed})"
            )

        return len(violations) == 0, violations

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed_files": self.allowed_files,
            "allowed_fields": self.allowed_fields,
            "max_changes_per_file": self.max_changes_per_file,
            "max_lines_changed": self.max_lines_changed,
            "description": self.description,
        }

    @classmethod
    def for_single_file(cls, file_path: str, description: str = "") -> "ImprovementScope":
        """Create a scope for a single file."""
        return cls(
            allowed_files=[file_path],
            max_changes_per_file=1,
            max_lines_changed=10,
            description=description or f"Single-file improvement: {file_path}",
        )


@dataclass
class ImprovementSnapshot:
    """Snapshot of file contents before improvement for rollback.

    Captures the exact state of all files in scope before any changes,
    enabling deterministic rollback if the improvement fails validation.
    """

    snapshot_id: str
    created_at: str
    files: dict[str, str]  # file_path -> content
    file_hashes: dict[str, str]  # file_path -> SHA256 hash

    def __post_init__(self):
        if not self.file_hashes:
            self.file_hashes = {
                path: self._hash_content(content) for path, content in self.files.items()
            }

    @staticmethod
    def _hash_content(content: str) -> str:
        return hashlib.sha256(content.encode()).hexdigest()

    @classmethod
    def capture(cls, file_paths: list[str]) -> "ImprovementSnapshot":
        """Capture a snapshot of the given files.

        Args:
            file_paths: List of file paths to snapshot.

        Returns:
            ImprovementSnapshot with file contents and hashes.

        Raises:
            FileNotFoundError: If any file does not exist.
        """
        snapshot_id = hashlib.sha256(
            f"{datetime.now().isoformat()}:{','.join(file_paths)}".encode()
        ).hexdigest()[:12]

        files = {}
        file_hashes = {}

        for path in file_paths:
            abs_path = str(Path(path).resolve())
            if os.path.exists(abs_path):
                with open(abs_path, "r", encoding="utf-8") as f:
                    content = f.read()
                files[abs_path] = content
                file_hashes[abs_path] = cls._hash_content(content)
            else:
                logger.warning(f"File not found for snapshot: {abs_path}")

        return cls(
            snapshot_id=snapshot_id,
            created_at=datetime.now().isoformat(),
            files=files,
            file_hashes=file_hashes,
        )

    def restore(self) -> list[str]:
        """Restore all files to their snapshot state.

        Returns:
            List of files that were restored.
        """
        restored = []
        for path, content in self.files.items():
            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(content)
                restored.append(path)
                logger.info(f"Restored file: {path}")
            except Exception as e:
                logger.error(f"Failed to restore {path}: {e}")
        return restored

    def verify_unchanged(self) -> tuple[bool, list[str]]:
        """Verify that files haven't changed since snapshot.

        Returns:
            Tuple of (all_unchanged, list of changed files).
        """
        changed = []
        for path, original_hash in self.file_hashes.items():
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    current_hash = self._hash_content(f.read())
                if current_hash != original_hash:
                    changed.append(path)
            else:
                changed.append(path)  # File was deleted
        return len(changed) == 0, changed

    def get_diff_for_file(self, file_path: str) -> Optional[dict[str, str]]:
        """Get before/after content for a specific file.

        Args:
            file_path: Path to the file.

        Returns:
            Dict with 'before' and 'after' content, or None if file not in snapshot.
        """
        abs_path = str(Path(file_path).resolve())
        if abs_path not in self.files:
            return None

        before = self.files[abs_path]
        after = ""
        if os.path.exists(abs_path):
            with open(abs_path, "r", encoding="utf-8") as f:
                after = f.read()

        return {"before": before, "after": after}

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "created_at": self.created_at,
            "file_count": len(self.files),
            "file_hashes": self.file_hashes,
        }


@dataclass
class ImprovementResult:
    """Result of a controlled improvement attempt."""

    improvement_id: str
    proposal: Optional[ImprovementProposal]
    scope: ImprovementScope
    snapshot: ImprovementSnapshot

    status: str  # "APPLIED" | "REJECTED" | "ROLLED_BACK" | "NO_PROPOSAL"
    rejection_reason: Optional[ImprovementRejectionReason] = None
    rejection_details: str = ""

    files_modified: list[str] = field(default_factory=list)
    files_restored: list[str] = field(default_factory=list)

    validation_passed: bool = False
    regression_check_passed: bool = False
    unrelated_pipelines_stable: bool = False

    before_after_diff: dict[str, dict[str, str]] = field(default_factory=dict)
    diff_hash: str = ""

    created_at: str = ""
    completed_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()

    def mark_completed(self):
        self.completed_at = datetime.now().isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "improvement_id": self.improvement_id,
            "proposal": self.proposal.to_dict() if self.proposal else None,
            "scope": self.scope.to_dict(),
            "snapshot_id": self.snapshot.snapshot_id,
            "status": self.status,
            "rejection_reason": self.rejection_reason.value if self.rejection_reason else None,
            "rejection_details": self.rejection_details,
            "files_modified": self.files_modified,
            "files_restored": self.files_restored,
            "validation_passed": self.validation_passed,
            "regression_check_passed": self.regression_check_passed,
            "unrelated_pipelines_stable": self.unrelated_pipelines_stable,
            "diff_hash": self.diff_hash,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
        }


@dataclass
class ControlledImprovementConfig(LearningCycleConfig):
    """Configuration for controlled improvement cycles.

    Extends LearningCycleConfig with improvement-specific settings.
    """

    improvement_scope: Optional[ImprovementScope] = None
    """Scope defining which files can be improved."""

    golden_projects: list[GoldenProjectConfig] = field(default_factory=list)
    """Golden projects for regression testing after improvement."""

    require_validation_pass: bool = True
    """Whether to require validation to pass after improvement."""

    require_regression_check: bool = True
    """Whether to require regression checks to pass."""

    rollback_on_failure: bool = True
    """Whether to automatically rollback on any failure."""

    def __post_init__(self):
        self.max_improvements = 1
        self.mode = "improvement"

    def validate(self) -> list[str]:
        """Validate the controlled improvement config.

        Returns:
            List of validation errors (empty if valid).
        """
        errors = []

        if self.max_improvements != 1:
            errors.append(
                f"max_improvements MUST be 1 for controlled improvement, got {self.max_improvements}"
            )

        if self.improvement_scope is None:
            errors.append("improvement_scope MUST be defined for controlled improvement")

        if self.require_regression_check and not self.golden_projects:
            errors.append("golden_projects MUST be defined when require_regression_check=True")

        return errors

    def to_cycle_config(self) -> CycleConfig:
        """Convert to CycleConfig for cycle execution."""
        return CycleConfig(
            project_root=self.project_root,
            task_description=self.task_description,
            max_improvements=1,
            max_runtime_hours=self.max_runtime_hours,
            gated_improvements=True,
            stop_on_convergence=True,
            golden_projects=self.golden_projects,
        )


class ImprovementScopeEnforcer:
    """Enforces improvement scope at runtime.

    This is the main enforcement mechanism that ensures improvements
    stay within their defined scope.
    """

    def __init__(self, scope: ImprovementScope):
        self._scope = scope
        self._attempted_files: list[str] = []
        self._violations: list[str] = []

    def check_file_access(self, file_path: str, operation: str = "write") -> bool:
        """Check if a file access is within scope.

        Args:
            file_path: Path to the file being accessed.
            operation: Type of operation (read/write).

        Returns:
            True if access is allowed, False otherwise.
        """
        if operation != "write":
            return True  # Read operations are always allowed

        self._attempted_files.append(file_path)

        if not self._scope.is_file_allowed(file_path):
            self._violations.append(f"Write to '{file_path}' is out of scope")
            return False

        return True

    def validate_proposal(self, proposal: ImprovementProposal) -> tuple[bool, list[str]]:
        """Validate a proposal against the scope."""
        return self._scope.validate_proposal(proposal)

    def get_violations(self) -> list[str]:
        """Get all recorded violations."""
        return self._violations.copy()

    def has_violations(self) -> bool:
        """Check if any violations have occurred."""
        return len(self._violations) > 0

    def assert_no_violations(self) -> None:
        """Assert that no scope violations have occurred.

        Raises:
            ImprovementScopeViolation: If any violations exist.
        """
        if self._violations:
            raise ImprovementScopeViolation(
                "Scope violations detected",
                self._attempted_files,
                self._scope.allowed_files,
            )


class ControlledImprovementRunner(GuardedCycleRunner):
    """CycleRunner for controlled single-shot improvements.

    Extends GuardedCycleRunner to:
    - Enable exactly one improvement
    - Enforce file scope
    - Capture before/after snapshots
    - Validate and rollback on failure

    IMPORTANT: This runner DISABLES the learning mode guards from GuardedCycleRunner
    because we explicitly want to allow the ImprovementAgent to run (once).
    """

    def __init__(
        self,
        odibi_root: str,
        memory_manager: Optional[Any] = None,
        azure_config: Optional[Any] = None,
    ):
        super().__init__(odibi_root, memory_manager, azure_config)
        self._config: Optional[ControlledImprovementConfig] = None
        self._scope_enforcer: Optional[ImprovementScopeEnforcer] = None
        self._snapshot: Optional[ImprovementSnapshot] = None
        self._improvement_result: Optional[ImprovementResult] = None
        self._improvement_applied: bool = False
        self._improvement_mode_enabled: bool = True

    def _should_skip_step(self, step: CycleStep, state: CycleState) -> tuple[bool, str]:
        """Override to allow improvement step with controlled scope.

        Args:
            step: The step to check.
            state: Current cycle state.

        Returns:
            Tuple of (should_skip, reason).
        """
        if step == CycleStep.IMPROVEMENT_PROPOSAL:
            if self._improvement_applied:
                return True, "Improvement already applied this cycle (single-shot)"
            return False, ""

        if step == CycleStep.REVIEW:
            if not self._improvement_applied:
                return True, "No improvement to review"
            return False, ""

        # Skip the GuardedCycleRunner check (which blocks improvements)
        # and go directly to the base CycleRunner check
        from .cycle import CycleRunner

        return CycleRunner._should_skip_step(self, step, state)

    def run_next_step(self, state: CycleState) -> CycleState:
        """Run the next step WITHOUT learning-mode guards.

        This overrides GuardedCycleRunner.run_next_step() to allow
        the ImprovementAgent to execute (controlled improvement mode).
        After the IMPROVEMENT_PROPOSAL step, automatically applies the proposal.
        After REGRESSION_CHECKS, rollback if regressions detected and rollback_on_failure is set.

        Args:
            state: Current cycle state.

        Returns:
            Updated cycle state.
        """
        current_step = state.current_step()

        # Bypass GuardedCycleRunner and call CycleRunner directly
        from .cycle import CycleRunner

        state = CycleRunner.run_next_step(self, state)

        # After IMPROVEMENT_PROPOSAL step completes, apply the improvement
        if current_step == CycleStep.IMPROVEMENT_PROPOSAL and not self._improvement_applied:
            self._apply_proposal_from_state(state)

        # After REGRESSION_CHECKS step completes, check for regressions and rollback if needed
        if current_step == CycleStep.REGRESSION_CHECKS:
            self._handle_regression_check_result(state)

        return state

    def _handle_regression_check_result(self, state: CycleState) -> None:
        """Handle the result of regression checks, rolling back if needed.

        Args:
            state: Cycle state after regression checks completed.
        """
        if not self._improvement_result or self._improvement_result.status != "APPLIED":
            return

        if not self._config:
            return

        # Check if regressions were detected
        regressions_detected = state.regressions_detected > 0

        # Also check golden project results
        if state.golden_project_results:
            failures = [
                r for r in state.golden_project_results if r.get("status") in ("FAILED", "ERROR")
            ]
            if failures:
                regressions_detected = True

        if regressions_detected:
            logger.warning(
                f"Regression detected after improvement, regressions_detected={state.regressions_detected}"
            )
            self._improvement_result.regression_check_passed = False
            self._improvement_result.rejection_reason = (
                ImprovementRejectionReason.REGRESSION_DETECTED
            )
            self._improvement_result.rejection_details = (
                f"Regressions detected in {state.regressions_detected} golden project(s)"
            )

            if self._config.rollback_on_failure:
                logger.info("Rolling back improvement due to regression failure")
                self.rollback_improvement()
            else:
                self._improvement_result.status = "REJECTED"
                self._improvement_result.mark_completed()
        else:
            logger.info("Regression checks passed")
            self._improvement_result.regression_check_passed = True

    def _build_context(self, step: CycleStep, state: CycleState) -> AgentContext:
        """Override to inject target file content and issue description.

        For controlled improvement:
        - OBSERVATION step: Inject the issue description so the observer knows what to look for
        - IMPROVEMENT_PROPOSAL step: Inject target file content for accurate diffs

        Args:
            step: The current step.
            state: Current cycle state.

        Returns:
            AgentContext with controlled improvement context injected.
        """
        from .cycle import CycleRunner

        context = CycleRunner._build_context(self, step, state)

        logger.info(f"[ControlledImprovement] _build_context called for step={step.value}")
        logger.info(f"[ControlledImprovement] self._config={self._config is not None}")
        if self._config:
            logger.info(
                f"[ControlledImprovement] improvement_scope={self._config.improvement_scope is not None}"
            )

        # For OBSERVATION step: inject the issue description so the observer knows what to look for
        if step == CycleStep.OBSERVATION and self._config:
            task_desc = self._config.task_description
            if task_desc and task_desc != "Autonomous learning cycle":
                target_files_content = self._read_target_files()
                file_content_section = ""
                if target_files_content:
                    for fpath, content in target_files_content.items():
                        file_content_section += (
                            f"\n--- FILE: {fpath} ---\n{content}\n--- END FILE ---\n"
                        )

                context.query = f"""CONTROLLED IMPROVEMENT MODE - Issue Analysis

ISSUE TO ANALYZE:
{task_desc}

TARGET FILE(S):
{file_content_section if file_content_section else "See improvement_scope.allowed_files"}

YOUR TASK:
1. Analyze the provided issue description
2. Examine the target file(s) for the described problem
3. Output a structured ObserverOutput with the issue classified

This is a controlled improvement cycle. The issue has been pre-identified.
Your job is to observe and classify it for the ImprovementAgent.
"""
                context.metadata["controlled_improvement"] = True
                context.metadata["issue_description"] = task_desc
                logger.info("[ControlledImprovement] Injected issue description for OBSERVATION")

        # For IMPROVEMENT_PROPOSAL step: inject target file content for accurate diffs
        if (
            step == CycleStep.IMPROVEMENT_PROPOSAL
            and self._config
            and self._config.improvement_scope
        ):
            logger.info("[ControlledImprovement] Injecting file content for IMPROVEMENT_PROPOSAL")
            logger.info(
                f"[ControlledImprovement] allowed_files={self._config.improvement_scope.allowed_files}"
            )
            target_files_content = self._read_target_files()
            logger.info(
                f"[ControlledImprovement] Read {len(target_files_content)} files, sizes: {[(k, len(v)) for k, v in target_files_content.items()]}"
            )
            if target_files_content:
                context.metadata["target_files"] = target_files_content
                context.metadata["improvement_scope"] = {
                    "allowed_files": self._config.improvement_scope.allowed_files,
                    "description": self._config.improvement_scope.description,
                }
                context.query = self._build_improvement_query_with_file_content(
                    context.query, target_files_content
                )

        return context

    def _read_target_files(self) -> dict[str, str]:
        """Read the content of all target files in the improvement scope.

        Returns:
            Dict mapping file paths to their content.
        """
        if not self._config or not self._config.improvement_scope:
            return {}

        files_content = {}
        for file_path in self._config.improvement_scope.allowed_files:
            try:
                if os.path.exists(file_path):
                    with open(file_path, "r", encoding="utf-8") as f:
                        files_content[file_path] = f.read()
            except Exception as e:
                logger.warning(f"Could not read target file {file_path}: {e}")

        return files_content

    def _get_transformer_schemas(self, files_content: dict[str, str]) -> str:
        """Extract transformer types from YAML files and look up their schemas.

        Args:
            files_content: Dict of file paths to their content.

        Returns:
            Schema documentation string for transformers used in the files.
        """
        import re

        transformers_found = set()
        for content in files_content.values():
            matches = re.findall(r"transformer:\s*(\w+)", content)
            transformers_found.update(matches)

        if not transformers_found:
            return ""

        schema_docs = []
        transformer_schemas = {
            "join": """JoinParams (from odibi/transformers/relational.py):
  - right_dataset: str (REQUIRED) - Name of the node/dataset to join with
  - "on": Union[str, List[str]] (REQUIRED) - Column(s) to join on
    NOTE: "on" must be QUOTED in YAML because unquoted 'on:' is parsed as boolean True
  - how: "inner" | "left" | "right" | "full" | "cross" | "anti" | "semi" (default: "left")
  - prefix: Optional[str] - Prefix for columns from right dataset

Example:
  transformer: join
  params:
    right_dataset: customers
    "on": customer_id
    how: left""",
            "union": """UnionParams:
  - datasets: List[str] (REQUIRED) - Names of datasets to union
  - mode: "by_position" | "by_name" (default: "by_position")""",
            "deduplicate": """DeduplicateParams:
  - columns: List[str] (REQUIRED) - Columns to deduplicate on
  - keep: "first" | "last" | "none" (default: "first")""",
            "fill_nulls": """FillNullsParams:
  - values: Dict[str, Any] (REQUIRED) - Column name to fill value mapping""",
        }

        for transformer in transformers_found:
            if transformer in transformer_schemas:
                schema_docs.append(
                    f"### {transformer} transformer\n{transformer_schemas[transformer]}"
                )

        if schema_docs:
            return "\n\nTRANSFORMER SCHEMAS (use these exact field names):\n" + "\n\n".join(
                schema_docs
            )
        return ""

    def _build_improvement_query_with_file_content(
        self, base_query: str, files_content: dict[str, str]
    ) -> str:
        """Build an enhanced improvement query that includes actual file content.

        Args:
            base_query: The original query.
            files_content: Dict of file paths to their content.

        Returns:
            Enhanced query with file content included.
        """
        file_sections = []
        for file_path, content in files_content.items():
            file_sections.append(f"=== FILE: {file_path} ===\n```yaml\n{content}\n```")

        files_block = "\n\n".join(file_sections)
        schema_block = self._get_transformer_schemas(files_content)

        return f"""{base_query}

IMPORTANT: You are in CONTROLLED IMPROVEMENT mode. You may ONLY modify the files listed below.

TARGET FILES:
{files_block}
{schema_block}

CRITICAL INSTRUCTIONS for 'before' content:
1. Use SMALL, TARGETED snippets - only the lines you are changing plus 1-2 lines of context
2. Do NOT copy the entire file - just the specific section being modified
3. Copy the 'before' content EXACTLY, character-for-character, including all whitespace and comments
4. The 'before' MUST be a substring that exists in the file above

Generate a proposal with:
- proposal: "IMPROVEMENT" or "NO_IMPROVEMENT"
- title: Brief description of the fix
- rationale: Why this change fixes the observed issue
- changes: Array of {{file, before, after}} - use SMALL targeted snippets, not entire files
- impact: {{risk, expected_benefit}}

Example of a GOOD change (small, targeted):
{{"file": "example.yaml", "before": "  params:\\n    value: old", "after": "  params:\\n    value: new"}}

Example of a BAD change (too large):
{{"file": "example.yaml", "before": "<entire 50+ line file>", "after": "<entire file with one change>"}}
"""

    def _apply_proposal_from_state(self, state: CycleState) -> None:
        """Extract and apply the improvement proposal from cycle state.

        Args:
            state: Cycle state containing the improvement proposal.
        """
        improvement_log = None
        for log in state.logs:
            if log.step == CycleStep.IMPROVEMENT_PROPOSAL.value:
                improvement_log = log
                break

        if not improvement_log or not improvement_log.output_summary:
            logger.warning("No improvement proposal found in state")
            return

        try:
            logger.info(
                f"[ControlledImprovement] Parsing proposal from output_summary: {improvement_log.output_summary[:200]}..."
            )
            proposal = ImprovementProposal.parse_from_response(improvement_log.output_summary)
            logger.info(
                f"[ControlledImprovement] Parsed proposal: {proposal.proposal}, changes={len(proposal.changes)}"
            )
            if proposal.is_none():
                logger.info("Improvement proposal is NO_IMPROVEMENT, skipping apply")
                self._improvement_result = self._create_no_proposal_result(state)
                return

            result = self.apply_improvement(proposal, state)
            logger.info(f"Applied improvement: status={result.status}")
        except Exception as e:
            logger.error(f"Failed to parse/apply improvement proposal: {e}")
            self._improvement_result = self._create_error_result(state, str(e))

    def _create_no_proposal_result(self, state: CycleState) -> "ImprovementResult":
        """Create an ImprovementResult for NO_PROPOSAL case."""
        improvement_id = hashlib.sha256(
            f"{state.cycle_id}:no_proposal:{datetime.now().isoformat()}".encode()
        ).hexdigest()[:12]

        result = ImprovementResult(
            improvement_id=improvement_id,
            proposal=ImprovementProposal(proposal="NO_IMPROVEMENT"),
            scope=self._config.improvement_scope if self._config else None,
            snapshot=self._snapshot,
            status="NO_PROPOSAL",
        )
        result.mark_completed()
        return result

    def _create_error_result(self, state: CycleState, error: str) -> "ImprovementResult":
        """Create an ImprovementResult for error case."""
        improvement_id = hashlib.sha256(
            f"{state.cycle_id}:error:{datetime.now().isoformat()}".encode()
        ).hexdigest()[:12]

        result = ImprovementResult(
            improvement_id=improvement_id,
            proposal=ImprovementProposal(proposal="NO_IMPROVEMENT"),
            scope=self._config.improvement_scope if self._config else None,
            snapshot=self._snapshot,
            status="REJECTED",
            rejection_reason=ImprovementRejectionReason.VALIDATION_FAILED,
            rejection_details=error,
        )
        result.mark_completed()
        return result

    def start_controlled_improvement_cycle(
        self,
        config: ControlledImprovementConfig,
    ) -> CycleState:
        """Start a controlled improvement cycle.

        Args:
            config: Controlled improvement configuration.

        Returns:
            Initial CycleState.

        Raises:
            ValueError: If config is invalid.
        """
        errors = config.validate()
        if errors:
            raise ValueError(f"Invalid config: {'; '.join(errors)}")

        self._config = config
        self._improvement_applied = False
        self._improvement_result = None

        if config.improvement_scope:
            self._scope_enforcer = ImprovementScopeEnforcer(config.improvement_scope)

            self._snapshot = ImprovementSnapshot.capture(config.improvement_scope.allowed_files)

            logger.info(
                f"Captured snapshot {self._snapshot.snapshot_id} "
                f"for {len(self._snapshot.files)} file(s)"
            )

        cycle_config = config.to_cycle_config()
        state = self.start_cycle(cycle_config)

        self._bind_sources_for_cycle(state)

        return state

    def apply_improvement(
        self,
        proposal: ImprovementProposal,
        state: CycleState,
    ) -> ImprovementResult:
        """Apply an improvement proposal with scope enforcement.

        Args:
            proposal: The improvement proposal to apply.
            state: Current cycle state.

        Returns:
            ImprovementResult with status and details.
        """
        if not self._config or not self._config.improvement_scope:
            raise ValueError("No improvement scope configured")

        if not self._snapshot:
            raise ValueError("No snapshot captured")

        improvement_id = hashlib.sha256(
            f"{state.cycle_id}:{datetime.now().isoformat()}".encode()
        ).hexdigest()[:12]

        result = ImprovementResult(
            improvement_id=improvement_id,
            proposal=proposal,
            scope=self._config.improvement_scope,
            snapshot=self._snapshot,
            status="PENDING",
        )

        if proposal.is_none():
            result.status = "NO_PROPOSAL"
            result.mark_completed()
            self._improvement_result = result
            return result

        is_valid, violations = self._scope_enforcer.validate_proposal(proposal)
        if not is_valid:
            result.status = "REJECTED"
            result.rejection_reason = ImprovementRejectionReason.SCOPE_VIOLATION
            result.rejection_details = "; ".join(violations)
            result.mark_completed()
            self._improvement_result = result
            logger.warning(f"Improvement rejected: {result.rejection_details}")
            return result

        files_modified = []
        logger.info(
            f"[ControlledImprovement] apply_improvement: proposal.changes has {len(proposal.changes)} change(s)"
        )
        for i, ch in enumerate(proposal.changes):
            logger.info(
                f"[ControlledImprovement] Change {i}: file={ch.file}, before_len={len(ch.before)}, after_len={len(ch.after)}"
            )

        if not proposal.changes:
            logger.warning("[ControlledImprovement] No changes in proposal - nothing to apply!")
            result.status = "NO_PROPOSAL"
            result.rejection_details = "Proposal contained no changes"
            result.mark_completed()
            self._improvement_result = result
            return result

        try:
            for change in proposal.changes:
                abs_path = str(Path(change.file).resolve())

                if not self._scope_enforcer.check_file_access(abs_path, "write"):
                    raise ImprovementScopeViolation(
                        f"Cannot write to {change.file}",
                        [change.file],
                        self._config.improvement_scope.allowed_files,
                    )

                if os.path.exists(abs_path):
                    with open(abs_path, "r", encoding="utf-8") as f:
                        current_content = f.read()

                    logger.info("[ControlledImprovement] Checking before content match:")
                    logger.info(f"[ControlledImprovement] before repr: {repr(change.before)}")
                    logger.info(
                        f"[ControlledImprovement] before in file: {change.before in current_content}"
                    )

                    if change.before not in current_content:
                        logger.error("[ControlledImprovement] MISMATCH - before content not found!")
                        logger.error(
                            f"[ControlledImprovement] Looking for: {repr(change.before[:100])}"
                        )
                        for i, line in enumerate(current_content.splitlines()):
                            if "on:" in line or "trip_id" in line:
                                logger.error(
                                    f"[ControlledImprovement] File line {i + 1}: {repr(line)}"
                                )
                        raise ValueError(
                            f"Before content not found in {change.file}. "
                            "File may have changed or proposal is stale."
                        )

                    new_content = current_content.replace(change.before, change.after, 1)

                    with open(abs_path, "w", encoding="utf-8") as f:
                        f.write(new_content)

                    files_modified.append(abs_path)

                    result.before_after_diff[abs_path] = {
                        "before": change.before,
                        "after": change.after,
                    }

            result.files_modified = files_modified
            result.diff_hash = self._compute_diff_hash(result.before_after_diff)
            self._improvement_applied = True

            logger.info(f"Applied improvement to {len(files_modified)} file(s)")

        except Exception as e:
            logger.error(f"Failed to apply improvement: {e}")
            if files_modified and self._config.rollback_on_failure:
                self._snapshot.restore()
                result.files_restored = files_modified
            result.status = "REJECTED"
            result.rejection_reason = ImprovementRejectionReason.VALIDATION_FAILED
            result.rejection_details = str(e)
            result.mark_completed()
            self._improvement_result = result
            return result

        result.status = "APPLIED"
        result.mark_completed()
        self._improvement_result = result
        return result

    def validate_improvement(self) -> tuple[bool, str]:
        """Validate the applied improvement by re-running pipelines.

        Runs `odibi validate` on each modified file to ensure the changes
        produce valid configuration. Returns failure if any validation fails.

        Returns:
            Tuple of (passed, error_message).
        """
        if not self._improvement_result:
            return False, "No improvement to validate"

        if self._improvement_result.status != "APPLIED":
            return False, f"Improvement status is {self._improvement_result.status}"

        logger.info("Validating improvement by re-running pipelines...")

        from .execution import OdibiRunner

        runner = OdibiRunner(self.odibi_root)

        errors = []
        for file_path in self._improvement_result.files_modified:
            try:
                result = runner.validate_config(
                    file_path,
                    agent_permissions=None,
                    agent_role=None,
                )
                if not result.success:
                    errors.append(f"{file_path}: {result.stderr or result.stdout}")
                    logger.warning(
                        f"Validation failed for {file_path}: exit_code={result.exit_code}"
                    )
            except Exception as e:
                errors.append(f"{file_path}: {e}")
                logger.warning(f"Validation error for {file_path}: {e}")

        if errors:
            error_msg = "; ".join(errors)
            self._improvement_result.validation_passed = False
            return False, error_msg

        self._improvement_result.validation_passed = True
        return True, ""

    def check_regressions(self, state: CycleState) -> tuple[bool, list[str]]:
        """Check for regressions in golden projects.

        Runs each golden project's pipeline and checks for successful exit codes.
        Any failure indicates a regression caused by the improvement.

        Args:
            state: Current cycle state.

        Returns:
            Tuple of (passed, list of failures).
        """
        if not self._config or not self._config.golden_projects:
            if self._improvement_result:
                self._improvement_result.regression_check_passed = True
            return True, []

        logger.info(
            f"Running regression checks on {len(self._config.golden_projects)} golden project(s)"
        )

        from .execution import OdibiRunner

        runner = OdibiRunner(self.odibi_root)
        workspace_root = self._config.project_root or self.odibi_root

        failures = []
        for golden in self._config.golden_projects:
            resolved_path = golden.resolve_path(workspace_root)
            logger.info(f"Running golden project: {golden.name} ({resolved_path})")

            try:
                result = runner.run_pipeline(
                    resolved_path,
                    dry_run=False,
                    agent_permissions=None,
                    agent_role=None,
                )
                if not result.success:
                    failure_msg = f"{golden.name}: exit_code={result.exit_code}"
                    if result.stderr:
                        failure_msg += f" - {result.stderr[:200]}"
                    failures.append(failure_msg)
                    logger.warning(f"Golden project {golden.name} FAILED: {failure_msg}")
                else:
                    logger.info(f"Golden project {golden.name} PASSED")
            except Exception as e:
                failures.append(f"{golden.name}: {e}")
                logger.error(f"Golden project {golden.name} ERROR: {e}")

        if self._improvement_result:
            self._improvement_result.regression_check_passed = len(failures) == 0

        return len(failures) == 0, failures

    def check_unrelated_pipelines_stable(self, state: CycleState) -> tuple[bool, list[str]]:
        """Check that unrelated pipelines haven't been affected.

        Args:
            state: Current cycle state.

        Returns:
            Tuple of (stable, list of affected files).
        """
        if not self._snapshot:
            if self._improvement_result:
                self._improvement_result.unrelated_pipelines_stable = True
            return True, []

        _, changed = self._snapshot.verify_unchanged()

        allowed = set(self._config.improvement_scope.allowed_files) if self._config else set()
        unrelated_changes = [f for f in changed if not any(a in f for a in allowed)]

        if self._improvement_result:
            self._improvement_result.unrelated_pipelines_stable = len(unrelated_changes) == 0

        return len(unrelated_changes) == 0, unrelated_changes

    def rollback_improvement(self) -> bool:
        """Rollback the applied improvement.

        Returns:
            True if rollback was successful.
        """
        if not self._snapshot:
            logger.warning("No snapshot available for rollback")
            return False

        if not self._improvement_result:
            logger.warning("No improvement to rollback")
            return False

        restored = self._snapshot.restore()
        self._improvement_result.files_restored = restored
        self._improvement_result.status = "ROLLED_BACK"
        self._improvement_result.mark_completed()
        self._improvement_applied = False

        logger.info(f"Rolled back improvement, restored {len(restored)} file(s)")
        return len(restored) > 0

    def finalize_improvement(self, state: CycleState) -> ImprovementResult:
        """Finalize the improvement process.

        Runs validation and regression checks, rolls back on failure.

        Args:
            state: Current cycle state.

        Returns:
            Final ImprovementResult.
        """
        if not self._improvement_result:
            raise ValueError("No improvement to finalize")

        if self._improvement_result.status != "APPLIED":
            self._improvement_result.mark_completed()
            return self._improvement_result

        validation_passed, validation_error = self.validate_improvement()
        if not validation_passed:
            self._improvement_result.rejection_reason = ImprovementRejectionReason.VALIDATION_FAILED
            self._improvement_result.rejection_details = validation_error
            if self._config and self._config.rollback_on_failure:
                self.rollback_improvement()
            else:
                self._improvement_result.status = "REJECTED"
            self._improvement_result.mark_completed()
            return self._improvement_result

        regression_passed, failures = self.check_regressions(state)
        if not regression_passed:
            self._improvement_result.rejection_reason = (
                ImprovementRejectionReason.REGRESSION_DETECTED
            )
            self._improvement_result.rejection_details = f"Regressions: {', '.join(failures)}"
            if self._config and self._config.rollback_on_failure:
                self.rollback_improvement()
            else:
                self._improvement_result.status = "REJECTED"
            self._improvement_result.mark_completed()
            return self._improvement_result

        stable, affected = self.check_unrelated_pipelines_stable(state)
        if not stable:
            self._improvement_result.rejection_reason = ImprovementRejectionReason.UNRELATED_CHANGES
            self._improvement_result.rejection_details = f"Affected: {', '.join(affected)}"
            if self._config and self._config.rollback_on_failure:
                self.rollback_improvement()
            else:
                self._improvement_result.status = "REJECTED"
            self._improvement_result.mark_completed()
            return self._improvement_result

        self._improvement_result.mark_completed()
        logger.info(f"Improvement {self._improvement_result.improvement_id} finalized successfully")
        return self._improvement_result

    def get_improvement_result(self) -> Optional[ImprovementResult]:
        """Get the current improvement result."""
        return self._improvement_result

    def _compute_diff_hash(self, diff: dict[str, dict[str, str]]) -> str:
        """Compute a deterministic hash of the diff."""
        sorted_items = sorted(diff.items())
        content = json.dumps(sorted_items, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

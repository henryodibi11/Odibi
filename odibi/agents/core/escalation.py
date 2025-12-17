"""Phase 12: Human-Gated Improvement Escalation.

This module provides the bridge from Issue Discovery (Phase 10-11) to
Controlled Improvement (Phase 9.G) under strict human control.

PHASE 12 HARD INVARIANTS (NON-NEGOTIABLE):

1. DISCOVERY IS FOREVER PASSIVE
   - Issue Discovery MUST NOT modify files
   - Issue Discovery MUST NOT trigger execution
   - Issue Discovery MUST NOT auto-select issues
   - Issue Discovery MUST NOT call Controlled Improvement
   - Discovery may only emit structured metadata

2. HUMAN INTENT IS THE AUTHORIZATION BOUNDARY
   - A controlled improvement may start ONLY if a human explicitly:
     * Selects an issue
     * Selects a target file
     * Confirms intent
   - No defaults. No background escalation. No "Fix All".

3. IMPROVEMENT SCOPE MUST BE EXPLICIT AND MINIMAL
   - Each improvement run MUST specify:
     * Exactly one target file
     * Exactly one issue (or a tightly defined group)
   - No implicit scope expansion. No guessing related files.

4. LEARNING HARNESS IS READ-ONLY FOREVER
   - Files under .odibi/learning_harness/**:
     * May execute
     * May be observed
     * May be used for regression
   - They MUST NEVER:
     * Be improvement targets
     * Be modified
     * Be suggested as fix locations
   - Violation = hard failure

5. IMPROVEMENTS MUST BE FULLY REVERSIBLE
   - Every improvement:
     * Must snapshot before apply
     * Must rollback on any regression failure
     * Must fail hard if rollback fails
   - Partial success is not allowed

6. NO HIDDEN KNOWLEDGE INJECTION
   - Agents may only reason using:
     * Indexed code
     * Retrieved schemas
     * Execution evidence
     * Explicit user-provided issue descriptions
   - Agents MUST NOT:
     * Assume intent
     * Invent undocumented rules
     * Treat comments as authoritative unless marked as evidence

7. APPROVAL IS MANDATORY
   - Even correct fixes:
     * Must be presented before apply
     * Must require explicit approval
     * Must be auditable
   - No silent auto-merge. No "safe fix" bypass.

8. FAILURE IS A VALID OUTCOME
   - The following are successful outcomes:
     * No proposal
     * Proposal rejected
     * Regression failure with rollback
   - The goal is system integrity, not change velocity.
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

from .controlled_improvement import (
    ControlledImprovementConfig,
    ControlledImprovementRunner,
    ImprovementResult,
    ImprovementScope,
    is_learning_harness_path,
)
from .cycle import GoldenProjectConfig
from .issue_discovery import (
    DiscoveredIssue,
    IssueDiscoveryResult,
)

logger = logging.getLogger(__name__)


class EscalationError(Exception):
    """Base exception for escalation errors."""

    pass


class EscalationValidationError(EscalationError):
    """Raised when escalation validation fails."""

    def __init__(self, violations: list[str]):
        self.violations = violations
        super().__init__(f"Escalation validation failed: {'; '.join(violations)}")


class HarnessEscalationBlockedError(EscalationError):
    """Raised when attempting to escalate to a learning harness file.

    This is a hard failure - learning harness files are NEVER improvement targets.
    """

    def __init__(self, file_path: str):
        self.file_path = file_path
        super().__init__(
            f"ESCALATION BLOCKED: '{file_path}' is in the protected learning harness. "
            f"Learning harness files are system validation fixtures and CANNOT be "
            f"improvement targets under any circumstances."
        )


class AmbiguousSelectionError(EscalationError):
    """Raised when the selection is ambiguous or incomplete."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"ESCALATION BLOCKED: Ambiguous selection - {reason}")


class MultiFileScopeError(EscalationError):
    """Raised when attempting to escalate to multiple files.

    Phase 12 enforces single-file scope for maximum control.
    """

    def __init__(self, files: list[str]):
        self.files = files
        super().__init__(
            f"ESCALATION BLOCKED: Multi-file scope not allowed. "
            f"Each escalation must target exactly one file. "
            f"Attempted files: {files}"
        )


class EscalationStatus(str, Enum):
    """Status of an escalation request."""

    PENDING_CONFIRMATION = "PENDING_CONFIRMATION"
    CONFIRMED = "CONFIRMED"
    REJECTED = "REJECTED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ROLLED_BACK = "ROLLED_BACK"


@dataclass(frozen=True)
class IssueSelection:
    """Immutable artifact representing user's explicit selection for improvement.

    This is the ONLY allowed bridge from Discovery to Controlled Improvement.
    Once created, it cannot be modified.

    INVARIANT: This artifact is created ONLY via explicit user action.

    Attributes:
        selection_id: Unique identifier for this selection.
        issue_id: ID of the selected issue.
        issue_type: Type of the selected issue.
        evidence: Evidence supporting the issue (copied, not referenced).
        target_file: Exactly one file to improve.
        user_intent: User-provided description of what they want to fix.
        created_at: Timestamp when selection was created.
        created_by: Identifier for who created this (audit trail).
    """

    selection_id: str
    issue_id: str
    issue_type: str
    evidence: tuple[dict[str, Any], ...]  # Immutable tuple of evidence dicts
    target_file: str
    user_intent: str
    created_at: str
    created_by: str = "user"

    def __post_init__(self):
        # Validate at creation time - fail fast
        if not self.issue_id:
            raise AmbiguousSelectionError("issue_id is required")
        if not self.target_file:
            raise AmbiguousSelectionError("target_file is required")
        if not self.user_intent or not self.user_intent.strip():
            raise AmbiguousSelectionError("user_intent is required - describe what to fix")
        if is_learning_harness_path(self.target_file):
            raise HarnessEscalationBlockedError(self.target_file)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for persistence."""
        return {
            "selection_id": self.selection_id,
            "issue_id": self.issue_id,
            "issue_type": self.issue_type,
            "evidence": list(self.evidence),
            "target_file": self.target_file,
            "user_intent": self.user_intent,
            "created_at": self.created_at,
            "created_by": self.created_by,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "IssueSelection":
        """Deserialize from dictionary."""
        return cls(
            selection_id=data["selection_id"],
            issue_id=data["issue_id"],
            issue_type=data["issue_type"],
            evidence=tuple(data.get("evidence", [])),
            target_file=data["target_file"],
            user_intent=data["user_intent"],
            created_at=data["created_at"],
            created_by=data.get("created_by", "user"),
        )


@dataclass
class EscalationRequest:
    """A request to escalate from discovery to controlled improvement.

    This represents the full context needed for human-gated escalation.
    It is NOT immutable - status changes as the request progresses.
    """

    request_id: str
    selection: IssueSelection
    status: EscalationStatus
    improvement_config: Optional[ControlledImprovementConfig] = None
    improvement_result: Optional[ImprovementResult] = None
    created_at: str = ""
    confirmed_at: Optional[str] = None
    completed_at: Optional[str] = None
    rejection_reason: Optional[str] = None
    audit_log: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        if not self.request_id:
            self.request_id = self._generate_request_id()
        self._log_event(
            "CREATED", f"Escalation request created for issue {self.selection.issue_id}"
        )

    def _generate_request_id(self) -> str:
        """Generate a unique request ID."""
        content = f"{self.selection.selection_id}:{self.created_at}"
        return f"ESC-{hashlib.sha256(content.encode()).hexdigest()[:12]}"

    def _log_event(self, event_type: str, details: str) -> None:
        """Add an audit log entry."""
        self.audit_log.append(
            {
                "timestamp": datetime.now().isoformat(),
                "event_type": event_type,
                "details": details,
            }
        )

    def confirm(self) -> None:
        """Confirm the escalation request.

        This marks the request as ready for improvement execution.
        Must be called explicitly by a human action.
        """
        if self.status != EscalationStatus.PENDING_CONFIRMATION:
            raise EscalationError(
                f"Cannot confirm: request is {self.status.value}, expected PENDING_CONFIRMATION"
            )
        self.status = EscalationStatus.CONFIRMED
        self.confirmed_at = datetime.now().isoformat()
        self._log_event("CONFIRMED", "User confirmed escalation request")

    def reject(self, reason: str) -> None:
        """Reject the escalation request."""
        self.status = EscalationStatus.REJECTED
        self.rejection_reason = reason
        self.completed_at = datetime.now().isoformat()
        self._log_event("REJECTED", f"User rejected escalation: {reason}")

    def mark_in_progress(self) -> None:
        """Mark the request as in progress."""
        if self.status != EscalationStatus.CONFIRMED:
            raise EscalationError(
                f"Cannot start: request is {self.status.value}, expected CONFIRMED"
            )
        self.status = EscalationStatus.IN_PROGRESS
        self._log_event("STARTED", "Controlled improvement started")

    def mark_completed(self, result: ImprovementResult) -> None:
        """Mark the request as completed."""
        self.improvement_result = result
        self.status = EscalationStatus.COMPLETED
        self.completed_at = datetime.now().isoformat()
        self._log_event("COMPLETED", f"Improvement completed with status: {result.status}")

    def mark_failed(self, error: str) -> None:
        """Mark the request as failed."""
        self.status = EscalationStatus.FAILED
        self.completed_at = datetime.now().isoformat()
        self._log_event("FAILED", f"Improvement failed: {error}")

    def mark_rolled_back(self) -> None:
        """Mark the request as rolled back."""
        self.status = EscalationStatus.ROLLED_BACK
        self.completed_at = datetime.now().isoformat()
        self._log_event("ROLLED_BACK", "Improvement rolled back due to regression or failure")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "request_id": self.request_id,
            "selection": self.selection.to_dict(),
            "status": self.status.value,
            "improvement_config": (
                self.improvement_config.to_dict() if self.improvement_config else None
            ),
            "improvement_result": (
                self.improvement_result.to_dict() if self.improvement_result else None
            ),
            "created_at": self.created_at,
            "confirmed_at": self.confirmed_at,
            "completed_at": self.completed_at,
            "rejection_reason": self.rejection_reason,
            "audit_log": self.audit_log,
        }


def create_issue_selection(
    issue: DiscoveredIssue,
    user_intent: str,
    target_file: Optional[str] = None,
    created_by: str = "user",
) -> IssueSelection:
    """Create an IssueSelection from a DiscoveredIssue.

    This is the ONLY way to create an IssueSelection.
    It enforces all validation at creation time.

    Args:
        issue: The discovered issue to select.
        user_intent: User's description of what they want to fix.
        target_file: Optional override for target file (defaults to issue location).
        created_by: Identifier for audit trail.

    Returns:
        An immutable IssueSelection.

    Raises:
        HarnessEscalationBlockedError: If target is in learning harness.
        AmbiguousSelectionError: If selection is incomplete.
    """
    # Use issue's file if not explicitly provided
    actual_target = target_file or issue.location.file_path

    # Block harness files immediately
    if is_learning_harness_path(actual_target):
        raise HarnessEscalationBlockedError(actual_target)

    # Verify issue is an improvement candidate
    if not issue.is_improvement_candidate():
        if issue.location.is_in_learning_harness():
            raise HarnessEscalationBlockedError(issue.location.file_path)
        raise AmbiguousSelectionError(
            f"Issue {issue.issue_id[:8]} is not an improvement candidate "
            f"(confidence: {issue.confidence.value}, evidence count: {len(issue.evidence)})"
        )

    # Generate selection ID
    selection_id = hashlib.sha256(
        f"{issue.issue_id}:{actual_target}:{datetime.now().isoformat()}".encode()
    ).hexdigest()[:16]

    # Copy evidence to ensure immutability
    evidence_copies = tuple(e.to_dict() for e in issue.evidence)

    return IssueSelection(
        selection_id=selection_id,
        issue_id=issue.issue_id,
        issue_type=issue.issue_type.value,
        evidence=evidence_copies,
        target_file=actual_target,
        user_intent=user_intent,
        created_at=datetime.now().isoformat(),
        created_by=created_by,
    )


class EscalationValidator:
    """Validates escalation requests before execution.

    This is the gatekeeper that ensures all Phase 12 invariants are enforced.
    """

    def __init__(self, project_root: str):
        self.project_root = project_root

    def validate_selection(self, selection: IssueSelection) -> list[str]:
        """Validate an IssueSelection for escalation.

        Args:
            selection: The selection to validate.

        Returns:
            List of validation errors (empty if valid).
        """
        errors = []

        # Check 1: Target file not in harness
        if is_learning_harness_path(selection.target_file):
            errors.append(f"HARNESS BLOCKED: '{selection.target_file}' is a learning harness file")

        # Check 2: Selection is not empty/ambiguous
        if not selection.issue_id:
            errors.append("AMBIGUOUS: No issue_id specified")

        if not selection.target_file:
            errors.append("AMBIGUOUS: No target_file specified")

        if not selection.user_intent or not selection.user_intent.strip():
            errors.append("AMBIGUOUS: No user_intent specified")

        # Check 3: Target file exists (unless new file creation is intended)
        target_path = Path(selection.target_file)
        if not target_path.is_absolute():
            target_path = Path(self.project_root) / selection.target_file

        if not target_path.exists():
            errors.append(f"TARGET NOT FOUND: '{selection.target_file}' does not exist")

        # Check 4: Evidence is present
        if not selection.evidence:
            errors.append("NO EVIDENCE: Issue must have supporting evidence")

        return errors

    def validate_for_escalation(
        self,
        selection: IssueSelection,
        discovery_result: Optional[IssueDiscoveryResult] = None,
    ) -> tuple[bool, list[str]]:
        """Full validation for escalation to controlled improvement.

        Args:
            selection: The issue selection.
            discovery_result: Optional discovery result to cross-reference.

        Returns:
            Tuple of (is_valid, list of errors).
        """
        errors = self.validate_selection(selection)

        # Cross-reference with discovery if available
        if discovery_result:
            issue = next(
                (i for i in discovery_result.issues if i.issue_id == selection.issue_id),
                None,
            )
            if not issue:
                errors.append(f"ISSUE NOT FOUND: {selection.issue_id[:8]} not in discovery result")
            elif not issue.is_improvement_candidate():
                errors.append(
                    f"NOT CANDIDATE: Issue {selection.issue_id[:8]} is not an improvement candidate"
                )

        return len(errors) == 0, errors


class HumanGatedEscalator:
    """Orchestrates human-gated escalation from discovery to improvement.

    This is the main interface for Phase 12. It ensures:
    - Human selection is explicit
    - Validation gates are enforced
    - Improvement scope is minimal
    - Rollback works correctly
    - Everything is auditable

    USAGE:
        escalator = HumanGatedEscalator(odibi_root, project_root)

        # Step 1: User selects an issue from discovery
        selection = escalator.create_selection_from_discovery(issue, user_intent)

        # Step 2: Create escalation request (pending confirmation)
        request = escalator.create_escalation_request(selection)

        # Step 3: User explicitly confirms
        escalator.confirm_escalation(request)

        # Step 4: Execute controlled improvement
        result = escalator.execute_escalation(request, golden_projects)
    """

    def __init__(
        self,
        odibi_root: str,
        project_root: str,
        azure_config: Optional[Any] = None,
    ):
        """Initialize the escalator.

        Args:
            odibi_root: Path to odibi repository root.
            project_root: Path to project being improved.
            azure_config: Optional Azure configuration for LLM.
        """
        self.odibi_root = odibi_root
        self.project_root = project_root
        self.azure_config = azure_config

        self._validator = EscalationValidator(project_root)
        self._requests_dir = os.path.join(odibi_root, ".odibi", "escalation_requests")
        os.makedirs(self._requests_dir, exist_ok=True)

        self._active_request: Optional[EscalationRequest] = None

    def create_selection_from_discovery(
        self,
        issue: DiscoveredIssue,
        user_intent: str,
        target_file: Optional[str] = None,
    ) -> IssueSelection:
        """Create an IssueSelection from a discovered issue.

        This is Step 1 of escalation - user explicitly selects an issue.

        Args:
            issue: The discovered issue.
            user_intent: User's description of what to fix.
            target_file: Optional override for target file.

        Returns:
            Immutable IssueSelection.

        Raises:
            HarnessEscalationBlockedError: If target is in harness.
            AmbiguousSelectionError: If selection is invalid.
        """
        return create_issue_selection(
            issue=issue,
            user_intent=user_intent,
            target_file=target_file,
        )

    def create_escalation_request(
        self,
        selection: IssueSelection,
    ) -> EscalationRequest:
        """Create an escalation request from a selection.

        This is Step 2 - creates a pending request that requires confirmation.

        Args:
            selection: The issue selection.

        Returns:
            EscalationRequest in PENDING_CONFIRMATION status.

        Raises:
            EscalationValidationError: If validation fails.
        """
        # Validate before creating request
        is_valid, errors = self._validator.validate_for_escalation(selection)
        if not is_valid:
            raise EscalationValidationError(errors)

        request = EscalationRequest(
            request_id="",
            selection=selection,
            status=EscalationStatus.PENDING_CONFIRMATION,
        )

        # Persist request
        self._persist_request(request)

        logger.info(
            f"Created escalation request {request.request_id} for issue {selection.issue_id[:8]}"
        )

        return request

    def confirm_escalation(self, request: EscalationRequest) -> None:
        """Confirm an escalation request.

        This is Step 3 - human explicitly confirms they want to proceed.

        Args:
            request: The request to confirm.

        Raises:
            EscalationError: If confirmation fails.
        """
        request.confirm()
        self._persist_request(request)

        logger.info(f"Confirmed escalation request {request.request_id}")

    def reject_escalation(self, request: EscalationRequest, reason: str) -> None:
        """Reject an escalation request.

        Args:
            request: The request to reject.
            reason: Reason for rejection.
        """
        request.reject(reason)
        self._persist_request(request)

        logger.info(f"Rejected escalation request {request.request_id}: {reason}")

    def execute_escalation(
        self,
        request: EscalationRequest,
        golden_projects: Optional[list[GoldenProjectConfig]] = None,
    ) -> ImprovementResult:
        """Execute an escalation - run controlled improvement.

        This is Step 4 - actually run the improvement with all safety guards.

        INVARIANT: Request MUST be in CONFIRMED status.

        Args:
            request: The confirmed escalation request.
            golden_projects: Optional golden projects for regression testing.

        Returns:
            ImprovementResult from the improvement cycle.

        Raises:
            EscalationError: If execution fails.
        """
        if request.status != EscalationStatus.CONFIRMED:
            raise EscalationError(
                f"Cannot execute: request status is {request.status.value}, "
                f"expected CONFIRMED. Human confirmation is REQUIRED."
            )

        self._active_request = request
        request.mark_in_progress()
        self._persist_request(request)

        try:
            # Build improvement config from selection
            config = self._build_improvement_config(request.selection, golden_projects)
            request.improvement_config = config

            # Run controlled improvement
            runner = ControlledImprovementRunner(
                odibi_root=self.odibi_root,
                azure_config=self.azure_config,
            )

            state = runner.start_controlled_improvement_cycle(config)

            while not state.is_finished():
                state = runner.run_next_step(state)

            result = runner.get_improvement_result()

            if result is None:
                raise EscalationError("Improvement cycle completed but no result available")

            # Update request based on result
            if result.status == "ROLLED_BACK":
                request.mark_rolled_back()
            elif result.status in ("APPLIED", "NO_PROPOSAL"):
                request.mark_completed(result)
            else:
                request.mark_failed(result.rejection_details or "Unknown failure")

            self._persist_request(request)

            logger.info(f"Escalation {request.request_id} completed with status {result.status}")

            return result

        except Exception as e:
            request.mark_failed(str(e))
            self._persist_request(request)
            logger.error(f"Escalation {request.request_id} failed: {e}")
            raise

        finally:
            self._active_request = None

    def _build_improvement_config(
        self,
        selection: IssueSelection,
        golden_projects: Optional[list[GoldenProjectConfig]],
    ) -> ControlledImprovementConfig:
        """Build ControlledImprovementConfig from selection.

        Args:
            selection: The issue selection.
            golden_projects: Optional golden projects.

        Returns:
            Config for controlled improvement.
        """
        # Build task description from selection
        task_description = self._build_task_description(selection)

        # Create single-file scope
        scope = ImprovementScope.for_single_file(
            selection.target_file,
            description=f"Fix issue {selection.issue_id[:8]}: {selection.user_intent}",
        )

        return ControlledImprovementConfig(
            project_root=self.project_root,
            task_description=task_description,
            improvement_scope=scope,
            golden_projects=golden_projects or [],
            require_validation_pass=True,
            require_regression_check=bool(golden_projects),
            rollback_on_failure=True,
        )

    def _build_task_description(self, selection: IssueSelection) -> str:
        """Build task description from selection.

        Args:
            selection: The issue selection.

        Returns:
            Task description string.
        """
        lines = [
            f"Fix the following issue in {selection.target_file}:",
            "",
            f"**Issue Type:** {selection.issue_type}",
            f"**Issue ID:** {selection.issue_id}",
            "",
            "**User Intent:**",
            selection.user_intent,
            "",
        ]

        if selection.evidence:
            lines.append("**Evidence:**")
            for i, ev in enumerate(selection.evidence, 1):
                lines.append(
                    f"  {i}. [{ev.get('evidence_type', 'unknown')}] {ev.get('excerpt', '')[:100]}"
                )
            lines.append("")

        return "\n".join(lines)

    def _persist_request(self, request: EscalationRequest) -> None:
        """Persist an escalation request to disk."""
        request_path = os.path.join(self._requests_dir, f"{request.request_id}.json")
        with open(request_path, "w", encoding="utf-8") as f:
            json.dump(request.to_dict(), f, indent=2)

    def load_request(self, request_id: str) -> Optional[EscalationRequest]:
        """Load an escalation request by ID.

        Args:
            request_id: The request ID.

        Returns:
            EscalationRequest or None if not found.
        """
        request_path = os.path.join(self._requests_dir, f"{request_id}.json")
        if not os.path.exists(request_path):
            return None

        with open(request_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        selection = IssueSelection.from_dict(data["selection"])
        request = EscalationRequest(
            request_id=data["request_id"],
            selection=selection,
            status=EscalationStatus(data["status"]),
            created_at=data["created_at"],
            confirmed_at=data.get("confirmed_at"),
            completed_at=data.get("completed_at"),
            rejection_reason=data.get("rejection_reason"),
            audit_log=data.get("audit_log", []),
        )
        return request

    def get_pending_requests(self) -> list[EscalationRequest]:
        """Get all pending escalation requests.

        Returns:
            List of requests in PENDING_CONFIRMATION status.
        """
        pending = []
        for filename in os.listdir(self._requests_dir):
            if filename.endswith(".json"):
                request_id = filename[:-5]
                request = self.load_request(request_id)
                if request and request.status == EscalationStatus.PENDING_CONFIRMATION:
                    pending.append(request)
        return pending


def validate_escalation_safety(
    selection: IssueSelection,
    discovery_result: Optional[IssueDiscoveryResult] = None,
) -> tuple[bool, list[str]]:
    """Standalone validation function for escalation safety.

    Use this to validate before creating requests.

    Args:
        selection: The issue selection.
        discovery_result: Optional discovery result for cross-reference.

    Returns:
        Tuple of (is_safe, list of violations).
    """
    violations = []

    # Check 1: Harness protection
    if is_learning_harness_path(selection.target_file):
        violations.append(f"HARNESS VIOLATION: {selection.target_file} is protected")

    # Check 2: Non-empty selection
    if not selection.issue_id:
        violations.append("EMPTY SELECTION: No issue selected")

    if not selection.target_file:
        violations.append("EMPTY SELECTION: No target file specified")

    if not selection.user_intent.strip():
        violations.append("EMPTY SELECTION: No user intent specified")

    # Check 3: Evidence present
    if not selection.evidence:
        violations.append("NO EVIDENCE: Selection has no supporting evidence")

    # Check 4: Cross-reference with discovery
    if discovery_result:
        issue_ids = {i.issue_id for i in discovery_result.issues}
        if selection.issue_id not in issue_ids:
            violations.append(f"UNKNOWN ISSUE: {selection.issue_id[:8]} not found in discovery")
        else:
            issue = next(i for i in discovery_result.issues if i.issue_id == selection.issue_id)
            if not issue.is_improvement_candidate():
                violations.append(
                    f"NOT CANDIDATE: Issue {selection.issue_id[:8]} cannot be improved"
                )

    return len(violations) == 0, violations

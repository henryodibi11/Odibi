"""Enforcement module for Odibi AI Agent Suite.

This module provides MECHANICAL enforcement of invariants that were previously
only DECLARED. All checks in this module raise exceptions on violation.

Phase 0 Invariants Enforced:
1. Permission enforcement (can_execute_tasks, can_edit_source)
2. Wall-clock timeout enforcement
3. Mechanical review gating
4. Guaranteed cycle termination
"""

import re
import threading
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .agent_base import AgentPermissions, AgentRole


class EnforcementError(Exception):
    """Base exception for all enforcement violations."""

    pass


class PermissionDeniedError(EnforcementError):
    """Raised when an agent attempts an action without permission."""

    pass


class WallClockTimeoutError(EnforcementError):
    """Raised when wall-clock time limit is exceeded."""

    pass


class ReviewGatingError(EnforcementError):
    """Raised when code changes are attempted without review approval."""

    pass


class CycleTerminationError(EnforcementError):
    """Raised when cycle termination invariant is violated."""

    pass


class CycleExitStatus(str, Enum):
    """Exit status for cycles."""

    COMPLETED = "completed"
    TIMEOUT = "timeout"
    INTERRUPTED = "interrupted"
    MAX_STEPS = "max_steps"
    ERROR = "error"


@dataclass
class ReviewDecision:
    """Structured review decision from ReviewerAgent."""

    approved: bool
    reviewer_role: str
    timestamp: str
    reasoning: str
    proposal_hash: str

    @classmethod
    def parse_from_response(cls, content: str, proposal_hash: str) -> "ReviewDecision":
        """Parse a ReviewDecision from agent response content.

        Args:
            content: The response content from ReviewerAgent.
            proposal_hash: Hash of the proposal being reviewed.

        Returns:
            ReviewDecision with parsed values.

        Raises:
            ReviewGatingError: If response cannot be parsed as valid decision.
        """
        if not content:
            raise ReviewGatingError("Empty review response cannot be parsed")

        content_upper = content.upper()

        approved_match = re.search(r"\bAPPROVED\b", content_upper)
        rejected_match = re.search(r"\bREJECTED\b", content_upper)

        if approved_match and rejected_match:
            approved_pos = approved_match.start()
            rejected_pos = rejected_match.start()
            approved = approved_pos < rejected_pos
        elif approved_match:
            approved = True
        elif rejected_match:
            approved = False
        else:
            raise ReviewGatingError(
                "Review response must contain explicit APPROVED or REJECTED keyword. "
                f"Got: {content[:200]}..."
            )

        return cls(
            approved=approved,
            reviewer_role="reviewer",
            timestamp=datetime.now().isoformat(),
            reasoning=content[:1000],
            proposal_hash=proposal_hash,
        )


class PermissionEnforcer:
    """Enforces permission checks at runtime."""

    ODIBI_SOURCE_PATTERNS = (
        "agents/core/",
        "agents/pipelines/",
        "agents/prompts/",
        "agents/ui/",
        "odibi/",
        "scripts/",
    )

    @classmethod
    def check_execute_permission(
        cls,
        permissions: "AgentPermissions",
        role: "AgentRole",
    ) -> None:
        """Check if agent has permission to execute tasks.

        Args:
            permissions: The agent's permissions.
            role: The agent's role.

        Raises:
            PermissionDeniedError: If agent lacks can_execute_tasks.
        """
        if not permissions.can_execute_tasks:
            raise PermissionDeniedError(
                f"Agent role '{role.value}' does not have can_execute_tasks permission. "
                f"Execution gateway access denied."
            )

    @classmethod
    def check_source_edit_permission(
        cls,
        permissions: "AgentPermissions",
        role: "AgentRole",
        target_path: str,
        odibi_root: str,
    ) -> None:
        """Check if agent has permission to edit source files.

        Args:
            permissions: The agent's permissions.
            role: The agent's role.
            target_path: Path being modified.
            odibi_root: Root path of Odibi repository.

        Raises:
            PermissionDeniedError: If agent lacks can_edit_source for Odibi files.
        """
        normalized_target = target_path.replace("\\", "/").lower()
        normalized_root = odibi_root.replace("\\", "/").lower()

        if normalized_target.startswith(normalized_root):
            relative_path = normalized_target[len(normalized_root) :].lstrip("/")
        else:
            relative_path = normalized_target

        is_odibi_source = any(
            relative_path.startswith(pattern.lower()) for pattern in cls.ODIBI_SOURCE_PATTERNS
        )

        if is_odibi_source and not permissions.can_edit_source:
            raise PermissionDeniedError(
                f"Agent role '{role.value}' does not have can_edit_source permission. "
                f"Cannot modify Odibi source file: {target_path}"
            )


class WallClockEnforcer:
    """Enforces wall-clock time limits on cycle execution."""

    def __init__(self, max_runtime_seconds: float):
        """Initialize wall-clock enforcer.

        Args:
            max_runtime_seconds: Maximum runtime in seconds.
        """
        self.max_runtime_seconds = max_runtime_seconds
        self.start_time: Optional[datetime] = None
        self._timer: Optional[threading.Timer] = None
        self._timeout_triggered = False
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the wall-clock timer."""
        with self._lock:
            self.start_time = datetime.now()
            self._timeout_triggered = False

    def elapsed_seconds(self) -> float:
        """Get elapsed time in seconds."""
        if not self.start_time:
            return 0.0
        return (datetime.now() - self.start_time).total_seconds()

    def remaining_seconds(self) -> float:
        """Get remaining time in seconds."""
        return max(0.0, self.max_runtime_seconds - self.elapsed_seconds())

    def is_expired(self) -> bool:
        """Check if wall-clock limit has been exceeded."""
        return self.elapsed_seconds() >= self.max_runtime_seconds

    def check_and_raise(self) -> None:
        """Check if expired and raise if so.

        Raises:
            WallClockTimeoutError: If wall-clock limit exceeded.
        """
        if self.is_expired():
            raise WallClockTimeoutError(
                f"Wall-clock timeout exceeded. "
                f"Limit: {self.max_runtime_seconds}s, "
                f"Elapsed: {self.elapsed_seconds():.1f}s"
            )


class ReviewGateEnforcer:
    """Enforces mechanical review gating for code changes."""

    def __init__(self):
        """Initialize review gate."""
        self._pending_proposal: Optional[str] = None
        self._pending_hash: Optional[str] = None
        self._last_decision: Optional[ReviewDecision] = None

    def register_proposal(self, proposal_content: str) -> str:
        """Register a proposal for review.

        Args:
            proposal_content: The proposal content.

        Returns:
            Hash identifier for the proposal.
        """
        import hashlib

        self._pending_hash = hashlib.sha256(proposal_content.encode()).hexdigest()[:16]
        self._pending_proposal = proposal_content
        self._last_decision = None
        return self._pending_hash

    def register_decision(self, decision: ReviewDecision) -> None:
        """Register a review decision.

        Args:
            decision: The parsed ReviewDecision.

        Raises:
            ReviewGatingError: If decision doesn't match pending proposal.
        """
        if not self._pending_hash:
            raise ReviewGatingError("No pending proposal to review")

        if decision.proposal_hash != self._pending_hash:
            raise ReviewGatingError(
                f"Review decision hash mismatch. "
                f"Expected: {self._pending_hash}, Got: {decision.proposal_hash}"
            )

        self._last_decision = decision

    def check_approval(self) -> None:
        """Check if current proposal is approved.

        Raises:
            ReviewGatingError: If no approval exists for pending proposal.
        """
        if not self._pending_hash:
            raise ReviewGatingError("Cannot apply changes: no proposal registered")

        if not self._last_decision:
            raise ReviewGatingError(
                "Cannot apply changes: proposal has not been reviewed. "
                "ReviewerAgent must emit explicit APPROVED decision."
            )

        if not self._last_decision.approved:
            raise ReviewGatingError(
                "Cannot apply changes: proposal was REJECTED by reviewer. "
                f"Reason: {self._last_decision.reasoning[:200]}"
            )

    def clear(self) -> None:
        """Clear pending proposal and decision."""
        self._pending_proposal = None
        self._pending_hash = None
        self._last_decision = None

    @property
    def is_approved(self) -> bool:
        """Check if there's an approved decision."""
        return (
            self._last_decision is not None
            and self._last_decision.approved
            and self._last_decision.proposal_hash == self._pending_hash
        )


class CycleTerminationEnforcer:
    """Enforces guaranteed cycle termination."""

    def __init__(self, max_steps: int, max_runtime_seconds: float):
        """Initialize termination enforcer.

        Args:
            max_steps: Maximum number of steps allowed.
            max_runtime_seconds: Maximum wall-clock time in seconds.
        """
        self.max_steps = max_steps
        self.wall_clock = WallClockEnforcer(max_runtime_seconds)
        self.steps_executed = 0
        self._interrupted = False

    def start(self) -> None:
        """Start the cycle."""
        self.wall_clock.start()
        self.steps_executed = 0
        self._interrupted = False

    def record_step(self) -> None:
        """Record a step execution."""
        self.steps_executed += 1

    def interrupt(self) -> None:
        """Mark cycle as interrupted."""
        self._interrupted = True

    def check_termination(self) -> Optional[CycleExitStatus]:
        """Check if cycle should terminate.

        Returns:
            CycleExitStatus if termination required, None otherwise.
        """
        if self._interrupted:
            return CycleExitStatus.INTERRUPTED

        if self.wall_clock.is_expired():
            return CycleExitStatus.TIMEOUT

        if self.steps_executed >= self.max_steps:
            return CycleExitStatus.MAX_STEPS

        return None

    def must_terminate(self) -> bool:
        """Check if cycle must terminate now."""
        return self.check_termination() is not None

    def enforce_termination(self) -> CycleExitStatus:
        """Enforce termination check and return status.

        Returns:
            CycleExitStatus indicating why cycle terminated.

        Raises:
            CycleTerminationError: If in invalid state.
        """
        status = self.check_termination()
        if status is None:
            raise CycleTerminationError(
                "enforce_termination called but no termination condition met"
            )
        return status

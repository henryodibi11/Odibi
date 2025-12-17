"""Structured schemas for agent communication contracts.

These schemas enforce strict data formats between agents in the cycle,
ensuring that:
1. ObserverAgent emits structured observations (not free text)
2. ImprovementAgent receives and processes observations automatically
3. ImprovementAgent emits structured proposals
4. ReviewerAgent can mechanically validate proposal contracts

This is a data-flow and contract fix, not a behavior expansion.
"""

import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Optional


class IssueSeverity(str, Enum):
    """Severity levels for observed issues."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class IssueType(str, Enum):
    """Types of issues that can be observed."""

    # Execution status issues
    EXECUTION_FAILURE = "EXECUTION_FAILURE"  # Pipeline Status = FAILURE
    PARTIAL_EXECUTION = "PARTIAL_EXECUTION"  # Pipeline Status = PARTIAL

    # Data issues
    DATA_QUALITY = "DATA_QUALITY"  # Missing fields, bad data, validation failures
    DATA_ERROR = "DATA_ERROR"  # Schema mismatches, null handling

    # Performance issues
    PERFORMANCE = "PERFORMANCE"  # Threshold exceeded, slow operations

    # Configuration/Auth issues
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"  # Invalid YAML, missing fields
    AUTH_ERROR = "AUTH_ERROR"  # Authentication/authorization failures

    # User experience issues
    UX_FRICTION = "UX_FRICTION"  # Confusing messages, manual steps required
    DX = "DX"  # Developer experience issues

    # General issues
    BUG = "BUG"  # Actual bugs, incorrect behavior
    EXECUTION_ERROR = "EXECUTION_ERROR"  # Generic runtime failures


class ProposalRisk(str, Enum):
    """Risk levels for improvement proposals."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


@dataclass
class ObservedIssue:
    """A single observed issue from the Observer agent."""

    type: str  # IssueType value
    location: str  # file:line or component
    description: str
    severity: str  # IssueSeverity value
    evidence: str  # log excerpt or observation

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ObservedIssue":
        return cls(
            type=data.get("type", IssueType.UX_FRICTION.value),
            location=data.get("location", "unknown"),
            description=data.get("description", ""),
            severity=data.get("severity", IssueSeverity.LOW.value),
            evidence=data.get("evidence", ""),
        )

    def is_valid(self) -> bool:
        """Check if the issue has all required fields populated."""
        return bool(self.type and self.location and self.description and self.severity)


@dataclass
class ObserverOutput:
    """Structured output from the Observer agent.

    This schema replaces free-text observations with a strict format
    that the ImprovementAgent can consume automatically.
    """

    issues: list[ObservedIssue] = field(default_factory=list)
    cycle_id: Optional[str] = None
    observation_summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "issues": [issue.to_dict() for issue in self.issues],
            "cycle_id": self.cycle_id,
            "observation_summary": self.observation_summary,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ObserverOutput":
        issues = [ObservedIssue.from_dict(issue) for issue in data.get("issues", [])]
        return cls(
            issues=issues,
            cycle_id=data.get("cycle_id"),
            observation_summary=data.get("observation_summary", ""),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "ObserverOutput":
        try:
            data = json.loads(json_str)
            return cls.from_dict(data)
        except json.JSONDecodeError:
            return cls()

    @classmethod
    def parse_from_response(cls, response_content: str) -> "ObserverOutput":
        """Parse ObserverOutput from agent response content.

        Attempts to extract JSON from the response. If the response
        contains a JSON block (```json ... ```), extracts from that.
        Falls back to attempting direct JSON parse.

        Args:
            response_content: Raw response from Observer agent.

        Returns:
            ObserverOutput (possibly empty if parsing fails).
        """
        content = response_content.strip()

        if "```json" in content:
            start = content.find("```json") + 7
            end = content.find("```", start)
            if end > start:
                content = content[start:end].strip()
        elif "```" in content:
            start = content.find("```") + 3
            end = content.find("```", start)
            if end > start:
                content = content[start:end].strip()

        if content.startswith("{"):
            brace_count = 0
            end_pos = 0
            for i, char in enumerate(content):
                if char == "{":
                    brace_count += 1
                elif char == "}":
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = i + 1
                        break
            if end_pos > 0:
                content = content[:end_pos]

        try:
            return cls.from_json(content)
        except Exception:
            return cls()

    def has_actionable_issues(self) -> bool:
        """Check if there are any actionable issues."""
        return len(self.issues) > 0 and any(issue.is_valid() for issue in self.issues)

    def get_high_severity_issues(self) -> list[ObservedIssue]:
        """Get issues with HIGH severity."""
        return [issue for issue in self.issues if issue.severity == IssueSeverity.HIGH.value]


@dataclass
class ProposalChange:
    """A single file change in an improvement proposal."""

    file: str
    before: str
    after: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProposalChange":
        return cls(
            file=data.get("file", ""),
            before=data.get("before", ""),
            after=data.get("after", ""),
        )

    def is_valid(self) -> bool:
        """Check if the change has all required fields."""
        return bool(self.file and self.before is not None and self.after is not None)


@dataclass
class ProposalImpact:
    """Impact assessment for an improvement proposal."""

    risk: str  # ProposalRisk value
    expected_benefit: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProposalImpact":
        return cls(
            risk=data.get("risk", ProposalRisk.LOW.value),
            expected_benefit=data.get("expected_benefit", ""),
        )


@dataclass
class ImprovementProposal:
    """Structured improvement proposal from the Improvement agent.

    This schema enforces a contract that the Reviewer can validate.
    Empty or vague proposals are explicitly represented.
    """

    proposal: str = ""  # "NONE" if no actionable change
    title: str = ""
    rationale: str = ""
    changes: list[ProposalChange] = field(default_factory=list)
    impact: Optional[ProposalImpact] = None
    reason: str = ""  # Used when proposal is "NONE"
    based_on_issues: list[str] = field(default_factory=list)  # Issue types addressed

    def to_dict(self) -> dict[str, Any]:
        result = {
            "proposal": self.proposal,
            "title": self.title,
            "rationale": self.rationale,
            "changes": [change.to_dict() for change in self.changes],
            "reason": self.reason,
            "based_on_issues": self.based_on_issues,
        }
        if self.impact:
            result["impact"] = self.impact.to_dict()
        return result

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ImprovementProposal":
        changes = [ProposalChange.from_dict(change) for change in data.get("changes", [])]
        impact_data = data.get("impact")
        impact = ProposalImpact.from_dict(impact_data) if impact_data else None

        return cls(
            proposal=data.get("proposal", ""),
            title=data.get("title", ""),
            rationale=data.get("rationale", ""),
            changes=changes,
            impact=impact,
            reason=data.get("reason", ""),
            based_on_issues=data.get("based_on_issues", []),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "ImprovementProposal":
        try:
            data = json.loads(json_str)
            return cls.from_dict(data)
        except json.JSONDecodeError:
            return cls()

    @classmethod
    def parse_from_response(cls, response_content: str) -> "ImprovementProposal":
        """Parse ImprovementProposal from agent response content.

        Args:
            response_content: Raw response from Improvement agent.

        Returns:
            ImprovementProposal (possibly empty if parsing fails).
        """
        content = response_content.strip()

        if "```json" in content:
            start = content.find("```json") + 7
            end = content.find("```", start)
            if end > start:
                content = content[start:end].strip()
        elif "```" in content:
            start = content.find("```") + 3
            end = content.find("```", start)
            if end > start:
                content = content[start:end].strip()

        if content.startswith("{"):
            brace_count = 0
            end_pos = 0
            for i, char in enumerate(content):
                if char == "{":
                    brace_count += 1
                elif char == "}":
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = i + 1
                        break
            if end_pos > 0:
                content = content[:end_pos]

        try:
            return cls.from_json(content)
        except Exception:
            return cls()

    @classmethod
    def none_proposal(cls, reason: str) -> "ImprovementProposal":
        """Create a NONE proposal with explicit reason.

        Use this when no actionable improvement is justified.
        """
        return cls(
            proposal="NONE",
            reason=reason,
        )

    def is_none(self) -> bool:
        """Check if this is a NONE proposal (no actionable change)."""
        return self.proposal.upper() == "NONE"

    def is_valid(self) -> bool:
        """Check if the proposal meets the contract requirements.

        A valid proposal either:
        1. Is a NONE proposal with a reason, OR
        2. Has title, rationale, at least one valid change, and impact
        """
        if self.is_none():
            return bool(self.reason)

        has_title = bool(self.title)
        has_rationale = bool(self.rationale)
        has_valid_changes = len(self.changes) > 0 and all(
            change.is_valid() for change in self.changes
        )
        has_impact = self.impact is not None and bool(self.impact.risk)

        return has_title and has_rationale and has_valid_changes and has_impact

    def get_validation_errors(self) -> list[str]:
        """Get list of validation errors for the proposal.

        Returns:
            List of error messages (empty if valid).
        """
        errors = []

        if self.is_none():
            if not self.reason:
                errors.append("NONE proposal must include a reason")
            return errors

        if not self.title:
            errors.append("Missing required field: title")
        if not self.rationale:
            errors.append("Missing required field: rationale")
        if not self.changes:
            errors.append("Missing required field: changes (at least one change required)")
        else:
            for i, change in enumerate(self.changes):
                if not change.is_valid():
                    if not change.file:
                        errors.append(f"Change {i + 1}: missing file path")
                    if change.before is None:
                        errors.append(f"Change {i + 1}: missing before content")
                    if change.after is None:
                        errors.append(f"Change {i + 1}: missing after content")

        if not self.impact:
            errors.append("Missing required field: impact")
        elif not self.impact.risk:
            errors.append("Impact must include risk level")

        return errors


@dataclass
class ReviewerValidation:
    """Result of Reviewer contract validation."""

    is_contract_valid: bool
    validation_errors: list[str] = field(default_factory=list)
    auto_rejected: bool = False
    auto_reject_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def validate_proposal(cls, proposal: ImprovementProposal) -> "ReviewerValidation":
        """Validate a proposal against the contract.

        This is the mechanical enforcement that the Reviewer uses
        to auto-reject malformed proposals.

        Args:
            proposal: The proposal to validate.

        Returns:
            ReviewerValidation with results.
        """
        errors = proposal.get_validation_errors()

        if errors:
            return cls(
                is_contract_valid=False,
                validation_errors=errors,
                auto_rejected=True,
                auto_reject_reason=f"Proposal rejected: {'; '.join(errors)}",
            )

        return cls(
            is_contract_valid=True,
            validation_errors=[],
            auto_rejected=False,
        )

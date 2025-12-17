"""Review Context Enrichment for Memory-Aware Review.

Phase 5.F: Provides advisory context to ReviewerAgent without influencing decisions.

This module enriches review context with:
- Proposal Scorecard (advisory)
- Indexed file-change summary (no code)
- Golden project coverage

CRITICAL CONSTRAINTS:
- All context is READ-ONLY and ADVISORY
- Reviewer MUST explicitly ignore these for decisions
- Does NOT influence approval/rejection
- Does NOT expose raw memory, embeddings, or code
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from odibi.agents.core.indexing import IndexedFileChange
from odibi.agents.core.proposal_scorecard import ProposalScorecard
from odibi.agents.core.schemas import ImprovementProposal

logger = logging.getLogger(__name__)

ADVISORY_DISCLAIMER = (
    "⚠️ ADVISORY ONLY: This information is contextual and does not approve, "
    "reject, or prioritize changes. Reviewer decisions must be independent."
)

ADVISORY_DISCLAIMER_SHORT = "Advisory only — no impact on approval"


@dataclass
class FileChangeSummary:
    """Safe summary of a file change (no code content).

    Contains ONLY safe, human-readable information.
    Explicitly does NOT contain:
    - Full diffs
    - Raw code content
    - Before/after snapshots
    - Executable content
    """

    file_path: str
    change_type: str  # ADD / MODIFY / DELETE
    description: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "file_path": self.file_path,
            "change_type": self.change_type,
            "description": self.description,
        }

    def to_human_readable(self) -> str:
        """Generate human-readable one-line summary."""
        return f"- [{self.change_type}] `{self.file_path}`: {self.description}"

    @classmethod
    def from_proposal_change(cls, change: Any) -> "FileChangeSummary":
        """Create from a ProposalChange object.

        Extracts ONLY safe summary information.
        """
        file_path = change.file if hasattr(change, "file") else str(change.get("file", "unknown"))
        before = change.before if hasattr(change, "before") else change.get("before", "")
        after = change.after if hasattr(change, "after") else change.get("after", "")

        if not before and after:
            change_type = "ADD"
            description = "New file or content added"
        elif before and not after:
            change_type = "DELETE"
            description = "Content removed"
        else:
            change_type = "MODIFY"
            description = "Content modified"

        return cls(
            file_path=file_path,
            change_type=change_type,
            description=description,
        )

    @classmethod
    def from_indexed_file_change(cls, ifc: IndexedFileChange) -> "FileChangeSummary":
        """Create from an IndexedFileChange."""
        return cls(
            file_path=ifc.file_path,
            change_type=ifc.change_type.value,
            description=ifc.summary,
        )


@dataclass
class AdvisoryContext:
    """Advisory context for review (read-only).

    This context is for HUMAN REFERENCE ONLY.
    It does NOT affect ReviewerAgent decision logic.

    The reviewer MUST ignore this data when making approval decisions.
    """

    scorecard: ProposalScorecard | None = None
    file_summaries: list[FileChangeSummary] = field(default_factory=list)
    golden_projects: list[str] = field(default_factory=list)
    disclaimer: str = ADVISORY_DISCLAIMER

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for metadata attachment."""
        return {
            "disclaimer": self.disclaimer,
            "scorecard": self.scorecard.to_dict() if self.scorecard else None,
            "file_summaries": [fs.to_dict() for fs in self.file_summaries],
            "golden_projects": self.golden_projects,
            "is_advisory": True,
            "affects_decision": False,
        }

    def to_human_readable(self) -> str:
        """Generate human-readable summary for reports."""
        lines = [
            "## Advisory Context",
            "",
            f"_{self.disclaimer}_",
            "",
        ]

        if self.scorecard:
            lines.append(self.scorecard.to_human_readable())
            lines.append("")

        if self.file_summaries:
            lines.append("### Files Affected (Summary)")
            lines.append("")
            lines.append(f"_{ADVISORY_DISCLAIMER_SHORT}_")
            lines.append("")
            for fs in self.file_summaries:
                lines.append(fs.to_human_readable())
            lines.append("")

        if self.golden_projects:
            lines.append("### Golden Project Coverage")
            lines.append("")
            lines.append(f"Changes will be tested against: {', '.join(self.golden_projects)}")
            lines.append("")

        return "\n".join(lines)

    @classmethod
    def empty(cls) -> "AdvisoryContext":
        """Create an empty advisory context."""
        return cls()


class ReviewContextEnricher:
    """Enriches review context with advisory information.

    This class adds contextual information to review metadata
    WITHOUT influencing the reviewer's decision logic.

    USAGE:
        enricher = ReviewContextEnricher()
        advisory = enricher.build_advisory_context(
            proposal=proposal,
            scorecard=scorecard,
            golden_project_names=["smoke_test"],
        )
        metadata["advisory_context"] = advisory.to_dict()

    CRITICAL: The ReviewerAgent MUST NOT use advisory_context
    for approval/rejection decisions. It is for human reference only.
    """

    def build_advisory_context(
        self,
        proposal: ImprovementProposal | None = None,
        scorecard: ProposalScorecard | None = None,
        golden_project_names: list[str] | None = None,
    ) -> AdvisoryContext:
        """Build advisory context from available data.

        Args:
            proposal: The improvement proposal (for file summaries).
            scorecard: The proposal scorecard (advisory).
            golden_project_names: List of golden project names.

        Returns:
            AdvisoryContext with all available information.
        """
        file_summaries = []

        if proposal and not proposal.is_none() and proposal.changes:
            for change in proposal.changes:
                fs = FileChangeSummary.from_proposal_change(change)
                file_summaries.append(fs)

        return AdvisoryContext(
            scorecard=scorecard,
            file_summaries=file_summaries,
            golden_projects=golden_project_names or [],
        )

    def build_from_dict(
        self,
        proposal_dict: dict[str, Any] | None = None,
        scorecard: ProposalScorecard | None = None,
        golden_project_names: list[str] | None = None,
    ) -> AdvisoryContext:
        """Build advisory context from dictionary data.

        Convenience method for integration with existing metadata.
        """
        proposal = None
        if proposal_dict:
            proposal = ImprovementProposal.from_dict(proposal_dict)

        return self.build_advisory_context(
            proposal=proposal,
            scorecard=scorecard,
            golden_project_names=golden_project_names,
        )


def validate_advisory_context_is_read_only(context: AdvisoryContext) -> bool:
    """Validate that advisory context contains no decision-making data.

    This is a runtime check to ensure advisory context
    does not accidentally include approval/rejection signals.

    Returns:
        True if context is safe (advisory only).
    """
    if context.scorecard:
        if hasattr(context.scorecard, "approve"):
            return False
        if hasattr(context.scorecard, "reject"):
            return False
        if hasattr(context.scorecard, "should_approve"):
            return False

    return True

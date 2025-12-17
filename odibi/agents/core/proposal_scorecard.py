from __future__ import annotations

"""Memory-Informed Proposal Scorecard.

Phase 5.E: Advisory scoring for improvement proposals using indexed memory.

This module generates a ProposalScorecard that evaluates improvement proposals
using indexed memory WITHOUT influencing control flow or decision making.

CRITICAL CONSTRAINTS:
- This is ADVISORY ONLY - it does not approve, reject, or apply changes
- It does not alter review gating logic
- All memory queries MUST use allowed intents only:
  - PATTERN_LOOKUP
  - RISK_ASSESSMENT
  - PRECEDENT_SUMMARY
  - CONVERGENCE_SIGNAL
- Output is descriptive, not prescriptive
- Does NOT influence execution flow
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from odibi.agents.core.embeddings import BaseEmbedder, get_default_embedder
from odibi.agents.core.memory_guardrails import (
    MemoryQueryIntent,
    MemoryQueryResult,
    MemoryQueryValidator,
)
from odibi.agents.core.schemas import ImprovementProposal
from odibi.agents.core.vector_store import BaseVectorStore

logger = logging.getLogger(__name__)


class ScoreLevel(str, Enum):
    """Level indicators for scorecard dimensions.

    These are informational labels, not approval/rejection signals.
    """

    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    UNKNOWN = "UNKNOWN"


@dataclass
class ScorecardDimension:
    """A single dimension of the proposal scorecard.

    Each dimension provides context but does NOT influence decisions.
    """

    level: ScoreLevel
    evidence: str
    source_cycle_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "level": self.level.value,
            "evidence": self.evidence,
            "source_cycle_ids": self.source_cycle_ids,
        }

    def to_human_readable(self, dimension_name: str) -> str:
        """Generate human-readable summary for this dimension."""
        cycle_refs = ""
        if self.source_cycle_ids:
            cycle_refs = f" (from cycles: {', '.join(self.source_cycle_ids[:3])})"
        return f"**{dimension_name}:** [{self.level.value}] {self.evidence}{cycle_refs}"


@dataclass
class GoldenProjectCoverage:
    """Coverage information for golden projects.

    Indicates which golden projects exercise files touched by the proposal.
    """

    projects: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"projects": self.projects}

    def to_human_readable(self) -> str:
        if not self.projects:
            return "**Golden Coverage:** No golden projects cover the changed files"
        return f"**Golden Coverage:** Changes are exercised by: {', '.join(self.projects)}"


@dataclass
class ProposalScorecard:
    """Memory-informed scorecard for an improvement proposal.

    This scorecard provides ADVISORY context only. It:
    - Does NOT approve or reject proposals
    - Does NOT rank proposals for execution
    - Does NOT modify agent decision logic
    - Does NOT suggest what to change next
    - Does NOT bypass reviewer
    - Does NOT influence execution flow

    USAGE:
        generator = ScorecardGenerator(vector_store, embedder)
        scorecard = generator.generate(proposal, config)
        # Attach to report and review context (read-only)
    """

    similarity: ScorecardDimension
    risk: ScorecardDimension
    golden_coverage: GoldenProjectCoverage
    novelty: ScorecardDimension
    generated_at: str = ""
    proposal_title: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to structured dictionary for tooling."""
        return {
            "proposal_title": self.proposal_title,
            "generated_at": self.generated_at,
            "similarity": self.similarity.to_dict(),
            "risk": self.risk.to_dict(),
            "golden_coverage": self.golden_coverage.to_dict(),
            "novelty": self.novelty.to_dict(),
        }

    def to_human_readable(self) -> str:
        """Generate human-readable summary for reports.

        This summary is for human review only.
        """
        lines = [
            "### Proposal Scorecard (Advisory)",
            "",
            "_This scorecard provides context only. It does not approve, reject, or influence execution._",
            "",
            self.similarity.to_human_readable("Similarity"),
            self.risk.to_human_readable("Historical Risk"),
            self.golden_coverage.to_human_readable(),
            self.novelty.to_human_readable("Novelty"),
        ]
        return "\n".join(lines)

    @classmethod
    def empty(cls, reason: str = "No proposal to score") -> "ProposalScorecard":
        """Create an empty scorecard when scoring is not possible."""
        unknown = ScorecardDimension(
            level=ScoreLevel.UNKNOWN,
            evidence=reason,
        )
        return cls(
            similarity=unknown,
            risk=unknown,
            golden_coverage=GoldenProjectCoverage(),
            novelty=unknown,
        )


class ScorecardGenerator:
    """Generates advisory scorecards for improvement proposals.

    Uses indexed memory with ALLOWED INTENTS ONLY:
    - PATTERN_LOOKUP
    - RISK_ASSESSMENT
    - PRECEDENT_SUMMARY
    - CONVERGENCE_SIGNAL

    Does NOT:
    - Decide actions
    - Suggest files to modify
    - Bypass review
    - Influence execution flow
    """

    def __init__(
        self,
        vector_store: BaseVectorStore | None = None,
        embedder: BaseEmbedder | None = None,
    ):
        """Initialize the scorecard generator.

        Args:
            vector_store: Vector store for memory queries.
            embedder: Embedder for query embedding.
        """
        self._vector_store = vector_store
        self._embedder = embedder
        self._query_validator = MemoryQueryValidator()

    @property
    def vector_store(self) -> BaseVectorStore | None:
        """Return vector store (may be None if not configured)."""
        return self._vector_store

    @property
    def embedder(self) -> BaseEmbedder:
        """Lazy-load embedder."""
        if self._embedder is None:
            self._embedder = get_default_embedder()
        return self._embedder

    def _execute_memory_query(
        self,
        query_text: str,
        intent: MemoryQueryIntent,
        filters: dict[str, Any] | None = None,
        top_k: int = 5,
    ) -> MemoryQueryResult:
        """Execute a validated memory query.

        All queries MUST go through the validator.

        Args:
            query_text: The query text.
            intent: The explicit allowed intent.
            filters: Optional metadata filters.
            top_k: Number of results.

        Returns:
            MemoryQueryResult with results.
        """
        query = self._query_validator.validate_and_create(
            query_text=query_text,
            intent=intent,
            filters=filters,
            top_k=top_k,
        )

        if self._vector_store is None:
            return MemoryQueryResult(query=query, results=[], result_count=0)

        try:
            query_embedding = self.embedder.embed_texts([query.query_text])[0]
            results = self._vector_store.similarity_search(
                query_embedding=query_embedding,
                k=query.top_k,
                filters=query.filters,
            )
            return MemoryQueryResult(
                query=query,
                results=results,
                result_count=len(results),
            )
        except Exception as e:
            logger.warning(f"Memory query failed: {e}")
            return MemoryQueryResult(query=query, results=[], result_count=0)

    def _score_similarity(self, proposal: ImprovementProposal) -> ScorecardDimension:
        """Score proposal similarity to previously approved improvements.

        Uses PATTERN_LOOKUP intent.

        Args:
            proposal: The improvement proposal.

        Returns:
            ScorecardDimension with similarity assessment.
        """
        if proposal.is_none():
            return ScorecardDimension(
                level=ScoreLevel.UNKNOWN,
                evidence="No proposal to compare",
            )

        query_text = (
            f"Previously approved improvements similar to: {proposal.title}. {proposal.rationale}"
        )
        result = self._execute_memory_query(
            query_text=query_text,
            intent=MemoryQueryIntent.PATTERN_LOOKUP,
            filters={"has_improvements": True},
            top_k=5,
        )

        if result.result_count == 0:
            return ScorecardDimension(
                level=ScoreLevel.LOW,
                evidence="No similar approved improvements found in memory",
            )

        cycle_ids = [r.get("cycle_id", "unknown") for r in result.results if r.get("cycle_id")]

        if result.result_count >= 3:
            level = ScoreLevel.HIGH
            evidence = (
                f"This proposal resembles {result.result_count} previously approved improvements"
            )
        elif result.result_count >= 1:
            level = ScoreLevel.MEDIUM
            evidence = f"Found {result.result_count} similar approved improvement(s)"
        else:
            level = ScoreLevel.LOW
            evidence = "Limited similarity to past improvements"

        return ScorecardDimension(
            level=level,
            evidence=evidence,
            source_cycle_ids=cycle_ids[:5],
        )

    def _score_risk(self, proposal: ImprovementProposal) -> ScorecardDimension:
        """Assess historical risk for files/areas touched by proposal.

        Uses RISK_ASSESSMENT intent.

        Args:
            proposal: The improvement proposal.

        Returns:
            ScorecardDimension with risk assessment.
        """
        if proposal.is_none() or not proposal.changes:
            return ScorecardDimension(
                level=ScoreLevel.UNKNOWN,
                evidence="No changes to assess risk for",
            )

        file_paths = [change.file for change in proposal.changes if change.file]
        if not file_paths:
            return ScorecardDimension(
                level=ScoreLevel.UNKNOWN,
                evidence="No file paths to assess",
            )

        query_text = f"Have changes to these files caused regressions: {', '.join(file_paths)}"
        result = self._execute_memory_query(
            query_text=query_text,
            intent=MemoryQueryIntent.RISK_ASSESSMENT,
            filters={"has_regressions": True},
            top_k=10,
        )

        if result.result_count == 0:
            return ScorecardDimension(
                level=ScoreLevel.LOW,
                evidence="No regressions found in memory for these files",
            )

        cycle_ids = [r.get("cycle_id", "unknown") for r in result.results if r.get("cycle_id")]
        regression_count = len([r for r in result.results if r.get("has_regressions")])

        if regression_count >= 3:
            level = ScoreLevel.HIGH
            evidence = f"Changes in this area caused regressions in {regression_count} past cycles"
        elif regression_count >= 1:
            level = ScoreLevel.MEDIUM
            evidence = f"Found {regression_count} past regression(s) involving similar changes"
        else:
            level = ScoreLevel.LOW
            evidence = "Low historical risk based on memory"

        return ScorecardDimension(
            level=level,
            evidence=evidence,
            source_cycle_ids=cycle_ids[:5],
        )

    def _score_golden_coverage(
        self,
        proposal: ImprovementProposal,
        golden_project_names: list[str],
    ) -> GoldenProjectCoverage:
        """Determine which golden projects exercise changed files.

        This is informational only - uses cycle config, not memory queries.

        Args:
            proposal: The improvement proposal.
            golden_project_names: List of configured golden project names.

        Returns:
            GoldenProjectCoverage with project list.
        """
        if proposal.is_none() or not proposal.changes:
            return GoldenProjectCoverage()

        return GoldenProjectCoverage(projects=golden_project_names)

    def _score_novelty(self, proposal: ImprovementProposal) -> ScorecardDimension:
        """Assess how novel this proposal is compared to past changes.

        Uses PRECEDENT_SUMMARY intent.

        Args:
            proposal: The improvement proposal.

        Returns:
            ScorecardDimension with novelty assessment.
        """
        if proposal.is_none():
            return ScorecardDimension(
                level=ScoreLevel.UNKNOWN,
                evidence="No proposal to assess novelty",
            )

        query_text = f"Precedents for change: {proposal.title}. {proposal.rationale}"
        result = self._execute_memory_query(
            query_text=query_text,
            intent=MemoryQueryIntent.PRECEDENT_SUMMARY,
            top_k=5,
        )

        cycle_ids = [r.get("cycle_id", "unknown") for r in result.results if r.get("cycle_id")]

        if result.result_count == 0:
            return ScorecardDimension(
                level=ScoreLevel.HIGH,
                evidence="No similar change has been indexed before (novel improvement)",
            )
        elif result.result_count <= 2:
            return ScorecardDimension(
                level=ScoreLevel.MEDIUM,
                evidence=f"Limited precedent: only {result.result_count} similar change(s) found",
                source_cycle_ids=cycle_ids,
            )
        else:
            return ScorecardDimension(
                level=ScoreLevel.LOW,
                evidence=f"Well-established pattern: {result.result_count} similar changes indexed",
                source_cycle_ids=cycle_ids,
            )

    def generate(
        self,
        proposal: ImprovementProposal,
        golden_project_names: list[str] | None = None,
    ) -> ProposalScorecard:
        """Generate a proposal scorecard.

        This is ADVISORY ONLY - does not influence execution.

        Args:
            proposal: The improvement proposal to score.
            golden_project_names: List of golden project names (for coverage info).

        Returns:
            ProposalScorecard with advisory scores.
        """
        from datetime import datetime

        if proposal.is_none():
            scorecard = ProposalScorecard.empty("Proposal is NONE - no changes to score")
            scorecard.generated_at = datetime.now().isoformat()
            return scorecard

        golden_names = golden_project_names or []

        scorecard = ProposalScorecard(
            similarity=self._score_similarity(proposal),
            risk=self._score_risk(proposal),
            golden_coverage=self._score_golden_coverage(proposal, golden_names),
            novelty=self._score_novelty(proposal),
            generated_at=datetime.now().isoformat(),
            proposal_title=proposal.title,
        )

        return scorecard

    def generate_from_dict(
        self,
        proposal_dict: dict[str, Any],
        golden_project_names: list[str] | None = None,
    ) -> ProposalScorecard:
        """Generate a scorecard from a proposal dictionary.

        Convenience method for integration with existing metadata.

        Args:
            proposal_dict: Dictionary representation of proposal.
            golden_project_names: List of golden project names.

        Returns:
            ProposalScorecard with advisory scores.
        """
        proposal = ImprovementProposal.from_dict(proposal_dict)
        return self.generate(proposal, golden_project_names)

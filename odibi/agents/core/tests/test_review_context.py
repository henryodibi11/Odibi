"""Tests for Review Context Enrichment.

Phase 5.F: Advisory context for review.
"""

import pytest

from odibi.agents.core.review_context import (
    ADVISORY_DISCLAIMER,
    ADVISORY_DISCLAIMER_SHORT,
    AdvisoryContext,
    FileChangeSummary,
    ReviewContextEnricher,
    validate_advisory_context_is_read_only,
)
from odibi.agents.core.proposal_scorecard import (
    GoldenProjectCoverage,
    ProposalScorecard,
    ScoreLevel,
    ScorecardDimension,
)
from odibi.agents.core.schemas import (
    ImprovementProposal,
    ProposalChange,
    ProposalImpact,
)


class TestFileChangeSummary:
    """Tests for FileChangeSummary dataclass."""

    def test_to_dict(self):
        """Test dictionary conversion."""
        fs = FileChangeSummary(
            file_path="src/main.py",
            change_type="MODIFY",
            description="Content modified",
        )
        data = fs.to_dict()

        assert data["file_path"] == "src/main.py"
        assert data["change_type"] == "MODIFY"
        assert data["description"] == "Content modified"

    def test_to_human_readable(self):
        """Test human-readable output."""
        fs = FileChangeSummary(
            file_path="src/main.py",
            change_type="MODIFY",
            description="Content modified",
        )
        readable = fs.to_human_readable()

        assert "[MODIFY]" in readable
        assert "`src/main.py`" in readable
        assert "Content modified" in readable

    def test_from_proposal_change_add(self):
        """Test creation from add change."""
        change = ProposalChange(file="new_file.py", before="", after="content")
        fs = FileChangeSummary.from_proposal_change(change)

        assert fs.change_type == "ADD"
        assert fs.file_path == "new_file.py"

    def test_from_proposal_change_delete(self):
        """Test creation from delete change."""
        change = ProposalChange(file="old_file.py", before="content", after="")
        fs = FileChangeSummary.from_proposal_change(change)

        assert fs.change_type == "DELETE"

    def test_from_proposal_change_modify(self):
        """Test creation from modify change."""
        change = ProposalChange(file="file.py", before="old", after="new")
        fs = FileChangeSummary.from_proposal_change(change)

        assert fs.change_type == "MODIFY"

    def test_from_dict_input(self):
        """Test creation from dictionary input."""
        change_dict = {"file": "test.py", "before": "a", "after": "b"}
        fs = FileChangeSummary.from_proposal_change(change_dict)

        assert fs.file_path == "test.py"
        assert fs.change_type == "MODIFY"


class TestAdvisoryContext:
    """Tests for AdvisoryContext dataclass."""

    @pytest.fixture
    def sample_scorecard(self):
        """Create a sample scorecard."""
        return ProposalScorecard(
            similarity=ScorecardDimension(ScoreLevel.HIGH, "Test similarity"),
            risk=ScorecardDimension(ScoreLevel.LOW, "Test risk"),
            golden_coverage=GoldenProjectCoverage(["smoke_test"]),
            novelty=ScorecardDimension(ScoreLevel.MEDIUM, "Test novelty"),
        )

    def test_to_dict_includes_advisory_flags(self):
        """Test that dictionary includes advisory flags."""
        context = AdvisoryContext()
        data = context.to_dict()

        assert data["is_advisory"] is True
        assert data["affects_decision"] is False
        assert "disclaimer" in data

    def test_to_dict_with_scorecard(self, sample_scorecard):
        """Test dictionary with scorecard attached."""
        context = AdvisoryContext(scorecard=sample_scorecard)
        data = context.to_dict()

        assert data["scorecard"] is not None
        assert data["scorecard"]["similarity"]["level"] == "HIGH"

    def test_to_dict_with_file_summaries(self):
        """Test dictionary with file summaries."""
        context = AdvisoryContext(
            file_summaries=[
                FileChangeSummary("a.py", "MODIFY", "Modified"),
                FileChangeSummary("b.py", "ADD", "Added"),
            ]
        )
        data = context.to_dict()

        assert len(data["file_summaries"]) == 2

    def test_to_human_readable_contains_disclaimer(self, sample_scorecard):
        """Test human-readable output contains disclaimer."""
        context = AdvisoryContext(scorecard=sample_scorecard)
        readable = context.to_human_readable()

        assert "ADVISORY" in readable.upper() or "advisory" in readable.lower()

    def test_to_human_readable_with_files(self):
        """Test human-readable output with files."""
        context = AdvisoryContext(file_summaries=[FileChangeSummary("test.py", "MODIFY", "Test")])
        readable = context.to_human_readable()

        assert "Files Affected" in readable
        assert "test.py" in readable

    def test_empty_context(self):
        """Test creating empty context."""
        context = AdvisoryContext.empty()

        assert context.scorecard is None
        assert context.file_summaries == []
        assert context.golden_projects == []


class TestReviewContextEnricher:
    """Tests for ReviewContextEnricher."""

    @pytest.fixture
    def enricher(self):
        return ReviewContextEnricher()

    @pytest.fixture
    def sample_proposal(self):
        return ImprovementProposal(
            proposal="FIX",
            title="Test fix",
            rationale="Test rationale",
            changes=[
                ProposalChange("src/a.py", "old", "new"),
                ProposalChange("src/b.py", "", "new content"),
            ],
            impact=ProposalImpact(risk="LOW", expected_benefit="Test"),
        )

    def test_build_advisory_context_basic(self, enricher):
        """Test building basic advisory context."""
        context = enricher.build_advisory_context()

        assert isinstance(context, AdvisoryContext)
        assert context.scorecard is None
        assert context.file_summaries == []

    def test_build_advisory_context_with_proposal(self, enricher, sample_proposal):
        """Test building context with proposal."""
        context = enricher.build_advisory_context(proposal=sample_proposal)

        assert len(context.file_summaries) == 2
        assert context.file_summaries[0].file_path == "src/a.py"

    def test_build_advisory_context_with_scorecard(self, enricher):
        """Test building context with scorecard."""
        scorecard = ProposalScorecard.empty("test")
        context = enricher.build_advisory_context(scorecard=scorecard)

        assert context.scorecard is scorecard

    def test_build_advisory_context_with_golden_projects(self, enricher):
        """Test building context with golden projects."""
        context = enricher.build_advisory_context(
            golden_project_names=["smoke_test", "integration_test"]
        )

        assert "smoke_test" in context.golden_projects
        assert "integration_test" in context.golden_projects

    def test_build_from_dict(self, enricher):
        """Test building from dictionary."""
        proposal_dict = {
            "proposal": "FIX",
            "title": "Test",
            "rationale": "Test",
            "changes": [{"file": "x.py", "before": "a", "after": "b"}],
            "impact": {"risk": "LOW", "expected_benefit": "Test"},
        }

        context = enricher.build_from_dict(proposal_dict)

        assert len(context.file_summaries) == 1

    def test_none_proposal_produces_empty_files(self, enricher):
        """Test that NONE proposal produces no file summaries."""
        proposal = ImprovementProposal.none_proposal("No issues")
        context = enricher.build_advisory_context(proposal=proposal)

        assert context.file_summaries == []


class TestAdvisoryValidation:
    """Tests for advisory context validation."""

    def test_validate_read_only_returns_true_for_valid(self):
        """Test that valid advisory context passes validation."""
        context = AdvisoryContext()
        assert validate_advisory_context_is_read_only(context) is True

    def test_validate_read_only_with_scorecard(self):
        """Test validation with scorecard attached."""
        scorecard = ProposalScorecard.empty("test")
        context = AdvisoryContext(scorecard=scorecard)

        assert validate_advisory_context_is_read_only(context) is True


class TestAdvisoryOnlyConstraints:
    """Tests to verify advisory context remains advisory."""

    def test_advisory_context_has_no_decision_methods(self):
        """Verify AdvisoryContext has no approval/rejection methods."""
        context = AdvisoryContext()

        assert not hasattr(context, "approve")
        assert not hasattr(context, "reject")
        assert not hasattr(context, "should_approve")
        assert not hasattr(context, "decide")
        assert not hasattr(context, "apply")

    def test_enricher_has_no_decision_methods(self):
        """Verify ReviewContextEnricher has no decision methods."""
        enricher = ReviewContextEnricher()

        assert not hasattr(enricher, "approve")
        assert not hasattr(enricher, "reject")
        assert not hasattr(enricher, "decide")
        assert not hasattr(enricher, "rank")

    def test_to_dict_contains_explicit_non_decision_flags(self):
        """Verify to_dict explicitly marks as non-decision."""
        context = AdvisoryContext()
        data = context.to_dict()

        assert data["is_advisory"] is True
        assert data["affects_decision"] is False

    def test_disclaimer_constants_exist(self):
        """Verify disclaimer constants are defined."""
        assert ADVISORY_DISCLAIMER
        assert ADVISORY_DISCLAIMER_SHORT
        assert "approve" in ADVISORY_DISCLAIMER.lower() or "ADVISORY" in ADVISORY_DISCLAIMER.upper()


class TestMissingMemoryHandling:
    """Tests for graceful handling when memory is unavailable."""

    def test_advisory_context_works_without_scorecard(self):
        """Test advisory context works without scorecard."""
        context = AdvisoryContext(
            file_summaries=[FileChangeSummary("test.py", "MODIFY", "Test")],
            golden_projects=["smoke_test"],
        )

        data = context.to_dict()
        assert data["scorecard"] is None
        assert len(data["file_summaries"]) == 1

    def test_enricher_works_without_memory(self):
        """Test enricher works when no memory/scorecard is provided."""
        enricher = ReviewContextEnricher()
        context = enricher.build_advisory_context()

        assert context is not None
        readable = context.to_human_readable()
        assert "## Advisory Context" in readable

    def test_human_readable_graceful_with_empty_data(self):
        """Test human-readable output is graceful with empty data."""
        context = AdvisoryContext.empty()
        readable = context.to_human_readable()

        assert "## Advisory Context" in readable

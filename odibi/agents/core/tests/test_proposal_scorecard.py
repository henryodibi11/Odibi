"""Tests for Memory-Informed Proposal Scorecard.

Phase 5.E: Advisory scoring for improvement proposals.
"""

import pytest
from unittest.mock import MagicMock, patch

from odibi.agents.core.proposal_scorecard import (
    GoldenProjectCoverage,
    ProposalScorecard,
    ScoreLevel,
    ScorecardDimension,
    ScorecardGenerator,
)
from odibi.agents.core.schemas import (
    ImprovementProposal,
    ProposalChange,
    ProposalImpact,
)
from odibi.agents.core.memory_guardrails import MemoryQueryIntent


class TestScoreLevel:
    """Tests for ScoreLevel enum."""

    def test_all_levels_defined(self):
        """Verify all required levels exist."""
        required_levels = ["HIGH", "MEDIUM", "LOW", "UNKNOWN"]
        for level_name in required_levels:
            assert hasattr(ScoreLevel, level_name)

    def test_level_values(self):
        """Test level enum values."""
        assert ScoreLevel.HIGH.value == "HIGH"
        assert ScoreLevel.MEDIUM.value == "MEDIUM"
        assert ScoreLevel.LOW.value == "LOW"
        assert ScoreLevel.UNKNOWN.value == "UNKNOWN"


class TestScorecardDimension:
    """Tests for ScorecardDimension dataclass."""

    def test_to_dict(self):
        """Test dictionary conversion."""
        dimension = ScorecardDimension(
            level=ScoreLevel.HIGH,
            evidence="Test evidence",
            source_cycle_ids=["cycle-1", "cycle-2"],
        )
        data = dimension.to_dict()

        assert data["level"] == "HIGH"
        assert data["evidence"] == "Test evidence"
        assert data["source_cycle_ids"] == ["cycle-1", "cycle-2"]

    def test_to_human_readable_with_cycles(self):
        """Test human-readable output with cycle references."""
        dimension = ScorecardDimension(
            level=ScoreLevel.HIGH,
            evidence="3 similar improvements found",
            source_cycle_ids=["cycle-1", "cycle-2"],
        )
        readable = dimension.to_human_readable("Similarity")

        assert "**Similarity:**" in readable
        assert "[HIGH]" in readable
        assert "3 similar improvements found" in readable
        assert "cycle-1" in readable

    def test_to_human_readable_without_cycles(self):
        """Test human-readable output without cycle references."""
        dimension = ScorecardDimension(
            level=ScoreLevel.LOW,
            evidence="No data available",
        )
        readable = dimension.to_human_readable("Risk")

        assert "**Risk:**" in readable
        assert "[LOW]" in readable
        assert "No data available" in readable
        assert "from cycles" not in readable


class TestGoldenProjectCoverage:
    """Tests for GoldenProjectCoverage dataclass."""

    def test_to_dict_with_projects(self):
        """Test dictionary conversion with projects."""
        coverage = GoldenProjectCoverage(projects=["smoke_test", "test_odibi_local"])
        data = coverage.to_dict()

        assert data["projects"] == ["smoke_test", "test_odibi_local"]

    def test_to_dict_empty(self):
        """Test dictionary conversion with no projects."""
        coverage = GoldenProjectCoverage()
        data = coverage.to_dict()

        assert data["projects"] == []

    def test_to_human_readable_with_projects(self):
        """Test human-readable output with projects."""
        coverage = GoldenProjectCoverage(projects=["smoke_test", "test_odibi_local"])
        readable = coverage.to_human_readable()

        assert "**Golden Coverage:**" in readable
        assert "smoke_test" in readable
        assert "test_odibi_local" in readable

    def test_to_human_readable_no_projects(self):
        """Test human-readable output with no projects."""
        coverage = GoldenProjectCoverage()
        readable = coverage.to_human_readable()

        assert "No golden projects cover" in readable


class TestProposalScorecard:
    """Tests for ProposalScorecard dataclass."""

    @pytest.fixture
    def sample_scorecard(self):
        """Create a sample scorecard for testing."""
        return ProposalScorecard(
            similarity=ScorecardDimension(
                level=ScoreLevel.HIGH,
                evidence="3 similar approved improvements",
                source_cycle_ids=["cycle-1", "cycle-2", "cycle-3"],
            ),
            risk=ScorecardDimension(
                level=ScoreLevel.LOW,
                evidence="No regressions in last 5 cycles",
            ),
            golden_coverage=GoldenProjectCoverage(projects=["smoke_test", "test_odibi_local"]),
            novelty=ScorecardDimension(
                level=ScoreLevel.MEDIUM,
                evidence="First time modifying S3 settings",
            ),
            proposal_title="Fix S3 concurrency",
            generated_at="2024-01-15T10:30:00",
        )

    def test_to_dict(self, sample_scorecard):
        """Test dictionary conversion."""
        data = sample_scorecard.to_dict()

        assert data["proposal_title"] == "Fix S3 concurrency"
        assert data["similarity"]["level"] == "HIGH"
        assert data["risk"]["level"] == "LOW"
        assert data["golden_coverage"]["projects"] == ["smoke_test", "test_odibi_local"]
        assert data["novelty"]["level"] == "MEDIUM"

    def test_to_human_readable(self, sample_scorecard):
        """Test human-readable output contains all sections."""
        readable = sample_scorecard.to_human_readable()

        assert "### Proposal Scorecard (Advisory)" in readable
        assert "does not approve, reject" in readable
        assert "**Similarity:**" in readable
        assert "**Historical Risk:**" in readable
        assert "**Golden Coverage:**" in readable
        assert "**Novelty:**" in readable

    def test_empty_scorecard(self):
        """Test creating an empty scorecard."""
        scorecard = ProposalScorecard.empty("No proposal to score")

        assert scorecard.similarity.level == ScoreLevel.UNKNOWN
        assert scorecard.risk.level == ScoreLevel.UNKNOWN
        assert scorecard.novelty.level == ScoreLevel.UNKNOWN
        assert scorecard.similarity.evidence == "No proposal to score"


class TestScorecardGenerator:
    """Tests for ScorecardGenerator."""

    @pytest.fixture
    def mock_vector_store(self):
        """Create a mock vector store."""
        store = MagicMock()
        store.similarity_search.return_value = []
        return store

    @pytest.fixture
    def mock_embedder(self):
        """Create a mock embedder."""
        embedder = MagicMock()
        embedder.embed_texts.return_value = [[0.1] * 384]
        return embedder

    @pytest.fixture
    def generator(self, mock_vector_store, mock_embedder):
        """Create a generator with mocked dependencies."""
        return ScorecardGenerator(
            vector_store=mock_vector_store,
            embedder=mock_embedder,
        )

    @pytest.fixture
    def sample_proposal(self):
        """Create a sample proposal."""
        return ImprovementProposal(
            proposal="FIX",
            title="Fix null handling in transformer",
            rationale="Null values cause pipeline failures",
            changes=[
                ProposalChange(
                    file="src/transformers/base.py",
                    before="value = data['key']",
                    after="value = data.get('key')",
                )
            ],
            impact=ProposalImpact(
                risk="LOW",
                expected_benefit="Prevents null pointer errors",
            ),
        )

    def test_generate_with_empty_memory(self, generator, sample_proposal):
        """Test scoring when memory has no data."""
        scorecard = generator.generate(sample_proposal, ["smoke_test"])

        assert scorecard.proposal_title == "Fix null handling in transformer"
        assert scorecard.similarity.level == ScoreLevel.LOW
        assert scorecard.risk.level == ScoreLevel.LOW
        assert "smoke_test" in scorecard.golden_coverage.projects

    def test_generate_with_similar_improvements(
        self, mock_vector_store, mock_embedder, sample_proposal
    ):
        """Test scoring when memory has similar improvements."""
        mock_vector_store.similarity_search.return_value = [
            {"cycle_id": "cycle-1", "has_improvements": True},
            {"cycle_id": "cycle-2", "has_improvements": True},
            {"cycle_id": "cycle-3", "has_improvements": True},
        ]

        generator = ScorecardGenerator(
            vector_store=mock_vector_store,
            embedder=mock_embedder,
        )
        scorecard = generator.generate(sample_proposal)

        assert scorecard.similarity.level == ScoreLevel.HIGH
        assert "3" in scorecard.similarity.evidence

    def test_generate_with_regressions_in_memory(
        self, mock_vector_store, mock_embedder, sample_proposal
    ):
        """Test scoring when memory shows regressions."""

        def mock_search(query_embedding, k, filters=None):
            if filters and filters.get("has_regressions"):
                return [
                    {"cycle_id": "cycle-x", "has_regressions": True},
                    {"cycle_id": "cycle-y", "has_regressions": True},
                    {"cycle_id": "cycle-z", "has_regressions": True},
                ]
            return []

        mock_vector_store.similarity_search.side_effect = mock_search

        generator = ScorecardGenerator(
            vector_store=mock_vector_store,
            embedder=mock_embedder,
        )
        scorecard = generator.generate(sample_proposal)

        assert scorecard.risk.level == ScoreLevel.HIGH

    def test_generate_none_proposal(self, generator):
        """Test scoring a NONE proposal."""
        proposal = ImprovementProposal.none_proposal("No issues found")
        scorecard = generator.generate(proposal)

        assert scorecard.similarity.level == ScoreLevel.UNKNOWN
        assert (
            "NONE" in scorecard.similarity.evidence
            or "No proposal" in scorecard.similarity.evidence
        )

    def test_generate_from_dict(self, generator):
        """Test generating from a proposal dictionary."""
        proposal_dict = {
            "proposal": "FIX",
            "title": "Test fix",
            "rationale": "Test rationale",
            "changes": [{"file": "test.py", "before": "a", "after": "b"}],
            "impact": {"risk": "LOW", "expected_benefit": "Testing"},
        }

        scorecard = generator.generate_from_dict(proposal_dict, ["test_project"])

        assert scorecard.proposal_title == "Test fix"
        assert "test_project" in scorecard.golden_coverage.projects

    def test_no_vector_store(self):
        """Test scoring without a configured vector store."""
        generator = ScorecardGenerator(vector_store=None)
        proposal = ImprovementProposal(
            proposal="FIX",
            title="Test",
            rationale="Test",
            changes=[ProposalChange(file="a.py", before="x", after="y")],
            impact=ProposalImpact(risk="LOW", expected_benefit="Test"),
        )

        scorecard = generator.generate(proposal)

        assert scorecard.similarity.level == ScoreLevel.LOW
        assert scorecard.risk.level == ScoreLevel.LOW


class TestMemoryQueryIntentCompliance:
    """Tests to verify only allowed query intents are used."""

    @pytest.fixture
    def generator(self):
        """Create a generator with mocked dependencies."""
        mock_store = MagicMock()
        mock_store.similarity_search.return_value = []
        mock_embedder = MagicMock()
        mock_embedder.embed_texts.return_value = [[0.1] * 384]
        return ScorecardGenerator(
            vector_store=mock_store,
            embedder=mock_embedder,
        )

    def test_similarity_uses_pattern_lookup(self, generator):
        """Verify similarity scoring uses PATTERN_LOOKUP intent."""
        proposal = ImprovementProposal(
            proposal="FIX",
            title="Test",
            rationale="Test",
            changes=[ProposalChange(file="a.py", before="x", after="y")],
            impact=ProposalImpact(risk="LOW", expected_benefit="Test"),
        )

        with patch.object(generator, "_execute_memory_query") as mock_query:
            mock_query.return_value = MagicMock(result_count=0, results=[])
            generator._score_similarity(proposal)

            call_args = mock_query.call_args
            assert call_args[1]["intent"] == MemoryQueryIntent.PATTERN_LOOKUP

    def test_risk_uses_risk_assessment(self, generator):
        """Verify risk scoring uses RISK_ASSESSMENT intent."""
        proposal = ImprovementProposal(
            proposal="FIX",
            title="Test",
            rationale="Test",
            changes=[ProposalChange(file="a.py", before="x", after="y")],
            impact=ProposalImpact(risk="LOW", expected_benefit="Test"),
        )

        with patch.object(generator, "_execute_memory_query") as mock_query:
            mock_query.return_value = MagicMock(result_count=0, results=[])
            generator._score_risk(proposal)

            call_args = mock_query.call_args
            assert call_args[1]["intent"] == MemoryQueryIntent.RISK_ASSESSMENT

    def test_novelty_uses_precedent_summary(self, generator):
        """Verify novelty scoring uses PRECEDENT_SUMMARY intent."""
        proposal = ImprovementProposal(
            proposal="FIX",
            title="Test",
            rationale="Test",
            changes=[ProposalChange(file="a.py", before="x", after="y")],
            impact=ProposalImpact(risk="LOW", expected_benefit="Test"),
        )

        with patch.object(generator, "_execute_memory_query") as mock_query:
            mock_query.return_value = MagicMock(result_count=0, results=[])
            generator._score_novelty(proposal)

            call_args = mock_query.call_args
            assert call_args[1]["intent"] == MemoryQueryIntent.PRECEDENT_SUMMARY


class TestConflictingSignals:
    """Tests for handling conflicting signals in memory."""

    @pytest.fixture
    def mock_embedder(self):
        """Create a mock embedder."""
        embedder = MagicMock()
        embedder.embed_texts.return_value = [[0.1] * 384]
        return embedder

    def test_conflicting_high_similarity_high_risk(self, mock_embedder):
        """Test scorecard with high similarity but high risk."""
        mock_store = MagicMock()

        def mock_search(query_embedding, k, filters=None):
            if filters and filters.get("has_improvements"):
                return [{"cycle_id": f"cycle-{i}", "has_improvements": True} for i in range(5)]
            elif filters and filters.get("has_regressions"):
                return [{"cycle_id": f"reg-{i}", "has_regressions": True} for i in range(5)]
            return []

        mock_store.similarity_search.side_effect = mock_search

        generator = ScorecardGenerator(
            vector_store=mock_store,
            embedder=mock_embedder,
        )
        proposal = ImprovementProposal(
            proposal="FIX",
            title="Risky but common fix",
            rationale="Known pattern with history",
            changes=[ProposalChange(file="a.py", before="x", after="y")],
            impact=ProposalImpact(risk="MEDIUM", expected_benefit="Test"),
        )

        scorecard = generator.generate(proposal)

        assert scorecard.similarity.level == ScoreLevel.HIGH
        assert scorecard.risk.level == ScoreLevel.HIGH

    def test_novel_change_low_risk(self, mock_embedder):
        """Test scorecard for novel change with low risk."""
        mock_store = MagicMock()
        mock_store.similarity_search.return_value = []

        generator = ScorecardGenerator(
            vector_store=mock_store,
            embedder=mock_embedder,
        )
        proposal = ImprovementProposal(
            proposal="FIX",
            title="Novel improvement",
            rationale="New approach",
            changes=[ProposalChange(file="new_file.py", before="", after="new code")],
            impact=ProposalImpact(risk="LOW", expected_benefit="Test"),
        )

        scorecard = generator.generate(proposal)

        assert scorecard.novelty.level == ScoreLevel.HIGH
        assert scorecard.risk.level == ScoreLevel.LOW


class TestScorecardIsAdvisoryOnly:
    """Tests to verify scorecard does NOT influence decisions."""

    def test_scorecard_has_no_approval_method(self):
        """Verify ProposalScorecard has no approval/rejection methods."""
        scorecard = ProposalScorecard.empty("test")

        assert not hasattr(scorecard, "approve")
        assert not hasattr(scorecard, "reject")
        assert not hasattr(scorecard, "should_approve")
        assert not hasattr(scorecard, "is_approved")
        assert not hasattr(scorecard, "decide")
        assert not hasattr(scorecard, "apply")

    def test_scorecard_output_is_read_only(self):
        """Verify scorecard output is descriptive, not prescriptive."""
        scorecard = ProposalScorecard(
            similarity=ScorecardDimension(ScoreLevel.HIGH, "test"),
            risk=ScorecardDimension(ScoreLevel.HIGH, "test"),
            golden_coverage=GoldenProjectCoverage(),
            novelty=ScorecardDimension(ScoreLevel.LOW, "test"),
        )

        readable = scorecard.to_human_readable()

        assert "approve" not in readable.lower() or "does not approve" in readable.lower()
        assert "reject" not in readable.lower() or "reject, or" in readable.lower()
        assert "should" not in readable.lower() or "should not" in readable.lower()
        assert "must" not in readable.lower()

    def test_generator_has_no_decision_methods(self):
        """Verify ScorecardGenerator has no decision-making methods."""
        generator = ScorecardGenerator()

        assert not hasattr(generator, "approve")
        assert not hasattr(generator, "reject")
        assert not hasattr(generator, "should_approve")
        assert not hasattr(generator, "decide")
        assert not hasattr(generator, "apply_change")
        assert not hasattr(generator, "rank_proposals")

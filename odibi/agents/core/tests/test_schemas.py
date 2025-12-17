"""Tests for agent communication schemas."""

from odibi.agents.core.schemas import (
    ImprovementProposal,
    IssueSeverity,
    IssueType,
    ObservedIssue,
    ObserverOutput,
    ProposalChange,
    ProposalImpact,
    ProposalRisk,
    ReviewerValidation,
)


class TestIssueTypes:
    """Tests for IssueType enum coverage."""

    def test_execution_failure_type_exists(self):
        assert IssueType.EXECUTION_FAILURE.value == "EXECUTION_FAILURE"

    def test_partial_execution_type_exists(self):
        assert IssueType.PARTIAL_EXECUTION.value == "PARTIAL_EXECUTION"

    def test_auth_error_type_exists(self):
        assert IssueType.AUTH_ERROR.value == "AUTH_ERROR"

    def test_data_quality_type_exists(self):
        assert IssueType.DATA_QUALITY.value == "DATA_QUALITY"

    def test_performance_type_exists(self):
        assert IssueType.PERFORMANCE.value == "PERFORMANCE"

    def test_all_issue_types(self):
        expected_types = {
            "EXECUTION_FAILURE",
            "PARTIAL_EXECUTION",
            "DATA_QUALITY",
            "DATA_ERROR",
            "PERFORMANCE",
            "CONFIGURATION_ERROR",
            "AUTH_ERROR",
            "UX_FRICTION",
            "DX",
            "BUG",
            "EXECUTION_ERROR",
        }
        actual_types = {t.value for t in IssueType}
        assert expected_types == actual_types


class TestObservedIssue:
    """Tests for ObservedIssue dataclass."""

    def test_valid_issue(self):
        issue = ObservedIssue(
            type=IssueType.BUG.value,
            location="core/engine.py:42",
            description="Null pointer when processing empty list",
            severity=IssueSeverity.HIGH.value,
            evidence="NoneType has no attribute 'items'",
        )
        assert issue.is_valid()

    def test_invalid_issue_missing_description(self):
        issue = ObservedIssue(
            type=IssueType.BUG.value,
            location="core/engine.py:42",
            description="",
            severity=IssueSeverity.HIGH.value,
            evidence="some evidence",
        )
        assert not issue.is_valid()

    def test_to_dict_roundtrip(self):
        issue = ObservedIssue(
            type=IssueType.UX_FRICTION.value,
            location="ui/display.py:10",
            description="Confusing error message",
            severity=IssueSeverity.MEDIUM.value,
            evidence="Error: undefined",
        )
        data = issue.to_dict()
        restored = ObservedIssue.from_dict(data)
        assert restored.type == issue.type
        assert restored.location == issue.location
        assert restored.description == issue.description


class TestObserverOutput:
    """Tests for ObserverOutput schema."""

    def test_empty_output(self):
        output = ObserverOutput()
        assert not output.has_actionable_issues()
        assert len(output.issues) == 0

    def test_with_issues(self):
        output = ObserverOutput(
            issues=[
                ObservedIssue(
                    type=IssueType.BUG.value,
                    location="test.py:1",
                    description="A bug",
                    severity=IssueSeverity.HIGH.value,
                    evidence="evidence",
                )
            ],
            observation_summary="Found one bug",
        )
        assert output.has_actionable_issues()
        assert len(output.issues) == 1

    def test_to_json_and_back(self):
        output = ObserverOutput(
            issues=[
                ObservedIssue(
                    type=IssueType.PERFORMANCE.value,
                    location="engine.py:100",
                    description="Slow query",
                    severity=IssueSeverity.MEDIUM.value,
                    evidence="10s execution time",
                )
            ],
            observation_summary="Performance issue found",
        )
        json_str = output.to_json()
        restored = ObserverOutput.from_json(json_str)
        assert len(restored.issues) == 1
        assert restored.issues[0].type == IssueType.PERFORMANCE.value

    def test_parse_from_response_with_json_block(self):
        response = """Here are my observations:

```json
{
  "issues": [
    {
      "type": "BUG",
      "location": "test.py:5",
      "description": "Found a bug",
      "severity": "HIGH",
      "evidence": "Error log"
    }
  ],
  "observation_summary": "One bug found"
}
```

End of observations."""
        output = ObserverOutput.parse_from_response(response)
        assert len(output.issues) == 1
        assert output.issues[0].type == "BUG"

    def test_parse_from_response_direct_json(self):
        response = '{"issues": [], "observation_summary": "No issues"}'
        output = ObserverOutput.parse_from_response(response)
        assert len(output.issues) == 0
        assert output.observation_summary == "No issues"

    def test_parse_from_response_invalid(self):
        response = "This is not JSON at all"
        output = ObserverOutput.parse_from_response(response)
        assert len(output.issues) == 0

    def test_get_high_severity_issues(self):
        output = ObserverOutput(
            issues=[
                ObservedIssue(
                    type="BUG",
                    location="a.py:1",
                    description="Low bug",
                    severity=IssueSeverity.LOW.value,
                    evidence="ev",
                ),
                ObservedIssue(
                    type="BUG",
                    location="b.py:2",
                    description="High bug",
                    severity=IssueSeverity.HIGH.value,
                    evidence="ev",
                ),
            ]
        )
        high_issues = output.get_high_severity_issues()
        assert len(high_issues) == 1
        assert high_issues[0].description == "High bug"

    def test_parse_execution_failure_response(self):
        """Test parsing Observer output with EXECUTION_FAILURE issues."""
        response = """```json
{
  "issues": [
    {
      "type": "EXECUTION_FAILURE",
      "location": "product_sync",
      "description": "Pipeline failed completely",
      "severity": "HIGH",
      "evidence": "Status: FAILURE"
    },
    {
      "type": "AUTH_ERROR",
      "location": "product_sync/extraction",
      "description": "API authentication failed",
      "severity": "HIGH",
      "evidence": "ERROR: API authentication failed (invalid client_secret)"
    }
  ],
  "observation_summary": "2 issues found"
}
```"""
        output = ObserverOutput.parse_from_response(response)
        assert len(output.issues) == 2
        assert output.issues[0].type == "EXECUTION_FAILURE"
        assert output.issues[1].type == "AUTH_ERROR"
        assert output.issues[0].severity == "HIGH"

    def test_parse_partial_execution_response(self):
        """Test parsing Observer output with PARTIAL_EXECUTION and warnings."""
        response = """{
  "issues": [
    {
      "type": "PARTIAL_EXECUTION",
      "location": "customer_ingestion",
      "description": "Pipeline completed with warnings",
      "severity": "MEDIUM",
      "evidence": "Status: PARTIAL"
    },
    {
      "type": "DATA_QUALITY",
      "location": "customer_ingestion/row_237",
      "description": "Missing email field filled with placeholder",
      "severity": "MEDIUM",
      "evidence": "WARNING: Row 237 missing email field"
    },
    {
      "type": "PERFORMANCE",
      "location": "customer_ingestion/dedupe_customers",
      "description": "Transform exceeded time threshold",
      "severity": "MEDIUM",
      "evidence": "WARNING: Python transform took 3.1s (threshold 2.0s)"
    }
  ],
  "observation_summary": "3 issues found"
}"""
        output = ObserverOutput.parse_from_response(response)
        assert len(output.issues) == 3
        assert output.issues[0].type == "PARTIAL_EXECUTION"
        assert output.issues[1].type == "DATA_QUALITY"
        assert output.issues[2].type == "PERFORMANCE"
        assert output.has_actionable_issues()


class TestImprovementProposal:
    """Tests for ImprovementProposal schema."""

    def test_none_proposal_valid(self):
        proposal = ImprovementProposal.none_proposal("No actionable issues observed")
        assert proposal.is_none()
        assert proposal.is_valid()

    def test_none_proposal_without_reason_invalid(self):
        proposal = ImprovementProposal(proposal="NONE", reason="")
        assert proposal.is_none()
        assert not proposal.is_valid()

    def test_valid_improvement_proposal(self):
        proposal = ImprovementProposal(
            proposal="IMPROVEMENT",
            title="Fix null handling",
            rationale="Prevents crashes on empty input",
            changes=[
                ProposalChange(
                    file="engine.py",
                    before="if data:",
                    after="if data is not None:",
                )
            ],
            impact=ProposalImpact(
                risk=ProposalRisk.LOW.value,
                expected_benefit="No more null crashes",
            ),
        )
        assert proposal.is_valid()
        assert not proposal.is_none()

    def test_invalid_proposal_missing_title(self):
        proposal = ImprovementProposal(
            proposal="IMPROVEMENT",
            title="",
            rationale="Some rationale",
            changes=[ProposalChange(file="a.py", before="x", after="y")],
            impact=ProposalImpact(risk="LOW", expected_benefit="benefit"),
        )
        assert not proposal.is_valid()
        errors = proposal.get_validation_errors()
        assert "title" in errors[0].lower()

    def test_invalid_proposal_no_changes(self):
        proposal = ImprovementProposal(
            proposal="IMPROVEMENT",
            title="Some title",
            rationale="Some rationale",
            changes=[],
            impact=ProposalImpact(risk="LOW", expected_benefit="benefit"),
        )
        assert not proposal.is_valid()
        errors = proposal.get_validation_errors()
        assert any("changes" in e.lower() for e in errors)

    def test_invalid_proposal_no_impact(self):
        proposal = ImprovementProposal(
            proposal="IMPROVEMENT",
            title="Some title",
            rationale="Some rationale",
            changes=[ProposalChange(file="a.py", before="x", after="y")],
            impact=None,
        )
        assert not proposal.is_valid()
        errors = proposal.get_validation_errors()
        assert any("impact" in e.lower() for e in errors)

    def test_parse_from_response(self):
        response = """```json
{
  "proposal": "IMPROVEMENT",
  "title": "Better error handling",
  "rationale": "Improves UX",
  "changes": [{"file": "a.py", "before": "old", "after": "new"}],
  "impact": {"risk": "LOW", "expected_benefit": "clearer errors"}
}
```"""
        proposal = ImprovementProposal.parse_from_response(response)
        assert proposal.title == "Better error handling"
        assert proposal.is_valid()

    def test_to_json_roundtrip(self):
        original = ImprovementProposal(
            proposal="IMPROVEMENT",
            title="Test",
            rationale="Testing",
            changes=[ProposalChange(file="x.py", before="a", after="b")],
            impact=ProposalImpact(risk="MEDIUM", expected_benefit="testing"),
        )
        json_str = original.to_json()
        restored = ImprovementProposal.from_json(json_str)
        assert restored.title == original.title
        assert len(restored.changes) == 1


class TestReviewerValidation:
    """Tests for ReviewerValidation."""

    def test_valid_proposal_passes(self):
        proposal = ImprovementProposal(
            proposal="IMPROVEMENT",
            title="Fix bug",
            rationale="Prevents crash",
            changes=[ProposalChange(file="a.py", before="x", after="y")],
            impact=ProposalImpact(risk="LOW", expected_benefit="stability"),
        )
        validation = ReviewerValidation.validate_proposal(proposal)
        assert validation.is_contract_valid
        assert not validation.auto_rejected

    def test_invalid_proposal_auto_rejected(self):
        proposal = ImprovementProposal(
            proposal="IMPROVEMENT",
            title="",
            rationale="",
            changes=[],
            impact=None,
        )
        validation = ReviewerValidation.validate_proposal(proposal)
        assert not validation.is_contract_valid
        assert validation.auto_rejected
        assert len(validation.validation_errors) > 0

    def test_none_proposal_with_reason_passes(self):
        proposal = ImprovementProposal.none_proposal("Nothing to fix")
        validation = ReviewerValidation.validate_proposal(proposal)
        assert validation.is_contract_valid
        assert not validation.auto_rejected

    def test_none_proposal_without_reason_rejected(self):
        proposal = ImprovementProposal(proposal="NONE", reason="")
        validation = ReviewerValidation.validate_proposal(proposal)
        assert not validation.is_contract_valid
        assert validation.auto_rejected

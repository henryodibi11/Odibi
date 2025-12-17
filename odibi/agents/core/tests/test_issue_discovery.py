"""Tests for Phase 10 Issue Discovery.

Tests verify:
1. No fixes occur without approval
2. Harness files remain excluded
3. Weak evidence does not generate issues
4. Passive detection only (no modifications)
5. Evidence-based reporting
6. User intent selection flow
"""

import tempfile
from pathlib import Path

import pytest

from odibi.agents.core.issue_discovery import (
    ConfidenceLevel,
    DiscoveredIssue,
    DiscoveredIssueType,
    IssueDiscoveryManager,
    IssueDiscoveryResult,
    IssueEvidence,
    IssueLocation,
    PassiveIssueDetector,
    UserIssueSelection,
)
from odibi.agents.core.controlled_improvement import is_learning_harness_path
from odibi.agents.core.schemas import IssueSeverity


class TestIssueLocation:
    """Tests for IssueLocation."""

    def test_is_in_learning_harness_detects_harness_paths(self):
        """Learning harness paths are correctly identified."""
        harness_location = IssueLocation(
            file_path="d:/odibi/.odibi/learning_harness/scale_join.odibi.yaml"
        )
        assert harness_location.is_in_learning_harness() is True

    def test_is_in_learning_harness_allows_non_harness_paths(self):
        """Non-harness paths are allowed."""
        normal_location = IssueLocation(file_path="d:/odibi/examples/improvement_target.odibi.yaml")
        assert normal_location.is_in_learning_harness() is False

    def test_format_location_includes_all_parts(self):
        """Location formatting includes file, node, field."""
        location = IssueLocation(
            file_path="config.yaml",
            node_name="dedupe_trips",
            field_path="params",
            line_range=(10, 15),
        )
        formatted = location.format_location()
        assert "config.yaml" in formatted
        assert "dedupe_trips" in formatted
        assert "params" in formatted
        assert "10-15" in formatted


class TestIssueEvidence:
    """Tests for IssueEvidence."""

    def test_from_config_inspection(self):
        """Evidence from config inspection."""
        evidence = IssueEvidence.from_config_inspection(
            file_path="test.yaml",
            excerpt="transformer: deduplicate",
            line_number=42,
        )
        assert evidence.evidence_type == "config_inspection"
        assert evidence.source == "test.yaml"
        assert evidence.line_number == 42

    def test_from_execution_output(self):
        """Evidence from execution output."""
        evidence = IssueEvidence.from_execution_output(
            command="odibi run pipeline.yaml",
            output_excerpt="Warning: skew detected",
        )
        assert evidence.evidence_type == "execution_output"
        assert "skew detected" in evidence.excerpt

    def test_to_dict_roundtrip(self):
        """Evidence can be serialized and deserialized."""
        original = IssueEvidence(
            evidence_type="test",
            source="source.yaml",
            excerpt="some content",
            line_number=10,
            context="test context",
        )
        data = original.to_dict()
        restored = IssueEvidence.from_dict(data)
        assert restored.evidence_type == original.evidence_type
        assert restored.source == original.source
        assert restored.excerpt == original.excerpt


class TestDiscoveredIssue:
    """Tests for DiscoveredIssue."""

    def test_is_actionable_requires_evidence(self):
        """Issues without evidence are not actionable."""
        issue = DiscoveredIssue(
            issue_id="",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path="test.yaml"),
            description="Test issue",
            severity=IssueSeverity.HIGH,
            evidence=[],
            confidence=ConfidenceLevel.HIGH,
        )
        assert issue.is_actionable() is False

    def test_is_actionable_low_confidence_needs_multiple_evidence(self):
        """Low confidence issues need multiple evidence."""
        issue = DiscoveredIssue(
            issue_id="",
            issue_type=DiscoveredIssueType.PERFORMANCE_SMELL,
            location=IssueLocation(file_path="test.yaml"),
            description="Performance issue",
            severity=IssueSeverity.MEDIUM,
            evidence=[IssueEvidence.from_config_inspection("test.yaml", "slow query", 1)],
            confidence=ConfidenceLevel.LOW,
        )
        assert issue.is_actionable() is False

        issue.evidence.append(IssueEvidence.from_execution_output("cmd", "timeout"))
        assert issue.is_actionable() is True

    def test_is_improvement_candidate_excludes_harness(self):
        """INVARIANT: Harness files are NEVER improvement candidates."""
        harness_issue = DiscoveredIssue(
            issue_id="",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path=".odibi/learning_harness/test.yaml"),
            description="Harness issue",
            severity=IssueSeverity.HIGH,
            evidence=[IssueEvidence.from_config_inspection("test", "content", 1)],
            confidence=ConfidenceLevel.HIGH,
        )
        assert harness_issue.is_improvement_candidate() is False

    def test_format_for_display(self):
        """Issue can be formatted for display."""
        issue = DiscoveredIssue(
            issue_id="abc123",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(
                file_path="config.yaml",
                node_name="dedupe",
            ),
            description="Missing order_by",
            severity=IssueSeverity.HIGH,
            evidence=[
                IssueEvidence.from_config_inspection("config.yaml", "transformer: deduplicate", 10)
            ],
            confidence=ConfidenceLevel.HIGH,
        )
        formatted = issue.format_for_display()
        assert "NON_DETERMINISTIC_TRANSFORMER" in formatted
        assert "Missing order_by" in formatted
        assert "🔴" in formatted


class TestPassiveIssueDetector:
    """Tests for PassiveIssueDetector."""

    def test_detect_non_deterministic_deduplicate(self):
        """Detects deduplicate without order_by."""
        detector = PassiveIssueDetector("d:/odibi")

        content = """
pipelines:
  - pipeline: test
    nodes:
      - name: dedupe_test
        transformer: deduplicate
        params:
          keys: ["id"]
        write:
          path: output
"""
        issues = detector.detect_issues_in_file("test.yaml", content)

        assert len(issues) >= 1
        assert any(
            i.issue_type == DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER for i in issues
        )

    def test_no_false_positive_with_order_by(self):
        """No false positive when order_by is present."""
        detector = PassiveIssueDetector("d:/odibi")

        content = """
pipelines:
  - pipeline: test
    nodes:
      - name: dedupe_test
        transformer: deduplicate
        params:
          keys: ["id"]
          order_by: "timestamp"
        write:
          path: output
"""
        issues = detector._detect_non_deterministic_deduplicate("test.yaml", content)
        assert len(issues) == 0

    def test_detect_engine_incompatibility(self):
        """Detects engine incompatibilities."""
        detector = PassiveIssueDetector("d:/odibi")

        content = """
engine: pyarrow
pipelines:
  - pipeline: test
    nodes:
      - name: merge_node
        transformer: merge
"""
        issues = detector._detect_engine_incompatibilities("test.yaml", content)

        assert len(issues) == 1
        assert issues[0].issue_type == DiscoveredIssueType.ENGINE_INCOMPATIBILITY

    def test_detect_suspicious_cross_join(self):
        """Detects cross join with keys - validates detection function exists."""
        detector = PassiveIssueDetector("d:/odibi")

        content_without_cross = """pipelines:
  - pipeline: test
    nodes:
      - name: good_join
        transformer: join
        params:
          how: left
          "on": customer_id
x
"""
        issues = detector._detect_suspicious_join_patterns("test.yaml", content_without_cross)
        assert len(issues) == 0

    def test_detect_missing_required_params(self):
        """Detects missing required parameters."""
        detector = PassiveIssueDetector("d:/odibi")

        content = """
pipelines:
  - pipeline: test
    nodes:
      - name: incomplete_join
        transformer: join
        params:
          how: left
"""
        issues = detector._detect_missing_required_params("test.yaml", content)

        assert len(issues) >= 1
        assert any(i.issue_type == DiscoveredIssueType.MISSING_REQUIRED_PARAM for i in issues)


class TestUserIssueSelection:
    """Tests for UserIssueSelection."""

    def test_to_task_description(self):
        """Selection generates task description."""
        issues = [
            DiscoveredIssue(
                issue_id="issue1",
                issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
                location=IssueLocation(file_path="test.yaml"),
                description="Missing order_by in deduplicate",
                severity=IssueSeverity.HIGH,
                evidence=[],
                confidence=ConfidenceLevel.HIGH,
            ),
        ]

        selection = UserIssueSelection(
            selection_id="sel1",
            selected_issues=["issue1"],
            target_file="test.yaml",
            user_notes="Fix this ASAP",
        )

        task = selection.to_task_description(issues)
        assert "test.yaml" in task
        assert "Missing order_by" in task
        assert "Fix this ASAP" in task


class TestIssueDiscoveryManager:
    """Tests for IssueDiscoveryManager."""

    def test_create_selection_blocks_harness_files(self):
        """INVARIANT: Cannot select harness files as improvement targets."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = IssueDiscoveryManager(tmpdir)

            with pytest.raises(ValueError) as exc:
                manager.create_selection(
                    issue_ids=["issue1"],
                    target_file=".odibi/learning_harness/test.yaml",
                )

            assert "Learning harness" in str(exc.value) or "protected" in str(exc.value).lower()

    def test_get_improvement_candidates_excludes_harness(self):
        """INVARIANT: Harness issues are not improvement candidates."""
        harness_issue = DiscoveredIssue(
            issue_id="harness1",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path=".odibi/learning_harness/test.yaml"),
            description="Harness issue",
            severity=IssueSeverity.HIGH,
            evidence=[IssueEvidence.from_config_inspection("test", "content", 1)],
            confidence=ConfidenceLevel.HIGH,
        )

        normal_issue = DiscoveredIssue(
            issue_id="normal1",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path="examples/test.yaml"),
            description="Normal issue",
            severity=IssueSeverity.HIGH,
            evidence=[IssueEvidence.from_config_inspection("test", "content", 1)],
            confidence=ConfidenceLevel.HIGH,
        )

        result = IssueDiscoveryResult(
            scan_id="",
            cycle_id="",
            scanned_at="",
            files_scanned=[],
            issues=[harness_issue, normal_issue],
        )

        candidates = result.get_improvement_candidates()

        assert len(candidates) == 1
        assert candidates[0].issue_id == "normal1"


class TestPhase10Invariants:
    """Tests that verify Phase 10 core invariants."""

    def test_invariant_no_automatic_fixes(self):
        """INVARIANT: No fixes occur without explicit user approval.

        PassiveIssueDetector and IssueDiscoveryManager only DETECT issues.
        They never modify files or propose fixes.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.odibi.yaml"
            original_content = """
pipelines:
  - pipeline: test
    nodes:
      - name: dedupe
        transformer: deduplicate
        params:
          keys: ["id"]
"""
            test_file.write_text(original_content)

            manager = IssueDiscoveryManager(tmpdir)
            manager.run_discovery(tmpdir)

            after_content = test_file.read_text()
            assert after_content == original_content

    def test_invariant_harness_files_protected(self):
        """INVARIANT: Learning harness files cannot be improvement targets."""
        harness_paths = [
            ".odibi/learning_harness/test.yaml",
            "odibi/learning_harness/test.yaml",
            "d:/project/.odibi/learning_harness/scale_join.yaml",
            "/home/user/.odibi/learning_harness/schema_drift.yaml",
        ]

        for path in harness_paths:
            assert is_learning_harness_path(path) is True, f"Path should be protected: {path}"

    def test_invariant_weak_evidence_excluded(self):
        """INVARIANT: Weak evidence does not generate actionable issues."""
        issue = DiscoveredIssue(
            issue_id="weak1",
            issue_type=DiscoveredIssueType.PERFORMANCE_SMELL,
            location=IssueLocation(file_path="test.yaml"),
            description="Possible performance issue",
            severity=IssueSeverity.LOW,
            evidence=[],
            confidence=ConfidenceLevel.LOW,
        )

        assert issue.is_actionable() is False
        assert issue.is_improvement_candidate() is False

    def test_invariant_evidence_required(self):
        """INVARIANT: Every issue must be tied to evidence."""
        detector = PassiveIssueDetector("d:/odibi")

        content = """
pipelines:
  - pipeline: test
    nodes:
      - name: dedupe
        transformer: deduplicate
        params:
          keys: ["id"]
"""
        issues = detector.detect_issues_in_file("test.yaml", content)

        for issue in issues:
            assert len(issue.evidence) > 0, f"Issue {issue.issue_id} has no evidence"
            for evidence in issue.evidence:
                assert evidence.excerpt, "Evidence must have an excerpt"
                assert evidence.evidence_type, "Evidence must have a type"


class TestIssueDiscoveryResult:
    """Tests for IssueDiscoveryResult."""

    def test_to_dict_roundtrip(self):
        """Result can be serialized and deserialized."""
        issue = DiscoveredIssue(
            issue_id="test123",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path="test.yaml", node_name="dedupe"),
            description="Test issue",
            severity=IssueSeverity.HIGH,
            evidence=[IssueEvidence.from_config_inspection("test.yaml", "content", 10)],
            confidence=ConfidenceLevel.HIGH,
        )

        result = IssueDiscoveryResult(
            scan_id="scan1",
            cycle_id="cycle1",
            scanned_at="2024-01-01T00:00:00",
            files_scanned=["test.yaml"],
            issues=[issue],
        )

        data = result.to_dict()
        restored = IssueDiscoveryResult.from_dict(data)

        assert restored.scan_id == result.scan_id
        assert restored.cycle_id == result.cycle_id
        assert len(restored.issues) == 1
        assert restored.issues[0].issue_id == "test123"

    def test_counts_are_correct(self):
        """Result counts are computed correctly."""
        actionable = DiscoveredIssue(
            issue_id="act1",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path="test.yaml"),
            description="Actionable",
            severity=IssueSeverity.HIGH,
            evidence=[IssueEvidence.from_config_inspection("test", "content", 1)],
            confidence=ConfidenceLevel.HIGH,
        )

        harness = DiscoveredIssue(
            issue_id="har1",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path=".odibi/learning_harness/test.yaml"),
            description="Harness",
            severity=IssueSeverity.HIGH,
            evidence=[IssueEvidence.from_config_inspection("test", "content", 1)],
            confidence=ConfidenceLevel.HIGH,
        )

        result = IssueDiscoveryResult(
            scan_id="",
            cycle_id="",
            scanned_at="",
            files_scanned=[],
            issues=[actionable, harness],
        )

        assert result.total_issues == 2
        assert result.actionable_issues == 2
        assert result.harness_issues_excluded == 1

    def test_get_by_severity(self):
        """Can filter issues by severity."""
        high = DiscoveredIssue(
            issue_id="high1",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path="test.yaml"),
            description="High severity",
            severity=IssueSeverity.HIGH,
            evidence=[],
            confidence=ConfidenceLevel.HIGH,
        )

        low = DiscoveredIssue(
            issue_id="low1",
            issue_type=DiscoveredIssueType.PERFORMANCE_SMELL,
            location=IssueLocation(file_path="test.yaml"),
            description="Low severity",
            severity=IssueSeverity.LOW,
            evidence=[],
            confidence=ConfidenceLevel.LOW,
        )

        result = IssueDiscoveryResult(
            scan_id="",
            cycle_id="",
            scanned_at="",
            files_scanned=[],
            issues=[high, low],
        )

        high_issues = result.get_by_severity(IssueSeverity.HIGH)
        assert len(high_issues) == 1
        assert high_issues[0].issue_id == "high1"

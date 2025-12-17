"""Tests for Phase 10.D Issue-to-Improvement Integration.

Tests verify:
1. User selection required before improvement
2. Controlled improvement flow unchanged
3. Validation catches errors before improvement starts
4. Learning harness files blocked at integration level
"""

import tempfile

import pytest

from odibi.agents.core.issue_discovery import (
    ConfidenceLevel,
    DiscoveredIssue,
    DiscoveredIssueType,
    IssueDiscoveryResult,
    IssueEvidence,
    IssueLocation,
    UserIssueSelection,
)
from odibi.agents.core.issue_to_improvement import (
    IssueImprovementIntegrationError,
    IssueImprovementIntegrator,
    IssueImprovementRequest,
)
from odibi.agents.core.controlled_improvement import is_learning_harness_path
from odibi.agents.core.cycle import GoldenProjectConfig
from odibi.agents.core.schemas import IssueSeverity


class TestIssueImprovementRequest:
    """Tests for IssueImprovementRequest validation."""

    def _create_test_issue(self, issue_id: str, file_path: str = "test.yaml") -> DiscoveredIssue:
        return DiscoveredIssue(
            issue_id=issue_id,
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path=file_path),
            description="Test issue",
            severity=IssueSeverity.HIGH,
            evidence=[IssueEvidence.from_config_inspection(file_path, "content", 1)],
            confidence=ConfidenceLevel.HIGH,
        )

    def test_validate_requires_selected_issues(self):
        """Validation fails without selected issues."""
        request = IssueImprovementRequest(
            selection=UserIssueSelection(
                selection_id="sel1",
                selected_issues=[],
                target_file="test.yaml",
            ),
            issues=[self._create_test_issue("issue1")],
            project_root="d:/odibi",
            golden_projects=[],
        )

        errors = request.validate()
        assert any("No issues selected" in e for e in errors)

    def test_validate_requires_target_file(self):
        """Validation fails without target file."""
        request = IssueImprovementRequest(
            selection=UserIssueSelection(
                selection_id="sel1",
                selected_issues=["issue1"],
                target_file="",
            ),
            issues=[self._create_test_issue("issue1")],
            project_root="d:/odibi",
            golden_projects=[],
        )

        errors = request.validate()
        assert any("No target file" in e for e in errors)

    def test_validate_blocks_harness_target(self):
        """INVARIANT: Validation blocks learning harness targets."""
        request = IssueImprovementRequest(
            selection=UserIssueSelection(
                selection_id="sel1",
                selected_issues=["issue1"],
                target_file=".odibi/learning_harness/test.yaml",
            ),
            issues=[self._create_test_issue("issue1")],
            project_root="d:/odibi",
            golden_projects=[],
        )

        errors = request.validate()
        assert any("learning harness" in e.lower() or "protected" in e.lower() for e in errors)

    def test_validate_checks_issue_exists(self):
        """Validation fails if selected issue doesn't exist."""
        request = IssueImprovementRequest(
            selection=UserIssueSelection(
                selection_id="sel1",
                selected_issues=["nonexistent"],
                target_file="test.yaml",
            ),
            issues=[self._create_test_issue("issue1")],
            project_root="d:/odibi",
            golden_projects=[],
        )

        errors = request.validate()
        assert any("not found" in e.lower() for e in errors)

    def test_validate_checks_issue_is_candidate(self):
        """Validation fails if issue is not an improvement candidate."""
        harness_issue = DiscoveredIssue(
            issue_id="harness1",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path=".odibi/learning_harness/test.yaml"),
            description="Harness issue",
            severity=IssueSeverity.HIGH,
            evidence=[IssueEvidence.from_config_inspection("test", "content", 1)],
            confidence=ConfidenceLevel.HIGH,
        )

        request = IssueImprovementRequest(
            selection=UserIssueSelection(
                selection_id="sel1",
                selected_issues=["harness1"],
                target_file="test.yaml",
            ),
            issues=[harness_issue],
            project_root="d:/odibi",
            golden_projects=[],
        )

        errors = request.validate()
        assert any("not an improvement candidate" in e.lower() for e in errors)

    def test_validate_requires_golden_projects_if_regression_check(self):
        """Validation fails if regression check required but no golden projects."""
        request = IssueImprovementRequest(
            selection=UserIssueSelection(
                selection_id="sel1",
                selected_issues=["issue1"],
                target_file="test.yaml",
            ),
            issues=[self._create_test_issue("issue1")],
            project_root="d:/odibi",
            golden_projects=[],
            require_regression_check=True,
        )

        errors = request.validate()
        assert any("golden projects" in e.lower() for e in errors)

    def test_get_task_description(self):
        """Task description includes issue details."""
        issue = self._create_test_issue("issue1")
        issue.description = "Missing order_by parameter"

        request = IssueImprovementRequest(
            selection=UserIssueSelection(
                selection_id="sel1",
                selected_issues=["issue1"],
                target_file="test.yaml",
                user_notes="Important fix",
            ),
            issues=[issue],
            project_root="d:/odibi",
            golden_projects=[],
            require_regression_check=False,
        )

        task = request.get_task_description()
        assert "test.yaml" in task
        assert "Missing order_by" in task
        assert "Important fix" in task

    def test_to_controlled_improvement_config_valid(self):
        """Valid request creates valid config."""
        issue = self._create_test_issue("issue1", "examples/test.yaml")

        request = IssueImprovementRequest(
            selection=UserIssueSelection(
                selection_id="sel1",
                selected_issues=["issue1"],
                target_file="examples/test.yaml",
            ),
            issues=[issue],
            project_root="d:/odibi",
            golden_projects=[],
            require_regression_check=False,
        )

        config = request.to_controlled_improvement_config()
        assert config.project_root == "d:/odibi"
        assert config.improvement_scope is not None
        assert "examples/test.yaml" in config.improvement_scope.allowed_files

    def test_to_controlled_improvement_config_invalid_raises(self):
        """Invalid request raises IssueImprovementIntegrationError."""
        request = IssueImprovementRequest(
            selection=UserIssueSelection(
                selection_id="sel1",
                selected_issues=[],
                target_file="",
            ),
            issues=[],
            project_root="d:/odibi",
            golden_projects=[],
        )

        with pytest.raises(IssueImprovementIntegrationError):
            request.to_controlled_improvement_config()


class TestIssueImprovementIntegrator:
    """Tests for IssueImprovementIntegrator."""

    def test_discover_issues(self):
        """Integrator can discover issues - validates integration exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            integrator = IssueImprovementIntegrator(tmpdir)
            result = integrator.discover_issues(tmpdir)

            assert result is not None
            assert hasattr(result, "issues")
            assert hasattr(result, "files_scanned")

    def test_create_selection_blocks_harness(self):
        """INVARIANT: Cannot select harness files as targets."""
        with tempfile.TemporaryDirectory() as tmpdir:
            integrator = IssueImprovementIntegrator(tmpdir)

            with pytest.raises(ValueError) as exc:
                integrator.create_selection(
                    issue_ids=["issue1"],
                    target_file=".odibi/learning_harness/test.yaml",
                )

            assert "protected" in str(exc.value).lower() or "harness" in str(exc.value).lower()

    def test_preview_task_description(self):
        """Can preview task description before running improvement."""
        with tempfile.TemporaryDirectory() as tmpdir:
            issue = DiscoveredIssue(
                issue_id="preview1",
                issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
                location=IssueLocation(file_path="test.yaml"),
                description="Preview test issue",
                severity=IssueSeverity.HIGH,
                evidence=[IssueEvidence.from_config_inspection("test", "content", 1)],
                confidence=ConfidenceLevel.HIGH,
            )

            result = IssueDiscoveryResult(
                scan_id="",
                cycle_id="",
                scanned_at="",
                files_scanned=[],
                issues=[issue],
            )

            integrator = IssueImprovementIntegrator(tmpdir)
            integrator._last_discovery = result

            selection = UserIssueSelection(
                selection_id="",
                selected_issues=["preview1"],
                target_file="test.yaml",
            )

            preview = integrator.preview_task_description(selection)
            assert "Preview test issue" in preview


class TestPhase10DInvariants:
    """Tests that verify Phase 10.D integration invariants."""

    def test_invariant_user_selection_required(self):
        """INVARIANT: Improvement cannot proceed without user selection.

        The integration flow requires:
        1. Discovery (passive)
        2. User selection (explicit approval)
        3. Improvement (only after approval)
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            integrator = IssueImprovementIntegrator(tmpdir)

            result = IssueDiscoveryResult(
                scan_id="",
                cycle_id="",
                scanned_at="",
                files_scanned=[],
                issues=[],
            )

            empty_selection = UserIssueSelection(
                selection_id="",
                selected_issues=[],
                target_file="",
            )

            with pytest.raises(IssueImprovementIntegrationError) as exc:
                integrator.run_improvement(
                    selection=empty_selection,
                    discovery_result=result,
                    project_root=tmpdir,
                )

            assert "validation failed" in str(exc.value).lower()

    def test_invariant_controlled_improvement_unchanged(self):
        """INVARIANT: Controlled improvement flow remains unchanged.

        The IssueImprovementRequest.to_controlled_improvement_config()
        creates a standard ControlledImprovementConfig with:
        - Same scope enforcement
        - Same rollback logic
        - Same regression gates
        """
        issue = DiscoveredIssue(
            issue_id="test1",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path="examples/test.yaml"),
            description="Test",
            severity=IssueSeverity.HIGH,
            evidence=[IssueEvidence.from_config_inspection("test", "content", 1)],
            confidence=ConfidenceLevel.HIGH,
        )

        golden = GoldenProjectConfig(
            name="golden1",
            path="golden.yaml",
        )

        request = IssueImprovementRequest(
            selection=UserIssueSelection(
                selection_id="",
                selected_issues=["test1"],
                target_file="examples/test.yaml",
            ),
            issues=[issue],
            project_root="d:/odibi",
            golden_projects=[golden],
            rollback_on_failure=True,
            require_regression_check=True,
        )

        config = request.to_controlled_improvement_config()

        assert config.max_improvements == 1
        assert config.rollback_on_failure is True
        assert config.require_regression_check is True
        assert len(config.golden_projects) == 1

    def test_invariant_harness_blocked_at_all_levels(self):
        """INVARIANT: Learning harness files blocked at all integration levels."""
        harness_paths = [
            ".odibi/learning_harness/test.yaml",
            "d:/odibi/.odibi/learning_harness/scale_join.yaml",
        ]

        for path in harness_paths:
            assert is_learning_harness_path(path) is True

            location = IssueLocation(file_path=path)
            assert location.is_in_learning_harness() is True

            issue = DiscoveredIssue(
                issue_id="harness",
                issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
                location=location,
                description="Harness issue",
                severity=IssueSeverity.HIGH,
                evidence=[IssueEvidence.from_config_inspection(path, "content", 1)],
                confidence=ConfidenceLevel.HIGH,
            )
            assert issue.is_improvement_candidate() is False

            request = IssueImprovementRequest(
                selection=UserIssueSelection(
                    selection_id="",
                    selected_issues=["harness"],
                    target_file=path,
                ),
                issues=[issue],
                project_root="d:/odibi",
                golden_projects=[],
            )
            errors = request.validate()
            assert len(errors) > 0

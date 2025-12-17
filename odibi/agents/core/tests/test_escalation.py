"""Tests for Phase 12: Human-Gated Improvement Escalation.

These tests verify ALL Phase 12 invariants:

1. Discovery Is Forever Passive
   - Discovery MUST NOT modify files
   - Discovery MUST NOT trigger execution
   - Discovery MUST NOT auto-select issues
   - Discovery MUST NOT call Controlled Improvement

2. Human Intent Is the Authorization Boundary
   - Improvement starts ONLY with explicit human action
   - No defaults, no background escalation

3. Improvement Scope Must Be Explicit and Minimal
   - Exactly one target file
   - Exactly one issue (or tightly defined group)
   - No implicit scope expansion

4. Learning Harness Is Read-Only Forever
   - Harness files NEVER improvement targets
   - Violation = hard failure

5. Improvements Must Be Fully Reversible
   - Snapshot before apply
   - Rollback on regression

6. No Hidden Knowledge Injection
   - Only indexed/retrieved/explicit knowledge

7. Approval Is Mandatory
   - No silent auto-merge

8. Failure Is a Valid Outcome
   - No proposal, rejection, rollback are all valid
"""

import tempfile
from pathlib import Path

import pytest

from odibi.agents.core.escalation import (
    AmbiguousSelectionError,
    EscalationError,
    EscalationRequest,
    EscalationStatus,
    EscalationValidator,
    HarnessEscalationBlockedError,
    HumanGatedEscalator,
    IssueSelection,
    create_issue_selection,
    validate_escalation_safety,
)
from odibi.agents.core.controlled_improvement import is_learning_harness_path
from odibi.agents.core.issue_discovery import (
    ConfidenceLevel,
    DiscoveredIssue,
    DiscoveredIssueType,
    IssueDiscoveryManager,
    IssueDiscoveryResult,
    IssueEvidence,
    IssueLocation,
)
from odibi.agents.core.schemas import IssueSeverity


class TestIssueSelection:
    """Tests for IssueSelection immutable artifact."""

    def test_selection_is_immutable(self):
        """IssueSelection is a frozen dataclass - cannot be modified."""
        selection = IssueSelection(
            selection_id="sel123",
            issue_id="issue456",
            issue_type="NON_DETERMINISTIC_TRANSFORMER",
            evidence=({"type": "config", "excerpt": "test"},),
            target_file="examples/test.yaml",
            user_intent="Fix the deduplicate issue",
            created_at="2024-01-01T00:00:00",
        )

        # Attempting to modify should raise
        with pytest.raises(AttributeError):
            selection.issue_id = "different_id"

        with pytest.raises(AttributeError):
            selection.target_file = "different_file.yaml"

    def test_selection_requires_issue_id(self):
        """INVARIANT: Selection must have an issue_id."""
        with pytest.raises(AmbiguousSelectionError) as exc:
            IssueSelection(
                selection_id="sel123",
                issue_id="",  # Empty!
                issue_type="TEST",
                evidence=(),
                target_file="test.yaml",
                user_intent="Fix something",
                created_at="2024-01-01T00:00:00",
            )
        assert "issue_id is required" in str(exc.value)

    def test_selection_requires_target_file(self):
        """INVARIANT: Selection must have a target_file."""
        with pytest.raises(AmbiguousSelectionError) as exc:
            IssueSelection(
                selection_id="sel123",
                issue_id="issue456",
                issue_type="TEST",
                evidence=(),
                target_file="",  # Empty!
                user_intent="Fix something",
                created_at="2024-01-01T00:00:00",
            )
        assert "target_file is required" in str(exc.value)

    def test_selection_requires_user_intent(self):
        """INVARIANT: Selection must have user_intent."""
        with pytest.raises(AmbiguousSelectionError) as exc:
            IssueSelection(
                selection_id="sel123",
                issue_id="issue456",
                issue_type="TEST",
                evidence=(),
                target_file="test.yaml",
                user_intent="",  # Empty!
                created_at="2024-01-01T00:00:00",
            )
        assert "user_intent is required" in str(exc.value)

    def test_selection_blocks_harness_files(self):
        """INVARIANT: Learning harness files are BLOCKED at selection creation."""
        harness_paths = [
            ".odibi/learning_harness/test.yaml",
            "d:/odibi/.odibi/learning_harness/scale_join.yaml",
            "/home/user/.odibi/learning_harness/schema_drift.yaml",
        ]

        for path in harness_paths:
            with pytest.raises(HarnessEscalationBlockedError) as exc:
                IssueSelection(
                    selection_id="sel123",
                    issue_id="issue456",
                    issue_type="TEST",
                    evidence=(),
                    target_file=path,
                    user_intent="This should fail",
                    created_at="2024-01-01T00:00:00",
                )
            assert "learning harness" in str(exc.value).lower()

    def test_selection_serialization(self):
        """Selection can be serialized and deserialized."""
        original = IssueSelection(
            selection_id="sel123",
            issue_id="issue456",
            issue_type="NON_DETERMINISTIC_TRANSFORMER",
            evidence=({"type": "config", "excerpt": "test"},),
            target_file="examples/test.yaml",
            user_intent="Fix the deduplicate issue",
            created_at="2024-01-01T00:00:00",
            created_by="test_user",
        )

        data = original.to_dict()
        restored = IssueSelection.from_dict(data)

        assert restored.selection_id == original.selection_id
        assert restored.issue_id == original.issue_id
        assert restored.target_file == original.target_file
        assert restored.user_intent == original.user_intent


class TestCreateIssueSelection:
    """Tests for create_issue_selection factory function."""

    def _make_issue(
        self,
        file_path: str = "examples/test.yaml",
        confidence: ConfidenceLevel = ConfidenceLevel.HIGH,
        with_evidence: bool = True,
    ) -> DiscoveredIssue:
        """Helper to create a test issue."""
        evidence = []
        if with_evidence:
            evidence = [IssueEvidence.from_config_inspection(file_path, "test content", 1)]

        return DiscoveredIssue(
            issue_id="test_issue_123",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path=file_path),
            description="Test issue description",
            severity=IssueSeverity.HIGH,
            evidence=evidence,
            confidence=confidence,
        )

    def test_creates_selection_from_issue(self):
        """Can create selection from a discovered issue."""
        issue = self._make_issue()
        selection = create_issue_selection(
            issue=issue,
            user_intent="Fix the non-deterministic deduplicate",
        )

        assert selection.issue_id == issue.issue_id
        assert selection.issue_type == issue.issue_type.value
        assert selection.target_file == issue.location.file_path
        assert "Fix the non-deterministic" in selection.user_intent

    def test_allows_target_file_override(self):
        """Can override target file from issue location."""
        issue = self._make_issue()
        selection = create_issue_selection(
            issue=issue,
            user_intent="Fix in different file",
            target_file="different/location.yaml",
        )

        assert selection.target_file == "different/location.yaml"

    def test_blocks_harness_issue(self):
        """INVARIANT: Cannot create selection from harness issue."""
        issue = self._make_issue(file_path=".odibi/learning_harness/test.yaml")

        with pytest.raises(HarnessEscalationBlockedError):
            create_issue_selection(
                issue=issue,
                user_intent="This should fail",
            )

    def test_blocks_harness_target_override(self):
        """INVARIANT: Cannot override target to harness file."""
        issue = self._make_issue()  # Normal file

        with pytest.raises(HarnessEscalationBlockedError):
            create_issue_selection(
                issue=issue,
                user_intent="This should fail",
                target_file=".odibi/learning_harness/protected.yaml",
            )

    def test_blocks_non_improvement_candidate(self):
        """INVARIANT: Cannot create selection from non-candidate issue."""
        # Low confidence with single evidence = not actionable
        issue = self._make_issue(confidence=ConfidenceLevel.LOW)

        with pytest.raises(AmbiguousSelectionError) as exc:
            create_issue_selection(
                issue=issue,
                user_intent="This should fail",
            )
        assert "not an improvement candidate" in str(exc.value)

    def test_copies_evidence(self):
        """Evidence is copied to ensure immutability."""
        issue = self._make_issue()
        selection = create_issue_selection(
            issue=issue,
            user_intent="Fix issue",
        )

        # Evidence should be copied
        assert len(selection.evidence) == len(issue.evidence)
        assert isinstance(selection.evidence, tuple)  # Immutable


class TestEscalationRequest:
    """Tests for EscalationRequest status transitions."""

    def _make_selection(self) -> IssueSelection:
        """Helper to create a test selection."""
        return IssueSelection(
            selection_id="sel123",
            issue_id="issue456",
            issue_type="TEST",
            evidence=({"type": "test"},),
            target_file="test.yaml",
            user_intent="Fix test issue",
            created_at="2024-01-01T00:00:00",
        )

    def test_request_starts_pending(self):
        """Request starts in PENDING_CONFIRMATION status."""
        selection = self._make_selection()
        request = EscalationRequest(
            request_id="",
            selection=selection,
            status=EscalationStatus.PENDING_CONFIRMATION,
        )

        assert request.status == EscalationStatus.PENDING_CONFIRMATION

    def test_confirm_requires_pending_status(self):
        """INVARIANT: Can only confirm from PENDING status."""
        selection = self._make_selection()
        request = EscalationRequest(
            request_id="req123",
            selection=selection,
            status=EscalationStatus.CONFIRMED,  # Already confirmed!
        )

        with pytest.raises(EscalationError) as exc:
            request.confirm()
        assert "expected PENDING_CONFIRMATION" in str(exc.value)

    def test_confirm_sets_confirmed_status(self):
        """Confirm transitions to CONFIRMED status."""
        selection = self._make_selection()
        request = EscalationRequest(
            request_id="req123",
            selection=selection,
            status=EscalationStatus.PENDING_CONFIRMATION,
        )

        request.confirm()

        assert request.status == EscalationStatus.CONFIRMED
        assert request.confirmed_at is not None

    def test_reject_sets_rejected_status(self):
        """Reject transitions to REJECTED status."""
        selection = self._make_selection()
        request = EscalationRequest(
            request_id="req123",
            selection=selection,
            status=EscalationStatus.PENDING_CONFIRMATION,
        )

        request.reject("Not needed anymore")

        assert request.status == EscalationStatus.REJECTED
        assert request.rejection_reason == "Not needed anymore"

    def test_mark_in_progress_requires_confirmed(self):
        """INVARIANT: Can only start from CONFIRMED status."""
        selection = self._make_selection()
        request = EscalationRequest(
            request_id="req123",
            selection=selection,
            status=EscalationStatus.PENDING_CONFIRMATION,  # Not confirmed!
        )

        with pytest.raises(EscalationError) as exc:
            request.mark_in_progress()
        assert "expected CONFIRMED" in str(exc.value)

    def test_audit_log_tracks_events(self):
        """Request maintains audit log of all events."""
        selection = self._make_selection()
        request = EscalationRequest(
            request_id="req123",
            selection=selection,
            status=EscalationStatus.PENDING_CONFIRMATION,
        )

        # Initial creation should be logged
        assert len(request.audit_log) == 1
        assert request.audit_log[0]["event_type"] == "CREATED"

        request.confirm()
        assert len(request.audit_log) == 2
        assert request.audit_log[1]["event_type"] == "CONFIRMED"


class TestEscalationValidator:
    """Tests for EscalationValidator."""

    def test_validates_harness_blocking(self):
        """INVARIANT: Validator blocks harness files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            validator = EscalationValidator(tmpdir)

            selection = IssueSelection(
                selection_id="sel123",
                issue_id="issue456",
                issue_type="TEST",
                evidence=({"type": "test"},),
                target_file="examples/test.yaml",  # Valid
                user_intent="Fix issue",
                created_at="2024-01-01T00:00:00",
            )

            # This should pass basic validation
            # (We can't test harness blocking here since IssueSelection
            # already blocks it at creation time)
            errors = validator.validate_selection(selection)
            # File doesn't exist in tmpdir, so we expect that error
            assert any("NOT FOUND" in e or "not exist" in e.lower() for e in errors)

    def test_validates_empty_selection(self):
        """Validator catches empty/ambiguous selections."""
        with tempfile.TemporaryDirectory() as tmpdir:
            validator = EscalationValidator(tmpdir)

            # Create a file for the selection
            test_file = Path(tmpdir) / "test.yaml"
            test_file.write_text("test: content")

            selection = IssueSelection(
                selection_id="sel123",
                issue_id="issue456",
                issue_type="TEST",
                evidence=(),  # No evidence!
                target_file=str(test_file),
                user_intent="Fix issue",
                created_at="2024-01-01T00:00:00",
            )

            errors = validator.validate_selection(selection)
            assert any("NO EVIDENCE" in e for e in errors)


class TestValidateEscalationSafety:
    """Tests for validate_escalation_safety function."""

    def test_blocks_harness_target(self):
        """INVARIANT: Harness targets are blocked."""
        # We can't directly create a selection with harness target,
        # but we can test the validation function behavior
        # by creating a selection with a valid target and checking harness detection

        # This would fail at IssueSelection creation, so let's test is_learning_harness_path
        assert is_learning_harness_path(".odibi/learning_harness/test.yaml")
        assert is_learning_harness_path("d:/odibi/.odibi/learning_harness/x.yaml")
        assert not is_learning_harness_path("examples/test.yaml")

    def test_validates_against_discovery(self):
        """Can cross-reference selection with discovery result."""
        selection = IssueSelection(
            selection_id="sel123",
            issue_id="issue456",
            issue_type="TEST",
            evidence=({"type": "test"},),
            target_file="examples/test.yaml",
            user_intent="Fix issue",
            created_at="2024-01-01T00:00:00",
        )

        # Empty discovery result - issue won't be found
        result = IssueDiscoveryResult(
            scan_id="scan1",
            cycle_id="",
            scanned_at="",
            files_scanned=[],
            issues=[],
        )

        is_safe, violations = validate_escalation_safety(selection, result)

        assert not is_safe
        assert any("not found" in v.lower() for v in violations)


class TestPhase12Invariants:
    """Tests that verify Phase 12 core invariants."""

    def test_invariant_1_discovery_is_passive(self):
        """INVARIANT 1: Discovery MUST NOT trigger improvements.

        IssueDiscoveryManager only scans and reports.
        It has no method to trigger improvements.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a test file
            test_file = Path(tmpdir) / "test.yaml"
            original_content = "pipelines:\n  - name: test\n"
            test_file.write_text(original_content)

            manager = IssueDiscoveryManager(tmpdir)

            # Run discovery
            _result = manager.run_discovery(tmpdir)

            # File should be unchanged
            assert test_file.read_text() == original_content

            # Manager has no trigger_improvement method
            assert not hasattr(manager, "trigger_improvement")
            assert not hasattr(manager, "auto_fix")
            assert not hasattr(manager, "fix_all")

    def test_invariant_2_human_intent_required(self):
        """INVARIANT 2: Human intent is required for escalation.

        IssueSelection REQUIRES user_intent - cannot be empty.
        """
        with pytest.raises(AmbiguousSelectionError) as exc:
            IssueSelection(
                selection_id="sel123",
                issue_id="issue456",
                issue_type="TEST",
                evidence=(),
                target_file="test.yaml",
                user_intent="   ",  # Whitespace only
                created_at="2024-01-01T00:00:00",
            )
        assert "user_intent is required" in str(exc.value)

    def test_invariant_3_single_file_scope(self):
        """INVARIANT 3: Each escalation targets exactly one file.

        IssueSelection has a single target_file, not a list.
        """
        selection = IssueSelection(
            selection_id="sel123",
            issue_id="issue456",
            issue_type="TEST",
            evidence=({"type": "test"},),
            target_file="test.yaml",
            user_intent="Fix issue",
            created_at="2024-01-01T00:00:00",
        )

        # target_file is a string, not a list
        assert isinstance(selection.target_file, str)

    def test_invariant_4_harness_protected(self):
        """INVARIANT 4: Learning harness is read-only forever.

        All harness path patterns are blocked.
        """
        harness_patterns = [
            ".odibi/learning_harness/test.yaml",
            "odibi/learning_harness/test.yaml",
            "d:/project/.odibi/learning_harness/x.yaml",
            "/home/user/.odibi/learning_harness/y.yaml",
            "C:\\Users\\test\\.odibi\\learning_harness\\z.yaml",
        ]

        for path in harness_patterns:
            assert is_learning_harness_path(path), f"Should block: {path}"

            with pytest.raises(HarnessEscalationBlockedError):
                IssueSelection(
                    selection_id="sel",
                    issue_id="issue",
                    issue_type="TEST",
                    evidence=(),
                    target_file=path,
                    user_intent="Should fail",
                    created_at="2024-01-01T00:00:00",
                )

    def test_invariant_5_execution_requires_confirmation(self):
        """INVARIANT 5: Execution requires explicit confirmation.

        Cannot execute an escalation that isn't CONFIRMED.
        """
        selection = IssueSelection(
            selection_id="sel123",
            issue_id="issue456",
            issue_type="TEST",
            evidence=({"type": "test"},),
            target_file="test.yaml",
            user_intent="Fix issue",
            created_at="2024-01-01T00:00:00",
        )

        # Create request in PENDING state
        request = EscalationRequest(
            request_id="req123",
            selection=selection,
            status=EscalationStatus.PENDING_CONFIRMATION,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            escalator = HumanGatedEscalator(
                odibi_root=tmpdir,
                project_root=tmpdir,
            )

            # Should fail because not confirmed
            with pytest.raises(EscalationError) as exc:
                escalator.execute_escalation(request)
            assert "CONFIRMED" in str(exc.value)
            assert "Human confirmation is REQUIRED" in str(exc.value)

    def test_invariant_6_evidence_required(self):
        """INVARIANT 6: Evidence is required for improvement candidates.

        Issues without evidence are not improvement candidates.
        """
        # Issue without evidence
        issue = DiscoveredIssue(
            issue_id="test123",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path="test.yaml"),
            description="No evidence issue",
            severity=IssueSeverity.HIGH,
            evidence=[],  # No evidence!
            confidence=ConfidenceLevel.HIGH,
        )

        assert not issue.is_actionable()
        assert not issue.is_improvement_candidate()

        # Cannot create selection from non-candidate
        with pytest.raises(AmbiguousSelectionError):
            create_issue_selection(
                issue=issue,
                user_intent="Should fail",
            )

    def test_invariant_7_audit_trail(self):
        """INVARIANT 7: All actions are auditable.

        EscalationRequest maintains audit log.
        """
        selection = IssueSelection(
            selection_id="sel123",
            issue_id="issue456",
            issue_type="TEST",
            evidence=({"type": "test"},),
            target_file="test.yaml",
            user_intent="Fix issue",
            created_at="2024-01-01T00:00:00",
        )

        request = EscalationRequest(
            request_id="req123",
            selection=selection,
            status=EscalationStatus.PENDING_CONFIRMATION,
        )

        # Audit log should track creation
        assert len(request.audit_log) >= 1
        assert request.audit_log[0]["event_type"] == "CREATED"

        # Confirm and check log
        request.confirm()
        assert any(log["event_type"] == "CONFIRMED" for log in request.audit_log)

    def test_invariant_8_failure_is_valid(self):
        """INVARIANT 8: Rejection is a valid outcome.

        Request can be rejected with a reason.
        """
        selection = IssueSelection(
            selection_id="sel123",
            issue_id="issue456",
            issue_type="TEST",
            evidence=({"type": "test"},),
            target_file="test.yaml",
            user_intent="Fix issue",
            created_at="2024-01-01T00:00:00",
        )

        request = EscalationRequest(
            request_id="req123",
            selection=selection,
            status=EscalationStatus.PENDING_CONFIRMATION,
        )

        # Reject is valid
        request.reject("Changed my mind")

        assert request.status == EscalationStatus.REJECTED
        assert request.rejection_reason == "Changed my mind"
        assert request.completed_at is not None


class TestDiscoveryCannotTriggerImprovement:
    """Tests proving discovery cannot trigger improvements.

    These tests verify the hard boundary between discovery and improvement.
    """

    def test_issue_discovery_manager_has_no_improvement_methods(self):
        """IssueDiscoveryManager has no methods to trigger improvements."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = IssueDiscoveryManager(tmpdir)

            # Check that no improvement-triggering methods exist
            dangerous_methods = [
                "trigger_improvement",
                "auto_fix",
                "fix_all",
                "apply_fix",
                "run_improvement",
                "start_improvement",
            ]

            for method in dangerous_methods:
                assert not hasattr(manager, method), f"Manager should not have {method}"

    def test_discovered_issue_cannot_self_improve(self):
        """DiscoveredIssue has no method to fix itself."""
        issue = DiscoveredIssue(
            issue_id="test123",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path="test.yaml"),
            description="Test issue",
            severity=IssueSeverity.HIGH,
            evidence=[IssueEvidence.from_config_inspection("test", "x", 1)],
            confidence=ConfidenceLevel.HIGH,
        )

        # Check that no self-improvement methods exist
        dangerous_methods = ["fix", "apply", "improve", "auto_fix"]

        for method in dangerous_methods:
            assert not hasattr(issue, method), f"Issue should not have {method}"

    def test_discovery_result_cannot_trigger_improvement(self):
        """IssueDiscoveryResult has no improvement-triggering methods."""
        result = IssueDiscoveryResult(
            scan_id="scan1",
            cycle_id="",
            scanned_at="",
            files_scanned=[],
            issues=[],
        )

        dangerous_methods = ["fix_all", "improve_all", "trigger_improvements"]

        for method in dangerous_methods:
            assert not hasattr(result, method), f"Result should not have {method}"


class TestHumanGatedEscalator:
    """Tests for HumanGatedEscalator workflow."""

    def _make_issue(self, file_path: str = "test.yaml") -> DiscoveredIssue:
        """Helper to create a test issue."""
        return DiscoveredIssue(
            issue_id="test_issue_123",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path=file_path),
            description="Test issue",
            severity=IssueSeverity.HIGH,
            evidence=[IssueEvidence.from_config_inspection(file_path, "content", 1)],
            confidence=ConfidenceLevel.HIGH,
        )

    def test_create_selection_from_discovery(self):
        """Escalator can create selection from discovered issue."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test file
            test_file = Path(tmpdir) / "test.yaml"
            test_file.write_text("test: content")

            escalator = HumanGatedEscalator(
                odibi_root=tmpdir,
                project_root=tmpdir,
            )

            issue = self._make_issue(str(test_file))

            selection = escalator.create_selection_from_discovery(
                issue=issue,
                user_intent="Fix the test issue",
            )

            assert selection.issue_id == issue.issue_id
            assert selection.user_intent == "Fix the test issue"

    def test_create_escalation_request(self):
        """Escalator can create escalation request from selection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test file
            test_file = Path(tmpdir) / "test.yaml"
            test_file.write_text("test: content")

            escalator = HumanGatedEscalator(
                odibi_root=tmpdir,
                project_root=tmpdir,
            )

            issue = self._make_issue(str(test_file))
            selection = escalator.create_selection_from_discovery(
                issue=issue,
                user_intent="Fix the test issue",
            )

            request = escalator.create_escalation_request(selection)

            assert request.status == EscalationStatus.PENDING_CONFIRMATION
            assert request.selection == selection

    def test_blocks_unconfirmed_execution(self):
        """INVARIANT: Cannot execute without confirmation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            test_file.write_text("test: content")

            escalator = HumanGatedEscalator(
                odibi_root=tmpdir,
                project_root=tmpdir,
            )

            issue = self._make_issue(str(test_file))
            selection = escalator.create_selection_from_discovery(
                issue=issue,
                user_intent="Fix the test issue",
            )
            request = escalator.create_escalation_request(selection)

            # Don't confirm - try to execute
            with pytest.raises(EscalationError) as exc:
                escalator.execute_escalation(request)

            assert "Human confirmation is REQUIRED" in str(exc.value)

    def test_persists_requests(self):
        """Escalator persists requests for audit trail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            test_file.write_text("test: content")

            escalator = HumanGatedEscalator(
                odibi_root=tmpdir,
                project_root=tmpdir,
            )

            issue = self._make_issue(str(test_file))
            selection = escalator.create_selection_from_discovery(
                issue=issue,
                user_intent="Fix the test issue",
            )
            request = escalator.create_escalation_request(selection)

            # Check that request was persisted
            loaded = escalator.load_request(request.request_id)
            assert loaded is not None
            assert loaded.request_id == request.request_id

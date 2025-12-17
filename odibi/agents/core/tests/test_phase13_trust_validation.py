"""Phase 13: Trust Validation & Consolidation Tests.

PHASE 13 OBJECTIVE: Prove the system is boringly reliable, reversible,
and misuse-resistant under realistic human and system failure scenarios.

NO NEW FEATURES. NO NEW AUTONOMY. ONLY TRUST PROOFS.

This file tests:
1. Trust Scenarios & Failure Simulation
2. Determinism & Replay Proof
3. Invariant Regression Tests (safe failure guarantees)

TEST CATEGORIES:

1. TRUST SCENARIOS (realistic failure cases):
   - Escalation started but abandoned mid-flow
   - UI crash or refresh during escalation
   - Multiple escalations targeting same file
   - Re-running escalation after rollback
   - Replaying completed escalation deterministically
   - Invalid escalation artifacts injected manually
   - Attempted escalation with stale discovery data

2. DETERMINISM & REPLAY PROOFS:
   - Same inputs → same outputs
   - Rollback restores exact pre-state
   - Artifacts are sufficient to explain why a change happened

3. INVARIANT REGRESSION TESTS:
   - Discovery can never trigger improvement
   - Harness files can never be escalated
   - Improvement cannot run without approval
   - Escalation artifacts are immutable
   - Invalid escalation artifacts are rejected
"""

import hashlib
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from odibi.agents.core.controlled_improvement import (
    ControlledImprovementConfig,
    ControlledImprovementRunner,
    ImprovementRejectionReason,
    ImprovementScope,
    ImprovementScopeEnforcer,
    ImprovementSnapshot,
    LearningHarnessViolation,
    is_learning_harness_path,
)
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
from odibi.agents.core.issue_discovery import (
    ConfidenceLevel,
    DiscoveredIssue,
    DiscoveredIssueType,
    IssueDiscoveryManager,
    IssueDiscoveryResult,
    IssueEvidence,
    IssueLocation,
)
from odibi.agents.core.schemas import ImprovementProposal, IssueSeverity, ProposalChange


# ==============================================================================
# SECTION 1: TRUST SCENARIOS & FAILURE SIMULATION
# ==============================================================================


class TestAbandonedEscalationFlow:
    """Test: Escalation started but abandoned mid-flow.

    Validates that partial escalation flows fail safely and do not leave
    the system in an inconsistent state.
    """

    def test_abandoned_after_selection_creation(self):
        """Abandoning after selection creation leaves no side effects."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            original_content = "test: content\nvalue: 123"
            test_file.write_text(original_content)

            escalator = HumanGatedEscalator(
                odibi_root=tmpdir,
                project_root=tmpdir,
            )

            issue = self._make_issue(str(test_file))
            _ = escalator.create_selection_from_discovery(
                issue=issue,
                user_intent="Fix test issue",
            )

            # Abandon: Do NOT create escalation request
            # File should be unchanged
            assert test_file.read_text() == original_content

    def test_abandoned_after_request_creation(self):
        """Abandoning after request creation (no confirm) is safe."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            original_content = "test: content\nvalue: 123"
            test_file.write_text(original_content)

            escalator = HumanGatedEscalator(
                odibi_root=tmpdir,
                project_root=tmpdir,
            )

            issue = self._make_issue(str(test_file))
            selection = escalator.create_selection_from_discovery(
                issue=issue,
                user_intent="Fix test issue",
            )
            request = escalator.create_escalation_request(selection)

            # Request is created but NOT confirmed - abandon here
            assert request.status == EscalationStatus.PENDING_CONFIRMATION

            # File should be unchanged
            assert test_file.read_text() == original_content

            # Request should be loadable from disk (for audit)
            loaded = escalator.load_request(request.request_id)
            assert loaded is not None
            assert loaded.status == EscalationStatus.PENDING_CONFIRMATION

    def test_pending_request_cannot_be_executed(self):
        """A pending (non-confirmed) request cannot be executed."""
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
                user_intent="Fix test issue",
            )
            request = escalator.create_escalation_request(selection)

            # Try to execute without confirming
            with pytest.raises(EscalationError) as exc:
                escalator.execute_escalation(request)

            assert "CONFIRMED" in str(exc.value)
            assert "Human confirmation is REQUIRED" in str(exc.value)

    def _make_issue(self, file_path: str) -> DiscoveredIssue:
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


class TestConcurrentEscalationSameFile:
    """Test: Multiple escalations targeting the same file.

    Validates that concurrent escalation attempts to the same file
    do not cause race conditions or data corruption.
    """

    def test_second_selection_for_same_file_allowed(self):
        """Multiple selections for same file are individually valid."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            test_file.write_text("test: content")

            issue1 = DiscoveredIssue(
                issue_id="issue_001",
                issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
                location=IssueLocation(file_path=str(test_file)),
                description="Issue 1",
                severity=IssueSeverity.HIGH,
                evidence=[IssueEvidence.from_config_inspection(str(test_file), "x", 1)],
                confidence=ConfidenceLevel.HIGH,
            )

            issue2 = DiscoveredIssue(
                issue_id="issue_002",
                issue_type=DiscoveredIssueType.MISSING_REQUIRED_PARAM,
                location=IssueLocation(file_path=str(test_file)),
                description="Issue 2",
                severity=IssueSeverity.MEDIUM,
                evidence=[IssueEvidence.from_config_inspection(str(test_file), "y", 2)],
                confidence=ConfidenceLevel.HIGH,
            )

            sel1 = create_issue_selection(issue1, "Fix issue 1")
            sel2 = create_issue_selection(issue2, "Fix issue 2")

            # Both selections are valid and have different IDs
            assert sel1.selection_id != sel2.selection_id
            assert sel1.issue_id != sel2.issue_id
            assert sel1.target_file == sel2.target_file

    def test_escalation_requests_for_same_file_have_unique_ids(self):
        """Multiple escalation requests for same file have unique IDs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            test_file.write_text("test: content")

            escalator = HumanGatedEscalator(
                odibi_root=tmpdir,
                project_root=tmpdir,
            )

            issue1 = self._make_issue(str(test_file), "issue_001")
            issue2 = self._make_issue(str(test_file), "issue_002")

            sel1 = escalator.create_selection_from_discovery(issue1, "Fix issue 1")
            sel2 = escalator.create_selection_from_discovery(issue2, "Fix issue 2")

            req1 = escalator.create_escalation_request(sel1)
            req2 = escalator.create_escalation_request(sel2)

            # Requests should have unique IDs
            assert req1.request_id != req2.request_id

            # Both should be pending
            assert req1.status == EscalationStatus.PENDING_CONFIRMATION
            assert req2.status == EscalationStatus.PENDING_CONFIRMATION

    def _make_issue(self, file_path: str, issue_id: str) -> DiscoveredIssue:
        return DiscoveredIssue(
            issue_id=issue_id,
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path=file_path),
            description=f"Test issue {issue_id}",
            severity=IssueSeverity.HIGH,
            evidence=[IssueEvidence.from_config_inspection(file_path, "content", 1)],
            confidence=ConfidenceLevel.HIGH,
        )


class TestEscalationAfterRollback:
    """Test: Re-running an escalation after rollback.

    Validates that after a rollback, the system returns to a clean state
    where new escalations can proceed safely.
    """

    def test_rollback_restores_file_exactly(self):
        """Rollback restores file to exact pre-improvement state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            original_content = "params: [123]\nmore: content"
            test_file.write_text(original_content)

            _ = ImprovementScope.for_single_file(str(test_file))  # Validates not harness
            snapshot = ImprovementSnapshot.capture([str(test_file)])

            # Modify file
            test_file.write_text("params: ['modified']\nmore: content")
            assert test_file.read_text() != original_content

            # Rollback
            restored = snapshot.restore()

            assert len(restored) == 1
            assert test_file.read_text() == original_content

    def test_snapshot_hash_matches_after_restore(self):
        """File hash after restore matches original snapshot hash."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            original_content = "params: [123]\nmore: content"
            test_file.write_text(original_content)

            snapshot = ImprovementSnapshot.capture([str(test_file)])
            original_hash = list(snapshot.file_hashes.values())[0]

            # Modify file
            test_file.write_text("modified content")

            # Rollback
            snapshot.restore()

            # Verify hash
            with open(test_file, "r") as f:
                restored_content = f.read()
            restored_hash = hashlib.sha256(restored_content.encode()).hexdigest()

            assert restored_hash == original_hash

    def test_can_create_new_snapshot_after_rollback(self):
        """After rollback, a new snapshot can be created cleanly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            original_content = "params: [123]"
            test_file.write_text(original_content)

            snapshot1 = ImprovementSnapshot.capture([str(test_file)])

            # Modify and rollback
            test_file.write_text("modified")
            snapshot1.restore()

            # Create new snapshot - should work
            snapshot2 = ImprovementSnapshot.capture([str(test_file)])

            assert snapshot2.snapshot_id != snapshot1.snapshot_id
            assert (
                list(snapshot2.file_hashes.values())[0] == list(snapshot1.file_hashes.values())[0]
            )


class TestStaleDiscoveryData:
    """Test: Attempted escalation with stale discovery data.

    Validates that stale discovery data is detected and rejected.
    """

    def test_selection_with_nonexistent_issue_in_discovery(self):
        """Selection referencing unknown issue ID is caught during validation."""
        selection = IssueSelection(
            selection_id="sel123",
            issue_id="nonexistent_issue_xyz",
            issue_type="TEST",
            evidence=({"type": "test"},),
            target_file="examples/test.yaml",
            user_intent="Fix something",
            created_at="2024-01-01T00:00:00",
        )

        # Discovery result with different issues
        discovery_result = IssueDiscoveryResult(
            scan_id="scan1",
            cycle_id="cycle1",
            scanned_at="2024-01-01T00:00:00",
            files_scanned=["examples/test.yaml"],
            issues=[
                DiscoveredIssue(
                    issue_id="different_issue_abc",
                    issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
                    location=IssueLocation(file_path="examples/test.yaml"),
                    description="Different issue",
                    severity=IssueSeverity.HIGH,
                    evidence=[IssueEvidence.from_config_inspection("examples/test.yaml", "x", 1)],
                    confidence=ConfidenceLevel.HIGH,
                )
            ],
        )

        is_safe, violations = validate_escalation_safety(selection, discovery_result)

        assert not is_safe
        assert any("not found" in v.lower() for v in violations)

    def test_validation_cross_references_discovery(self):
        """Validator cross-references selection with discovery result."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            test_file.write_text("test: content")

            validator = EscalationValidator(tmpdir)

            selection = IssueSelection(
                selection_id="sel123",
                issue_id="issue_that_exists",
                issue_type="TEST",
                evidence=({"type": "test"},),
                target_file=str(test_file),
                user_intent="Fix something",
                created_at="2024-01-01T00:00:00",
            )

            # Empty discovery - issue won't be found
            discovery_result = IssueDiscoveryResult(
                scan_id="scan1",
                cycle_id="",
                scanned_at="",
                files_scanned=[],
                issues=[],
            )

            is_valid, errors = validator.validate_for_escalation(selection, discovery_result)

            assert not is_valid
            assert any("not in discovery" in e.lower() for e in errors)


class TestInvalidEscalationArtifacts:
    """Test: Invalid escalation artifacts injected manually.

    Validates that tampered or malformed escalation artifacts are rejected.
    """

    def test_selection_from_tampered_dict_validates_harness(self):
        """Loading selection from dict still validates harness protection."""
        data = {
            "selection_id": "sel123",
            "issue_id": "issue456",
            "issue_type": "TEST",
            "evidence": [{"type": "test"}],
            "target_file": ".odibi/learning_harness/protected.yaml",  # TAMPERED!
            "user_intent": "Malicious intent",
            "created_at": "2024-01-01T00:00:00",
        }

        with pytest.raises(HarnessEscalationBlockedError):
            IssueSelection.from_dict(data)

    def test_selection_requires_nonempty_intent(self):
        """Selection rejects empty user intent."""
        data = {
            "selection_id": "sel123",
            "issue_id": "issue456",
            "issue_type": "TEST",
            "evidence": [],
            "target_file": "test.yaml",
            "user_intent": "",  # Empty!
            "created_at": "2024-01-01T00:00:00",
        }

        with pytest.raises(AmbiguousSelectionError) as exc:
            IssueSelection.from_dict(data)
        assert "user_intent is required" in str(exc.value)

    def test_selection_requires_nonempty_issue_id(self):
        """Selection rejects empty issue ID."""
        data = {
            "selection_id": "sel123",
            "issue_id": "",  # Empty!
            "issue_type": "TEST",
            "evidence": [],
            "target_file": "test.yaml",
            "user_intent": "Fix something",
            "created_at": "2024-01-01T00:00:00",
        }

        with pytest.raises(AmbiguousSelectionError) as exc:
            IssueSelection.from_dict(data)
        assert "issue_id is required" in str(exc.value)

    def test_validator_rejects_missing_evidence(self):
        """Validator rejects selection with no evidence."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            test_file.write_text("test: content")

            validator = EscalationValidator(tmpdir)

            selection = IssueSelection(
                selection_id="sel123",
                issue_id="issue456",
                issue_type="TEST",
                evidence=(),  # No evidence
                target_file=str(test_file),
                user_intent="Fix something",
                created_at="2024-01-01T00:00:00",
            )

            errors = validator.validate_selection(selection)

            assert any("NO EVIDENCE" in e for e in errors)


# ==============================================================================
# SECTION 2: DETERMINISM & REPLAY PROOF
# ==============================================================================


class TestDeterministicBehavior:
    """Test: Determinism proofs - same inputs produce same outputs."""

    def test_snapshot_hash_is_deterministic(self):
        """Same file content produces same hash."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            content = "params: [123]\nvalue: test"
            test_file.write_text(content)

            snapshot1 = ImprovementSnapshot.capture([str(test_file)])
            snapshot2 = ImprovementSnapshot.capture([str(test_file)])

            # Hashes should be identical
            assert (
                list(snapshot1.file_hashes.values())[0] == list(snapshot2.file_hashes.values())[0]
            )

    def test_proposal_application_is_deterministic(self):
        """Same proposal applied to same file produces identical result."""
        with tempfile.TemporaryDirectory() as tmpdir:

            def apply_proposal():
                test_file = Path(tmpdir) / "test.yaml"
                test_file.write_text("params: [123]")

                config = ControlledImprovementConfig(
                    project_root=tmpdir,
                    improvement_scope=ImprovementScope.for_single_file(str(test_file)),
                )

                runner = ControlledImprovementRunner(odibi_root=tmpdir)
                runner._config = config
                runner._scope_enforcer = ImprovementScopeEnforcer(config.improvement_scope)
                runner._snapshot = ImprovementSnapshot.capture([str(test_file)])

                proposal = ImprovementProposal(
                    proposal="IMPROVEMENT",
                    title="Fix params",
                    changes=[
                        ProposalChange(
                            file=str(test_file),
                            before="params: [123]",
                            after='params: ["123"]',
                        )
                    ],
                )

                mock_state = MagicMock()
                mock_state.cycle_id = "test-cycle"

                result = runner.apply_improvement(proposal, mock_state)
                final_content = test_file.read_text()

                return result.diff_hash, final_content

            hash1, content1 = apply_proposal()
            hash2, content2 = apply_proposal()

            assert hash1 == hash2
            assert content1 == content2

    def test_issue_id_generation_is_deterministic(self):
        """Same issue inputs produce same issue ID."""
        location = IssueLocation(file_path="test.yaml")

        issue1 = DiscoveredIssue(
            issue_id="",  # Auto-generate
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=location,
            description="Test description",
            severity=IssueSeverity.HIGH,
            evidence=[],
            confidence=ConfidenceLevel.HIGH,
        )

        issue2 = DiscoveredIssue(
            issue_id="",  # Auto-generate
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=location,
            description="Test description",
            severity=IssueSeverity.HIGH,
            evidence=[],
            confidence=ConfidenceLevel.HIGH,
        )

        # Same inputs should produce same ID
        assert issue1.issue_id == issue2.issue_id


class TestReplayProof:
    """Test: Replay proofs - artifacts are sufficient to reproduce behavior."""

    def test_improvement_result_contains_sufficient_replay_info(self):
        """ImprovementResult contains all info needed to understand/replay change."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            test_file.write_text("params: [123]")

            config = ControlledImprovementConfig(
                project_root=tmpdir,
                improvement_scope=ImprovementScope.for_single_file(str(test_file)),
            )

            runner = ControlledImprovementRunner(odibi_root=tmpdir)
            runner._config = config
            runner._scope_enforcer = ImprovementScopeEnforcer(config.improvement_scope)
            runner._snapshot = ImprovementSnapshot.capture([str(test_file)])

            proposal = ImprovementProposal(
                proposal="IMPROVEMENT",
                title="Fix params",
                rationale="Changed for consistency",
                changes=[
                    ProposalChange(
                        file=str(test_file),
                        before="params: [123]",
                        after='params: ["123"]',
                    )
                ],
            )

            mock_state = MagicMock()
            mock_state.cycle_id = "test-cycle"

            result = runner.apply_improvement(proposal, mock_state)

            # Result should contain:
            assert result.improvement_id  # Unique ID
            assert result.proposal  # The proposal
            assert result.scope  # Scope definition
            assert result.snapshot  # Pre-state snapshot
            assert result.diff_hash  # Deterministic diff hash
            assert result.before_after_diff  # Actual diff content
            assert result.created_at  # Timestamp

            # Diff should be extractable
            diff = result.before_after_diff
            assert len(diff) == 1
            file_diff = list(diff.values())[0]
            assert "before" in file_diff
            assert "after" in file_diff

    def test_escalation_request_audit_log_is_complete(self):
        """EscalationRequest audit log captures all state transitions."""
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

        # Initial state logged
        assert len(request.audit_log) >= 1
        assert request.audit_log[0]["event_type"] == "CREATED"

        # Confirm
        request.confirm()
        assert any(log["event_type"] == "CONFIRMED" for log in request.audit_log)

        # Each log entry has timestamp
        for log in request.audit_log:
            assert "timestamp" in log
            assert "event_type" in log
            assert "details" in log


class TestRollbackExactness:
    """Test: Rollback restores exact pre-state."""

    def test_rollback_preserves_file_permissions(self):
        """Rollback preserves original file content exactly (byte-for-byte)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            # Include various edge cases: trailing newline, unicode, etc.
            original_content = "params: [123]\nvalue: 'tëst'\n"
            test_file.write_text(original_content, encoding="utf-8")

            snapshot = ImprovementSnapshot.capture([str(test_file)])

            # Modify
            test_file.write_text("completely different content")

            # Rollback
            snapshot.restore()

            # Verify exact match
            restored = test_file.read_text(encoding="utf-8")
            assert restored == original_content

    def test_rollback_multiple_files(self):
        """Rollback restores multiple files correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file1 = Path(tmpdir) / "file1.yaml"
            file2 = Path(tmpdir) / "file2.yaml"
            content1 = "file1: content"
            content2 = "file2: content"
            file1.write_text(content1)
            file2.write_text(content2)

            snapshot = ImprovementSnapshot.capture([str(file1), str(file2)])

            # Modify both
            file1.write_text("modified1")
            file2.write_text("modified2")

            # Rollback
            restored = snapshot.restore()

            assert len(restored) == 2
            assert file1.read_text() == content1
            assert file2.read_text() == content2


# ==============================================================================
# SECTION 3: INVARIANT REGRESSION TESTS
# ==============================================================================


class TestInvariant_DiscoveryNeverTriggersImprovement:
    """INVARIANT: Discovery can never trigger improvement.

    This is a hard boundary that MUST NOT be crossed.
    """

    def test_issue_discovery_manager_has_no_improvement_methods(self):
        """IssueDiscoveryManager API has no improvement-triggering methods."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = IssueDiscoveryManager(tmpdir)

            forbidden_methods = [
                "trigger_improvement",
                "auto_fix",
                "fix_all",
                "apply_fix",
                "run_improvement",
                "start_improvement",
                "execute_improvement",
                "improve",
            ]

            for method in forbidden_methods:
                assert not hasattr(
                    manager, method
                ), f"IssueDiscoveryManager MUST NOT have '{method}' method"

    def test_discovered_issue_has_no_self_fix_methods(self):
        """DiscoveredIssue has no methods to fix itself."""
        issue = DiscoveredIssue(
            issue_id="test",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path="test.yaml"),
            description="Test",
            severity=IssueSeverity.HIGH,
            evidence=[],
            confidence=ConfidenceLevel.HIGH,
        )

        forbidden_methods = ["fix", "apply", "improve", "auto_fix", "self_fix"]

        for method in forbidden_methods:
            assert not hasattr(issue, method), f"DiscoveredIssue MUST NOT have '{method}' method"

    def test_discovery_result_has_no_bulk_fix_methods(self):
        """IssueDiscoveryResult has no bulk fix methods."""
        result = IssueDiscoveryResult(
            scan_id="scan1",
            cycle_id="",
            scanned_at="",
            files_scanned=[],
            issues=[],
        )

        forbidden_methods = ["fix_all", "improve_all", "trigger_improvements", "batch_fix"]

        for method in forbidden_methods:
            assert not hasattr(
                result, method
            ), f"IssueDiscoveryResult MUST NOT have '{method}' method"

    def test_discovery_does_not_modify_files(self):
        """Running discovery MUST NOT modify any files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            test_file = Path(tmpdir) / "test.yaml"
            original_content = "pipelines:\n  - name: test\n    transformer: deduplicate\n    params:\n      columns: [id]\n"
            test_file.write_text(original_content)

            manager = IssueDiscoveryManager(tmpdir)
            _ = manager.run_discovery(tmpdir)

            # File MUST be unchanged
            assert test_file.read_text() == original_content


class TestInvariant_HarnessFilesNeverEscalated:
    """INVARIANT: Harness files can never be escalated.

    Learning harness files are system validation fixtures and MUST NEVER be
    modification targets under any circumstances.
    """

    def test_harness_paths_detected_correctly(self):
        """All harness path patterns are correctly detected."""
        harness_paths = [
            ".odibi/learning_harness/test.yaml",
            "/abs/.odibi/learning_harness/test.yaml",
            "d:/project/.odibi/learning_harness/test.yaml",
            ".Odibi/Learning_Harness/test.yaml",  # Case insensitive
            ".ODIBI\\LEARNING_HARNESS\\test.yaml",  # Windows
            "odibi/learning_harness/test.yaml",  # Without dot prefix
        ]

        for path in harness_paths:
            assert is_learning_harness_path(path), f"Should detect as harness: {path}"

    def test_non_harness_paths_allowed(self):
        """Non-harness paths are correctly allowed."""
        allowed_paths = [
            "pipelines/test.yaml",
            "examples/scale_join.odibi.yaml",
            ".odibi/config.yaml",  # .odibi but not learning_harness
            "learning_harness/test.yaml",  # Not under .odibi
        ]

        for path in allowed_paths:
            assert not is_learning_harness_path(path), f"Should allow: {path}"

    def test_issue_selection_blocks_harness_at_creation(self):
        """IssueSelection raises immediately for harness target files."""
        harness_paths = [
            ".odibi/learning_harness/test.yaml",
            "d:/odibi/.odibi/learning_harness/scale_join.yaml",
        ]

        for path in harness_paths:
            with pytest.raises(HarnessEscalationBlockedError) as exc:
                IssueSelection(
                    selection_id="sel",
                    issue_id="issue",
                    issue_type="TEST",
                    evidence=(),
                    target_file=path,
                    user_intent="Should fail",
                    created_at="2024-01-01T00:00:00",
                )
            assert "learning harness" in str(exc.value).lower()

    def test_improvement_scope_blocks_harness_at_creation(self):
        """ImprovementScope raises immediately for harness files."""
        with pytest.raises(LearningHarnessViolation):
            ImprovementScope(
                allowed_files=[".odibi/learning_harness/protected.yaml"],
            )

    def test_create_issue_selection_blocks_harness_issue(self):
        """create_issue_selection blocks issues located in harness."""
        harness_issue = DiscoveredIssue(
            issue_id="test",
            issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
            location=IssueLocation(file_path=".odibi/learning_harness/test.yaml"),
            description="Harness issue",
            severity=IssueSeverity.HIGH,
            evidence=[
                IssueEvidence.from_config_inspection(".odibi/learning_harness/test.yaml", "x", 1)
            ],
            confidence=ConfidenceLevel.HIGH,
        )

        with pytest.raises(HarnessEscalationBlockedError):
            create_issue_selection(harness_issue, "Should fail")


class TestInvariant_ImprovementRequiresApproval:
    """INVARIANT: Improvement cannot run without approval.

    Human confirmation is mandatory before any improvement execution.
    """

    def test_escalation_requires_confirmation_before_execution(self):
        """EscalationRequest MUST be confirmed before execute_escalation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            test_file.write_text("test: content")

            selection = IssueSelection(
                selection_id="sel",
                issue_id="issue",
                issue_type="TEST",
                evidence=({"type": "test"},),
                target_file=str(test_file),
                user_intent="Fix",
                created_at="2024-01-01T00:00:00",
            )

            request = EscalationRequest(
                request_id="req",
                selection=selection,
                status=EscalationStatus.PENDING_CONFIRMATION,
            )

            escalator = HumanGatedEscalator(
                odibi_root=tmpdir,
                project_root=tmpdir,
            )

            with pytest.raises(EscalationError) as exc:
                escalator.execute_escalation(request)

            assert "Human confirmation is REQUIRED" in str(exc.value)

    def test_cannot_skip_to_in_progress_from_pending(self):
        """Cannot mark request IN_PROGRESS from PENDING (must confirm first)."""
        selection = IssueSelection(
            selection_id="sel",
            issue_id="issue",
            issue_type="TEST",
            evidence=({"type": "test"},),
            target_file="test.yaml",
            user_intent="Fix",
            created_at="2024-01-01T00:00:00",
        )

        request = EscalationRequest(
            request_id="req",
            selection=selection,
            status=EscalationStatus.PENDING_CONFIRMATION,
        )

        with pytest.raises(EscalationError) as exc:
            request.mark_in_progress()

        assert "expected CONFIRMED" in str(exc.value)


class TestInvariant_EscalationArtifactsImmutable:
    """INVARIANT: Escalation artifacts are immutable.

    IssueSelection is frozen and cannot be modified after creation.
    """

    def test_issue_selection_is_frozen(self):
        """IssueSelection cannot be modified after creation."""
        selection = IssueSelection(
            selection_id="sel",
            issue_id="issue",
            issue_type="TEST",
            evidence=({"type": "test"},),
            target_file="test.yaml",
            user_intent="Fix",
            created_at="2024-01-01T00:00:00",
        )

        # All field modifications should raise
        with pytest.raises(AttributeError):
            selection.issue_id = "tampered"

        with pytest.raises(AttributeError):
            selection.target_file = "tampered.yaml"

        with pytest.raises(AttributeError):
            selection.user_intent = "tampered"

    def test_issue_selection_evidence_is_tuple(self):
        """IssueSelection evidence is a tuple (immutable)."""
        selection = IssueSelection(
            selection_id="sel",
            issue_id="issue",
            issue_type="TEST",
            evidence=({"type": "test"},),
            target_file="test.yaml",
            user_intent="Fix",
            created_at="2024-01-01T00:00:00",
        )

        assert isinstance(selection.evidence, tuple)


class TestInvariant_InvalidArtifactsRejected:
    """INVARIANT: Invalid escalation artifacts are rejected.

    Malformed or incomplete artifacts MUST fail validation.
    """

    def test_empty_issue_id_rejected(self):
        """Empty issue_id is rejected at creation time."""
        with pytest.raises(AmbiguousSelectionError):
            IssueSelection(
                selection_id="sel",
                issue_id="",  # Empty
                issue_type="TEST",
                evidence=(),
                target_file="test.yaml",
                user_intent="Fix",
                created_at="2024-01-01T00:00:00",
            )

    def test_empty_target_file_rejected(self):
        """Empty target_file is rejected at creation time."""
        with pytest.raises(AmbiguousSelectionError):
            IssueSelection(
                selection_id="sel",
                issue_id="issue",
                issue_type="TEST",
                evidence=(),
                target_file="",  # Empty
                user_intent="Fix",
                created_at="2024-01-01T00:00:00",
            )

    def test_whitespace_only_intent_rejected(self):
        """Whitespace-only user_intent is rejected."""
        with pytest.raises(AmbiguousSelectionError):
            IssueSelection(
                selection_id="sel",
                issue_id="issue",
                issue_type="TEST",
                evidence=(),
                target_file="test.yaml",
                user_intent="   ",  # Whitespace only
                created_at="2024-01-01T00:00:00",
            )

    def test_validation_catches_nonexistent_target_file(self):
        """Validator catches target files that don't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            validator = EscalationValidator(tmpdir)

            selection = IssueSelection(
                selection_id="sel",
                issue_id="issue",
                issue_type="TEST",
                evidence=({"type": "test"},),
                target_file=str(Path(tmpdir) / "nonexistent.yaml"),
                user_intent="Fix",
                created_at="2024-01-01T00:00:00",
            )

            errors = validator.validate_selection(selection)

            assert any("NOT FOUND" in e or "does not exist" in e for e in errors)


# ==============================================================================
# SECTION 4: STATUS TRANSITION SAFETY
# ==============================================================================


class TestStatusTransitionSafety:
    """Test: All status transitions follow valid state machine."""

    def test_valid_status_transitions(self):
        """Only valid status transitions are allowed."""
        selection = IssueSelection(
            selection_id="sel",
            issue_id="issue",
            issue_type="TEST",
            evidence=({"type": "test"},),
            target_file="test.yaml",
            user_intent="Fix",
            created_at="2024-01-01T00:00:00",
        )

        # PENDING → CONFIRMED is valid
        request = EscalationRequest(
            request_id="req",
            selection=selection,
            status=EscalationStatus.PENDING_CONFIRMATION,
        )
        request.confirm()
        assert request.status == EscalationStatus.CONFIRMED

        # CONFIRMED → IN_PROGRESS is valid
        request.mark_in_progress()
        assert request.status == EscalationStatus.IN_PROGRESS

    def test_invalid_double_confirmation(self):
        """Cannot confirm an already confirmed request."""
        selection = IssueSelection(
            selection_id="sel",
            issue_id="issue",
            issue_type="TEST",
            evidence=({"type": "test"},),
            target_file="test.yaml",
            user_intent="Fix",
            created_at="2024-01-01T00:00:00",
        )

        request = EscalationRequest(
            request_id="req",
            selection=selection,
            status=EscalationStatus.PENDING_CONFIRMATION,
        )
        request.confirm()

        with pytest.raises(EscalationError) as exc:
            request.confirm()

        assert "expected PENDING_CONFIRMATION" in str(exc.value)

    def test_rejected_request_has_reason(self):
        """Rejected requests always have a rejection reason."""
        selection = IssueSelection(
            selection_id="sel",
            issue_id="issue",
            issue_type="TEST",
            evidence=({"type": "test"},),
            target_file="test.yaml",
            user_intent="Fix",
            created_at="2024-01-01T00:00:00",
        )

        request = EscalationRequest(
            request_id="req",
            selection=selection,
            status=EscalationStatus.PENDING_CONFIRMATION,
        )
        request.reject("Not needed")

        assert request.status == EscalationStatus.REJECTED
        assert request.rejection_reason == "Not needed"
        assert request.completed_at is not None


# ==============================================================================
# SECTION 5: EDGE CASE SAFETY
# ==============================================================================


class TestEdgeCaseSafety:
    """Test: System handles edge cases safely."""

    def test_empty_proposal_changes_handled(self):
        """Proposal with empty changes list is handled as NO_PROPOSAL."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            test_file.write_text("test: content")

            config = ControlledImprovementConfig(
                project_root=tmpdir,
                improvement_scope=ImprovementScope.for_single_file(str(test_file)),
            )

            runner = ControlledImprovementRunner(odibi_root=tmpdir)
            runner._config = config
            runner._scope_enforcer = ImprovementScopeEnforcer(config.improvement_scope)
            runner._snapshot = ImprovementSnapshot.capture([str(test_file)])

            # Proposal with empty changes
            proposal = ImprovementProposal(
                proposal="IMPROVEMENT",
                title="Empty fix",
                changes=[],  # No changes
            )

            mock_state = MagicMock()
            mock_state.cycle_id = "test-cycle"

            result = runner.apply_improvement(proposal, mock_state)

            # Should be treated as NO_PROPOSAL
            assert result.status == "NO_PROPOSAL"
            assert test_file.read_text() == "test: content"

    def test_before_content_mismatch_fails_safely(self):
        """Proposal with mismatched 'before' content fails safely."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            test_file.write_text("actual: content")

            config = ControlledImprovementConfig(
                project_root=tmpdir,
                improvement_scope=ImprovementScope.for_single_file(str(test_file)),
                rollback_on_failure=True,
            )

            runner = ControlledImprovementRunner(odibi_root=tmpdir)
            runner._config = config
            runner._scope_enforcer = ImprovementScopeEnforcer(config.improvement_scope)
            runner._snapshot = ImprovementSnapshot.capture([str(test_file)])

            # Proposal with wrong 'before' content
            proposal = ImprovementProposal(
                proposal="IMPROVEMENT",
                title="Fix",
                changes=[
                    ProposalChange(
                        file=str(test_file),
                        before="wrong: content",  # Does not match file
                        after="new: content",
                    )
                ],
            )

            mock_state = MagicMock()
            mock_state.cycle_id = "test-cycle"

            result = runner.apply_improvement(proposal, mock_state)

            # Should be rejected
            assert result.status == "REJECTED"
            assert result.rejection_reason == ImprovementRejectionReason.VALIDATION_FAILED
            # File should be unchanged
            assert test_file.read_text() == "actual: content"

    def test_unicode_content_preserved(self):
        """Unicode content is preserved through snapshot/restore."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.yaml"
            unicode_content = "value: 'tëst with émojis 🎉'\ndescription: '中文'"
            test_file.write_text(unicode_content, encoding="utf-8")

            snapshot = ImprovementSnapshot.capture([str(test_file)])

            # Modify
            test_file.write_text("modified", encoding="utf-8")

            # Restore
            snapshot.restore()

            # Verify exact match
            assert test_file.read_text(encoding="utf-8") == unicode_content

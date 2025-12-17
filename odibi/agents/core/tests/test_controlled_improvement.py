"""Tests for Phase 9.G: Controlled Improvement Activation.

Test coverage:
1. ImprovementAgent runs only once (single-shot)
2. Only scoped files can be modified
3. Diff is minimal and deterministic
4. A second improvement attempt is blocked
5. Rollback works if validation fails
6. Unrelated pipelines remain stable
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from odibi.agents.core.controlled_improvement import (
    ControlledImprovementConfig,
    ControlledImprovementRunner,
    ImprovementResult,
    ImprovementRejectionReason,
    ImprovementScope,
    ImprovementScopeEnforcer,
    ImprovementScopeViolation,
    ImprovementSnapshot,
    LearningHarnessViolation,
    is_learning_harness_path,
)
from odibi.agents.core.cycle import CycleState, CycleStep, GoldenProjectConfig
from odibi.agents.core.schemas import ImprovementProposal, ProposalChange, ProposalImpact


class TestImprovementScope:
    """Tests for ImprovementScope enforcement."""

    def test_single_file_scope(self):
        """ImprovementScope.for_single_file() creates correct scope."""
        scope = ImprovementScope.for_single_file(
            "/path/to/scale_join.odibi.yaml",
            "Fix validation error",
        )

        assert len(scope.allowed_files) == 1
        assert scope.max_changes_per_file == 1
        assert scope.max_lines_changed == 10
        assert scope.description == "Fix validation error"

    def test_file_allowed_check(self):
        """is_file_allowed() correctly identifies allowed files."""
        scope = ImprovementScope(
            allowed_files=["scale_join.odibi.yaml"],
        )

        assert scope.is_file_allowed("/project/scale_join.odibi.yaml")  # Full path match
        assert scope.is_file_allowed("scale_join.odibi.yaml")  # Exact match
        assert not scope.is_file_allowed("/project/other.yaml")
        assert not scope.is_file_allowed("/project/skew_test.odibi.yaml")

    def test_validate_proposal_in_scope(self):
        """validate_proposal() accepts proposals within scope."""
        scope = ImprovementScope(
            allowed_files=["scale_join.odibi.yaml"],
            max_changes_per_file=1,
            max_lines_changed=10,
        )

        proposal = ImprovementProposal(
            proposal="IMPROVEMENT",
            title="Fix params type",
            rationale="Change string to correct type",
            changes=[
                ProposalChange(
                    file="scale_join.odibi.yaml",
                    before="params: [123]",
                    after='params: ["123"]',
                )
            ],
            impact=ProposalImpact(risk="LOW", expected_benefit="Fix validation"),
        )

        is_valid, violations = scope.validate_proposal(proposal)

        assert is_valid
        assert len(violations) == 0

    def test_validate_proposal_out_of_scope(self):
        """validate_proposal() rejects proposals outside scope."""
        scope = ImprovementScope(
            allowed_files=["scale_join.odibi.yaml"],
        )

        proposal = ImprovementProposal(
            proposal="IMPROVEMENT",
            title="Fix other file",
            changes=[
                ProposalChange(
                    file="skew_test.odibi.yaml",  # Not in scope
                    before="old",
                    after="new",
                )
            ],
        )

        is_valid, violations = scope.validate_proposal(proposal)

        assert not is_valid
        assert len(violations) == 1
        assert "not in allowed scope" in violations[0]


class TestLearningHarnessExclusion:
    """Tests for learning harness path exclusion (hard rule).

    Files under .odibi/learning_harness/ are system validation fixtures
    and must NEVER be improvement targets.
    """

    def test_is_learning_harness_path_detects_harness_files(self):
        """is_learning_harness_path() correctly identifies harness paths."""
        # Unix-style paths
        assert is_learning_harness_path(".odibi/learning_harness/scale_join.odibi.yaml")
        assert is_learning_harness_path("/project/.odibi/learning_harness/stress_test.yaml")
        assert is_learning_harness_path("path/to/.odibi/learning_harness/nested/config.yaml")

        # Windows-style paths
        assert is_learning_harness_path(".odibi\\learning_harness\\scale_join.odibi.yaml")
        assert is_learning_harness_path("C:\\projects\\.odibi\\learning_harness\\test.yaml")

        # Mixed case (should be case-insensitive)
        assert is_learning_harness_path(".ODIBI/LEARNING_HARNESS/test.yaml")
        assert is_learning_harness_path(".Odibi/Learning_Harness/Test.yaml")

    def test_is_learning_harness_path_allows_non_harness_files(self):
        """is_learning_harness_path() returns False for non-harness paths."""
        assert not is_learning_harness_path("pipelines/scale_join.odibi.yaml")
        assert not is_learning_harness_path("/project/configs/test.yaml")
        assert not is_learning_harness_path(".odibi/config.yaml")  # Not in learning_harness
        assert not is_learning_harness_path("learning_harness/test.yaml")  # Not under .odibi

    def test_scope_rejects_harness_file_on_creation(self):
        """ImprovementScope raises LearningHarnessViolation for harness files."""
        with pytest.raises(LearningHarnessViolation) as exc_info:
            ImprovementScope(
                allowed_files=[".odibi/learning_harness/scale_join.odibi.yaml"],
            )

        assert "scale_join.odibi.yaml" in str(exc_info.value)
        assert "system validation fixture" in str(exc_info.value)
        assert "cannot be modified" in str(exc_info.value)

    def test_for_single_file_rejects_harness_file(self):
        """ImprovementScope.for_single_file() rejects harness files."""
        with pytest.raises(LearningHarnessViolation) as exc_info:
            ImprovementScope.for_single_file(
                "/project/.odibi/learning_harness/stress_test.odibi.yaml",
                "Attempt to improve harness file",
            )

        assert "stress_test.odibi.yaml" in str(exc_info.value)

    def test_scope_accepts_non_harness_user_pipeline(self):
        """ImprovementScope accepts normal user pipeline configs."""
        scope = ImprovementScope(
            allowed_files=["pipelines/user_pipeline.odibi.yaml"],
        )
        assert len(scope.allowed_files) == 1

        scope2 = ImprovementScope.for_single_file(
            "/project/configs/scale_join.odibi.yaml",
            "User's own config",
        )
        assert len(scope2.allowed_files) == 1

    def test_scope_rejects_any_harness_file_in_list(self):
        """ImprovementScope rejects if ANY file in allowed_files is a harness file."""
        with pytest.raises(LearningHarnessViolation):
            ImprovementScope(
                allowed_files=[
                    "pipelines/user_config.yaml",  # OK
                    ".odibi/learning_harness/harness_config.yaml",  # NOT OK
                ],
            )

    def test_learning_harness_violation_has_clear_message(self):
        """LearningHarnessViolation provides actionable error message."""
        error = LearningHarnessViolation(".odibi/learning_harness/test.yaml")

        assert ".odibi/learning_harness/test.yaml" in str(error)
        assert "system validation fixture" in str(error)
        assert "cannot be modified" in str(error)
        assert "regression checks" in str(error)


class TestImprovementSnapshot:
    """Tests for ImprovementSnapshot capture and restore."""

    def test_capture_creates_snapshot(self):
        """ImprovementSnapshot.capture() creates snapshot with hashes."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = Path(temp_dir) / "test.yaml"
            test_file.write_text("original: content")

            snapshot = ImprovementSnapshot.capture([str(test_file)])

            assert snapshot.snapshot_id
            assert len(snapshot.files) == 1
            assert len(snapshot.file_hashes) == 1
            assert "original: content" in snapshot.files[str(test_file.resolve())]

    def test_restore_reverts_changes(self):
        """ImprovementSnapshot.restore() reverts file changes."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = Path(temp_dir) / "test.yaml"
            original_content = "original: content"
            test_file.write_text(original_content)

            snapshot = ImprovementSnapshot.capture([str(test_file)])

            test_file.write_text("modified: content")
            assert test_file.read_text() == "modified: content"

            restored = snapshot.restore()

            assert len(restored) == 1
            assert test_file.read_text() == original_content

    def test_verify_unchanged_detects_modifications(self):
        """verify_unchanged() detects when files have changed."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = Path(temp_dir) / "test.yaml"
            test_file.write_text("original: content")

            snapshot = ImprovementSnapshot.capture([str(test_file)])

            unchanged, changed = snapshot.verify_unchanged()
            assert unchanged
            assert len(changed) == 0

            test_file.write_text("modified: content")

            unchanged, changed = snapshot.verify_unchanged()
            assert not unchanged
            assert len(changed) == 1

    def test_get_diff_for_file(self):
        """get_diff_for_file() returns before/after content."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = Path(temp_dir) / "test.yaml"
            test_file.write_text("original: content")

            snapshot = ImprovementSnapshot.capture([str(test_file)])
            test_file.write_text("modified: content")

            diff = snapshot.get_diff_for_file(str(test_file))

            assert diff is not None
            assert diff["before"] == "original: content"
            assert diff["after"] == "modified: content"


class TestImprovementScopeEnforcer:
    """Tests for ImprovementScopeEnforcer runtime checks."""

    def test_check_file_access_within_scope(self):
        """check_file_access() allows writes to scoped files."""
        scope = ImprovementScope(allowed_files=["scale_join.odibi.yaml"])
        enforcer = ImprovementScopeEnforcer(scope)

        assert enforcer.check_file_access("scale_join.odibi.yaml", "write")
        assert not enforcer.has_violations()

    def test_check_file_access_outside_scope(self):
        """check_file_access() blocks writes to non-scoped files."""
        scope = ImprovementScope(allowed_files=["scale_join.odibi.yaml"])
        enforcer = ImprovementScopeEnforcer(scope)

        result = enforcer.check_file_access("other.yaml", "write")

        assert not result
        assert enforcer.has_violations()
        assert "out of scope" in enforcer.get_violations()[0]

    def test_assert_no_violations_raises(self):
        """assert_no_violations() raises when violations exist."""
        scope = ImprovementScope(allowed_files=["scale_join.odibi.yaml"])
        enforcer = ImprovementScopeEnforcer(scope)

        enforcer.check_file_access("other.yaml", "write")

        with pytest.raises(ImprovementScopeViolation):
            enforcer.assert_no_violations()


class TestControlledImprovementConfig:
    """Tests for ControlledImprovementConfig validation."""

    def test_max_improvements_must_be_one(self):
        """ControlledImprovementConfig enforces max_improvements=1."""
        config = ControlledImprovementConfig(
            project_root="/project",
            improvement_scope=ImprovementScope.for_single_file("test.yaml"),
        )

        assert config.max_improvements == 1

    def test_validate_requires_scope(self):
        """validate() requires improvement_scope to be set."""
        config = ControlledImprovementConfig(
            project_root="/project",
            improvement_scope=None,  # Missing scope
        )

        errors = config.validate()

        assert len(errors) > 0
        assert any("improvement_scope" in e for e in errors)

    def test_validate_requires_golden_projects_for_regression(self):
        """validate() requires golden_projects when regression check is enabled."""
        config = ControlledImprovementConfig(
            project_root="/project",
            improvement_scope=ImprovementScope.for_single_file("test.yaml"),
            require_regression_check=True,
            golden_projects=[],  # Empty
        )

        errors = config.validate()

        assert len(errors) > 0
        assert any("golden_projects" in e for e in errors)

    def test_valid_config_passes(self):
        """A fully configured ControlledImprovementConfig passes validation."""
        config = ControlledImprovementConfig(
            project_root="/project",
            improvement_scope=ImprovementScope.for_single_file("scale_join.odibi.yaml"),
            require_regression_check=True,
            golden_projects=[GoldenProjectConfig(name="skew_test", path="skew_test.odibi.yaml")],
        )

        errors = config.validate()

        assert len(errors) == 0


class TestControlledImprovementRunner:
    """Tests for ControlledImprovementRunner behavior."""

    def test_improvement_step_runs_once(self):
        """Improvement step should run only once (single-shot)."""
        runner = ControlledImprovementRunner(odibi_root="d:/odibi")
        runner._improvement_applied = False

        mock_state = MagicMock(spec=CycleState)
        mock_state.config = MagicMock()
        mock_state.config.max_improvements = 1

        should_skip_first, _ = runner._should_skip_step(CycleStep.IMPROVEMENT_PROPOSAL, mock_state)
        assert not should_skip_first, "First improvement should NOT be skipped"

        runner._improvement_applied = True

        should_skip_second, reason = runner._should_skip_step(
            CycleStep.IMPROVEMENT_PROPOSAL, mock_state
        )
        assert should_skip_second, "Second improvement MUST be skipped"
        assert "single-shot" in reason.lower()

    def test_review_skipped_without_improvement(self):
        """Review step should be skipped if no improvement was applied."""
        runner = ControlledImprovementRunner(odibi_root="d:/odibi")
        runner._improvement_applied = False

        mock_state = MagicMock(spec=CycleState)

        should_skip, reason = runner._should_skip_step(CycleStep.REVIEW, mock_state)

        assert should_skip
        assert "no improvement" in reason.lower()

    def test_apply_improvement_enforces_scope(self):
        """apply_improvement() rejects proposals outside scope."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = Path(temp_dir) / "scale_join.odibi.yaml"
            test_file.write_text("params: [123]")

            config = ControlledImprovementConfig(
                project_root=temp_dir,
                improvement_scope=ImprovementScope.for_single_file(str(test_file)),
            )

            runner = ControlledImprovementRunner(odibi_root="d:/odibi")
            runner._config = config
            runner._scope_enforcer = ImprovementScopeEnforcer(config.improvement_scope)
            runner._snapshot = ImprovementSnapshot.capture([str(test_file)])

            bad_proposal = ImprovementProposal(
                proposal="IMPROVEMENT",
                title="Modify wrong file",
                changes=[
                    ProposalChange(
                        file="other_file.yaml",  # Not in scope
                        before="old",
                        after="new",
                    )
                ],
            )

            mock_state = MagicMock(spec=CycleState)
            mock_state.cycle_id = "test-cycle"

            result = runner.apply_improvement(bad_proposal, mock_state)

            assert result.status == "REJECTED"
            assert result.rejection_reason == ImprovementRejectionReason.SCOPE_VIOLATION

    def test_apply_improvement_success(self):
        """apply_improvement() applies valid proposals."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = Path(temp_dir) / "scale_join.odibi.yaml"
            test_file.write_text("params: [123]")

            config = ControlledImprovementConfig(
                project_root=temp_dir,
                improvement_scope=ImprovementScope.for_single_file(str(test_file)),
                require_regression_check=False,
            )

            runner = ControlledImprovementRunner(odibi_root="d:/odibi")
            runner._config = config
            runner._scope_enforcer = ImprovementScopeEnforcer(config.improvement_scope)
            runner._snapshot = ImprovementSnapshot.capture([str(test_file)])

            proposal = ImprovementProposal(
                proposal="IMPROVEMENT",
                title="Fix params type",
                changes=[
                    ProposalChange(
                        file=str(test_file),
                        before="params: [123]",
                        after='params: ["123"]',
                    )
                ],
            )

            mock_state = MagicMock(spec=CycleState)
            mock_state.cycle_id = "test-cycle"

            result = runner.apply_improvement(proposal, mock_state)

            assert result.status == "APPLIED"
            assert len(result.files_modified) == 1
            assert test_file.read_text() == 'params: ["123"]'

    def test_rollback_on_validation_failure(self):
        """Rollback restores files when validation fails."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = Path(temp_dir) / "scale_join.odibi.yaml"
            original_content = "params: [123]"
            test_file.write_text(original_content)

            config = ControlledImprovementConfig(
                project_root=temp_dir,
                improvement_scope=ImprovementScope.for_single_file(str(test_file)),
                rollback_on_failure=True,
            )

            runner = ControlledImprovementRunner(odibi_root="d:/odibi")
            runner._config = config
            runner._scope_enforcer = ImprovementScopeEnforcer(config.improvement_scope)
            runner._snapshot = ImprovementSnapshot.capture([str(test_file)])

            proposal = ImprovementProposal(
                proposal="IMPROVEMENT",
                title="Fix params type",
                changes=[
                    ProposalChange(
                        file=str(test_file),
                        before="params: [123]",
                        after='params: ["123"]',
                    )
                ],
            )

            mock_state = MagicMock(spec=CycleState)
            mock_state.cycle_id = "test-cycle"

            result = runner.apply_improvement(proposal, mock_state)
            assert result.status == "APPLIED"
            assert test_file.read_text() == 'params: ["123"]'

            rollback_success = runner.rollback_improvement()

            assert rollback_success
            assert test_file.read_text() == original_content
            assert runner._improvement_result.status == "ROLLED_BACK"

    def test_diff_is_deterministic(self):
        """Same proposal produces identical diff hash."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = Path(temp_dir) / "scale_join.odibi.yaml"

            def run_improvement():
                test_file.write_text("params: [123]")

                config = ControlledImprovementConfig(
                    project_root=temp_dir,
                    improvement_scope=ImprovementScope.for_single_file(str(test_file)),
                )

                runner = ControlledImprovementRunner(odibi_root="d:/odibi")
                runner._config = config
                runner._scope_enforcer = ImprovementScopeEnforcer(config.improvement_scope)
                runner._snapshot = ImprovementSnapshot.capture([str(test_file)])

                proposal = ImprovementProposal(
                    proposal="IMPROVEMENT",
                    title="Fix params type",
                    changes=[
                        ProposalChange(
                            file=str(test_file),
                            before="params: [123]",
                            after='params: ["123"]',
                        )
                    ],
                )

                mock_state = MagicMock(spec=CycleState)
                mock_state.cycle_id = "test-cycle"

                return runner.apply_improvement(proposal, mock_state)

            result1 = run_improvement()
            result2 = run_improvement()

            assert result1.diff_hash == result2.diff_hash, "Diff must be deterministic"

    def test_none_proposal_returns_no_proposal_status(self):
        """NONE proposal returns NO_PROPOSAL status without modification."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = Path(temp_dir) / "scale_join.odibi.yaml"
            test_file.write_text("params: [123]")

            config = ControlledImprovementConfig(
                project_root=temp_dir,
                improvement_scope=ImprovementScope.for_single_file(str(test_file)),
            )

            runner = ControlledImprovementRunner(odibi_root="d:/odibi")
            runner._config = config
            runner._scope_enforcer = ImprovementScopeEnforcer(config.improvement_scope)
            runner._snapshot = ImprovementSnapshot.capture([str(test_file)])

            none_proposal = ImprovementProposal.none_proposal("No actionable issues found")

            mock_state = MagicMock(spec=CycleState)
            mock_state.cycle_id = "test-cycle"

            result = runner.apply_improvement(none_proposal, mock_state)

            assert result.status == "NO_PROPOSAL"
            assert len(result.files_modified) == 0
            assert test_file.read_text() == "params: [123]"  # Unchanged


class TestImprovementResultSerialization:
    """Tests for ImprovementResult serialization."""

    def test_to_dict_includes_all_fields(self):
        """to_dict() serializes all important fields."""
        scope = ImprovementScope.for_single_file("test.yaml")
        snapshot = ImprovementSnapshot(
            snapshot_id="snap123",
            created_at="2024-01-01T00:00:00",
            files={"test.yaml": "content"},
            file_hashes={"test.yaml": "abc123"},
        )

        result = ImprovementResult(
            improvement_id="imp123",
            proposal=None,
            scope=scope,
            snapshot=snapshot,
            status="APPLIED",
            files_modified=["test.yaml"],
            validation_passed=True,
            diff_hash="diff123",
        )

        result_dict = result.to_dict()

        assert result_dict["improvement_id"] == "imp123"
        assert result_dict["status"] == "APPLIED"
        assert result_dict["files_modified"] == ["test.yaml"]
        assert result_dict["validation_passed"] is True
        assert result_dict["diff_hash"] == "diff123"


class TestTransformerSchemaLookup:
    """Tests for _get_transformer_schemas() helper."""

    def test_extracts_join_schema(self):
        """Schema lookup extracts join transformer schema from YAML."""
        runner = ControlledImprovementRunner.__new__(ControlledImprovementRunner)

        files_content = {
            "test.yaml": """
pipelines:
  - pipeline: test
    nodes:
      - name: join_node
        transformer: join
        params:
          right_dataset: other
"""
        }

        schema = runner._get_transformer_schemas(files_content)

        assert "join transformer" in schema
        assert "right_dataset" in schema
        assert '"on"' in schema  # Note about quoting
        assert "YAML" in schema  # Warning about YAML boolean

    def test_no_transformer_returns_empty(self):
        """Schema lookup returns empty string for files without transformers."""
        runner = ControlledImprovementRunner.__new__(ControlledImprovementRunner)

        files_content = {
            "test.yaml": """
pipelines:
  - pipeline: test
    nodes:
      - name: read_node
        read:
          connection: source
          format: csv
"""
        }

        schema = runner._get_transformer_schemas(files_content)

        assert schema == ""

    def test_multiple_transformers(self):
        """Schema lookup handles multiple transformers in one file."""
        runner = ControlledImprovementRunner.__new__(ControlledImprovementRunner)

        files_content = {
            "test.yaml": """
nodes:
  - transformer: join
  - transformer: union
  - transformer: deduplicate
"""
        }

        schema = runner._get_transformer_schemas(files_content)

        assert "join transformer" in schema
        assert "union transformer" in schema
        assert "deduplicate transformer" in schema


class TestGoldenProjectPathResolution:
    """Tests for GoldenProjectConfig.resolve_path() auto-resolution to learning harness."""

    def test_absolute_path_unchanged(self):
        """Absolute paths are returned as-is."""
        from odibi.agents.core.cycle import GoldenProjectConfig
        from pathlib import Path

        gp = GoldenProjectConfig(name="test", path="D:/absolute/path/test.yaml")
        resolved = gp.resolve_path("D:/workspace")
        # Normalize both for Windows/Unix path comparison
        assert Path(resolved) == Path("D:/absolute/path/test.yaml")

    def test_relative_path_resolved_to_workspace(self, tmp_path):
        """Relative paths that exist in workspace are resolved there."""
        from odibi.agents.core.cycle import GoldenProjectConfig

        # Create file in workspace
        test_file = tmp_path / "my_pipeline.yaml"
        test_file.write_text("test content")

        gp = GoldenProjectConfig(name="test", path="my_pipeline.yaml")
        resolved = gp.resolve_path(str(tmp_path))
        assert resolved == str(test_file)

    def test_auto_resolve_to_learning_harness(self, tmp_path):
        """Paths that don't exist in workspace auto-resolve to .odibi/learning_harness/."""
        from odibi.agents.core.cycle import GoldenProjectConfig

        # Create file in learning harness, NOT in workspace root
        harness_dir = tmp_path / ".odibi" / "learning_harness"
        harness_dir.mkdir(parents=True)
        harness_file = harness_dir / "scale_join.odibi.yaml"
        harness_file.write_text("harness content")

        gp = GoldenProjectConfig(name="scale_join", path="scale_join.odibi.yaml")
        resolved = gp.resolve_path(str(tmp_path))

        # Should resolve to harness location
        assert resolved == str(harness_file)

    def test_workspace_takes_precedence_over_harness(self, tmp_path):
        """If file exists in both locations, workspace takes precedence."""
        from odibi.agents.core.cycle import GoldenProjectConfig

        # Create file in BOTH workspace root and harness
        workspace_file = tmp_path / "test.yaml"
        workspace_file.write_text("workspace content")

        harness_dir = tmp_path / ".odibi" / "learning_harness"
        harness_dir.mkdir(parents=True)
        harness_file = harness_dir / "test.yaml"
        harness_file.write_text("harness content")

        gp = GoldenProjectConfig(name="test", path="test.yaml")
        resolved = gp.resolve_path(str(tmp_path))

        # Workspace should take precedence
        assert resolved == str(workspace_file)

    def test_fallback_when_not_found_anywhere(self, tmp_path):
        """If file doesn't exist anywhere, falls back to workspace + path."""
        from odibi.agents.core.cycle import GoldenProjectConfig

        gp = GoldenProjectConfig(name="nonexistent", path="nonexistent.yaml")
        resolved = gp.resolve_path(str(tmp_path))

        # Should fall back to workspace_root + path
        assert resolved == str(tmp_path / "nonexistent.yaml")

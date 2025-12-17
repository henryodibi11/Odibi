"""Tests for diff capture utilities.

Phase E3: Diff Capture

These tests verify that:
1. No changes → empty diff (explicit, not None)
2. Single-file change → diff captured
3. Multi-file change → diff captured
4. Diff path validation (cannot escape sandbox)
5. Repeatability (same change → same diff hash)
"""

import subprocess
import sys
from pathlib import Path

import pytest

from odibi.agents.explorer.clone_manager import RepoCloneManager
from odibi.agents.explorer.diff_capture import (
    ARTIFACTS_DIR_NAME,
    EMPTY_DIFF_MARKER,
    DiffPathViolation,
    DiffResult,
    capture_diff,
    capture_filesystem_diff,
    compute_diff_hash,
    create_baseline_snapshot,
    ensure_artifacts_dir,
    get_artifacts_dir,
    is_git_repo,
    validate_diff_path,
)
from odibi.agents.explorer.experiment_runner import (
    ExperimentRunner,
    ExperimentSpec,
)


class TestArtifactsDirectory:
    """Tests for artifacts directory handling."""

    def test_get_artifacts_dir(self, tmp_path: Path):
        sandbox_path = tmp_path / "sandbox"
        sandbox_path.mkdir()

        artifacts_dir = get_artifacts_dir(sandbox_path)

        assert artifacts_dir == sandbox_path / ARTIFACTS_DIR_NAME
        assert not artifacts_dir.exists()

    def test_ensure_artifacts_dir_creates(self, tmp_path: Path):
        sandbox_path = tmp_path / "sandbox"
        sandbox_path.mkdir()

        artifacts_dir = ensure_artifacts_dir(sandbox_path)

        assert artifacts_dir.exists()
        assert artifacts_dir.is_dir()

    def test_ensure_artifacts_dir_idempotent(self, tmp_path: Path):
        sandbox_path = tmp_path / "sandbox"
        sandbox_path.mkdir()

        dir1 = ensure_artifacts_dir(sandbox_path)
        dir2 = ensure_artifacts_dir(sandbox_path)

        assert dir1 == dir2
        assert dir1.exists()


class TestGitDetection:
    """Tests for git repository detection."""

    def test_non_git_dir_returns_false(self, tmp_path: Path):
        assert is_git_repo(tmp_path) is False

    def test_git_repo_returns_true(self, tmp_path: Path):
        try:
            subprocess.run(
                ["git", "init"],
                cwd=str(tmp_path),
                capture_output=True,
                check=True,
            )
        except FileNotFoundError:
            pytest.skip("git not available")

        assert is_git_repo(tmp_path) is True


class TestDiffHash:
    """Tests for diff hash computation."""

    def test_same_content_same_hash(self):
        content = "some diff content"
        hash1 = compute_diff_hash(content)
        hash2 = compute_diff_hash(content)

        assert hash1 == hash2

    def test_different_content_different_hash(self):
        hash1 = compute_diff_hash("content a")
        hash2 = compute_diff_hash("content b")

        assert hash1 != hash2

    def test_hash_is_deterministic(self):
        content = "deterministic content"
        hashes = [compute_diff_hash(content) for _ in range(10)]

        assert all(h == hashes[0] for h in hashes)


class TestBaselineSnapshot:
    """Tests for baseline snapshot creation."""

    def test_creates_snapshot_of_files(self, tmp_path: Path):
        (tmp_path / "file1.txt").write_text("content1")
        (tmp_path / "file2.txt").write_text("content2")

        snapshot = create_baseline_snapshot(tmp_path)

        assert "file1.txt" in snapshot
        assert "file2.txt" in snapshot
        assert len(snapshot) == 2

    def test_includes_subdirectories(self, tmp_path: Path):
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "nested.txt").write_text("nested content")

        snapshot = create_baseline_snapshot(tmp_path)

        assert "subdir/nested.txt" in snapshot

    def test_excludes_artifacts_dir(self, tmp_path: Path):
        (tmp_path / "file.txt").write_text("content")
        artifacts = tmp_path / ARTIFACTS_DIR_NAME
        artifacts.mkdir()
        (artifacts / "artifact.txt").write_text("artifact")

        snapshot = create_baseline_snapshot(tmp_path)

        assert "file.txt" in snapshot
        assert f"{ARTIFACTS_DIR_NAME}/artifact.txt" not in snapshot


class TestFilesystemDiff:
    """Tests for filesystem-based diff capture."""

    def test_no_baseline_returns_empty(self, tmp_path: Path):
        (tmp_path / "file.txt").write_text("content")

        diff = capture_filesystem_diff(tmp_path, baseline_snapshot=None)

        assert diff == ""

    def test_no_changes_returns_empty(self, tmp_path: Path):
        (tmp_path / "file.txt").write_text("content")
        baseline = create_baseline_snapshot(tmp_path)

        diff = capture_filesystem_diff(tmp_path, baseline_snapshot=baseline)

        assert diff == ""

    def test_modified_file_detected(self, tmp_path: Path):
        (tmp_path / "file.txt").write_text("original")
        baseline = create_baseline_snapshot(tmp_path)
        (tmp_path / "file.txt").write_text("modified")

        diff = capture_filesystem_diff(tmp_path, baseline_snapshot=baseline)

        assert "file.txt" in diff
        assert "modified" in diff

    def test_new_file_detected(self, tmp_path: Path):
        (tmp_path / "existing.txt").write_text("existing")
        baseline = create_baseline_snapshot(tmp_path)
        (tmp_path / "new.txt").write_text("new content")

        diff = capture_filesystem_diff(tmp_path, baseline_snapshot=baseline)

        assert "new.txt" in diff
        assert "added" in diff

    def test_deleted_file_detected(self, tmp_path: Path):
        (tmp_path / "toremove.txt").write_text("will be removed")
        baseline = create_baseline_snapshot(tmp_path)
        (tmp_path / "toremove.txt").unlink()

        diff = capture_filesystem_diff(tmp_path, baseline_snapshot=baseline)

        assert "toremove.txt" in diff
        assert "deleted" in diff


class TestDiffPathValidation:
    """Tests for diff path validation."""

    def test_path_inside_sandbox_passes(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        sandbox_path = sandbox_root / "sandbox1"
        trusted_repo = tmp_path / "odibi"
        sandbox_path.mkdir(parents=True)
        trusted_repo.mkdir()

        diff_path = sandbox_path / ARTIFACTS_DIR_NAME / "exp.diff"

        validate_diff_path(diff_path, sandbox_path, sandbox_root, trusted_repo)

    def test_path_outside_sandbox_rejected(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        sandbox_path = sandbox_root / "sandbox1"
        trusted_repo = tmp_path / "odibi"
        sandbox_path.mkdir(parents=True)
        trusted_repo.mkdir()

        diff_path = tmp_path / "outside" / "exp.diff"

        with pytest.raises(DiffPathViolation):
            validate_diff_path(diff_path, sandbox_path, sandbox_root, trusted_repo)

    def test_path_in_trusted_repo_rejected(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        sandbox_path = trusted_repo / "fake_sandbox"
        sandbox_root.mkdir()
        trusted_repo.mkdir()
        sandbox_path.mkdir()

        diff_path = sandbox_path / "exp.diff"

        from odibi.agents.explorer.path_validation import PathValidationError

        with pytest.raises((DiffPathViolation, PathValidationError)):
            validate_diff_path(diff_path, sandbox_path, sandbox_root, trusted_repo)


class TestCaptureDiffIntegration:
    """Integration tests for capture_diff function."""

    @pytest.fixture
    def sandbox_setup(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        sandbox_path = sandbox_root / "test_sandbox"
        sandbox_root.mkdir()
        trusted_repo.mkdir()
        sandbox_path.mkdir()

        return sandbox_path, sandbox_root, trusted_repo

    def test_capture_empty_diff(self, sandbox_setup):
        sandbox_path, sandbox_root, trusted_repo = sandbox_setup
        (sandbox_path / "file.txt").write_text("content")
        baseline = create_baseline_snapshot(sandbox_path)

        result = capture_diff(
            sandbox_path=sandbox_path,
            sandbox_root=sandbox_root,
            trusted_repo_root=trusted_repo,
            experiment_id="exp_empty",
            baseline_snapshot=baseline,
        )

        assert result.is_empty is True
        assert result.diff_content == EMPTY_DIFF_MARKER
        assert result.diff_path.exists()

    def test_capture_with_changes(self, sandbox_setup):
        sandbox_path, sandbox_root, trusted_repo = sandbox_setup
        (sandbox_path / "file.txt").write_text("original")
        baseline = create_baseline_snapshot(sandbox_path)
        (sandbox_path / "file.txt").write_text("modified")

        result = capture_diff(
            sandbox_path=sandbox_path,
            sandbox_root=sandbox_root,
            trusted_repo_root=trusted_repo,
            experiment_id="exp_changes",
            baseline_snapshot=baseline,
        )

        assert result.is_empty is False
        assert "file.txt" in result.diff_content
        assert result.diff_path.exists()

    def test_diff_path_in_artifacts_dir(self, sandbox_setup):
        sandbox_path, sandbox_root, trusted_repo = sandbox_setup

        result = capture_diff(
            sandbox_path=sandbox_path,
            sandbox_root=sandbox_root,
            trusted_repo_root=trusted_repo,
            experiment_id="exp_path",
            baseline_snapshot=None,
        )

        assert ARTIFACTS_DIR_NAME in str(result.diff_path)
        assert result.diff_path.parent.name == ARTIFACTS_DIR_NAME

    def test_diff_file_has_correct_extension(self, sandbox_setup):
        sandbox_path, sandbox_root, trusted_repo = sandbox_setup

        result = capture_diff(
            sandbox_path=sandbox_path,
            sandbox_root=sandbox_root,
            trusted_repo_root=trusted_repo,
            experiment_id="exp_ext",
            baseline_snapshot=None,
        )

        assert result.diff_path.suffix == ".diff"


class TestRepeatability:
    """Tests for diff repeatability."""

    @pytest.fixture
    def sandbox_setup(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        sandbox_path = sandbox_root / "test_sandbox"
        sandbox_root.mkdir()
        trusted_repo.mkdir()
        sandbox_path.mkdir()

        return sandbox_path, sandbox_root, trusted_repo

    def test_same_change_same_hash(self, sandbox_setup):
        sandbox_path, sandbox_root, trusted_repo = sandbox_setup
        (sandbox_path / "file.txt").write_text("original")
        baseline = create_baseline_snapshot(sandbox_path)
        (sandbox_path / "file.txt").write_text("modified")

        result1 = capture_diff(
            sandbox_path=sandbox_path,
            sandbox_root=sandbox_root,
            trusted_repo_root=trusted_repo,
            experiment_id="exp_repeat1",
            baseline_snapshot=baseline,
        )

        result2 = capture_diff(
            sandbox_path=sandbox_path,
            sandbox_root=sandbox_root,
            trusted_repo_root=trusted_repo,
            experiment_id="exp_repeat2",
            baseline_snapshot=baseline,
        )

        assert result1.diff_hash == result2.diff_hash

    def test_different_change_different_hash(self, sandbox_setup):
        sandbox_path, sandbox_root, trusted_repo = sandbox_setup
        (sandbox_path / "file.txt").write_text("original")
        baseline = create_baseline_snapshot(sandbox_path)

        (sandbox_path / "file.txt").write_text("change A")
        result1 = capture_diff(
            sandbox_path=sandbox_path,
            sandbox_root=sandbox_root,
            trusted_repo_root=trusted_repo,
            experiment_id="exp_diff1",
            baseline_snapshot=baseline,
        )

        (sandbox_path / "file.txt").write_text("change B")
        result2 = capture_diff(
            sandbox_path=sandbox_path,
            sandbox_root=sandbox_root,
            trusted_repo_root=trusted_repo,
            experiment_id="exp_diff2",
            baseline_snapshot=baseline,
        )

        assert result1.diff_hash != result2.diff_hash


class TestExperimentRunnerDiffIntegration:
    """Tests for diff capture integrated with ExperimentRunner."""

    @pytest.fixture
    def runner_with_sandbox(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        source_repo = tmp_path / "source"
        source_repo.mkdir()
        (source_repo / "file.txt").write_text("original content")

        manager = RepoCloneManager(sandbox_root, trusted_repo)
        sandbox_info = manager.create_sandbox(source_repo)
        runner = ExperimentRunner(manager)

        return runner, sandbox_info

    def test_experiment_captures_diff_by_default(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        runner.create_baseline(sandbox_info.sandbox_id)

        if sys.platform == "win32":
            spec = ExperimentSpec(
                name="modify_test",
                description="Modify a file",
                commands=["echo modified > file.txt"],
                timeout_seconds=30,
                capture_diff=True,
            )
        else:
            spec = ExperimentSpec(
                name="modify_test",
                description="Modify a file",
                commands=['echo "modified" > file.txt'],
                timeout_seconds=30,
                capture_diff=True,
            )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert result.diff_path is not None
        assert result.diff_path.exists()
        assert result.diff_is_empty is False
        assert result.diff_hash != ""

    def test_experiment_no_diff_when_disabled(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        spec = ExperimentSpec(
            name="no_diff_test",
            description="Don't capture diff",
            commands=["echo hello"],
            timeout_seconds=30,
            capture_diff=False,
        )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert result.diff_path is None
        assert result.diff_content == ""

    def test_experiment_empty_diff_explicit(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        runner.create_baseline(sandbox_info.sandbox_id)

        spec = ExperimentSpec(
            name="no_change_test",
            description="No changes made",
            commands=["echo hello"],
            timeout_seconds=30,
            capture_diff=True,
        )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert result.diff_is_empty is True
        assert result.diff_content == EMPTY_DIFF_MARKER
        assert result.diff_path is not None

    def test_extract_diff_manual(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        runner.create_baseline(sandbox_info.sandbox_id)

        sandbox_path = sandbox_info.sandbox_path
        (sandbox_path / "file.txt").write_text("manually modified")

        diff_result = runner.extract_diff(sandbox_info.sandbox_id)

        assert diff_result.is_empty is False
        assert "file.txt" in diff_result.diff_content

    def test_multi_file_change_captured(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        sandbox_path = sandbox_info.sandbox_path
        (sandbox_path / "file2.txt").write_text("file 2 original")
        (sandbox_path / "subdir").mkdir()
        (sandbox_path / "subdir" / "file3.txt").write_text("file 3 original")

        runner.create_baseline(sandbox_info.sandbox_id)

        (sandbox_path / "file.txt").write_text("file 1 modified")
        (sandbox_path / "file2.txt").write_text("file 2 modified")
        (sandbox_path / "subdir" / "file3.txt").write_text("file 3 modified")

        diff_result = runner.extract_diff(sandbox_info.sandbox_id)

        assert diff_result.is_empty is False
        assert "file.txt" in diff_result.diff_content
        assert "file2.txt" in diff_result.diff_content
        assert "file3.txt" in diff_result.diff_content


class TestDiffResult:
    """Tests for DiffResult dataclass."""

    def test_to_dict(self, tmp_path: Path):
        diff_path = tmp_path / "test.diff"
        result = DiffResult(
            diff_content="some diff",
            diff_path=diff_path,
            is_empty=False,
            diff_hash="abc123",
            method="filesystem",
            captured_at="2024-01-01T00:00:00",
        )

        d = result.to_dict()

        assert d["diff_content"] == "some diff"
        assert d["diff_path"] == str(diff_path)
        assert d["is_empty"] is False
        assert d["diff_hash"] == "abc123"
        assert d["method"] == "filesystem"

    def test_immutable(self, tmp_path: Path):
        diff_path = tmp_path / "test.diff"
        result = DiffResult(
            diff_content="content",
            diff_path=diff_path,
            is_empty=False,
            diff_hash="hash",
            method="git",
            captured_at="2024-01-01T00:00:00",
        )

        with pytest.raises(AttributeError):
            result.diff_content = "new content"

"""Tests for path validation utilities.

Phase E1: Sandbox Isolation

These tests verify that:
1. Sandbox roots cannot be inside trusted paths
2. Sandbox paths must be inside sandbox root
3. Paths containing trusted patterns are rejected
4. Source repos must exist
"""

from pathlib import Path

import pytest

from odibi.agents.explorer.path_validation import (
    PathValidationError,
    TrustedPathViolation,
    contains_trusted_pattern,
    is_path_inside,
    validate_sandbox_path,
    validate_sandbox_root,
    validate_source_repo,
)


class TestIsPathInside:
    """Tests for is_path_inside utility."""

    def test_child_inside_parent(self, tmp_path: Path):
        parent = tmp_path / "parent"
        child = parent / "child"
        parent.mkdir()
        child.mkdir()

        assert is_path_inside(child, parent) is True

    def test_parent_not_inside_child(self, tmp_path: Path):
        parent = tmp_path / "parent"
        child = parent / "child"
        parent.mkdir()
        child.mkdir()

        assert is_path_inside(parent, child) is False

    def test_same_path_is_inside(self, tmp_path: Path):
        assert is_path_inside(tmp_path, tmp_path) is True

    def test_sibling_not_inside(self, tmp_path: Path):
        sibling1 = tmp_path / "sibling1"
        sibling2 = tmp_path / "sibling2"
        sibling1.mkdir()
        sibling2.mkdir()

        assert is_path_inside(sibling1, sibling2) is False
        assert is_path_inside(sibling2, sibling1) is False


class TestContainsTrustedPattern:
    """Tests for trusted pattern detection."""

    def test_detects_agents_core(self):
        assert contains_trusted_pattern(Path("D:/odibi/agents/core/file.py")) is True
        assert contains_trusted_pattern(Path("D:\\odibi\\agents\\core\\file.py")) is True

    def test_detects_learning_harness(self):
        assert contains_trusted_pattern(Path("D:/odibi/.odibi/learning_harness/test.yaml")) is True

    def test_case_insensitive(self):
        assert contains_trusted_pattern(Path("D:/odibi/AGENTS/CORE/file.py")) is True

    def test_safe_paths_pass(self):
        assert contains_trusted_pattern(Path("D:/sandbox/myproject")) is False
        assert contains_trusted_pattern(Path("D:/odibi_explorer_sandboxes/abc123")) is False


class TestValidateSandboxRoot:
    """Tests for sandbox root validation."""

    def test_sandbox_inside_trusted_repo_rejected(self, tmp_path: Path):
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()
        sandbox_root = trusted_repo / "sandboxes"

        with pytest.raises(TrustedPathViolation) as exc_info:
            validate_sandbox_root(sandbox_root, trusted_repo)

        assert exc_info.value.trusted_path == trusted_repo.resolve()

    def test_trusted_repo_inside_sandbox_rejected(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        sandbox_root.mkdir()
        trusted_repo = sandbox_root / "odibi"
        trusted_repo.mkdir()

        with pytest.raises(TrustedPathViolation):
            validate_sandbox_root(sandbox_root, trusted_repo)

    def test_sandbox_with_trusted_pattern_rejected(self, tmp_path: Path):
        sandbox_root = tmp_path / "agents" / "core"
        trusted_repo = tmp_path / "other"
        trusted_repo.mkdir()

        with pytest.raises(PathValidationError) as exc_info:
            validate_sandbox_root(sandbox_root, trusted_repo)

        assert "protected pattern" in exc_info.value.reason

    def test_separate_paths_accepted(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        validate_sandbox_root(sandbox_root, trusted_repo)

    def test_default_paths_are_valid(self):
        """The default sandbox root and trusted repo should be valid."""
        from odibi.agents.explorer.path_validation import (
            DEFAULT_SANDBOX_ROOT,
            DEFAULT_TRUSTED_REPO_ROOT,
        )

        validate_sandbox_root(DEFAULT_SANDBOX_ROOT, DEFAULT_TRUSTED_REPO_ROOT)


class TestValidateSandboxPath:
    """Tests for specific sandbox path validation."""

    def test_path_inside_sandbox_root_accepted(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        sandbox_root.mkdir()
        sandbox_path = sandbox_root / "abc123"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        validate_sandbox_path(sandbox_path, sandbox_root, trusted_repo)

    def test_path_outside_sandbox_root_rejected(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        sandbox_root.mkdir()
        sandbox_path = tmp_path / "other" / "abc123"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        with pytest.raises(PathValidationError) as exc_info:
            validate_sandbox_path(sandbox_path, sandbox_root, trusted_repo)

        assert "must be inside sandbox root" in str(exc_info.value)

    def test_path_inside_trusted_rejected(self, tmp_path: Path):
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()
        sandbox_root = tmp_path / "sandboxes"
        sandbox_root.mkdir()
        sandbox_in_trusted = trusted_repo / "bad_sandbox"

        sandbox_root_fake = trusted_repo

        with pytest.raises(TrustedPathViolation):
            validate_sandbox_path(sandbox_in_trusted, sandbox_root_fake, trusted_repo)


class TestValidateSourceRepo:
    """Tests for source repository validation."""

    def test_existing_directory_accepted(self, tmp_path: Path):
        source_repo = tmp_path / "myrepo"
        source_repo.mkdir()

        validate_source_repo(source_repo)

    def test_nonexistent_path_rejected(self, tmp_path: Path):
        source_repo = tmp_path / "nonexistent"

        with pytest.raises(PathValidationError) as exc_info:
            validate_source_repo(source_repo)

        assert "does not exist" in str(exc_info.value)

    def test_file_path_rejected(self, tmp_path: Path):
        source_file = tmp_path / "file.txt"
        source_file.write_text("content")

        with pytest.raises(PathValidationError) as exc_info:
            validate_source_repo(source_file)

        assert "must be a directory" in str(exc_info.value)

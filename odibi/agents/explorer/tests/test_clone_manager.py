"""Tests for RepoCloneManager.

Phase E1: Sandbox Isolation

These tests verify that:
1. Sandboxes are created as isolated copies
2. Sandboxes are destroyed completely
3. Path validation prevents sandboxes inside trusted paths
4. Cleanup-on-exit removes all sandboxes
"""

from pathlib import Path

import pytest

from odibi.agents.explorer.clone_manager import (
    RepoCloneManager,
    SandboxInfo,
    _active_managers,
    _cleanup_all_managers,
)
from odibi.agents.explorer.path_validation import TrustedPathViolation


class TestRepoCloneManagerInit:
    """Tests for RepoCloneManager initialization."""

    def test_creates_sandbox_root_if_missing(self, tmp_path: Path):
        sandbox_root = tmp_path / "new_sandbox_root"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        assert not sandbox_root.exists()

        manager = RepoCloneManager(sandbox_root, trusted_repo)

        assert sandbox_root.exists()
        assert manager.sandbox_root == sandbox_root

    def test_rejects_sandbox_root_inside_trusted(self, tmp_path: Path):
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()
        sandbox_root = trusted_repo / "sandboxes"

        with pytest.raises(TrustedPathViolation):
            RepoCloneManager(sandbox_root, trusted_repo)

    def test_registers_in_active_managers(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        manager = RepoCloneManager(sandbox_root, trusted_repo)

        assert manager in _active_managers


class TestCreateSandbox:
    """Tests for sandbox creation."""

    @pytest.fixture
    def setup_manager(self, tmp_path: Path):
        """Create a manager and source repo for testing."""
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        source_repo = tmp_path / "source_project"
        source_repo.mkdir()
        (source_repo / "file1.py").write_text("# file 1")
        (source_repo / "subdir").mkdir()
        (source_repo / "subdir" / "file2.py").write_text("# file 2")

        manager = RepoCloneManager(sandbox_root, trusted_repo)
        return manager, source_repo

    def test_creates_isolated_copy(self, setup_manager):
        manager, source_repo = setup_manager

        info = manager.create_sandbox(source_repo)

        assert info.sandbox_path.exists()
        assert (info.sandbox_path / "file1.py").read_text() == "# file 1"
        assert (info.sandbox_path / "subdir" / "file2.py").read_text() == "# file 2"

    def test_returns_sandbox_info(self, setup_manager):
        manager, source_repo = setup_manager

        info = manager.create_sandbox(source_repo)

        assert isinstance(info, SandboxInfo)
        assert info.sandbox_id is not None
        assert info.sandbox_path.is_absolute()
        assert info.source_repo == source_repo.resolve()
        assert info.is_active is True
        assert info.created_at is not None

    def test_sandbox_path_inside_sandbox_root(self, setup_manager):
        manager, source_repo = setup_manager

        info = manager.create_sandbox(source_repo)

        assert str(info.sandbox_path).startswith(str(manager.sandbox_root))

    def test_tracks_active_sandbox(self, setup_manager):
        manager, source_repo = setup_manager

        info = manager.create_sandbox(source_repo)

        assert manager.get_sandbox(info.sandbox_id) == info
        assert info in manager.list_active_sandboxes()

    def test_rejects_nonexistent_source(self, setup_manager):
        manager, _ = setup_manager
        nonexistent = manager.sandbox_root.parent / "nonexistent"

        from odibi.agents.explorer.path_validation import PathValidationError

        with pytest.raises(PathValidationError):
            manager.create_sandbox(nonexistent)

    def test_ignores_pycache(self, setup_manager):
        manager, source_repo = setup_manager
        pycache = source_repo / "__pycache__"
        pycache.mkdir()
        (pycache / "file.pyc").write_bytes(b"bytecode")

        info = manager.create_sandbox(source_repo)

        assert not (info.sandbox_path / "__pycache__").exists()

    def test_modifications_dont_affect_source(self, setup_manager):
        manager, source_repo = setup_manager

        info = manager.create_sandbox(source_repo)
        (info.sandbox_path / "file1.py").write_text("# modified")

        assert (source_repo / "file1.py").read_text() == "# file 1"


class TestDestroySandbox:
    """Tests for sandbox destruction."""

    @pytest.fixture
    def manager_with_sandbox(self, tmp_path: Path):
        """Create a manager with an active sandbox."""
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        source_repo = tmp_path / "source"
        source_repo.mkdir()
        (source_repo / "file.py").write_text("content")

        manager = RepoCloneManager(sandbox_root, trusted_repo)
        info = manager.create_sandbox(source_repo)
        return manager, info

    def test_removes_sandbox_directory(self, manager_with_sandbox):
        manager, info = manager_with_sandbox
        sandbox_path = info.sandbox_path

        assert sandbox_path.exists()

        result = manager.destroy_sandbox(info.sandbox_id)

        assert result is True
        assert not sandbox_path.exists()

    def test_removes_from_active_sandboxes(self, manager_with_sandbox):
        manager, info = manager_with_sandbox

        manager.destroy_sandbox(info.sandbox_id)

        assert manager.get_sandbox(info.sandbox_id) is None
        assert info not in manager.list_active_sandboxes()

    def test_returns_false_for_unknown_id(self, manager_with_sandbox):
        manager, _ = manager_with_sandbox

        result = manager.destroy_sandbox("nonexistent_id")

        assert result is False

    def test_idempotent_destroy(self, manager_with_sandbox):
        manager, info = manager_with_sandbox

        result1 = manager.destroy_sandbox(info.sandbox_id)
        result2 = manager.destroy_sandbox(info.sandbox_id)

        assert result1 is True
        assert result2 is False


class TestCleanupAll:
    """Tests for cleanup_all and cleanup-on-exit."""

    def test_destroys_all_sandboxes(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        source1 = tmp_path / "source1"
        source1.mkdir()
        source2 = tmp_path / "source2"
        source2.mkdir()

        manager = RepoCloneManager(sandbox_root, trusted_repo)
        info1 = manager.create_sandbox(source1)
        info2 = manager.create_sandbox(source2)

        count = manager.cleanup_all()

        assert count == 2
        assert not info1.sandbox_path.exists()
        assert not info2.sandbox_path.exists()
        assert len(manager.list_active_sandboxes()) == 0

    def test_cleanup_all_managers_function(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        source = tmp_path / "source"
        source.mkdir()

        manager = RepoCloneManager(sandbox_root, trusted_repo)
        info = manager.create_sandbox(source)

        _cleanup_all_managers()

        assert not info.sandbox_path.exists()

    def test_cleanup_continues_on_error(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        source1 = tmp_path / "source1"
        source1.mkdir()
        source2 = tmp_path / "source2"
        source2.mkdir()

        manager = RepoCloneManager(sandbox_root, trusted_repo)
        info1 = manager.create_sandbox(source1)
        info2 = manager.create_sandbox(source2)

        import shutil

        shutil.rmtree(info1.sandbox_path)

        count = manager.cleanup_all()

        assert not info2.sandbox_path.exists()


class TestSandboxIsolation:
    """Integration tests for sandbox isolation guarantees."""

    def test_cannot_create_sandbox_in_trusted_path(self, tmp_path: Path):
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()
        sandbox_root = tmp_path / "sandboxes"

        source = trusted_repo / "agents" / "core"
        source.mkdir(parents=True)
        (source / "file.py").write_text("trusted")

        manager = RepoCloneManager(sandbox_root, trusted_repo)

        info = manager.create_sandbox(source)

        assert not str(info.sandbox_path).startswith(str(trusted_repo))
        assert str(info.sandbox_path).startswith(str(sandbox_root))

    def test_multiple_sandboxes_isolated(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        source = tmp_path / "source"
        source.mkdir()
        (source / "file.py").write_text("original")

        manager = RepoCloneManager(sandbox_root, trusted_repo)
        info1 = manager.create_sandbox(source)
        info2 = manager.create_sandbox(source)

        (info1.sandbox_path / "file.py").write_text("modified1")

        assert (info2.sandbox_path / "file.py").read_text() == "original"
        assert (source / "file.py").read_text() == "original"

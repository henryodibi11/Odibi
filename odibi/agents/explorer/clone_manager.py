"""RepoCloneManager: Creates and manages sandboxed repository clones.

TRUST BOUNDARY:
- This component creates ISOLATED copies of repositories
- Clones are disposable and have no connection to the trusted codebase
- All paths returned are within the sandbox directory
- Path validation ensures sandboxes cannot exist inside trusted paths
"""

import atexit
import hashlib
import logging
import shutil
import weakref
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from odibi.agents.explorer.path_validation import (
    DEFAULT_SANDBOX_ROOT,
    DEFAULT_TRUSTED_REPO_ROOT,
    validate_sandbox_path,
    validate_sandbox_root,
    validate_source_repo,
)

logger = logging.getLogger(__name__)

_active_managers: weakref.WeakSet["RepoCloneManager"] = weakref.WeakSet()
_cleanup_registered: bool = False


def _cleanup_all_managers() -> None:
    """Cleanup hook called at interpreter exit.

    Destroys all sandboxes from all active managers.
    """
    for manager in list(_active_managers):
        try:
            count = manager.cleanup_all()
            if count > 0:
                logger.info(
                    f"Exit cleanup: destroyed {count} sandboxes from manager "
                    f"at {manager.sandbox_root}"
                )
        except Exception as e:
            logger.warning(f"Exit cleanup failed for manager: {e}")


def _register_cleanup_hook() -> None:
    """Register the atexit cleanup hook (once)."""
    global _cleanup_registered
    if not _cleanup_registered:
        atexit.register(_cleanup_all_managers)
        _cleanup_registered = True


@dataclass(frozen=True)
class SandboxInfo:
    """Immutable record of a sandbox clone.

    Attributes:
        sandbox_id: Unique identifier for this sandbox.
        sandbox_path: Absolute path to the cloned repo (within sandbox root).
        source_repo: Path to the original repo that was cloned.
        created_at: ISO timestamp of clone creation.
        is_active: Whether the sandbox is still valid for use.
    """

    sandbox_id: str
    sandbox_path: Path
    source_repo: Path
    created_at: str
    is_active: bool = True


class SandboxCreationError(Exception):
    """Raised when sandbox creation fails."""

    def __init__(self, message: str, sandbox_id: str, cause: Optional[Exception] = None):
        self.sandbox_id = sandbox_id
        self.cause = cause
        super().__init__(f"{message} (sandbox_id={sandbox_id})")


class SandboxNotFoundError(Exception):
    """Raised when sandbox is not found."""

    def __init__(self, sandbox_id: str):
        self.sandbox_id = sandbox_id
        super().__init__(f"Sandbox not found: {sandbox_id}")


class RepoCloneManager:
    """Manages creation and cleanup of sandboxed repository clones.

    This component ensures that all Explorer operations happen on
    isolated copies, never on the trusted codebase.

    Attributes:
        sandbox_root: Base directory where all clones are created.
        trusted_repo_root: Root of the trusted Odibi repository.
    """

    def __init__(
        self,
        sandbox_root: Path = DEFAULT_SANDBOX_ROOT,
        trusted_repo_root: Path = DEFAULT_TRUSTED_REPO_ROOT,
    ) -> None:
        """Initialize the clone manager.

        Args:
            sandbox_root: Base directory for all sandbox clones.
                          Must be outside the trusted codebase.
            trusted_repo_root: Root of the trusted Odibi repository.

        Raises:
            TrustedPathViolation: If sandbox_root is inside trusted paths.
        """
        self._sandbox_root = Path(sandbox_root)
        self._trusted_repo_root = Path(trusted_repo_root)
        self._active_sandboxes: dict[str, SandboxInfo] = {}

        validate_sandbox_root(self._sandbox_root, self._trusted_repo_root)

        self._sandbox_root.mkdir(parents=True, exist_ok=True)

        _active_managers.add(self)
        _register_cleanup_hook()

        logger.info(f"RepoCloneManager initialized with sandbox_root={self._sandbox_root}")

    @property
    def sandbox_root(self) -> Path:
        """Read-only access to sandbox root path."""
        return self._sandbox_root

    @property
    def trusted_repo_root(self) -> Path:
        """Read-only access to trusted repo root path."""
        return self._trusted_repo_root

    def create_sandbox(self, source_repo: Path) -> SandboxInfo:
        """Create a new sandboxed clone of a repository.

        Args:
            source_repo: Path to the repository to clone.

        Returns:
            SandboxInfo describing the created sandbox.

        Raises:
            PathValidationError: If source_repo doesn't exist.
            SandboxCreationError: If clone operation fails.
        """
        source_repo = Path(source_repo)
        validate_source_repo(source_repo, self._trusted_repo_root)

        sandbox_id = self._generate_sandbox_id(source_repo)
        sandbox_path = self._sandbox_root / sandbox_id

        validate_sandbox_path(sandbox_path, self._sandbox_root, self._trusted_repo_root)

        if sandbox_path.exists():
            raise SandboxCreationError(
                "Sandbox directory already exists (hash collision or incomplete cleanup)",
                sandbox_id,
            )

        try:
            shutil.copytree(
                source_repo,
                sandbox_path,
                symlinks=False,
                ignore=shutil.ignore_patterns(
                    "__pycache__",
                    "*.pyc",
                    ".git",
                    "*.egg-info",
                    ".pytest_cache",
                    ".mypy_cache",
                    ".ruff_cache",
                ),
            )
        except Exception as e:
            if sandbox_path.exists():
                try:
                    shutil.rmtree(sandbox_path)
                except Exception:
                    pass
            raise SandboxCreationError(
                f"Failed to copy repository: {e}",
                sandbox_id,
                cause=e,
            )

        created_at = datetime.now().isoformat()
        info = SandboxInfo(
            sandbox_id=sandbox_id,
            sandbox_path=sandbox_path,
            source_repo=source_repo.resolve(),
            created_at=created_at,
            is_active=True,
        )

        self._active_sandboxes[sandbox_id] = info
        logger.info(f"Created sandbox {sandbox_id} from {source_repo}")

        return info

    def destroy_sandbox(self, sandbox_id: str) -> bool:
        """Remove a sandbox and clean up its resources.

        Args:
            sandbox_id: ID of the sandbox to destroy.

        Returns:
            True if sandbox was destroyed, False if not found.
        """
        if sandbox_id not in self._active_sandboxes:
            return False

        info = self._active_sandboxes[sandbox_id]
        sandbox_path = info.sandbox_path

        validate_sandbox_path(sandbox_path, self._sandbox_root, self._trusted_repo_root)

        if sandbox_path.exists():
            try:
                shutil.rmtree(sandbox_path)
                logger.info(f"Destroyed sandbox {sandbox_id} at {sandbox_path}")
            except Exception as e:
                logger.warning(f"Failed to fully remove sandbox {sandbox_id}: {e}")
                raise

        del self._active_sandboxes[sandbox_id]
        return True

    def get_sandbox(self, sandbox_id: str) -> Optional[SandboxInfo]:
        """Retrieve info about an active sandbox.

        Args:
            sandbox_id: ID of the sandbox to look up.

        Returns:
            SandboxInfo if found and active, None otherwise.
        """
        return self._active_sandboxes.get(sandbox_id)

    def list_active_sandboxes(self) -> list[SandboxInfo]:
        """List all currently active sandboxes.

        Returns:
            List of SandboxInfo for all active sandboxes.
        """
        return list(self._active_sandboxes.values())

    def cleanup_all(self) -> int:
        """Destroy all active sandboxes.

        Returns:
            Number of sandboxes cleaned up.
        """
        sandbox_ids = list(self._active_sandboxes.keys())
        count = 0

        for sandbox_id in sandbox_ids:
            try:
                if self.destroy_sandbox(sandbox_id):
                    count += 1
            except Exception as e:
                logger.warning(f"Failed to cleanup sandbox {sandbox_id}: {e}")

        return count

    @staticmethod
    def _generate_sandbox_id(source_repo: Path) -> str:
        """Generate a unique sandbox ID.

        Args:
            source_repo: Source repo path (used in hash).

        Returns:
            Unique sandbox ID string.
        """
        timestamp = datetime.now().isoformat()
        content = f"{source_repo.resolve()}:{timestamp}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

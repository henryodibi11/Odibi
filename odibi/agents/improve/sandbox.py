"""Sandbox management for the Improvement Environment.

Provides SandboxInfo dataclass and sandbox lifecycle utilities.

TRUST BOUNDARY:
- Sandboxes are ALWAYS created under the environment's sandboxes/ directory
- Sandboxes are NEVER created inside the Sacred repo
- Sandboxes are disposable and destroyed after each cycle
"""

import hashlib
import logging
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class SandboxError(Exception):
    """Base exception for sandbox operations."""

    pass


class SandboxCreationError(SandboxError):
    """Raised when sandbox creation fails."""

    def __init__(
        self,
        message: str,
        sandbox_id: str,
        cause: Optional[Exception] = None,
    ):
        self.sandbox_id = sandbox_id
        self.cause = cause
        super().__init__(f"{message} (sandbox_id={sandbox_id})")


class SandboxNotFoundError(SandboxError):
    """Raised when sandbox is not found."""

    def __init__(self, sandbox_id: str):
        self.sandbox_id = sandbox_id
        super().__init__(f"Sandbox not found: {sandbox_id}")


class SandboxPathViolation(SandboxError):
    """Raised when sandbox path violates safety rules."""

    def __init__(self, path: Path, reason: str):
        self.path = path
        self.reason = reason
        super().__init__(f"Sandbox path violation: {path} - {reason}")


@dataclass(frozen=True)
class SandboxInfo:
    """Immutable record of a sandbox clone.

    Attributes:
        sandbox_id: Unique identifier for this sandbox (e.g., "cycle_001").
        sandbox_path: Absolute path to the sandbox directory.
        created_at: ISO timestamp of sandbox creation.
        is_active: Whether the sandbox is still valid for use.
    """

    sandbox_id: str
    sandbox_path: Path
    created_at: str
    is_active: bool = True

    def __post_init__(self) -> None:
        """Validate sandbox info on creation."""
        if not self.sandbox_id:
            raise ValueError("sandbox_id cannot be empty")


def generate_sandbox_id(cycle_id: str) -> str:
    """Generate a unique sandbox ID for a cycle.

    Args:
        cycle_id: The cycle identifier (e.g., "cycle_001").

    Returns:
        Unique sandbox ID combining cycle_id and timestamp hash.
    """
    timestamp = datetime.now().isoformat()
    content = f"{cycle_id}:{timestamp}"
    hash_suffix = hashlib.sha256(content.encode()).hexdigest()[:8]
    return f"{cycle_id}_{hash_suffix}"


def validate_sandbox_path(
    sandbox_path: Path,
    sandboxes_root: Path,
    sacred_repo: Path,
) -> None:
    """Validate that a sandbox path is safe.

    Checks:
    1. Sandbox path is inside sandboxes root
    2. Sandbox path is NOT inside sacred repo
    3. Sacred repo is NOT inside sandbox path

    Args:
        sandbox_path: The proposed sandbox path.
        sandboxes_root: The root directory for all sandboxes.
        sacred_repo: The Sacred source repository (never touched).

    Raises:
        SandboxPathViolation: If any safety check fails.
    """
    sandbox_resolved = sandbox_path.resolve()
    sandboxes_resolved = sandboxes_root.resolve()
    sacred_resolved = sacred_repo.resolve()

    # Check 1: Sandbox must be inside sandboxes root
    try:
        sandbox_resolved.relative_to(sandboxes_resolved)
    except ValueError:
        raise SandboxPathViolation(
            sandbox_resolved,
            f"sandbox must be inside {sandboxes_resolved}",
        )

    # Check 2: Sandbox must NOT be inside sacred repo
    try:
        sandbox_resolved.relative_to(sacred_resolved)
        raise SandboxPathViolation(
            sandbox_resolved,
            f"CRITICAL: sandbox cannot be inside sacred repo {sacred_resolved}",
        )
    except ValueError:
        pass  # Expected - sandbox should not be inside sacred

    # Check 3: Sacred must NOT be inside sandbox
    try:
        sacred_resolved.relative_to(sandbox_resolved)
        raise SandboxPathViolation(
            sandbox_resolved,
            "CRITICAL: sacred repo cannot be inside sandbox",
        )
    except ValueError:
        pass  # Expected - sacred should not be inside sandbox


def create_sandbox_clone(
    source_path: Path,
    sandbox_path: Path,
) -> None:
    """Clone source directory to sandbox path.

    Uses shutil.copytree (not git) for a complete copy.
    Excludes common cache directories.

    Args:
        source_path: Path to copy from (Master or Sacred).
        sandbox_path: Path to copy to (sandbox directory).

    Raises:
        SandboxCreationError: If copy fails.
    """
    if sandbox_path.exists():
        raise SandboxCreationError(
            "Sandbox directory already exists",
            sandbox_path.name,
        )

    try:
        shutil.copytree(
            source_path,
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
                ".venv",
                "venv",
                ".odibi",  # Large cache data
                "stories",  # Execution traces
                "data",  # Test data
                ".coverage",
                "coverage.xml",
            ),
        )
        logger.info(f"Created sandbox clone at {sandbox_path}")
    except Exception as e:
        # Cleanup partial copy
        if sandbox_path.exists():
            try:
                shutil.rmtree(sandbox_path)
            except Exception:
                pass
        raise SandboxCreationError(
            f"Failed to copy source to sandbox: {e}",
            sandbox_path.name,
            cause=e,
        )


def destroy_sandbox(sandbox_path: Path, sandboxes_root: Path) -> bool:
    """Destroy a sandbox directory.

    Args:
        sandbox_path: Path to the sandbox to destroy.
        sandboxes_root: Root directory for sandboxes (for validation).

    Returns:
        True if sandbox was destroyed, False if it didn't exist.

    Raises:
        SandboxPathViolation: If sandbox_path is not inside sandboxes_root.
    """
    sandbox_resolved = sandbox_path.resolve()
    sandboxes_resolved = sandboxes_root.resolve()

    # Safety check: ensure we're only deleting from sandboxes root
    try:
        sandbox_resolved.relative_to(sandboxes_resolved)
    except ValueError:
        raise SandboxPathViolation(
            sandbox_resolved,
            f"can only destroy sandboxes inside {sandboxes_resolved}",
        )

    if not sandbox_resolved.exists():
        return False

    try:
        shutil.rmtree(sandbox_resolved)
        logger.info(f"Destroyed sandbox at {sandbox_resolved}")
        return True
    except Exception as e:
        logger.warning(f"Failed to fully destroy sandbox {sandbox_resolved}: {e}")
        raise

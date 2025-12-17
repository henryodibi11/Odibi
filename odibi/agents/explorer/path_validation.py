"""Path validation utilities for Explorer sandbox isolation.

TRUST BOUNDARY:
- Ensures sandboxes are NEVER created inside trusted paths
- Validates all paths before any filesystem operations
- Fail-fast on any violation
"""

from pathlib import Path
from typing import FrozenSet


class PathValidationError(Exception):
    """Raised when a path violates sandbox isolation rules."""

    def __init__(self, message: str, path: Path, reason: str):
        self.path = path
        self.reason = reason
        super().__init__(f"{message}: {path} ({reason})")


class TrustedPathViolation(PathValidationError):
    """Raised when attempting to create sandbox inside trusted path."""

    def __init__(self, path: Path, trusted_path: Path):
        super().__init__(
            "SANDBOX ISOLATION VIOLATION: Cannot create sandbox inside trusted path",
            path,
            f"overlaps with trusted path: {trusted_path}",
        )
        self.trusted_path = trusted_path


TRUSTED_PATH_PATTERNS: FrozenSet[str] = frozenset(
    {
        "agents/core",
        "agents\\core",
        ".odibi/learning_harness",
        ".odibi\\learning_harness",
    }
)

DEFAULT_TRUSTED_REPO_ROOT = Path("D:/odibi")
DEFAULT_SANDBOX_ROOT = Path("D:/odibi_explorer_sandboxes")


def normalize_path(path: Path) -> Path:
    """Normalize path for consistent comparison.

    Args:
        path: Path to normalize.

    Returns:
        Resolved, absolute path.
    """
    return path.resolve()


def is_path_inside(child: Path, parent: Path) -> bool:
    """Check if child path is inside parent path.

    Args:
        child: Potential child path.
        parent: Potential parent path.

    Returns:
        True if child is inside or equal to parent.
    """
    child_normalized = normalize_path(child)
    parent_normalized = normalize_path(parent)

    try:
        child_normalized.relative_to(parent_normalized)
        return True
    except ValueError:
        return False


def contains_trusted_pattern(path: Path) -> bool:
    """Check if path contains any trusted path pattern.

    Args:
        path: Path to check.

    Returns:
        True if path contains a trusted pattern.
    """
    path_str = str(path).lower()
    for pattern in TRUSTED_PATH_PATTERNS:
        if pattern.lower() in path_str:
            return True
    return False


def validate_sandbox_root(
    sandbox_root: Path,
    trusted_repo_root: Path = DEFAULT_TRUSTED_REPO_ROOT,
) -> None:
    """Validate that sandbox root is safe for sandbox creation.

    Checks:
    1. Sandbox root is not inside trusted repo
    2. Sandbox root does not contain trusted path patterns
    3. Trusted repo is not inside sandbox root

    Args:
        sandbox_root: Proposed sandbox root directory.
        trusted_repo_root: Root of the trusted Odibi repository.

    Raises:
        TrustedPathViolation: If sandbox root violates isolation rules.
    """
    sandbox_normalized = normalize_path(sandbox_root)
    trusted_normalized = normalize_path(trusted_repo_root)

    if is_path_inside(sandbox_normalized, trusted_normalized):
        raise TrustedPathViolation(sandbox_normalized, trusted_normalized)

    if is_path_inside(trusted_normalized, sandbox_normalized):
        raise TrustedPathViolation(
            sandbox_normalized,
            trusted_normalized,
        )

    if contains_trusted_pattern(sandbox_normalized):
        raise PathValidationError(
            "Sandbox root contains trusted path pattern",
            sandbox_normalized,
            "path contains protected pattern like 'agents/core' or '.odibi/learning_harness'",
        )


def validate_sandbox_path(
    sandbox_path: Path,
    sandbox_root: Path,
    trusted_repo_root: Path = DEFAULT_TRUSTED_REPO_ROOT,
) -> None:
    """Validate that a specific sandbox path is safe.

    Checks:
    1. Sandbox path is inside sandbox root
    2. Sandbox path is not inside trusted repo
    3. Sandbox path does not contain trusted patterns

    Args:
        sandbox_path: Specific sandbox directory path.
        sandbox_root: Root directory for all sandboxes.
        trusted_repo_root: Root of the trusted Odibi repository.

    Raises:
        PathValidationError: If path is not inside sandbox root.
        TrustedPathViolation: If path violates isolation rules.
    """
    sandbox_normalized = normalize_path(sandbox_path)
    root_normalized = normalize_path(sandbox_root)
    trusted_normalized = normalize_path(trusted_repo_root)

    if not is_path_inside(sandbox_normalized, root_normalized):
        raise PathValidationError(
            "Sandbox path must be inside sandbox root",
            sandbox_normalized,
            f"expected inside: {root_normalized}",
        )

    if is_path_inside(sandbox_normalized, trusted_normalized):
        raise TrustedPathViolation(sandbox_normalized, trusted_normalized)

    if contains_trusted_pattern(sandbox_normalized):
        raise PathValidationError(
            "Sandbox path contains trusted path pattern",
            sandbox_normalized,
            "path contains protected pattern",
        )


def validate_source_repo(
    source_repo: Path,
    trusted_repo_root: Path = DEFAULT_TRUSTED_REPO_ROOT,
) -> None:
    """Validate that source repo exists and is readable.

    Does NOT prevent cloning from trusted repo (that's the point).
    Just validates the path exists.

    Args:
        source_repo: Path to repository to clone.
        trusted_repo_root: Root of the trusted Odibi repository.

    Raises:
        PathValidationError: If source repo doesn't exist.
    """
    source_normalized = normalize_path(source_repo)

    if not source_normalized.exists():
        raise PathValidationError(
            "Source repository does not exist",
            source_normalized,
            "path not found",
        )

    if not source_normalized.is_dir():
        raise PathValidationError(
            "Source repository must be a directory",
            source_normalized,
            "not a directory",
        )

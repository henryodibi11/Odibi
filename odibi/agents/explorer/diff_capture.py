"""Diff capture utilities for Explorer experiments.

TRUST BOUNDARY:
- Diffs are computed ONLY within sandbox directories
- Diff artifacts are written ONLY to sandbox-local paths
- Diff paths are validated before any write operation
- This module never references the trusted Odibi repo
"""

import hashlib
import logging
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from odibi.agents.explorer.path_validation import (
    is_path_inside,
    validate_sandbox_path,
)

logger = logging.getLogger(__name__)

ARTIFACTS_DIR_NAME = ".explorer_artifacts"
DIFF_FILE_EXTENSION = ".diff"
EMPTY_DIFF_MARKER = "# EMPTY DIFF - No changes detected\n"


class DiffCaptureError(Exception):
    """Raised when diff capture fails."""

    def __init__(self, message: str, sandbox_path: Path, cause: Optional[Exception] = None):
        self.sandbox_path = sandbox_path
        self.cause = cause
        super().__init__(f"Diff capture failed in {sandbox_path}: {message}")


class DiffPathViolation(Exception):
    """Raised when diff path would escape sandbox."""

    def __init__(self, diff_path: Path, sandbox_path: Path):
        self.diff_path = diff_path
        self.sandbox_path = sandbox_path
        super().__init__(f"DIFF PATH VIOLATION: {diff_path} is outside sandbox {sandbox_path}")


@dataclass(frozen=True)
class DiffResult:
    """Immutable result of a diff capture operation.

    Attributes:
        diff_content: The actual diff content (may be empty marker).
        diff_path: Path where diff was written (inside sandbox).
        is_empty: Whether the diff represents no changes.
        diff_hash: SHA256 hash of diff content for repeatability verification.
        method: Method used to capture diff ('git' or 'filesystem').
        captured_at: ISO timestamp when diff was captured.
    """

    diff_content: str
    diff_path: Path
    is_empty: bool
    diff_hash: str
    method: str
    captured_at: str

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "diff_content": self.diff_content,
            "diff_path": str(self.diff_path),
            "is_empty": self.is_empty,
            "diff_hash": self.diff_hash,
            "method": self.method,
            "captured_at": self.captured_at,
        }


def get_artifacts_dir(sandbox_path: Path) -> Path:
    """Get the artifacts directory path for a sandbox.

    Args:
        sandbox_path: Root path of the sandbox.

    Returns:
        Path to the artifacts directory (not yet created).
    """
    return sandbox_path / ARTIFACTS_DIR_NAME


def ensure_artifacts_dir(sandbox_path: Path) -> Path:
    """Ensure artifacts directory exists in sandbox.

    Args:
        sandbox_path: Root path of the sandbox.

    Returns:
        Path to the created/existing artifacts directory.
    """
    artifacts_dir = get_artifacts_dir(sandbox_path)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    return artifacts_dir


def is_git_repo(path: Path) -> bool:
    """Check if path is inside a git repository.

    Args:
        path: Path to check.

    Returns:
        True if path is a git repo or inside one.
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--git-dir"],
            cwd=str(path),
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def compute_diff_hash(content: str) -> str:
    """Compute deterministic hash of diff content.

    Args:
        content: Diff content string.

    Returns:
        SHA256 hash (first 16 chars).
    """
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def capture_git_diff(sandbox_path: Path) -> str:
    """Capture diff using git.

    Captures both staged and unstaged changes.

    Args:
        sandbox_path: Root path of the sandbox (must be git repo).

    Returns:
        Diff content string (may be empty).

    Raises:
        DiffCaptureError: If git diff fails.
    """
    try:
        result = subprocess.run(
            ["git", "diff", "HEAD", "--no-color"],
            cwd=str(sandbox_path),
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode != 0:
            result_all = subprocess.run(
                ["git", "diff", "--no-color"],
                cwd=str(sandbox_path),
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result_all.returncode == 0:
                return result_all.stdout
            raise DiffCaptureError(
                f"git diff failed: {result.stderr}",
                sandbox_path,
            )

        return result.stdout

    except subprocess.TimeoutExpired:
        raise DiffCaptureError("git diff timed out", sandbox_path)
    except FileNotFoundError:
        raise DiffCaptureError("git not found", sandbox_path)


def capture_filesystem_diff(
    sandbox_path: Path,
    baseline_snapshot: Optional[dict[str, str]] = None,
) -> str:
    """Capture diff using filesystem comparison.

    This is a fallback when sandbox is not a git repo. It compares
    current file contents against a baseline snapshot.

    Args:
        sandbox_path: Root path of the sandbox.
        baseline_snapshot: Dict of {relative_path: content_hash} at baseline.
                          If None, returns empty diff (no baseline to compare).

    Returns:
        Diff content string in unified diff-like format.
    """
    if baseline_snapshot is None:
        return ""

    diff_lines = []
    current_files = {}

    for root, _, files in os.walk(sandbox_path):
        root_path = Path(root)
        if ARTIFACTS_DIR_NAME in root_path.parts:
            continue
        if ".git" in root_path.parts:
            continue

        for file_name in files:
            file_path = root_path / file_name
            rel_path = file_path.relative_to(sandbox_path)
            rel_path_str = str(rel_path).replace("\\", "/")

            try:
                content = file_path.read_text(encoding="utf-8", errors="replace")
                content_hash = hashlib.sha256(content.encode()).hexdigest()
                current_files[rel_path_str] = content_hash
            except (OSError, UnicodeDecodeError):
                continue

    for rel_path, old_hash in baseline_snapshot.items():
        if rel_path not in current_files:
            diff_lines.append(f"--- a/{rel_path}")
            diff_lines.append("+++ /dev/null")
            diff_lines.append("@@ deleted @@")
            diff_lines.append("-[file deleted]")
        elif current_files[rel_path] != old_hash:
            diff_lines.append(f"--- a/{rel_path}")
            diff_lines.append(f"+++ b/{rel_path}")
            diff_lines.append("@@ modified @@")
            diff_lines.append(
                f" [content changed, hash: {old_hash[:8]} -> {current_files[rel_path][:8]}]"
            )

    for rel_path in current_files:
        if rel_path not in baseline_snapshot:
            diff_lines.append("--- /dev/null")
            diff_lines.append(f"+++ b/{rel_path}")
            diff_lines.append("@@ added @@")
            diff_lines.append("+[new file]")

    return "\n".join(diff_lines)


def create_baseline_snapshot(sandbox_path: Path) -> dict[str, str]:
    """Create a baseline snapshot of sandbox files.

    Args:
        sandbox_path: Root path of the sandbox.

    Returns:
        Dict of {relative_path: content_hash}.
    """
    snapshot = {}

    for root, _, files in os.walk(sandbox_path):
        root_path = Path(root)
        if ARTIFACTS_DIR_NAME in root_path.parts:
            continue
        if ".git" in root_path.parts:
            continue

        for file_name in files:
            file_path = root_path / file_name
            rel_path = file_path.relative_to(sandbox_path)
            rel_path_str = str(rel_path).replace("\\", "/")

            try:
                content = file_path.read_text(encoding="utf-8", errors="replace")
                content_hash = hashlib.sha256(content.encode()).hexdigest()
                snapshot[rel_path_str] = content_hash
            except (OSError, UnicodeDecodeError):
                continue

    return snapshot


def validate_diff_path(
    diff_path: Path,
    sandbox_path: Path,
    sandbox_root: Path,
    trusted_repo_root: Path,
) -> None:
    """Validate that diff path is safe.

    Args:
        diff_path: Path where diff will be written.
        sandbox_path: Path of the sandbox.
        sandbox_root: Root directory for all sandboxes.
        trusted_repo_root: Root of the trusted Odibi repository.

    Raises:
        DiffPathViolation: If diff path is outside sandbox.
        PathValidationError: If path validation fails.
    """
    diff_path_resolved = diff_path.resolve()
    sandbox_path_resolved = sandbox_path.resolve()

    if not is_path_inside(diff_path_resolved, sandbox_path_resolved):
        raise DiffPathViolation(diff_path_resolved, sandbox_path_resolved)

    validate_sandbox_path(diff_path_resolved, sandbox_root, trusted_repo_root)


def capture_diff(
    sandbox_path: Path,
    sandbox_root: Path,
    trusted_repo_root: Path,
    experiment_id: str,
    baseline_snapshot: Optional[dict[str, str]] = None,
) -> DiffResult:
    """Capture diff from sandbox and write to artifacts.

    This is the main entry point for diff capture. It:
    1. Determines the best method (git or filesystem)
    2. Captures the diff content
    3. Validates the output path
    4. Writes diff to artifacts directory
    5. Returns a DiffResult

    Args:
        sandbox_path: Root path of the sandbox.
        sandbox_root: Root directory for all sandboxes.
        trusted_repo_root: Root of the trusted Odibi repository.
        experiment_id: ID of the experiment (for filename).
        baseline_snapshot: Baseline for filesystem diff (optional).

    Returns:
        DiffResult with captured diff details.

    Raises:
        DiffCaptureError: If diff capture fails.
        DiffPathViolation: If diff path would escape sandbox.
    """
    captured_at = datetime.now().isoformat()

    artifacts_dir = ensure_artifacts_dir(sandbox_path)
    diff_filename = f"{experiment_id}{DIFF_FILE_EXTENSION}"
    diff_path = artifacts_dir / diff_filename

    validate_diff_path(diff_path, sandbox_path, sandbox_root, trusted_repo_root)

    use_git = is_git_repo(sandbox_path)
    method = "git" if use_git else "filesystem"

    if use_git:
        diff_content = capture_git_diff(sandbox_path)
    else:
        diff_content = capture_filesystem_diff(sandbox_path, baseline_snapshot)

    is_empty = len(diff_content.strip()) == 0

    if is_empty:
        diff_content = EMPTY_DIFF_MARKER

    diff_hash = compute_diff_hash(diff_content)

    diff_path.write_text(diff_content, encoding="utf-8")
    logger.info(
        f"Captured diff for {experiment_id}: method={method}, is_empty={is_empty}, hash={diff_hash}"
    )

    return DiffResult(
        diff_content=diff_content,
        diff_path=diff_path,
        is_empty=is_empty,
        diff_hash=diff_hash,
        method=method,
        captured_at=captured_at,
    )


def read_diff(diff_path: Path) -> Optional[str]:
    """Read diff content from a diff file.

    Args:
        diff_path: Path to the diff file.

    Returns:
        Diff content string, or None if file doesn't exist.
    """
    if not diff_path.exists():
        return None
    return diff_path.read_text(encoding="utf-8")

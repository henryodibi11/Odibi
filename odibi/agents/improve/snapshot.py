"""Snapshot management for Master rollback support.

Provides compressed snapshots of the Master directory before promotions,
enabling rollback if issues are discovered.

SAFETY:
- Snapshots are stored in environment_root/snapshots/
- Snapshots are compressed (.tar.gz) by default
- Old snapshots are rotated based on max_snapshots config
"""

import logging
import shutil
import tarfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class SnapshotError(Exception):
    """Base exception for snapshot operations."""

    pass


class SnapshotCreationError(SnapshotError):
    """Raised when snapshot creation fails."""

    def __init__(self, message: str, cause: Optional[Exception] = None):
        self.cause = cause
        super().__init__(message)


class SnapshotRestoreError(SnapshotError):
    """Raised when snapshot restoration fails."""

    def __init__(self, message: str, snapshot_path: Path, cause: Optional[Exception] = None):
        self.snapshot_path = snapshot_path
        self.cause = cause
        super().__init__(f"{message}: {snapshot_path}")


@dataclass(frozen=True)
class SnapshotInfo:
    """Information about a Master snapshot.

    Attributes:
        snapshot_id: Unique identifier (e.g., "before_cycle_005").
        snapshot_path: Absolute path to the snapshot file.
        created_at: ISO timestamp of snapshot creation.
        size_bytes: Size of the snapshot file in bytes.
        compressed: Whether the snapshot is compressed.
    """

    snapshot_id: str
    snapshot_path: Path
    created_at: str
    size_bytes: int
    compressed: bool


def create_snapshot(
    source_path: Path,
    snapshots_dir: Path,
    snapshot_id: str,
    compress: bool = True,
) -> SnapshotInfo:
    """Create a snapshot of the source directory.

    Args:
        source_path: Path to snapshot (typically Master directory).
        snapshots_dir: Directory to store snapshots.
        snapshot_id: Unique identifier for this snapshot.
        compress: Whether to create compressed .tar.gz (default True).

    Returns:
        SnapshotInfo describing the created snapshot.

    Raises:
        SnapshotCreationError: If snapshot creation fails.
    """
    if not source_path.exists():
        raise SnapshotCreationError(f"Source path does not exist: {source_path}")

    snapshots_dir.mkdir(parents=True, exist_ok=True)

    created_at = datetime.now().isoformat()

    if compress:
        snapshot_filename = f"{snapshot_id}.tar.gz"
        snapshot_path = snapshots_dir / snapshot_filename

        try:
            with tarfile.open(snapshot_path, "w:gz") as tar:
                # Add source directory with relative path
                tar.add(source_path, arcname=source_path.name)
            logger.info(f"Created compressed snapshot at {snapshot_path}")
        except Exception as e:
            # Cleanup partial snapshot
            if snapshot_path.exists():
                try:
                    snapshot_path.unlink()
                except Exception:
                    pass
            raise SnapshotCreationError(
                f"Failed to create compressed snapshot: {e}",
                cause=e,
            )
    else:
        snapshot_filename = snapshot_id
        snapshot_path = snapshots_dir / snapshot_filename

        try:
            shutil.copytree(
                source_path,
                snapshot_path,
                symlinks=False,
                ignore=shutil.ignore_patterns(
                    "__pycache__",
                    "*.pyc",
                    ".pytest_cache",
                    ".mypy_cache",
                    ".ruff_cache",
                ),
            )
            logger.info(f"Created directory snapshot at {snapshot_path}")
        except Exception as e:
            if snapshot_path.exists():
                try:
                    shutil.rmtree(snapshot_path)
                except Exception:
                    pass
            raise SnapshotCreationError(
                f"Failed to create directory snapshot: {e}",
                cause=e,
            )

    # Get size
    if compress:
        size_bytes = snapshot_path.stat().st_size
    else:
        size_bytes = sum(f.stat().st_size for f in snapshot_path.rglob("*") if f.is_file())

    return SnapshotInfo(
        snapshot_id=snapshot_id,
        snapshot_path=snapshot_path,
        created_at=created_at,
        size_bytes=size_bytes,
        compressed=compress,
    )


def restore_snapshot(
    snapshot_path: Path,
    target_path: Path,
    compressed: bool = True,
) -> None:
    """Restore a snapshot to target path.

    This will DELETE the target directory and replace it with snapshot contents.

    Args:
        snapshot_path: Path to the snapshot file or directory.
        target_path: Path to restore to (will be deleted and recreated).
        compressed: Whether the snapshot is compressed.

    Raises:
        SnapshotRestoreError: If restoration fails.
    """
    if not snapshot_path.exists():
        raise SnapshotRestoreError(
            "Snapshot does not exist",
            snapshot_path,
        )

    # Remove existing target
    if target_path.exists():
        try:
            shutil.rmtree(target_path)
            logger.info(f"Removed existing target at {target_path}")
        except Exception as e:
            raise SnapshotRestoreError(
                f"Failed to remove existing target: {e}",
                snapshot_path,
                cause=e,
            )

    # Ensure parent exists
    target_path.parent.mkdir(parents=True, exist_ok=True)

    if compressed:
        try:
            with tarfile.open(snapshot_path, "r:gz") as tar:
                # Extract to parent, then rename if needed
                tar.extractall(target_path.parent, filter="data")

            # tarfile extracts with the original directory name
            # We may need to rename it
            extracted_name = None
            with tarfile.open(snapshot_path, "r:gz") as tar:
                members = tar.getmembers()
                if members:
                    extracted_name = members[0].name.split("/")[0]

            if extracted_name and extracted_name != target_path.name:
                extracted_path = target_path.parent / extracted_name
                if extracted_path.exists():
                    extracted_path.rename(target_path)

            logger.info(f"Restored snapshot from {snapshot_path} to {target_path}")
        except Exception as e:
            raise SnapshotRestoreError(
                f"Failed to restore compressed snapshot: {e}",
                snapshot_path,
                cause=e,
            )
    else:
        try:
            shutil.copytree(snapshot_path, target_path)
            logger.info(f"Restored snapshot from {snapshot_path} to {target_path}")
        except Exception as e:
            raise SnapshotRestoreError(
                f"Failed to restore directory snapshot: {e}",
                snapshot_path,
                cause=e,
            )


def list_snapshots(snapshots_dir: Path) -> list[SnapshotInfo]:
    """List all snapshots in the snapshots directory.

    Args:
        snapshots_dir: Directory containing snapshots.

    Returns:
        List of SnapshotInfo, sorted by creation time (oldest first).
    """
    if not snapshots_dir.exists():
        return []

    snapshots = []

    for item in snapshots_dir.iterdir():
        if item.name.startswith("."):
            continue

        if item.is_file() and item.suffix == ".gz" and item.stem.endswith(".tar"):
            # Compressed snapshot
            snapshot_id = item.stem[:-4]  # Remove .tar
            size_bytes = item.stat().st_size
            created_at = datetime.fromtimestamp(item.stat().st_mtime).isoformat()
            snapshots.append(
                SnapshotInfo(
                    snapshot_id=snapshot_id,
                    snapshot_path=item,
                    created_at=created_at,
                    size_bytes=size_bytes,
                    compressed=True,
                )
            )
        elif item.is_dir():
            # Directory snapshot
            snapshot_id = item.name
            size_bytes = sum(f.stat().st_size for f in item.rglob("*") if f.is_file())
            created_at = datetime.fromtimestamp(item.stat().st_mtime).isoformat()
            snapshots.append(
                SnapshotInfo(
                    snapshot_id=snapshot_id,
                    snapshot_path=item,
                    created_at=created_at,
                    size_bytes=size_bytes,
                    compressed=False,
                )
            )

    # Sort by creation time (oldest first)
    snapshots.sort(key=lambda s: s.created_at)
    return snapshots


def rotate_snapshots(
    snapshots_dir: Path,
    max_snapshots: int,
) -> list[Path]:
    """Rotate old snapshots to stay within limit.

    Deletes oldest snapshots first until count <= max_snapshots.

    Args:
        snapshots_dir: Directory containing snapshots.
        max_snapshots: Maximum number of snapshots to keep.

    Returns:
        List of deleted snapshot paths.
    """
    snapshots = list_snapshots(snapshots_dir)

    if len(snapshots) <= max_snapshots:
        return []

    to_delete = snapshots[: len(snapshots) - max_snapshots]
    deleted = []

    for snapshot in to_delete:
        try:
            if snapshot.compressed:
                snapshot.snapshot_path.unlink()
            else:
                shutil.rmtree(snapshot.snapshot_path)
            deleted.append(snapshot.snapshot_path)
            logger.info(f"Rotated old snapshot: {snapshot.snapshot_path}")
        except Exception as e:
            logger.warning(f"Failed to delete snapshot {snapshot.snapshot_path}: {e}")

    return deleted

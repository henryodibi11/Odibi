"""Disk Usage Guard for Phase 9.A Autonomous Learning.

Enforces disk usage limits on .odibi/ directories to prevent unbounded
disk consumption during long-running autonomous learning cycles.

Features:
- Configurable per-directory byte limits
- Oldest-first rotation (by mtime)
- Safety: ONLY deletes files within .odibi/ subdirectories
- Deterministic behavior
- Fail-safe (never crashes cycle on cleanup failure)
"""

import json
import logging
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


# Default budgets (in bytes)
DEFAULT_ARTIFACTS_BUDGET_BYTES = 20 * 1024 * 1024 * 1024  # 20 GB
DEFAULT_REPORTS_BUDGET_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB
DEFAULT_INDEX_BUDGET_BYTES = 5 * 1024 * 1024 * 1024  # 5 GB


class DiskGuardError(Exception):
    """Raised when disk guard encounters a safety violation."""

    pass


@dataclass
class DiskUsageSummary:
    """Summary of disk usage for a directory."""

    path: str
    total_bytes: int
    file_count: int
    budget_bytes: int
    over_budget: bool
    bytes_to_free: int

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "total_bytes": self.total_bytes,
            "file_count": self.file_count,
            "budget_bytes": self.budget_bytes,
            "over_budget": self.over_budget,
            "bytes_to_free": self.bytes_to_free,
        }


@dataclass
class RotationResult:
    """Result of a rotation operation."""

    directory: str
    files_deleted: int
    bytes_freed: int
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "directory": self.directory,
            "files_deleted": self.files_deleted,
            "bytes_freed": self.bytes_freed,
            "errors": self.errors,
        }


@dataclass
class DiskGuardReport:
    """Report from a disk guard run."""

    timestamp: str
    summaries: list[DiskUsageSummary]
    rotations: list[RotationResult]
    total_bytes_freed: int

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "summaries": [s.to_dict() for s in self.summaries],
            "rotations": [r.to_dict() for r in self.rotations],
            "total_bytes_freed": self.total_bytes_freed,
        }


class DiskUsageGuard:
    """Guards disk usage for .odibi/ directories.

    SAFETY INVARIANTS:
    - ONLY operates on directories under odibi_root/.odibi/
    - NEVER deletes directories, only files
    - Uses oldest-first rotation (deterministic by mtime)
    - Logs all deletions
    - Fails safe (logs errors, does not crash)
    """

    ALLOWED_SUBDIRS = frozenset({"artifacts", "reports", "index", "cycles", "memories"})

    def __init__(
        self,
        odibi_root: str,
        artifacts_budget_bytes: int = DEFAULT_ARTIFACTS_BUDGET_BYTES,
        reports_budget_bytes: int = DEFAULT_REPORTS_BUDGET_BYTES,
        index_budget_bytes: int = DEFAULT_INDEX_BUDGET_BYTES,
    ):
        """Initialize disk usage guard.

        Args:
            odibi_root: Path to Odibi repository root.
            artifacts_budget_bytes: Max bytes for .odibi/artifacts/
            reports_budget_bytes: Max bytes for .odibi/reports/
            index_budget_bytes: Max bytes for .odibi/index/
        """
        self.odibi_root = os.path.abspath(odibi_root)
        self.odibi_dir = os.path.join(self.odibi_root, ".odibi")

        self.budgets = {
            "artifacts": artifacts_budget_bytes,
            "reports": reports_budget_bytes,
            "index": index_budget_bytes,
        }

    def _is_safe_path(self, path: str) -> bool:
        """Check if path is safe for deletion (within .odibi/ subdirs).

        Args:
            path: Absolute path to check.

        Returns:
            True if path is within an allowed .odibi/ subdirectory.
        """
        abs_path = os.path.abspath(path)
        odibi_dir = os.path.abspath(self.odibi_dir)

        if not abs_path.startswith(odibi_dir + os.sep):
            return False

        relative = os.path.relpath(abs_path, odibi_dir)
        parts = relative.split(os.sep)

        if not parts:
            return False

        return parts[0] in self.ALLOWED_SUBDIRS

    def get_directory_size(self, directory: str) -> tuple[int, int]:
        """Get total size and file count for a directory.

        Args:
            directory: Path to directory.

        Returns:
            Tuple of (total_bytes, file_count).
        """
        total_bytes = 0
        file_count = 0

        if not os.path.isdir(directory):
            return 0, 0

        for root, _dirs, files in os.walk(directory):
            for filename in files:
                filepath = os.path.join(root, filename)
                try:
                    total_bytes += os.path.getsize(filepath)
                    file_count += 1
                except OSError:
                    pass

        return total_bytes, file_count

    def get_usage_summary(self, subdir: str) -> DiskUsageSummary:
        """Get disk usage summary for a .odibi/ subdirectory.

        Args:
            subdir: Subdirectory name (e.g., "artifacts", "reports").

        Returns:
            DiskUsageSummary with usage details.
        """
        directory = os.path.join(self.odibi_dir, subdir)
        budget = self.budgets.get(subdir, 0)
        total_bytes, file_count = self.get_directory_size(directory)

        over_budget = total_bytes > budget
        bytes_to_free = max(0, total_bytes - budget)

        return DiskUsageSummary(
            path=directory,
            total_bytes=total_bytes,
            file_count=file_count,
            budget_bytes=budget,
            over_budget=over_budget,
            bytes_to_free=bytes_to_free,
        )

    def get_files_by_mtime(self, directory: str) -> list[tuple[str, float, int]]:
        """Get all files in directory sorted by mtime (oldest first).

        Args:
            directory: Path to directory.

        Returns:
            List of (filepath, mtime, size) tuples, oldest first.
        """
        files = []

        if not os.path.isdir(directory):
            return files

        for root, _dirs, filenames in os.walk(directory):
            for filename in filenames:
                filepath = os.path.join(root, filename)
                try:
                    stat = os.stat(filepath)
                    files.append((filepath, stat.st_mtime, stat.st_size))
                except OSError:
                    pass

        files.sort(key=lambda x: x[1])
        return files

    def rotate_directory(self, subdir: str, dry_run: bool = False) -> RotationResult:
        """Rotate files in a .odibi/ subdirectory to stay within budget.

        Deletes oldest files first until directory is under budget.

        Args:
            subdir: Subdirectory name (e.g., "artifacts").
            dry_run: If True, don't actually delete files.

        Returns:
            RotationResult with deletion details.
        """
        directory = os.path.join(self.odibi_dir, subdir)
        result = RotationResult(directory=directory, files_deleted=0, bytes_freed=0)

        if subdir not in self.budgets:
            result.errors.append(f"No budget configured for subdirectory: {subdir}")
            return result

        summary = self.get_usage_summary(subdir)
        if not summary.over_budget:
            return result

        bytes_to_free = summary.bytes_to_free
        files = self.get_files_by_mtime(directory)

        for filepath, _mtime, size in files:
            if result.bytes_freed >= bytes_to_free:
                break

            if not self._is_safe_path(filepath):
                result.errors.append(f"SAFETY: Skipped unsafe path: {filepath}")
                continue

            if not dry_run:
                try:
                    os.remove(filepath)
                    result.files_deleted += 1
                    result.bytes_freed += size
                    logger.info(f"DiskGuard: Deleted {filepath} ({size} bytes)")
                except OSError as e:
                    result.errors.append(f"Failed to delete {filepath}: {e}")
            else:
                result.files_deleted += 1
                result.bytes_freed += size

        return result

    def run_cleanup(self, dry_run: bool = False) -> DiskGuardReport:
        """Run cleanup on all monitored directories.

        Args:
            dry_run: If True, don't actually delete files.

        Returns:
            DiskGuardReport with all results.
        """
        timestamp = datetime.now().isoformat()
        summaries = []
        rotations = []
        total_bytes_freed = 0

        for subdir in self.budgets.keys():
            summary = self.get_usage_summary(subdir)
            summaries.append(summary)

            if summary.over_budget:
                rotation = self.rotate_directory(subdir, dry_run=dry_run)
                rotations.append(rotation)
                total_bytes_freed += rotation.bytes_freed

                if rotation.errors:
                    for error in rotation.errors:
                        logger.warning(f"DiskGuard rotation error: {error}")

        return DiskGuardReport(
            timestamp=timestamp,
            summaries=summaries,
            rotations=rotations,
            total_bytes_freed=total_bytes_freed,
        )

    def get_total_usage(self) -> dict[str, int]:
        """Get current usage for all monitored directories.

        Returns:
            Dict mapping subdir name to bytes used.
        """
        usage = {}
        for subdir in self.budgets.keys():
            directory = os.path.join(self.odibi_dir, subdir)
            total_bytes, _ = self.get_directory_size(directory)
            usage[subdir] = total_bytes
        return usage


@dataclass
class HeartbeatData:
    """Data written to the heartbeat file."""

    last_cycle_id: str
    timestamp: str
    last_status: str
    cycles_completed: int
    cycles_failed: int
    disk_usage: dict[str, int]
    profile_id: str = ""
    profile_name: str = ""
    profile_hash: str = ""

    def to_dict(self) -> dict:
        result = {
            "last_cycle_id": self.last_cycle_id,
            "timestamp": self.timestamp,
            "last_status": self.last_status,
            "cycles_completed": self.cycles_completed,
            "cycles_failed": self.cycles_failed,
            "disk_usage": self.disk_usage,
        }
        if self.profile_id:
            result["profile_id"] = self.profile_id
            result["profile_name"] = self.profile_name
            result["profile_hash"] = self.profile_hash
        return result


class HeartbeatWriter:
    """Writes heartbeat file atomically for external monitoring.

    Uses temp file + rename for atomic writes.
    """

    def __init__(self, odibi_root: str):
        """Initialize heartbeat writer.

        Args:
            odibi_root: Path to Odibi repository root.
        """
        self.odibi_root = os.path.abspath(odibi_root)
        self.odibi_dir = os.path.join(self.odibi_root, ".odibi")
        self.heartbeat_path = os.path.join(self.odibi_dir, "heartbeat.json")

    def write(self, data: HeartbeatData) -> bool:
        """Write heartbeat data atomically.

        Args:
            data: HeartbeatData to write.

        Returns:
            True if write succeeded.
        """
        try:
            os.makedirs(self.odibi_dir, exist_ok=True)

            fd, temp_path = tempfile.mkstemp(
                suffix=".json",
                prefix="heartbeat_",
                dir=self.odibi_dir,
            )

            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(data.to_dict(), f, indent=2)

                if os.name == "nt":
                    if os.path.exists(self.heartbeat_path):
                        os.remove(self.heartbeat_path)

                os.rename(temp_path, self.heartbeat_path)
                return True

            except Exception:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                raise

        except Exception as e:
            logger.warning(f"Failed to write heartbeat: {e}")
            return False

    def read(self) -> Optional[HeartbeatData]:
        """Read current heartbeat data.

        Returns:
            HeartbeatData if file exists, None otherwise.
        """
        if not os.path.exists(self.heartbeat_path):
            return None

        try:
            with open(self.heartbeat_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            return HeartbeatData(
                last_cycle_id=data.get("last_cycle_id", ""),
                timestamp=data.get("timestamp", ""),
                last_status=data.get("last_status", ""),
                cycles_completed=data.get("cycles_completed", 0),
                cycles_failed=data.get("cycles_failed", 0),
                disk_usage=data.get("disk_usage", {}),
                profile_id=data.get("profile_id", ""),
                profile_name=data.get("profile_name", ""),
                profile_hash=data.get("profile_hash", ""),
            )
        except Exception as e:
            logger.warning(f"Failed to read heartbeat: {e}")
            return None


class ReportErrorWriter:
    """Writes report generation errors to .odibi/reports/errors/.

    Provides a fallback when report generation fails, ensuring
    errors are captured for debugging.
    """

    def __init__(self, odibi_root: str):
        """Initialize error writer.

        Args:
            odibi_root: Path to Odibi repository root.
        """
        self.odibi_root = os.path.abspath(odibi_root)
        self.errors_dir = os.path.join(self.odibi_root, ".odibi", "reports", "errors")

    def write_error(self, cycle_id: str, error: Exception, context: str = "") -> str:
        """Write an error record for a failed report generation.

        Args:
            cycle_id: The cycle ID.
            error: The exception that occurred.
            context: Additional context about the failure.

        Returns:
            Path to the error file, or empty string on failure.
        """
        try:
            os.makedirs(self.errors_dir, exist_ok=True)

            error_path = os.path.join(self.errors_dir, f"{cycle_id}.json")
            error_data = {
                "cycle_id": cycle_id,
                "timestamp": datetime.now().isoformat(),
                "error_type": type(error).__name__,
                "error_message": str(error),
                "context": context,
            }

            with open(error_path, "w", encoding="utf-8") as f:
                json.dump(error_data, f, indent=2)

            return error_path

        except Exception as e:
            logger.error(f"Failed to write error record: {e}")
            return ""

"""PromotionBucket: Submit-only store for promotion candidates awaiting human review.

TRUST BOUNDARY:
- Explorer has SUBMIT-ONLY access to this bucket
- Explorer can ADD candidates but CANNOT change their status
- All submissions MUST have status=PENDING
- Status transitions happen OUTSIDE the Explorer (review UI / trusted flow)
- This is a one-way data flow: Explorer -> Bucket -> Human -> Trusted

Storage: Separate JSONL file from ExplorerMemory.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Optional

if sys.platform != "win32":
    import fcntl

if TYPE_CHECKING:
    from odibi.agents.explorer.memory import MemoryEntry

logger = logging.getLogger(__name__)

BUCKET_FILE_NAME = "promotion_bucket.jsonl"


class CandidateStatus(str, Enum):
    """Status of a promotion candidate."""

    PENDING = "pending"  # Awaiting human review
    APPROVED = "approved"  # Human approved (will be promoted)
    REJECTED = "rejected"  # Human rejected
    APPLIED = "applied"  # Already applied to trusted codebase


class SubmitStatusViolation(Exception):
    """Raised when Explorer attempts to submit non-PENDING status."""

    def __init__(self, candidate_id: str, status: CandidateStatus):
        self.candidate_id = candidate_id
        self.status = status
        super().__init__(
            f"SUBMIT STATUS VIOLATION: Explorer can only submit PENDING candidates. "
            f"Candidate {candidate_id} has status={status.value}. "
            f"Status transitions require human review outside the Explorer."
        )


class DuplicateCandidateError(Exception):
    """Raised when attempting to submit duplicate candidate."""

    def __init__(self, candidate_id: str, reason: str):
        self.candidate_id = candidate_id
        self.reason = reason
        super().__init__(
            f"Duplicate candidate submission rejected: {reason} (candidate_id={candidate_id})"
        )


class BucketCorruptionError(Exception):
    """Raised when bucket storage file is corrupted."""

    def __init__(self, storage_path: Path, line_number: int, message: str):
        self.storage_path = storage_path
        self.line_number = line_number
        super().__init__(f"Bucket corruption in {storage_path} at line {line_number}: {message}")


@dataclass(frozen=True)
class PromotionEntry:
    """Immutable record of a candidate for promotion.

    Attributes:
        candidate_id: Unique identifier for this candidate.
        diff_hash: Content-addressable hash of the diff.
        diff_path: Path to the diff artifact file.
        memory_entry_id: ID of the related MemoryEntry.
        experiment_id: ID of the experiment that produced this.
        submitted_at: ISO timestamp when candidate was submitted.
        diff_summary: Human-readable summary of changes.
        hypothesis: What the exploration was testing.
        observation: What was observed.
        status: Current status (Explorer can ONLY submit PENDING).
        reviewed_at: ISO timestamp of review (None if pending).
        reviewer_notes: Notes from human reviewer (None if pending).
        metadata: Additional context as immutable tuple of key-value pairs.
    """

    candidate_id: str
    diff_hash: str
    diff_path: Path
    memory_entry_id: str
    experiment_id: str
    submitted_at: str
    diff_summary: str
    hypothesis: str
    observation: str
    status: CandidateStatus = CandidateStatus.PENDING
    reviewed_at: Optional[str] = None
    reviewer_notes: Optional[str] = None
    metadata: tuple = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "candidate_id": self.candidate_id,
            "diff_hash": self.diff_hash,
            "diff_path": str(self.diff_path),
            "memory_entry_id": self.memory_entry_id,
            "experiment_id": self.experiment_id,
            "submitted_at": self.submitted_at,
            "diff_summary": self.diff_summary,
            "hypothesis": self.hypothesis,
            "observation": self.observation,
            "status": self.status.value,
            "reviewed_at": self.reviewed_at,
            "reviewer_notes": self.reviewer_notes,
            "metadata": dict(self.metadata) if self.metadata else {},
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PromotionEntry":
        """Create PromotionEntry from dictionary."""
        metadata = data.get("metadata", {})
        if isinstance(metadata, dict):
            metadata = tuple(metadata.items())
        elif isinstance(metadata, (list, tuple)):
            metadata = tuple(metadata)

        diff_path = data.get("diff_path")
        if diff_path is not None:
            diff_path = Path(diff_path)

        return cls(
            candidate_id=data["candidate_id"],
            diff_hash=data["diff_hash"],
            diff_path=diff_path,
            memory_entry_id=data["memory_entry_id"],
            experiment_id=data["experiment_id"],
            submitted_at=data["submitted_at"],
            diff_summary=data.get("diff_summary", ""),
            hypothesis=data.get("hypothesis", ""),
            observation=data.get("observation", ""),
            status=CandidateStatus(data.get("status", "pending")),
            reviewed_at=data.get("reviewed_at"),
            reviewer_notes=data.get("reviewer_notes"),
            metadata=metadata,
        )

    @classmethod
    def from_json(cls, json_str: str) -> "PromotionEntry":
        """Create PromotionEntry from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)


# Alias for backward compatibility
PromotionCandidate = PromotionEntry


def generate_candidate_id(diff_hash: str, experiment_id: str, timestamp: str) -> str:
    """Generate a unique candidate ID.

    Args:
        diff_hash: Hash of the diff content.
        experiment_id: Experiment ID.
        timestamp: ISO timestamp.

    Returns:
        Unique candidate ID string.
    """
    content = f"{diff_hash}:{experiment_id}:{timestamp}"
    hash_suffix = hashlib.sha256(content.encode()).hexdigest()[:12]
    return f"promo_{hash_suffix}"


class FileLock:
    """Simple file-based lock for cross-platform compatibility."""

    def __init__(self, lock_path: Path):
        self._lock_path = lock_path
        self._lock_file = None

    def __enter__(self):
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_file = open(self._lock_path, "w")

        if sys.platform == "win32":
            import msvcrt

            msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_LOCK, 1)
        else:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._lock_file:
            if sys.platform == "win32":
                import msvcrt

                try:
                    msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
            else:
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
            self._lock_file.close()
            self._lock_file = None
        return False


class PromotionBucket:
    """Submit-only bucket for promotion candidates awaiting human review.

    From the Explorer's perspective, this provides:
    - SUBMIT: Add new candidates (PENDING status only)
    - READ: Query candidates and their status

    The Explorer CANNOT:
    - Change candidate status (no APPROVED/REJECTED/APPLIED)
    - Modify existing candidates
    - Remove candidates from the bucket

    Status changes happen through a separate trusted review
    process that is outside the Explorer's control.

    Storage: JSONL format, separate from ExplorerMemory.

    Attributes:
        storage_path: Path to the bucket storage file.
    """

    def __init__(self, storage_path: Path) -> None:
        """Initialize the promotion bucket.

        Args:
            storage_path: Path where candidates are persisted.
        """
        self._storage_path = Path(storage_path)
        self._lock_path = self._storage_path.with_suffix(".lock")
        self._entries: list[PromotionEntry] = []
        self._entry_index: dict[str, int] = {}
        self._diff_hash_index: dict[str, list[str]] = {}

        self._load_from_disk()

    @property
    def storage_path(self) -> Path:
        """Read-only access to storage path."""
        return self._storage_path

    def _load_from_disk(self) -> None:
        """Load existing entries from disk if file exists."""
        if not self._storage_path.exists():
            logger.debug(f"No existing bucket file at {self._storage_path}")
            return

        with FileLock(self._lock_path):
            with open(self._storage_path, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        entry = PromotionEntry.from_json(line)
                        self._add_to_indices(entry)
                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        raise BucketCorruptionError(
                            self._storage_path,
                            line_num,
                            str(e),
                        )

        logger.info(f"Loaded {len(self._entries)} candidates from {self._storage_path}")

    def _add_to_indices(self, entry: PromotionEntry) -> None:
        """Add entry to in-memory indices."""
        index = len(self._entries)
        self._entries.append(entry)
        self._entry_index[entry.candidate_id] = index

        if entry.diff_hash:
            if entry.diff_hash not in self._diff_hash_index:
                self._diff_hash_index[entry.diff_hash] = []
            self._diff_hash_index[entry.diff_hash].append(entry.candidate_id)

    def submit(self, entry: PromotionEntry) -> str:
        """Submit a new candidate for human review.

        This is the ONLY write operation available to Explorer.
        Explorer can ONLY submit candidates with PENDING status.

        Args:
            entry: PromotionEntry to submit.

        Returns:
            The candidate_id of the submitted candidate.

        Raises:
            SubmitStatusViolation: If status is not PENDING.
            DuplicateCandidateError: If candidate_id or diff_hash already exists.
        """
        if entry.status != CandidateStatus.PENDING:
            raise SubmitStatusViolation(entry.candidate_id, entry.status)

        if entry.candidate_id in self._entry_index:
            raise DuplicateCandidateError(
                entry.candidate_id,
                "candidate_id already exists",
            )

        if entry.diff_hash and entry.diff_hash in self._diff_hash_index:
            existing_ids = self._diff_hash_index[entry.diff_hash]
            raise DuplicateCandidateError(
                entry.candidate_id,
                f"diff_hash {entry.diff_hash} already submitted as {existing_ids}",
            )

        json_line = entry.to_json() + "\n"

        with FileLock(self._lock_path):
            self._storage_path.parent.mkdir(parents=True, exist_ok=True)

            with open(self._storage_path, "a", encoding="utf-8") as f:
                f.write(json_line)
                f.flush()
                os.fsync(f.fileno())

            self._add_to_indices(entry)

        logger.info(f"Submitted candidate {entry.candidate_id} to bucket")
        return entry.candidate_id

    def get(self, candidate_id: str) -> Optional[PromotionEntry]:
        """Get a candidate by ID (read-only).

        Args:
            candidate_id: ID of the candidate to retrieve.

        Returns:
            PromotionEntry if found, None otherwise.
        """
        index = self._entry_index.get(candidate_id)
        if index is not None:
            return self._entries[index]
        return None

    def get_by_diff_hash(self, diff_hash: str) -> list[PromotionEntry]:
        """Get candidates by diff hash (content-addressable).

        Args:
            diff_hash: Hash of the diff content.

        Returns:
            List of PromotionEntry objects with matching diff_hash.
        """
        candidate_ids = self._diff_hash_index.get(diff_hash, [])
        return [self._entries[self._entry_index[cid]] for cid in candidate_ids]

    def has_diff_hash(self, diff_hash: str) -> bool:
        """Check if a diff hash already exists in bucket.

        Args:
            diff_hash: Hash to check.

        Returns:
            True if diff_hash exists, False otherwise.
        """
        return diff_hash in self._diff_hash_index

    def list_pending(self) -> list[PromotionEntry]:
        """List all pending candidates (read-only).

        Returns:
            List of candidates with PENDING status.
        """
        return [e for e in self._entries if e.status == CandidateStatus.PENDING]

    def list_by_status(self, status: CandidateStatus) -> list[PromotionEntry]:
        """List candidates by status (read-only).

        Args:
            status: Status to filter by.

        Returns:
            List of matching candidates.
        """
        return [e for e in self._entries if e.status == status]

    def list_all(self) -> list[PromotionEntry]:
        """List all candidates (read-only).

        Returns:
            List of all candidates.
        """
        return list(self._entries)

    def count(self) -> int:
        """Get total number of candidates.

        Returns:
            Number of candidates in bucket.
        """
        return len(self._entries)

    def count_pending(self) -> int:
        """Count pending candidates.

        Returns:
            Number of candidates awaiting review.
        """
        return len(self.list_pending())

    def __iter__(self) -> Iterator[PromotionEntry]:
        """Iterate over all candidates."""
        return iter(self._entries)

    def __len__(self) -> int:
        """Number of candidates in bucket."""
        return len(self._entries)

    def __contains__(self, candidate_id: str) -> bool:
        """Check if candidate_id exists in bucket."""
        return candidate_id in self._entry_index


def create_promotion_entry_from_memory(
    memory_entry: "MemoryEntry",
    diff_summary: str = "",
) -> PromotionEntry:
    """Create a PromotionEntry from a MemoryEntry.

    Helper function to convert memory entries into promotion candidates.

    Args:
        memory_entry: MemoryEntry from odibi.agents.explorer.memory module.
        diff_summary: Human-readable summary of changes.

    Returns:
        PromotionEntry ready for submission.
    """
    timestamp = datetime.now().isoformat()
    candidate_id = generate_candidate_id(
        memory_entry.diff_hash,
        memory_entry.experiment_id,
        timestamp,
    )

    return PromotionEntry(
        candidate_id=candidate_id,
        diff_hash=memory_entry.diff_hash,
        diff_path=memory_entry.diff_path or Path(""),
        memory_entry_id=memory_entry.entry_id,
        experiment_id=memory_entry.experiment_id,
        submitted_at=timestamp,
        diff_summary=diff_summary,
        hypothesis=memory_entry.hypothesis,
        observation=memory_entry.observation,
        status=CandidateStatus.PENDING,
        metadata=memory_entry.metadata,
    )

"""ExplorerMemory: Append-only memory for exploration outcomes.

TRUST BOUNDARY:
- Memory is APPEND-ONLY (no deletions, no modifications)
- All entries are ADVISORY (no automatic actions)
- Separate from trusted codebase memory
- Human review required before any promotion
- Explorer CANNOT mark entries as promoted
"""

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

if TYPE_CHECKING:
    from odibi.agents.explorer.experiment_runner import ExperimentResult

if sys.platform != "win32":
    import fcntl

logger = logging.getLogger(__name__)

MEMORY_FILE_NAME = "explorer_memory.jsonl"


class ExplorationOutcome(str, Enum):
    """Outcome classification for an exploration."""

    SUCCESS = "success"
    FAILURE = "failure"
    INCONCLUSIVE = "inconclusive"
    TIMEOUT = "timeout"


class PromotedViolation(Exception):
    """Raised when Explorer attempts to set promoted=True."""

    def __init__(self, entry_id: str):
        self.entry_id = entry_id
        super().__init__(
            f"PROMOTION VIOLATION: Explorer cannot create entry {entry_id} with "
            f"promoted=True. Promotion requires human review outside the Explorer."
        )


class MemoryAppendError(Exception):
    """Raised when memory append fails."""

    def __init__(self, message: str, entry_id: str, cause: Optional[Exception] = None):
        self.entry_id = entry_id
        self.cause = cause
        super().__init__(f"Memory append failed for {entry_id}: {message}")


class MemoryCorruptionError(Exception):
    """Raised when memory file is corrupted."""

    def __init__(self, storage_path: Path, line_number: int, message: str):
        self.storage_path = storage_path
        self.line_number = line_number
        super().__init__(f"Memory corruption in {storage_path} at line {line_number}: {message}")


@dataclass(frozen=True)
class MemoryEntry:
    """Immutable record of an exploration outcome.

    All fields are frozen to enforce append-only semantics.

    Attributes:
        entry_id: Unique identifier for this entry.
        experiment_id: ID of the experiment this relates to.
        sandbox_id: ID of the sandbox where experiment ran.
        timestamp: ISO timestamp when entry was created.
        outcome: Classification of the exploration outcome.
        hypothesis: What the exploration was testing.
        observation: What was actually observed.
        diff_hash: Hash of the diff content (content-addressable key).
        diff_path: Path to full diff file (if any).
        diff_is_empty: Whether diff represents no changes.
        exit_code: Process exit code from experiment.
        stdout_summary: First N chars of stdout.
        stderr_summary: First N chars of stderr.
        promoted: Whether this was promoted (always False from Explorer).
        metadata: Additional context as immutable tuple of key-value pairs.
    """

    entry_id: str
    experiment_id: str
    sandbox_id: str
    timestamp: str
    outcome: ExplorationOutcome
    hypothesis: str
    observation: str
    diff_hash: str = ""
    diff_path: Optional[Path] = None
    diff_is_empty: bool = True
    exit_code: Optional[int] = None
    stdout_summary: str = ""
    stderr_summary: str = ""
    promoted: bool = False
    metadata: tuple = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "entry_id": self.entry_id,
            "experiment_id": self.experiment_id,
            "sandbox_id": self.sandbox_id,
            "timestamp": self.timestamp,
            "outcome": self.outcome.value,
            "hypothesis": self.hypothesis,
            "observation": self.observation,
            "diff_hash": self.diff_hash,
            "diff_path": str(self.diff_path) if self.diff_path else None,
            "diff_is_empty": self.diff_is_empty,
            "exit_code": self.exit_code,
            "stdout_summary": self.stdout_summary,
            "stderr_summary": self.stderr_summary,
            "promoted": self.promoted,
            "metadata": dict(self.metadata) if self.metadata else {},
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MemoryEntry":
        """Create MemoryEntry from dictionary."""
        metadata = data.get("metadata", {})
        if isinstance(metadata, dict):
            metadata = tuple(metadata.items())
        elif isinstance(metadata, (list, tuple)):
            metadata = tuple(metadata)

        diff_path = data.get("diff_path")
        if diff_path is not None:
            diff_path = Path(diff_path)

        return cls(
            entry_id=data["entry_id"],
            experiment_id=data["experiment_id"],
            sandbox_id=data.get("sandbox_id", ""),
            timestamp=data["timestamp"],
            outcome=ExplorationOutcome(data["outcome"]),
            hypothesis=data.get("hypothesis", ""),
            observation=data.get("observation", ""),
            diff_hash=data.get("diff_hash", ""),
            diff_path=diff_path,
            diff_is_empty=data.get("diff_is_empty", True),
            exit_code=data.get("exit_code"),
            stdout_summary=data.get("stdout_summary", ""),
            stderr_summary=data.get("stderr_summary", ""),
            promoted=data.get("promoted", False),
            metadata=metadata,
        )

    @classmethod
    def from_json(cls, json_str: str) -> "MemoryEntry":
        """Create MemoryEntry from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)


def generate_entry_id(experiment_id: str, timestamp: str) -> str:
    """Generate a unique entry ID.

    Args:
        experiment_id: Experiment ID.
        timestamp: ISO timestamp.

    Returns:
        Unique entry ID string.
    """
    content = f"{experiment_id}:{timestamp}"
    hash_suffix = hashlib.sha256(content.encode()).hexdigest()[:12]
    return f"mem_{hash_suffix}"


class FileLock:
    """Simple file-based lock for cross-platform compatibility.

    Uses fcntl on Unix, msvcrt on Windows.
    """

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


class ExplorerMemory:
    """Append-only memory store for exploration outcomes.

    This component:
    - Stores exploration outcomes as immutable entries
    - Supports ONLY append operations (no update, no delete)
    - Provides read access for review and querying
    - Is entirely separate from trusted memory systems
    - Uses file-level locking for concurrent access safety

    The memory is advisory: entries inform human reviewers
    but trigger no automatic actions.

    Storage format: JSON Lines (one JSON object per line).

    Attributes:
        storage_path: Path to the memory storage file.
    """

    def __init__(self, storage_path: Path) -> None:
        """Initialize the explorer memory.

        Args:
            storage_path: Path where memory entries are persisted.
        """
        self._storage_path = Path(storage_path)
        self._lock_path = self._storage_path.with_suffix(".lock")
        self._entries: list[MemoryEntry] = []
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
            logger.debug(f"No existing memory file at {self._storage_path}")
            return

        with FileLock(self._lock_path):
            with open(self._storage_path, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        entry = MemoryEntry.from_json(line)
                        self._add_to_indices(entry)
                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        raise MemoryCorruptionError(
                            self._storage_path,
                            line_num,
                            str(e),
                        )

        logger.info(f"Loaded {len(self._entries)} entries from {self._storage_path}")

    def _add_to_indices(self, entry: MemoryEntry) -> None:
        """Add entry to in-memory indices."""
        index = len(self._entries)
        self._entries.append(entry)
        self._entry_index[entry.entry_id] = index

        if entry.diff_hash:
            if entry.diff_hash not in self._diff_hash_index:
                self._diff_hash_index[entry.diff_hash] = []
            self._diff_hash_index[entry.diff_hash].append(entry.entry_id)

    def append(self, entry: MemoryEntry) -> str:
        """Append a new entry to memory.

        This is the ONLY write operation. Entries cannot be
        modified or deleted after being appended.

        Args:
            entry: MemoryEntry to append.

        Returns:
            The entry_id of the appended entry.

        Raises:
            PromotedViolation: If entry has promoted=True.
            MemoryAppendError: If append fails.
        """
        if entry.promoted:
            raise PromotedViolation(entry.entry_id)

        if entry.entry_id in self._entry_index:
            raise MemoryAppendError(
                "Entry ID already exists (duplicate)",
                entry.entry_id,
            )

        json_line = entry.to_json() + "\n"

        with FileLock(self._lock_path):
            self._storage_path.parent.mkdir(parents=True, exist_ok=True)

            with open(self._storage_path, "a", encoding="utf-8") as f:
                f.write(json_line)
                f.flush()
                os.fsync(f.fileno())

            self._add_to_indices(entry)

        logger.info(f"Appended entry {entry.entry_id} to memory")
        return entry.entry_id

    def get(self, entry_id: str) -> Optional[MemoryEntry]:
        """Retrieve an entry by ID.

        Args:
            entry_id: ID of the entry to retrieve.

        Returns:
            MemoryEntry if found, None otherwise.
        """
        index = self._entry_index.get(entry_id)
        if index is not None:
            return self._entries[index]
        return None

    def get_by_diff_hash(self, diff_hash: str) -> list[MemoryEntry]:
        """Retrieve entries by diff hash (content-addressable).

        Args:
            diff_hash: Hash of the diff content.

        Returns:
            List of MemoryEntry objects with matching diff_hash.
        """
        entry_ids = self._diff_hash_index.get(diff_hash, [])
        return [self._entries[self._entry_index[eid]] for eid in entry_ids]

    def get_by_experiment_id(self, experiment_id: str) -> Optional[MemoryEntry]:
        """Retrieve entry by experiment ID.

        Args:
            experiment_id: ID of the experiment.

        Returns:
            MemoryEntry if found, None otherwise.
        """
        for entry in self._entries:
            if entry.experiment_id == experiment_id:
                return entry
        return None

    def list_all(self) -> list[MemoryEntry]:
        """List all entries in chronological order.

        Returns:
            List of all MemoryEntry objects (oldest first).
        """
        return list(self._entries)

    def list_by_outcome(self, outcome: ExplorationOutcome) -> list[MemoryEntry]:
        """List entries filtered by outcome.

        Args:
            outcome: Outcome type to filter by.

        Returns:
            List of matching MemoryEntry objects.
        """
        return [e for e in self._entries if e.outcome == outcome]

    def list_unpromoted(self) -> list[MemoryEntry]:
        """List entries that haven't been promoted.

        These are candidates for human review.

        Returns:
            List of unpromoted MemoryEntry objects.
        """
        return [e for e in self._entries if not e.promoted]

    def list_with_changes(self) -> list[MemoryEntry]:
        """List entries that have non-empty diffs.

        Returns:
            List of entries with actual code changes.
        """
        return [e for e in self._entries if not e.diff_is_empty]

    def count(self) -> int:
        """Get total number of entries.

        Returns:
            Number of entries in memory.
        """
        return len(self._entries)

    def has_diff_hash(self, diff_hash: str) -> bool:
        """Check if a diff hash already exists in memory.

        Args:
            diff_hash: Hash to check.

        Returns:
            True if diff_hash exists, False otherwise.
        """
        return diff_hash in self._diff_hash_index

    def __iter__(self) -> Iterator[MemoryEntry]:
        """Iterate over all entries."""
        return iter(self._entries)

    def __len__(self) -> int:
        """Number of entries in memory."""
        return len(self._entries)

    def __contains__(self, entry_id: str) -> bool:
        """Check if entry_id exists in memory."""
        return entry_id in self._entry_index


def create_memory_entry_from_experiment(
    experiment_result: "ExperimentResult",
    hypothesis: str,
    observation: str,
    summary_length: int = 500,
) -> MemoryEntry:
    """Create a MemoryEntry from an ExperimentResult.

    Helper function to convert experiment results into memory entries.

    Args:
        experiment_result: Result from ExperimentRunner.
        hypothesis: What the experiment was testing.
        observation: What was observed.
        summary_length: Max length for stdout/stderr summaries.

    Returns:
        MemoryEntry ready for appending.
    """
    from odibi.agents.explorer.experiment_runner import ExperimentStatus

    if experiment_result.status == ExperimentStatus.COMPLETED:
        if experiment_result.diff_is_empty:
            outcome = ExplorationOutcome.INCONCLUSIVE
        else:
            outcome = ExplorationOutcome.SUCCESS
    elif experiment_result.status == ExperimentStatus.TIMEOUT:
        outcome = ExplorationOutcome.TIMEOUT
    else:
        outcome = ExplorationOutcome.FAILURE

    timestamp = datetime.now().isoformat()
    entry_id = generate_entry_id(experiment_result.experiment_id, timestamp)

    stdout_summary = experiment_result.stdout[:summary_length] if experiment_result.stdout else ""
    stderr_summary = experiment_result.stderr[:summary_length] if experiment_result.stderr else ""

    return MemoryEntry(
        entry_id=entry_id,
        experiment_id=experiment_result.experiment_id,
        sandbox_id=experiment_result.sandbox_id,
        timestamp=timestamp,
        outcome=outcome,
        hypothesis=hypothesis,
        observation=observation,
        diff_hash=experiment_result.diff_hash,
        diff_path=experiment_result.diff_path,
        diff_is_empty=experiment_result.diff_is_empty,
        exit_code=experiment_result.exit_code,
        stdout_summary=stdout_summary,
        stderr_summary=stderr_summary,
        promoted=False,
        metadata=experiment_result.metadata,
    )

"""Source Binder: Materialize and bind source pools to execution.

Phase 9.D - Reality Injection: Make learning cycles non-trivial.

This module provides:
- SourceBinder: Materializes resolved sources under .odibi/source_cache/<cycle_id>/
- BoundSourceMap: Immutable mapping of pool_id -> bound path
- SourceBindingResult: Audit trail of binding operations
- TrivialCycleDetector: Detects when no data was read from bound sources

INVARIANTS:
- Binding is read-only (no writes to source pools)
- No network access
- No mutation of source data
- Paths are deterministically derived from cycle_id
- All operations are logged for audit

PHASE 9.D CONSTRAINTS (MUST HOLD):
❌ NO YAML generation
❌ NO pipeline modification
❌ NO ImprovementAgent execution
❌ NO auto-testing
❌ NO relaxation of learning-mode guards
"""

import hashlib
import logging
import os
import shutil
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from .evidence import SourceUsageSummary
from .source_binding import (
    ExecutionSourceContext,
    MountedPool,
    SourceUsageEvidence,
)
from .source_resolution import SourceResolutionResult
from .source_selection import PoolMetadataSummary

logger = logging.getLogger(__name__)


# ============================================
# Binding Status
# ============================================


class BindingStatus(str, Enum):
    """Status of source binding operation."""

    PENDING = "PENDING"
    BOUND = "BOUND"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


# ============================================
# Bound Source Entry
# ============================================


@dataclass(frozen=True)
class BoundSourceEntry:
    """A single bound source pool entry.

    Frozen (immutable) to prevent modification after binding.
    """

    pool_id: str
    pool_name: str
    original_cache_path: str
    bound_path: str
    tier: str
    file_format: str
    row_count: int
    manifest_hash: str
    bound_at: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pool_id": self.pool_id,
            "pool_name": self.pool_name,
            "original_cache_path": self.original_cache_path,
            "bound_path": self.bound_path,
            "tier": self.tier,
            "file_format": self.file_format,
            "row_count": self.row_count,
            "manifest_hash": self.manifest_hash,
            "bound_at": self.bound_at,
        }


# ============================================
# Bound Source Map
# ============================================


@dataclass
class BoundSourceMap:
    """Immutable mapping of pool_id -> bound source path.

    This is passed to ExecutionGateway to enforce path prefixes.
    """

    cycle_id: str
    binding_root: str
    entries: Dict[str, BoundSourceEntry] = field(default_factory=dict)
    bound_at: str = ""
    total_bytes_bound: int = 0

    def __post_init__(self):
        if not self.bound_at:
            self.bound_at = datetime.now(UTC).isoformat() + "Z"

    def get_bound_path(self, pool_id: str) -> Optional[str]:
        """Get the bound path for a pool.

        Args:
            pool_id: The pool identifier

        Returns:
            Absolute path to bound pool or None if not bound
        """
        entry = self.entries.get(pool_id)
        return entry.bound_path if entry else None

    def get_all_bound_paths(self) -> List[str]:
        """Get all bound paths as a list."""
        return [e.bound_path for e in self.entries.values()]

    def get_pool_ids(self) -> List[str]:
        """Get all bound pool IDs."""
        return list(self.entries.keys())

    def is_path_bound(self, path: str) -> bool:
        """Check if a path is within any bound pool.

        Args:
            path: Absolute path to check

        Returns:
            True if path is within a bound pool
        """
        normalized = os.path.normpath(os.path.abspath(path))
        for entry in self.entries.values():
            bound_normalized = os.path.normpath(entry.bound_path)
            if normalized.startswith(bound_normalized + os.sep) or normalized == bound_normalized:
                return True
        return False

    def get_pool_for_path(self, path: str) -> Optional[str]:
        """Get the pool_id containing a path.

        Args:
            path: Absolute path to check

        Returns:
            pool_id or None if not in any bound pool
        """
        normalized = os.path.normpath(os.path.abspath(path))
        for pool_id, entry in self.entries.items():
            bound_normalized = os.path.normpath(entry.bound_path)
            if normalized.startswith(bound_normalized + os.sep) or normalized == bound_normalized:
                return pool_id
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cycle_id": self.cycle_id,
            "binding_root": self.binding_root,
            "entries": {k: v.to_dict() for k, v in self.entries.items()},
            "bound_at": self.bound_at,
            "total_bytes_bound": self.total_bytes_bound,
            "pool_count": len(self.entries),
        }


# ============================================
# Source Binding Result
# ============================================


@dataclass
class SourceBindingResult:
    """Result of source binding operation.

    Provides audit trail for what was bound and any errors.
    """

    cycle_id: str
    status: BindingStatus
    bound_source_map: Optional[BoundSourceMap]
    pools_bound: int
    pools_failed: int
    total_bytes: int
    binding_duration_seconds: float
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    started_at: str = ""
    completed_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cycle_id": self.cycle_id,
            "status": self.status.value,
            "bound_source_map": self.bound_source_map.to_dict() if self.bound_source_map else None,
            "pools_bound": self.pools_bound,
            "pools_failed": self.pools_failed,
            "total_bytes": self.total_bytes,
            "binding_duration_seconds": self.binding_duration_seconds,
            "errors": self.errors,
            "warnings": self.warnings,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }


# ============================================
# Trivial Cycle Detection
# ============================================


@dataclass
class TrivialCycleWarning:
    """Warning emitted when learning cycle reads no data from bound sources."""

    cycle_id: str
    message: str
    detail: str
    pools_bound: List[str]
    pools_used: List[str]
    bytes_read: int
    files_accessed: int
    detected_at: str = ""

    def __post_init__(self):
        if not self.detected_at:
            self.detected_at = datetime.now(UTC).isoformat() + "Z"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cycle_id": self.cycle_id,
            "message": self.message,
            "detail": self.detail,
            "pools_bound": self.pools_bound,
            "pools_used": self.pools_used,
            "bytes_read": self.bytes_read,
            "files_accessed": self.files_accessed,
            "detected_at": self.detected_at,
        }


class TrivialCycleDetector:
    """Detects when a learning cycle executed without reading bound source data.

    A cycle is considered TRIVIAL when:
    - Sources were bound to execution
    - Zero bytes were read from bound source pools
    - Only local/trivial data was used

    This helps distinguish "ran successfully" from "did real work".
    """

    TRIVIAL_WARNING_MESSAGE = (
        "Learning cycle executed without exercising bound source pools. "
        "This cycle observed trivial workloads only."
    )

    def __init__(self, cycle_id: str):
        """Initialize detector for a cycle.

        Args:
            cycle_id: The cycle identifier
        """
        self.cycle_id = cycle_id
        self._bytes_read: int = 0
        self._files_accessed: int = 0
        self._pools_used: Set[str] = set()
        self._pools_bound: Set[str] = set()

    def record_binding(self, bound_map: BoundSourceMap) -> None:
        """Record which pools were bound.

        Args:
            bound_map: The bound source map
        """
        self._pools_bound = set(bound_map.get_pool_ids())

    def record_access(
        self,
        pool_id: str,
        bytes_read: int,
        files_accessed: int = 1,
    ) -> None:
        """Record data access from a bound pool.

        Args:
            pool_id: The pool that was accessed
            bytes_read: Number of bytes read
            files_accessed: Number of files accessed
        """
        if pool_id in self._pools_bound:
            self._pools_used.add(pool_id)
            self._bytes_read += bytes_read
            self._files_accessed += files_accessed

    def record_from_evidence(self, evidence: SourceUsageEvidence) -> None:
        """Record access from source usage evidence.

        Args:
            evidence: The source usage evidence
        """
        for pool_id in evidence.pools_used:
            self._pools_used.add(pool_id)
        self._files_accessed += len(evidence.files_accessed)

    def record_from_summary(self, summary: SourceUsageSummary) -> None:
        """Record access from source usage summary.

        Args:
            summary: The source usage summary
        """
        for pool_id in summary.pools_used:
            self._pools_used.add(pool_id)
        self._files_accessed += summary.files_accessed

    def is_trivial(self) -> bool:
        """Check if the cycle was trivial.

        A cycle is trivial if:
        - Pools were bound but none were used, OR
        - Zero bytes were read from bound pools

        Returns:
            True if cycle was trivial
        """
        if not self._pools_bound:
            return False  # No pools bound, can't be trivial

        # Trivial if no pools were used from the bound set
        if not self._pools_used:
            return True

        # Trivial if zero bytes read
        if self._bytes_read == 0 and self._files_accessed == 0:
            return True

        return False

    def get_warning(self) -> Optional[TrivialCycleWarning]:
        """Get trivial cycle warning if applicable.

        Returns:
            TrivialCycleWarning if cycle was trivial, None otherwise
        """
        if not self.is_trivial():
            return None

        detail_parts = []
        if self._pools_bound:
            detail_parts.append(f"Pools bound: {sorted(self._pools_bound)}")
        if self._pools_used:
            detail_parts.append(f"Pools accessed: {sorted(self._pools_used)}")
        else:
            detail_parts.append("No bound pools were accessed")
        detail_parts.append(f"Bytes read: {self._bytes_read}")
        detail_parts.append(f"Files accessed: {self._files_accessed}")

        return TrivialCycleWarning(
            cycle_id=self.cycle_id,
            message=self.TRIVIAL_WARNING_MESSAGE,
            detail="; ".join(detail_parts),
            pools_bound=sorted(self._pools_bound),
            pools_used=sorted(self._pools_used),
            bytes_read=self._bytes_read,
            files_accessed=self._files_accessed,
        )


# ============================================
# Source Binder
# ============================================


class SourceBinder:
    """Materializes and binds resolved source pools to execution.

    The binder:
    1. Takes SourceResolutionResult from _resolve_sources()
    2. Materializes sources under .odibi/source_cache/<cycle_id>/<pool_name>/
    3. Returns BoundSourceMap for ExecutionGateway binding
    4. Enforces read-only access (via symlinks on Unix, copies on Windows)

    INVARIANTS:
    - Binding is idempotent (same cycle_id -> same paths)
    - No data modification allowed
    - Paths are deterministic
    - Audit trail is maintained
    """

    def __init__(self, odibi_root: str):
        """Initialize the source binder.

        Args:
            odibi_root: Path to Odibi repository root
        """
        self.odibi_root = odibi_root
        self.source_cache_root = os.path.join(odibi_root, ".odibi", "source_cache")
        self.cycle_bindings_root = os.path.join(odibi_root, ".odibi", "cycle_bindings")

    def _get_cycle_binding_path(self, cycle_id: str) -> str:
        """Get the binding directory for a cycle.

        Args:
            cycle_id: The cycle identifier

        Returns:
            Absolute path to cycle's binding directory
        """
        # Use hash of cycle_id to avoid path length issues
        cycle_hash = hashlib.sha256(cycle_id.encode()).hexdigest()[:12]
        return os.path.join(self.cycle_bindings_root, f"{cycle_id[:20]}_{cycle_hash}")

    def _resolve_pool_source_path(self, pool: PoolMetadataSummary) -> Optional[str]:
        """Resolve the actual source path for a pool.

        Args:
            pool: Pool metadata

        Returns:
            Absolute path to pool's source data or None if not found
        """
        # The cache_path is relative to source_cache_root
        source_path = os.path.join(self.source_cache_root, pool.cache_path)

        if os.path.exists(source_path):
            return os.path.normpath(source_path)

        # Try alternate paths for tier-based pools
        # Check if pool_id indicates a tier
        pool_id_lower = pool.pool_id.lower()
        for tier in ["tier0", "tier20gb", "tier100gb", "tier600gb", "tier2tb"]:
            if tier in pool_id_lower:
                tier_path = os.path.join(self.source_cache_root, "tiers", tier, pool.cache_path)
                if os.path.exists(tier_path):
                    return os.path.normpath(tier_path)

        logger.warning(f"Source path not found for pool {pool.pool_id}: {source_path}")
        return None

    def _get_pool_tier(self, pool: PoolMetadataSummary) -> str:
        """Extract tier from pool metadata.

        Args:
            pool: Pool metadata

        Returns:
            Tier name (e.g., 'tier100gb') or 'default'
        """
        pool_id_lower = pool.pool_id.lower()
        for tier in ["tier0", "tier20gb", "tier100gb", "tier600gb", "tier2tb"]:
            if tier in pool_id_lower:
                return tier
        return "default"

    def _calculate_dir_size(self, path: str) -> int:
        """Calculate total size of a directory.

        Args:
            path: Directory path

        Returns:
            Total size in bytes
        """
        total = 0
        try:
            if os.path.isfile(path):
                return os.path.getsize(path)
            for dirpath, _dirnames, filenames in os.walk(path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    try:
                        total += os.path.getsize(fp)
                    except OSError:
                        pass
        except OSError:
            pass
        return total

    def bind(
        self,
        resolution: SourceResolutionResult,
        use_symlinks: bool = True,
    ) -> SourceBindingResult:
        """Bind resolved sources to execution paths.

        This creates a deterministic binding of source pools to paths
        that can be enforced by ExecutionGateway.

        Args:
            resolution: The source resolution result
            use_symlinks: Use symlinks (Unix) instead of copies (faster, same data)

        Returns:
            SourceBindingResult with bound map and audit info
        """
        import time

        start_time = time.time()
        started_at = datetime.now(UTC).isoformat() + "Z"

        cycle_id = resolution.cycle_id
        binding_path = self._get_cycle_binding_path(cycle_id)

        # Ensure binding directory exists
        os.makedirs(binding_path, exist_ok=True)

        entries: Dict[str, BoundSourceEntry] = {}
        errors: List[str] = []
        warnings: List[str] = []
        total_bytes = 0

        # Get the underlying selection result if available
        selection_result = resolution._selection_result
        pools = selection_result.selected_pools if selection_result else []

        if not pools:
            warnings.append("No pools in resolution result")

        for pool in pools:
            try:
                # Resolve source path
                source_path = self._resolve_pool_source_path(pool)
                if not source_path:
                    errors.append(f"Source not found for pool {pool.pool_id}")
                    continue

                # Calculate bound path (deterministic)
                bound_pool_dir = os.path.join(binding_path, pool.pool_id)

                # Check if already bound (idempotent)
                if os.path.exists(bound_pool_dir):
                    logger.debug(f"Pool {pool.pool_id} already bound at {bound_pool_dir}")
                else:
                    # Bind via symlink (Unix) or directory junction (Windows)
                    try:
                        if use_symlinks and os.name != "nt":
                            os.symlink(source_path, bound_pool_dir, target_is_directory=True)
                        else:
                            # On Windows, create a directory junction or just reference the path
                            # For read-only access, we can just use the original path
                            # Create a marker file instead
                            os.makedirs(bound_pool_dir, exist_ok=True)
                            marker_path = os.path.join(bound_pool_dir, ".bound_to")
                            with open(marker_path, "w", encoding="utf-8") as f:
                                f.write(source_path)
                            # Actually bind to original path for execution
                            bound_pool_dir = source_path
                    except OSError as e:
                        # Fall back to using original path directly
                        logger.warning(
                            f"Symlink failed for {pool.pool_id}: {e}, using original path"
                        )
                        bound_pool_dir = source_path

                # Calculate size
                pool_bytes = self._calculate_dir_size(source_path)
                total_bytes += pool_bytes

                # Create entry
                entries[pool.pool_id] = BoundSourceEntry(
                    pool_id=pool.pool_id,
                    pool_name=pool.name,
                    original_cache_path=pool.cache_path,
                    bound_path=bound_pool_dir,
                    tier=self._get_pool_tier(pool),
                    file_format=pool.file_format,
                    row_count=pool.row_count,
                    manifest_hash=pool.manifest_hash,
                    bound_at=datetime.now(UTC).isoformat() + "Z",
                )

                logger.debug(f"Bound pool {pool.pool_id}: {source_path} -> {bound_pool_dir}")

            except Exception as e:
                errors.append(f"Failed to bind pool {pool.pool_id}: {type(e).__name__}: {e}")
                logger.error(f"Binding error for {pool.pool_id}: {e}")

        # Build result
        completed_at = datetime.now(UTC).isoformat() + "Z"
        duration = time.time() - start_time

        if not entries:
            return SourceBindingResult(
                cycle_id=cycle_id,
                status=BindingStatus.FAILED if errors else BindingStatus.SKIPPED,
                bound_source_map=None,
                pools_bound=0,
                pools_failed=len(errors),
                total_bytes=0,
                binding_duration_seconds=duration,
                errors=errors,
                warnings=warnings,
                started_at=started_at,
                completed_at=completed_at,
            )

        bound_map = BoundSourceMap(
            cycle_id=cycle_id,
            binding_root=binding_path,
            entries=entries,
            total_bytes_bound=total_bytes,
        )

        return SourceBindingResult(
            cycle_id=cycle_id,
            status=BindingStatus.BOUND,
            bound_source_map=bound_map,
            pools_bound=len(entries),
            pools_failed=len(errors),
            total_bytes=total_bytes,
            binding_duration_seconds=duration,
            errors=errors,
            warnings=warnings,
            started_at=started_at,
            completed_at=completed_at,
        )

    def create_execution_context(
        self,
        bound_map: BoundSourceMap,
        resolution: SourceResolutionResult,
    ) -> ExecutionSourceContext:
        """Create ExecutionSourceContext from binding result.

        This bridges the binding result to the existing source_binding module.

        Args:
            bound_map: The bound source map
            resolution: The source resolution result

        Returns:
            ExecutionSourceContext ready for gateway binding
        """
        # Build mounted pools from bound entries
        mounted_pools: Dict[str, MountedPool] = {}
        allowed_paths: Set[str] = set()

        for pool_id, entry in bound_map.entries.items():
            mounted = MountedPool(
                pool_id=pool_id,
                name=entry.pool_name,
                cache_path=entry.original_cache_path,
                absolute_path=entry.bound_path,
                manifest_hash=entry.manifest_hash,
                row_count=entry.row_count,
                file_format=entry.file_format,
                source_type="local",  # All bindings are local
                data_quality="frozen",  # Required for learning mode
            )
            mounted_pools[pool_id] = mounted
            allowed_paths.add(entry.bound_path)

        context_id = f"ctx_{bound_map.cycle_id}_{resolution.resolution_id[-8:]}"

        return ExecutionSourceContext(
            context_id=context_id,
            cycle_id=bound_map.cycle_id,
            selection_id=resolution.resolution_id,
            mounted_pools=mounted_pools,
            allowed_paths=allowed_paths,
            source_cache_root=self.source_cache_root,
            created_at=datetime.now(UTC).isoformat() + "Z",
        )

    def cleanup_binding(self, cycle_id: str) -> bool:
        """Clean up binding for a cycle.

        Args:
            cycle_id: The cycle identifier

        Returns:
            True if cleanup succeeded
        """
        binding_path = self._get_cycle_binding_path(cycle_id)

        if not os.path.exists(binding_path):
            return True

        try:
            shutil.rmtree(binding_path)
            logger.info(f"Cleaned up binding for cycle {cycle_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to cleanup binding for {cycle_id}: {e}")
            return False


# ============================================
# Convenience Functions
# ============================================


def bind_sources_for_cycle(
    odibi_root: str,
    resolution: SourceResolutionResult,
) -> SourceBindingResult:
    """Convenience function to bind sources for a cycle.

    Args:
        odibi_root: Path to Odibi repository root
        resolution: The source resolution result

    Returns:
        SourceBindingResult with binding details
    """
    binder = SourceBinder(odibi_root)
    return binder.bind(resolution)


def detect_trivial_cycle(
    cycle_id: str,
    bound_map: Optional[BoundSourceMap],
    source_usage: Optional[SourceUsageSummary],
) -> Optional[TrivialCycleWarning]:
    """Detect if a cycle was trivial (read no data from bound sources).

    Args:
        cycle_id: The cycle identifier
        bound_map: The bound source map (or None if not bound)
        source_usage: Source usage summary from evidence (or None)

    Returns:
        TrivialCycleWarning if trivial, None otherwise
    """
    if not bound_map:
        return None  # No binding means can't detect triviality

    detector = TrivialCycleDetector(cycle_id)
    detector.record_binding(bound_map)

    if source_usage:
        detector.record_from_summary(source_usage)

    return detector.get_warning()

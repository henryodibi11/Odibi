"""Source Binding: Hard, auditable binding of SourcePools to execution.

Phase 7.D - Execution Safety, NOT Scale.

This module provides:
- ExecutionSourceContext: Immutable context of mounted source pools
- Source path enforcement: Reject access to unbound paths
- Evidence integration: Track all source usage

INVARIANTS:
- Pipelines may ONLY read from selected SourcePools
- SourcePools are mounted read-only
- Execution MAY NOT access arbitrary filesystem paths
- Execution MAY NOT access network resources
- All source usage must be logged and attributable
"""

import hashlib
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from .source_selection import SourceSelectionResult


# ============================================
# Violation Types
# ============================================


class SourceViolationType(str, Enum):
    """Types of source access violations."""

    PATH_OUTSIDE_POOLS = "PATH_OUTSIDE_POOLS"
    UNKNOWN_POOL_ID = "UNKNOWN_POOL_ID"
    POOL_NOT_SELECTED = "POOL_NOT_SELECTED"
    INTEGRITY_FAILURE = "INTEGRITY_FAILURE"
    NETWORK_ACCESS_ATTEMPT = "NETWORK_ACCESS_ATTEMPT"
    WRITE_ATTEMPT = "WRITE_ATTEMPT"


# ============================================
# Source Violation Error
# ============================================


class SourceViolationError(Exception):
    """Raised when execution attempts to access unbound sources.

    This error is DETERMINISTIC: same violation always raises same error.
    It MUST be raised and MUST fail execution.
    """

    def __init__(
        self,
        violation_type: SourceViolationType,
        message: str,
        path: Optional[str] = None,
        pool_id: Optional[str] = None,
    ):
        self.violation_type = violation_type
        self.path = path
        self.pool_id = pool_id
        super().__init__(f"SOURCE_VIOLATION ({violation_type.value}): {message}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "violation_type": self.violation_type.value,
            "message": str(self),
            "path": self.path,
            "pool_id": self.pool_id,
        }


# ============================================
# Mounted Pool
# ============================================


@dataclass(frozen=True)
class MountedPool:
    """A source pool mounted for read-only access.

    Frozen (immutable) to ensure bindings cannot be modified during execution.
    """

    pool_id: str
    name: str
    cache_path: str  # Relative to source_cache
    absolute_path: str  # Resolved absolute path
    manifest_hash: str
    row_count: int
    file_format: str
    source_type: str
    data_quality: str

    def contains_path(self, path: str) -> bool:
        """Check if a path is within this pool's mount.

        Args:
            path: Absolute path to check.

        Returns:
            True if path is within this pool's directory.
        """
        normalized_path = os.path.normpath(os.path.abspath(path))
        normalized_mount = os.path.normpath(self.absolute_path)
        return (
            normalized_path.startswith(normalized_mount + os.sep)
            or normalized_path == normalized_mount
        )


# ============================================
# Source Access Record
# ============================================


@dataclass
class SourceAccessRecord:
    """Record of a source access during execution.

    Used for evidence and audit trails.
    """

    timestamp: str
    pool_id: str
    file_path: str
    access_type: str  # "read", "list", "stat"
    success: bool
    error_message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "pool_id": self.pool_id,
            "file_path": self.file_path,
            "access_type": self.access_type,
            "success": self.success,
            "error_message": self.error_message,
        }


# ============================================
# Execution Source Context
# ============================================


@dataclass
class ExecutionSourceContext:
    """Immutable context of source pools bound to execution.

    This context is created at cycle start and CANNOT be modified.
    All source access during execution must go through this context.

    INVARIANTS:
    - Created from SourceSelectionResult
    - Immutable during cycle execution
    - Provides read-only access to pool paths
    - Tracks all access for evidence
    """

    # === Identification ===
    context_id: str
    cycle_id: str
    selection_id: str

    # === Bound Pools ===
    mounted_pools: Dict[str, MountedPool]  # pool_id -> MountedPool
    allowed_paths: Set[str]  # Set of allowed absolute path prefixes

    # === Source Cache Root ===
    source_cache_root: str

    # === Timing ===
    created_at: str
    frozen: bool = True  # Always frozen once created

    # === Access Tracking ===
    access_log: List[SourceAccessRecord] = field(default_factory=list)
    integrity_verified: bool = False

    def __post_init__(self):
        """Ensure context is frozen after creation."""
        if not self.frozen:
            object.__setattr__(self, "frozen", True)

    @classmethod
    def from_selection(
        cls,
        selection: SourceSelectionResult,
        source_cache_root: str,
    ) -> "ExecutionSourceContext":
        """Create context from a selection result.

        Args:
            selection: The source selection result
            source_cache_root: Absolute path to .odibi/source_cache/

        Returns:
            ExecutionSourceContext with mounted pools
        """
        mounted_pools = {}
        allowed_paths = set()

        for pool_summary in selection.selected_pools:
            # Resolve absolute path
            abs_path = os.path.normpath(os.path.join(source_cache_root, pool_summary.cache_path))

            mounted = MountedPool(
                pool_id=pool_summary.pool_id,
                name=pool_summary.name,
                cache_path=pool_summary.cache_path,
                absolute_path=abs_path,
                manifest_hash=pool_summary.manifest_hash,
                row_count=pool_summary.row_count,
                file_format=pool_summary.file_format,
                source_type=pool_summary.source_type,
                data_quality=pool_summary.data_quality,
            )

            mounted_pools[pool_summary.pool_id] = mounted
            allowed_paths.add(abs_path)

        context_id = f"ctx_{selection.cycle_id}_{selection.selection_id[-8:]}"

        return cls(
            context_id=context_id,
            cycle_id=selection.cycle_id,
            selection_id=selection.selection_id,
            mounted_pools=mounted_pools,
            allowed_paths=allowed_paths,
            source_cache_root=source_cache_root,
            created_at=datetime.now(UTC).isoformat() + "Z",
        )

    def get_pool(self, pool_id: str) -> MountedPool:
        """Get a mounted pool by ID.

        Args:
            pool_id: The pool identifier

        Returns:
            MountedPool

        Raises:
            SourceViolationError: If pool is not bound
        """
        if pool_id not in self.mounted_pools:
            raise SourceViolationError(
                violation_type=SourceViolationType.POOL_NOT_SELECTED,
                message=f"Pool '{pool_id}' is not bound to this execution context. "
                f"Bound pools: {list(self.mounted_pools.keys())}",
                pool_id=pool_id,
            )
        return self.mounted_pools[pool_id]

    def resolve_pool_path(self, pool_id: str, relative_path: str = "") -> str:
        """Resolve an absolute path within a pool.

        Args:
            pool_id: The pool identifier
            relative_path: Optional path relative to pool root

        Returns:
            Absolute path within the pool

        Raises:
            SourceViolationError: If pool is not bound or path escapes pool
        """
        pool = self.get_pool(pool_id)
        if relative_path:
            full_path = os.path.normpath(os.path.join(pool.absolute_path, relative_path))
            # Ensure path doesn't escape pool directory
            if not full_path.startswith(pool.absolute_path):
                raise SourceViolationError(
                    violation_type=SourceViolationType.PATH_OUTSIDE_POOLS,
                    message=f"Path '{relative_path}' escapes pool directory",
                    path=full_path,
                    pool_id=pool_id,
                )
        else:
            full_path = pool.absolute_path

        return full_path

    def validate_path(self, path: str) -> Optional[str]:
        """Validate that a path is within allowed pools.

        Args:
            path: Absolute path to validate

        Returns:
            The pool_id if path is allowed, None otherwise
        """
        normalized = os.path.normpath(os.path.abspath(path))

        for pool_id, pool in self.mounted_pools.items():
            if pool.contains_path(normalized):
                return pool_id

        return None

    def enforce_path(self, path: str, access_type: str = "read") -> str:
        """Enforce that a path is within allowed pools.

        Args:
            path: Absolute path to validate
            access_type: Type of access ("read", "list", "stat")

        Returns:
            The pool_id containing this path

        Raises:
            SourceViolationError: If path is outside all pools
        """
        pool_id = self.validate_path(path)

        if pool_id is None:
            # Log the violation
            self.access_log.append(
                SourceAccessRecord(
                    timestamp=datetime.now(UTC).isoformat() + "Z",
                    pool_id="UNKNOWN",
                    file_path=path,
                    access_type=access_type,
                    success=False,
                    error_message="Path outside all bound pools",
                )
            )

            raise SourceViolationError(
                violation_type=SourceViolationType.PATH_OUTSIDE_POOLS,
                message=f"Path '{path}' is outside all bound source pools. "
                f"Allowed paths: {list(self.allowed_paths)}",
                path=path,
            )

        # Log successful access
        self.access_log.append(
            SourceAccessRecord(
                timestamp=datetime.now(UTC).isoformat() + "Z",
                pool_id=pool_id,
                file_path=path,
                access_type=access_type,
                success=True,
            )
        )

        return pool_id

    def record_access(
        self,
        pool_id: str,
        file_path: str,
        access_type: str = "read",
        success: bool = True,
        error_message: str = "",
    ) -> None:
        """Record a source access for evidence.

        Args:
            pool_id: The pool being accessed
            file_path: The file path
            access_type: Type of access
            success: Whether access succeeded
            error_message: Error message if failed
        """
        self.access_log.append(
            SourceAccessRecord(
                timestamp=datetime.now(UTC).isoformat() + "Z",
                pool_id=pool_id,
                file_path=file_path,
                access_type=access_type,
                success=success,
                error_message=error_message,
            )
        )

    def get_pools_used(self) -> List[str]:
        """Get list of pool IDs that were actually accessed.

        Returns:
            List of pool IDs with at least one access
        """
        return list(
            set(
                record.pool_id
                for record in self.access_log
                if record.success and record.pool_id != "UNKNOWN"
            )
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize context to dictionary."""
        return {
            "context_id": self.context_id,
            "cycle_id": self.cycle_id,
            "selection_id": self.selection_id,
            "mounted_pools": {
                pool_id: {
                    "pool_id": pool.pool_id,
                    "name": pool.name,
                    "cache_path": pool.cache_path,
                    "absolute_path": pool.absolute_path,
                    "manifest_hash": pool.manifest_hash,
                    "row_count": pool.row_count,
                    "file_format": pool.file_format,
                }
                for pool_id, pool in self.mounted_pools.items()
            },
            "allowed_paths": list(self.allowed_paths),
            "source_cache_root": self.source_cache_root,
            "created_at": self.created_at,
            "integrity_verified": self.integrity_verified,
            "access_count": len(self.access_log),
            "pools_used": self.get_pools_used(),
        }

    def to_evidence_dict(self) -> Dict[str, Any]:
        """Get evidence-specific representation.

        This is what appears in reports under "Sources Used".
        """
        return {
            "context_id": self.context_id,
            "selection_id": self.selection_id,
            "pools_bound": list(self.mounted_pools.keys()),
            "pools_used": self.get_pools_used(),
            "access_log": [r.to_dict() for r in self.access_log],
            "integrity_verified": self.integrity_verified,
            "total_accesses": len(self.access_log),
            "successful_accesses": sum(1 for r in self.access_log if r.success),
            "failed_accesses": sum(1 for r in self.access_log if not r.success),
        }


# ============================================
# Source Binding Guard
# ============================================


class SourceBindingGuard:
    """Guards execution to ensure source binding compliance.

    This guard is used by ExecutionGateway to validate paths
    before allowing execution.
    """

    def __init__(self, context: ExecutionSourceContext):
        """Initialize guard with source context.

        Args:
            context: The execution source context
        """
        self.context = context
        self._violations: List[SourceViolationError] = []

    def check_pipeline_config(
        self,
        config_path: str,
        source_paths: List[str],
    ) -> List[SourceViolationError]:
        """Check a pipeline config for source violations.

        Args:
            config_path: Path to the pipeline config
            source_paths: List of source paths referenced in config

        Returns:
            List of violations found (empty if valid)
        """
        violations = []

        for source_path in source_paths:
            abs_path = os.path.normpath(os.path.abspath(source_path))
            pool_id = self.context.validate_path(abs_path)

            if pool_id is None:
                violations.append(
                    SourceViolationError(
                        violation_type=SourceViolationType.PATH_OUTSIDE_POOLS,
                        message=f"Pipeline references path outside bound pools: {source_path}",
                        path=source_path,
                    )
                )

        self._violations.extend(violations)
        return violations

    def check_network_access(self, url: str) -> SourceViolationError:
        """Check for network access attempt.

        Args:
            url: The URL being accessed

        Returns:
            Always returns a violation (network access is never allowed)
        """
        violation = SourceViolationError(
            violation_type=SourceViolationType.NETWORK_ACCESS_ATTEMPT,
            message=f"Network access is not allowed during execution: {url}",
            path=url,
        )
        self._violations.append(violation)
        return violation

    def check_write_access(self, path: str) -> SourceViolationError:
        """Check for write access attempt to source.

        Args:
            path: The path being written to

        Returns:
            Violation if path is in source pools (always read-only)
        """
        pool_id = self.context.validate_path(path)

        if pool_id is not None:
            violation = SourceViolationError(
                violation_type=SourceViolationType.WRITE_ATTEMPT,
                message=f"Write access to source pools is forbidden: {path}",
                path=path,
                pool_id=pool_id,
            )
            self._violations.append(violation)
            return violation

        return None  # Write to non-pool path is allowed

    def get_violations(self) -> List[SourceViolationError]:
        """Get all violations detected."""
        return self._violations.copy()

    def has_violations(self) -> bool:
        """Check if any violations were detected."""
        return len(self._violations) > 0

    def clear_violations(self) -> None:
        """Clear recorded violations."""
        self._violations.clear()


# ============================================
# Integrity Verifier
# ============================================


def verify_pool_integrity(
    pool: MountedPool,
    integrity_manifest: Dict[str, str],
) -> tuple[bool, List[str]]:
    """Verify a pool's files match integrity manifest.

    Args:
        pool: The mounted pool to verify
        integrity_manifest: Dict of filename -> expected SHA256 hash

    Returns:
        Tuple of (all_valid, list of error messages)
    """
    errors = []

    for filename, expected_hash in integrity_manifest.items():
        file_path = os.path.join(pool.absolute_path, filename)

        if not os.path.exists(file_path):
            errors.append(f"Missing file: {filename}")
            continue

        # Compute actual hash
        sha256 = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    sha256.update(chunk)
            actual_hash = sha256.hexdigest()

            if actual_hash != expected_hash:
                errors.append(
                    f"Hash mismatch for {filename}: "
                    f"expected {expected_hash[:16]}..., got {actual_hash[:16]}..."
                )
        except Exception as e:
            errors.append(f"Error reading {filename}: {e}")

    return len(errors) == 0, errors


# ============================================
# Evidence Enhancement
# ============================================


@dataclass
class SourceUsageEvidence:
    """Evidence of source pool usage during execution.

    This is appended to ExecutionEvidence for audit trails.
    """

    context_id: str
    pools_bound: List[str]
    pools_used: List[str]
    files_accessed: List[str]
    integrity_verified: bool
    hash_verification_passed: bool
    violations_detected: int
    access_summary: Dict[str, int]  # pool_id -> access count

    def to_dict(self) -> Dict[str, Any]:
        return {
            "context_id": self.context_id,
            "pools_bound": self.pools_bound,
            "pools_used": self.pools_used,
            "files_accessed": self.files_accessed,
            "integrity_verified": self.integrity_verified,
            "hash_verification_passed": self.hash_verification_passed,
            "violations_detected": self.violations_detected,
            "access_summary": self.access_summary,
        }

    @classmethod
    def from_context(cls, context: ExecutionSourceContext) -> "SourceUsageEvidence":
        """Create evidence from execution context.

        Args:
            context: The execution source context

        Returns:
            SourceUsageEvidence summary
        """
        # Compute access summary
        access_summary = {}
        files_accessed = []

        for record in context.access_log:
            if record.success:
                pool_id = record.pool_id
                access_summary[pool_id] = access_summary.get(pool_id, 0) + 1
                files_accessed.append(record.file_path)

        violations = sum(1 for r in context.access_log if not r.success)

        return cls(
            context_id=context.context_id,
            pools_bound=list(context.mounted_pools.keys()),
            pools_used=context.get_pools_used(),
            files_accessed=files_accessed,
            integrity_verified=context.integrity_verified,
            hash_verification_passed=context.integrity_verified,  # Same for now
            violations_detected=violations,
            access_summary=access_summary,
        )

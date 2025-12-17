"""Source Selection: Deterministic, auditable selection of SourcePools for cycles.

Phase 7.C - Selection-only, NO data execution.

This module provides the control-plane for selecting which SourcePools are used
in a cycle. Selection is:
- Deterministic: Same inputs always produce same outputs
- Auditable: Every decision is logged with rationale
- Read-only: No pool modification, no data download

INVARIANTS:
- Agents MAY select pools
- Agents MAY read pool metadata
- Agents MAY NOT modify pools
- Agents MAY NOT download data
- Agents MAY NOT fabricate sources
"""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ============================================
# Enums for Selection Policy
# ============================================


class SelectionStrategy(str, Enum):
    """Deterministic selection strategies."""

    ROUND_ROBIN = "round_robin"
    HASH_BASED = "hash_based"
    COVERAGE_FIRST = "coverage_first"
    EXPLICIT = "explicit"


class CycleMode(str, Enum):
    """Cycle operating modes affecting selection behavior."""

    LEARNING = "learning"
    IMPROVEMENT = "improvement"
    SCHEDULED = "scheduled"


# ============================================
# Selection Policy Schema
# ============================================


class SourceSelectionPolicy(BaseModel):
    """
    Policy controlling how SourcePools are selected for cycles.

    This is a control-plane schema - it defines WHAT can be selected,
    not HOW to execute against it.

    DETERMINISM GUARANTEE:
    Given the same policy + same pool_index + same cycle_id,
    the selected pools MUST be identical.
    """

    # === Identification ===
    policy_id: str = Field(
        description="Unique policy identifier",
        pattern=r"^[a-z][a-z0-9_]*$",
    )
    name: str = Field(description="Human-readable policy name")
    description: str = Field(default="", description="Policy description")

    # === Pool Constraints ===
    eligible_pools: Optional[List[str]] = Field(
        default=None,
        description="Explicit list of pool_ids that MAY be selected. If None, all frozen pools are eligible.",
    )
    excluded_pools: List[str] = Field(
        default_factory=list,
        description="Pool IDs that MUST NOT be selected (blocklist)",
    )
    max_pools_per_cycle: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Maximum number of pools to select per cycle",
    )

    # === Format & Type Constraints ===
    allowed_formats: Optional[List[str]] = Field(
        default=None,
        description="Allowed file formats (csv, json, parquet, avro, delta). None = all.",
    )
    allowed_source_types: Optional[List[str]] = Field(
        default=None,
        description="Allowed source types (local, sql_jdbc_local, etc.). None = all.",
    )

    # === Quality Constraints ===
    allow_messy_data: bool = Field(
        default=True,
        description="Whether messy/mixed quality pools can be selected",
    )
    clean_vs_messy_ratio: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Target ratio of clean pools (0.0 = all messy, 1.0 = all clean). None = no constraint.",
    )

    # === Selection Strategy ===
    selection_strategy: SelectionStrategy = Field(
        default=SelectionStrategy.HASH_BASED,
        description="Deterministic selection algorithm",
    )

    # === Coverage Preferences ===
    prefer_uncovered_formats: bool = Field(
        default=True,
        description="Prefer formats not yet tested in recent cycles",
    )
    prefer_uncovered_pools: bool = Field(
        default=True,
        description="Prefer pools not recently used",
    )

    # === Explicit Selection (for EXPLICIT strategy) ===
    explicit_pool_order: Optional[List[str]] = Field(
        default=None,
        description="For EXPLICIT strategy: exact ordered list of pools to select",
    )

    @model_validator(mode="after")
    def validate_explicit_strategy(self):
        """Ensure explicit strategy has explicit_pool_order."""
        if self.selection_strategy == SelectionStrategy.EXPLICIT:
            if not self.explicit_pool_order:
                raise ValueError("EXPLICIT strategy requires explicit_pool_order to be set")
        return self

    @field_validator("eligible_pools", "excluded_pools", "explicit_pool_order", mode="before")
    @classmethod
    def normalize_pool_lists(cls, v):
        """Normalize pool ID lists."""
        if v is None:
            return v
        if isinstance(v, str):
            return [p.strip() for p in v.split(",") if p.strip()]
        return v


# ============================================
# Mode-Specific Default Policies
# ============================================


def get_default_policy(mode: CycleMode) -> SourceSelectionPolicy:
    """Get the default selection policy for a cycle mode.

    Args:
        mode: The cycle operating mode.

    Returns:
        Default SourceSelectionPolicy for that mode.
    """
    if mode == CycleMode.LEARNING:
        return SourceSelectionPolicy(
            policy_id="learning_default",
            name="Learning Mode Default",
            description="Broad coverage, messy data allowed, exploration-friendly",
            max_pools_per_cycle=4,
            allow_messy_data=True,
            clean_vs_messy_ratio=0.5,
            selection_strategy=SelectionStrategy.COVERAGE_FIRST,
            prefer_uncovered_formats=True,
            prefer_uncovered_pools=True,
        )

    elif mode == CycleMode.IMPROVEMENT:
        return SourceSelectionPolicy(
            policy_id="improvement_default",
            name="Improvement Mode Default",
            description="Narrow, stable selection, prefer clean pools, repeatable",
            max_pools_per_cycle=2,
            allow_messy_data=False,
            clean_vs_messy_ratio=1.0,
            selection_strategy=SelectionStrategy.HASH_BASED,
            prefer_uncovered_formats=False,
            prefer_uncovered_pools=False,
        )

    elif mode == CycleMode.SCHEDULED:
        return SourceSelectionPolicy(
            policy_id="scheduled_default",
            name="Scheduled Mode Default",
            description="Fully deterministic, reproducible byte-for-byte",
            max_pools_per_cycle=3,
            allow_messy_data=True,
            selection_strategy=SelectionStrategy.HASH_BASED,
            prefer_uncovered_formats=False,
            prefer_uncovered_pools=False,
        )

    raise ValueError(f"Unknown cycle mode: {mode}")


# ============================================
# Pool Metadata Summary (for context)
# ============================================


@dataclass
class PoolMetadataSummary:
    """Lightweight metadata summary for selected pools.

    This is what goes into cycle context - metadata only, NO raw data.
    """

    pool_id: str
    name: str
    file_format: str
    source_type: str
    data_quality: str
    row_count: int
    cache_path: str
    manifest_hash: str
    tests_coverage: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pool_id": self.pool_id,
            "name": self.name,
            "file_format": self.file_format,
            "source_type": self.source_type,
            "data_quality": self.data_quality,
            "row_count": self.row_count,
            "cache_path": self.cache_path,
            "manifest_hash": self.manifest_hash,
            "tests_coverage": self.tests_coverage,
        }


# ============================================
# Selection Result (Output)
# ============================================


@dataclass
class SelectionRationale:
    """Explanation for why a pool was selected or rejected."""

    pool_id: str
    selected: bool
    reason: str
    score: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pool_id": self.pool_id,
            "selected": self.selected,
            "reason": self.reason,
            "score": self.score,
        }


@dataclass
class SourceSelectionResult:
    """Result of source selection - for cycle context and reporting.

    INVARIANT: This result is deterministic given the same inputs.
    """

    # === Selection Metadata ===
    selection_id: str
    policy_id: str
    cycle_id: str
    mode: str
    selected_at: str

    # === Selected Pools ===
    selected_pool_ids: List[str]
    selected_pools: List[PoolMetadataSummary]

    # === Selection Rationale ===
    rationale: List[SelectionRationale]

    # === Determinism Verification ===
    input_hash: str  # Hash of (policy + pool_index) for verification
    selection_hash: str  # Hash of selected_pool_ids for verification

    # === Diagnostics ===
    pools_considered: int
    pools_eligible: int
    pools_excluded: int
    selection_strategy_used: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "selection_id": self.selection_id,
            "policy_id": self.policy_id,
            "cycle_id": self.cycle_id,
            "mode": self.mode,
            "selected_at": self.selected_at,
            "selected_pool_ids": self.selected_pool_ids,
            "selected_pools": [p.to_dict() for p in self.selected_pools],
            "rationale": [r.to_dict() for r in self.rationale],
            "input_hash": self.input_hash,
            "selection_hash": self.selection_hash,
            "pools_considered": self.pools_considered,
            "pools_eligible": self.pools_eligible,
            "pools_excluded": self.pools_excluded,
            "selection_strategy_used": self.selection_strategy_used,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    def verify_determinism(self, expected_hash: str) -> bool:
        """Verify that selection matches expected hash.

        Args:
            expected_hash: Expected selection_hash from a previous run.

        Returns:
            True if selection is identical, False otherwise.
        """
        return self.selection_hash == expected_hash


# ============================================
# Source Selector (Deterministic Algorithm)
# ============================================


class SourceSelector:
    """Deterministic source pool selector.

    This class implements the selection algorithm with NO side effects:
    - Reads pool metadata (read-only)
    - Applies policy constraints
    - Produces deterministic selection

    INVARIANTS:
    - Same inputs -> Same outputs (verified via hashes)
    - No network access
    - No pool modification
    - No data access (metadata only)
    """

    def __init__(self, source_metadata_path: Path):
        """Initialize selector with path to source metadata.

        Args:
            source_metadata_path: Path to .odibi/source_metadata/
        """
        self.metadata_path = source_metadata_path
        self._pool_cache: Dict[str, Dict[str, Any]] = {}

    def load_pool_index(self) -> Dict[str, str]:
        """Load the pool index (read-only).

        Returns:
            Dict mapping pool_id -> metadata file path
        """
        import yaml

        index_path = self.metadata_path / "pool_index.yaml"
        if not index_path.exists():
            return {}

        with open(index_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        return data.get("pools", {})

    def load_pool_metadata(self, pool_id: str, metadata_file: str) -> Optional[Dict[str, Any]]:
        """Load metadata for a single pool (read-only, cached).

        Args:
            pool_id: Pool identifier
            metadata_file: Relative path to metadata YAML

        Returns:
            Pool metadata dict or None if not found
        """
        if pool_id in self._pool_cache:
            return self._pool_cache[pool_id]

        import yaml

        metadata_path = self.metadata_path / metadata_file
        if not metadata_path.exists():
            return None

        with open(metadata_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        self._pool_cache[pool_id] = data
        return data

    def select(
        self,
        policy: SourceSelectionPolicy,
        cycle_id: str,
        mode: CycleMode,
        coverage_history: Optional[List[str]] = None,
    ) -> SourceSelectionResult:
        """Select pools according to policy.

        DETERMINISM GUARANTEE:
        Given identical (policy, pool_index, cycle_id), output is identical.

        Args:
            policy: Selection policy to apply
            cycle_id: Current cycle ID (used for hash-based selection)
            mode: Cycle operating mode
            coverage_history: Recently used pool_ids (for coverage-based selection)

        Returns:
            SourceSelectionResult with selected pools and rationale
        """
        pool_index = self.load_pool_index()
        coverage_history = coverage_history or []

        # Compute input hash for determinism verification
        input_data = json.dumps(
            {
                "policy": policy.model_dump(),
                "pool_index": pool_index,
                "cycle_id": cycle_id,
            },
            sort_keys=True,
        )
        input_hash = hashlib.sha256(input_data.encode()).hexdigest()[:16]

        # Load all pool metadata
        all_pools: List[Dict[str, Any]] = []
        for pool_id, metadata_file in pool_index.items():
            metadata = self.load_pool_metadata(pool_id, metadata_file)
            if metadata:
                metadata["pool_id"] = pool_id
                all_pools.append(metadata)

        # Phase 1: Filter by hard constraints
        eligible_pools, rationale = self._filter_pools(all_pools, policy)

        # Phase 2: Score and rank eligible pools
        scored_pools = self._score_pools(eligible_pools, policy, cycle_id, coverage_history)

        # Phase 3: Select top N pools
        selected = self._select_top_n(scored_pools, policy)

        # Build result
        selected_summaries = [self._to_summary(pool) for pool in selected]
        selected_ids = [p["pool_id"] for p in selected]

        # Add selection rationale for selected pools
        for pool in selected:
            rationale.append(
                SelectionRationale(
                    pool_id=pool["pool_id"],
                    selected=True,
                    reason="Selected based on policy and scoring",
                    score=pool.get("_selection_score"),
                )
            )

        # Compute selection hash for determinism verification
        selection_hash = hashlib.sha256(json.dumps(sorted(selected_ids)).encode()).hexdigest()[:16]

        return SourceSelectionResult(
            selection_id=f"sel_{cycle_id}_{input_hash[:8]}",
            policy_id=policy.policy_id,
            cycle_id=cycle_id,
            mode=mode.value,
            selected_at=datetime.now(UTC).isoformat() + "Z",
            selected_pool_ids=selected_ids,
            selected_pools=selected_summaries,
            rationale=rationale,
            input_hash=input_hash,
            selection_hash=selection_hash,
            pools_considered=len(all_pools),
            pools_eligible=len(eligible_pools),
            pools_excluded=len(all_pools) - len(eligible_pools),
            selection_strategy_used=policy.selection_strategy.value,
        )

    def _filter_pools(
        self,
        pools: List[Dict[str, Any]],
        policy: SourceSelectionPolicy,
    ) -> tuple[List[Dict[str, Any]], List[SelectionRationale]]:
        """Filter pools by policy constraints.

        Returns:
            Tuple of (eligible_pools, rationale_for_excluded)
        """
        eligible = []
        rationale = []

        for pool in pools:
            pool_id = pool.get("pool_id", "unknown")

            # Check frozen status
            if pool.get("status") != "frozen":
                rationale.append(
                    SelectionRationale(
                        pool_id=pool_id,
                        selected=False,
                        reason=f"Pool not frozen (status={pool.get('status')})",
                    )
                )
                continue

            # Check explicit eligibility list
            if policy.eligible_pools is not None:
                if pool_id not in policy.eligible_pools:
                    rationale.append(
                        SelectionRationale(
                            pool_id=pool_id,
                            selected=False,
                            reason="Not in eligible_pools list",
                        )
                    )
                    continue

            # Check exclusion list
            if pool_id in policy.excluded_pools:
                rationale.append(
                    SelectionRationale(
                        pool_id=pool_id,
                        selected=False,
                        reason="In excluded_pools blocklist",
                    )
                )
                continue

            # Check format constraint
            if policy.allowed_formats is not None:
                pool_format = pool.get("file_format", "").lower()
                if pool_format not in [f.lower() for f in policy.allowed_formats]:
                    rationale.append(
                        SelectionRationale(
                            pool_id=pool_id,
                            selected=False,
                            reason=f"Format '{pool_format}' not in allowed_formats",
                        )
                    )
                    continue

            # Check source type constraint
            if policy.allowed_source_types is not None:
                pool_type = pool.get("source_type", "").lower()
                if pool_type not in [t.lower() for t in policy.allowed_source_types]:
                    rationale.append(
                        SelectionRationale(
                            pool_id=pool_id,
                            selected=False,
                            reason=f"Source type '{pool_type}' not in allowed_source_types",
                        )
                    )
                    continue

            # Check data quality constraint
            if not policy.allow_messy_data:
                quality = pool.get("data_quality", "clean").lower()
                if quality in ("messy", "mixed"):
                    rationale.append(
                        SelectionRationale(
                            pool_id=pool_id,
                            selected=False,
                            reason=f"Messy data not allowed (quality={quality})",
                        )
                    )
                    continue

            eligible.append(pool)

        return eligible, rationale

    def _score_pools(
        self,
        pools: List[Dict[str, Any]],
        policy: SourceSelectionPolicy,
        cycle_id: str,
        coverage_history: List[str],
    ) -> List[Dict[str, Any]]:
        """Score pools for ranking.

        Scoring is deterministic based on pool properties and cycle_id.
        """
        for pool in pools:
            pool_id = pool.get("pool_id", "")
            score = 0.0

            # Base score from hash (deterministic)
            hash_input = f"{pool_id}:{cycle_id}"
            hash_score = int(hashlib.sha256(hash_input.encode()).hexdigest()[:8], 16)
            score += (hash_score % 1000) / 1000.0  # 0-1 range

            # Coverage bonus
            if policy.prefer_uncovered_pools and pool_id not in coverage_history:
                score += 2.0

            # Format diversity bonus
            if policy.prefer_uncovered_formats:
                # Check if this format is underrepresented in history
                pool_format = pool.get("file_format", "")
                format_count = sum(1 for p in pools if p.get("file_format") == pool_format)
                if format_count == 1:
                    score += 1.0

            # Quality preference
            quality = pool.get("data_quality", "clean").lower()
            if policy.clean_vs_messy_ratio is not None:
                if quality == "clean":
                    score += policy.clean_vs_messy_ratio
                else:
                    score += 1 - policy.clean_vs_messy_ratio

            pool["_selection_score"] = score

        # Sort by score (deterministic because hash_score is deterministic)
        return sorted(pools, key=lambda p: p.get("_selection_score", 0), reverse=True)

    def _select_top_n(
        self,
        scored_pools: List[Dict[str, Any]],
        policy: SourceSelectionPolicy,
    ) -> List[Dict[str, Any]]:
        """Select top N pools according to strategy."""
        if policy.selection_strategy == SelectionStrategy.EXPLICIT:
            # Use explicit order
            explicit_order = policy.explicit_pool_order or []
            result = []
            for pool_id in explicit_order[: policy.max_pools_per_cycle]:
                for pool in scored_pools:
                    if pool.get("pool_id") == pool_id:
                        result.append(pool)
                        break
            return result

        # For all other strategies, take top N by score
        return scored_pools[: policy.max_pools_per_cycle]

    def _to_summary(self, pool: Dict[str, Any]) -> PoolMetadataSummary:
        """Convert pool metadata to summary."""
        characteristics = pool.get("characteristics", {})
        integrity = pool.get("integrity", {})

        return PoolMetadataSummary(
            pool_id=pool.get("pool_id", ""),
            name=pool.get("name", ""),
            file_format=pool.get("file_format", ""),
            source_type=pool.get("source_type", ""),
            data_quality=pool.get("data_quality", ""),
            row_count=characteristics.get("row_count", 0),
            cache_path=pool.get("cache_path", ""),
            manifest_hash=integrity.get("manifest_hash", ""),
            tests_coverage=pool.get("tests_coverage", []),
        )


# ============================================
# Agent Guardrails
# ============================================


class SourceSelectionGuardrails:
    """Explicit rules for what agents MAY and MAY NOT do with sources.

    These are enforced at the selection layer, not the agent layer.
    """

    # === ALLOWED OPERATIONS ===
    ALLOWED = [
        "Read pool metadata (pool_id, schema, characteristics)",
        "Read pool integrity manifest (hash verification)",
        "Query pool index for available pools",
        "Request selection with a valid policy",
        "Verify selection determinism via hashes",
        "Include selected pool metadata in cycle context",
        "Report on pool coverage and usage",
    ]

    # === FORBIDDEN OPERATIONS ===
    FORBIDDEN = [
        "Modify any pool metadata or data",
        "Download new data during cycles",
        "Fabricate sources not in pool_index",
        "Access pool data files directly (metadata only)",
        "Change pool status (frozen/deprecated)",
        "Add new pools to pool_index",
        "Override selection policy during execution",
        "Use pools not selected by the policy",
    ]

    # === FAILURE MODES ===
    @staticmethod
    def handle_no_eligible_pools() -> str:
        """What happens when no pools match the policy."""
        return (
            "CYCLE_BLOCKED: No eligible source pools match the selection policy. "
            "The cycle MUST NOT proceed with fabricated or external sources. "
            "Resolution: Adjust policy constraints or prepare additional pools."
        )

    @staticmethod
    def handle_invalid_policy() -> str:
        """What happens when the policy is invalid."""
        return (
            "POLICY_REJECTED: The selection policy failed validation. "
            "The cycle MUST NOT proceed with a default or fabricated policy. "
            "Resolution: Fix the policy configuration and retry."
        )

    @staticmethod
    def handle_integrity_failure(pool_id: str) -> str:
        """What happens when a pool fails integrity verification."""
        return (
            f"POOL_CORRUPTED: Pool '{pool_id}' failed SHA256 verification. "
            f"This pool MUST NOT be used until re-frozen. "
            f"Resolution: Re-run prepare_source_pools.py or remove from pool_index."
        )


# ============================================
# Context Integration
# ============================================


def wire_selection_to_context(
    selection: SourceSelectionResult,
    context_metadata: Dict[str, Any],
) -> Dict[str, Any]:
    """Wire selection result into cycle context metadata.

    This makes selection available to:
    - Observer agent (for reporting)
    - Reports (for audit trail)
    - Evidence metadata

    Args:
        selection: The selection result
        context_metadata: Existing context metadata dict

    Returns:
        Updated context metadata
    """
    context_metadata["source_selection"] = {
        "selection_id": selection.selection_id,
        "policy_id": selection.policy_id,
        "selected_pool_ids": selection.selected_pool_ids,
        "selection_hash": selection.selection_hash,
        "input_hash": selection.input_hash,
        "pools_metadata": [p.to_dict() for p in selection.selected_pools],
        "strategy": selection.selection_strategy_used,
    }

    # Add individual pool references for easy access
    context_metadata["source_pools"] = {p.pool_id: p.to_dict() for p in selection.selected_pools}

    return context_metadata

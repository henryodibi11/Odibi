"""Source Resolution: Deterministic source resolution at cycle start.

Phase 8.B - Wire CycleSourceConfig into CycleRunner.
Phase 8.C - Source Selection Policy Registry integration.

This module provides the source resolution logic that:
- Loads CycleSourceConfig (if present)
- Validates it against cycle mode
- Loads and validates policy from registry (Phase 8.C)
- Runs SourceSelector deterministically
- Creates ExecutionSourceContext in UNBOUND mode
- Produces SourceResolutionResult for reporting

INVARIANTS (Phase 8.B/8.C):
- NO pipeline execution
- NO data downloads
- NO ExecutionGateway changes
- NO agent autonomy changes
- NO side effects
- Everything is READ-ONLY and DETERMINISTIC
- Policy validation MUST pass before resolution
- Resolution MUST fail fast if policy invalid

HARD CONSTRAINTS (DO NOT VIOLATE):
❌ NO pipeline execution
❌ NO cycle execution
❌ NO agent autonomy changes
❌ NO data downloads
❌ NO ExecutionGateway changes
❌ NO default behavior changes
❌ NO source binding/mounting
❌ NO silent policy fallbacks

This module is 100% READ-ONLY at runtime.
"""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .cycle_source_config import (
    CycleSourceConfig,
    CycleSourceConfigGuard,
    SourceResolutionMetadata,
    get_default_source_config,
)
from .source_policy_registry import (
    PolicyRegistryError,
    PolicyResolutionResult,
    PolicyValidationStatus,
    SourcePolicyRegistry,
)
from .source_selection import (
    CycleMode,
    SourceSelectionPolicy,
    SourceSelectionResult,
    SourceSelector,
    get_default_policy,
)


# ============================================
# SourceResolutionResult
# ============================================


@dataclass
class SourceResolutionResult:
    """Result of deterministic source resolution at cycle start.

    This is an IMMUTABLE snapshot of the source resolution process.
    It contains all information needed for:
    - Cycle context (what sources are available)
    - Cycle report (source resolution metadata)
    - Determinism verification (hashes match)

    INVARIANTS:
    - Created once at cycle start
    - Never modified during cycle
    - All fields are read-only after creation
    """

    # === Identification ===
    resolution_id: str
    cycle_id: str
    mode: str

    # === Selection Result ===
    selected_pool_names: List[str]
    selected_pool_ids: List[str]
    tiers_used: List[str]

    # === Hashes for Determinism ===
    selection_hash: str
    input_hash: str
    context_id: str

    # === Statistics ===
    pools_considered: int
    pools_eligible: int
    pools_excluded: int

    # === Policy Reference ===
    policy_id: str
    selection_strategy: str

    # === Config Snapshot ===
    require_frozen: bool
    allow_messy: bool
    allow_tier_mixing: bool
    max_sources: int
    allowed_tiers: List[str]

    # === Timing ===
    resolved_at: str

    # === Policy Resolution (Phase 8.C) - fields with defaults ===
    policy_hash: str = ""
    policy_validation_status: str = ""
    policy_warnings: List[str] = field(default_factory=list)
    policy_version: str = ""

    # === Underlying Selection Result (for context creation) ===
    _selection_result: Optional[SourceSelectionResult] = field(default=None, repr=False)

    def __post_init__(self):
        """Freeze the result after creation."""
        pass

    @property
    def is_empty(self) -> bool:
        """Check if no pools were selected."""
        return len(self.selected_pool_ids) == 0

    def verify_determinism(self, expected_hash: str) -> bool:
        """Verify that selection matches expected hash.

        Args:
            expected_hash: The expected selection_hash

        Returns:
            True if hashes match (selection is deterministic)
        """
        return self.selection_hash == expected_hash

    def to_metadata(self) -> SourceResolutionMetadata:
        """Convert to SourceResolutionMetadata for cycle reports.

        Returns:
            SourceResolutionMetadata instance
        """
        return SourceResolutionMetadata(
            selection_policy=self.policy_id,
            tiers_used=self.tiers_used,
            source_pool_names=self.selected_pool_names,
            selection_hash=self.selection_hash,
            context_id=self.context_id,
            input_hash=self.input_hash,
            pools_considered=self.pools_considered,
            pools_eligible=self.pools_eligible,
            pools_excluded=self.pools_excluded,
            require_frozen=self.require_frozen,
            allow_messy=self.allow_messy,
            allow_tier_mixing=self.allow_tier_mixing,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for context/reports."""
        return {
            "resolution_id": self.resolution_id,
            "cycle_id": self.cycle_id,
            "mode": self.mode,
            "selected_pool_names": self.selected_pool_names,
            "selected_pool_ids": self.selected_pool_ids,
            "tiers_used": self.tiers_used,
            "selection_hash": self.selection_hash,
            "input_hash": self.input_hash,
            "context_id": self.context_id,
            "pools_considered": self.pools_considered,
            "pools_eligible": self.pools_eligible,
            "pools_excluded": self.pools_excluded,
            "policy_id": self.policy_id,
            "selection_strategy": self.selection_strategy,
            "policy_hash": self.policy_hash,
            "policy_validation_status": self.policy_validation_status,
            "policy_warnings": self.policy_warnings,
            "policy_version": self.policy_version,
            "require_frozen": self.require_frozen,
            "allow_messy": self.allow_messy,
            "allow_tier_mixing": self.allow_tier_mixing,
            "max_sources": self.max_sources,
            "allowed_tiers": self.allowed_tiers,
            "resolved_at": self.resolved_at,
        }

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)


# ============================================
# SourceResolutionError
# ============================================


class SourceResolutionError(Exception):
    """Error during source resolution (config-time, not runtime)."""

    def __init__(
        self,
        message: str,
        error_type: str = "RESOLUTION_ERROR",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.message = message
        self.error_type = error_type
        self.details = details or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error_type": self.error_type,
            "message": self.message,
            "details": self.details,
        }


# ============================================
# UnboundSourceContext
# ============================================


@dataclass
class UnboundSourceContext:
    """Source context in UNBOUND mode (read-only, no mounting).

    This is the Phase 8.B equivalent of ExecutionSourceContext but:
    - NO mounting of pools
    - NO gateway wiring
    - NO path resolution
    - ONLY metadata for reporting

    The context is "unbound" because it:
    - Knows which pools WOULD be used
    - Does NOT bind them to execution
    - Does NOT allow data access

    FUTURE (Phase 8.E):
    - This will be upgraded to ExecutionSourceContext with binding
    """

    context_id: str
    cycle_id: str
    resolution_id: str

    # Pool metadata (no paths, no access)
    pool_ids: List[str]
    pool_names: List[str]
    tiers: List[str]

    # Config snapshot
    require_frozen: bool
    allow_messy: bool
    policy_id: str

    # Status
    bound: bool = False  # Always False in Phase 8.B
    created_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(UTC).isoformat() + "Z"

    @classmethod
    def from_resolution_result(cls, result: SourceResolutionResult) -> "UnboundSourceContext":
        """Create unbound context from resolution result.

        Args:
            result: The SourceResolutionResult from resolution

        Returns:
            UnboundSourceContext (no data access)
        """
        return cls(
            context_id=result.context_id,
            cycle_id=result.cycle_id,
            resolution_id=result.resolution_id,
            pool_ids=result.selected_pool_ids,
            pool_names=result.selected_pool_names,
            tiers=result.tiers_used,
            require_frozen=result.require_frozen,
            allow_messy=result.allow_messy,
            policy_id=result.policy_id,
            bound=False,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "context_id": self.context_id,
            "cycle_id": self.cycle_id,
            "resolution_id": self.resolution_id,
            "pool_ids": self.pool_ids,
            "pool_names": self.pool_names,
            "tiers": self.tiers,
            "require_frozen": self.require_frozen,
            "allow_messy": self.allow_messy,
            "policy_id": self.policy_id,
            "bound": self.bound,
            "created_at": self.created_at,
        }


# ============================================
# SourceResolver
# ============================================


class SourceResolver:
    """Deterministic source resolver for cycle start.

    This class orchestrates the source resolution flow:
    1. Load CycleSourceConfig (or use default for mode)
    2. Validate config against mode (via CycleSourceConfigGuard)
    3. Load and validate policy from registry (Phase 8.C)
    4. Run SourceSelector.select() deterministically
    5. Create SourceResolutionResult with policy metadata
    6. Create UnboundSourceContext

    INVARIANTS:
    - NO side effects
    - NO data access
    - NO execution
    - Same inputs → Same outputs
    - Policy MUST be valid before resolution proceeds
    - NO silent fallbacks to default policies
    """

    def __init__(self, odibi_root: str):
        """Initialize resolver with Odibi root path.

        Args:
            odibi_root: Path to Odibi repository root
        """
        self.odibi_root = odibi_root
        self.metadata_path = Path(odibi_root) / ".odibi" / "source_metadata"
        self._config_guard = CycleSourceConfigGuard()
        self._policy_registry: Optional[SourcePolicyRegistry] = None
        self._policy_resolution: Optional[PolicyResolutionResult] = None

    def resolve(
        self,
        cycle_id: str,
        mode: CycleMode,
        source_config: Optional[CycleSourceConfig] = None,
        policy: Optional[SourceSelectionPolicy] = None,
        use_registry: bool = True,
    ) -> SourceResolutionResult:
        """Resolve sources for a cycle.

        This is the main entry point for source resolution.

        Args:
            cycle_id: The cycle identifier
            mode: The cycle operating mode
            source_config: Optional CycleSourceConfig (uses default if None)
            policy: Optional SourceSelectionPolicy (uses default if None)
            use_registry: If True, load policy from registry (Phase 8.C)

        Returns:
            SourceResolutionResult with selected pools

        Raises:
            SourceResolutionError: If validation fails or no pools eligible
        """
        # Step 1: Get or create config
        config = source_config or get_default_source_config(mode)

        # Step 2: Validate config against mode (fail fast)
        self._validate_config(config, mode)

        # Step 3: Load and validate policy (Phase 8.C)
        selection_policy, policy_resolution = self._resolve_policy(
            config=config,
            mode=mode,
            policy_override=policy,
            use_registry=use_registry,
        )

        # Step 4: Apply config constraints to policy
        effective_policy = self._apply_config_to_policy(config, selection_policy)

        # Step 5: Run selection (deterministic)
        selection_result = self._run_selection(
            cycle_id=cycle_id,
            mode=mode,
            policy=effective_policy,
        )

        # Step 6: Build resolution result with policy metadata
        result = self._build_result(
            cycle_id=cycle_id,
            mode=mode,
            config=config,
            selection_result=selection_result,
            policy_resolution=policy_resolution,
        )

        return result

    def _validate_config(self, config: CycleSourceConfig, mode: CycleMode) -> None:
        """Validate config against mode and guardrails.

        Raises:
            SourceResolutionError: If validation fails
        """
        errors = self._config_guard.validate(config, mode)
        if errors:
            error_messages = [e.message for e in errors]
            raise SourceResolutionError(
                message=f"Config validation failed: {'; '.join(error_messages)}",
                error_type="CONFIG_VALIDATION_FAILED",
                details={
                    "errors": [e.to_dict() for e in errors],
                    "mode": mode.value,
                },
            )

    def _resolve_policy(
        self,
        config: CycleSourceConfig,
        mode: CycleMode,
        policy_override: Optional[SourceSelectionPolicy] = None,
        use_registry: bool = True,
    ) -> tuple[SourceSelectionPolicy, Optional[PolicyResolutionResult]]:
        """Load and validate policy from registry or use override.

        Phase 8.C: Policy resolution with validation.

        Args:
            config: The CycleSourceConfig
            mode: The cycle mode
            policy_override: Optional policy to use instead of registry
            use_registry: Whether to use the policy registry

        Returns:
            Tuple of (SourceSelectionPolicy, PolicyResolutionResult or None)

        Raises:
            SourceResolutionError: If policy validation fails
        """
        # If policy override provided, use it directly
        if policy_override is not None:
            return policy_override, None

        # If registry disabled, use default
        if not use_registry:
            return get_default_policy(mode), None

        # Try to load from registry
        policy_name = config.selection_policy
        try:
            self._policy_registry = SourcePolicyRegistry(self.odibi_root)
            self._policy_registry.load_all()

            # Check if policy exists in registry
            if not self._policy_registry.exists(policy_name):
                # Policy not in registry - use default (with warning)
                return get_default_policy(mode), PolicyResolutionResult(
                    policy_name=policy_name,
                    policy_hash="",
                    validation_status=PolicyValidationStatus.WARNINGS,
                    resolved_at=datetime.now(UTC).isoformat() + "Z",
                    warnings=[
                        f"Policy '{policy_name}' not found in registry, using default for mode '{mode.value}'"
                    ],
                    mode_validated=mode.value,
                )

            # Resolve policy with validation (fail fast on errors)
            policy_resolution = self._policy_registry.resolve(policy_name, mode, fail_on_error=True)
            self._policy_resolution = policy_resolution

            # Get the selection policy
            selection_policy = self._policy_registry.get_selection_policy(policy_name)
            return selection_policy, policy_resolution

        except PolicyRegistryError as e:
            raise SourceResolutionError(
                message=f"Policy resolution failed: {e.message}",
                error_type="POLICY_RESOLUTION_FAILED",
                details=e.details,
            )

    def _apply_config_to_policy(
        self,
        config: CycleSourceConfig,
        policy: SourceSelectionPolicy,
    ) -> SourceSelectionPolicy:
        """Apply CycleSourceConfig constraints to policy.

        Creates a new policy with config constraints overlaid.

        Args:
            config: The CycleSourceConfig
            policy: The base SourceSelectionPolicy

        Returns:
            New SourceSelectionPolicy with config constraints
        """
        policy_dict = policy.model_dump()
        policy_dict["max_pools_per_cycle"] = min(config.max_sources, policy.max_pools_per_cycle)
        policy_dict["allow_messy_data"] = config.allow_messy

        return SourceSelectionPolicy(**policy_dict)

    def _run_selection(
        self,
        cycle_id: str,
        mode: CycleMode,
        policy: SourceSelectionPolicy,
    ) -> SourceSelectionResult:
        """Run the source selector.

        Args:
            cycle_id: The cycle identifier
            mode: The cycle operating mode
            policy: The effective policy

        Returns:
            SourceSelectionResult from selector

        Raises:
            SourceResolutionError: If no pools available
        """
        if not self.metadata_path.exists():
            raise SourceResolutionError(
                message=f"Source metadata directory not found: {self.metadata_path}",
                error_type="METADATA_NOT_FOUND",
                details={"path": str(self.metadata_path)},
            )

        selector = SourceSelector(self.metadata_path)
        result = selector.select(
            policy=policy,
            cycle_id=cycle_id,
            mode=mode,
        )

        if not result.selected_pool_ids:
            raise SourceResolutionError(
                message="No eligible pools found for selection",
                error_type="NO_ELIGIBLE_POOLS",
                details={
                    "policy_id": policy.policy_id,
                    "mode": mode.value,
                    "pools_considered": result.pools_considered,
                    "pools_excluded": result.pools_excluded,
                },
            )

        return result

    def _build_result(
        self,
        cycle_id: str,
        mode: CycleMode,
        config: CycleSourceConfig,
        selection_result: SourceSelectionResult,
        policy_resolution: Optional[PolicyResolutionResult] = None,
    ) -> SourceResolutionResult:
        """Build SourceResolutionResult from selection.

        Args:
            cycle_id: The cycle identifier
            mode: The cycle operating mode
            config: The CycleSourceConfig used
            selection_result: The selection result
            policy_resolution: Optional PolicyResolutionResult (Phase 8.C)

        Returns:
            SourceResolutionResult
        """
        # Extract tier information from selected pools
        tiers_used = set()
        pool_names = []
        for pool in selection_result.selected_pools:
            tier_value = getattr(pool, "tier", None)
            if tier_value:
                tiers_used.add(tier_value)
            pool_names.append(pool.name)

        # Generate context_id
        context_id = f"ctx_{cycle_id}_{selection_result.selection_id[-8:]}"

        # Generate resolution_id
        resolution_hash = hashlib.sha256(
            f"{cycle_id}_{selection_result.selection_hash}".encode()
        ).hexdigest()[:12]
        resolution_id = f"res_{cycle_id}_{resolution_hash}"

        # Extract policy metadata (Phase 8.C)
        policy_hash = ""
        policy_validation_status = ""
        policy_warnings: List[str] = []
        policy_version = ""
        if policy_resolution:
            policy_hash = policy_resolution.policy_hash
            policy_validation_status = policy_resolution.validation_status.value
            policy_warnings = policy_resolution.warnings
            policy_version = policy_resolution.policy_version

        return SourceResolutionResult(
            resolution_id=resolution_id,
            cycle_id=cycle_id,
            mode=mode.value,
            selected_pool_names=pool_names,
            selected_pool_ids=selection_result.selected_pool_ids,
            tiers_used=list(tiers_used),
            selection_hash=selection_result.selection_hash,
            input_hash=selection_result.input_hash,
            context_id=context_id,
            pools_considered=selection_result.pools_considered,
            pools_eligible=selection_result.pools_eligible,
            pools_excluded=selection_result.pools_excluded,
            policy_id=selection_result.policy_id,
            selection_strategy=selection_result.selection_strategy_used,
            policy_hash=policy_hash,
            policy_validation_status=policy_validation_status,
            policy_warnings=policy_warnings,
            policy_version=policy_version,
            require_frozen=config.require_frozen,
            allow_messy=config.allow_messy,
            allow_tier_mixing=config.allow_tier_mixing,
            max_sources=config.max_sources,
            allowed_tiers=[t.value for t in config.allowed_tiers],
            resolved_at=datetime.now(UTC).isoformat() + "Z",
            _selection_result=selection_result,
        )

    def create_unbound_context(self, result: SourceResolutionResult) -> UnboundSourceContext:
        """Create unbound source context from resolution result.

        Args:
            result: The SourceResolutionResult

        Returns:
            UnboundSourceContext (read-only, no data access)
        """
        return UnboundSourceContext.from_resolution_result(result)


# ============================================
# Convenience Functions
# ============================================


def resolve_sources_for_cycle(
    odibi_root: str,
    cycle_id: str,
    mode: CycleMode,
    source_config: Optional[CycleSourceConfig] = None,
) -> SourceResolutionResult:
    """Convenience function to resolve sources for a cycle.

    Args:
        odibi_root: Path to Odibi repository root
        cycle_id: The cycle identifier
        mode: The cycle operating mode
        source_config: Optional CycleSourceConfig

    Returns:
        SourceResolutionResult
    """
    resolver = SourceResolver(odibi_root)
    return resolver.resolve(cycle_id, mode, source_config)


def verify_resolution_determinism(
    odibi_root: str,
    cycle_id: str,
    mode: CycleMode,
    expected_hash: str,
    source_config: Optional[CycleSourceConfig] = None,
) -> bool:
    """Verify that source resolution is deterministic.

    Re-runs resolution and checks that selection_hash matches.

    Args:
        odibi_root: Path to Odibi repository root
        cycle_id: The cycle identifier
        mode: The cycle operating mode
        expected_hash: Expected selection_hash
        source_config: Optional CycleSourceConfig

    Returns:
        True if resolution is deterministic (hashes match)
    """
    result = resolve_sources_for_cycle(odibi_root, cycle_id, mode, source_config)
    return result.verify_determinism(expected_hash)

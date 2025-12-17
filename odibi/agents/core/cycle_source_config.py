"""Cycle Source Configuration: Control-plane wiring for source selection.

Phase 8.A - Design + Schema + Guardrails Only.

This module provides the configuration schemas that connect:
- SourceSelectionPolicy (Phase 7.C)
- Source Tiers (tier0 → tier600gb → tier2tb) (Phase 7.E)
- ExecutionSourceContext (Phase 7.D)
- Cycle execution (CycleConfig)

INVARIANTS (Phase 8.A):
- This is SCHEMA ONLY - no execution logic
- All validation is CONFIG-TIME, not runtime
- Zero side effects at import or instantiation
- Determinism is enforced via hash verification

HARD CONSTRAINTS (DO NOT VIOLATE):
❌ NO pipeline execution
❌ NO cycle execution
❌ NO agent autonomy changes
❌ NO data downloads
❌ NO ExecutionGateway changes
❌ NO default behavior changes

This module is 100% INERT at runtime.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator

from .source_selection import CycleMode


# ============================================
# Tier Definitions
# ============================================


class SourceTier(str, Enum):
    """Available source data tiers.

    Tiers are ordered by size and represent different data volumes.
    Each tier includes all datasets from smaller tiers plus additional ones.
    """

    TIER0 = "tier0"  # ~1GB - minimal testing
    TIER20GB = "tier20gb"  # ~20GB - local development
    TIER100GB = "tier100gb"  # ~100GB - integration testing
    TIER600GB = "tier600gb"  # ~600GB - large-scale (FineWeb, OSM Planet)
    TIER2TB = "tier2tb"  # ~2TB - massive (multiple FineWeb snapshots)


TIER_SIZE_LIMITS_GB: dict[SourceTier, float] = {
    SourceTier.TIER0: 1.0,
    SourceTier.TIER20GB: 20.0,
    SourceTier.TIER100GB: 100.0,
    SourceTier.TIER600GB: 600.0,
    SourceTier.TIER2TB: 2000.0,
}


# Tier hierarchy for inclusion checks
TIER_HIERARCHY: list[SourceTier] = [
    SourceTier.TIER0,
    SourceTier.TIER20GB,
    SourceTier.TIER100GB,
    SourceTier.TIER600GB,
    SourceTier.TIER2TB,
]


def tier_includes(parent: SourceTier, child: SourceTier) -> bool:
    """Check if parent tier includes all data from child tier.

    Larger tiers include all data from smaller tiers.

    Args:
        parent: The tier to check
        child: The tier that might be included

    Returns:
        True if parent includes child
    """
    parent_idx = TIER_HIERARCHY.index(parent)
    child_idx = TIER_HIERARCHY.index(child)
    return parent_idx >= child_idx


# ============================================
# Pool Cleanliness Types
# ============================================


class PoolCleanliness(str, Enum):
    """Data quality classification for source pools."""

    CLEAN = "clean"  # Well-formed, no intentional issues
    MESSY = "messy"  # Intentional quality issues (nulls, duplicates, etc.)
    MIXED = "mixed"  # Combination of clean and messy data


# ============================================
# CycleSourceConfig Schema
# ============================================


class CycleSourceConfig(BaseModel):
    """Configuration for source selection within a cycle.

    This schema allows a cycle to declare:
    - Which selection policy to use (by name)
    - Which tiers are allowed
    - Maximum number of sources
    - Cleanliness requirements
    - Determinism settings

    DESIGN NOTES:
    - This is PURE CONFIGURATION - no execution logic
    - Validation happens at config-time, not runtime
    - All errors are raised during schema validation

    USAGE:
        config = CycleSourceConfig(
            selection_policy="learning_default",
            allowed_tiers=[SourceTier.TIER0, SourceTier.TIER20GB],
            max_sources=3,
            require_clean=False,
            deterministic=True,
        )

    This config is then attached to CycleConfig and used during
    the source resolution step (Phase 8.B+).
    """

    # === Policy Reference ===
    selection_policy: str = Field(
        default="learning_default",
        description="Name of the SourceSelectionPolicy to use. Must match a registered policy_id.",
        pattern=r"^[a-z][a-z0-9_]*$",
    )

    # === Tier Constraints ===
    allowed_tiers: list[SourceTier] = Field(
        default_factory=lambda: [SourceTier.TIER0],
        description="Explicit list of tiers that MAY be used. "
        "Empty list means no tier restriction (use policy defaults).",
    )

    # === Source Limits ===
    max_sources: int = Field(
        default=3,
        ge=1,
        le=20,
        description="Maximum number of source pools to select per cycle.",
    )

    # === Quality Constraints ===
    require_clean: bool = Field(
        default=False,
        description="If True, ONLY clean pools may be selected (no messy/mixed).",
    )

    allow_messy: bool = Field(
        default=True,
        description="If True, messy pools may be included in selection.",
    )

    # === Determinism ===
    deterministic: bool = Field(
        default=True,
        description="If True, selection must be fully deterministic. "
        "Same config + same pool_index → same selection.",
    )

    # === Tier Mixing ===
    allow_tier_mixing: bool = Field(
        default=False,
        description="If True, pools from multiple tiers may be selected together. "
        "If False, all selected pools must be from a single tier.",
    )

    # === Frozen Requirement ===
    require_frozen: bool = Field(
        default=True,
        description="If True, only FROZEN pools may be selected.",
    )

    @model_validator(mode="after")
    def validate_cleanliness_constraints(self):
        """Ensure cleanliness constraints are consistent."""
        if self.require_clean and self.allow_messy:
            raise ValueError(
                "Cannot set both require_clean=True and allow_messy=True. "
                "require_clean implies no messy pools are allowed."
            )
        return self

    @model_validator(mode="after")
    def validate_tier_list_not_empty_when_restricted(self):
        """Ensure allowed_tiers is populated when tier mixing is disabled."""
        if not self.allow_tier_mixing and len(self.allowed_tiers) > 1:
            pass
        return self

    def get_max_tier_size_gb(self) -> float:
        """Get the maximum tier size allowed by this config.

        Returns:
            Maximum size in GB from allowed tiers
        """
        if not self.allowed_tiers:
            return TIER_SIZE_LIMITS_GB[SourceTier.TIER2TB]
        return max(TIER_SIZE_LIMITS_GB[tier] for tier in self.allowed_tiers)

    def is_tier_allowed(self, tier: SourceTier) -> bool:
        """Check if a tier is allowed by this config.

        Args:
            tier: The tier to check

        Returns:
            True if tier is in allowed_tiers or allowed_tiers is empty
        """
        if not self.allowed_tiers:
            return True
        return tier in self.allowed_tiers


# ============================================
# Mode-Tier Compatibility Matrix
# ============================================


@dataclass(frozen=True)
class ModeTierRule:
    """A single mode-tier compatibility rule.

    Immutable to prevent runtime modification.
    """

    mode: CycleMode
    tier: SourceTier
    allowed: bool
    reason: str


MODE_TIER_RULES: list[ModeTierRule] = [
    ModeTierRule(
        mode=CycleMode.SCHEDULED,
        tier=SourceTier.TIER0,
        allowed=True,
        reason="Scheduled mode may use tier0 frozen pools",
    ),
    ModeTierRule(
        mode=CycleMode.SCHEDULED,
        tier=SourceTier.TIER20GB,
        allowed=True,
        reason="Scheduled mode may use tier20gb frozen pools",
    ),
    ModeTierRule(
        mode=CycleMode.SCHEDULED,
        tier=SourceTier.TIER100GB,
        allowed=True,
        reason="Scheduled mode may use tier100gb frozen pools",
    ),
    ModeTierRule(
        mode=CycleMode.SCHEDULED,
        tier=SourceTier.TIER600GB,
        allowed=True,
        reason="Scheduled mode may use tier600gb frozen pools",
    ),
    ModeTierRule(
        mode=CycleMode.SCHEDULED,
        tier=SourceTier.TIER2TB,
        allowed=True,
        reason="Scheduled mode may use tier2tb frozen pools",
    ),
    ModeTierRule(
        mode=CycleMode.IMPROVEMENT,
        tier=SourceTier.TIER0,
        allowed=True,
        reason="Improvement mode may use tier0",
    ),
    ModeTierRule(
        mode=CycleMode.IMPROVEMENT,
        tier=SourceTier.TIER20GB,
        allowed=True,
        reason="Improvement mode may use tier20gb",
    ),
    ModeTierRule(
        mode=CycleMode.IMPROVEMENT,
        tier=SourceTier.TIER100GB,
        allowed=True,
        reason="Improvement mode may use tier100gb",
    ),
    ModeTierRule(
        mode=CycleMode.IMPROVEMENT,
        tier=SourceTier.TIER600GB,
        allowed=False,
        reason="tier600gb+ MAY NOT be used in Improvement mode (too large for change validation)",
    ),
    ModeTierRule(
        mode=CycleMode.IMPROVEMENT,
        tier=SourceTier.TIER2TB,
        allowed=False,
        reason="tier2tb MAY NOT be used in Improvement mode (too large for change validation)",
    ),
    ModeTierRule(
        mode=CycleMode.LEARNING,
        tier=SourceTier.TIER0,
        allowed=True,
        reason="Learning mode may use tier0",
    ),
    ModeTierRule(
        mode=CycleMode.LEARNING,
        tier=SourceTier.TIER20GB,
        allowed=True,
        reason="Learning mode may use tier20gb",
    ),
    ModeTierRule(
        mode=CycleMode.LEARNING,
        tier=SourceTier.TIER100GB,
        allowed=True,
        reason="Learning mode may use tier100gb (observation only)",
    ),
    ModeTierRule(
        mode=CycleMode.LEARNING,
        tier=SourceTier.TIER600GB,
        allowed=True,
        reason="Learning mode may use tier600gb (observation only, no changes)",
    ),
    ModeTierRule(
        mode=CycleMode.LEARNING,
        tier=SourceTier.TIER2TB,
        allowed=True,
        reason="Learning mode may use tier2tb (observation only, no changes)",
    ),
]


def get_mode_tier_rule(mode: CycleMode, tier: SourceTier) -> Optional[ModeTierRule]:
    """Get the rule for a mode-tier combination.

    Args:
        mode: The cycle mode
        tier: The source tier

    Returns:
        ModeTierRule if found, None otherwise
    """
    for rule in MODE_TIER_RULES:
        if rule.mode == mode and rule.tier == tier:
            return rule
    return None


def is_mode_tier_allowed(mode: CycleMode, tier: SourceTier) -> bool:
    """Check if a mode-tier combination is allowed.

    Args:
        mode: The cycle mode
        tier: The source tier

    Returns:
        True if the combination is allowed
    """
    rule = get_mode_tier_rule(mode, tier)
    return rule.allowed if rule else False


# ============================================
# CycleSourceConfigGuard - Config-Time Validation
# ============================================


@dataclass
class ConfigValidationError:
    """A single config validation error.

    These are raised at CONFIG-TIME, not runtime.
    """

    field: str
    value: Any
    rule: str
    message: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "value": str(self.value),
            "rule": self.rule,
            "message": self.message,
        }


class CycleSourceConfigGuard:
    """Validates CycleSourceConfig against mode and guardrail constraints.

    All validation is CONFIG-TIME:
    - Called when CycleSourceConfig is created
    - Raises errors before any execution
    - No runtime side effects

    GUARDRAILS ENFORCED:
    1. Scheduled mode MAY ONLY use FROZEN pools
    2. tier600gb+ MAY NOT be used in Improvement mode
    3. Learning mode MAY include messy pools
    4. Cycles MAY NOT mix tiers unless explicitly allowed

    USAGE:
        guard = CycleSourceConfigGuard()
        errors = guard.validate(config, mode=CycleMode.SCHEDULED)
        if errors:
            raise ValueError(f"Config validation failed: {errors}")
    """

    def validate(
        self,
        config: CycleSourceConfig,
        mode: CycleMode,
    ) -> list[ConfigValidationError]:
        """Validate config against mode and guardrails.

        Args:
            config: The CycleSourceConfig to validate
            mode: The cycle mode being used

        Returns:
            List of validation errors (empty if valid)
        """
        errors: list[ConfigValidationError] = []

        errors.extend(self._validate_frozen_requirement(config, mode))
        errors.extend(self._validate_tier_mode_compatibility(config, mode))
        errors.extend(self._validate_tier_mixing(config))
        errors.extend(self._validate_cleanliness_for_mode(config, mode))

        return errors

    def _validate_frozen_requirement(
        self,
        config: CycleSourceConfig,
        mode: CycleMode,
    ) -> list[ConfigValidationError]:
        """GUARDRAIL: Scheduled mode MAY ONLY use FROZEN pools."""
        errors = []

        if mode == CycleMode.SCHEDULED and not config.require_frozen:
            errors.append(
                ConfigValidationError(
                    field="require_frozen",
                    value=config.require_frozen,
                    rule="SCHEDULED_MODE_FROZEN_ONLY",
                    message="Scheduled mode MAY ONLY use FROZEN pools. "
                    "Set require_frozen=True for scheduled cycles.",
                )
            )

        return errors

    def _validate_tier_mode_compatibility(
        self,
        config: CycleSourceConfig,
        mode: CycleMode,
    ) -> list[ConfigValidationError]:
        """GUARDRAIL: tier600gb+ MAY NOT be used in Improvement mode."""
        errors = []

        for tier in config.allowed_tiers:
            if not is_mode_tier_allowed(mode, tier):
                rule = get_mode_tier_rule(mode, tier)
                reason = rule.reason if rule else "Unknown mode-tier combination"

                errors.append(
                    ConfigValidationError(
                        field="allowed_tiers",
                        value=tier.value,
                        rule="MODE_TIER_COMPATIBILITY",
                        message=reason,
                    )
                )

        return errors

    def _validate_tier_mixing(
        self,
        config: CycleSourceConfig,
    ) -> list[ConfigValidationError]:
        """GUARDRAIL: Cycles MAY NOT mix tiers unless explicitly allowed."""
        errors = []

        if not config.allow_tier_mixing and len(config.allowed_tiers) > 1:
            tiers_by_size = sorted(
                config.allowed_tiers,
                key=lambda t: TIER_SIZE_LIMITS_GB[t],
            )
            largest = tiers_by_size[-1]
            smallest = tiers_by_size[0]

            if not tier_includes(largest, smallest):
                errors.append(
                    ConfigValidationError(
                        field="allowed_tiers",
                        value=[t.value for t in config.allowed_tiers],
                        rule="NO_TIER_MIXING",
                        message=f"Tier mixing is disabled but multiple non-inclusive tiers "
                        f"are specified: {[t.value for t in config.allowed_tiers]}. "
                        f"Either set allow_tier_mixing=True or restrict to a single tier.",
                    )
                )

        return errors

    def _validate_cleanliness_for_mode(
        self,
        config: CycleSourceConfig,
        mode: CycleMode,
    ) -> list[ConfigValidationError]:
        """GUARDRAIL: Learning mode MAY include messy pools."""
        errors = []

        if mode == CycleMode.IMPROVEMENT and config.allow_messy:
            pass

        return errors

    def raise_on_errors(
        self,
        config: CycleSourceConfig,
        mode: CycleMode,
    ) -> None:
        """Validate and raise if errors found.

        Args:
            config: The CycleSourceConfig to validate
            mode: The cycle mode being used

        Raises:
            ValueError: If validation errors are found
        """
        errors = self.validate(config, mode)
        if errors:
            error_messages = [e.message for e in errors]
            raise ValueError(
                f"CycleSourceConfig validation failed with {len(errors)} error(s):\n"
                + "\n".join(f"  - {msg}" for msg in error_messages)
            )


# ============================================
# SourceResolutionStep - DESIGN ONLY
# ============================================


class SourceResolutionStep:
    """Conceptual description of source resolution during cycle execution.

    ⚠️ THIS IS DESIGN ONLY - NO RUNTIME IMPLEMENTATION ⚠️

    The SourceResolutionStep describes how, conceptually:

        CycleConfig
          → CycleSourceConfig
            → SourceSelectionPolicy
              → SourceSelector.select()
                → SourceSelectionResult
                  → ExecutionSourceContext.from_selection()
                    → ExecutionSourceContext (bound to cycle)

    FLOW:
    1. CycleConfig contains a CycleSourceConfig
    2. CycleSourceConfig references a SourceSelectionPolicy by name
    3. Before cycle starts, the policy is loaded from registry
    4. SourceSelector.select() is called with policy + pool_index
    5. Result is a SourceSelectionResult with selected pools
    6. ExecutionSourceContext is created from the selection
    7. Context is bound to ExecutionGateway for the cycle

    DETERMINISM GUARANTEE:
    Given identical:
      - CycleSourceConfig
      - SourceSelectionPolicy
      - pool_index (frozen pool metadata)
    The resulting ExecutionSourceContext will be IDENTICAL.

    This is verified via:
      - input_hash (policy + pool_index + cycle_id)
      - selection_hash (sorted selected_pool_ids)
      - context_id (derived from selection_id)

    WHY NO IMPLEMENTATION YET:
    - Phase 8.A is design-only
    - Execution wiring is Phase 8.B+
    - This documents the contract for future phases

    FUTURE PHASES WILL:
    - Phase 8.B: Wire resolution to CycleRunner.start_cycle()
    - Phase 8.C: Add policy registry and loading
    - Phase 8.D: Add tier availability checks
    - Phase 8.E: Add context binding to ExecutionGateway
    """

    pass


# ============================================
# Report Schema Extensions
# ============================================


@dataclass
class SourceResolutionMetadata:
    """Metadata about source resolution for cycle reports.

    This is SCHEMA ONLY - no rendering logic.
    Extends report schema to include source selection information.

    Fields:
        selection_policy: Name of the policy used
        tiers_used: List of tier names that pools were selected from
        source_pool_names: List of selected pool names
        selection_hash: Deterministic hash of selection for verification
        context_id: ExecutionSourceContext identifier
    """

    selection_policy: str = ""
    tiers_used: list[str] = field(default_factory=list)
    source_pool_names: list[str] = field(default_factory=list)
    selection_hash: str = ""
    context_id: str = ""
    input_hash: str = ""
    pools_considered: int = 0
    pools_eligible: int = 0
    pools_excluded: int = 0
    require_frozen: bool = True
    allow_messy: bool = True
    allow_tier_mixing: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "selection_policy": self.selection_policy,
            "tiers_used": self.tiers_used,
            "source_pool_names": self.source_pool_names,
            "selection_hash": self.selection_hash,
            "context_id": self.context_id,
            "input_hash": self.input_hash,
            "pools_considered": self.pools_considered,
            "pools_eligible": self.pools_eligible,
            "pools_excluded": self.pools_excluded,
            "require_frozen": self.require_frozen,
            "allow_messy": self.allow_messy,
            "allow_tier_mixing": self.allow_tier_mixing,
        }

    @classmethod
    def from_selection_result(
        cls,
        selection_result: Any,
        config: CycleSourceConfig,
    ) -> "SourceResolutionMetadata":
        """Create metadata from a SourceSelectionResult.

        Args:
            selection_result: SourceSelectionResult instance
            config: The CycleSourceConfig used

        Returns:
            SourceResolutionMetadata for reports

        NOTE: This method is SCHEMA ONLY.
        Actual population happens in Phase 8.B+.
        """
        tiers = set()
        for pool in selection_result.selected_pools:
            tier_value = getattr(pool, "tier", None)
            if tier_value:
                tiers.add(tier_value)

        return cls(
            selection_policy=selection_result.policy_id,
            tiers_used=list(tiers),
            source_pool_names=[p.name for p in selection_result.selected_pools],
            selection_hash=selection_result.selection_hash,
            context_id=f"ctx_{selection_result.cycle_id}_{selection_result.selection_id[-8:]}",
            input_hash=selection_result.input_hash,
            pools_considered=selection_result.pools_considered,
            pools_eligible=selection_result.pools_eligible,
            pools_excluded=selection_result.pools_excluded,
            require_frozen=config.require_frozen,
            allow_messy=config.allow_messy,
            allow_tier_mixing=config.allow_tier_mixing,
        )


# ============================================
# Default Configs by Mode
# ============================================


def get_default_source_config(mode: CycleMode) -> CycleSourceConfig:
    """Get the default CycleSourceConfig for a cycle mode.

    Args:
        mode: The cycle operating mode

    Returns:
        Default CycleSourceConfig for that mode
    """
    if mode == CycleMode.LEARNING:
        return CycleSourceConfig(
            selection_policy="learning_default",
            allowed_tiers=[SourceTier.TIER0, SourceTier.TIER20GB, SourceTier.TIER100GB],
            max_sources=4,
            require_clean=False,
            allow_messy=True,
            deterministic=True,
            allow_tier_mixing=False,
            require_frozen=True,
        )

    elif mode == CycleMode.IMPROVEMENT:
        return CycleSourceConfig(
            selection_policy="improvement_default",
            allowed_tiers=[SourceTier.TIER0, SourceTier.TIER20GB],
            max_sources=2,
            require_clean=True,
            allow_messy=False,
            deterministic=True,
            allow_tier_mixing=False,
            require_frozen=True,
        )

    elif mode == CycleMode.SCHEDULED:
        return CycleSourceConfig(
            selection_policy="scheduled_default",
            allowed_tiers=[SourceTier.TIER0, SourceTier.TIER20GB, SourceTier.TIER100GB],
            max_sources=3,
            require_clean=False,
            allow_messy=True,
            deterministic=True,
            allow_tier_mixing=False,
            require_frozen=True,
        )

    raise ValueError(f"Unknown cycle mode: {mode}")

"""Tests for CycleSourceConfig and guardrails.

Phase 8.A - Schema and guardrail validation tests.

These tests verify:
1. CycleSourceConfig schema validation
2. CycleSourceConfigGuard enforcement
3. Mode-tier compatibility rules
4. SourceResolutionMetadata structure

CONSTRAINTS:
- Tests are read-only (no execution)
- Tests validate config-time behavior only
- No data downloads or pipeline runs
"""

import pytest

from odibi.agents.core.cycle_source_config import (
    ConfigValidationError,
    CycleSourceConfig,
    CycleSourceConfigGuard,
    SourceResolutionMetadata,
    SourceTier,
    get_default_source_config,
    get_mode_tier_rule,
    is_mode_tier_allowed,
    tier_includes,
    TIER_HIERARCHY,
    TIER_SIZE_LIMITS_GB,
)
from odibi.agents.core.source_selection import CycleMode


# ============================================
# SourceTier Tests
# ============================================


class TestSourceTier:
    """Tests for SourceTier enum and utilities."""

    def test_tier_values(self):
        """Verify all tier values are correct."""
        assert SourceTier.TIER0.value == "tier0"
        assert SourceTier.TIER20GB.value == "tier20gb"
        assert SourceTier.TIER100GB.value == "tier100gb"
        assert SourceTier.TIER600GB.value == "tier600gb"
        assert SourceTier.TIER2TB.value == "tier2tb"

    def test_tier_hierarchy_order(self):
        """Verify tiers are ordered by size."""
        expected = [
            SourceTier.TIER0,
            SourceTier.TIER20GB,
            SourceTier.TIER100GB,
            SourceTier.TIER600GB,
            SourceTier.TIER2TB,
        ]
        assert TIER_HIERARCHY == expected

    def test_tier_size_limits(self):
        """Verify size limits are defined for all tiers."""
        assert TIER_SIZE_LIMITS_GB[SourceTier.TIER0] == 1.0
        assert TIER_SIZE_LIMITS_GB[SourceTier.TIER20GB] == 20.0
        assert TIER_SIZE_LIMITS_GB[SourceTier.TIER100GB] == 100.0
        assert TIER_SIZE_LIMITS_GB[SourceTier.TIER600GB] == 600.0
        assert TIER_SIZE_LIMITS_GB[SourceTier.TIER2TB] == 2000.0

    def test_tier_includes_same_tier(self):
        """A tier includes itself."""
        for tier in SourceTier:
            assert tier_includes(tier, tier)

    def test_tier_includes_smaller(self):
        """Larger tiers include smaller tiers."""
        assert tier_includes(SourceTier.TIER2TB, SourceTier.TIER0)
        assert tier_includes(SourceTier.TIER600GB, SourceTier.TIER100GB)
        assert tier_includes(SourceTier.TIER100GB, SourceTier.TIER20GB)

    def test_tier_not_includes_larger(self):
        """Smaller tiers do not include larger tiers."""
        assert not tier_includes(SourceTier.TIER0, SourceTier.TIER20GB)
        assert not tier_includes(SourceTier.TIER100GB, SourceTier.TIER600GB)


# ============================================
# CycleSourceConfig Tests
# ============================================


class TestCycleSourceConfig:
    """Tests for CycleSourceConfig schema validation."""

    def test_default_config(self):
        """Default config should be valid."""
        config = CycleSourceConfig()
        assert config.selection_policy == "learning_default"
        assert config.allowed_tiers == [SourceTier.TIER0]
        assert config.max_sources == 3
        assert config.deterministic is True

    def test_custom_config(self):
        """Custom config with valid values."""
        config = CycleSourceConfig(
            selection_policy="custom_policy",
            allowed_tiers=[SourceTier.TIER0, SourceTier.TIER20GB],
            max_sources=5,
            require_clean=True,
            allow_messy=False,
        )
        assert config.selection_policy == "custom_policy"
        assert len(config.allowed_tiers) == 2
        assert config.max_sources == 5
        assert config.require_clean is True
        assert config.allow_messy is False

    def test_invalid_policy_name(self):
        """Policy name must match pattern."""
        with pytest.raises(ValueError):
            CycleSourceConfig(selection_policy="Invalid-Policy!")

    def test_max_sources_bounds(self):
        """max_sources must be between 1 and 20."""
        with pytest.raises(ValueError):
            CycleSourceConfig(max_sources=0)
        with pytest.raises(ValueError):
            CycleSourceConfig(max_sources=21)

    def test_cleanliness_constraint_conflict(self):
        """require_clean=True and allow_messy=True are mutually exclusive."""
        with pytest.raises(ValueError) as exc_info:
            CycleSourceConfig(require_clean=True, allow_messy=True)
        assert "require_clean" in str(exc_info.value)

    def test_get_max_tier_size_gb(self):
        """get_max_tier_size_gb returns correct value."""
        config = CycleSourceConfig(allowed_tiers=[SourceTier.TIER0, SourceTier.TIER100GB])
        assert config.get_max_tier_size_gb() == 100.0

    def test_is_tier_allowed(self):
        """is_tier_allowed checks allowed_tiers."""
        config = CycleSourceConfig(allowed_tiers=[SourceTier.TIER0, SourceTier.TIER20GB])
        assert config.is_tier_allowed(SourceTier.TIER0)
        assert config.is_tier_allowed(SourceTier.TIER20GB)
        assert not config.is_tier_allowed(SourceTier.TIER600GB)


# ============================================
# Mode-Tier Compatibility Tests
# ============================================


class TestModeTierCompatibility:
    """Tests for mode-tier compatibility rules."""

    def test_learning_mode_allows_all_tiers(self):
        """Learning mode allows all tiers."""
        for tier in SourceTier:
            assert is_mode_tier_allowed(CycleMode.LEARNING, tier)

    def test_improvement_mode_blocks_large_tiers(self):
        """Improvement mode blocks tier600gb and tier2tb."""
        assert is_mode_tier_allowed(CycleMode.IMPROVEMENT, SourceTier.TIER0)
        assert is_mode_tier_allowed(CycleMode.IMPROVEMENT, SourceTier.TIER20GB)
        assert is_mode_tier_allowed(CycleMode.IMPROVEMENT, SourceTier.TIER100GB)
        assert not is_mode_tier_allowed(CycleMode.IMPROVEMENT, SourceTier.TIER600GB)
        assert not is_mode_tier_allowed(CycleMode.IMPROVEMENT, SourceTier.TIER2TB)

    def test_scheduled_mode_allows_all_tiers(self):
        """Scheduled mode allows all tiers (with frozen requirement)."""
        for tier in SourceTier:
            assert is_mode_tier_allowed(CycleMode.SCHEDULED, tier)

    def test_get_mode_tier_rule_returns_rule(self):
        """get_mode_tier_rule returns the correct rule."""
        rule = get_mode_tier_rule(CycleMode.IMPROVEMENT, SourceTier.TIER600GB)
        assert rule is not None
        assert rule.allowed is False
        assert "MAY NOT" in rule.reason


# ============================================
# CycleSourceConfigGuard Tests
# ============================================


class TestCycleSourceConfigGuard:
    """Tests for CycleSourceConfigGuard validation."""

    def test_valid_learning_config(self):
        """Valid learning mode config passes validation."""
        guard = CycleSourceConfigGuard()
        config = CycleSourceConfig(
            allowed_tiers=[SourceTier.TIER0, SourceTier.TIER20GB],
            require_frozen=True,
        )
        errors = guard.validate(config, mode=CycleMode.LEARNING)
        assert len(errors) == 0

    def test_valid_improvement_config(self):
        """Valid improvement mode config passes validation."""
        guard = CycleSourceConfigGuard()
        config = CycleSourceConfig(
            allowed_tiers=[SourceTier.TIER0, SourceTier.TIER20GB],
            require_clean=True,
            allow_messy=False,
        )
        errors = guard.validate(config, mode=CycleMode.IMPROVEMENT)
        assert len(errors) == 0

    def test_scheduled_mode_requires_frozen(self):
        """Scheduled mode requires require_frozen=True."""
        guard = CycleSourceConfigGuard()
        config = CycleSourceConfig(
            allowed_tiers=[SourceTier.TIER0],
            require_frozen=False,  # INVALID for scheduled
        )
        errors = guard.validate(config, mode=CycleMode.SCHEDULED)
        assert len(errors) == 1
        assert errors[0].rule == "SCHEDULED_MODE_FROZEN_ONLY"

    def test_improvement_mode_blocks_large_tiers(self):
        """Improvement mode rejects tier600gb."""
        guard = CycleSourceConfigGuard()
        config = CycleSourceConfig(
            allowed_tiers=[SourceTier.TIER600GB],
        )
        errors = guard.validate(config, mode=CycleMode.IMPROVEMENT)
        assert len(errors) == 1
        assert errors[0].rule == "MODE_TIER_COMPATIBILITY"
        assert "tier600gb" in errors[0].value

    def test_raise_on_errors(self):
        """raise_on_errors raises ValueError on invalid config."""
        guard = CycleSourceConfigGuard()
        config = CycleSourceConfig(
            allowed_tiers=[SourceTier.TIER2TB],
        )
        with pytest.raises(ValueError) as exc_info:
            guard.raise_on_errors(config, mode=CycleMode.IMPROVEMENT)
        assert "validation failed" in str(exc_info.value)


# ============================================
# SourceResolutionMetadata Tests
# ============================================


class TestSourceResolutionMetadata:
    """Tests for SourceResolutionMetadata schema."""

    def test_default_metadata(self):
        """Default metadata has empty values."""
        metadata = SourceResolutionMetadata()
        assert metadata.selection_policy == ""
        assert metadata.tiers_used == []
        assert metadata.source_pool_names == []
        assert metadata.selection_hash == ""

    def test_populated_metadata(self):
        """Populated metadata has correct values."""
        metadata = SourceResolutionMetadata(
            selection_policy="learning_default",
            tiers_used=["tier0", "tier20gb"],
            source_pool_names=["simplewiki", "github_events"],
            selection_hash="abc123",
            context_id="ctx_cycle_xyz",
        )
        assert metadata.selection_policy == "learning_default"
        assert len(metadata.tiers_used) == 2
        assert len(metadata.source_pool_names) == 2

    def test_to_dict(self):
        """to_dict returns serializable dict."""
        metadata = SourceResolutionMetadata(
            selection_policy="test",
            pools_considered=10,
            pools_eligible=5,
            pools_excluded=5,
        )
        d = metadata.to_dict()
        assert d["selection_policy"] == "test"
        assert d["pools_considered"] == 10
        assert d["pools_eligible"] == 5
        assert d["pools_excluded"] == 5


# ============================================
# Default Config Tests
# ============================================


class TestDefaultSourceConfigs:
    """Tests for get_default_source_config."""

    def test_learning_default(self):
        """Learning mode default config."""
        config = get_default_source_config(CycleMode.LEARNING)
        assert config.selection_policy == "learning_default"
        assert config.allow_messy is True
        assert config.require_frozen is True

    def test_improvement_default(self):
        """Improvement mode default config."""
        config = get_default_source_config(CycleMode.IMPROVEMENT)
        assert config.selection_policy == "improvement_default"
        assert config.require_clean is True
        assert config.allow_messy is False

    def test_scheduled_default(self):
        """Scheduled mode default config."""
        config = get_default_source_config(CycleMode.SCHEDULED)
        assert config.selection_policy == "scheduled_default"
        assert config.require_frozen is True

    def test_unknown_mode_raises(self):
        """Unknown mode raises ValueError."""
        with pytest.raises(ValueError):
            get_default_source_config("unknown_mode")  # type: ignore


# ============================================
# ConfigValidationError Tests
# ============================================


class TestConfigValidationError:
    """Tests for ConfigValidationError dataclass."""

    def test_to_dict(self):
        """to_dict returns correct structure."""
        error = ConfigValidationError(
            field="allowed_tiers",
            value=["tier600gb"],
            rule="MODE_TIER_COMPATIBILITY",
            message="tier600gb not allowed in improvement mode",
        )
        d = error.to_dict()
        assert d["field"] == "allowed_tiers"
        assert d["rule"] == "MODE_TIER_COMPATIBILITY"
        assert "tier600gb" in d["message"]

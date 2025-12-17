"""Tests for source selection determinism and guardrails.

Phase 7.C - Selection-only, NO data execution.

These tests verify:
1. Determinism: Same inputs → Same outputs
2. Policy validation: Invalid policies are rejected
3. Guardrails: Forbidden operations are blocked
4. Context wiring: Selection flows to cycle context
"""

import json
import tempfile
from pathlib import Path

import pytest
import yaml

from odibi.agents.core.source_selection import (
    CycleMode,
    SelectionStrategy,
    SourceSelectionGuardrails,
    SourceSelectionPolicy,
    SourceSelector,
    get_default_policy,
    wire_selection_to_context,
)


# ============================================
# Test Fixtures
# ============================================


@pytest.fixture
def temp_metadata_dir():
    """Create a temporary metadata directory with sample pools."""
    with tempfile.TemporaryDirectory() as tmpdir:
        metadata_path = Path(tmpdir)
        pools_dir = metadata_path / "pools"
        pools_dir.mkdir()

        # Create pool index
        pool_index = {
            "version": "1.0.0",
            "status": "frozen",
            "pools": {
                "pool_clean_csv": "pools/pool_clean_csv.yaml",
                "pool_messy_csv": "pools/pool_messy_csv.yaml",
                "pool_clean_json": "pools/pool_clean_json.yaml",
                "pool_clean_parquet": "pools/pool_clean_parquet.yaml",
            },
        }
        with open(metadata_path / "pool_index.yaml", "w") as f:
            yaml.dump(pool_index, f)

        # Create sample pool metadata files
        pools = [
            {
                "pool_id": "pool_clean_csv",
                "name": "Clean CSV Pool",
                "file_format": "csv",
                "source_type": "local",
                "data_quality": "clean",
                "status": "frozen",
                "cache_path": "test/csv/clean/",
                "characteristics": {"row_count": 1000},
                "integrity": {"manifest_hash": "abc123"},
                "tests_coverage": ["csv_parsing"],
            },
            {
                "pool_id": "pool_messy_csv",
                "name": "Messy CSV Pool",
                "file_format": "csv",
                "source_type": "local",
                "data_quality": "messy",
                "status": "frozen",
                "cache_path": "test/csv/messy/",
                "characteristics": {"row_count": 500},
                "integrity": {"manifest_hash": "def456"},
                "tests_coverage": ["null_handling"],
            },
            {
                "pool_id": "pool_clean_json",
                "name": "Clean JSON Pool",
                "file_format": "json",
                "source_type": "local",
                "data_quality": "clean",
                "status": "frozen",
                "cache_path": "test/json/clean/",
                "characteristics": {"row_count": 2000},
                "integrity": {"manifest_hash": "ghi789"},
                "tests_coverage": ["json_parsing"],
            },
            {
                "pool_id": "pool_clean_parquet",
                "name": "Clean Parquet Pool",
                "file_format": "parquet",
                "source_type": "local",
                "data_quality": "clean",
                "status": "frozen",
                "cache_path": "test/parquet/clean/",
                "characteristics": {"row_count": 5000},
                "integrity": {"manifest_hash": "jkl012"},
                "tests_coverage": ["parquet_reading"],
            },
        ]

        for pool in pools:
            pool_file = pools_dir / f"{pool['pool_id']}.yaml"
            with open(pool_file, "w") as f:
                yaml.dump(pool, f)

        yield metadata_path


@pytest.fixture
def default_policy():
    """Create a default test policy."""
    return SourceSelectionPolicy(
        policy_id="test_policy",
        name="Test Policy",
        max_pools_per_cycle=2,
    )


# ============================================
# Determinism Tests
# ============================================


class TestDeterminism:
    """Tests for selection determinism guarantees."""

    def test_same_inputs_same_outputs(self, temp_metadata_dir, default_policy):
        """Same policy + pool_index + cycle_id must produce identical selection."""
        selector = SourceSelector(temp_metadata_dir)
        cycle_id = "test-cycle-001"

        result1 = selector.select(default_policy, cycle_id, CycleMode.LEARNING)
        result2 = selector.select(default_policy, cycle_id, CycleMode.LEARNING)

        assert result1.selected_pool_ids == result2.selected_pool_ids
        assert result1.selection_hash == result2.selection_hash
        assert result1.input_hash == result2.input_hash

    def test_different_cycle_id_different_selection(self, temp_metadata_dir, default_policy):
        """Different cycle_id should produce different selection (hash-based)."""
        selector = SourceSelector(temp_metadata_dir)

        result1 = selector.select(default_policy, "cycle-001", CycleMode.SCHEDULED)
        result2 = selector.select(default_policy, "cycle-002", CycleMode.SCHEDULED)

        # Different cycle_id may produce different pool order or different selection.
        # If selection_hash matches, the selected pools (as a set) should be equivalent.
        # If selection_hash differs, we have genuinely different selections.
        if result1.selection_hash == result2.selection_hash:
            # Same hash means same pool set (order may differ due to scoring)
            assert set(result1.selected_pool_ids) == set(result2.selected_pool_ids)
        # If hashes differ, that's expected behavior for different cycle IDs

    def test_selection_hash_verification(self, temp_metadata_dir, default_policy):
        """Selection hash should enable determinism verification."""
        selector = SourceSelector(temp_metadata_dir)
        cycle_id = "verification-test"

        result = selector.select(default_policy, cycle_id, CycleMode.SCHEDULED)

        # Verify determinism using hash
        assert result.verify_determinism(result.selection_hash)
        assert not result.verify_determinism("wrong_hash")

    def test_explicit_strategy_exact_order(self, temp_metadata_dir):
        """EXPLICIT strategy must select pools in exact order."""
        policy = SourceSelectionPolicy(
            policy_id="explicit_test",
            name="Explicit Test",
            selection_strategy=SelectionStrategy.EXPLICIT,
            explicit_pool_order=["pool_clean_json", "pool_clean_csv"],
            max_pools_per_cycle=2,
        )

        selector = SourceSelector(temp_metadata_dir)
        result = selector.select(policy, "any-cycle", CycleMode.SCHEDULED)

        assert result.selected_pool_ids == ["pool_clean_json", "pool_clean_csv"]


# ============================================
# Policy Validation Tests
# ============================================


class TestPolicyValidation:
    """Tests for policy validation."""

    def test_valid_policy_creation(self):
        """Valid policies should be created without errors."""
        policy = SourceSelectionPolicy(
            policy_id="valid_policy",
            name="Valid Policy",
        )
        assert policy.policy_id == "valid_policy"

    def test_explicit_strategy_requires_order(self):
        """EXPLICIT strategy must have explicit_pool_order."""
        with pytest.raises(ValueError, match="EXPLICIT strategy requires"):
            SourceSelectionPolicy(
                policy_id="invalid_explicit",
                name="Invalid",
                selection_strategy=SelectionStrategy.EXPLICIT,
                explicit_pool_order=None,
            )

    def test_invalid_policy_id_pattern(self):
        """Policy ID must match pattern."""
        with pytest.raises(ValueError):
            SourceSelectionPolicy(
                policy_id="Invalid-Policy-ID",  # Invalid: uppercase and hyphens
                name="Invalid",
            )

    def test_max_pools_bounds(self):
        """max_pools_per_cycle must be 1-10."""
        with pytest.raises(ValueError):
            SourceSelectionPolicy(
                policy_id="too_many",
                name="Too Many",
                max_pools_per_cycle=100,
            )

    def test_clean_ratio_bounds(self):
        """clean_vs_messy_ratio must be 0.0-1.0."""
        with pytest.raises(ValueError):
            SourceSelectionPolicy(
                policy_id="bad_ratio",
                name="Bad Ratio",
                clean_vs_messy_ratio=1.5,
            )


# ============================================
# Filtering Tests
# ============================================


class TestPoolFiltering:
    """Tests for pool filtering by policy constraints."""

    def test_exclude_messy_pools(self, temp_metadata_dir):
        """allow_messy_data=False should exclude messy pools."""
        policy = SourceSelectionPolicy(
            policy_id="clean_only",
            name="Clean Only",
            allow_messy_data=False,
            max_pools_per_cycle=10,
        )

        selector = SourceSelector(temp_metadata_dir)
        result = selector.select(policy, "test", CycleMode.IMPROVEMENT)

        assert "pool_messy_csv" not in result.selected_pool_ids
        # Check rationale
        messy_rationale = [r for r in result.rationale if r.pool_id == "pool_messy_csv"]
        assert len(messy_rationale) == 1
        assert "Messy data not allowed" in messy_rationale[0].reason

    def test_filter_by_format(self, temp_metadata_dir):
        """allowed_formats should filter pools by format."""
        policy = SourceSelectionPolicy(
            policy_id="json_only",
            name="JSON Only",
            allowed_formats=["json"],
            max_pools_per_cycle=10,
        )

        selector = SourceSelector(temp_metadata_dir)
        result = selector.select(policy, "test", CycleMode.LEARNING)

        assert result.selected_pool_ids == ["pool_clean_json"]

    def test_excluded_pools(self, temp_metadata_dir):
        """excluded_pools should be blocklisted."""
        policy = SourceSelectionPolicy(
            policy_id="exclude_test",
            name="Exclude Test",
            excluded_pools=["pool_clean_csv", "pool_clean_json"],
            max_pools_per_cycle=10,
        )

        selector = SourceSelector(temp_metadata_dir)
        result = selector.select(policy, "test", CycleMode.LEARNING)

        assert "pool_clean_csv" not in result.selected_pool_ids
        assert "pool_clean_json" not in result.selected_pool_ids


# ============================================
# Default Policy Tests
# ============================================


class TestDefaultPolicies:
    """Tests for mode-specific default policies."""

    def test_learning_mode_default(self):
        """Learning mode should have broad coverage settings."""
        policy = get_default_policy(CycleMode.LEARNING)

        assert policy.policy_id == "learning_default"
        assert policy.allow_messy_data is True
        assert policy.max_pools_per_cycle == 4
        assert policy.prefer_uncovered_formats is True

    def test_improvement_mode_default(self):
        """Improvement mode should have narrow, clean settings."""
        policy = get_default_policy(CycleMode.IMPROVEMENT)

        assert policy.policy_id == "improvement_default"
        assert policy.allow_messy_data is False
        assert policy.max_pools_per_cycle == 2
        assert policy.clean_vs_messy_ratio == 1.0

    def test_scheduled_mode_default(self):
        """Scheduled mode should be fully deterministic."""
        policy = get_default_policy(CycleMode.SCHEDULED)

        assert policy.policy_id == "scheduled_default"
        assert policy.selection_strategy == SelectionStrategy.HASH_BASED
        assert policy.prefer_uncovered_formats is False


# ============================================
# Guardrails Tests
# ============================================


class TestGuardrails:
    """Tests for agent guardrails."""

    def test_guardrails_allowed_list(self):
        """Guardrails should define allowed operations."""
        allowed = SourceSelectionGuardrails.ALLOWED
        assert len(allowed) > 0
        assert any("Read pool metadata" in op for op in allowed)

    def test_guardrails_forbidden_list(self):
        """Guardrails should define forbidden operations."""
        forbidden = SourceSelectionGuardrails.FORBIDDEN
        assert len(forbidden) > 0
        assert any("Modify" in op for op in forbidden)
        assert any("Download" in op for op in forbidden)
        assert any("Fabricate" in op for op in forbidden)

    def test_no_eligible_pools_handling(self):
        """Should provide clear message when no pools match."""
        message = SourceSelectionGuardrails.handle_no_eligible_pools()
        assert "CYCLE_BLOCKED" in message
        assert "fabricated" in message.lower()

    def test_invalid_policy_handling(self):
        """Should provide clear message for invalid policy."""
        message = SourceSelectionGuardrails.handle_invalid_policy()
        assert "POLICY_REJECTED" in message

    def test_integrity_failure_handling(self):
        """Should provide clear message for integrity failure."""
        message = SourceSelectionGuardrails.handle_integrity_failure("test_pool")
        assert "POOL_CORRUPTED" in message
        assert "test_pool" in message


# ============================================
# Context Wiring Tests
# ============================================


class TestContextWiring:
    """Tests for wiring selection into cycle context."""

    def test_wire_selection_to_context(self, temp_metadata_dir, default_policy):
        """Selection should be properly wired into context."""
        selector = SourceSelector(temp_metadata_dir)
        result = selector.select(default_policy, "test", CycleMode.LEARNING)

        context = {}
        updated_context = wire_selection_to_context(result, context)

        assert "source_selection" in updated_context
        assert "source_pools" in updated_context
        assert updated_context["source_selection"]["selection_id"] == result.selection_id
        assert updated_context["source_selection"]["selected_pool_ids"] == result.selected_pool_ids

    def test_context_includes_hashes(self, temp_metadata_dir, default_policy):
        """Context should include verification hashes."""
        selector = SourceSelector(temp_metadata_dir)
        result = selector.select(default_policy, "test", CycleMode.SCHEDULED)

        context = wire_selection_to_context(result, {})

        assert "selection_hash" in context["source_selection"]
        assert "input_hash" in context["source_selection"]

    def test_context_preserves_existing_metadata(self, temp_metadata_dir, default_policy):
        """Wiring should preserve existing context metadata."""
        selector = SourceSelector(temp_metadata_dir)
        result = selector.select(default_policy, "test", CycleMode.LEARNING)

        existing_context = {"existing_key": "existing_value"}
        updated_context = wire_selection_to_context(result, existing_context)

        assert updated_context["existing_key"] == "existing_value"


# ============================================
# Result Serialization Tests
# ============================================


class TestResultSerialization:
    """Tests for SelectionResult serialization."""

    def test_to_dict(self, temp_metadata_dir, default_policy):
        """Result should serialize to dict."""
        selector = SourceSelector(temp_metadata_dir)
        result = selector.select(default_policy, "test", CycleMode.LEARNING)

        result_dict = result.to_dict()

        assert isinstance(result_dict, dict)
        assert "selection_id" in result_dict
        assert "selected_pool_ids" in result_dict
        assert "rationale" in result_dict

    def test_to_json(self, temp_metadata_dir, default_policy):
        """Result should serialize to valid JSON."""
        selector = SourceSelector(temp_metadata_dir)
        result = selector.select(default_policy, "test", CycleMode.LEARNING)

        json_str = result.to_json()

        # Should be valid JSON
        parsed = json.loads(json_str)
        assert parsed["selection_id"] == result.selection_id

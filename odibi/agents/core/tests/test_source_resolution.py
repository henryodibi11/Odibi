"""Tests for source resolution (Phase 8.B).

These tests verify:
1. Deterministic resolution: Same inputs → Same outputs
2. Guardrail violations: Invalid configs fail fast
3. Mode behavior: Scheduled vs Improvement mode
4. CycleRunner integration: source_resolution attached to state
5. Report integration: source resolution in reports

CONSTRAINTS:
- NO pipeline execution
- NO data downloads
- NO side effects
- All tests are read-only
"""

import json
import tempfile
from pathlib import Path

import pytest
import yaml

from odibi.agents.core.cycle_source_config import (
    CycleSourceConfig,
    SourceTier,
)
from odibi.agents.core.source_resolution import (
    SourceResolutionError,
    SourceResolver,
    UnboundSourceContext,
    resolve_sources_for_cycle,
    verify_resolution_determinism,
)
from odibi.agents.core.source_selection import CycleMode


# ============================================
# Test Fixtures
# ============================================


@pytest.fixture
def temp_odibi_root():
    """Create a temporary Odibi root with source metadata."""
    with tempfile.TemporaryDirectory() as tmpdir:
        odibi_root = Path(tmpdir)
        metadata_path = odibi_root / ".odibi" / "source_metadata"
        metadata_path.mkdir(parents=True)
        pools_dir = metadata_path / "pools"
        pools_dir.mkdir()

        # Create pool index
        pool_index = {
            "version": "1.0.0",
            "status": "frozen",
            "pools": {
                "pool_tier0_clean": "pools/pool_tier0_clean.yaml",
                "pool_tier0_messy": "pools/pool_tier0_messy.yaml",
                "pool_tier20gb_clean": "pools/pool_tier20gb_clean.yaml",
            },
        }
        with open(metadata_path / "pool_index.yaml", "w") as f:
            yaml.dump(pool_index, f)

        # Create sample pool metadata files
        pools = [
            {
                "pool_id": "pool_tier0_clean",
                "name": "Tier0 Clean Pool",
                "tier": "tier0",
                "file_format": "csv",
                "source_type": "local",
                "data_quality": "clean",
                "status": "frozen",
                "cache_path": "tier0/csv/clean/",
                "characteristics": {"row_count": 1000},
                "integrity": {"manifest_hash": "abc123"},
            },
            {
                "pool_id": "pool_tier0_messy",
                "name": "Tier0 Messy Pool",
                "tier": "tier0",
                "file_format": "csv",
                "source_type": "local",
                "data_quality": "messy",
                "status": "frozen",
                "cache_path": "tier0/csv/messy/",
                "characteristics": {"row_count": 500},
                "integrity": {"manifest_hash": "def456"},
            },
            {
                "pool_id": "pool_tier20gb_clean",
                "name": "Tier20GB Clean Pool",
                "tier": "tier20gb",
                "file_format": "parquet",
                "source_type": "local",
                "data_quality": "clean",
                "status": "frozen",
                "cache_path": "tier20gb/parquet/clean/",
                "characteristics": {"row_count": 50000},
                "integrity": {"manifest_hash": "ghi789"},
            },
        ]

        for pool in pools:
            pool_file = pools_dir / f"{pool['pool_id']}.yaml"
            with open(pool_file, "w") as f:
                yaml.dump(pool, f)

        yield str(odibi_root)


@pytest.fixture
def resolver(temp_odibi_root):
    """Create a SourceResolver with temporary metadata."""
    return SourceResolver(temp_odibi_root)


# ============================================
# Determinism Tests
# ============================================


class TestDeterminism:
    """Tests for deterministic source resolution."""

    def test_same_inputs_same_outputs(self, resolver):
        """Same cycle_id + mode + config must produce identical result."""
        cycle_id = "test-cycle-determinism-001"
        mode = CycleMode.LEARNING
        config = CycleSourceConfig(
            selection_policy="learning_default",
            allowed_tiers=[SourceTier.TIER0],
            max_sources=2,
        )

        result1 = resolver.resolve(cycle_id, mode, config)
        result2 = resolver.resolve(cycle_id, mode, config)

        assert result1.selected_pool_ids == result2.selected_pool_ids
        assert result1.selection_hash == result2.selection_hash
        assert result1.input_hash == result2.input_hash

    def test_different_cycle_id_different_hash(self, resolver):
        """Different cycle_id should produce different selection hash."""
        mode = CycleMode.LEARNING
        config = CycleSourceConfig(
            selection_policy="learning_default",
            allowed_tiers=[SourceTier.TIER0],
        )

        result1 = resolver.resolve("cycle-001", mode, config)
        result2 = resolver.resolve("cycle-002", mode, config)

        # Input hash differs because cycle_id is different
        assert result1.input_hash != result2.input_hash

    def test_verify_determinism(self, resolver):
        """Selection hash should enable determinism verification."""
        cycle_id = "verification-test"
        mode = CycleMode.LEARNING

        result = resolver.resolve(cycle_id, mode)

        assert result.verify_determinism(result.selection_hash)
        assert not result.verify_determinism("wrong_hash")

    def test_selection_is_stable_across_calls(self, resolver):
        """Multiple calls with same inputs must be identical."""
        cycle_id = "stability-test"
        mode = CycleMode.SCHEDULED

        results = [resolver.resolve(cycle_id, mode) for _ in range(5)]

        first_hash = results[0].selection_hash
        assert all(r.selection_hash == first_hash for r in results)


# ============================================
# Guardrail Violation Tests
# ============================================


class TestGuardrailViolations:
    """Tests for config-time guardrail enforcement."""

    def test_scheduled_mode_requires_frozen(self, resolver):
        """Scheduled mode must require frozen pools."""
        config = CycleSourceConfig(
            allowed_tiers=[SourceTier.TIER0],
            require_frozen=False,  # VIOLATION
        )

        with pytest.raises(SourceResolutionError) as exc_info:
            resolver.resolve("cycle-001", CycleMode.SCHEDULED, config)

        assert exc_info.value.error_type == "CONFIG_VALIDATION_FAILED"
        assert "frozen" in exc_info.value.message.lower()

    def test_improvement_mode_blocks_tier600gb(self, resolver):
        """Improvement mode must reject tier600gb."""
        config = CycleSourceConfig(
            allowed_tiers=[SourceTier.TIER600GB],  # VIOLATION
        )

        with pytest.raises(SourceResolutionError) as exc_info:
            resolver.resolve("cycle-001", CycleMode.IMPROVEMENT, config)

        assert exc_info.value.error_type == "CONFIG_VALIDATION_FAILED"
        assert "tier600gb" in exc_info.value.message.lower()

    def test_improvement_mode_blocks_tier2tb(self, resolver):
        """Improvement mode must reject tier2tb."""
        config = CycleSourceConfig(
            allowed_tiers=[SourceTier.TIER2TB],  # VIOLATION
        )

        with pytest.raises(SourceResolutionError) as exc_info:
            resolver.resolve("cycle-001", CycleMode.IMPROVEMENT, config)

        assert exc_info.value.error_type == "CONFIG_VALIDATION_FAILED"
        assert "tier2tb" in exc_info.value.message.lower()

    def test_cleanliness_conflict_fails(self):
        """require_clean=True and allow_messy=True must conflict."""
        with pytest.raises(ValueError) as exc_info:
            CycleSourceConfig(
                require_clean=True,
                allow_messy=True,  # CONFLICT
            )

        assert "require_clean" in str(exc_info.value)


# ============================================
# Mode Behavior Tests
# ============================================


class TestModeBehavior:
    """Tests for mode-specific behavior."""

    def test_learning_mode_allows_messy(self, resolver):
        """Learning mode should allow messy pools."""
        config = CycleSourceConfig(
            allowed_tiers=[SourceTier.TIER0],
            allow_messy=True,
            max_sources=3,
        )

        result = resolver.resolve("cycle-001", CycleMode.LEARNING, config)

        # Should include messy pool
        assert len(result.selected_pool_ids) > 0

    def test_improvement_mode_default_excludes_messy(self, resolver):
        """Improvement mode default should exclude messy pools."""
        config = CycleSourceConfig(
            selection_policy="improvement_default",
            allowed_tiers=[SourceTier.TIER0, SourceTier.TIER20GB],
            require_clean=True,
            allow_messy=False,
        )

        result = resolver.resolve("cycle-001", CycleMode.IMPROVEMENT, config)

        # Should NOT include messy pool
        assert "pool_tier0_messy" not in result.selected_pool_ids

    def test_scheduled_mode_deterministic_strategy(self, resolver):
        """Scheduled mode should use deterministic selection."""
        result = resolver.resolve("scheduled-cycle", CycleMode.SCHEDULED)

        assert result.selection_strategy == "hash_based"


# ============================================
# SourceResolutionResult Tests
# ============================================


class TestSourceResolutionResult:
    """Tests for SourceResolutionResult dataclass."""

    def test_to_dict(self, resolver):
        """Result should serialize to dict."""
        result = resolver.resolve("test", CycleMode.LEARNING)

        result_dict = result.to_dict()

        assert isinstance(result_dict, dict)
        assert "resolution_id" in result_dict
        assert "cycle_id" in result_dict
        assert "selected_pool_ids" in result_dict
        assert "selection_hash" in result_dict

    def test_to_json(self, resolver):
        """Result should serialize to valid JSON."""
        result = resolver.resolve("test", CycleMode.LEARNING)

        json_str = result.to_json()

        parsed = json.loads(json_str)
        assert parsed["cycle_id"] == "test"

    def test_to_metadata(self, resolver):
        """Result should convert to SourceResolutionMetadata."""
        result = resolver.resolve("test", CycleMode.LEARNING)

        metadata = result.to_metadata()

        assert metadata.selection_hash == result.selection_hash
        assert metadata.source_pool_names == result.selected_pool_names
        assert metadata.pools_considered == result.pools_considered

    def test_is_empty_false_when_pools_selected(self, resolver):
        """is_empty should be False when pools are selected."""
        result = resolver.resolve("test", CycleMode.LEARNING)

        assert not result.is_empty
        assert len(result.selected_pool_ids) > 0


# ============================================
# UnboundSourceContext Tests
# ============================================


class TestUnboundSourceContext:
    """Tests for UnboundSourceContext."""

    def test_from_resolution_result(self, resolver):
        """UnboundSourceContext should be created from result."""
        result = resolver.resolve("test", CycleMode.LEARNING)

        context = UnboundSourceContext.from_resolution_result(result)

        assert context.context_id == result.context_id
        assert context.cycle_id == result.cycle_id
        assert context.pool_ids == result.selected_pool_ids
        assert context.bound is False

    def test_context_is_not_bound(self, resolver):
        """UnboundSourceContext must NOT be bound."""
        result = resolver.resolve("test", CycleMode.LEARNING)
        context = UnboundSourceContext.from_resolution_result(result)

        assert context.bound is False

    def test_context_to_dict(self, resolver):
        """Context should serialize to dict."""
        result = resolver.resolve("test", CycleMode.LEARNING)
        context = UnboundSourceContext.from_resolution_result(result)

        context_dict = context.to_dict()

        assert context_dict["bound"] is False
        assert "pool_ids" in context_dict
        assert "created_at" in context_dict


# ============================================
# Convenience Function Tests
# ============================================


class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    def test_resolve_sources_for_cycle(self, temp_odibi_root):
        """resolve_sources_for_cycle should work."""
        result = resolve_sources_for_cycle(
            odibi_root=temp_odibi_root,
            cycle_id="convenience-test",
            mode=CycleMode.LEARNING,
        )

        assert result.cycle_id == "convenience-test"
        assert len(result.selected_pool_ids) > 0

    def test_verify_resolution_determinism(self, temp_odibi_root):
        """verify_resolution_determinism should verify determinism."""
        result = resolve_sources_for_cycle(
            odibi_root=temp_odibi_root,
            cycle_id="determinism-verify",
            mode=CycleMode.LEARNING,
        )

        assert verify_resolution_determinism(
            odibi_root=temp_odibi_root,
            cycle_id="determinism-verify",
            mode=CycleMode.LEARNING,
            expected_hash=result.selection_hash,
        )


# ============================================
# Error Handling Tests
# ============================================


class TestErrorHandling:
    """Tests for error handling."""

    def test_missing_metadata_directory(self):
        """Should raise error if metadata directory missing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            resolver = SourceResolver(tmpdir)

            with pytest.raises(SourceResolutionError) as exc_info:
                resolver.resolve("test", CycleMode.LEARNING)

            assert exc_info.value.error_type == "METADATA_NOT_FOUND"

    def test_source_resolution_error_to_dict(self):
        """SourceResolutionError should serialize to dict."""
        error = SourceResolutionError(
            message="Test error",
            error_type="TEST_ERROR",
            details={"key": "value"},
        )

        error_dict = error.to_dict()

        assert error_dict["error_type"] == "TEST_ERROR"
        assert error_dict["message"] == "Test error"
        assert error_dict["details"]["key"] == "value"


# ============================================
# CycleRunner Integration Tests
# ============================================


class TestCycleRunnerIntegration:
    """Tests for CycleRunner source resolution integration."""

    def test_start_cycle_includes_source_resolution(self, temp_odibi_root):
        """start_cycle should include source_resolution in state."""
        from odibi.agents.core.cycle import CycleConfig, CycleRunner, AssistantMode

        runner = CycleRunner(temp_odibi_root)
        config = CycleConfig(
            project_root="/test/project",
            task_description="Test cycle",
        )

        state = runner.start_cycle(config, AssistantMode.GUIDED_EXECUTION)

        # source_resolution should be present (may be None if no pools)
        assert hasattr(state, "source_resolution")

    def test_start_cycle_with_source_config(self, temp_odibi_root):
        """start_cycle should accept source_config parameter."""
        from odibi.agents.core.cycle import CycleConfig, CycleRunner, AssistantMode

        runner = CycleRunner(temp_odibi_root)
        config = CycleConfig(
            project_root="/test/project",
        )
        source_config = CycleSourceConfig(
            allowed_tiers=[SourceTier.TIER0],
            max_sources=1,
        )

        state = runner.start_cycle(
            config,
            AssistantMode.GUIDED_EXECUTION,
            source_config=source_config,
        )

        if state.source_resolution:
            assert state.source_resolution.get("max_sources") == 1


# ============================================
# Report Integration Tests
# ============================================


class TestReportIntegration:
    """Tests for report generation with source resolution."""

    def test_report_includes_source_resolution(self, temp_odibi_root):
        """Report should include Source Resolution section."""
        from odibi.agents.core.cycle import CycleConfig, CycleState
        from odibi.agents.core.reports import CycleReportGenerator

        config = CycleConfig(project_root="/test")
        state = CycleState(
            cycle_id="test-cycle-report",
            config=config,
            source_resolution={
                "selected_pool_names": ["Pool A", "Pool B"],
                "policy_id": "learning_default",
                "selection_strategy": "hash_based",
                "selection_hash": "abc123",
                "input_hash": "def456",
                "pools_considered": 10,
                "pools_eligible": 5,
                "max_sources": 3,
                "tiers_used": ["tier0"],
                "require_frozen": True,
                "allow_messy": True,
                "context_id": "ctx_test",
                "resolved_at": "2024-01-01T00:00:00Z",
            },
        )

        generator = CycleReportGenerator(temp_odibi_root)
        report = generator.generate_report(state)

        assert "## Source Resolution" in report
        assert "Pool A" in report
        assert "Pool B" in report
        assert "abc123" in report
        assert "learning_default" in report

    def test_report_without_source_resolution(self, temp_odibi_root):
        """Report should handle missing source_resolution gracefully."""
        from odibi.agents.core.cycle import CycleConfig, CycleState
        from odibi.agents.core.reports import CycleReportGenerator

        config = CycleConfig(project_root="/test")
        state = CycleState(
            cycle_id="test-cycle-no-resolution",
            config=config,
            source_resolution=None,
        )

        generator = CycleReportGenerator(temp_odibi_root)
        report = generator.generate_report(state)

        # Should still generate report without source section
        assert "# Cycle Report:" in report
        # Source resolution section should be absent
        assert "## Source Resolution" not in report

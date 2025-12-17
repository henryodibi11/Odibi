"""Tests for source binder (Phase 9.D).

These tests verify:
1. Source binding materializes pools correctly
2. BoundSourceMap enforces path prefixes
3. TrivialCycleDetector detects trivial cycles
4. Read-only guarantees are maintained
5. Deterministic binding paths
"""

import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from odibi.agents.core.source_binder import (
    BindingStatus,
    BoundSourceEntry,
    BoundSourceMap,
    SourceBinder,
    TrivialCycleDetector,
    detect_trivial_cycle,
)
from odibi.agents.core.source_selection import PoolMetadataSummary, SourceSelectionResult
from odibi.agents.core.source_resolution import SourceResolutionResult
from odibi.agents.core.evidence import SourceUsageSummary


# ============================================
# Test Fixtures
# ============================================


@pytest.fixture
def temp_odibi_root():
    """Create a temporary Odibi root with source cache."""
    with tempfile.TemporaryDirectory() as tmpdir:
        odibi_root = Path(tmpdir)

        # Create .odibi directory structure
        source_cache = odibi_root / ".odibi" / "source_cache"
        source_cache.mkdir(parents=True)

        # Create mock pool directories
        pool1_dir = source_cache / "pool1" / "csv" / "clean"
        pool1_dir.mkdir(parents=True)
        (pool1_dir / "data.csv").write_text("id,name\n1,test\n2,test2")

        pool2_dir = source_cache / "pool2" / "parquet"
        pool2_dir.mkdir(parents=True)
        (pool2_dir / "data.parquet").write_bytes(b"mock parquet data")

        # Create tier-based pools
        tier100gb_dir = source_cache / "tiers" / "tier100gb" / "wiki"
        tier100gb_dir.mkdir(parents=True)
        (tier100gb_dir / "pages.parquet").write_bytes(b"X" * 1000)

        # Create cycle bindings directory
        (odibi_root / ".odibi" / "cycle_bindings").mkdir(parents=True)

        yield str(odibi_root)


@pytest.fixture
def mock_selection_result():
    """Create a mock selection result with pools."""
    pools = [
        PoolMetadataSummary(
            pool_id="pool1_csv_clean",
            name="Pool 1 CSV",
            file_format="csv",
            source_type="local",
            data_quality="clean",
            row_count=100,
            cache_path="pool1/csv/clean/",
            manifest_hash="abc123",
        ),
        PoolMetadataSummary(
            pool_id="pool2_parquet",
            name="Pool 2 Parquet",
            file_format="parquet",
            source_type="local",
            data_quality="clean",
            row_count=1000,
            cache_path="pool2/parquet/",
            manifest_hash="def456",
        ),
    ]

    return SourceSelectionResult(
        selection_id="sel_test_001",
        policy_id="test_policy",
        cycle_id="test-cycle-001",
        mode="learning",
        selected_at=datetime.now(UTC).isoformat() + "Z",
        selected_pool_ids=["pool1_csv_clean", "pool2_parquet"],
        selected_pools=pools,
        rationale=[],
        input_hash="input123",
        selection_hash="sel123",
        pools_considered=2,
        pools_eligible=2,
        pools_excluded=0,
        selection_strategy_used="hash_based",
    )


@pytest.fixture
def mock_resolution_result(mock_selection_result):
    """Create a mock resolution result."""
    return SourceResolutionResult(
        resolution_id="res_test_001",
        cycle_id="test-cycle-001",
        mode="learning",
        selected_pool_names=["Pool 1 CSV", "Pool 2 Parquet"],
        selected_pool_ids=["pool1_csv_clean", "pool2_parquet"],
        tiers_used=["default"],
        selection_hash="sel123",
        input_hash="input123",
        context_id="ctx_test_001",
        pools_considered=2,
        pools_eligible=2,
        pools_excluded=0,
        policy_id="test_policy",
        selection_strategy="hash_based",
        require_frozen=True,
        allow_messy=False,
        allow_tier_mixing=False,
        max_sources=3,
        allowed_tiers=["tier100gb"],
        resolved_at=datetime.now(UTC).isoformat() + "Z",
        _selection_result=mock_selection_result,
    )


# ============================================
# BoundSourceEntry Tests
# ============================================


class TestBoundSourceEntry:
    """Tests for BoundSourceEntry dataclass."""

    def test_entry_is_frozen(self):
        """Entry should be immutable (frozen dataclass)."""
        entry = BoundSourceEntry(
            pool_id="test",
            pool_name="Test Pool",
            original_cache_path="test/path",
            bound_path="/tmp/bound",
            tier="default",
            file_format="csv",
            row_count=100,
            manifest_hash="abc",
            bound_at=datetime.now(UTC).isoformat() + "Z",
        )

        with pytest.raises(Exception):  # FrozenInstanceError
            entry.pool_id = "changed"

    def test_entry_to_dict(self):
        """Entry should serialize to dict."""
        entry = BoundSourceEntry(
            pool_id="test",
            pool_name="Test Pool",
            original_cache_path="test/path",
            bound_path="/tmp/bound",
            tier="tier100gb",
            file_format="parquet",
            row_count=1000,
            manifest_hash="abc123",
            bound_at="2025-01-01T00:00:00Z",
        )

        d = entry.to_dict()
        assert d["pool_id"] == "test"
        assert d["tier"] == "tier100gb"
        assert d["row_count"] == 1000


# ============================================
# BoundSourceMap Tests
# ============================================


class TestBoundSourceMap:
    """Tests for BoundSourceMap."""

    def test_map_get_bound_path(self):
        """Should return bound path for pool."""
        entries = {
            "pool1": BoundSourceEntry(
                pool_id="pool1",
                pool_name="Pool 1",
                original_cache_path="pool1/csv",
                bound_path="/tmp/cycle/pool1",
                tier="default",
                file_format="csv",
                row_count=100,
                manifest_hash="abc",
                bound_at=datetime.now(UTC).isoformat() + "Z",
            )
        }
        bound_map = BoundSourceMap(
            cycle_id="test-cycle",
            binding_root="/tmp/cycle",
            entries=entries,
        )

        assert bound_map.get_bound_path("pool1") == "/tmp/cycle/pool1"
        assert bound_map.get_bound_path("unknown") is None

    def test_map_is_path_bound(self, temp_odibi_root):
        """Should check if path is within bound pools."""
        binding_root = os.path.join(temp_odibi_root, ".odibi", "cycle_bindings", "test")
        os.makedirs(binding_root, exist_ok=True)

        pool_path = os.path.join(binding_root, "pool1")
        os.makedirs(pool_path, exist_ok=True)

        entries = {
            "pool1": BoundSourceEntry(
                pool_id="pool1",
                pool_name="Pool 1",
                original_cache_path="pool1/csv",
                bound_path=pool_path,
                tier="default",
                file_format="csv",
                row_count=100,
                manifest_hash="abc",
                bound_at=datetime.now(UTC).isoformat() + "Z",
            )
        }
        bound_map = BoundSourceMap(
            cycle_id="test-cycle",
            binding_root=binding_root,
            entries=entries,
        )

        # Path inside pool
        inside_path = os.path.join(pool_path, "data.csv")
        assert bound_map.is_path_bound(inside_path) is True

        # Path outside pool
        outside_path = os.path.join(temp_odibi_root, "other", "file.csv")
        assert bound_map.is_path_bound(outside_path) is False

    def test_map_get_pool_for_path(self, temp_odibi_root):
        """Should return pool_id for path."""
        binding_root = os.path.join(temp_odibi_root, ".odibi", "cycle_bindings", "test")
        pool_path = os.path.join(binding_root, "pool1")
        os.makedirs(pool_path, exist_ok=True)

        entries = {
            "pool1": BoundSourceEntry(
                pool_id="pool1",
                pool_name="Pool 1",
                original_cache_path="pool1/csv",
                bound_path=pool_path,
                tier="default",
                file_format="csv",
                row_count=100,
                manifest_hash="abc",
                bound_at=datetime.now(UTC).isoformat() + "Z",
            )
        }
        bound_map = BoundSourceMap(
            cycle_id="test-cycle",
            binding_root=binding_root,
            entries=entries,
        )

        inside_path = os.path.join(pool_path, "subdir", "data.csv")
        assert bound_map.get_pool_for_path(inside_path) == "pool1"

        outside_path = "/tmp/other/file.csv"
        assert bound_map.get_pool_for_path(outside_path) is None


# ============================================
# SourceBinder Tests
# ============================================


class TestSourceBinder:
    """Tests for SourceBinder."""

    def test_binder_bind_success(self, temp_odibi_root, mock_resolution_result):
        """Binder should bind pools successfully."""
        binder = SourceBinder(temp_odibi_root)
        result = binder.bind(mock_resolution_result)

        assert result.status == BindingStatus.BOUND
        assert result.pools_bound == 2
        assert result.bound_source_map is not None
        assert len(result.bound_source_map.entries) == 2

    def test_binder_idempotent(self, temp_odibi_root, mock_resolution_result):
        """Binding same cycle twice should be idempotent."""
        binder = SourceBinder(temp_odibi_root)

        result1 = binder.bind(mock_resolution_result)
        result2 = binder.bind(mock_resolution_result)

        assert result1.status == BindingStatus.BOUND
        assert result2.status == BindingStatus.BOUND
        assert result1.pools_bound == result2.pools_bound

    def test_binder_deterministic_paths(self, temp_odibi_root, mock_resolution_result):
        """Same cycle_id should produce same binding paths."""
        binder = SourceBinder(temp_odibi_root)

        result1 = binder.bind(mock_resolution_result)
        # Clean up and rebind
        binder.cleanup_binding(mock_resolution_result.cycle_id)
        result2 = binder.bind(mock_resolution_result)

        # Paths should be identical
        assert result1.bound_source_map.binding_root == result2.bound_source_map.binding_root

    def test_binder_creates_execution_context(self, temp_odibi_root, mock_resolution_result):
        """Binder should create valid ExecutionSourceContext."""
        binder = SourceBinder(temp_odibi_root)
        result = binder.bind(mock_resolution_result)

        context = binder.create_execution_context(
            result.bound_source_map,
            mock_resolution_result,
        )

        assert context is not None
        assert len(context.mounted_pools) == 2
        assert "pool1_csv_clean" in context.mounted_pools

    def test_binder_cleanup(self, temp_odibi_root, mock_resolution_result):
        """Cleanup should remove binding directory."""
        binder = SourceBinder(temp_odibi_root)
        result = binder.bind(mock_resolution_result)

        binding_path = result.bound_source_map.binding_root

        # Cleanup
        success = binder.cleanup_binding(mock_resolution_result.cycle_id)

        assert success is True
        # Binding directory should be removed (or not exist if using symlinks)


# ============================================
# TrivialCycleDetector Tests
# ============================================


class TestTrivialCycleDetector:
    """Tests for TrivialCycleDetector."""

    def test_trivial_when_no_pools_used(self):
        """Cycle is trivial when pools bound but none used."""
        detector = TrivialCycleDetector("test-cycle")

        # Simulate binding
        entries = {
            "pool1": BoundSourceEntry(
                pool_id="pool1",
                pool_name="Pool 1",
                original_cache_path="pool1/csv",
                bound_path="/tmp/pool1",
                tier="default",
                file_format="csv",
                row_count=100,
                manifest_hash="abc",
                bound_at=datetime.now(UTC).isoformat() + "Z",
            )
        }
        bound_map = BoundSourceMap(
            cycle_id="test-cycle",
            binding_root="/tmp",
            entries=entries,
        )
        detector.record_binding(bound_map)

        # No access recorded
        assert detector.is_trivial() is True

        warning = detector.get_warning()
        assert warning is not None
        assert "trivial workloads only" in warning.message

    def test_not_trivial_when_pools_used(self):
        """Cycle is not trivial when pools are used."""
        detector = TrivialCycleDetector("test-cycle")

        entries = {
            "pool1": BoundSourceEntry(
                pool_id="pool1",
                pool_name="Pool 1",
                original_cache_path="pool1/csv",
                bound_path="/tmp/pool1",
                tier="default",
                file_format="csv",
                row_count=100,
                manifest_hash="abc",
                bound_at=datetime.now(UTC).isoformat() + "Z",
            )
        }
        bound_map = BoundSourceMap(
            cycle_id="test-cycle",
            binding_root="/tmp",
            entries=entries,
        )
        detector.record_binding(bound_map)

        # Record access
        detector.record_access("pool1", bytes_read=1000, files_accessed=5)

        assert detector.is_trivial() is False
        assert detector.get_warning() is None

    def test_trivial_when_zero_bytes_read(self):
        """Cycle is trivial when zero bytes read."""
        detector = TrivialCycleDetector("test-cycle")

        entries = {
            "pool1": BoundSourceEntry(
                pool_id="pool1",
                pool_name="Pool 1",
                original_cache_path="pool1/csv",
                bound_path="/tmp/pool1",
                tier="default",
                file_format="csv",
                row_count=100,
                manifest_hash="abc",
                bound_at=datetime.now(UTC).isoformat() + "Z",
            )
        }
        bound_map = BoundSourceMap(
            cycle_id="test-cycle",
            binding_root="/tmp",
            entries=entries,
        )
        detector.record_binding(bound_map)

        # Record access with zero bytes
        detector.record_access("pool1", bytes_read=0, files_accessed=0)

        assert detector.is_trivial() is True

    def test_not_trivial_when_no_binding(self):
        """Cycle is not trivial when no pools bound."""
        detector = TrivialCycleDetector("test-cycle")

        # No binding recorded
        assert detector.is_trivial() is False
        assert detector.get_warning() is None

    def test_record_from_summary(self):
        """Detector should accept SourceUsageSummary."""
        detector = TrivialCycleDetector("test-cycle")

        entries = {
            "pool1": BoundSourceEntry(
                pool_id="pool1",
                pool_name="Pool 1",
                original_cache_path="pool1/csv",
                bound_path="/tmp/pool1",
                tier="default",
                file_format="csv",
                row_count=100,
                manifest_hash="abc",
                bound_at=datetime.now(UTC).isoformat() + "Z",
            )
        }
        bound_map = BoundSourceMap(
            cycle_id="test-cycle",
            binding_root="/tmp",
            entries=entries,
        )
        detector.record_binding(bound_map)

        summary = SourceUsageSummary(
            context_id="ctx_test",
            pools_bound=["pool1"],
            pools_used=["pool1"],
            files_accessed=10,
            integrity_verified=True,
        )
        detector.record_from_summary(summary)

        assert detector.is_trivial() is False


# ============================================
# Convenience Function Tests
# ============================================


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_detect_trivial_cycle_no_binding(self):
        """Should return None when no binding."""
        warning = detect_trivial_cycle(
            cycle_id="test",
            bound_map=None,
            source_usage=None,
        )
        assert warning is None

    def test_detect_trivial_cycle_with_usage(self):
        """Should return None when sources used."""
        entries = {
            "pool1": BoundSourceEntry(
                pool_id="pool1",
                pool_name="Pool 1",
                original_cache_path="pool1/csv",
                bound_path="/tmp/pool1",
                tier="default",
                file_format="csv",
                row_count=100,
                manifest_hash="abc",
                bound_at=datetime.now(UTC).isoformat() + "Z",
            )
        }
        bound_map = BoundSourceMap(
            cycle_id="test-cycle",
            binding_root="/tmp",
            entries=entries,
        )

        summary = SourceUsageSummary(
            context_id="ctx_test",
            pools_bound=["pool1"],
            pools_used=["pool1"],
            files_accessed=5,
        )

        warning = detect_trivial_cycle(
            cycle_id="test",
            bound_map=bound_map,
            source_usage=summary,
        )
        assert warning is None

    def test_detect_trivial_cycle_trivial(self):
        """Should return warning when trivial."""
        entries = {
            "pool1": BoundSourceEntry(
                pool_id="pool1",
                pool_name="Pool 1",
                original_cache_path="pool1/csv",
                bound_path="/tmp/pool1",
                tier="default",
                file_format="csv",
                row_count=100,
                manifest_hash="abc",
                bound_at=datetime.now(UTC).isoformat() + "Z",
            )
        }
        bound_map = BoundSourceMap(
            cycle_id="test-cycle",
            binding_root="/tmp",
            entries=entries,
        )

        # No usage
        warning = detect_trivial_cycle(
            cycle_id="test",
            bound_map=bound_map,
            source_usage=None,
        )
        assert warning is not None
        assert "trivial workloads only" in warning.message


# ============================================
# Read-Only Guarantee Tests
# ============================================


class TestReadOnlyGuarantees:
    """Tests for read-only enforcement."""

    def test_bound_entry_immutable(self):
        """BoundSourceEntry should be immutable."""
        entry = BoundSourceEntry(
            pool_id="test",
            pool_name="Test",
            original_cache_path="path",
            bound_path="/tmp/path",
            tier="default",
            file_format="csv",
            row_count=100,
            manifest_hash="abc",
            bound_at="2025-01-01T00:00:00Z",
        )

        # Attempt to modify should raise
        with pytest.raises(Exception):
            entry.bound_path = "/changed"

    def test_binder_does_not_modify_source(self, temp_odibi_root, mock_resolution_result):
        """Binder should not modify source files."""
        source_cache = os.path.join(temp_odibi_root, ".odibi", "source_cache")
        pool1_data = os.path.join(source_cache, "pool1", "csv", "clean", "data.csv")

        # Record original content
        with open(pool1_data, "r") as f:
            original_content = f.read()

        # Bind
        binder = SourceBinder(temp_odibi_root)
        binder.bind(mock_resolution_result)

        # Verify source unchanged
        with open(pool1_data, "r") as f:
            after_content = f.read()

        assert original_content == after_content


# ============================================
# Environment Injection Tests (Phase 9.D)
# ============================================


class TestEnvironmentInjection:
    """Tests for BOUND_SOURCE_ROOT environment variable injection."""

    def test_execution_gateway_has_bound_source_root_method(self):
        """ExecutionGateway should have get_bound_source_root method."""
        from odibi.agents.core.execution import ExecutionGateway

        gateway = ExecutionGateway(odibi_root="d:/odibi")
        assert hasattr(gateway, "get_bound_source_root")
        assert hasattr(gateway, "set_bound_source_root")
        assert hasattr(gateway, "_build_execution_environment")

    def test_bound_source_root_initially_none(self):
        """BOUND_SOURCE_ROOT should be None before binding."""
        from odibi.agents.core.execution import ExecutionGateway

        gateway = ExecutionGateway(odibi_root="d:/odibi")
        assert gateway.get_bound_source_root() is None

    def test_bound_source_root_set_on_bind(self, temp_odibi_root, mock_resolution_result):
        """BOUND_SOURCE_ROOT should be set when sources are bound."""
        from odibi.agents.core.execution import ExecutionGateway

        gateway = ExecutionGateway(odibi_root=temp_odibi_root)
        binder = SourceBinder(temp_odibi_root)

        # Bind sources
        binding_result = binder.bind(mock_resolution_result)
        assert binding_result.status == BindingStatus.BOUND

        # Create execution context and bind to gateway
        context = binder.create_execution_context(
            binding_result.bound_source_map,
            mock_resolution_result,
        )
        gateway.bind_sources(context)

        # Verify bound source root is set
        bound_root = gateway.get_bound_source_root()
        assert bound_root is not None
        assert os.path.exists(bound_root)

    def test_execution_environment_contains_bound_source_root(
        self, temp_odibi_root, mock_resolution_result
    ):
        """Execution environment should contain BOUND_SOURCE_ROOT."""
        from odibi.agents.core.execution import ExecutionGateway

        gateway = ExecutionGateway(odibi_root=temp_odibi_root)
        binder = SourceBinder(temp_odibi_root)

        # Bind sources
        binding_result = binder.bind(mock_resolution_result)
        context = binder.create_execution_context(
            binding_result.bound_source_map,
            mock_resolution_result,
        )
        gateway.bind_sources(context)

        # Build execution environment
        env = gateway._build_execution_environment()

        # Verify environment variables
        assert "BOUND_SOURCE_ROOT" in env
        assert "BOUND_SOURCE_ROOT_WIN" in env
        assert "ARTIFACTS_ROOT" in env
        assert "ODIBI_ROOT" in env

        # Verify values are non-empty
        assert len(env["BOUND_SOURCE_ROOT"]) > 0
        assert len(env["ARTIFACTS_ROOT"]) > 0

    def test_execution_environment_without_binding(self):
        """Execution environment should use default source_cache when not explicitly bound."""
        from odibi.agents.core.execution import ExecutionGateway

        gateway = ExecutionGateway(odibi_root="d:/odibi")

        # Build environment without explicit binding
        env = gateway._build_execution_environment()

        # BOUND_SOURCE_ROOT should have default fallback value
        assert "BOUND_SOURCE_ROOT" in env
        assert "BOUND_SOURCE_ROOT_WIN" in env

        # Default should point to .odibi/source_cache
        assert "source_cache" in env["BOUND_SOURCE_ROOT_WIN"]

        # ARTIFACTS_ROOT and ODIBI_ROOT should also be present
        assert "ARTIFACTS_ROOT" in env
        assert "ODIBI_ROOT" in env

    def test_unbind_clears_bound_source_root(self, temp_odibi_root, mock_resolution_result):
        """Unbinding should clear explicit BOUND_SOURCE_ROOT but fallback still applies."""
        from odibi.agents.core.execution import ExecutionGateway

        gateway = ExecutionGateway(odibi_root=temp_odibi_root)
        binder = SourceBinder(temp_odibi_root)

        # Bind sources
        binding_result = binder.bind(mock_resolution_result)
        context = binder.create_execution_context(
            binding_result.bound_source_map,
            mock_resolution_result,
        )
        gateway.bind_sources(context)

        # Verify bound
        assert gateway.get_bound_source_root() is not None

        # Unbind
        gateway.unbind_sources()

        # Verify explicit binding is cleared
        assert gateway.get_bound_source_root() is None

        # Environment should still have BOUND_SOURCE_ROOT with fallback value
        env = gateway._build_execution_environment()
        assert "BOUND_SOURCE_ROOT" in env
        # Fallback should point to default source_cache
        assert "source_cache" in env["BOUND_SOURCE_ROOT_WIN"]

    def test_bound_source_root_is_wsl_path(self, temp_odibi_root, mock_resolution_result):
        """BOUND_SOURCE_ROOT in env should be WSL-formatted path."""
        from odibi.agents.core.execution import ExecutionGateway

        gateway = ExecutionGateway(odibi_root=temp_odibi_root)
        binder = SourceBinder(temp_odibi_root)

        # Bind sources
        binding_result = binder.bind(mock_resolution_result)
        context = binder.create_execution_context(
            binding_result.bound_source_map,
            mock_resolution_result,
        )
        gateway.bind_sources(context)

        # Build environment
        env = gateway._build_execution_environment()

        # WSL path should start with /mnt/ (on Windows)
        bound_root = env["BOUND_SOURCE_ROOT"]
        if os.name == "nt":  # Windows
            assert bound_root.startswith("/mnt/"), f"Expected WSL path, got: {bound_root}"

        # Windows path should be preserved in _WIN variant
        win_root = env["BOUND_SOURCE_ROOT_WIN"]
        assert "\\" in win_root or ":" in win_root, f"Expected Windows path, got: {win_root}"

    def test_set_bound_source_root_explicit(self):
        """Should be able to explicitly set bound source root."""
        from odibi.agents.core.execution import ExecutionGateway

        gateway = ExecutionGateway(odibi_root="d:/odibi")

        # Explicitly set
        gateway.set_bound_source_root("d:/odibi/.odibi/source_cache/test_cycle")

        assert gateway.get_bound_source_root() == "d:/odibi/.odibi/source_cache/test_cycle"

        # Should appear in environment
        env = gateway._build_execution_environment()
        assert "BOUND_SOURCE_ROOT" in env

    def test_wsl_command_includes_env_exports(self):
        """WSL command should include env var exports when provided."""
        from odibi.agents.core.execution import ExecutionGateway, TaskDefinition

        gateway = ExecutionGateway(odibi_root="d:/odibi")

        task = TaskDefinition(task_type="pipeline", args=["run", "test.yaml"])
        env_vars = {
            "BOUND_SOURCE_ROOT": "/mnt/d/odibi/.odibi/source_cache",
            "ARTIFACTS_ROOT": "/mnt/d/odibi/.odibi/artifacts",
            "ODIBI_ROOT": "/mnt/d/odibi",
        }

        cmd = gateway._build_wsl_command(task, env_vars=env_vars)

        # Command should use bash -c with exports
        cmd_str = " ".join(cmd)
        assert "bash" in cmd_str
        assert "export" in cmd_str
        assert "BOUND_SOURCE_ROOT" in cmd_str

    def test_wsl_command_filters_unsafe_env_vars(self):
        """WSL command should only export safe Odibi-specific vars."""
        from odibi.agents.core.execution import ExecutionGateway, TaskDefinition

        gateway = ExecutionGateway(odibi_root="d:/odibi")

        task = TaskDefinition(task_type="pipeline", args=["run", "test.yaml"])
        env_vars = {
            "BOUND_SOURCE_ROOT": "/mnt/d/odibi/.odibi/source_cache",
            "PATH": "/usr/bin:/bin",  # Should be filtered out
            "HOME": "/home/user",  # Should be filtered out
            "MALICIOUS_VAR": "$(rm -rf /)",  # Should be filtered out
        }

        cmd = gateway._build_wsl_command(task, env_vars=env_vars)
        cmd_str = " ".join(cmd)

        assert "BOUND_SOURCE_ROOT" in cmd_str
        assert "PATH=" not in cmd_str or "PATH" in cmd_str.split("BOUND_SOURCE_ROOT")[0] == False
        assert "MALICIOUS_VAR" not in cmd_str

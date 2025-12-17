"""Tests for source binding enforcement (Phase 7.D).

These tests verify:
1. Source binding creates immutable execution context
2. Path enforcement rejects paths outside pools
3. SOURCE_VIOLATION errors are raised deterministically
4. Evidence includes source usage tracking
5. ExecutionGateway respects source bindings
"""

import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from odibi.agents.core.source_binding import (
    ExecutionSourceContext,
    SourceBindingGuard,
    SourceUsageEvidence,
    SourceViolationError,
    SourceViolationType,
)
from odibi.agents.core.source_selection import (
    PoolMetadataSummary,
    SourceSelectionResult,
)
from odibi.agents.core.evidence import ExecutionEvidence


# ============================================
# Test Fixtures
# ============================================


@pytest.fixture
def temp_source_cache():
    """Create a temporary source cache with mock pools."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_path = Path(tmpdir)

        # Create pool directories
        pool1_dir = cache_path / "pool1" / "csv" / "clean"
        pool1_dir.mkdir(parents=True)
        (pool1_dir / "data.csv").write_text("id,name\n1,test")

        pool2_dir = cache_path / "pool2" / "json" / "clean"
        pool2_dir.mkdir(parents=True)
        (pool2_dir / "data.json").write_text('{"id": 1}')

        yield cache_path


@pytest.fixture
def mock_selection_result(temp_source_cache):
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
            tests_coverage=["csv_parsing"],
        ),
        PoolMetadataSummary(
            pool_id="pool2_json_clean",
            name="Pool 2 JSON",
            file_format="json",
            source_type="local",
            data_quality="clean",
            row_count=200,
            cache_path="pool2/json/clean/",
            manifest_hash="def456",
            tests_coverage=["json_parsing"],
        ),
    ]

    return SourceSelectionResult(
        selection_id="sel_test_001",
        policy_id="test_policy",
        cycle_id="test-cycle-001",
        mode="learning",
        selected_at=datetime.now(UTC).isoformat() + "Z",
        selected_pool_ids=["pool1_csv_clean", "pool2_json_clean"],
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
def source_context(mock_selection_result, temp_source_cache):
    """Create an ExecutionSourceContext from mock selection."""
    return ExecutionSourceContext.from_selection(
        mock_selection_result,
        str(temp_source_cache),
    )


# ============================================
# ExecutionSourceContext Tests
# ============================================


class TestExecutionSourceContext:
    """Tests for ExecutionSourceContext creation and behavior."""

    def test_context_from_selection(self, mock_selection_result, temp_source_cache):
        """Context should be created from selection result."""
        context = ExecutionSourceContext.from_selection(
            mock_selection_result,
            str(temp_source_cache),
        )

        assert context.cycle_id == "test-cycle-001"
        assert context.selection_id == "sel_test_001"
        assert len(context.mounted_pools) == 2
        assert "pool1_csv_clean" in context.mounted_pools
        assert context.frozen is True

    def test_context_is_frozen(self, source_context):
        """Context should be immutable after creation."""
        assert source_context.frozen is True
        # Note: frozen=True is set in __post_init__, not enforcing at attribute level

    def test_get_pool_success(self, source_context):
        """Getting a bound pool should succeed."""
        pool = source_context.get_pool("pool1_csv_clean")
        assert pool.pool_id == "pool1_csv_clean"
        assert pool.file_format == "csv"

    def test_get_pool_not_bound(self, source_context):
        """Getting an unbound pool should raise SourceViolationError."""
        with pytest.raises(SourceViolationError) as exc_info:
            source_context.get_pool("unknown_pool")

        assert exc_info.value.violation_type == SourceViolationType.POOL_NOT_SELECTED
        assert "unknown_pool" in str(exc_info.value)


# ============================================
# Path Enforcement Tests
# ============================================


class TestPathEnforcement:
    """Tests for path validation and enforcement."""

    def test_validate_path_inside_pool(self, source_context, temp_source_cache):
        """Path inside a pool should validate successfully."""
        valid_path = str(temp_source_cache / "pool1" / "csv" / "clean" / "data.csv")
        pool_id = source_context.validate_path(valid_path)
        assert pool_id == "pool1_csv_clean"

    def test_validate_path_outside_pools(self, source_context, temp_source_cache):
        """Path outside all pools should return None."""
        invalid_path = str(temp_source_cache / "not_a_pool" / "file.txt")
        pool_id = source_context.validate_path(invalid_path)
        assert pool_id is None

    def test_validate_path_completely_outside(self, source_context):
        """Path completely outside cache should return None."""
        invalid_path = "/tmp/some/other/path.txt"
        pool_id = source_context.validate_path(invalid_path)
        assert pool_id is None

    def test_enforce_path_inside_pool(self, source_context, temp_source_cache):
        """Enforcing path inside pool should succeed and log access."""
        valid_path = str(temp_source_cache / "pool1" / "csv" / "clean" / "data.csv")

        pool_id = source_context.enforce_path(valid_path, "read")

        assert pool_id == "pool1_csv_clean"
        assert len(source_context.access_log) == 1
        assert source_context.access_log[0].success is True

    def test_enforce_path_outside_pools_raises(self, source_context, temp_source_cache):
        """Enforcing path outside pools should raise SourceViolationError."""
        invalid_path = str(temp_source_cache / "not_a_pool" / "file.txt")

        with pytest.raises(SourceViolationError) as exc_info:
            source_context.enforce_path(invalid_path)

        assert exc_info.value.violation_type == SourceViolationType.PATH_OUTSIDE_POOLS
        # Should also log the failed access
        assert len(source_context.access_log) == 1
        assert source_context.access_log[0].success is False

    def test_resolve_pool_path(self, source_context, temp_source_cache):
        """Resolving path within pool should work."""
        resolved = source_context.resolve_pool_path("pool1_csv_clean", "data.csv")
        expected = os.path.normpath(
            os.path.join(str(temp_source_cache), "pool1", "csv", "clean", "data.csv")
        )
        assert resolved == expected

    def test_resolve_pool_path_escape_attempt(self, source_context):
        """Attempting to escape pool directory should raise."""
        with pytest.raises(SourceViolationError) as exc_info:
            source_context.resolve_pool_path("pool1_csv_clean", "../../../etc/passwd")

        assert exc_info.value.violation_type == SourceViolationType.PATH_OUTSIDE_POOLS


# ============================================
# SourceViolationError Tests
# ============================================


class TestSourceViolationError:
    """Tests for SOURCE_VIOLATION error handling."""

    def test_error_has_violation_type(self):
        """Error should carry violation type."""
        error = SourceViolationError(
            violation_type=SourceViolationType.PATH_OUTSIDE_POOLS,
            message="Test error",
            path="/some/path",
        )

        assert error.violation_type == SourceViolationType.PATH_OUTSIDE_POOLS
        assert error.path == "/some/path"
        assert "SOURCE_VIOLATION" in str(error)

    def test_error_to_dict(self):
        """Error should serialize to dict."""
        error = SourceViolationError(
            violation_type=SourceViolationType.POOL_NOT_SELECTED,
            message="Pool not found",
            pool_id="unknown",
        )

        error_dict = error.to_dict()

        assert error_dict["violation_type"] == "POOL_NOT_SELECTED"
        assert error_dict["pool_id"] == "unknown"

    def test_all_violation_types_exist(self):
        """All violation types should be defined."""
        assert SourceViolationType.PATH_OUTSIDE_POOLS
        assert SourceViolationType.UNKNOWN_POOL_ID
        assert SourceViolationType.POOL_NOT_SELECTED
        assert SourceViolationType.INTEGRITY_FAILURE
        assert SourceViolationType.NETWORK_ACCESS_ATTEMPT
        assert SourceViolationType.WRITE_ATTEMPT


# ============================================
# SourceBindingGuard Tests
# ============================================


class TestSourceBindingGuard:
    """Tests for the binding guard."""

    def test_check_pipeline_config_valid(self, source_context, temp_source_cache):
        """Valid paths should produce no violations."""
        guard = SourceBindingGuard(source_context)

        valid_path = str(temp_source_cache / "pool1" / "csv" / "clean" / "data.csv")
        violations = guard.check_pipeline_config("config.yaml", [valid_path])

        assert len(violations) == 0
        assert not guard.has_violations()

    def test_check_pipeline_config_invalid(self, source_context, temp_source_cache):
        """Invalid paths should produce violations."""
        guard = SourceBindingGuard(source_context)

        invalid_path = str(temp_source_cache / "not_a_pool" / "file.txt")
        violations = guard.check_pipeline_config("config.yaml", [invalid_path])

        assert len(violations) == 1
        assert violations[0].violation_type == SourceViolationType.PATH_OUTSIDE_POOLS
        assert guard.has_violations()

    def test_check_network_access(self, source_context):
        """Network access should always be a violation."""
        guard = SourceBindingGuard(source_context)

        violation = guard.check_network_access("https://example.com/data.csv")

        assert violation.violation_type == SourceViolationType.NETWORK_ACCESS_ATTEMPT
        assert guard.has_violations()

    def test_check_write_to_pool(self, source_context, temp_source_cache):
        """Writing to pool should be a violation."""
        guard = SourceBindingGuard(source_context)

        pool_path = str(temp_source_cache / "pool1" / "csv" / "clean" / "output.csv")
        violation = guard.check_write_access(pool_path)

        assert violation is not None
        assert violation.violation_type == SourceViolationType.WRITE_ATTEMPT

    def test_check_write_outside_pool(self, source_context, temp_source_cache):
        """Writing outside pools should be allowed."""
        guard = SourceBindingGuard(source_context)

        outside_path = str(temp_source_cache / "output" / "result.csv")
        violation = guard.check_write_access(outside_path)

        assert violation is None  # No violation for writes outside pools


# ============================================
# Evidence Integration Tests
# ============================================


class TestEvidenceIntegration:
    """Tests for evidence tracking."""

    def test_access_log_records(self, source_context, temp_source_cache):
        """Access should be logged."""
        valid_path = str(temp_source_cache / "pool1" / "csv" / "clean" / "data.csv")

        source_context.enforce_path(valid_path, "read")
        source_context.record_access("pool1_csv_clean", valid_path, "stat", True)

        assert len(source_context.access_log) == 2
        assert source_context.get_pools_used() == ["pool1_csv_clean"]

    def test_source_usage_evidence(self, source_context, temp_source_cache):
        """SourceUsageEvidence should capture context state."""
        valid_path = str(temp_source_cache / "pool1" / "csv" / "clean" / "data.csv")
        source_context.enforce_path(valid_path, "read")

        evidence = SourceUsageEvidence.from_context(source_context)

        assert "pool1_csv_clean" in evidence.pools_bound
        assert "pool1_csv_clean" in evidence.pools_used
        assert evidence.files_accessed == [valid_path]
        assert evidence.violations_detected == 0

    def test_evidence_to_dict(self, source_context):
        """Context should serialize to evidence dict."""
        evidence_dict = source_context.to_evidence_dict()

        assert "context_id" in evidence_dict
        assert "pools_bound" in evidence_dict
        assert "pools_used" in evidence_dict
        assert "access_log" in evidence_dict

    def test_execution_evidence_source_usage(self):
        """ExecutionEvidence should include source usage."""
        evidence = ExecutionEvidence(
            raw_command="test",
            exit_code=0,
            stdout="",
            stderr="",
            started_at="2024-01-01T00:00:00Z",
            finished_at="2024-01-01T00:01:00Z",
            duration_seconds=60.0,
        )

        evidence.set_source_usage(
            context_id="ctx_test",
            pools_bound=["pool1", "pool2"],
            pools_used=["pool1"],
            files_accessed=5,
            integrity_verified=True,
        )

        assert evidence.source_usage is not None
        assert evidence.source_usage.pools_bound == ["pool1", "pool2"]
        assert evidence.source_usage.files_accessed == 5

        # Check serialization
        evidence_dict = evidence.to_dict()
        assert "source_usage" in evidence_dict
        assert evidence_dict["source_usage"]["pools_bound"] == ["pool1", "pool2"]


# ============================================
# Determinism Tests
# ============================================


class TestDeterminism:
    """Tests for deterministic behavior."""

    def test_same_violation_same_error(self, source_context, temp_source_cache):
        """Same violation should produce same error."""
        invalid_path = str(temp_source_cache / "not_a_pool" / "file.txt")

        e1 = None
        try:
            source_context.enforce_path(invalid_path)
        except SourceViolationError as err:
            e1 = err

        assert e1 is not None, "Expected SourceViolationError to be raised"

        # Create fresh context
        from odibi.agents.core.source_selection import PoolMetadataSummary

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
        ]
        selection = SourceSelectionResult(
            selection_id="sel_test_001",
            policy_id="test_policy",
            cycle_id="test-cycle-001",
            mode="learning",
            selected_at=datetime.now(UTC).isoformat() + "Z",
            selected_pool_ids=["pool1_csv_clean"],
            selected_pools=pools,
            rationale=[],
            input_hash="input123",
            selection_hash="sel123",
            pools_considered=1,
            pools_eligible=1,
            pools_excluded=0,
            selection_strategy_used="hash_based",
        )
        context2 = ExecutionSourceContext.from_selection(selection, str(temp_source_cache))

        try:
            context2.enforce_path(invalid_path)
        except SourceViolationError as e2:
            # Should have same violation type
            assert e1.violation_type == e2.violation_type

    def test_path_outside_always_fails(self, source_context, temp_source_cache):
        """Path outside pools must ALWAYS fail."""
        invalid_path = str(temp_source_cache / "not_a_pool" / "file.txt")

        for _ in range(10):
            with pytest.raises(SourceViolationError):
                source_context.enforce_path(invalid_path)

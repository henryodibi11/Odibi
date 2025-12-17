"""Tests for Phase 9.A Disk Usage Guard.

Tests verify:
- Rotation deletes oldest files first
- Never deletes outside .odibi/
- Budgets are enforced
- Heartbeat content is stable and atomic
- Report error artifacts are created
"""

import json
import os
import time

import pytest

from odibi.agents.core.disk_guard import (
    DEFAULT_ARTIFACTS_BUDGET_BYTES,
    DEFAULT_INDEX_BUDGET_BYTES,
    DEFAULT_REPORTS_BUDGET_BYTES,
    DiskGuardReport,
    DiskUsageGuard,
    DiskUsageSummary,
    HeartbeatData,
    HeartbeatWriter,
    ReportErrorWriter,
    RotationResult,
)


class TestDiskUsageGuard:
    """Tests for DiskUsageGuard."""

    @pytest.fixture
    def temp_odibi_root(self, tmp_path):
        """Create a temporary Odibi root with .odibi structure."""
        odibi_root = tmp_path / "odibi"
        odibi_root.mkdir()

        odibi_dir = odibi_root / ".odibi"
        odibi_dir.mkdir()

        for subdir in ["artifacts", "reports", "index"]:
            (odibi_dir / subdir).mkdir()

        return str(odibi_root)

    def test_default_budgets(self, temp_odibi_root):
        """Default budgets should be set correctly."""
        guard = DiskUsageGuard(temp_odibi_root)

        assert guard.budgets["artifacts"] == DEFAULT_ARTIFACTS_BUDGET_BYTES
        assert guard.budgets["reports"] == DEFAULT_REPORTS_BUDGET_BYTES
        assert guard.budgets["index"] == DEFAULT_INDEX_BUDGET_BYTES

    def test_custom_budgets(self, temp_odibi_root):
        """Custom budgets should override defaults."""
        guard = DiskUsageGuard(
            temp_odibi_root,
            artifacts_budget_bytes=1000,
            reports_budget_bytes=500,
            index_budget_bytes=200,
        )

        assert guard.budgets["artifacts"] == 1000
        assert guard.budgets["reports"] == 500
        assert guard.budgets["index"] == 200

    def test_is_safe_path_allows_odibi_subdirs(self, temp_odibi_root):
        """_is_safe_path should allow .odibi/ subdirectories."""
        guard = DiskUsageGuard(temp_odibi_root)
        odibi_dir = os.path.join(temp_odibi_root, ".odibi")

        assert guard._is_safe_path(os.path.join(odibi_dir, "artifacts", "file.txt"))
        assert guard._is_safe_path(os.path.join(odibi_dir, "reports", "cycle.md"))
        assert guard._is_safe_path(os.path.join(odibi_dir, "index", "data.json"))

    def test_is_safe_path_blocks_outside_odibi(self, temp_odibi_root):
        """_is_safe_path should block paths outside .odibi/."""
        guard = DiskUsageGuard(temp_odibi_root)

        assert not guard._is_safe_path(os.path.join(temp_odibi_root, "src", "file.py"))
        assert not guard._is_safe_path("/etc/passwd")
        assert not guard._is_safe_path(temp_odibi_root)

    def test_is_safe_path_blocks_odibi_root(self, temp_odibi_root):
        """_is_safe_path should block .odibi/ root itself."""
        guard = DiskUsageGuard(temp_odibi_root)
        odibi_dir = os.path.join(temp_odibi_root, ".odibi")

        assert not guard._is_safe_path(odibi_dir)

    def test_is_safe_path_blocks_disallowed_subdirs(self, temp_odibi_root):
        """_is_safe_path should block disallowed subdirectories."""
        guard = DiskUsageGuard(temp_odibi_root)
        odibi_dir = os.path.join(temp_odibi_root, ".odibi")

        assert not guard._is_safe_path(os.path.join(odibi_dir, "secrets", "key.pem"))
        assert not guard._is_safe_path(os.path.join(odibi_dir, "config", "settings.yaml"))

    def test_get_directory_size_empty(self, temp_odibi_root):
        """get_directory_size should return 0 for empty directory."""
        guard = DiskUsageGuard(temp_odibi_root)
        artifacts_dir = os.path.join(temp_odibi_root, ".odibi", "artifacts")

        total_bytes, file_count = guard.get_directory_size(artifacts_dir)

        assert total_bytes == 0
        assert file_count == 0

    def test_get_directory_size_with_files(self, temp_odibi_root):
        """get_directory_size should sum file sizes correctly."""
        guard = DiskUsageGuard(temp_odibi_root)
        artifacts_dir = os.path.join(temp_odibi_root, ".odibi", "artifacts")

        (artifacts_dir_path := os.path.join(artifacts_dir, "cycle1"))
        os.makedirs(artifacts_dir_path, exist_ok=True)

        with open(os.path.join(artifacts_dir_path, "file1.txt"), "w") as f:
            f.write("A" * 100)
        with open(os.path.join(artifacts_dir_path, "file2.txt"), "w") as f:
            f.write("B" * 200)

        total_bytes, file_count = guard.get_directory_size(artifacts_dir)

        assert total_bytes == 300
        assert file_count == 2

    def test_get_usage_summary(self, temp_odibi_root):
        """get_usage_summary should return correct summary."""
        guard = DiskUsageGuard(temp_odibi_root, artifacts_budget_bytes=1000)
        artifacts_dir = os.path.join(temp_odibi_root, ".odibi", "artifacts")

        with open(os.path.join(artifacts_dir, "file.txt"), "w") as f:
            f.write("X" * 500)

        summary = guard.get_usage_summary("artifacts")

        assert summary.total_bytes == 500
        assert summary.file_count == 1
        assert summary.budget_bytes == 1000
        assert summary.over_budget is False
        assert summary.bytes_to_free == 0

    def test_get_usage_summary_over_budget(self, temp_odibi_root):
        """get_usage_summary should detect over-budget condition."""
        guard = DiskUsageGuard(temp_odibi_root, artifacts_budget_bytes=100)
        artifacts_dir = os.path.join(temp_odibi_root, ".odibi", "artifacts")

        with open(os.path.join(artifacts_dir, "file.txt"), "w") as f:
            f.write("X" * 500)

        summary = guard.get_usage_summary("artifacts")

        assert summary.total_bytes == 500
        assert summary.over_budget is True
        assert summary.bytes_to_free == 400

    def test_get_files_by_mtime_oldest_first(self, temp_odibi_root):
        """get_files_by_mtime should return files sorted oldest first."""
        guard = DiskUsageGuard(temp_odibi_root)
        artifacts_dir = os.path.join(temp_odibi_root, ".odibi", "artifacts")

        file1 = os.path.join(artifacts_dir, "old.txt")
        file2 = os.path.join(artifacts_dir, "new.txt")

        with open(file1, "w") as f:
            f.write("old")
        time.sleep(0.1)
        with open(file2, "w") as f:
            f.write("new")

        files = guard.get_files_by_mtime(artifacts_dir)

        assert len(files) == 2
        assert files[0][0] == file1
        assert files[1][0] == file2

    def test_rotate_directory_deletes_oldest_first(self, temp_odibi_root):
        """rotate_directory should delete oldest files first."""
        guard = DiskUsageGuard(temp_odibi_root, artifacts_budget_bytes=100)
        artifacts_dir = os.path.join(temp_odibi_root, ".odibi", "artifacts")

        old_file = os.path.join(artifacts_dir, "old.txt")
        with open(old_file, "w") as f:
            f.write("O" * 100)

        time.sleep(0.1)

        new_file = os.path.join(artifacts_dir, "new.txt")
        with open(new_file, "w") as f:
            f.write("N" * 100)

        result = guard.rotate_directory("artifacts")

        assert result.files_deleted >= 1
        assert result.bytes_freed >= 100
        assert not os.path.exists(old_file)
        assert os.path.exists(new_file)

    def test_rotate_directory_respects_budget(self, temp_odibi_root):
        """rotate_directory should stop when under budget."""
        guard = DiskUsageGuard(temp_odibi_root, artifacts_budget_bytes=200)
        artifacts_dir = os.path.join(temp_odibi_root, ".odibi", "artifacts")

        for i in range(5):
            with open(os.path.join(artifacts_dir, f"file{i}.txt"), "w") as f:
                f.write("X" * 100)
            time.sleep(0.05)

        result = guard.rotate_directory("artifacts")

        assert result.bytes_freed >= 300
        summary = guard.get_usage_summary("artifacts")
        assert summary.total_bytes <= 200

    def test_rotate_directory_dry_run(self, temp_odibi_root):
        """rotate_directory with dry_run should not delete files."""
        guard = DiskUsageGuard(temp_odibi_root, artifacts_budget_bytes=50)
        artifacts_dir = os.path.join(temp_odibi_root, ".odibi", "artifacts")

        file_path = os.path.join(artifacts_dir, "file.txt")
        with open(file_path, "w") as f:
            f.write("X" * 100)

        result = guard.rotate_directory("artifacts", dry_run=True)

        assert result.files_deleted == 1
        assert result.bytes_freed == 100
        assert os.path.exists(file_path)

    def test_rotate_directory_never_deletes_outside_odibi(self, temp_odibi_root):
        """rotate_directory should never delete files outside .odibi/."""
        guard = DiskUsageGuard(temp_odibi_root, artifacts_budget_bytes=10)

        src_file = os.path.join(temp_odibi_root, "src", "important.py")
        os.makedirs(os.path.dirname(src_file), exist_ok=True)
        with open(src_file, "w") as f:
            f.write("critical code")

        artifacts_dir = os.path.join(temp_odibi_root, ".odibi", "artifacts")
        with open(os.path.join(artifacts_dir, "artifact.txt"), "w") as f:
            f.write("X" * 100)

        guard.rotate_directory("artifacts")

        assert os.path.exists(src_file)

    def test_run_cleanup_all_directories(self, temp_odibi_root):
        """run_cleanup should process all monitored directories."""
        guard = DiskUsageGuard(
            temp_odibi_root,
            artifacts_budget_bytes=50,
            reports_budget_bytes=50,
            index_budget_bytes=50,
        )

        for subdir in ["artifacts", "reports", "index"]:
            dir_path = os.path.join(temp_odibi_root, ".odibi", subdir)
            with open(os.path.join(dir_path, "file.txt"), "w") as f:
                f.write("X" * 100)

        report = guard.run_cleanup()

        assert len(report.summaries) == 3
        assert report.total_bytes_freed >= 150

    def test_run_cleanup_returns_report(self, temp_odibi_root):
        """run_cleanup should return a proper DiskGuardReport."""
        guard = DiskUsageGuard(temp_odibi_root)

        report = guard.run_cleanup()

        assert isinstance(report, DiskGuardReport)
        assert report.timestamp
        assert isinstance(report.summaries, list)
        assert isinstance(report.rotations, list)

    def test_get_total_usage(self, temp_odibi_root):
        """get_total_usage should return usage for all directories."""
        guard = DiskUsageGuard(temp_odibi_root)

        artifacts_dir = os.path.join(temp_odibi_root, ".odibi", "artifacts")
        with open(os.path.join(artifacts_dir, "file.txt"), "w") as f:
            f.write("X" * 100)

        usage = guard.get_total_usage()

        assert "artifacts" in usage
        assert "reports" in usage
        assert "index" in usage
        assert usage["artifacts"] == 100


class TestHeartbeatWriter:
    """Tests for HeartbeatWriter."""

    @pytest.fixture
    def temp_odibi_root(self, tmp_path):
        """Create a temporary Odibi root."""
        odibi_root = tmp_path / "odibi"
        odibi_root.mkdir()
        return str(odibi_root)

    def test_write_creates_heartbeat_file(self, temp_odibi_root):
        """write should create heartbeat.json file."""
        writer = HeartbeatWriter(temp_odibi_root)

        data = HeartbeatData(
            last_cycle_id="cycle-001",
            timestamp="2024-01-01T12:00:00",
            last_status="COMPLETED",
            cycles_completed=5,
            cycles_failed=1,
            disk_usage={"artifacts": 1000, "reports": 500},
        )

        result = writer.write(data)

        assert result is True
        assert os.path.exists(writer.heartbeat_path)

    def test_write_content_is_valid_json(self, temp_odibi_root):
        """write should produce valid JSON."""
        writer = HeartbeatWriter(temp_odibi_root)

        data = HeartbeatData(
            last_cycle_id="cycle-002",
            timestamp="2024-01-02T14:30:00",
            last_status="RUNNING",
            cycles_completed=10,
            cycles_failed=2,
            disk_usage={"artifacts": 2000},
        )

        writer.write(data)

        with open(writer.heartbeat_path, "r") as f:
            content = json.load(f)

        assert content["last_cycle_id"] == "cycle-002"
        assert content["cycles_completed"] == 10
        assert content["disk_usage"]["artifacts"] == 2000

    def test_read_returns_heartbeat_data(self, temp_odibi_root):
        """read should return HeartbeatData from file."""
        writer = HeartbeatWriter(temp_odibi_root)

        original = HeartbeatData(
            last_cycle_id="cycle-003",
            timestamp="2024-01-03T09:00:00",
            last_status="FAILED",
            cycles_completed=3,
            cycles_failed=1,
            disk_usage={"index": 500},
        )

        writer.write(original)
        read_data = writer.read()

        assert read_data is not None
        assert read_data.last_cycle_id == "cycle-003"
        assert read_data.last_status == "FAILED"
        assert read_data.cycles_completed == 3

    def test_read_returns_none_when_no_file(self, temp_odibi_root):
        """read should return None when heartbeat file doesn't exist."""
        writer = HeartbeatWriter(temp_odibi_root)

        result = writer.read()

        assert result is None

    def test_write_overwrites_existing(self, temp_odibi_root):
        """write should overwrite existing heartbeat file."""
        writer = HeartbeatWriter(temp_odibi_root)

        data1 = HeartbeatData(
            last_cycle_id="cycle-001",
            timestamp="2024-01-01T00:00:00",
            last_status="COMPLETED",
            cycles_completed=1,
            cycles_failed=0,
            disk_usage={},
        )
        writer.write(data1)

        data2 = HeartbeatData(
            last_cycle_id="cycle-002",
            timestamp="2024-01-02T00:00:00",
            last_status="RUNNING",
            cycles_completed=2,
            cycles_failed=0,
            disk_usage={},
        )
        writer.write(data2)

        read_data = writer.read()

        assert read_data.last_cycle_id == "cycle-002"
        assert read_data.cycles_completed == 2


class TestReportErrorWriter:
    """Tests for ReportErrorWriter."""

    @pytest.fixture
    def temp_odibi_root(self, tmp_path):
        """Create a temporary Odibi root."""
        odibi_root = tmp_path / "odibi"
        odibi_root.mkdir()
        return str(odibi_root)

    def test_write_error_creates_file(self, temp_odibi_root):
        """write_error should create error file."""
        writer = ReportErrorWriter(temp_odibi_root)

        error = ValueError("Something went wrong")
        path = writer.write_error("cycle-001", error, "During report generation")

        assert path != ""
        assert os.path.exists(path)

    def test_write_error_content(self, temp_odibi_root):
        """write_error should write correct content."""
        writer = ReportErrorWriter(temp_odibi_root)

        error = RuntimeError("Test error message")
        path = writer.write_error("cycle-002", error, "Test context")

        with open(path, "r") as f:
            content = json.load(f)

        assert content["cycle_id"] == "cycle-002"
        assert content["error_type"] == "RuntimeError"
        assert content["error_message"] == "Test error message"
        assert content["context"] == "Test context"
        assert "timestamp" in content

    def test_write_error_creates_errors_directory(self, temp_odibi_root):
        """write_error should create errors directory if needed."""
        writer = ReportErrorWriter(temp_odibi_root)

        error = Exception("Test")
        writer.write_error("cycle-003", error)

        assert os.path.isdir(writer.errors_dir)

    def test_write_error_deterministic_path(self, temp_odibi_root):
        """write_error should use deterministic path based on cycle_id."""
        writer = ReportErrorWriter(temp_odibi_root)

        error = Exception("Test")
        path = writer.write_error("my-cycle-id", error)

        assert path.endswith("my-cycle-id.json")


class TestDiskUsageSummary:
    """Tests for DiskUsageSummary data class."""

    def test_to_dict(self):
        """to_dict should include all fields."""
        summary = DiskUsageSummary(
            path="/path/to/dir",
            total_bytes=1000,
            file_count=5,
            budget_bytes=2000,
            over_budget=False,
            bytes_to_free=0,
        )

        d = summary.to_dict()

        assert d["path"] == "/path/to/dir"
        assert d["total_bytes"] == 1000
        assert d["file_count"] == 5
        assert d["budget_bytes"] == 2000
        assert d["over_budget"] is False


class TestRotationResult:
    """Tests for RotationResult data class."""

    def test_to_dict(self):
        """to_dict should include all fields."""
        result = RotationResult(
            directory="/path/to/dir",
            files_deleted=3,
            bytes_freed=1500,
            errors=["Error 1", "Error 2"],
        )

        d = result.to_dict()

        assert d["directory"] == "/path/to/dir"
        assert d["files_deleted"] == 3
        assert d["bytes_freed"] == 1500
        assert len(d["errors"]) == 2


class TestDiskGuardReport:
    """Tests for DiskGuardReport data class."""

    def test_to_dict(self):
        """to_dict should include all fields."""
        summary = DiskUsageSummary(
            path="/test",
            total_bytes=100,
            file_count=1,
            budget_bytes=200,
            over_budget=False,
            bytes_to_free=0,
        )
        rotation = RotationResult(
            directory="/test",
            files_deleted=0,
            bytes_freed=0,
        )

        report = DiskGuardReport(
            timestamp="2024-01-01T00:00:00",
            summaries=[summary],
            rotations=[rotation],
            total_bytes_freed=0,
        )

        d = report.to_dict()

        assert d["timestamp"] == "2024-01-01T00:00:00"
        assert len(d["summaries"]) == 1
        assert len(d["rotations"]) == 1

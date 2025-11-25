"""Tests for Delta Lake support in PandasEngine."""

import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pytest

# Try to import delta dependencies
try:
    from deltalake import write_deltalake

    DELTA_AVAILABLE = True
except ImportError:
    DELTA_AVAILABLE = False

from odibi.connections.local import LocalConnection
from odibi.engine.pandas_engine import PandasEngine

pytestmark = pytest.mark.skipif(
    not DELTA_AVAILABLE,
    reason="Delta Lake tests require 'pip install odibi[pandas]' or 'pip install deltalake'",
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    tmp = tempfile.mkdtemp()
    yield tmp
    shutil.rmtree(tmp)


@pytest.fixture
def engine():
    """Create PandasEngine instance."""
    return PandasEngine()


@pytest.fixture
def sample_df():
    """Create sample DataFrame."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "city": ["NYC", "LA", "SF", "NYC", "LA"],
        }
    )


class TestDeltaWrite:
    """Test Delta table write operations."""

    def test_write_delta_basic(self, engine, sample_df, temp_dir):
        """Test basic Delta table write."""
        conn = LocalConnection(base_path=temp_dir)
        delta_path = "test_table.delta"

        # Write Delta table
        engine.write(sample_df, connection=conn, format="delta", path=delta_path, mode="overwrite")

        # Verify Delta table exists
        full_path = Path(temp_dir) / delta_path
        assert full_path.exists()
        assert (full_path / "_delta_log").exists()

    def test_write_delta_append(self, engine, sample_df, temp_dir):
        """Test Delta table append mode."""
        conn = LocalConnection(base_path=temp_dir)
        delta_path = "append_table.delta"

        # Write initial data
        engine.write(sample_df, connection=conn, format="delta", path=delta_path, mode="overwrite")

        # Append more data
        more_data = pd.DataFrame(
            {"id": [6, 7], "name": ["Frank", "Grace"], "age": [50, 55], "city": ["SF", "NYC"]}
        )
        engine.write(more_data, connection=conn, format="delta", path=delta_path, mode="append")

        # Read back and verify
        result = engine.read(conn, format="delta", path=delta_path)
        assert len(result) == 7  # 5 + 2

    def test_write_delta_with_partitioning(self, engine, sample_df, temp_dir):
        """Test Delta table with partitioning."""
        conn = LocalConnection(base_path=temp_dir)
        delta_path = "partitioned_table.delta"

        # Write with partitioning (should emit warning)
        with pytest.warns(UserWarning, match="Partitioning can cause performance issues"):
            engine.write(
                sample_df,
                connection=conn,
                format="delta",
                path=delta_path,
                mode="overwrite",
                options={"partition_by": ["city"]},
            )

        # Verify partitioned structure exists
        full_path = Path(temp_dir) / delta_path
        assert full_path.exists()


class TestDeltaRead:
    """Test Delta table read operations."""

    def test_read_delta_basic(self, engine, sample_df, temp_dir):
        """Test basic Delta table read."""
        conn = LocalConnection(base_path=temp_dir)
        delta_path = "read_table.delta"

        # Write Delta table
        write_deltalake(str(Path(temp_dir) / delta_path), sample_df)

        # Read back
        result = engine.read(conn, format="delta", path=delta_path)

        assert len(result) == 5
        assert list(result.columns) == ["id", "name", "age", "city"]

    def test_read_delta_time_travel(self, engine, sample_df, temp_dir):
        """Test Delta table time travel (read specific version)."""
        conn = LocalConnection(base_path=temp_dir)
        delta_path = "time_travel_table.delta"
        full_path = str(Path(temp_dir) / delta_path)

        # Write version 0
        write_deltalake(full_path, sample_df, mode="overwrite")

        # Write version 1 (different data)
        new_data = pd.DataFrame(
            {
                "id": [10, 20],
                "name": ["New1", "New2"],
                "age": [60, 65],
                "city": ["Boston", "Seattle"],
            }
        )
        write_deltalake(full_path, new_data, mode="overwrite")

        # Read latest version
        latest = engine.read(conn, format="delta", path=delta_path)
        assert len(latest) == 2

        # Read version 0 (time travel)
        version_0 = engine.read(conn, format="delta", path=delta_path, options={"versionAsOf": 0})
        assert len(version_0) == 5


class TestDeltaVacuum:
    """Test Delta table VACUUM operations."""

    def test_vacuum_delta(self, engine, sample_df, temp_dir):
        """Test VACUUM operation."""
        conn = LocalConnection(base_path=temp_dir)
        delta_path = "vacuum_table.delta"
        full_path = str(Path(temp_dir) / delta_path)

        # Write multiple versions
        write_deltalake(full_path, sample_df, mode="overwrite")
        write_deltalake(full_path, sample_df, mode="append")

        # Vacuum (retention 0 hours for testing, bypass safety check)
        result = engine.vacuum_delta(
            conn, delta_path, retention_hours=0, dry_run=False, enforce_retention_duration=False
        )

        assert "files_deleted" in result
        assert isinstance(result["files_deleted"], int)

    def test_vacuum_delta_dry_run(self, engine, sample_df, temp_dir):
        """Test VACUUM dry run mode."""
        conn = LocalConnection(base_path=temp_dir)
        delta_path = "vacuum_dry_run_table.delta"
        full_path = str(Path(temp_dir) / delta_path)

        # Write data
        write_deltalake(full_path, sample_df, mode="overwrite")

        # Vacuum dry run (bypass safety check for testing)
        result = engine.vacuum_delta(
            conn, delta_path, retention_hours=0, dry_run=True, enforce_retention_duration=False
        )

        assert "files_deleted" in result


class TestDeltaHistory:
    """Test Delta table history operations."""

    def test_get_delta_history(self, engine, sample_df, temp_dir):
        """Test getting Delta table history."""
        conn = LocalConnection(base_path=temp_dir)
        delta_path = "history_table.delta"
        full_path = str(Path(temp_dir) / delta_path)

        # Write multiple versions
        write_deltalake(full_path, sample_df, mode="overwrite")
        write_deltalake(full_path, sample_df, mode="append")

        # Get history
        history = engine.get_delta_history(conn, delta_path)

        assert isinstance(history, list)
        assert len(history) >= 2  # At least 2 versions

    def test_get_delta_history_with_limit(self, engine, sample_df, temp_dir):
        """Test getting Delta table history with limit."""
        conn = LocalConnection(base_path=temp_dir)
        delta_path = "history_limit_table.delta"
        full_path = str(Path(temp_dir) / delta_path)

        # Write multiple versions
        write_deltalake(full_path, sample_df, mode="overwrite")
        write_deltalake(full_path, sample_df, mode="append")
        write_deltalake(full_path, sample_df, mode="append")

        # Get history with limit
        history = engine.get_delta_history(conn, delta_path, limit=2)

        assert len(history) == 2


class TestDeltaRestore:
    """Test Delta table restore operations."""

    def test_restore_delta(self, engine, sample_df, temp_dir):
        """Test restoring Delta table to previous version."""
        conn = LocalConnection(base_path=temp_dir)
        delta_path = "restore_table.delta"
        full_path = str(Path(temp_dir) / delta_path)

        # Write version 0
        write_deltalake(full_path, sample_df, mode="overwrite")

        # Write version 1 (different data)
        new_data = pd.DataFrame(
            {"id": [100], "name": ["Modified"], "age": [99], "city": ["Modified"]}
        )
        write_deltalake(full_path, new_data, mode="overwrite")

        # Verify version 1
        current = engine.read(conn, format="delta", path=delta_path)
        assert len(current) == 1

        # Restore to version 0
        engine.restore_delta(conn, delta_path, version=0)

        # Verify restoration
        restored = engine.read(conn, format="delta", path=delta_path)
        assert len(restored) == 5


class TestDeltaErrorHandling:
    """Test Delta error handling."""

    def test_read_nonexistent_delta_table(self, engine, temp_dir):
        """Test reading non-existent Delta table."""
        conn = LocalConnection(base_path=temp_dir)

        with pytest.raises(Exception):  # DeltaTable will raise error
            engine.read(conn, format="delta", path="nonexistent.delta")

    def test_write_delta_import_error(self, engine, sample_df, temp_dir, monkeypatch):
        """Test write Delta with import error."""
        # Mock import failure
        import builtins

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "deltalake":
                raise ImportError("Mock import error")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)

        conn = LocalConnection(base_path=temp_dir)

        with pytest.raises(ImportError, match="Delta Lake support requires"):
            engine.write(sample_df, conn, format="delta", path="test.delta")

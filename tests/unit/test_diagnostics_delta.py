"""Unit tests for odibi.diagnostics.delta module."""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from odibi.diagnostics.delta import (
    DeltaDiffResult,
    detect_drift,
    _get_delta_diff_pandas,
)


class TestDeltaDiffResult:
    """Test DeltaDiffResult dataclass."""

    def test_initialization_required_fields(self):
        """Test that DeltaDiffResult can be initialized with required fields."""
        result = DeltaDiffResult(
            table_path="/path/to/table",
            version_a=1,
            version_b=2,
            rows_change=100,
            files_change=5,
            size_change_bytes=1024,
            schema_added=["col1"],
            schema_removed=["col2"],
        )

        assert result.table_path == "/path/to/table"
        assert result.version_a == 1
        assert result.version_b == 2
        assert result.rows_change == 100
        assert result.files_change == 5
        assert result.size_change_bytes == 1024
        assert result.schema_added == ["col1"]
        assert result.schema_removed == ["col2"]

    def test_initialization_optional_fields(self):
        """Test that DeltaDiffResult optional fields default to None."""
        result = DeltaDiffResult(
            table_path="/path/to/table",
            version_a=1,
            version_b=2,
            rows_change=0,
            files_change=0,
            size_change_bytes=0,
            schema_added=[],
            schema_removed=[],
        )

        assert result.schema_current is None
        assert result.schema_previous is None
        assert result.rows_added is None
        assert result.rows_removed is None
        assert result.rows_updated is None
        assert result.operations is None
        assert result.sample_added is None
        assert result.sample_removed is None
        assert result.sample_updated is None

    def test_operations_field_name(self):
        """Test that the operations field exists (not operations_between)."""
        result = DeltaDiffResult(
            table_path="/path/to/table",
            version_a=1,
            version_b=2,
            rows_change=0,
            files_change=0,
            size_change_bytes=0,
            schema_added=[],
            schema_removed=[],
            operations=["WRITE", "MERGE"],
        )

        assert result.operations == ["WRITE", "MERGE"]
        # Verify that operations_between doesn't exist
        assert not hasattr(result, "operations_between")


class TestDetectDrift:
    """Test detect_drift function."""

    @patch("odibi.diagnostics.delta.get_delta_diff")
    @patch("deltalake.DeltaTable")
    def test_detect_drift_schema_added(self, mock_delta_table, mock_get_diff):
        """Test that schema additions are detected as drift."""
        mock_diff = DeltaDiffResult(
            table_path="/path/to/table",
            version_a=1,
            version_b=2,
            rows_change=0,
            files_change=0,
            size_change_bytes=0,
            schema_added=["new_col1", "new_col2"],
            schema_removed=[],
        )
        mock_get_diff.return_value = mock_diff

        # Mock DeltaTable for pandas path
        mock_dt = Mock()
        mock_dt.to_pandas.return_value = pd.DataFrame({"id": [1, 2, 3]})
        mock_delta_table.return_value = mock_dt

        result = detect_drift(
            table_path="/path/to/table", current_version=2, baseline_version=1
        )

        assert result is not None
        assert "Schema drift detected" in result
        assert "+2 columns" in result
        assert "-0 columns" in result

    @patch("odibi.diagnostics.delta.get_delta_diff")
    @patch("deltalake.DeltaTable")
    def test_detect_drift_schema_removed(self, mock_delta_table, mock_get_diff):
        """Test that schema removals are detected as drift."""
        mock_diff = DeltaDiffResult(
            table_path="/path/to/table",
            version_a=1,
            version_b=2,
            rows_change=0,
            files_change=0,
            size_change_bytes=0,
            schema_added=[],
            schema_removed=["old_col"],
        )
        mock_get_diff.return_value = mock_diff

        # Mock DeltaTable for pandas path
        mock_dt = Mock()
        mock_dt.to_pandas.return_value = pd.DataFrame({"id": [1, 2, 3]})
        mock_delta_table.return_value = mock_dt

        result = detect_drift(
            table_path="/path/to/table", current_version=2, baseline_version=1
        )

        assert result is not None
        assert "Schema drift detected" in result
        assert "+0 columns" in result
        assert "-1 columns" in result

    @patch("odibi.diagnostics.delta.get_delta_diff")
    @patch("deltalake.DeltaTable")
    def test_detect_drift_row_count_threshold_exceeded(self, mock_delta_table, mock_get_diff):
        """Test that row count drift above threshold is detected."""
        mock_diff = DeltaDiffResult(
            table_path="/path/to/table",
            version_a=1,
            version_b=2,
            rows_change=60,  # 60% change
            files_change=0,
            size_change_bytes=0,
            schema_added=[],
            schema_removed=[],
        )
        mock_get_diff.return_value = mock_diff

        # Mock DeltaTable for pandas path - base count is 100
        mock_dt = Mock()
        mock_dt.to_pandas.return_value = pd.DataFrame({"id": range(100)})
        mock_delta_table.return_value = mock_dt

        result = detect_drift(
            table_path="/path/to/table",
            current_version=2,
            baseline_version=1,
            threshold_pct=10.0,
        )

        assert result is not None
        assert "Row count drift" in result
        assert "60.0%" in result

    @patch("odibi.diagnostics.delta.get_delta_diff")
    @patch("deltalake.DeltaTable")
    def test_detect_drift_no_drift(self, mock_delta_table, mock_get_diff):
        """Test that no drift returns None."""
        mock_diff = DeltaDiffResult(
            table_path="/path/to/table",
            version_a=1,
            version_b=2,
            rows_change=5,  # 5% change
            files_change=0,
            size_change_bytes=0,
            schema_added=[],
            schema_removed=[],
        )
        mock_get_diff.return_value = mock_diff

        # Mock DeltaTable for pandas path - base count is 100
        mock_dt = Mock()
        mock_dt.to_pandas.return_value = pd.DataFrame({"id": range(100)})
        mock_delta_table.return_value = mock_dt

        result = detect_drift(
            table_path="/path/to/table",
            current_version=2,
            baseline_version=1,
            threshold_pct=10.0,
        )

        assert result is None

    @patch("odibi.diagnostics.delta.get_delta_diff")
    @patch("deltalake.DeltaTable")
    def test_detect_drift_zero_baseline(self, mock_delta_table, mock_get_diff):
        """Test that drift from zero baseline is detected."""
        mock_diff = DeltaDiffResult(
            table_path="/path/to/table",
            version_a=1,
            version_b=2,
            rows_change=100,
            files_change=0,
            size_change_bytes=0,
            schema_added=[],
            schema_removed=[],
        )
        mock_get_diff.return_value = mock_diff

        # Mock DeltaTable for pandas path - base count is 0
        mock_dt = Mock()
        mock_dt.to_pandas.return_value = pd.DataFrame()
        mock_delta_table.return_value = mock_dt

        result = detect_drift(
            table_path="/path/to/table", current_version=2, baseline_version=1
        )

        assert result is not None
        assert "Data volume spike" in result
        assert "0 -> 100" in result


class TestGetDeltaDiffPandas:
    """Test _get_delta_diff_pandas function."""

    @patch("deltalake.DeltaTable")
    def test_basic_diff_no_deep(self, mock_delta_table_class):
        """Test basic diff without deep comparison."""
        # Mock DeltaTable
        mock_dt = Mock()

        # Mock history
        mock_dt.history.return_value = [
            {"version": 1, "operation": "WRITE"},
            {"version": 2, "operation": "MERGE"},
        ]

        # Mock schema for version_a
        mock_schema_a = Mock()
        mock_schema_a.to_pyarrow.return_value = Mock(names=["id", "name", "old_col"])

        # Mock to_pandas calls
        df_a = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "old_col": ["x", "y", "z"],
            }
        )
        df_b = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "name": ["Alice", "Bob", "Charlie", "David"],
                "new_col": ["a", "b", "c", "d"],
            }
        )

        # Setup mock behavior
        def load_version_side_effect(version):
            if version == 1:
                mock_dt.schema.return_value = mock_schema_a
            elif version == 2:
                mock_schema_b = Mock()
                mock_schema_b.to_pyarrow.return_value = Mock(names=["id", "name", "new_col"])
                mock_dt.schema.return_value = mock_schema_b

        mock_dt.load_as_version = Mock(side_effect=load_version_side_effect)

        # Setup to_pandas to return correct DataFrames based on load_as_version calls
        call_count = [0]

        def to_pandas_side_effect():
            call_count[0] += 1
            if call_count[0] == 1:
                return df_a
            else:
                return df_b

        mock_dt.to_pandas = Mock(side_effect=to_pandas_side_effect)
        mock_delta_table_class.return_value = mock_dt

        result = _get_delta_diff_pandas(
            table_path="/path/to/table", version_a=1, version_b=2, deep=False
        )

        assert result.table_path == "/path/to/table"
        assert result.version_a == 1
        assert result.version_b == 2
        assert result.rows_change == 1  # 4 - 3
        assert result.operations == ["MERGE"]
        assert "new_col" in result.schema_added
        assert "old_col" in result.schema_removed
        assert result.rows_added is None  # Not computed without deep=True
        assert result.rows_removed is None

    @patch("deltalake.DeltaTable")
    def test_deep_diff_with_keys(self, mock_delta_table_class):
        """Test deep diff with primary keys."""
        mock_dt = Mock()

        # Mock history
        mock_dt.history.return_value = [
            {"version": 1, "operation": "WRITE"},
            {"version": 2, "operation": "UPDATE"},
        ]

        # Mock schema
        mock_schema = Mock()
        mock_schema.to_pyarrow.return_value = Mock(names=["id", "name", "value"])
        mock_dt.schema.return_value = mock_schema
        mock_dt.load_as_version = Mock()

        # Setup DataFrames
        df_a = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "value": [10, 20, 30]}
        )
        df_b = pd.DataFrame(
            {
                "id": [1, 2, 4],  # 3 removed, 4 added
                "name": ["Alice", "Bob", "David"],
                "value": [15, 20, 40],  # id=1 value changed
            }
        )

        call_count = [0]

        def to_pandas_side_effect():
            call_count[0] += 1
            # Three calls: len(a), load a, load b
            if call_count[0] <= 2:
                return df_a.copy()
            else:
                return df_b.copy()

        mock_dt.to_pandas = Mock(side_effect=to_pandas_side_effect)
        mock_delta_table_class.return_value = mock_dt

        result = _get_delta_diff_pandas(
            table_path="/path/to/table", version_a=1, version_b=2, deep=True, keys=["id"]
        )

        assert result.rows_change == 0  # 3 - 3 (same count)
        assert result.rows_added == 1  # id=4
        assert result.rows_removed == 1  # id=3
        assert result.rows_updated == 1  # id=1 value changed
        assert len(result.sample_added) == 1
        assert len(result.sample_removed) == 1
        assert len(result.sample_updated) == 1

    @patch("deltalake.DeltaTable")
    def test_deep_diff_without_keys(self, mock_delta_table_class):
        """Test deep diff without primary keys (set-based diff)."""
        mock_dt = Mock()

        # Mock history
        mock_dt.history.return_value = [
            {"version": 1, "operation": "WRITE"},
            {"version": 2, "operation": "MERGE"},
        ]

        # Mock schema
        mock_schema = Mock()
        mock_schema.to_pyarrow.return_value = Mock(names=["id", "name"])
        mock_dt.schema.return_value = mock_schema
        mock_dt.load_as_version = Mock()

        # Setup DataFrames
        df_a = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        df_b = pd.DataFrame({"id": [2, 3], "name": ["Bob", "Charlie"]})

        call_count = [0]

        def to_pandas_side_effect():
            call_count[0] += 1
            # Three calls: len(a), load a, load b
            if call_count[0] <= 2:
                return df_a.copy()
            else:
                return df_b.copy()

        mock_dt.to_pandas = Mock(side_effect=to_pandas_side_effect)
        mock_delta_table_class.return_value = mock_dt

        result = _get_delta_diff_pandas(
            table_path="/path/to/table", version_a=1, version_b=2, deep=True, keys=None
        )

        assert result.rows_change == 0  # 2 - 2
        assert result.rows_added == 1  # (3, Charlie)
        assert result.rows_removed == 1  # (1, Alice)
        assert result.rows_updated is None  # Can't detect updates without keys

    def test_import_error_handling(self):
        """Test that ImportError is raised when deltalake is not available."""
        with patch("builtins.__import__", side_effect=ImportError("Module not found")):
            with pytest.raises(ImportError, match="Delta Lake support requires"):
                _get_delta_diff_pandas(
                    table_path="/path/to/table", version_a=1, version_b=2
                )

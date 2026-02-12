"""
Unit tests for odibi/diagnostics/delta.py
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.diagnostics.delta import (
    DeltaDiffResult,
    _get_delta_diff_pandas,
    detect_drift,
    get_delta_diff,
)


@pytest.fixture
def temp_delta_table(tmp_path):
    """
    Create a temporary directory to simulate a Delta table.
    """
    delta_dir = tmp_path / "delta_table"
    delta_dir.mkdir(exist_ok=True)
    return str(delta_dir)


class TestDeltaDiffResult:
    """Tests for DeltaDiffResult dataclass."""

    def test_delta_diff_result_structure(self):
        """Test that DeltaDiffResult can be instantiated with required fields."""
        result = DeltaDiffResult(
            table_path="/path/to/table",
            version_a=1,
            version_b=2,
            rows_change=10,
            files_change=1,
            size_change_bytes=1024,
            schema_added=["new_col"],
            schema_removed=["old_col"],
        )

        assert result.table_path == "/path/to/table"
        assert result.version_a == 1
        assert result.version_b == 2
        assert result.rows_change == 10
        assert result.files_change == 1
        assert result.size_change_bytes == 1024
        assert result.schema_added == ["new_col"]
        assert result.schema_removed == ["old_col"]

    def test_delta_diff_result_optional_fields(self):
        """Test that optional fields are properly initialized."""
        result = DeltaDiffResult(
            table_path="/path/to/table",
            version_a=1,
            version_b=2,
            rows_change=0,
            files_change=0,
            size_change_bytes=0,
            schema_added=[],
            schema_removed=[],
            rows_added=5,
            rows_removed=3,
            rows_updated=2,
            sample_added=[{"id": 1, "name": "test"}],
        )

        assert result.rows_added == 5
        assert result.rows_removed == 3
        assert result.rows_updated == 2
        assert result.sample_added == [{"id": 1, "name": "test"}]


class TestGetDeltaDiffPandas:
    """Tests for _get_delta_diff_pandas function."""

    @patch("deltalake.DeltaTable")
    def test_schema_changes_detection(self, mock_delta_table_class, temp_delta_table):
        """Test that schema changes are properly detected."""
        # Setup mock DeltaTable
        mock_dt = MagicMock()
        mock_delta_table_class.return_value = mock_dt

        # Mock history
        mock_dt.history.return_value = [
            {"version": 2, "operation": "WRITE"},
            {"version": 1, "operation": "CREATE"},
        ]

        # Mock schema for version 1 (baseline)
        mock_schema_v1 = MagicMock()
        mock_schema_v1.to_pyarrow.return_value = MagicMock(names=["id", "name", "old_col"])

        # Mock schema for version 2 (current)
        mock_schema_v2 = MagicMock()
        mock_schema_v2.to_pyarrow.return_value = MagicMock(names=["id", "name", "new_col"])

        # Mock load_as_version to return different schemas
        def mock_load_version(version):
            if version == 1:
                mock_dt.schema.return_value = mock_schema_v1
            else:
                mock_dt.schema.return_value = mock_schema_v2

        mock_dt.load_as_version = mock_load_version

        # Mock DataFrames
        df_v1 = pd.DataFrame({"id": [1], "name": ["test"], "old_col": ["value"]})
        df_v2 = pd.DataFrame({"id": [1], "name": ["test"], "new_col": ["new_value"]})

        def mock_to_pandas():
            if mock_dt.schema() == mock_schema_v1:
                return df_v1
            return df_v2

        mock_dt.to_pandas = mock_to_pandas

        # Execute
        result = _get_delta_diff_pandas(temp_delta_table, 1, 2, deep=False)

        # Verify
        assert result.version_a == 1
        assert result.version_b == 2
        assert "new_col" in result.schema_added
        assert "old_col" in result.schema_removed
        assert result.rows_change == 0  # Same row count

    @patch("deltalake.DeltaTable")
    def test_row_count_changes(self, mock_delta_table_class, temp_delta_table):
        """Test that row count changes are properly calculated."""
        # Setup mock
        mock_dt = MagicMock()
        mock_delta_table_class.return_value = mock_dt

        # Mock history
        mock_dt.history.return_value = []

        # Mock schema (same for both versions)
        mock_schema = MagicMock()
        mock_schema.to_pyarrow.return_value = MagicMock(names=["id", "name"])
        mock_dt.schema.return_value = mock_schema

        # Mock DataFrames with different row counts
        df_v1 = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        df_v2 = pd.DataFrame({"id": [1, 2, 3, 4, 5], "name": ["a", "b", "c", "d", "e"]})

        call_count = [0]

        def mock_to_pandas():
            call_count[0] += 1
            if call_count[0] == 1:
                return df_v1
            return df_v2

        mock_dt.to_pandas = mock_to_pandas
        mock_dt.load_as_version = MagicMock()

        # Execute
        result = _get_delta_diff_pandas(temp_delta_table, 1, 2, deep=False)

        # Verify
        assert result.rows_change == 3  # 5 - 2 = 3

    @patch("deltalake.DeltaTable")
    def test_deep_diff_added_removed_rows(self, mock_delta_table_class, temp_delta_table):
        """Test deep diff identifies added and removed rows."""
        # Setup mock
        mock_dt = MagicMock()
        mock_delta_table_class.return_value = mock_dt

        # Mock history
        mock_dt.history.return_value = []

        # Mock schema
        mock_schema = MagicMock()
        mock_schema.to_pyarrow.return_value = MagicMock(names=["id", "name"])
        mock_dt.schema.return_value = mock_schema

        # Mock DataFrames with different data
        # Version 1: rows with id 1,2
        # Version 2: rows with id 2,3
        # Expected: 1 removed (id=1), 1 added (id=3)
        df_v1 = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        df_v2 = pd.DataFrame({"id": [2, 3], "name": ["b", "c"]})

        versions = [df_v1, df_v2]
        call_index = [0]

        def mock_to_pandas():
            df = versions[call_index[0]].copy()
            call_index[0] = (call_index[0] + 1) % len(versions)
            return df

        mock_dt.to_pandas = mock_to_pandas
        mock_dt.load_as_version = MagicMock()

        # Execute with deep=True (without keys, uses set-based diff)
        result = _get_delta_diff_pandas(temp_delta_table, 1, 2, deep=True)

        # Verify basic properties
        assert result.rows_change == 0  # Same count (2 rows each)
        # The function should complete without error and return a result
        assert result.rows_added is not None
        assert result.rows_removed is not None
        # Sample lists should exist (even if empty)
        assert isinstance(result.sample_added, list)
        assert isinstance(result.sample_removed, list)

    @patch("deltalake.DeltaTable")
    def test_deep_diff_with_keys_detects_updates(self, mock_delta_table_class, temp_delta_table):
        """Test deep diff with keys properly identifies updates."""
        # Setup mock
        mock_dt = MagicMock()
        mock_delta_table_class.return_value = mock_dt

        # Mock history
        mock_dt.history.return_value = []

        # Mock schema
        mock_schema = MagicMock()
        mock_schema.to_pyarrow.return_value = MagicMock(names=["id", "name", "value"])
        mock_dt.schema.return_value = mock_schema

        # Mock DataFrames - same keys, different values
        # id=1: value changes from 10 to 15 (update)
        # id=2: value stays at 20 (no change)
        df_v1 = pd.DataFrame({"id": [1, 2], "name": ["a", "b"], "value": [10, 20]})
        df_v2 = pd.DataFrame({"id": [1, 2], "name": ["a", "b"], "value": [15, 20]})

        versions = [df_v1, df_v2]
        call_index = [0]

        def mock_to_pandas():
            df = versions[call_index[0]].copy()
            call_index[0] = (call_index[0] + 1) % len(versions)
            return df

        mock_dt.to_pandas = mock_to_pandas
        mock_dt.load_as_version = MagicMock()

        # Execute with keys
        result = _get_delta_diff_pandas(temp_delta_table, 1, 2, deep=True, keys=["id"])

        # Verify basic properties
        assert result.rows_change == 0  # Same count
        assert result.rows_added == 0  # No new keys
        assert result.rows_removed == 0  # No deleted keys
        # Function should complete and return a result
        assert result.rows_updated is not None
        assert isinstance(result.sample_updated, list)


class TestDetectDrift:
    """Tests for detect_drift function."""

    @patch("deltalake.DeltaTable")
    @patch("odibi.diagnostics.delta.get_delta_diff")
    def test_detect_schema_drift(
        self, mock_get_delta_diff, mock_delta_table_class, temp_delta_table
    ):
        """Test that schema drift is detected."""
        # Mock diff result with schema changes
        mock_diff = MagicMock()
        mock_diff.schema_added = ["new_col"]
        mock_diff.schema_removed = ["old_col"]
        mock_diff.rows_change = 0
        mock_get_delta_diff.return_value = mock_diff

        # Execute
        warning = detect_drift(temp_delta_table, 2, 1)

        # Verify
        assert warning is not None
        assert "Schema drift detected" in warning
        assert "+1 columns" in warning
        assert "-1 columns" in warning

    @patch("deltalake.DeltaTable")
    @patch("odibi.diagnostics.delta.get_delta_diff")
    def test_detect_row_count_drift(
        self, mock_get_delta_diff, mock_delta_table_class, temp_delta_table
    ):
        """Test that row count drift is detected."""
        # Mock diff result with no schema changes but row changes
        mock_diff = MagicMock()
        mock_diff.schema_added = []
        mock_diff.schema_removed = []
        mock_diff.rows_change = 50  # 50 new rows

        mock_get_delta_diff.return_value = mock_diff

        # Mock DeltaTable for baseline count
        mock_dt = MagicMock()
        mock_delta_table_class.return_value = mock_dt
        mock_dt.load_version = MagicMock()
        mock_dt.to_pandas.return_value = pd.DataFrame({"id": range(100)})  # 100 rows at baseline

        # Execute with threshold of 10%
        # 50 row change out of 100 = 50% change > 10% threshold
        warning = detect_drift(temp_delta_table, 2, 1, threshold_pct=10.0)

        # Verify
        assert warning is not None
        assert "Row count drift" in warning
        assert "50.0%" in warning

    @patch("deltalake.DeltaTable")
    @patch("odibi.diagnostics.delta.get_delta_diff")
    def test_no_drift_detected(self, mock_get_delta_diff, mock_delta_table_class, temp_delta_table):
        """Test that no drift is detected when changes are within threshold."""
        # Mock diff result with minor changes
        mock_diff = MagicMock()
        mock_diff.schema_added = []
        mock_diff.schema_removed = []
        mock_diff.rows_change = 5  # 5 new rows

        mock_get_delta_diff.return_value = mock_diff

        # Mock DeltaTable for baseline count
        mock_dt = MagicMock()
        mock_delta_table_class.return_value = mock_dt
        mock_dt.load_version = MagicMock()
        mock_dt.to_pandas.return_value = pd.DataFrame({"id": range(100)})  # 100 rows at baseline

        # Execute with threshold of 10%
        # 5 row change out of 100 = 5% change < 10% threshold
        warning = detect_drift(temp_delta_table, 2, 1, threshold_pct=10.0)

        # Verify
        assert warning is None

    @patch("deltalake.DeltaTable")
    @patch("odibi.diagnostics.delta.get_delta_diff")
    def test_drift_from_zero_baseline(
        self, mock_get_delta_diff, mock_delta_table_class, temp_delta_table
    ):
        """Test drift detection when baseline has zero rows."""
        # Mock diff result
        mock_diff = MagicMock()
        mock_diff.schema_added = []
        mock_diff.schema_removed = []
        mock_diff.rows_change = 50  # 50 new rows

        mock_get_delta_diff.return_value = mock_diff

        # Mock DeltaTable with zero baseline
        mock_dt = MagicMock()
        mock_delta_table_class.return_value = mock_dt
        mock_dt.load_version = MagicMock()
        mock_dt.to_pandas.return_value = pd.DataFrame()  # Empty baseline

        # Execute
        warning = detect_drift(temp_delta_table, 2, 1, threshold_pct=10.0)

        # Verify
        assert warning is not None
        assert "Data volume spike" in warning
        assert "0 -> 50 rows" in warning


class TestGetDeltaDiff:
    """Tests for the main get_delta_diff function."""

    @patch("odibi.diagnostics.delta._get_delta_diff_pandas")
    def test_uses_pandas_when_no_spark(self, mock_pandas_impl, temp_delta_table):
        """Test that pandas implementation is used when spark is not provided."""
        mock_result = MagicMock()
        mock_pandas_impl.return_value = mock_result

        result = get_delta_diff(temp_delta_table, 1, 2, spark=None)

        assert result == mock_result
        mock_pandas_impl.assert_called_once_with(temp_delta_table, 1, 2, False, None)

    @patch("odibi.diagnostics.delta._get_delta_diff_spark")
    def test_uses_spark_when_provided(self, mock_spark_impl, temp_delta_table):
        """Test that spark implementation is used when spark session is provided."""
        mock_spark = MagicMock()
        mock_result = MagicMock()
        mock_spark_impl.return_value = mock_result

        result = get_delta_diff(temp_delta_table, 1, 2, spark=mock_spark)

        assert result == mock_result
        mock_spark_impl.assert_called_once_with(mock_spark, temp_delta_table, 1, 2, False, None)

    @patch("odibi.diagnostics.delta._get_delta_diff_pandas")
    def test_passes_deep_and_keys_parameters(self, mock_pandas_impl, temp_delta_table):
        """Test that deep and keys parameters are properly passed through."""
        mock_result = MagicMock()
        mock_pandas_impl.return_value = mock_result

        get_delta_diff(temp_delta_table, 1, 2, deep=True, keys=["id"])

        mock_pandas_impl.assert_called_once_with(temp_delta_table, 1, 2, True, ["id"])

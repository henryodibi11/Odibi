"""
Unit tests for odibi.diagnostics.delta module.

Tests the DeltaDiffResult dataclass, detect_drift function,
and _get_delta_diff_pandas with mocked Delta Lake dependencies.
"""

from unittest.mock import MagicMock, patch

import pandas as pd

from odibi.diagnostics.delta import (
    DeltaDiffResult,
    _get_delta_diff_pandas,
    detect_drift,
    get_delta_diff,
)


class TestDeltaDiffResult:
    """Tests for DeltaDiffResult dataclass."""

    def test_deltadiffresult_basic_fields(self):
        """Test that DeltaDiffResult can be instantiated with required fields."""
        result = DeltaDiffResult(
            table_path="/path/to/table",
            version_a=1,
            version_b=2,
            rows_change=100,
            files_change=5,
            size_change_bytes=1024,
            schema_added=["new_col"],
            schema_removed=["old_col"],
        )

        assert result.table_path == "/path/to/table"
        assert result.version_a == 1
        assert result.version_b == 2
        assert result.rows_change == 100
        assert result.files_change == 5
        assert result.size_change_bytes == 1024
        assert result.schema_added == ["new_col"]
        assert result.schema_removed == ["old_col"]

    def test_deltadiffresult_operations_field(self):
        """Test that operations field exists and can be set (renamed from operations_between)."""
        result = DeltaDiffResult(
            table_path="/path/to/table",
            version_a=1,
            version_b=2,
            rows_change=0,
            files_change=0,
            size_change_bytes=0,
            schema_added=[],
            schema_removed=[],
            operations=["WRITE", "OPTIMIZE"],
        )

        assert result.operations == ["WRITE", "OPTIMIZE"]

    def test_deltadiffresult_optional_fields(self):
        """Test that optional fields can be set."""
        result = DeltaDiffResult(
            table_path="/path/to/table",
            version_a=1,
            version_b=2,
            rows_change=100,
            files_change=5,
            size_change_bytes=1024,
            schema_added=[],
            schema_removed=[],
            schema_current=["col1", "col2"],
            schema_previous=["col1"],
            rows_added=50,
            rows_removed=10,
            rows_updated=20,
            sample_added=[{"id": 1}],
            sample_removed=[{"id": 2}],
            sample_updated=[{"id": 3}],
        )

        assert result.schema_current == ["col1", "col2"]
        assert result.schema_previous == ["col1"]
        assert result.rows_added == 50
        assert result.rows_removed == 10
        assert result.rows_updated == 20
        assert result.sample_added == [{"id": 1}]
        assert result.sample_removed == [{"id": 2}]
        assert result.sample_updated == [{"id": 3}]


class TestDetectDrift:
    """Tests for detect_drift function."""

    @patch("odibi.diagnostics.delta.get_delta_diff")
    @patch("deltalake.DeltaTable")
    def test_detect_drift_schema_added(self, mock_delta_table, mock_get_delta_diff):
        """Test drift detection when columns are added."""
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
        mock_get_delta_diff.return_value = mock_diff

        # Mock DeltaTable.to_pandas() to return data with proper count
        mock_dt_instance = MagicMock()
        mock_dt_instance.to_pandas.return_value = pd.DataFrame({"col1": [1, 2, 3]})
        mock_delta_table.return_value = mock_dt_instance

        warning = detect_drift("/path/to/table", 2, 1, spark=None, threshold_pct=10.0)

        assert warning is not None
        assert "Schema drift detected" in warning
        assert "+2 columns" in warning

    @patch("odibi.diagnostics.delta.get_delta_diff")
    @patch("deltalake.DeltaTable")
    def test_detect_drift_schema_removed(self, mock_delta_table, mock_get_delta_diff):
        """Test drift detection when columns are removed."""
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
        mock_get_delta_diff.return_value = mock_diff

        # Mock DeltaTable.to_pandas() to return data with proper count
        mock_dt_instance = MagicMock()
        mock_dt_instance.to_pandas.return_value = pd.DataFrame({"col1": [1, 2, 3]})
        mock_delta_table.return_value = mock_dt_instance

        warning = detect_drift("/path/to/table", 2, 1, spark=None, threshold_pct=10.0)

        assert warning is not None
        assert "Schema drift detected" in warning
        assert "-1 columns" in warning

    @patch("odibi.diagnostics.delta.get_delta_diff")
    @patch("deltalake.DeltaTable")
    def test_detect_drift_row_count_change_exceeds_threshold(
        self, mock_delta_table, mock_get_delta_diff
    ):
        """Test drift detection when row count change exceeds threshold."""
        mock_diff = DeltaDiffResult(
            table_path="/path/to/table",
            version_a=1,
            version_b=2,
            rows_change=50,  # 50% change
            files_change=0,
            size_change_bytes=0,
            schema_added=[],
            schema_removed=[],
        )
        mock_get_delta_diff.return_value = mock_diff

        # Mock DeltaTable.to_pandas() to return baseline with 100 rows
        mock_dt_instance = MagicMock()
        mock_dt_instance.to_pandas.return_value = pd.DataFrame({"col1": range(100)})
        mock_delta_table.return_value = mock_dt_instance

        warning = detect_drift("/path/to/table", 2, 1, spark=None, threshold_pct=10.0)

        assert warning is not None
        assert "Row count drift" in warning
        assert "50.0%" in warning

    @patch("odibi.diagnostics.delta.get_delta_diff")
    @patch("deltalake.DeltaTable")
    def test_detect_drift_no_drift(self, mock_delta_table, mock_get_delta_diff):
        """Test that no warning is returned when there's no significant drift."""
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
        mock_get_delta_diff.return_value = mock_diff

        # Mock DeltaTable.to_pandas() to return baseline with 100 rows
        mock_dt_instance = MagicMock()
        mock_dt_instance.to_pandas.return_value = pd.DataFrame({"col1": range(100)})
        mock_delta_table.return_value = mock_dt_instance

        warning = detect_drift("/path/to/table", 2, 1, spark=None, threshold_pct=10.0)

        assert warning is None

    @patch("odibi.diagnostics.delta.get_delta_diff")
    @patch("deltalake.DeltaTable")
    def test_detect_drift_zero_baseline(self, mock_delta_table, mock_get_delta_diff):
        """Test drift detection when baseline has zero rows."""
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
        mock_get_delta_diff.return_value = mock_diff

        # Mock DeltaTable.to_pandas() to return empty baseline
        mock_dt_instance = MagicMock()
        mock_dt_instance.to_pandas.return_value = pd.DataFrame()
        mock_delta_table.return_value = mock_dt_instance

        warning = detect_drift("/path/to/table", 2, 1, spark=None, threshold_pct=10.0)

        assert warning is not None
        assert "Data volume spike" in warning
        assert "0 -> 100 rows" in warning


class TestGetDeltaDiffPandas:
    """Tests for _get_delta_diff_pandas function."""

    @patch("deltalake.DeltaTable")
    def test_get_delta_diff_pandas_basic(self, mock_delta_table_class):
        """Test basic metadata diff without deep comparison."""
        # Mock DeltaTable instance
        mock_dt = MagicMock()

        # Mock history
        mock_dt.history.return_value = [
            {"version": 2, "operation": "WRITE"},
            {"version": 1, "operation": "CREATE"},
        ]

        # Mock schema for version_a
        mock_schema_a = MagicMock()
        mock_schema_a.to_pyarrow.return_value = MagicMock(names=["col1", "col2"])

        # Mock to_pandas for version_a (called 3 times: len(), df_a, df_b)
        df_a = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        df_b = pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5], "col3": [6, 7, 8]})

        # Configure mock to return different data per call
        mock_dt.schema.return_value = mock_schema_a
        mock_dt.to_pandas.side_effect = [df_a, df_a, df_b]  # 3 calls

        mock_delta_table_class.return_value = mock_dt

        result = _get_delta_diff_pandas("/path/to/table", 1, 2, deep=False, keys=None)

        assert result.table_path == "/path/to/table"
        assert result.version_a == 1
        assert result.version_b == 2
        assert result.rows_change == 1  # 3 - 2
        assert result.schema_added == ["col3"]
        assert result.schema_removed == []
        assert result.operations == ["WRITE"]

    @patch("deltalake.DeltaTable")
    def test_get_delta_diff_pandas_deep_with_keys(self, mock_delta_table_class):
        """Test deep diff with key-based comparison."""
        mock_dt = MagicMock()

        # Mock history
        mock_dt.history.return_value = [
            {"version": 2, "operation": "MERGE"},
            {"version": 1, "operation": "WRITE"},
        ]

        # Mock schema
        mock_schema = MagicMock()
        mock_schema.to_pyarrow.return_value = MagicMock(names=["id", "value"])

        # Mock dataframes (3 calls: len(), df_a, df_b)
        df_a = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        df_b = pd.DataFrame(
            {
                "id": [1, 2, 4],  # id=3 removed, id=4 added, id=2 updated
                "value": ["a", "b_updated", "d"],
            }
        )

        mock_dt.schema.return_value = mock_schema
        mock_dt.to_pandas.side_effect = [df_a, df_a, df_b]  # 3 calls

        mock_delta_table_class.return_value = mock_dt

        result = _get_delta_diff_pandas("/path/to/table", 1, 2, deep=True, keys=["id"])

        assert result.rows_added == 1  # id=4
        assert result.rows_removed == 1  # id=3
        assert result.rows_updated == 1  # id=2
        assert len(result.sample_added) > 0
        assert len(result.sample_removed) > 0
        assert len(result.sample_updated) > 0

    @patch("deltalake.DeltaTable")
    def test_get_delta_diff_pandas_deep_without_keys(self, mock_delta_table_class):
        """Test deep diff with set-based comparison (no keys)."""
        mock_dt = MagicMock()

        # Mock history
        mock_dt.history.return_value = [{"version": 2, "operation": "WRITE"}]

        # Mock schema
        mock_schema = MagicMock()
        mock_schema.to_pyarrow.return_value = MagicMock(names=["col1", "col2"])

        # Mock dataframes (3 calls: len(), df_a, df_b)
        df_a = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        df_b = pd.DataFrame({"col1": [1, 3], "col2": ["a", "c"]})

        mock_dt.schema.return_value = mock_schema
        mock_dt.to_pandas.side_effect = [df_a, df_a, df_b]  # 3 calls

        mock_delta_table_class.return_value = mock_dt

        result = _get_delta_diff_pandas("/path/to/table", 1, 2, deep=True, keys=None)

        assert result.rows_added == 1
        assert result.rows_removed == 1
        assert result.rows_updated is None  # No update detection without keys


class TestGetDeltaDiff:
    """Tests for get_delta_diff routing function."""

    @patch("odibi.diagnostics.delta._get_delta_diff_pandas")
    def test_get_delta_diff_routes_to_pandas(self, mock_pandas_impl):
        """Test that get_delta_diff routes to pandas implementation when spark is None."""
        mock_result = DeltaDiffResult(
            table_path="/path/to/table",
            version_a=1,
            version_b=2,
            rows_change=0,
            files_change=0,
            size_change_bytes=0,
            schema_added=[],
            schema_removed=[],
        )
        mock_pandas_impl.return_value = mock_result

        result = get_delta_diff("/path/to/table", 1, 2, spark=None, deep=False, keys=None)

        mock_pandas_impl.assert_called_once_with("/path/to/table", 1, 2, False, None)
        assert result == mock_result

    @patch("odibi.diagnostics.delta._get_delta_diff_spark")
    def test_get_delta_diff_routes_to_spark(self, mock_spark_impl):
        """Test that get_delta_diff routes to spark implementation when spark is provided."""
        mock_spark = MagicMock()
        mock_result = DeltaDiffResult(
            table_path="/path/to/table",
            version_a=1,
            version_b=2,
            rows_change=0,
            files_change=0,
            size_change_bytes=0,
            schema_added=[],
            schema_removed=[],
        )
        mock_spark_impl.return_value = mock_result

        result = get_delta_diff("/path/to/table", 1, 2, spark=mock_spark, deep=True, keys=["id"])

        mock_spark_impl.assert_called_once_with(mock_spark, "/path/to/table", 1, 2, True, ["id"])
        assert result == mock_result

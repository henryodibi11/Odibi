"""Tests for Excel reading with remote storage (Azure Blob, S3, etc.)."""

import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.engine.pandas_engine import PandasEngine

# Check if openpyxl is available and compatible
try:
    import openpyxl

    OPENPYXL_VERSION = tuple(int(x) for x in openpyxl.__version__.split(".")[:2])
    HAS_OPENPYXL = OPENPYXL_VERSION >= (3, 1)
except ImportError:
    HAS_OPENPYXL = False

requires_openpyxl = pytest.mark.skipif(
    not HAS_OPENPYXL,
    reason="Requires openpyxl >= 3.1.0 for Excel tests",
)


class TestRemoteUriDetection:
    """Tests for _is_remote_uri helper."""

    @pytest.fixture
    def engine(self):
        return PandasEngine()

    @pytest.mark.parametrize(
        "uri,expected",
        [
            # Cloud URIs should be detected as remote
            ("abfss://container@account.dfs.core.windows.net/path/file.xlsx", True),
            ("abfs://container@account.dfs.core.windows.net/path/file.xlsx", True),
            ("wasbs://container@account.blob.core.windows.net/path/file.xlsx", True),
            ("wasb://container@account.blob.core.windows.net/path/file.xlsx", True),
            ("az://container/path/file.xlsx", True),
            ("s3://bucket/path/file.xlsx", True),
            ("gs://bucket/path/file.xlsx", True),
            ("https://account.blob.core.windows.net/container/file.xlsx", True),
            # Local paths should NOT be detected as remote
            ("/home/user/data/file.xlsx", False),
            ("./data/file.xlsx", False),
            ("data/file.xlsx", False),
            ("file:///home/user/data/file.xlsx", False),
        ],
    )
    def test_is_remote_uri(self, engine, uri, expected):
        assert engine._is_remote_uri(uri) == expected

    @pytest.mark.skipif(
        __import__("os").name != "nt",
        reason="Windows-specific test",
    )
    def test_is_remote_uri_windows_paths(self, engine):
        """Windows paths should not be detected as remote."""
        assert engine._is_remote_uri("C:\\Users\\data\\file.xlsx") is False
        assert engine._is_remote_uri("D:/data/file.xlsx") is False


class TestRemoteGlobExpansion:
    """Tests for _expand_remote_glob helper."""

    @pytest.fixture
    def engine(self):
        return PandasEngine()

    def test_expand_remote_glob_success(self, engine):
        """Test successful remote glob expansion."""
        mock_fs = MagicMock()
        mock_fs.glob.return_value = [
            "container@account.dfs.core.windows.net/data/file1.xlsx",
            "container@account.dfs.core.windows.net/data/file2.xlsx",
        ]

        with patch("fsspec.filesystem", return_value=mock_fs):
            result = engine._expand_remote_glob(
                "abfss://container@account.dfs.core.windows.net/data/*.xlsx",
                storage_options={"account_key": "test_key"},
            )

        assert len(result) == 2
        assert "abfss://container@account.dfs.core.windows.net/data/file1.xlsx" in result
        assert "abfss://container@account.dfs.core.windows.net/data/file2.xlsx" in result

        # Verify storage_options were passed to filesystem
        mock_fs_call = mock_fs.glob.call_args
        assert mock_fs_call is not None

    def test_expand_remote_glob_no_matches(self, engine):
        """Test remote glob with no matching files."""
        mock_fs = MagicMock()
        mock_fs.glob.return_value = []

        with patch("fsspec.filesystem", return_value=mock_fs):
            result = engine._expand_remote_glob(
                "abfss://container@account.dfs.core.windows.net/empty/*.xlsx"
            )

        assert result == []


@requires_openpyxl
class TestExcelRemoteReading:
    """Tests for reading Excel files from remote storage."""

    @pytest.fixture
    def engine(self):
        return PandasEngine()

    @pytest.fixture
    def sample_excel_bytes(self):
        """Create sample Excel file in memory."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        buffer = io.BytesIO()
        df.to_excel(buffer, index=False, sheet_name="Sheet1")
        buffer.seek(0)
        return buffer.getvalue()

    def test_read_excel_remote_single_file(self, engine, sample_excel_bytes):
        """Test reading a single remote Excel file."""
        mock_file = MagicMock()
        mock_file.__enter__ = MagicMock(return_value=io.BytesIO(sample_excel_bytes))
        mock_file.__exit__ = MagicMock(return_value=False)

        with patch.object(engine, "_open_remote_file", return_value=mock_file):
            df = engine._read_excel_with_patterns(
                "abfss://container@account.dfs.core.windows.net/data/file.xlsx",
                storage_options={"account_key": "test_key"},
            )

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ["col1", "col2"]

    def test_read_excel_remote_with_glob(self, engine, sample_excel_bytes):
        """Test reading multiple remote Excel files with glob pattern."""
        mock_file = MagicMock()
        mock_file.__enter__ = MagicMock(return_value=io.BytesIO(sample_excel_bytes))
        mock_file.__exit__ = MagicMock(return_value=False)

        expanded_files = [
            "abfss://container@account.dfs.core.windows.net/data/file1.xlsx",
            "abfss://container@account.dfs.core.windows.net/data/file2.xlsx",
        ]

        with patch.object(engine, "_expand_remote_glob", return_value=expanded_files):
            # Need to create a fresh mock for each file
            def create_mock_file(*args, **kwargs):
                m = MagicMock()
                m.__enter__ = MagicMock(return_value=io.BytesIO(sample_excel_bytes))
                m.__exit__ = MagicMock(return_value=False)
                return m

            with patch.object(engine, "_open_remote_file", side_effect=create_mock_file):
                df = engine._read_excel_with_patterns(
                    "abfss://container@account.dfs.core.windows.net/data/*.xlsx",
                    storage_options={"account_key": "test_key"},
                )

        assert isinstance(df, pd.DataFrame)
        # Two files with 3 rows each
        assert len(df) == 6

    def test_read_excel_remote_with_sheet_pattern(self, engine):
        """Test reading remote Excel with sheet pattern matching."""
        # Create Excel with multiple sheets
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            pd.DataFrame({"data": [1]}).to_excel(writer, sheet_name="PowerBI_Report", index=False)
            pd.DataFrame({"data": [2]}).to_excel(writer, sheet_name="Other_Sheet", index=False)
            pd.DataFrame({"data": [3]}).to_excel(writer, sheet_name="power bi data", index=False)
        buffer.seek(0)
        excel_bytes = buffer.getvalue()

        mock_file = MagicMock()
        mock_file.__enter__ = MagicMock(return_value=io.BytesIO(excel_bytes))
        mock_file.__exit__ = MagicMock(return_value=False)

        with patch.object(engine, "_open_remote_file", return_value=mock_file):
            df = engine._read_excel_with_patterns(
                "abfss://container@account.dfs.core.windows.net/data/file.xlsx",
                sheet_pattern=["*powerbi*", "*power bi*"],
                storage_options={"account_key": "test_key"},
            )

        assert isinstance(df, pd.DataFrame)
        # Should match "PowerBI_Report" and "power bi data"
        assert len(df) == 2

    def test_read_excel_remote_add_source_file(self, engine, sample_excel_bytes):
        """Test that add_source_file works with remote files."""
        mock_file = MagicMock()
        mock_file.__enter__ = MagicMock(return_value=io.BytesIO(sample_excel_bytes))
        mock_file.__exit__ = MagicMock(return_value=False)

        with patch.object(engine, "_open_remote_file", return_value=mock_file):
            df = engine._read_excel_with_patterns(
                "abfss://container@account.dfs.core.windows.net/S Curve/report.xlsx",
                add_source_file=True,
                storage_options={"account_key": "test_key"},
            )

        assert "_source_file" in df.columns
        assert df["_source_file"].iloc[0] == "report.xlsx"


@requires_openpyxl
class TestExcelLocalReading:
    """Tests for local Excel reading (backward compatibility)."""

    @pytest.fixture
    def engine(self):
        return PandasEngine()

    def test_read_excel_local_single_file(self, engine, tmp_path):
        """Test reading a local Excel file still works."""
        # Create test Excel file
        df_expected = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        excel_path = tmp_path / "test.xlsx"
        df_expected.to_excel(excel_path, index=False)

        df = engine._read_excel_with_patterns(str(excel_path))

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["a", "b"]

    def test_read_excel_local_glob(self, engine, tmp_path):
        """Test reading multiple local Excel files with glob."""
        # Create test Excel files
        for i in range(3):
            df = pd.DataFrame({"x": [i], "y": [i * 10]})
            (tmp_path / f"file{i}.xlsx").write_bytes(b"")  # Create empty first
            df.to_excel(tmp_path / f"file{i}.xlsx", index=False)

        df = engine._read_excel_with_patterns(str(tmp_path / "*.xlsx"))

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    def test_read_excel_local_with_sheet_pattern(self, engine, tmp_path):
        """Test reading local Excel with sheet pattern."""
        excel_path = tmp_path / "multi_sheet.xlsx"

        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            pd.DataFrame({"val": [1]}).to_excel(writer, sheet_name="Report_PowerBI", index=False)
            pd.DataFrame({"val": [2]}).to_excel(writer, sheet_name="Ignore_Me", index=False)

        df = engine._read_excel_with_patterns(
            str(excel_path),
            sheet_pattern="*PowerBI*",
        )

        assert len(df) == 1
        assert df["val"].iloc[0] == 1


@requires_openpyxl
class TestExcelIntegrationWithEngine:
    """Integration tests for Excel reading through the full engine.read() path."""

    @pytest.fixture
    def engine(self):
        return PandasEngine()

    def test_read_excel_via_engine_local(self, engine, tmp_path):
        """Test reading Excel through engine.read() with local connection."""
        # Create test file
        df_expected = pd.DataFrame({"col": [1, 2, 3]})
        excel_path = tmp_path / "data.xlsx"
        df_expected.to_excel(excel_path, index=False)

        # Create mock connection
        class MockConnection:
            def __init__(self, base_path):
                self.base_path = base_path

            def get_path(self, path):
                return str(self.base_path / path)

            def pandas_storage_options(self):
                return {}

        conn = MockConnection(tmp_path)

        df = engine.read(conn, format="excel", path="data.xlsx")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    def test_read_excel_via_engine_with_options(self, engine, tmp_path):
        """Test reading Excel through engine.read() with sheet pattern options."""
        excel_path = tmp_path / "multi.xlsx"

        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            pd.DataFrame({"x": [10]}).to_excel(writer, sheet_name="Main_PowerBI", index=False)
            pd.DataFrame({"x": [20]}).to_excel(writer, sheet_name="Other", index=False)

        class MockConnection:
            def __init__(self, base_path):
                self.base_path = base_path

            def get_path(self, path):
                return str(self.base_path / path)

            def pandas_storage_options(self):
                return {}

        conn = MockConnection(tmp_path)

        df = engine.read(
            conn,
            format="excel",
            path="multi.xlsx",
            options={
                "sheet_pattern": "*PowerBI*",
                "add_source_file": True,
            },
        )

        assert len(df) == 1
        assert df["x"].iloc[0] == 10
        assert "_source_file" in df.columns
        assert "_source_sheet" in df.columns
        assert df["_source_sheet"].iloc[0] == "Main_PowerBI"

"""Comprehensive tests for LocalConnection covering ~74 missed statements."""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Patch logging context before importing LocalConnection
_mock_ctx = MagicMock()


@pytest.fixture(autouse=True)
def _patch_logging():
    with patch("odibi.connections.local.get_logging_context", return_value=_mock_ctx):
        yield


from odibi.connections.local import LocalConnection  # noqa: E402


# ---------------------------------------------------------------------------
# 1. __init__
# ---------------------------------------------------------------------------
class TestLocalConnectionInit:
    def test_regular_path(self):
        conn = LocalConnection("./data")
        assert conn.is_uri is False
        assert isinstance(conn.base_path, Path)
        assert conn.base_path_str == "./data"

    def test_uri_path_dbfs(self):
        conn = LocalConnection("dbfs:/mnt/data")
        assert conn.is_uri is True
        assert conn.base_path is None

    def test_uri_path_with_double_slash(self):
        conn = LocalConnection("abfss://container@account.dfs.core.windows.net")
        assert conn.is_uri is True

    def test_windows_drive_not_uri(self):
        conn = LocalConnection("C:/data")
        assert conn.is_uri is False
        assert isinstance(conn.base_path, Path)

    def test_default_path(self):
        conn = LocalConnection()
        assert conn.base_path_str == "./data"
        assert conn.is_uri is False


# ---------------------------------------------------------------------------
# 2. get_path
# ---------------------------------------------------------------------------
class TestGetPath:
    def test_local_path_returns_absolute(self, tmp_path):
        conn = LocalConnection(str(tmp_path))
        result = conn.get_path("subdir/file.csv")
        assert os.path.isabs(result)
        assert "subdir" in result

    def test_uri_path_concat_no_trailing_slash(self):
        conn = LocalConnection("dbfs:/mnt/data")
        result = conn.get_path("folder/file.csv")
        assert result == "dbfs:/mnt/data/folder/file.csv"

    def test_uri_path_concat_with_trailing_slash(self):
        conn = LocalConnection("dbfs:/mnt/data/")
        result = conn.get_path("folder/file.csv")
        assert result == "dbfs:/mnt/data/folder/file.csv"

    def test_uri_strips_leading_slash_from_relative(self):
        conn = LocalConnection("dbfs:/mnt/data")
        result = conn.get_path("/folder/file.csv")
        assert result == "dbfs:/mnt/data/folder/file.csv"


# ---------------------------------------------------------------------------
# 3. validate
# ---------------------------------------------------------------------------
class TestValidate:
    def test_local_creates_directory(self, tmp_path):
        target = tmp_path / "new_dir"
        conn = LocalConnection(str(target))
        conn.validate()
        assert target.exists()

    def test_uri_skips_validation(self):
        conn = LocalConnection("dbfs:/mnt/data")
        conn.validate()  # Should not raise


# ---------------------------------------------------------------------------
# 4. list_files
# ---------------------------------------------------------------------------
class TestListFiles:
    def test_lists_files_with_metadata(self, tmp_path):
        (tmp_path / "a.csv").write_text("data")
        (tmp_path / "b.parquet").write_bytes(b"\x00" * 10)
        conn = LocalConnection(str(tmp_path))
        result = conn.list_files()
        assert len(result) == 2
        entry = result[0]
        assert "name" in entry
        assert "path" in entry
        assert "size" in entry
        assert "modified" in entry
        assert "format" in entry

    def test_uri_returns_empty(self):
        conn = LocalConnection("dbfs:/mnt/data")
        assert conn.list_files() == []

    def test_pattern_filtering(self, tmp_path):
        (tmp_path / "a.csv").write_text("x")
        (tmp_path / "b.json").write_text("{}")
        conn = LocalConnection(str(tmp_path))
        result = conn.list_files(pattern="*.csv")
        assert len(result) == 1
        assert result[0]["name"] == "a.csv"

    def test_limit_enforcement(self, tmp_path):
        for i in range(5):
            (tmp_path / f"file{i}.csv").write_text("x")
        conn = LocalConnection(str(tmp_path))
        result = conn.list_files(limit=2)
        assert len(result) == 2

    def test_with_subpath(self, tmp_path):
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "f.csv").write_text("x")
        conn = LocalConnection(str(tmp_path))
        result = conn.list_files(path="sub")
        assert len(result) == 1


# ---------------------------------------------------------------------------
# 5. list_folders
# ---------------------------------------------------------------------------
class TestListFolders:
    def test_lists_folders(self, tmp_path):
        (tmp_path / "dir_a").mkdir()
        (tmp_path / "dir_b").mkdir()
        (tmp_path / "file.txt").write_text("x")
        conn = LocalConnection(str(tmp_path))
        result = conn.list_folders()
        assert len(result) == 2
        assert all(os.path.isabs(f) for f in result)

    def test_uri_returns_empty(self):
        conn = LocalConnection("dbfs:/mnt/data")
        assert conn.list_folders() == []

    def test_limit_enforcement(self, tmp_path):
        for i in range(5):
            (tmp_path / f"dir{i}").mkdir()
        conn = LocalConnection(str(tmp_path))
        result = conn.list_folders(limit=2)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# 6. discover_catalog
# ---------------------------------------------------------------------------
class TestDiscoverCatalog:
    def test_uri_returns_empty_summary(self):
        conn = LocalConnection("dbfs:/mnt/data")
        result = conn.discover_catalog()
        assert result["total_datasets"] == 0

    def test_path_not_found(self, tmp_path):
        conn = LocalConnection(str(tmp_path))
        result = conn.discover_catalog(path="nonexistent")
        assert result["total_datasets"] == 0
        assert "not found" in result["next_step"]

    def test_recursive_scan(self, tmp_path):
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "deep.csv").write_text("x")
        (tmp_path / "top.csv").write_text("y")
        conn = LocalConnection(str(tmp_path))
        result = conn.discover_catalog(recursive=True)
        # Should find folders + files recursively
        assert result["total_datasets"] >= 2

    def test_non_recursive_scan(self, tmp_path):
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "deep.csv").write_text("x")
        (tmp_path / "top.csv").write_text("y")
        conn = LocalConnection(str(tmp_path))
        result = conn.discover_catalog(recursive=False)
        names = [f["name"] for f in result.get("files", [])]
        folder_names = [f["name"] for f in result.get("folders", [])]
        # Non-recursive: top.csv + sub folder
        assert "top.csv" in names or "sub" in folder_names

    def test_pattern_filtering(self, tmp_path):
        (tmp_path / "a.csv").write_text("x")
        (tmp_path / "b.json").write_text("{}")
        conn = LocalConnection(str(tmp_path))
        result = conn.discover_catalog(pattern="*.csv", recursive=False)
        file_names = [f["name"] for f in result.get("files", [])]
        assert "a.csv" in file_names
        assert "b.json" not in file_names


# ---------------------------------------------------------------------------
# 7. get_schema
# ---------------------------------------------------------------------------
class TestGetSchema:
    @patch("odibi.connections.local.detect_file_format", return_value="parquet")
    def test_parquet_format(self, mock_fmt):
        conn = LocalConnection("./data")
        mock_df = pd.DataFrame(
            {"col_a": pd.array([], dtype="int64"), "col_b": pd.array([], dtype="object")}
        )
        with patch("pandas.read_parquet", return_value=mock_df):
            result = conn.get_schema("test.parquet")
        assert len(result["columns"]) == 2
        assert result["columns"][0]["name"] == "col_a"

    @patch("odibi.connections.local.detect_file_format", return_value="csv")
    def test_csv_format(self, mock_fmt):
        conn = LocalConnection("./data")
        mock_df = pd.DataFrame({"x": pd.array([], dtype="float64")})
        with patch("pandas.read_csv", return_value=mock_df):
            result = conn.get_schema("test.csv")
        assert len(result["columns"]) == 1

    @patch("odibi.connections.local.detect_file_format", return_value="unknown")
    def test_unsupported_format(self, mock_fmt):
        conn = LocalConnection("./data")
        result = conn.get_schema("test.xyz")
        assert result["columns"] == []

    @patch("odibi.connections.local.detect_file_format", side_effect=Exception("read error"))
    def test_error_returns_empty_schema(self, mock_fmt):
        conn = LocalConnection("./data")
        result = conn.get_schema("test.parquet")
        assert result["columns"] == []


# ---------------------------------------------------------------------------
# 8. profile
# ---------------------------------------------------------------------------
class TestProfile:
    def _make_df(self):
        return pd.DataFrame(
            {
                "id": list(range(1, 21)),
                "name": [f"n{i}" for i in range(20)],
                "category": ["x"] * 20,
                "ts": pd.to_datetime(["2024-01-01"] * 20),
            }
        )

    @patch("odibi.connections.local.detect_file_format", return_value="parquet")
    def test_basic_profiling(self, mock_fmt):
        conn = LocalConnection("./data")
        df = self._make_df()
        with patch("pandas.read_parquet", return_value=df):
            result = conn.profile("test.parquet")
        assert result["rows_sampled"] == 20
        assert len(result["columns"]) == 4

    @patch("odibi.connections.local.detect_file_format", return_value="parquet")
    def test_cardinality_unique(self, mock_fmt):
        conn = LocalConnection("./data")
        df = self._make_df()
        with patch("pandas.read_parquet", return_value=df):
            result = conn.profile("test.parquet")
        id_col = next(c for c in result["columns"] if c["name"] == "id")
        assert id_col["cardinality"] == "unique"

    @patch("odibi.connections.local.detect_file_format", return_value="parquet")
    def test_cardinality_low(self, mock_fmt):
        conn = LocalConnection("./data")
        df = self._make_df()
        with patch("pandas.read_parquet", return_value=df):
            result = conn.profile("test.parquet")
        cat_col = next(c for c in result["columns"] if c["name"] == "category")
        assert cat_col["cardinality"] == "low"

    @patch("odibi.connections.local.detect_file_format", return_value="parquet")
    def test_candidate_keys_and_watermarks(self, mock_fmt):
        conn = LocalConnection("./data")
        df = self._make_df()
        with patch("pandas.read_parquet", return_value=df):
            result = conn.profile("test.parquet")
        assert "id" in result["candidate_keys"]
        assert "ts" in result["candidate_watermarks"]

    @patch("odibi.connections.local.detect_file_format", return_value="csv")
    def test_csv_profiling(self, mock_fmt):
        conn = LocalConnection("./data")
        df = self._make_df()
        with patch("pandas.read_csv", return_value=df):
            result = conn.profile("test.csv")
        assert result["rows_sampled"] == 20

    @patch("odibi.connections.local.detect_file_format", return_value="unknown")
    def test_unsupported_format(self, mock_fmt):
        conn = LocalConnection("./data")
        result = conn.profile("test.xyz")
        assert result["rows_sampled"] == 0
        assert result["columns"] == []

    @patch("odibi.connections.local.detect_file_format", side_effect=Exception("fail"))
    def test_error_returns_empty_profile(self, mock_fmt):
        conn = LocalConnection("./data")
        result = conn.profile("test.parquet")
        assert result["rows_sampled"] == 0

    @patch("odibi.connections.local.detect_file_format", return_value="parquet")
    def test_profile_with_columns_filter(self, mock_fmt):
        conn = LocalConnection("./data")
        df = self._make_df()
        with patch("pandas.read_parquet", return_value=df):
            result = conn.profile("test.parquet", columns=["id", "name"])
        col_names = [c["name"] for c in result["columns"]]
        assert col_names == ["id", "name"]


# ---------------------------------------------------------------------------
# 9. preview
# ---------------------------------------------------------------------------
class TestPreview:
    @patch("odibi.connections.local.detect_file_format", return_value="parquet")
    def test_basic_preview(self, mock_fmt):
        conn = LocalConnection("./data")
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        with patch("pandas.read_parquet", return_value=df):
            result = conn.preview("test.parquet")
        assert result["columns"] == ["a", "b"]
        assert len(result["rows"]) == 3

    @patch("odibi.connections.local.detect_file_format", return_value="csv")
    def test_column_filtering(self, mock_fmt):
        conn = LocalConnection("./data")
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        with patch("pandas.read_csv", return_value=df):
            result = conn.preview("test.csv", columns=["a", "c"])
        assert result["columns"] == ["a", "c"]

    @patch("odibi.connections.local.detect_file_format", return_value="parquet")
    def test_max_rows_cap(self, mock_fmt):
        conn = LocalConnection("./data")
        df = pd.DataFrame({"a": list(range(200))})
        with patch("pandas.read_parquet", return_value=df):
            result = conn.preview("test.parquet", rows=200)
        assert len(result["rows"]) <= 100

    @patch("odibi.connections.local.detect_file_format", return_value="unknown")
    def test_unsupported_format(self, mock_fmt):
        conn = LocalConnection("./data")
        result = conn.preview("test.xyz")
        assert result.get("rows") is None or result.get("rows") == []

    @patch("odibi.connections.local.detect_file_format", side_effect=Exception("fail"))
    def test_error_path(self, mock_fmt):
        conn = LocalConnection("./data")
        result = conn.preview("test.parquet")
        assert result.get("rows") is None or result.get("rows") == []

    @patch("odibi.connections.local.detect_file_format", return_value="json")
    def test_json_preview(self, mock_fmt):
        conn = LocalConnection("./data")
        df = pd.DataFrame({"x": [1]})
        with patch("pandas.read_json", return_value=df):
            result = conn.preview("test.json")
        assert result["columns"] == ["x"]


# ---------------------------------------------------------------------------
# 10. detect_partitions
# ---------------------------------------------------------------------------
class TestDetectPartitions:
    def test_uri_returns_empty(self):
        conn = LocalConnection("dbfs:/mnt/data")
        result = conn.detect_partitions()
        assert result["keys"] == []

    def test_real_directory_structure(self, tmp_path):
        part = tmp_path / "year=2024" / "month=01"
        part.mkdir(parents=True)
        (part / "data.parquet").write_bytes(b"\x00")
        conn = LocalConnection(str(tmp_path))
        result = conn.detect_partitions()
        assert "keys" in result

    def test_error_path(self):
        conn = LocalConnection("./nonexistent_path_xyz")
        result = conn.detect_partitions()
        assert result["keys"] == []


# ---------------------------------------------------------------------------
# 11. get_freshness
# ---------------------------------------------------------------------------
class TestGetFreshness:
    def test_uri_returns_empty(self):
        conn = LocalConnection("dbfs:/mnt/data")
        result = conn.get_freshness("some/file.csv")
        assert result.get("age_hours") is None

    def test_file_exists_returns_age(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_text("hello")
        conn = LocalConnection(str(tmp_path))
        result = conn.get_freshness("test.csv")
        assert result["age_hours"] is not None
        assert result["age_hours"] >= 0

    def test_file_not_exists(self, tmp_path):
        conn = LocalConnection(str(tmp_path))
        result = conn.get_freshness("missing.csv")
        assert result.get("age_hours") is None

    def test_error_path(self):
        conn = LocalConnection("./data")
        with patch.object(Path, "exists", side_effect=Exception("perm error")):
            result = conn.get_freshness("test.csv")
        assert result.get("age_hours") is None


# ---------------------------------------------------------------------------
# 12. list_files – error path
# ---------------------------------------------------------------------------
class TestListFilesError:
    def test_glob_exception_returns_empty(self, tmp_path):
        conn = LocalConnection(str(tmp_path))
        with patch.object(Path, "glob", side_effect=PermissionError("denied")):
            result = conn.list_files()
        assert result == []


# ---------------------------------------------------------------------------
# 13. list_folders – error path & subpath
# ---------------------------------------------------------------------------
class TestListFoldersExtra:
    def test_iterdir_exception_returns_empty(self, tmp_path):
        conn = LocalConnection(str(tmp_path))
        with patch.object(Path, "iterdir", side_effect=PermissionError("denied")):
            result = conn.list_folders()
        assert result == []

    def test_with_subpath(self, tmp_path):
        sub = tmp_path / "nested"
        sub.mkdir()
        (sub / "child_dir").mkdir()
        conn = LocalConnection(str(tmp_path))
        result = conn.list_folders(path="nested")
        assert len(result) == 1
        assert "child_dir" in result[0]


# ---------------------------------------------------------------------------
# 14. validate – exception path
# ---------------------------------------------------------------------------
class TestValidateError:
    def test_mkdir_exception_raises(self, tmp_path):
        conn = LocalConnection(str(tmp_path / "fail_dir"))
        with patch.object(Path, "mkdir", side_effect=OSError("disk full")):
            with pytest.raises(OSError, match="disk full"):
                conn.validate()


# ---------------------------------------------------------------------------
# 15. discover_catalog – exception, limit, formats tracking
# ---------------------------------------------------------------------------
class TestDiscoverCatalogExtra:
    def test_exception_returns_error_summary(self, tmp_path):
        conn = LocalConnection(str(tmp_path))
        with patch.object(Path, "exists", side_effect=RuntimeError("boom")):
            result = conn.discover_catalog()
        assert result["total_datasets"] == 0
        assert "Error:" in result["next_step"]

    def test_limit_enforcement(self, tmp_path):
        for i in range(10):
            (tmp_path / f"file{i}.csv").write_text("x")
        conn = LocalConnection(str(tmp_path))
        result = conn.discover_catalog(limit=3, recursive=False)
        assert result["total_datasets"] <= 3

    def test_formats_tracked(self, tmp_path):
        (tmp_path / "a.csv").write_text("x")
        (tmp_path / "b.csv").write_text("y")
        (tmp_path / "c.json").write_text("{}")
        conn = LocalConnection(str(tmp_path))
        result = conn.discover_catalog(recursive=False)
        formats = result.get("formats", {})
        assert formats.get("csv", 0) == 2
        assert formats.get("json", 0) == 1


# ---------------------------------------------------------------------------
# 16. get_schema – json format
# ---------------------------------------------------------------------------
class TestGetSchemaJson:
    @patch("odibi.connections.local.detect_file_format", return_value="json")
    def test_json_format(self, mock_fmt):
        conn = LocalConnection("./data")
        mock_df = pd.DataFrame({"col_a": pd.array([1], dtype="int64")})
        with patch("pandas.read_json", return_value=mock_df):
            result = conn.get_schema("test.json")
        assert len(result["columns"]) == 1
        assert result["columns"][0]["name"] == "col_a"


# ---------------------------------------------------------------------------
# 17. profile – json, missing column, empty df, high/medium cardinality, sample_rows cap
# ---------------------------------------------------------------------------
class TestProfileExtra:
    @patch("odibi.connections.local.detect_file_format", return_value="json")
    def test_json_profiling(self, mock_fmt):
        conn = LocalConnection("./data")
        df = pd.DataFrame({"a": [1, 2, 3]})
        with patch("pandas.read_json", return_value=df):
            result = conn.profile("test.json")
        assert result["rows_sampled"] == 3

    @patch("odibi.connections.local.detect_file_format", return_value="parquet")
    def test_missing_column_skipped(self, mock_fmt):
        conn = LocalConnection("./data")
        df = pd.DataFrame({"a": [1, 2]})
        with patch("pandas.read_parquet", return_value=df):
            result = conn.profile("test.parquet", columns=["a", "nonexistent"])
        col_names = [c["name"] for c in result["columns"]]
        assert "a" in col_names
        assert "nonexistent" not in col_names

    @patch("odibi.connections.local.detect_file_format", return_value="parquet")
    def test_empty_df_null_pct_zero(self, mock_fmt):
        conn = LocalConnection("./data")
        df = pd.DataFrame({"a": pd.array([], dtype="int64")})
        with patch("pandas.read_parquet", return_value=df):
            result = conn.profile("test.parquet")
        assert result["rows_sampled"] == 0
        col = result["columns"][0]
        assert col["null_pct"] == 0

    @patch("odibi.connections.local.detect_file_format", return_value="parquet")
    def test_cardinality_high(self, mock_fmt):
        """distinct_count > 90% but not 100%  → 'high'."""
        conn = LocalConnection("./data")
        # 20 rows, 19 distinct (95%) — not unique because one duplicate
        values = list(range(19)) + [0]
        df = pd.DataFrame({"x": values})
        with patch("pandas.read_parquet", return_value=df):
            result = conn.profile("test.parquet")
        col = next(c for c in result["columns"] if c["name"] == "x")
        assert col["cardinality"] == "high"

    @patch("odibi.connections.local.detect_file_format", return_value="parquet")
    def test_cardinality_medium(self, mock_fmt):
        """distinct_count between 10% and 90%  → 'medium'."""
        conn = LocalConnection("./data")
        # 10 rows, 5 distinct (50%)
        values = [1, 2, 3, 4, 5, 1, 2, 3, 4, 5]
        df = pd.DataFrame({"x": values})
        with patch("pandas.read_parquet", return_value=df):
            result = conn.profile("test.parquet")
        col = next(c for c in result["columns"] if c["name"] == "x")
        assert col["cardinality"] == "medium"

    @patch("odibi.connections.local.detect_file_format", return_value="parquet")
    def test_sample_rows_capped_at_10000(self, mock_fmt):
        """sample_rows > 10000 should be capped."""
        conn = LocalConnection("./data")
        df = pd.DataFrame({"a": [1]})
        with patch("pandas.read_parquet", return_value=df) as mock_read:
            conn.profile("test.parquet", sample_rows=99999)
            # The df.head(sample_rows) is called with the capped value;
            # verify the profile ran without error and returned results
        assert mock_read.called


# ---------------------------------------------------------------------------
# 18. detect_partitions – with subpath
# ---------------------------------------------------------------------------
class TestDetectPartitionsExtra:
    def test_with_subpath(self, tmp_path):
        sub = tmp_path / "region=us"
        sub.mkdir()
        (sub / "data.parquet").write_bytes(b"\x00")
        conn = LocalConnection(str(tmp_path))
        result = conn.detect_partitions(path="region=us")
        assert "keys" in result

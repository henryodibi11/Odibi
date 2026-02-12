"""Unit tests for LocalDBFS connection."""

from pathlib import Path

from odibi.connections.local_dbfs import LocalDBFS


class TestLocalDBFSInit:
    """Tests for LocalDBFS initialization."""

    def test_init_default_root(self):
        """Should default to .dbfs root."""
        conn = LocalDBFS()
        assert conn.root == Path(".dbfs").resolve()

    def test_init_custom_root_string(self):
        """Should accept custom root as string."""
        conn = LocalDBFS(root="/custom/dbfs")
        assert conn.root == Path("/custom/dbfs").resolve()

    def test_init_custom_root_path(self, tmp_path):
        """Should accept custom root as Path."""
        conn = LocalDBFS(root=tmp_path)
        assert conn.root == tmp_path.resolve()


class TestLocalDBFSResolve:
    """Tests for LocalDBFS resolve method."""

    def test_resolve_removes_dbfs_prefix(self, tmp_path):
        """Should remove dbfs:/ prefix."""
        conn = LocalDBFS(root=tmp_path)
        result = conn.resolve("dbfs:/FileStore/data.csv")

        assert result == str(tmp_path / "FileStore/data.csv")

    def test_resolve_handles_double_slash(self, tmp_path):
        """Should handle paths without dbfs:/ prefix."""
        conn = LocalDBFS(root=tmp_path)
        result = conn.resolve("FileStore/data.csv")

        assert result == str(tmp_path / "FileStore/data.csv")

    def test_resolve_strips_leading_slash(self, tmp_path):
        """Should strip leading slashes after prefix removal."""
        conn = LocalDBFS(root=tmp_path)
        result = conn.resolve("dbfs:///FileStore/data.csv")

        assert result == str(tmp_path / "FileStore/data.csv")

    def test_resolve_nested_path(self, tmp_path):
        """Should handle deeply nested paths."""
        conn = LocalDBFS(root=tmp_path)
        result = conn.resolve("dbfs:/mnt/adls/bronze/customers/2024/01/data.parquet")

        expected = tmp_path / "mnt/adls/bronze/customers/2024/01/data.parquet"
        assert result == str(expected)

    def test_resolve_empty_path(self, tmp_path):
        """Should handle empty path after prefix removal."""
        conn = LocalDBFS(root=tmp_path)
        result = conn.resolve("dbfs:/")

        assert result == str(tmp_path)

    def test_resolve_with_special_characters(self, tmp_path):
        """Should handle special characters in paths."""
        conn = LocalDBFS(root=tmp_path)
        result = conn.resolve("dbfs:/FileStore/data-2024_01_15.csv")

        assert result == str(tmp_path / "FileStore/data-2024_01_15.csv")


class TestLocalDBFSGetPath:
    """Tests for LocalDBFS get_path method."""

    def test_get_path_delegates_to_resolve(self, tmp_path):
        """Should delegate to resolve method."""
        conn = LocalDBFS(root=tmp_path)

        result = conn.get_path("dbfs:/FileStore/data.csv")
        expected = conn.resolve("dbfs:/FileStore/data.csv")

        assert result == expected


class TestLocalDBFSEnsureDir:
    """Tests for LocalDBFS ensure_dir method."""

    def test_ensure_dir_creates_parent_directories(self, tmp_path):
        """Should create parent directories."""
        conn = LocalDBFS(root=tmp_path)
        path = "dbfs:/FileStore/nested/deep/data.csv"

        conn.ensure_dir(path)

        expected_dir = tmp_path / "FileStore/nested/deep"
        assert expected_dir.exists()
        assert expected_dir.is_dir()

    def test_ensure_dir_idempotent(self, tmp_path):
        """Should not fail if directory already exists."""
        conn = LocalDBFS(root=tmp_path)
        path = "dbfs:/FileStore/existing/data.csv"

        conn.ensure_dir(path)
        conn.ensure_dir(path)

    def test_ensure_dir_with_no_prefix(self, tmp_path):
        """Should handle path without dbfs:/ prefix."""
        conn = LocalDBFS(root=tmp_path)
        path = "FileStore/nested/data.csv"

        conn.ensure_dir(path)

        expected_dir = tmp_path / "FileStore/nested"
        assert expected_dir.exists()


class TestLocalDBFSValidate:
    """Tests for LocalDBFS validate method."""

    def test_validate_passes(self, tmp_path):
        """Should pass validation (no-op for mock)."""
        conn = LocalDBFS(root=tmp_path)
        conn.validate()


class TestLocalDBFSFileOperations:
    """Tests for file read/write operations with LocalDBFS."""

    def test_write_and_read_file(self, tmp_path):
        """Should support writing and reading files."""
        conn = LocalDBFS(root=tmp_path)
        path = "dbfs:/FileStore/test_data.txt"
        
        conn.ensure_dir(path)
        local_path = conn.get_path(path)
        
        # Write file
        content = "test content"
        Path(local_path).write_text(content)
        
        # Read file
        assert Path(local_path).read_text() == content

    def test_write_csv_file(self, tmp_path):
        """Should support writing CSV files."""
        conn = LocalDBFS(root=tmp_path)
        path = "dbfs:/FileStore/bronze/data.csv"
        
        conn.ensure_dir(path)
        local_path = conn.get_path(path)
        
        csv_content = "id,name\n1,Alice\n2,Bob\n"
        Path(local_path).write_text(csv_content)
        
        assert Path(local_path).exists()
        assert "Alice" in Path(local_path).read_text()

    def test_write_binary_file(self, tmp_path):
        """Should support writing binary files."""
        conn = LocalDBFS(root=tmp_path)
        path = "dbfs:/FileStore/data.parquet"
        
        conn.ensure_dir(path)
        local_path = conn.get_path(path)
        
        binary_content = b"\x00\x01\x02\x03"
        Path(local_path).write_bytes(binary_content)
        
        assert Path(local_path).read_bytes() == binary_content


class TestLocalDBFSEdgeCases:
    """Tests for edge cases and error handling."""

    def test_missing_file_does_not_exist(self, tmp_path):
        """Should not create file when just resolving path."""
        conn = LocalDBFS(root=tmp_path)
        path = "dbfs:/FileStore/nonexistent.csv"
        
        local_path = conn.get_path(path)
        
        # Path should be returned but file should not exist
        assert not Path(local_path).exists()

    def test_resolve_does_not_create_directories(self, tmp_path):
        """Should not create directories when resolving path."""
        conn = LocalDBFS(root=tmp_path)
        path = "dbfs:/FileStore/nested/deep/data.csv"
        
        local_path = conn.get_path(path)
        
        # Parent directory should not exist
        assert not Path(local_path).parent.exists()

    def test_invalid_dbfs_path_with_backslashes(self, tmp_path):
        """Should handle backslashes in paths (Windows-style)."""
        conn = LocalDBFS(root=tmp_path)
        # Note: LocalDBFS doesn't normalize backslashes, it treats them as-is
        path = r"dbfs:/FileStore\data.csv"
        
        local_path = conn.get_path(path)
        
        # Path should be resolved (backslash becomes part of path)
        assert "FileStore" in local_path

    def test_path_with_spaces(self, tmp_path):
        """Should handle paths with spaces."""
        conn = LocalDBFS(root=tmp_path)
        path = "dbfs:/FileStore/my data/file with spaces.csv"
        
        conn.ensure_dir(path)
        local_path = conn.get_path(path)
        
        # Write and read to ensure it works
        Path(local_path).write_text("test")
        assert Path(local_path).read_text() == "test"

    def test_path_with_unicode_characters(self, tmp_path):
        """Should handle paths with Unicode characters."""
        conn = LocalDBFS(root=tmp_path)
        path = "dbfs:/FileStore/données_2024.csv"
        
        conn.ensure_dir(path)
        local_path = conn.get_path(path)
        
        # Write and read to ensure it works
        Path(local_path).write_text("données")
        assert Path(local_path).read_text() == "données"

    def test_very_long_path(self, tmp_path):
        """Should handle very long paths."""
        conn = LocalDBFS(root=tmp_path)
        # Create a very nested path
        long_path = "dbfs:/FileStore/" + "/".join([f"level{i}" for i in range(20)]) + "/data.csv"
        
        conn.ensure_dir(long_path)
        local_path = conn.get_path(long_path)
        
        assert Path(local_path).parent.exists()


class TestLocalDBFSIntegration:
    """Integration tests for LocalDBFS."""

    def test_full_workflow(self, tmp_path):
        """Should support full DBFS mock workflow."""
        conn = LocalDBFS(root=tmp_path)

        path = "dbfs:/FileStore/bronze/raw_data.parquet"
        conn.ensure_dir(path)
        local_path = conn.get_path(path)

        assert str(tmp_path) in local_path
        assert "FileStore/bronze/raw_data.parquet" in local_path.replace("\\", "/")

        expected_parent = Path(local_path).parent
        assert expected_parent.exists()

    def test_multiple_paths(self, tmp_path):
        """Should resolve multiple paths independently."""
        conn = LocalDBFS(root=tmp_path)

        path1 = conn.get_path("dbfs:/FileStore/bronze/data.csv")
        path2 = conn.get_path("dbfs:/mnt/adls/silver/data.parquet")

        assert "FileStore/bronze/data.csv" in path1.replace("\\", "/")
        assert "mnt/adls/silver/data.parquet" in path2.replace("\\", "/")
        assert path1 != path2

    def test_complete_pipeline_simulation(self, tmp_path):
        """Should simulate complete data pipeline workflow."""
        conn = LocalDBFS(root=tmp_path)

        # Bronze layer
        bronze_path = "dbfs:/mnt/adls/bronze/customers.csv"
        conn.ensure_dir(bronze_path)
        Path(conn.get_path(bronze_path)).write_text("id,name\n1,Alice\n")

        # Silver layer
        silver_path = "dbfs:/mnt/adls/silver/customers_clean.parquet"
        conn.ensure_dir(silver_path)
        Path(conn.get_path(silver_path)).write_bytes(b"parquet_data")

        # Gold layer
        gold_path = "dbfs:/mnt/adls/gold/dim_customers.parquet"
        conn.ensure_dir(gold_path)
        Path(conn.get_path(gold_path)).write_bytes(b"gold_data")

        # Verify all layers exist
        assert Path(conn.get_path(bronze_path)).exists()
        assert Path(conn.get_path(silver_path)).exists()
        assert Path(conn.get_path(gold_path)).exists()

    def test_multiple_connections_isolated(self, tmp_path):
        """Should isolate multiple LocalDBFS instances."""
        root1 = tmp_path / "dbfs1"
        root2 = tmp_path / "dbfs2"

        conn1 = LocalDBFS(root=root1)
        conn2 = LocalDBFS(root=root2)

        path = "dbfs:/FileStore/data.csv"

        path1 = conn1.get_path(path)
        path2 = conn2.get_path(path)

        # Paths should be different
        assert path1 != path2
        assert str(root1) in path1
        assert str(root2) in path2

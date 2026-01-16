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


class TestLocalDBFSValidate:
    """Tests for LocalDBFS validate method."""

    def test_validate_passes(self, tmp_path):
        """Should pass validation (no-op for mock)."""
        conn = LocalDBFS(root=tmp_path)
        conn.validate()


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

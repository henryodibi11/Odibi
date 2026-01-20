"""Unit tests for LocalConnection."""

import os
from pathlib import Path

from odibi.connections.local import LocalConnection


class TestLocalConnectionInit:
    """Tests for LocalConnection initialization."""

    def test_init_default_path(self):
        """Should default to ./data."""
        conn = LocalConnection()
        assert conn.base_path_str == "./data"
        assert conn.is_uri is False

    def test_init_custom_path(self):
        """Should accept custom base path."""
        conn = LocalConnection(base_path="/custom/path")
        assert conn.base_path_str == "/custom/path"
        assert conn.is_uri is False

    def test_init_uri_path_with_double_slash(self):
        """Should detect URI paths with ://."""
        conn = LocalConnection(base_path="dbfs://mnt/data")
        assert conn.is_uri is True
        assert conn.base_path is None

    def test_init_uri_path_with_single_slash(self):
        """Should detect URI paths with :/."""
        conn = LocalConnection(base_path="dbfs:/FileStore/data")
        assert conn.is_uri is True
        assert conn.base_path is None

    def test_init_windows_path_not_uri(self):
        """Windows absolute paths should NOT be treated as URIs."""
        conn = LocalConnection(base_path="C:/Users/test/data")
        assert conn.is_uri is False
        assert conn.base_path is not None

    def test_init_windows_path_backslash(self):
        """Windows paths with backslash should NOT be treated as URIs."""
        conn = LocalConnection(base_path="D:\\Projects\\data")
        assert conn.is_uri is False
        assert conn.base_path is not None

    def test_init_local_path_creates_path_object(self):
        """Should create Path object for local paths."""
        conn = LocalConnection(base_path="/local/path")
        assert isinstance(conn.base_path, Path)


class TestLocalConnectionGetPath:
    """Tests for LocalConnection get_path method."""

    def test_get_path_local(self, tmp_path):
        """Should resolve local path to absolute."""
        conn = LocalConnection(base_path=str(tmp_path))
        result = conn.get_path("subdir/file.parquet")

        expected = str((tmp_path / "subdir/file.parquet").absolute())
        assert result == expected

    def test_get_path_uri_with_trailing_slash(self):
        """Should join URI with trailing slash."""
        conn = LocalConnection(base_path="dbfs:/FileStore/")
        result = conn.get_path("data/file.csv")

        assert result == "dbfs:/FileStore/data/file.csv"

    def test_get_path_uri_without_trailing_slash(self):
        """Should add slash when joining URI."""
        conn = LocalConnection(base_path="dbfs:/FileStore")
        result = conn.get_path("data/file.csv")

        assert result == "dbfs:/FileStore/data/file.csv"

    def test_get_path_uri_strips_leading_slash(self):
        """Should strip leading slash from relative path."""
        conn = LocalConnection(base_path="dbfs:/FileStore")
        result = conn.get_path("/data/file.csv")

        assert result == "dbfs:/FileStore/data/file.csv"

    def test_get_path_uri_handles_backslash(self):
        """Should handle backslash in relative path."""
        conn = LocalConnection(base_path="dbfs:/FileStore")
        result = conn.get_path("\\data\\file.csv")

        assert result == "dbfs:/FileStore/data\\file.csv"


class TestLocalConnectionValidate:
    """Tests for LocalConnection validate method."""

    def test_validate_creates_directory(self, tmp_path):
        """Should create base directory if it doesn't exist."""
        new_path = tmp_path / "new_dir" / "nested"
        conn = LocalConnection(base_path=str(new_path))

        conn.validate()

        assert new_path.exists()
        assert new_path.is_dir()

    def test_validate_existing_directory(self, tmp_path):
        """Should not fail if directory already exists."""
        conn = LocalConnection(base_path=str(tmp_path))

        conn.validate()

    def test_validate_skips_uri_paths(self):
        """Should skip validation for URI paths."""
        conn = LocalConnection(base_path="dbfs:/FileStore/data")

        conn.validate()


class TestLocalConnectionIntegration:
    """Integration tests for LocalConnection."""

    def test_full_workflow(self, tmp_path):
        """Should support full create-validate-get_path workflow."""
        base = tmp_path / "data"
        conn = LocalConnection(base_path=str(base))

        conn.validate()

        path1 = conn.get_path("bronze/customers.parquet")
        path2 = conn.get_path("silver/dim_customer.parquet")

        assert "bronze" in path1
        assert "silver" in path2
        assert str(base.absolute()) in path1
        assert str(base.absolute()) in path2

    def test_relative_path(self):
        """Should handle relative base path."""
        conn = LocalConnection(base_path="./data")
        result = conn.get_path("test.csv")

        assert os.path.isabs(result)
        assert "data" in result
        assert "test.csv" in result

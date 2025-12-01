"""Tests for content hash utilities (skip_if_unchanged feature)."""

import hashlib

import pandas as pd
import pytest


class TestComputeDataframeHash:
    """Tests for compute_dataframe_hash function."""

    def test_same_content_same_hash(self):
        """Same DataFrame content should produce same hash."""
        from odibi.utils.content_hash import compute_dataframe_hash

        df1 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        df2 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})

        hash1 = compute_dataframe_hash(df1)
        hash2 = compute_dataframe_hash(df2)

        assert hash1 == hash2

    def test_different_content_different_hash(self):
        """Different DataFrame content should produce different hash."""
        from odibi.utils.content_hash import compute_dataframe_hash

        df1 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        df2 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "d"]})

        hash1 = compute_dataframe_hash(df1)
        hash2 = compute_dataframe_hash(df2)

        assert hash1 != hash2

    def test_empty_dataframe(self):
        """Empty DataFrame should produce consistent hash."""
        from odibi.utils.content_hash import compute_dataframe_hash

        df1 = pd.DataFrame()
        df2 = pd.DataFrame()

        hash1 = compute_dataframe_hash(df1)
        hash2 = compute_dataframe_hash(df2)

        assert hash1 == hash2
        assert hash1 == hashlib.sha256(b"EMPTY_DATAFRAME").hexdigest()

    def test_column_subset(self):
        """Hash should only consider specified columns."""
        from odibi.utils.content_hash import compute_dataframe_hash

        df1 = pd.DataFrame({"id": [1, 2], "value": ["a", "b"], "ts": [100, 200]})
        df2 = pd.DataFrame({"id": [1, 2], "value": ["a", "b"], "ts": [300, 400]})

        # Full hash should differ (ts is different)
        hash1_full = compute_dataframe_hash(df1)
        hash2_full = compute_dataframe_hash(df2)
        assert hash1_full != hash2_full

        # Subset hash should match (only id and value)
        hash1_subset = compute_dataframe_hash(df1, columns=["id", "value"])
        hash2_subset = compute_dataframe_hash(df2, columns=["id", "value"])
        assert hash1_subset == hash2_subset

    def test_sort_columns_determinism(self):
        """Hash should be deterministic regardless of row order when sorted."""
        from odibi.utils.content_hash import compute_dataframe_hash

        df1 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        df2 = pd.DataFrame({"id": [3, 1, 2], "value": ["c", "a", "b"]})

        # Without sorting, different order = different hash
        hash1_unsorted = compute_dataframe_hash(df1)
        hash2_unsorted = compute_dataframe_hash(df2)
        assert hash1_unsorted != hash2_unsorted

        # With sorting, same content = same hash
        hash1_sorted = compute_dataframe_hash(df1, sort_columns=["id"])
        hash2_sorted = compute_dataframe_hash(df2, sort_columns=["id"])
        assert hash1_sorted == hash2_sorted

    def test_missing_columns_raises(self):
        """Should raise ValueError if specified columns don't exist."""
        from odibi.utils.content_hash import compute_dataframe_hash

        df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})

        with pytest.raises(ValueError, match="Hash columns not found"):
            compute_dataframe_hash(df, columns=["id", "nonexistent"])

    def test_missing_sort_columns_raises(self):
        """Should raise ValueError if sort columns don't exist."""
        from odibi.utils.content_hash import compute_dataframe_hash

        df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})

        with pytest.raises(ValueError, match="Sort columns not found"):
            compute_dataframe_hash(df, sort_columns=["nonexistent"])

    def test_hash_is_sha256(self):
        """Hash should be a valid SHA256 hex digest (64 characters)."""
        from odibi.utils.content_hash import compute_dataframe_hash

        df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        hash_val = compute_dataframe_hash(df)

        assert len(hash_val) == 64
        assert all(c in "0123456789abcdef" for c in hash_val)


class TestDeltaContentHashStorage:
    """Tests for Delta table hash storage/retrieval."""

    @pytest.fixture
    def temp_delta_path(self, tmp_path):
        """Create a temporary Delta table."""
        delta_path = tmp_path / "test_delta"

        try:
            from deltalake import write_deltalake
        except ImportError:
            pytest.skip("deltalake not installed")

        df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        write_deltalake(str(delta_path), df)

        return str(delta_path)

    def test_get_hash_returns_none_for_new_table(self, temp_delta_path):
        """Should return None if no hash has been stored."""
        from odibi.utils.content_hash import get_delta_content_hash

        result = get_delta_content_hash(temp_delta_path)
        assert result is None

    def test_set_and_get_hash(self, temp_delta_path):
        """Should be able to store and retrieve hash."""
        from odibi.utils.content_hash import (
            get_delta_content_hash,
            set_delta_content_hash,
        )

        test_hash = "abc123def456" * 5 + "abcd"  # 64 chars

        set_delta_content_hash(temp_delta_path, test_hash)
        retrieved = get_delta_content_hash(temp_delta_path)

        assert retrieved == test_hash

    def test_get_hash_nonexistent_table(self):
        """Should return None for non-existent table."""
        from odibi.utils.content_hash import get_delta_content_hash

        result = get_delta_content_hash("/nonexistent/path/to/table")
        assert result is None


class TestWriteConfigSkipIfUnchanged:
    """Tests for skip_if_unchanged configuration."""

    def test_config_defaults(self):
        """Default values should be correct."""
        from odibi.config import WriteConfig

        config = WriteConfig(
            connection="test",
            format="delta",
            table="test_table",
        )

        assert config.skip_if_unchanged is False
        assert config.skip_hash_columns is None
        assert config.skip_hash_sort_columns is None

    def test_config_with_skip_enabled(self):
        """Should accept skip_if_unchanged configuration."""
        from odibi.config import WriteConfig

        config = WriteConfig(
            connection="test",
            format="delta",
            table="test_table",
            skip_if_unchanged=True,
            skip_hash_columns=["id", "value"],
            skip_hash_sort_columns=["id"],
        )

        assert config.skip_if_unchanged is True
        assert config.skip_hash_columns == ["id", "value"]
        assert config.skip_hash_sort_columns == ["id"]

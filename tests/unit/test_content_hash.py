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


class TestContentHashStateStorage:
    """Tests for content hash storage via state backend."""

    @pytest.fixture
    def mock_state_backend(self):
        """Create a mock state backend."""
        from unittest.mock import MagicMock

        backend = MagicMock()
        backend._hwm_store = {}

        def get_hwm(key):
            return backend._hwm_store.get(key)

        def set_hwm(key, value):
            backend._hwm_store[key] = value

        backend.get_hwm = get_hwm
        backend.set_hwm = set_hwm
        return backend

    def test_get_hash_returns_none_when_not_set(self, mock_state_backend):
        """Should return None if no hash has been stored."""
        from odibi.utils.content_hash import get_content_hash_from_state

        result = get_content_hash_from_state(mock_state_backend, "test_node", "test_table")
        assert result is None

    def test_set_and_get_hash(self, mock_state_backend):
        """Should be able to store and retrieve hash."""
        from odibi.utils.content_hash import (
            get_content_hash_from_state,
            set_content_hash_in_state,
        )

        test_hash = "abc123def456" * 5 + "abcd"  # 64 chars

        set_content_hash_in_state(mock_state_backend, "test_node", "test_table", test_hash)
        retrieved = get_content_hash_from_state(mock_state_backend, "test_node", "test_table")

        assert retrieved == test_hash

    def test_get_hash_with_none_backend(self):
        """Should return None when backend is None."""
        from odibi.utils.content_hash import get_content_hash_from_state

        result = get_content_hash_from_state(None, "test_node", "test_table")
        assert result is None

    def test_make_content_hash_key(self):
        """Should generate correct key format."""
        from odibi.utils.content_hash import make_content_hash_key

        key = make_content_hash_key("my_node", "my_table")
        assert key == "content_hash:my_node:my_table"


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

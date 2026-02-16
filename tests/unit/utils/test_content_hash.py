"""Tests for content hashing utilities."""

import hashlib
from unittest.mock import MagicMock

import pandas as pd
import pytest

from odibi.utils.content_hash import (
    compute_dataframe_hash,
    get_content_hash_from_state,
    make_content_hash_key,
    set_content_hash_in_state,
)


class TestComputeDataframeHash:
    """Tests for compute_dataframe_hash."""

    def test_deterministic_same_dataframe(self):
        df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        h1 = compute_dataframe_hash(df)
        h2 = compute_dataframe_hash(df)
        assert h1 == h2
        assert len(h1) == 64

    def test_different_data_produces_different_hash(self):
        df1 = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        df2 = pd.DataFrame({"id": [1, 2], "value": ["a", "c"]})
        assert compute_dataframe_hash(df1) != compute_dataframe_hash(df2)

    def test_empty_dataframe_returns_known_hash(self):
        df = pd.DataFrame({"id": [], "value": []})
        expected = hashlib.sha256(b"EMPTY_DATAFRAME").hexdigest()
        assert compute_dataframe_hash(df) == expected

    def test_column_subset_only_selected_columns_affect_hash(self):
        df1 = pd.DataFrame({"id": [1, 2], "value": ["a", "b"], "extra": [10, 20]})
        df2 = pd.DataFrame({"id": [1, 2], "value": ["a", "b"], "extra": [99, 99]})
        h1 = compute_dataframe_hash(df1, columns=["id", "value"])
        h2 = compute_dataframe_hash(df2, columns=["id", "value"])
        assert h1 == h2

    def test_sort_columns_same_data_different_order(self):
        df1 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        df2 = pd.DataFrame({"id": [3, 1, 2], "value": ["c", "a", "b"]})
        h1 = compute_dataframe_hash(df1, sort_columns=["id"])
        h2 = compute_dataframe_hash(df2, sort_columns=["id"])
        assert h1 == h2

    def test_missing_hash_columns_raises(self):
        df = pd.DataFrame({"id": [1], "value": ["a"]})
        with pytest.raises(ValueError, match="Hash columns not found"):
            compute_dataframe_hash(df, columns=["id", "nonexistent"])

    def test_missing_sort_columns_raises(self):
        df = pd.DataFrame({"id": [1], "value": ["a"]})
        with pytest.raises(ValueError, match="Sort columns not found"):
            compute_dataframe_hash(df, sort_columns=["nonexistent"])


class TestMakeContentHashKey:
    """Tests for make_content_hash_key."""

    def test_returns_expected_format(self):
        result = make_content_hash_key("my_node", "my_table")
        assert result == "content_hash:my_node:my_table"


class TestGetContentHashFromState:
    """Tests for get_content_hash_from_state."""

    def test_returns_hash_from_state(self):
        backend = MagicMock()
        backend.get_hwm.return_value = {"hash": "abc123"}
        result = get_content_hash_from_state(backend, "node", "table")
        assert result == "abc123"
        backend.get_hwm.assert_called_once_with("content_hash:node:table")

    def test_returns_none_when_backend_is_none(self):
        assert get_content_hash_from_state(None, "node", "table") is None

    def test_returns_none_when_key_not_found(self):
        backend = MagicMock()
        backend.get_hwm.return_value = None
        assert get_content_hash_from_state(backend, "node", "table") is None

    def test_returns_none_on_exception(self):
        backend = MagicMock()
        backend.get_hwm.side_effect = RuntimeError("boom")
        assert get_content_hash_from_state(backend, "node", "table") is None


class TestSetContentHashInState:
    """Tests for set_content_hash_in_state."""

    def test_stores_hash_in_state(self):
        backend = MagicMock()
        set_content_hash_in_state(backend, "node", "table", "hash123")
        backend.set_hwm.assert_called_once()
        key, value = backend.set_hwm.call_args[0]
        assert key == "content_hash:node:table"
        assert value["hash"] == "hash123"

    def test_does_nothing_when_backend_is_none(self):
        set_content_hash_in_state(None, "node", "table", "hash123")

    def test_stored_value_includes_hash_and_timestamp(self):
        backend = MagicMock()
        set_content_hash_in_state(backend, "node", "table", "hash123")
        _, value = backend.set_hwm.call_args[0]
        assert "hash" in value
        assert "timestamp" in value
        assert value["hash"] == "hash123"

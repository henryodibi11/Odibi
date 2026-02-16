"""Tests for odibi.utils.content_hash module."""

import hashlib

import pandas as pd
import pytest

from odibi.utils.content_hash import (
    compute_dataframe_hash,
    get_content_hash_from_state,
    make_content_hash_key,
    set_content_hash_in_state,
)


# ---------- compute_dataframe_hash ----------


class TestComputeDataframeHash:
    """Test compute_dataframe_hash."""

    def test_basic_dataframe(self):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        result = compute_dataframe_hash(df)
        assert isinstance(result, str)
        assert len(result) == 64  # SHA256 hex digest

    def test_consistent_across_calls(self):
        df = pd.DataFrame({"id": [3, 1, 2], "val": ["c", "a", "b"]})
        h1 = compute_dataframe_hash(df, sort_columns=["id"])
        h2 = compute_dataframe_hash(df, sort_columns=["id"])
        assert h1 == h2

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        expected = hashlib.sha256(b"EMPTY_DATAFRAME").hexdigest()
        assert compute_dataframe_hash(df) == expected

    def test_column_subset(self):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        h_all = compute_dataframe_hash(df)
        h_sub = compute_dataframe_hash(df, columns=["a", "b"])
        assert h_all != h_sub

    def test_column_subset_only_selected_columns_affect_hash(self):
        df1 = pd.DataFrame({"id": [1, 2], "value": ["a", "b"], "extra": [10, 20]})
        df2 = pd.DataFrame({"id": [1, 2], "value": ["a", "b"], "extra": [99, 99]})
        h1 = compute_dataframe_hash(df1, columns=["id", "value"])
        h2 = compute_dataframe_hash(df2, columns=["id", "value"])
        assert h1 == h2

    def test_missing_columns_raises(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Hash columns not found"):
            compute_dataframe_hash(df, columns=["a", "missing"])

    def test_missing_sort_columns_raises(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Sort columns not found"):
            compute_dataframe_hash(df, sort_columns=["missing"])

    def test_different_data_different_hash(self):
        df1 = pd.DataFrame({"x": [1, 2]})
        df2 = pd.DataFrame({"x": [3, 4]})
        assert compute_dataframe_hash(df1) != compute_dataframe_hash(df2)

    def test_sort_columns_same_data_different_order(self):
        df1 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        df2 = pd.DataFrame({"id": [3, 1, 2], "value": ["c", "a", "b"]})
        h1 = compute_dataframe_hash(df1, sort_columns=["id"])
        h2 = compute_dataframe_hash(df2, sort_columns=["id"])
        assert h1 == h2


# ---------- has_content_changed (via get/set round-trip) ----------


class TestHasContentChanged:
    """Test change detection via hash comparison."""

    def test_unchanged_content(self):
        df = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        h1 = compute_dataframe_hash(df, sort_columns=["id"])
        h2 = compute_dataframe_hash(df, sort_columns=["id"])
        assert h1 == h2  # unchanged

    def test_changed_content(self):
        df1 = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        df2 = pd.DataFrame({"id": [1, 2], "val": ["a", "c"]})
        h1 = compute_dataframe_hash(df1, sort_columns=["id"])
        h2 = compute_dataframe_hash(df2, sort_columns=["id"])
        assert h1 != h2  # changed


# ---------- save / get content hash (state backend) ----------


class _DummyStateBackend:
    """Minimal state backend for testing."""

    def __init__(self):
        self.data = {}

    def get_hwm(self, key):
        return self.data.get(key)

    def set_hwm(self, key, value):
        self.data[key] = value


class TestSaveContentHash:
    """Test set_content_hash_in_state stores correctly."""

    def test_stores_hash_and_timestamp(self):
        backend = _DummyStateBackend()
        set_content_hash_in_state(backend, "node_a", "table_b", "abc123")
        key = make_content_hash_key("node_a", "table_b")
        stored = backend.data[key]
        assert stored["hash"] == "abc123"
        assert "timestamp" in stored

    def test_none_backend_is_noop(self):
        set_content_hash_in_state(None, "n", "t", "h")  # should not raise


class TestGetContentHash:
    """Test get_content_hash_from_state retrieval."""

    def test_returns_stored_hash(self):
        backend = _DummyStateBackend()
        set_content_hash_in_state(backend, "n", "t", "xyz")
        result = get_content_hash_from_state(backend, "n", "t")
        assert result == "xyz"

    def test_none_backend_returns_none(self):
        assert get_content_hash_from_state(None, "n", "t") is None

    def test_missing_key_returns_none(self):
        backend = _DummyStateBackend()
        assert get_content_hash_from_state(backend, "no", "entry") is None

    def test_non_dict_value_returns_none(self):
        backend = _DummyStateBackend()
        key = make_content_hash_key("n", "t")
        backend.data[key] = "not_a_dict"
        assert get_content_hash_from_state(backend, "n", "t") is None

    def test_exception_returns_none(self):
        class _BrokenBackend:
            def get_hwm(self, key):
                raise RuntimeError("boom")

        assert get_content_hash_from_state(_BrokenBackend(), "n", "t") is None


# ---------- make_content_hash_key ----------


class TestGetContentHashKey:
    """Test make_content_hash_key."""

    def test_with_node_name(self):
        key = make_content_hash_key("load_sales", "fact_sales")
        assert key == "content_hash:load_sales:fact_sales"

    def test_different_inputs_different_keys(self):
        k1 = make_content_hash_key("a", "b")
        k2 = make_content_hash_key("c", "d")
        assert k1 != k2

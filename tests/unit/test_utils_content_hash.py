import hashlib
import pandas as pd
import pytest

from odibi.utils.content_hash import (
    compute_dataframe_hash,
    compute_spark_dataframe_hash,
    make_content_hash_key,
    get_content_hash_from_state,
    set_content_hash_in_state,
)

# Tests for compute_dataframe_hash


def test_compute_dataframe_hash_empty_dataframe_returns_empty_hash():
    """test_compute_dataframe_hash_empty_dataframe_returns_empty_hash:
    For an empty DataFrame, the function should return the hash of b"EMPTY_DATAFRAME".
    """
    df = pd.DataFrame()
    expected = hashlib.sha256(b"EMPTY_DATAFRAME").hexdigest()
    result = compute_dataframe_hash(df)
    assert result == expected


def test_compute_dataframe_hash_valid_columns_returns_deterministic_hash():
    """test_compute_dataframe_hash_valid_columns_returns_deterministic_hash:
    When a subset of columns and sort order is specified, the hash should be deterministic.
    """
    df = pd.DataFrame({"id": [2, 1], "value": ["b", "a"], "extra": [10, 20]})
    # Use only 'id' and 'value' columns and sort by 'id'
    hash1 = compute_dataframe_hash(df, columns=["id", "value"], sort_columns=["id"])
    hash2 = compute_dataframe_hash(df, columns=["id", "value"], sort_columns=["id"])
    assert hash1 == hash2


def test_compute_dataframe_hash_missing_columns_raises_value_error():
    """test_compute_dataframe_hash_missing_columns_raises_value_error:
    If specified columns for hashing are missing, a ValueError should be raised.
    """
    df = pd.DataFrame({"a": [1, 2]})
    with pytest.raises(ValueError) as exc_info:
        compute_dataframe_hash(df, columns=["a", "b"])
    assert "Hash columns not found in DataFrame:" in str(exc_info.value)


def test_compute_dataframe_hash_missing_sort_columns_raises_value_error():
    """test_compute_dataframe_hash_missing_sort_columns_raises_value_error:
    If specified sort columns are missing in the DataFrame, a ValueError should be raised.
    """
    df = pd.DataFrame({"a": [1, 2]})
    with pytest.raises(ValueError) as exc_info:
        compute_dataframe_hash(df, sort_columns=["b"])
    assert "Sort columns not found in DataFrame:" in str(exc_info.value)


# Dummy classes for simulating Spark DataFrame behavior


class DummySparkDFEmpty:
    def isEmpty(self):
        return True


class DummySparkDFLegacy:
    def __init__(self, pandas_df):
        self._pandas_df = pandas_df

    def isEmpty(self):
        return False

    def orderBy(self, sort_columns):
        # Simulate ordering; in reality, this would sort the DataFrame.
        return self

    def toPandas(self):
        return self._pandas_df


# Tests for compute_spark_dataframe_hash


def test_compute_spark_dataframe_hash_empty_returns_empty_hash():
    """test_compute_spark_dataframe_hash_empty_returns_empty_hash:
    For an empty Spark DataFrame, the function should return the hash of b"EMPTY_DATAFRAME".
    """
    dummy = DummySparkDFEmpty()
    expected = hashlib.sha256(b"EMPTY_DATAFRAME").hexdigest()
    result = compute_spark_dataframe_hash(dummy)
    assert result == expected


def test_compute_spark_dataframe_hash_legacy_returns_correct_hash():
    """test_compute_spark_dataframe_hash_legacy_returns_correct_hash:
    For legacy mode (distributed=False), the hash computed should match the SHA256 hash of the CSV representation.
    """
    df = pd.DataFrame({"x": [1, 2]})
    dummy = DummySparkDFLegacy(df)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    expected = hashlib.sha256(csv_bytes).hexdigest()
    result = compute_spark_dataframe_hash(dummy, distributed=False, sort_columns=["x"])
    assert result == expected


# Tests for make_content_hash_key


def test_make_content_hash_key_returns_correct_key():
    """test_make_content_hash_key_returns_correct_key:
    The function should generate a key in the format 'content_hash:{node_name}:{table_name}'.
    """
    key = make_content_hash_key("node1", "tableA")
    assert key == "content_hash:node1:tableA"


# Dummy state backend for testing get and set content hash functions


class DummyStateBackend:
    def __init__(self, data=None, should_raise=False):
        self.data = data or {}
        self.should_raise = should_raise

    def get_hwm(self, key):
        if self.should_raise:
            raise Exception("Simulated error")
        return self.data.get(key)

    def set_hwm(self, key, value):
        self.data[key] = value


# Tests for get_content_hash_from_state


def test_get_content_hash_from_state_none_backend_returns_none():
    """test_get_content_hash_from_state_none_backend_returns_none:
    If the state backend is None, the function should return None.
    """
    result = get_content_hash_from_state(None, "node1", "tableA")
    assert result is None


def test_get_content_hash_from_state_valid_returns_hash():
    """test_get_content_hash_from_state_valid_returns_hash:
    When the backend returns a dict containing a 'hash', the function should return that hash.
    """
    key = make_content_hash_key("node1", "tableA")
    state_data = {key: {"hash": "abc123"}}
    backend = DummyStateBackend(state_data)
    result = get_content_hash_from_state(backend, "node1", "tableA")
    assert result == "abc123"


def test_get_content_hash_from_state_non_dict_returns_none():
    """test_get_content_hash_from_state_non_dict_returns_none:
    If the value retrieved from the state backend is not a dict, the function should return None.
    """
    key = make_content_hash_key("node1", "tableA")
    state_data = {key: "not_a_dict"}
    backend = DummyStateBackend(state_data)
    result = get_content_hash_from_state(backend, "node1", "tableA")
    assert result is None


def test_get_content_hash_from_state_exception_returns_none():
    """test_get_content_hash_from_state_exception_returns_none:
    If an exception occurs when accessing the state backend, the function should return None.
    """
    backend = DummyStateBackend(should_raise=True)
    result = get_content_hash_from_state(backend, "node1", "tableA")
    assert result is None


# Tests for set_content_hash_in_state


def test_set_content_hash_in_state_stores_value():
    """test_set_content_hash_in_state_stores_value:
    The function should store a dict with the hash and a timestamp in the state backend.
    """
    backend = DummyStateBackend()
    set_content_hash_in_state(backend, "node1", "tableA", "hash_value")
    key = make_content_hash_key("node1", "tableA")
    stored = backend.data.get(key)
    assert stored is not None
    assert stored.get("hash") == "hash_value"
    assert "timestamp" in stored


def test_set_content_hash_in_state_with_none_backend():
    """test_set_content_hash_in_state_with_none_backend:
    If the state backend is None, the function should not raise an error.
    """
    # Should not raise any exception when backend is None
    set_content_hash_in_state(None, "node1", "tableA", "hash_value")

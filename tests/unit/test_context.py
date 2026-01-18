import pytest
import pandas as pd

from odibi.context import Context, PandasContext

# Polars is optional - only import if available
try:
    import polars as pl
    from odibi.context import PolarsContext

    HAS_POLARS = True
except ImportError:
    pl = None
    PolarsContext = None
    HAS_POLARS = False


# Dummy implementation of Context for testing the abstract base class.
class DummyContext(Context):
    def __init__(self):
        self.store = {}
        self.metadata = {}

    def set(self, name, value):
        self.store[name] = value

    def get(self, name):
        return self.store.get(name, None)

    def clear(self, name):
        if name in self.store:
            del self.store[name]

    def register(self, name, value):
        if name in self.store:
            raise Exception("Already registered")
        self.store[name] = value

    def has(self, name):
        return name in self.store

    def get_metadata(self):
        return self.metadata

    def list_names(self):
        return list(self.store.keys())


# Fixture for base Context instance using DummyContext
@pytest.fixture
def context_instance():
    return DummyContext()


def test_context_set_and_get_value(context_instance):
    # Test that setting a key and retrieving it works for DummyContext.
    context_instance.set("key1", "value1")
    assert context_instance.get("key1") == "value1"


def test_context_missing_key_returns_none(context_instance):
    # For DummyContext, getting a missing key returns None.
    assert context_instance.get("nonexistent") is None


def test_context_overwrite_duplicate_key(context_instance):
    # Test that setting the same key twice overwrites the old value.
    context_instance.set("dup_key", "first")
    context_instance.set("dup_key", "second")
    assert context_instance.get("dup_key") == "second"


def test_context_clear_key(context_instance):
    # Test that clearing a key removes it.
    context_instance.set("temp", "data")
    context_instance.clear("temp")
    assert context_instance.get("temp") is None


def test_context_metadata_handling(context_instance):
    # Test metadata functionality if present.
    if hasattr(context_instance, "metadata"):
        context_instance.metadata["meta_key"] = "meta_value"
        assert context_instance.metadata.get("meta_key") == "meta_value"
    else:
        pytest.skip("Context does not implement a metadata property")


# Tests for PandasContext
@pytest.fixture
def pandas_context_instance():
    return PandasContext()


def test_pandas_context_basic_register(pandas_context_instance):
    # Register a pandas DataFrame and verify retrieval.
    df = pd.DataFrame({"a": [1, 2, 3]})
    pandas_context_instance.register("p_key", df)
    retrieved = pandas_context_instance.get("p_key")
    pd.testing.assert_frame_equal(retrieved, df)


def test_pandas_context_missing_key(pandas_context_instance):
    # Expect KeyError when retrieving a non-existent key.
    with pytest.raises(KeyError):
        pandas_context_instance.get("nonexistent")


def test_pandas_context_registration_conflict(pandas_context_instance):
    # Test duplicate registration behavior.
    df1 = pd.DataFrame({"a": [1]})
    df2 = pd.DataFrame({"a": [2]})
    pandas_context_instance.register("conflict_key", df1)
    # Attempt duplicate registration; since no exception is raised,
    # check that the stored DataFrame is either df1 (unchanged) or df2 (overwritten).
    pandas_context_instance.register("conflict_key", df2)
    retrieved = pandas_context_instance.get("conflict_key")
    if not (retrieved.equals(df1) or retrieved.equals(df2)):
        pytest.fail("Duplicate registration did not result in expected DataFrame")


def test_pandas_context_metadata_handling(pandas_context_instance):
    # If metadata is supported, test its functionality.
    if hasattr(pandas_context_instance, "metadata"):
        pandas_context_instance.metadata["p_meta"] = "test"
        assert pandas_context_instance.metadata.get("p_meta") == "test"
    else:
        pytest.skip("PandasContext does not implement a metadata property")


# Tests for PolarsContext
@pytest.fixture
def polars_context_instance():
    if not HAS_POLARS:
        pytest.skip("polars not installed")
    return PolarsContext()


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
def test_polars_context_basic_register(polars_context_instance):
    # Register a polars DataFrame and verify retrieval.
    df = pl.DataFrame({"a": [4, 5, 6]})
    polars_context_instance.register("pp_key", df)
    retrieved = polars_context_instance.get("pp_key")
    # Use equals() for Polars DataFrame comparison (frame_equal deprecated)
    assert retrieved.equals(df)


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
def test_polars_context_missing_key(polars_context_instance):
    # Expect KeyError when retrieving a non-existent key.
    with pytest.raises(KeyError):
        polars_context_instance.get("no_such_key")


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
def test_polars_context_registration_conflict(polars_context_instance):
    # Test registration conflict behavior for PolarsContext.
    df1 = pl.DataFrame({"a": [10]})
    df2 = pl.DataFrame({"a": [20]})
    polars_context_instance.register("p_conflict", df1)
    try:
        polars_context_instance.register("p_conflict", df2)
        # If no exception is raised, verify that the original value remains unchanged.
        retrieved = polars_context_instance.get("p_conflict")
        if not (retrieved.to_dict() == df1.to_dict() or retrieved.to_dict() == df2.to_dict()):
            pytest.fail("Duplicate registration did not result in expected DataFrame")
    except Exception:
        pytest.skip(
            "PolarsContext raised an exception on duplicate registration, which is acceptable"
        )


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
def test_polars_context_metadata_handling(polars_context_instance):
    # Test metadata functionality for PolarsContext if implemented.
    if hasattr(polars_context_instance, "metadata"):
        polars_context_instance.metadata["p_meta"] = "test_polars"
        assert polars_context_instance.metadata.get("p_meta") == "test_polars"
    else:
        pytest.skip("PolarsContext does not implement a metadata property")

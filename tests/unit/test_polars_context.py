import threading

import pytest

from odibi.context import PolarsContext


class MockPolarsFrame:
    """Simple stand-in for Polars DataFrame/LazyFrame."""

    def __init__(self, name: str = "mock"):
        self.name = name


@pytest.fixture
def polars_context():
    return PolarsContext()


def test_register_and_get(polars_context):
    df = MockPolarsFrame("df1")
    polars_context.register("node1", df)

    assert polars_context.has("node1")
    assert polars_context.get("node1") is df


def test_get_missing_raises(polars_context):
    with pytest.raises(KeyError) as exc:
        polars_context.get("missing")
    assert "not found in context" in str(exc.value)


def test_list_names(polars_context):
    df1 = MockPolarsFrame("a")
    df2 = MockPolarsFrame("b")

    polars_context.register("node1", df1)
    polars_context.register("node2", df2)

    names = polars_context.list_names()
    assert set(names) == {"node1", "node2"}


def test_metadata_storage(polars_context):
    df = MockPolarsFrame()
    metadata = {"pii": True}

    polars_context.register("node1", df, metadata=metadata)
    assert polars_context.get_metadata("node1") == metadata


def test_clear(polars_context):
    df = MockPolarsFrame()
    polars_context.register("node1", df)
    assert polars_context.has("node1")

    polars_context.clear()
    assert not polars_context.has("node1")
    assert polars_context.list_names() == []


def test_unregister(polars_context):
    df = MockPolarsFrame()
    polars_context.register("node1", df)

    polars_context.unregister("node1")
    assert not polars_context.has("node1")


def test_thread_safety(polars_context):
    """Concurrent registration sanity check."""

    def register_many(thread_idx: int):
        for i in range(100):
            df = MockPolarsFrame(f"{thread_idx}_{i}")
            polars_context.register(f"node_{thread_idx}_{i}", df)

    threads = [threading.Thread(target=register_many, args=(t_idx,)) for t_idx in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(polars_context.list_names()) == 1000

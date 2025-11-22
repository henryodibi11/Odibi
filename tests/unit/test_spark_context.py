import pytest
from unittest.mock import MagicMock
import threading
from odibi.context import SparkContext


class MockSparkSession:
    def __init__(self):
        self.catalog = MagicMock()
        self.table = MagicMock()


class MockDataFrame:
    def __init__(self, name="mock"):
        self.name = name
        self.createOrReplaceTempView = MagicMock()


@pytest.fixture
def spark_context():
    session = MockSparkSession()
    # Inject the class mock so isinstance checks pass if pyspark not present
    ctx = SparkContext(session)
    # If pyspark not installed, _spark_df_type is Any, so check passes.
    # If installed, we need to mock it or ensure our mock passes.
    # For unit tests here, we can override _spark_df_type to allow our mock
    ctx._spark_df_type = (MockDataFrame, type(MagicMock()))
    return ctx


def test_view_name_validation(spark_context):
    """Test that invalid names are rejected immediately."""
    df = MockDataFrame()

    # Case 1: Valid name -> should succeed
    spark_context.register("simple_node", df)
    df.createOrReplaceTempView.assert_called_with("simple_node")

    # Case 2: Spaces -> should error
    with pytest.raises(ValueError) as exc:
        spark_context.register("Load Data", df)
    assert "Invalid node name" in str(exc.value)

    # Case 3: Special chars -> should error
    with pytest.raises(ValueError) as exc:
        spark_context.register("my-node", df)
    assert "alphanumeric" in str(exc.value)


def test_get_returns_table(spark_context):
    """Verify get logic."""
    df = MockDataFrame()
    spark_context.register("node1", df)

    # Get calls table()
    spark_context.get("node1")
    spark_context.spark.table.assert_called_with("node1")


def test_thread_safety(spark_context):
    """Test concurrent registration (sanity check)."""
    # This is hard to deterministically prove fail without lock,
    # but we run it to ensure no deadlocks or errors.

    def register_many():
        for i in range(100):
            df = MockDataFrame()
            spark_context.register(f"node_{threading.get_ident()}_{i}", df)

    threads = [threading.Thread(target=register_many) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(spark_context.list_names()) == 1000


def test_clear(spark_context):
    """Test clearing context."""
    df = MockDataFrame()
    spark_context.register("node1", df)
    assert spark_context.has("node1")

    spark_context.clear()
    assert not spark_context.has("node1")
    spark_context.spark.catalog.dropTempView.assert_called()

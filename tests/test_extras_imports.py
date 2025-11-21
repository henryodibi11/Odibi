"""Test optional dependency imports and guards."""

import pytest


@pytest.mark.extras
def test_spark_engine_import_without_pyspark(monkeypatch):
    """SparkEngine should raise helpful error when pyspark missing."""
    # Hide pyspark
    monkeypatch.setitem(__import__("sys").modules, "pyspark", None)
    monkeypatch.setitem(__import__("sys").modules, "pyspark.sql", None)

    from odibi.engine.spark_engine import SparkEngine

    with pytest.raises(ImportError, match="pip install odibi\\[spark\\]"):
        SparkEngine()


@pytest.mark.extras
@pytest.mark.skipif(
    not pytest.importorskip("pyspark", minversion="3.4"), reason="pyspark not installed"
)
def test_spark_engine_import_with_pyspark():
    """SparkEngine should initialize when pyspark is available."""
    from unittest.mock import MagicMock
    from odibi.engine.spark_engine import SparkEngine

    # Mock SparkSession to avoid Java dependency
    mock_spark = MagicMock()
    engine = SparkEngine(spark_session=mock_spark)
    assert engine.name == "spark"
    assert hasattr(engine, "get_schema")
    assert hasattr(engine, "get_shape")


@pytest.mark.extras
def test_spark_engine_methods_not_implemented():
    """SparkEngine methods should work for Phase 2A."""
    pytest.importorskip("pyspark")
    from unittest.mock import MagicMock
    from odibi.engine.spark_engine import SparkEngine

    # Mock SparkSession to avoid Java dependency
    mock_spark = MagicMock()
    engine = SparkEngine(spark_session=mock_spark)

    # Phase 2A: read/write/execute_sql are implemented
    # execute_transform should raise NotImplementedError
    with pytest.raises(NotImplementedError, match="Phase 2B"):
        engine.execute_transform()

    # execute_operation raises ValueError for unsupported operations
    with pytest.raises(ValueError, match="Unsupported operation"):
        engine.execute_operation("op", {}, None)

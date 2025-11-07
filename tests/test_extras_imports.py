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
    from odibi.engine.spark_engine import SparkEngine

    engine = SparkEngine()
    assert engine.name == "spark"
    assert hasattr(engine, "get_schema")
    assert hasattr(engine, "get_shape")


@pytest.mark.extras
def test_spark_engine_methods_not_implemented():
    """SparkEngine stubs should raise NotImplementedError with helpful messages."""
    pytest.importorskip("pyspark")
    from odibi.engine.spark_engine import SparkEngine

    engine = SparkEngine()

    with pytest.raises(NotImplementedError, match="Phase 3"):
        engine.read()

    with pytest.raises(NotImplementedError, match="Phase 3"):
        engine.write()

    with pytest.raises(NotImplementedError, match="Phase 3"):
        engine.execute_sql()

"""Engine implementations for ODIBI."""

from odibi.engine.base import Engine
from odibi.engine.pandas_engine import PandasEngine

__all__ = ["Engine", "PandasEngine"]


# Lazy import for Spark (optional dependency)
def get_spark_engine():
    from .spark_engine import SparkEngine

    return SparkEngine

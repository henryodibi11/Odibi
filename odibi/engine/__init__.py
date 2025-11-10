"""Engine implementations for ODIBI."""

from odibi.engine.base import Engine
from odibi.engine.pandas_engine import PandasEngine

# Try to import SparkEngine (optional dependency)
try:
    from odibi.engine.spark_engine import SparkEngine

    __all__ = ["Engine", "PandasEngine", "SparkEngine"]
except ImportError:
    # PySpark not available
    __all__ = ["Engine", "PandasEngine"]


# Lazy import helper for Spark (backward compatibility)
def get_spark_engine():
    from .spark_engine import SparkEngine

    return SparkEngine

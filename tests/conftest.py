import sys
import pytest

if sys.platform == "win32":
    try:
        from odibi.engine import spark_engine

        spark_engine.SparkEngine.__init__ = lambda self, *args, **kwargs: pytest.skip(
            "Skipping Spark tests on Windows due to missing winutils"
        )
    except Exception:
        pass
    try:
        from pyspark import SparkContext

        SparkContext.__init__ = lambda self, *args, **kwargs: pytest.skip(
            "Skipping Spark tests (SparkContext) on Windows due to missing winutils"
        )
    except Exception:
        pass
    try:
        from pyspark import SparkContext

        SparkContext.__new__ = lambda cls, *args, **kwargs: pytest.skip(
            "Skipping Spark tests via __new__ on Windows due to missing winutils"
        )
    except Exception:
        pass


def _is_spark_or_delta_name(name: str) -> bool:
    """Check if a filename (not full path) contains spark or delta keywords."""
    name = name.lower()
    return "spark" in name or "delta" in name


def pytest_ignore_collect(collection_path, config):
    # Ignore collection of test FILES whose name contains spark/delta on Windows.
    # Only checks the filename, not the full path, to avoid false positives
    # (e.g., a parent directory named "delta" or test content mentioning delta).
    if sys.platform == "win32" and _is_spark_or_delta_name(collection_path.name):
        return True


def pytest_collection_modifyitems(config, items):
    if sys.platform == "win32":
        items[:] = [item for item in items if not _is_spark_or_delta_name(item.fspath.basename)]

import sys
import logging
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


def pytest_collection_modifyitems(config, items):
    if sys.platform == "win32":
        items[:] = [
            item
            for item in items
            if not (
                ("spark" in item.nodeid.lower() or "delta" in item.nodeid.lower())
                or ("spark" in str(item.fspath).lower() or "delta" in str(item.fspath).lower())
            )
        ]


def pytest_runtest_setup(item):
    # Additional hook to skip tests during setup if they involve Spark/Delta functionality
    if sys.platform == "win32":
        test_id = item.nodeid.lower()
        if "spark" in test_id or "delta" in test_id:
            pytest.skip("Skipping Spark/Delta tests on Windows due to missing winutils")


def pytest_ignore_collect(collection_path, config):
    # Ignore collection of any test file that appears to be Spark or Delta related on Windows
    if sys.platform == "win32" and (
        "spark" in str(collection_path).lower() or "delta" in str(collection_path).lower()
    ):
        return True


def pytest_runtest_call(item):
    if sys.platform == "win32":
        test_id = item.nodeid.lower()
        if "spark" in test_id or "delta" in test_id:
            pytest.skip("Skipping Spark/Delta tests on Windows due to missing winutils")


@pytest.fixture(autouse=True)
def configure_logging():
    """Configure logging to avoid Rich Text object issues in tests."""
    # Disable Rich handlers and use basic logging
    root = logging.getLogger()
    # Remove all Rich handlers
    for handler in root.handlers[:]:
        if hasattr(handler, "__class__") and "Rich" in handler.__class__.__name__:
            root.removeHandler(handler)
    # Set to WARNING level to reduce noise in tests
    logging.basicConfig(level=logging.WARNING, format="%(message)s", force=True)
    yield
    # Cleanup after test
    logging.basicConfig(level=logging.INFO, force=True)

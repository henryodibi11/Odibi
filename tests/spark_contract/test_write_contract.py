import inspect

from odibi.engine.spark_engine import SparkEngine


def test_spark_write_signature():
    """Verify SparkEngine.write signature matches expectation."""
    sig = inspect.signature(SparkEngine.write)
    params = sig.parameters

    expected_params = [
        "df",
        "connection",
        "format",
        "table",
        "path",
        "register_table",
        "mode",
        "options",
    ]

    for param in expected_params:
        assert param in params, f"Missing parameter: {param}"


def test_spark_write_docstring():
    """Verify docstring includes upsert and append_once."""
    doc = SparkEngine.write.__doc__
    assert "upsert" in doc, "Docstring missing 'upsert'"
    assert "append_once" in doc, "Docstring missing 'append_once'"

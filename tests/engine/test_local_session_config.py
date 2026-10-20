"""SparkEngine must configure the Delta SQL extension and catalog on the local builder.

Regression for issue #357: ``configure_spark_with_delta_pip`` only adds the Delta
jars (``spark.jars.packages``); the freshly built local session also needs
``spark.sql.extensions`` and ``spark.sql.catalog.spark_catalog``, or every Delta
operation fails with DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG.

The builder configuration is tested through the module-level helper because
tests/conftest.py replaces ``SparkEngine.__init__`` with ``pytest.skip`` on
Windows (no winutils), so no Windows-runnable test can construct the engine.
The real session round trip lives in test_spark_local_delta_roundtrip.py.
"""

from odibi.engine.spark_engine import _configure_delta_session

DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"


class _RecordingBuilder:
    """Minimal stand-in for ``SparkSession.Builder`` recording ``config()`` calls."""

    def __init__(self):
        self.options = {}

    def config(self, key, value=None):
        self.options[key] = value
        return self


def test_configures_delta_extension_and_catalog():
    builder = _RecordingBuilder()

    result = _configure_delta_session(builder)

    assert result is builder
    assert builder.options == {
        "spark.sql.extensions": DELTA_EXTENSION,
        "spark.sql.catalog.spark_catalog": DELTA_CATALOG,
    }


def test_keeps_options_already_set_on_the_builder():
    builder = _RecordingBuilder()
    builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")

    _configure_delta_session(builder)

    assert builder.options["spark.sql.sources.partitionOverwriteMode"] == "dynamic"
    assert builder.options["spark.sql.extensions"] == DELTA_EXTENSION
    assert len(builder.options) == 3

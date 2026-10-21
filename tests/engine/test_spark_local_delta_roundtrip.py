"""A locally built SparkEngine session must write and read Delta (issue #357).

When no session is active, ``SparkEngine`` builds a local SparkSession. The
builder must set ``spark.sql.extensions`` and ``spark.sql.catalog.spark_catalog``,
or the first Delta operation fails with
DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG.

The test needs a real JVM/Spark session, so it carries the repo's ``extras``
marker: the base CI job runs ``-m "not extras"`` in one shared process, while
the WSL test runner (odibi_test.py, one process per file) applies no marker
filter. It skips when a session is already active, when pyspark's symbols are
unimportable (CI runners, see engine_session_guard), or when Spark cannot start,
and it stops the session it built so single-process runs are not left with one.
"""

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

try:
    from pyspark.sql import SparkSession  # noqa: E402
except ImportError:  # pragma: no cover - CI runners ship pyspark with unimportable symbols
    SparkSession = None  # type: ignore[assignment]

from odibi.connections.local import LocalConnection  # noqa: E402
from odibi.engine.spark_engine import SparkEngine  # noqa: E402
from tests.engine.engine_session_guard import usable_spark_session  # noqa: E402

pytestmark = pytest.mark.extras


def test_local_session_roundtrips_delta(tmp_path):
    if SparkSession is None:
        pytest.skip("pyspark.sql symbols are not importable in this environment")

    usable_spark_session(SparkSession)

    try:
        engine = SparkEngine()
    except RuntimeError as e:
        pytest.skip(f"Spark could not start in this environment: {e}")

    try:
        connection = LocalConnection(base_path=str(tmp_path))

        df = engine.spark.createDataFrame(
            [(1, "alpha", 10.5), (2, "beta", 20.0), (3, "gamma", 0.0)],
            ["order_id", "customer", "amount"],
        )

        commit = engine.write(
            df, connection=connection, format="delta", path="orders", mode="overwrite"
        )

        assert commit is not None, "engine.write must return the Delta commit metadata"
        assert commit.get("version") == 0

        back = engine.read(connection, format="delta", path="orders")
        rows = sorted((row.order_id, row.customer, row.amount) for row in back.collect())
        assert rows == [(1, "alpha", 10.5), (2, "beta", 20.0), (3, "gamma", 0.0)]
    finally:
        engine.spark.stop()

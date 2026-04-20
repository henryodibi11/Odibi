"""Unit tests for CatalogManager Spark-mode read branches using MagicMock.

All pyspark imports are mocked — no real SparkSession or pyspark is needed.
Test names avoid the word 'delta' so the Windows conftest skip filter is not triggered.
"""

import sys
import types
from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Provide a fake pyspark module tree so `from pyspark.sql import functions as F`
# works inside CatalogManager methods without a real pyspark installation.
# ---------------------------------------------------------------------------


class _MockColumn:
    """Fake Column that supports ==, >=, &, so compound filter expressions don't raise."""

    def __eq__(self, other):
        return _MockColumn()

    def __ge__(self, other):
        return _MockColumn()

    def __and__(self, other):
        return _MockColumn()

    def __rand__(self, other):
        return _MockColumn()

    def __hash__(self):
        return id(self)

    def __invert__(self):
        return _MockColumn()

    def cast(self, dataType):
        return _MockColumn()

    def desc(self):
        return _MockColumn()


class _MockFunctions(MagicMock):
    """Mock pyspark.sql.functions whose col() returns _MockColumn instances."""

    def col(self, name):
        return _MockColumn()

    def avg(self, name):
        return _MockColumn()

    def lit(self, value):
        return _MockColumn()

    def date_sub(self, col, days):
        return _MockColumn()

    def current_date(self):
        return _MockColumn()


# Import catalog FIRST so its fallback types (StructType, etc.) are defined,
# THEN install mock pyspark so `from pyspark.sql import functions as F` works
# inside method bodies without a real PySpark.
from odibi.catalog import CatalogManager  # noqa: E402
from odibi.config import SystemConfig  # noqa: E402

_mock_functions = _MockFunctions()
_mock_sql = types.ModuleType("pyspark.sql")
_mock_sql.functions = _mock_functions
_mock_sql.SparkSession = MagicMock

# Build pyspark.sql.types with the real fallback types from catalog.py
from odibi.catalog import (  # noqa: E402
    ArrayType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_mock_types = types.ModuleType("pyspark.sql.types")
_mock_types.StringType = StringType
_mock_types.LongType = LongType
_mock_types.DoubleType = DoubleType
_mock_types.DateType = DateType
_mock_types.TimestampType = TimestampType
_mock_types.ArrayType = ArrayType
_mock_types.StructField = StructField
_mock_types.StructType = StructType
_mock_sql.types = _mock_types

_mock_pyspark = types.ModuleType("pyspark")
_mock_pyspark.sql = _mock_sql

sys.modules["pyspark"] = _mock_pyspark
sys.modules["pyspark.sql"] = _mock_sql
sys.modules["pyspark.sql.functions"] = _mock_functions
sys.modules["pyspark.sql.types"] = _mock_types


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------
@pytest.fixture
def spark_catalog(tmp_path):
    mock_spark = MagicMock()
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    cm = CatalogManager(spark=mock_spark, config=config, base_path=base_path)
    cm.project = "test_project"
    return cm, mock_spark


def _delta_chain(mock_spark):
    """Return the mock sitting at spark.read.format('delta').load(...)."""
    return mock_spark.read.format.return_value.load.return_value


# ---------------------------------------------------------------------------
# 1. _get_all_pipelines_cached — Spark path
# ---------------------------------------------------------------------------
class TestGetAllPipelinesCached:
    def test_returns_dict_keyed_by_pipeline_name(self, spark_catalog):
        cm, mock_spark = spark_catalog
        row1 = MagicMock()
        row1.asDict.return_value = {"pipeline_name": "p1", "version_hash": "h1"}
        row2 = MagicMock()
        row2.asDict.return_value = {"pipeline_name": "p2", "version_hash": "h2"}
        _delta_chain(mock_spark).collect.return_value = [row1, row2]

        result = cm._get_all_pipelines_cached()

        assert "p1" in result
        assert result["p1"]["version_hash"] == "h1"
        assert "p2" in result

    def test_empty_collect_returns_empty_dict(self, spark_catalog):
        cm, mock_spark = spark_catalog
        _delta_chain(mock_spark).collect.return_value = []

        assert cm._get_all_pipelines_cached() == {}

    def test_caches_on_second_call(self, spark_catalog):
        cm, mock_spark = spark_catalog
        row = MagicMock()
        row.asDict.return_value = {"pipeline_name": "p1", "version_hash": "h1"}
        _delta_chain(mock_spark).collect.return_value = [row]

        cm._get_all_pipelines_cached()
        cm._get_all_pipelines_cached()

        # collect should be called only once despite two invocations
        assert _delta_chain(mock_spark).collect.call_count == 1


# ---------------------------------------------------------------------------
# 2. _get_all_nodes_cached — Spark path
# ---------------------------------------------------------------------------
class TestGetAllNodesCached:
    def test_returns_nested_dict(self, spark_catalog):
        cm, mock_spark = spark_catalog
        row1 = MagicMock()
        row1.__getitem__ = lambda self, k: {
            "pipeline_name": "p1",
            "node_name": "n1",
            "version_hash": "h1",
        }[k]
        chain = _delta_chain(mock_spark)
        chain.select.return_value.collect.return_value = [row1]

        result = cm._get_all_nodes_cached()

        assert result == {"p1": {"n1": "h1"}}

    def test_groups_multiple_nodes_under_same_pipeline(self, spark_catalog):
        cm, mock_spark = spark_catalog

        def make_row(p, n, h):
            r = MagicMock()
            r.__getitem__ = lambda self, k: {"pipeline_name": p, "node_name": n, "version_hash": h}[
                k
            ]
            return r

        rows = [make_row("p1", "n1", "h1"), make_row("p1", "n2", "h2"), make_row("p2", "nA", "hA")]
        chain = _delta_chain(mock_spark)
        chain.select.return_value.collect.return_value = rows

        result = cm._get_all_nodes_cached()

        assert result["p1"] == {"n1": "h1", "n2": "h2"}
        assert result["p2"] == {"nA": "hA"}

    def test_exception_returns_empty_dict(self, spark_catalog):
        cm, mock_spark = spark_catalog
        _delta_chain(mock_spark).select.side_effect = Exception("boom")

        result = cm._get_all_nodes_cached()
        assert result == {}


# ---------------------------------------------------------------------------
# 3. _get_all_outputs_cached — Spark path
# ---------------------------------------------------------------------------
class TestGetAllOutputsCached:
    def test_returns_correct_cache_structure(self, spark_catalog):
        cm, mock_spark = spark_catalog
        row = MagicMock()
        row.asDict.return_value = {"pipeline_name": "p1", "node_name": "n1", "output_type": "table"}
        _delta_chain(mock_spark).collect.return_value = [row]

        result = cm._get_all_outputs_cached()

        assert "p1.n1" in result
        assert result["p1.n1"]["output_type"] == "table"

    def test_empty_returns_empty_dict(self, spark_catalog):
        cm, mock_spark = spark_catalog
        _delta_chain(mock_spark).collect.return_value = []

        assert cm._get_all_outputs_cached() == {}


# ---------------------------------------------------------------------------
# 4. _table_exists — Spark path
# ---------------------------------------------------------------------------
class TestTableExists:
    def test_successful_collect_returns_true(self, spark_catalog):
        cm, mock_spark = spark_catalog
        _delta_chain(mock_spark).limit.return_value.collect.return_value = [MagicMock()]

        assert cm._table_exists("/some/path") is True

    def test_path_does_not_exist_returns_false(self, spark_catalog):
        cm, mock_spark = spark_catalog
        _delta_chain(mock_spark).limit.return_value.collect.side_effect = Exception(
            "Path does not exist: /some/path"
        )

        assert cm._table_exists("/some/path") is False

    def test_filenotfound_returns_false(self, spark_catalog):
        cm, mock_spark = spark_catalog
        _delta_chain(mock_spark).limit.return_value.collect.side_effect = Exception(
            "FileNotFoundException: /some/path"
        )

        assert cm._table_exists("/some/path") is False

    def test_analysis_exception_returns_false(self, spark_catalog):
        cm, mock_spark = spark_catalog
        exc = type("AnalysisException", (Exception,), {})("table not found")
        _delta_chain(mock_spark).limit.return_value.collect.side_effect = exc

        assert cm._table_exists("/some/path") is False

    def test_other_exception_returns_false(self, spark_catalog):
        cm, mock_spark = spark_catalog
        _delta_chain(mock_spark).limit.return_value.collect.side_effect = Exception(
            "some auth error"
        )

        assert cm._table_exists("/some/path") is False


# ---------------------------------------------------------------------------
# 5. _read_table — Spark path
# ---------------------------------------------------------------------------
class TestReadTable:
    def test_returns_to_pandas_result(self, spark_catalog):
        cm, mock_spark = spark_catalog
        expected = pd.DataFrame({"a": [1, 2]})
        _delta_chain(mock_spark).toPandas.return_value = expected

        result = cm._read_table("/some/path")
        pd.testing.assert_frame_equal(result, expected)

    def test_exception_returns_empty_dataframe(self, spark_catalog):
        cm, mock_spark = spark_catalog
        _delta_chain(mock_spark).toPandas.side_effect = Exception("boom")

        result = cm._read_table("/some/path")
        assert isinstance(result, pd.DataFrame)
        assert result.empty


# ---------------------------------------------------------------------------
# 6. get_average_volume — Spark path
# ---------------------------------------------------------------------------
class TestGetAverageVolume:
    def test_returns_average(self, spark_catalog):
        cm, mock_spark = spark_catalog
        # Use a tuple — catalog code does stats[0]
        mock_stats = (100.0,)
        chain = _delta_chain(mock_spark)
        chain.filter.return_value.agg.return_value.first.return_value = mock_stats

        result = cm.get_average_volume("node_a", days=7)
        assert result == 100.0

    def test_returns_none_when_stats_none(self, spark_catalog):
        cm, mock_spark = spark_catalog
        chain = _delta_chain(mock_spark)
        # stats is truthy but stats[0] is None
        chain.filter.return_value.agg.return_value.first.return_value = (None,)

        result = cm.get_average_volume("node_a")
        # stats[0] is None but `stats` is truthy, so code returns None (stats is truthy tuple)
        # Actually catalog code: `return stats[0] if stats else None` → returns None value
        assert result is None

    def test_exception_returns_none(self, spark_catalog):
        cm, mock_spark = spark_catalog
        _delta_chain(mock_spark).filter.side_effect = Exception("boom")

        assert cm.get_average_volume("node_a") is None


# ---------------------------------------------------------------------------
# 7. get_average_duration — Spark path
# ---------------------------------------------------------------------------
class TestGetAverageDuration:
    def test_returns_ms_divided_by_1000(self, spark_catalog):
        cm, mock_spark = spark_catalog
        mock_stats = (5000.0,)
        chain = _delta_chain(mock_spark)
        chain.filter.return_value.agg.return_value.first.return_value = mock_stats

        result = cm.get_average_duration("node_a", days=7)
        assert result == 5.0

    def test_returns_none_when_value_is_none(self, spark_catalog):
        cm, mock_spark = spark_catalog
        mock_stats = (None,)
        chain = _delta_chain(mock_spark)
        chain.filter.return_value.agg.return_value.first.return_value = mock_stats

        result = cm.get_average_duration("node_a")
        assert result is None


# ---------------------------------------------------------------------------
# 8. resolve_table_path — Spark path
# ---------------------------------------------------------------------------
class TestResolveTablePath:
    def test_returns_path_when_found(self, spark_catalog):
        cm, mock_spark = spark_catalog
        mock_row = MagicMock()
        mock_row.path = "/data/gold/orders"
        chain = _delta_chain(mock_spark)
        chain.filter.return_value.select.return_value.first.return_value = mock_row

        assert cm.resolve_table_path("gold.orders") == "/data/gold/orders"

    def test_returns_none_when_not_found(self, spark_catalog):
        cm, mock_spark = spark_catalog
        chain = _delta_chain(mock_spark)
        chain.filter.return_value.select.return_value.first.return_value = None

        assert cm.resolve_table_path("nonexistent") is None

    def test_exception_returns_none(self, spark_catalog):
        cm, mock_spark = spark_catalog
        _delta_chain(mock_spark).filter.side_effect = Exception("boom")

        assert cm.resolve_table_path("any") is None


# ---------------------------------------------------------------------------
# 9. _get_latest_schema — Spark path
# ---------------------------------------------------------------------------
class TestGetLatestSchema:
    def test_returns_row_as_dict_when_found(self, spark_catalog):
        cm, mock_spark = spark_catalog
        mock_row = MagicMock()
        mock_row.asDict.return_value = {"table_path": "silver/x", "schema_version": 3}
        chain = _delta_chain(mock_spark)
        chain.filter.return_value.orderBy.return_value.first.return_value = mock_row

        result = cm._get_latest_schema("silver/x")
        assert result == {"table_path": "silver/x", "schema_version": 3}

    def test_returns_none_when_not_found(self, spark_catalog):
        cm, mock_spark = spark_catalog
        chain = _delta_chain(mock_spark)
        chain.filter.return_value.orderBy.return_value.first.return_value = None

        assert cm._get_latest_schema("silver/x") is None


# ---------------------------------------------------------------------------
# 10. get_schema_history — Spark path
# ---------------------------------------------------------------------------
class TestGetSchemaHistory:
    def test_returns_list_of_dicts(self, spark_catalog):
        cm, mock_spark = spark_catalog
        row1 = MagicMock()
        row1.asDict.return_value = {"schema_version": 2}
        row2 = MagicMock()
        row2.asDict.return_value = {"schema_version": 1}
        chain = _delta_chain(mock_spark)
        chain.filter.return_value.orderBy.return_value.limit.return_value.collect.return_value = [
            row1,
            row2,
        ]

        result = cm.get_schema_history("silver/x", limit=10)
        assert result == [{"schema_version": 2}, {"schema_version": 1}]

    def test_exception_returns_empty_list(self, spark_catalog):
        cm, mock_spark = spark_catalog
        _delta_chain(mock_spark).filter.side_effect = Exception("boom")

        assert cm.get_schema_history("silver/x") == []


# ---------------------------------------------------------------------------
# 11. is_spark_mode property
# ---------------------------------------------------------------------------
class TestIsSparkMode:
    def test_true_when_spark_is_set(self, spark_catalog):
        cm, _ = spark_catalog
        assert cm.is_spark_mode is True

    def test_false_when_spark_is_none(self, tmp_path):
        config = SystemConfig(connection="local", path="_odibi_system")
        cm = CatalogManager(spark=None, config=config, base_path=str(tmp_path))
        assert cm.is_spark_mode is False


# ---------------------------------------------------------------------------
# 12. _get_run_ids_spark
# ---------------------------------------------------------------------------
class TestGetRunIdsSpark:
    def test_returns_list_of_run_ids(self, spark_catalog):
        cm, mock_spark = spark_catalog
        row1 = MagicMock()
        row1.__getitem__ = lambda self, k: "run-001"
        row2 = MagicMock()
        row2.__getitem__ = lambda self, k: "run-002"
        chain = _delta_chain(mock_spark)
        chain.select.return_value.collect.return_value = [row1, row2]

        result = cm._get_run_ids_spark(None, None)
        assert result == ["run-001", "run-002"]

    def test_with_pipeline_name_filter(self, spark_catalog):
        cm, mock_spark = spark_catalog
        row = MagicMock()
        row.__getitem__ = lambda self, k: "run-filtered"
        chain = _delta_chain(mock_spark)
        # After filter, select/collect still works
        chain.filter.return_value.select.return_value.collect.return_value = [row]

        result = cm._get_run_ids_spark("my_pipeline", None)
        assert result == ["run-filtered"]

    def test_with_since_date_filter(self, spark_catalog):
        cm, mock_spark = spark_catalog
        row = MagicMock()
        row.__getitem__ = lambda self, k: "run-since"
        chain = _delta_chain(mock_spark)
        chain.filter.return_value.filter.return_value.select.return_value.collect.return_value = [
            row
        ]

        result = cm._get_run_ids_spark("pipe", date(2025, 1, 1))
        assert result == ["run-since"]

    def test_exception_returns_empty_list(self, spark_catalog):
        cm, mock_spark = spark_catalog
        _delta_chain(mock_spark).filter.side_effect = Exception("boom")

        assert cm._get_run_ids_spark("p", None) == []


# ---------------------------------------------------------------------------
# 13. _get_pipeline_run_spark
# ---------------------------------------------------------------------------
class TestGetPipelineRunSpark:
    def test_returns_as_dict_when_found(self, spark_catalog):
        cm, mock_spark = spark_catalog
        mock_row = MagicMock()
        mock_row.asDict.return_value = {"run_id": "r1", "status": "SUCCESS"}
        chain = _delta_chain(mock_spark)
        chain.filter.return_value.collect.return_value = [mock_row]

        result = cm._get_pipeline_run_spark("r1")
        assert result == {"run_id": "r1", "status": "SUCCESS"}

    def test_returns_none_when_empty(self, spark_catalog):
        cm, mock_spark = spark_catalog
        chain = _delta_chain(mock_spark)
        chain.filter.return_value.collect.return_value = []

        assert cm._get_pipeline_run_spark("r999") is None

    def test_exception_returns_none(self, spark_catalog):
        cm, mock_spark = spark_catalog
        _delta_chain(mock_spark).filter.side_effect = Exception("boom")

        assert cm._get_pipeline_run_spark("r1") is None

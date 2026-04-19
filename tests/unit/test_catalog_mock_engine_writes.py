"""Tests for Spark write branches in CatalogManager using MagicMock for SparkSession.

All PySpark imports are mocked — no real Spark is needed.
Test names avoid the word 'delta' so conftest skips on Windows don't apply.
"""

import sys
import types
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

# Import catalog FIRST so its fallback types (StructType, etc.) are defined,
# THEN install mock pyspark so `from pyspark.sql import functions as F` works
# inside method bodies without a real PySpark.
from odibi.catalog import CatalogManager  # noqa: E402
from odibi.config import SystemConfig  # noqa: E402

# ---------------------------------------------------------------------------
# Install fake pyspark modules using the real fallback types from catalog.py
# ---------------------------------------------------------------------------
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

# Reuse existing mock functions if already installed (e.g., by reads file),
# otherwise create a fresh MagicMock.
_mock_functions = sys.modules.get("pyspark.sql.functions") or MagicMock()
_mock_types = types.ModuleType("pyspark.sql.types")
_mock_types.StringType = StringType
_mock_types.LongType = LongType
_mock_types.DoubleType = DoubleType
_mock_types.DateType = DateType
_mock_types.TimestampType = TimestampType
_mock_types.ArrayType = ArrayType
_mock_types.StructField = StructField
_mock_types.StructType = StructType

_mock_sql = types.ModuleType("pyspark.sql")
_mock_sql.functions = _mock_functions
_mock_sql.SparkSession = MagicMock
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


# =========================================================================
# 1. _ensure_table
# =========================================================================


class TestEnsureTable:
    def test_creates_table_without_partitions(self, spark_catalog):
        cm, mock_spark = spark_catalog
        schema = MagicMock()
        with patch.object(cm, "_table_exists", return_value=False):
            cm._ensure_table("meta_runs", schema)
        mock_spark.createDataFrame.assert_called_once_with([], schema)
        writer = mock_spark.createDataFrame.return_value.write.format.return_value
        writer.save.assert_called_once_with(cm.tables["meta_runs"])

    def test_creates_table_with_partition_cols(self, spark_catalog):
        cm, mock_spark = spark_catalog
        schema = MagicMock()
        with patch.object(cm, "_table_exists", return_value=False):
            cm._ensure_table("meta_runs", schema, partition_cols=["date"])
        writer = mock_spark.createDataFrame.return_value.write.format.return_value
        writer.partitionBy.assert_called_once_with("date")
        writer.partitionBy.return_value.save.assert_called_once_with(cm.tables["meta_runs"])

    def test_skips_creation_when_table_exists(self, spark_catalog):
        cm, mock_spark = spark_catalog
        schema = MagicMock()
        with patch.object(cm, "_table_exists", return_value=True):
            cm._ensure_table("meta_runs", schema)
        mock_spark.createDataFrame.assert_not_called()


# =========================================================================
# 2. register_pipelines_batch
# =========================================================================


class TestRegisterPipelinesBatch:
    def _make_records(self, n=1):
        return [
            {
                "pipeline_name": f"pipe_{i}",
                "version_hash": "abc",
                "description": "desc",
                "layer": "silver",
                "schedule": "daily",
                "tags_json": "{}",
            }
            for i in range(n)
        ]

    def test_calls_create_dataframe_sql_and_drop(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm.register_pipelines_batch(self._make_records())
        mock_spark.createDataFrame.assert_called_once()
        mock_spark.sql.assert_called_once()
        mock_spark.catalog.dropTempView.assert_called_once()

    def test_merge_sql_contains_merge_into(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm.register_pipelines_batch(self._make_records())
        sql_arg = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO" in sql_arg

    def test_invalidates_pipelines_cache(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm._pipelines_cache = {"old": "data"}
        cm.register_pipelines_batch(self._make_records())
        assert cm._pipelines_cache is None

    def test_empty_records_is_noop(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm.register_pipelines_batch([])
        mock_spark.createDataFrame.assert_not_called()


# =========================================================================
# 3. register_nodes_batch
# =========================================================================


class TestRegisterNodesBatch:
    def _make_records(self, n=1):
        return [
            {
                "pipeline_name": f"pipe_{i}",
                "node_name": f"node_{i}",
                "version_hash": "abc",
                "type": "transform",
                "config_json": "{}",
            }
            for i in range(n)
        ]

    def test_calls_create_dataframe_sql_and_drop(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm.register_nodes_batch(self._make_records())
        mock_spark.createDataFrame.assert_called_once()
        mock_spark.sql.assert_called_once()
        mock_spark.catalog.dropTempView.assert_called_once()

    def test_invalidates_nodes_cache(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm._nodes_cache = {"old": "data"}
        cm.register_nodes_batch(self._make_records())
        assert cm._nodes_cache is None

    def test_empty_records_is_noop(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm.register_nodes_batch([])
        mock_spark.createDataFrame.assert_not_called()


# =========================================================================
# 4. register_outputs_batch
# =========================================================================


class TestRegisterOutputsBatch:
    def _make_records(self, n=1):
        return [
            {
                "pipeline_name": f"pipe_{i}",
                "node_name": f"node_{i}",
                "output_type": "managed_table",
                "connection_name": None,
                "path": "/tmp/out",
                "format": "parquet",
                "table_name": None,
                "last_run": datetime.now(timezone.utc),
                "row_count": 100,
            }
            for i in range(n)
        ]

    def test_calls_create_dataframe_sql_and_drop(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm.register_outputs_batch(self._make_records())
        mock_spark.createDataFrame.assert_called_once()
        mock_spark.sql.assert_called_once()
        mock_spark.catalog.dropTempView.assert_called_once()

    def test_invalidates_outputs_cache(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm._outputs_cache = {"old": "data"}
        cm.register_outputs_batch(self._make_records())
        assert cm._outputs_cache is None


# =========================================================================
# 5. log_run
# =========================================================================


class TestLogRun:
    def test_write_append_mode(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm.log_run("run1", "pipe", "node", "success", 100, 500, "{}")
        mock_spark.createDataFrame.assert_called_once()
        chain = mock_spark.createDataFrame.return_value
        wc1 = chain.withColumn.return_value
        wc2 = wc1.withColumn.return_value
        wc2.write.format.return_value.mode.return_value.save.assert_called_once_with(
            cm.tables["meta_runs"]
        )

    def test_passes_correct_data(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm.log_run("run1", "pipe", "node", "success", 42, 999, '{"k": "v"}')
        args = mock_spark.createDataFrame.call_args[0][0]
        row = args[0]
        assert row[0] == "run1"
        assert row[1] == "test_project"
        assert row[2] == "pipe"
        assert row[3] == "node"
        assert row[4] == "success"
        assert row[5] == 42
        assert row[6] == 999


# =========================================================================
# 6. log_runs_batch
# =========================================================================


class TestLogRunsBatch:
    def test_creates_dataframe_and_appends(self, spark_catalog):
        cm, mock_spark = spark_catalog
        records = [
            {"run_id": "r1", "pipeline_name": "p", "node_name": "n", "status": "success"},
            {"run_id": "r2", "pipeline_name": "p", "node_name": "n2", "status": "failed"},
        ]
        cm.log_runs_batch(records)
        mock_spark.createDataFrame.assert_called_once()
        chain = mock_spark.createDataFrame.return_value
        wc1 = chain.withColumn.return_value
        wc2 = wc1.withColumn.return_value
        wc2.write.format.return_value.mode.return_value.save.assert_called_once()


# =========================================================================
# 7. log_pipeline_run
# =========================================================================


class TestLogPipelineRun:
    def _make_run(self, **overrides):
        data = {
            "run_id": "pr1",
            "pipeline_name": "pipe",
            "owner": "owner",
            "layer": "silver",
            "run_start_at": datetime.now(timezone.utc),
            "run_end_at": datetime.now(timezone.utc),
            "duration_ms": 1000,
            "status": "success",
            "nodes_total": 3,
            "nodes_succeeded": 3,
            "nodes_failed": 0,
            "nodes_skipped": 0,
            "rows_processed": 500,
            "error_summary": None,
            "terminal_nodes": "n1,n2",
            "environment": "dev",
            "databricks_cluster_id": None,
            "databricks_job_id": None,
            "databricks_workspace_id": None,
            "estimated_cost_usd": None,
            "actual_cost_usd": None,
            "cost_source": None,
            "created_at": datetime.now(timezone.utc),
        }
        data.update(overrides)
        return data

    def test_write_append_to_pipeline_runs(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm.log_pipeline_run(self._make_run())
        mock_spark.createDataFrame.assert_called_once()
        writer = mock_spark.createDataFrame.return_value.write
        writer.format.return_value.mode.return_value.save.assert_called_once_with(
            cm.tables["meta_pipeline_runs"]
        )

    def test_handles_optional_fields(self, spark_catalog):
        cm, mock_spark = spark_catalog
        run = self._make_run(owner=None, layer=None, error_summary=None)
        cm.log_pipeline_run(run)
        mock_spark.createDataFrame.assert_called_once()


# =========================================================================
# 8. log_node_runs_batch
# =========================================================================


class TestLogNodeRunsBatch:
    def test_creates_dataframe_and_appends(self, spark_catalog):
        cm, mock_spark = spark_catalog
        results = [
            {
                "run_id": "r1",
                "node_id": "nid1",
                "pipeline_name": "pipe",
                "node_name": "n1",
                "status": "success",
                "run_start_at": datetime.now(timezone.utc),
                "run_end_at": datetime.now(timezone.utc),
                "duration_ms": 100,
                "rows_processed": 50,
                "estimated_cost_usd": None,
                "metrics_json": "{}",
                "environment": "dev",
                "created_at": datetime.now(timezone.utc),
            }
        ]
        cm.log_node_runs_batch(results)
        mock_spark.createDataFrame.assert_called_once()
        writer = mock_spark.createDataFrame.return_value.write
        writer.format.return_value.mode.return_value.save.assert_called_once_with(
            cm.tables["meta_node_runs"]
        )


# =========================================================================
# 9. log_failure
# =========================================================================


class TestLogFailure:
    def test_creates_dataframe_and_appends(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm.log_failure("f1", "r1", "pipe", "node", "ValueError", "oops", "traceback")
        mock_spark.createDataFrame.assert_called_once()
        writer = mock_spark.createDataFrame.return_value.write
        writer.format.return_value.mode.return_value.save.assert_called_once_with(
            cm.tables["meta_failures"]
        )

    def test_truncates_error_and_stack(self, spark_catalog):
        cm, mock_spark = spark_catalog
        long_msg = "x" * 2000
        long_trace = "y" * 5000
        cm.log_failure("f1", "r1", "pipe", "node", "Err", long_msg, long_trace)
        row = mock_spark.createDataFrame.call_args[0][0][0]
        # error_message at index 6, stack_trace at index 8
        assert len(row[6]) == 1000
        assert len(row[8]) == 2000


# =========================================================================
# 10. log_pattern
# =========================================================================


class TestLogPattern:
    def test_creates_dataframe_and_appends(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm.log_pattern("my_table", "scd2", "{}", 0.95)
        mock_spark.createDataFrame.assert_called_once()
        writer = mock_spark.createDataFrame.return_value.write
        writer.format.return_value.mode.return_value.save.assert_called_once_with(
            cm.tables["meta_patterns"]
        )


# =========================================================================
# 11. register_asset
# =========================================================================


class TestRegisterAsset:
    def test_calls_sql_with_merge_and_drops_view(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm.register_asset("proj", "tbl", "/path", "parquet", "scd1", "hash1")
        mock_spark.sql.assert_called_once()
        sql_arg = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO" in sql_arg
        mock_spark.catalog.dropTempView.assert_called_once()


# =========================================================================
# 12. track_schema
# =========================================================================


class TestTrackSchema:
    def test_writes_schema_record(self, spark_catalog):
        cm, mock_spark = spark_catalog
        with patch.object(cm, "_get_latest_schema", return_value=None):
            result = cm.track_schema(
                "silver/customers",
                {"id": "int", "name": "string"},
                "pipe",
                "node",
                "run1",
            )
        mock_spark.createDataFrame.assert_called_once()
        writer = mock_spark.createDataFrame.return_value.write
        writer.format.return_value.mode.return_value.save.assert_called_once_with(
            cm.tables["meta_schemas"]
        )
        assert result["changed"] is True
        assert result["version"] == 1


# =========================================================================
# 13. record_lineage
# =========================================================================


class TestRecordLineage:
    def test_calls_sql_with_merge_and_drops_view(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm.record_lineage("src_tbl", "tgt_tbl", "pipe", "node", "run1")
        mock_spark.sql.assert_called_once()
        sql_arg = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO" in sql_arg
        mock_spark.catalog.dropTempView.assert_called_once()


# =========================================================================
# 14. record_lineage_batch
# =========================================================================


class TestRecordLineageBatch:
    def test_calls_sql_with_merge_and_drops_view(self, spark_catalog):
        cm, mock_spark = spark_catalog
        records = [
            {
                "source_table": "src",
                "target_table": "tgt",
                "target_pipeline": "pipe",
                "target_node": "node",
                "run_id": "r1",
            }
        ]
        cm.record_lineage_batch(records)
        mock_spark.sql.assert_called_once()
        sql_arg = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO" in sql_arg
        mock_spark.catalog.dropTempView.assert_called_once()


# =========================================================================
# 15. register_assets_batch
# =========================================================================


class TestRegisterAssetsBatch:
    def test_calls_sql_with_merge_and_drops_view(self, spark_catalog):
        cm, mock_spark = spark_catalog
        records = [
            {
                "project": "proj",
                "table_name": "tbl",
                "path": "/p",
                "format": "parquet",
                "pattern_type": "scd1",
            }
        ]
        cm.register_assets_batch(records)
        mock_spark.sql.assert_called_once()
        sql_arg = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO" in sql_arg
        mock_spark.catalog.dropTempView.assert_called_once()


# =========================================================================
# 16. remove_pipeline
# =========================================================================


class TestRemovePipeline:
    def test_reads_filters_overwrites_both_tables(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm.remove_pipeline("old_pipe")
        # read.format().load() called twice: meta_pipelines and meta_nodes
        assert mock_spark.read.format.return_value.load.call_count == 2
        load_rv = mock_spark.read.format.return_value.load.return_value
        assert load_rv.cache.call_count == 2
        filter_rv = load_rv.filter.return_value
        assert filter_rv.write.format.return_value.mode.return_value.save.call_count == 2


# =========================================================================
# 17. remove_node
# =========================================================================


class TestRemoveNode:
    def test_reads_filters_overwrites_nodes(self, spark_catalog):
        cm, mock_spark = spark_catalog
        cm.remove_node("pipe", "bad_node")
        mock_spark.read.format.return_value.load.assert_called_once_with(cm.tables["meta_nodes"])
        load_rv = mock_spark.read.format.return_value.load.return_value
        load_rv.cache.assert_called_once()
        filter_rv = load_rv.filter.return_value
        filter_rv.write.format.return_value.mode.return_value.save.assert_called_once_with(
            cm.tables["meta_nodes"]
        )


# =========================================================================
# 18. _optimize_spark
# =========================================================================


class TestOptimizeSpark:
    def test_optimize_with_zorder_and_vacuum(self, spark_catalog):
        cm, mock_spark = spark_catalog
        result = cm._optimize_spark("meta_runs", "/path/meta_runs", 168)
        calls = [c[0][0] for c in mock_spark.sql.call_args_list]
        assert any("OPTIMIZE" in c and "ZORDER BY" in c for c in calls)
        assert any("VACUUM" in c for c in calls)
        assert result["success"] is True
        assert result["zorder"] == "timestamp"

    def test_falls_back_to_plain_optimize_on_zorder_failure(self, spark_catalog):
        cm, mock_spark = spark_catalog

        def side_effect(sql):
            if "ZORDER" in sql:
                raise Exception("ZORDER failed")
            return MagicMock()

        mock_spark.sql.side_effect = side_effect
        result = cm._optimize_spark("meta_runs", "/path/meta_runs", 168)
        assert result["success"] is True
        plain_calls = [c[0][0] for c in mock_spark.sql.call_args_list]
        assert any("OPTIMIZE" in c and "ZORDER" not in c for c in plain_calls)

    def test_table_without_zorder_column(self, spark_catalog):
        cm, mock_spark = spark_catalog
        result = cm._optimize_spark("unknown_table", "/path/unknown", 168)
        calls = [c[0][0] for c in mock_spark.sql.call_args_list]
        assert not any("ZORDER" in c for c in calls)
        assert any("OPTIMIZE" in c for c in calls)
        assert any("VACUUM" in c for c in calls)
        assert result["zorder"] is None


# =========================================================================
# 19. get_upstream / get_downstream
# =========================================================================


class TestGetUpstreamDownstream:
    def _setup_lineage_read(self, mock_spark, relationships):
        """Configure mock spark.read to return lineage rows for BFS traversal."""

        def make_row(rec):
            r = MagicMock()
            r.asDict.return_value = dict(rec)
            for k, v in rec.items():
                setattr(r, k, v)
            return r

        rows = [make_row(r) for r in relationships]

        def fake_filter(cond):
            filtered = MagicMock()
            filtered.collect.return_value = rows
            return filtered

        df = MagicMock()
        df.filter.side_effect = fake_filter
        mock_spark.read.format.return_value.load.return_value = df

    def test_upstream_bfs_with_mocked_reads(self, spark_catalog):
        cm, mock_spark = spark_catalog
        relationships = [{"source_table": "bronze/raw", "target_table": "silver/clean", "depth": 0}]
        self._setup_lineage_read(mock_spark, relationships)
        result = cm.get_upstream("silver/clean", depth=1)
        assert len(result) >= 1

    def test_downstream_bfs_with_mocked_reads(self, spark_catalog):
        cm, mock_spark = spark_catalog
        relationships = [{"source_table": "silver/clean", "target_table": "gold/agg", "depth": 0}]
        self._setup_lineage_read(mock_spark, relationships)
        result = cm.get_downstream("silver/clean", depth=1)
        assert len(result) >= 1


# =========================================================================
# 20. _migrate_schema_if_needed
# =========================================================================


class TestMigrateSchemaIfNeeded:
    def test_detects_type_mismatch_and_migrates(self, spark_catalog):
        cm, mock_spark = spark_catalog

        from odibi.catalog import ArrayType, StringType, StructField, StructType

        existing_array_type = ArrayType(StringType())

        existing_field = MagicMock()
        existing_field.name = "tags"
        existing_field.dataType = existing_array_type

        existing_df = MagicMock()
        existing_df.schema.fields = [existing_field]
        mock_spark.read.format.return_value.load.return_value = existing_df

        expected_schema = StructType([StructField("tags", StringType(), True)])

        cm._migrate_schema_if_needed("meta_test", "/path/meta_test", expected_schema)

        existing_df.withColumn.assert_called_once()
        migrated = existing_df.withColumn.return_value
        migrated.write.format.return_value.mode.return_value.option.return_value.save.assert_called_once()

    def test_no_migration_when_types_match(self, spark_catalog):
        cm, mock_spark = spark_catalog

        from odibi.catalog import StringType, StructField, StructType

        mock_str_type = StringType()

        existing_field = MagicMock()
        existing_field.name = "name"
        existing_field.dataType = mock_str_type

        existing_df = MagicMock()
        existing_df.schema.fields = [existing_field]
        mock_spark.read.format.return_value.load.return_value = existing_df

        expected_schema = StructType([StructField("name", StringType(), True)])

        cm._migrate_schema_if_needed("meta_test", "/path/meta_test", expected_schema)

        existing_df.withColumn.assert_not_called()

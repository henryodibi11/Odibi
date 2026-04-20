"""Tests for CatalogManager batch registration and utility methods.

Covers: register_pipelines_batch, register_nodes_batch, register_outputs_batch (Pandas paths),
        log_pattern, register_asset, resolve_table_path, get_pipeline_hash,
        get_average_volume, get_average_duration, log_metrics  (issue #291 + utilities).

NOTE: Test names avoid 'spark' and 'delta' to prevent conftest.py from skipping on Windows.
"""

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import write_deltalake

from odibi.catalog import CatalogManager
from odibi.config import SystemConfig
from odibi.engine.pandas_engine import PandasEngine


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog_manager(tmp_path):
    """CatalogManager in Pandas mode, bootstrapped, with project set."""
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    engine = PandasEngine(config={})
    cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
    cm.bootstrap()
    cm.project = "test_project"
    return cm


@pytest.fixture
def bare_catalog(tmp_path):
    """CatalogManager with no engine and no spark."""
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=None)
    return cm


def _read_table(cm, table_key):
    """Read a catalog table and return a pandas DataFrame."""
    from deltalake import DeltaTable

    dt = DeltaTable(cm.tables[table_key])
    return dt.to_pandas()


def _seed_runs(cm, records):
    """Seed meta_runs with run records."""
    schema = pa.schema(
        [
            ("run_id", pa.string()),
            ("project", pa.string()),
            ("pipeline_name", pa.string()),
            ("node_name", pa.string()),
            ("status", pa.string()),
            ("rows_processed", pa.int64()),
            ("duration_ms", pa.int64()),
            ("metrics_json", pa.string()),
            ("environment", pa.string()),
            ("timestamp", pa.timestamp("us", tz="UTC")),
            ("date", pa.date32()),
        ]
    )
    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "run_id": [r.get("run_id", "r1") for r in records],
            "project": [r.get("project", "proj") for r in records],
            "pipeline_name": [r.get("pipeline_name", "pipe") for r in records],
            "node_name": [r["node_name"] for r in records],
            "status": [r.get("status", "SUCCESS") for r in records],
            "rows_processed": [r.get("rows_processed", 100) for r in records],
            "duration_ms": [r.get("duration_ms", 5000) for r in records],
            "metrics_json": [r.get("metrics_json", "{}") for r in records],
            "environment": [r.get("environment") for r in records],
            "timestamp": [r.get("timestamp", now) for r in records],
            "date": [
                r.get("timestamp", now).date()
                if hasattr(r.get("timestamp", now), "date")
                else now.date()
                for r in records
            ],
        }
    )
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    write_deltalake(cm.tables["meta_runs"], table, mode="overwrite", engine="rust")


def _seed_tables(cm, records):
    """Seed meta_tables with asset records."""
    schema = pa.schema(
        [
            ("project", pa.string()),
            ("table_name", pa.string()),
            ("path", pa.string()),
            ("format", pa.string()),
            ("pattern_type", pa.string()),
            ("schema_hash", pa.string()),
            ("updated_at", pa.timestamp("us", tz="UTC")),
        ]
    )
    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "project": [r.get("project", "proj") for r in records],
            "table_name": [r["table_name"] for r in records],
            "path": [r["path"] for r in records],
            "format": [r.get("format", "parquet") for r in records],
            "pattern_type": [r.get("pattern_type", "dimension") for r in records],
            "schema_hash": [r.get("schema_hash", "") for r in records],
            "updated_at": [now] * len(records),
        }
    )
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    write_deltalake(cm.tables["meta_tables"], table, mode="overwrite", engine="rust")


def _seed_pipelines(cm, records):
    """Seed meta_pipelines with pipeline records."""
    schema = pa.schema(
        [
            ("pipeline_name", pa.string()),
            ("version_hash", pa.string()),
            ("description", pa.string()),
            ("layer", pa.string()),
            ("schedule", pa.string()),
            ("tags_json", pa.string()),
            ("updated_at", pa.timestamp("us", tz="UTC")),
        ]
    )
    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "pipeline_name": [r["pipeline_name"] for r in records],
            "version_hash": [r["version_hash"] for r in records],
            "description": [r.get("description", "desc") for r in records],
            "layer": [r.get("layer", "bronze") for r in records],
            "schedule": [r.get("schedule", "daily") for r in records],
            "tags_json": [r.get("tags_json", "{}") for r in records],
            "updated_at": [now] * len(records),
        }
    )
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    write_deltalake(cm.tables["meta_pipelines"], table, mode="overwrite", engine="rust")


# ===========================================================================
# register_pipelines_batch()
# ===========================================================================


class TestRegisterPipelinesBatch:
    """Tests for CatalogManager.register_pipelines_batch() Pandas paths."""

    def test_empty_records_no_op(self, catalog_manager):
        catalog_manager.register_pipelines_batch([])

    def test_no_engine_no_op(self, bare_catalog):
        bare_catalog.register_pipelines_batch(
            [
                {
                    "pipeline_name": "p",
                    "version_hash": "h",
                    "description": "d",
                    "layer": "bronze",
                    "schedule": "daily",
                    "tags_json": "{}",
                }
            ]
        )

    def test_registers_multiple_pipelines(self, catalog_manager):
        records = [
            {
                "pipeline_name": "etl_orders",
                "version_hash": "abc",
                "description": "Orders ETL",
                "layer": "silver",
                "schedule": "hourly",
                "tags_json": '{"team": "data"}',
            },
            {
                "pipeline_name": "etl_customers",
                "version_hash": "def",
                "description": "Customers",
                "layer": "silver",
                "schedule": "daily",
                "tags_json": "{}",
            },
        ]
        catalog_manager.register_pipelines_batch(records)
        df = _read_table(catalog_manager, "meta_pipelines")
        assert len(df) == 2
        assert set(df["pipeline_name"]) == {"etl_orders", "etl_customers"}

    def test_upsert_updates_existing(self, catalog_manager):
        records = [
            {
                "pipeline_name": "pipe",
                "version_hash": "v1",
                "description": "d",
                "layer": "bronze",
                "schedule": "daily",
                "tags_json": "{}",
            },
        ]
        catalog_manager.register_pipelines_batch(records)
        records[0]["version_hash"] = "v2"
        catalog_manager.register_pipelines_batch(records)
        df = _read_table(catalog_manager, "meta_pipelines")
        assert len(df) == 1
        assert df.iloc[0]["version_hash"] == "v2"

    def test_clears_cache_after_registration(self, catalog_manager):
        catalog_manager._pipelines_cache = {"stale": {}}
        records = [
            {
                "pipeline_name": "p",
                "version_hash": "h",
                "description": "d",
                "layer": "bronze",
                "schedule": "daily",
                "tags_json": "{}",
            },
        ]
        catalog_manager.register_pipelines_batch(records)
        assert catalog_manager._pipelines_cache is None

    def test_failure_logs_warning(self, catalog_manager):
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(catalog_manager.engine, "write", side_effect=IOError("fail")):
                catalog_manager.register_pipelines_batch(
                    [
                        {
                            "pipeline_name": "p",
                            "version_hash": "h",
                            "description": "d",
                            "layer": "bronze",
                            "schedule": "daily",
                            "tags_json": "{}",
                        }
                    ]
                )
        mock_logger.warning.assert_called_once()
        assert "Failed to batch register pipelines" in mock_logger.warning.call_args[0][0]


# ===========================================================================
# register_nodes_batch()
# ===========================================================================


class TestRegisterNodesBatch:
    """Tests for CatalogManager.register_nodes_batch() Pandas paths."""

    def test_empty_records_no_op(self, catalog_manager):
        catalog_manager.register_nodes_batch([])

    def test_no_engine_no_op(self, bare_catalog):
        bare_catalog.register_nodes_batch(
            [
                {
                    "pipeline_name": "p",
                    "node_name": "n",
                    "version_hash": "h",
                    "type": "transform",
                    "config_json": "{}",
                }
            ]
        )

    def test_registers_multiple_nodes(self, catalog_manager):
        records = [
            {
                "pipeline_name": "pipe",
                "node_name": "load",
                "version_hash": "h1",
                "type": "extract",
                "config_json": '{"source": "api"}',
            },
            {
                "pipeline_name": "pipe",
                "node_name": "transform",
                "version_hash": "h2",
                "type": "transform",
                "config_json": "{}",
            },
        ]
        catalog_manager.register_nodes_batch(records)
        df = _read_table(catalog_manager, "meta_nodes")
        assert len(df) == 2
        assert set(df["node_name"]) == {"load", "transform"}

    def test_upsert_updates_existing(self, catalog_manager):
        records = [
            {
                "pipeline_name": "pipe",
                "node_name": "n",
                "version_hash": "v1",
                "type": "transform",
                "config_json": "{}",
            },
        ]
        catalog_manager.register_nodes_batch(records)
        records[0]["version_hash"] = "v2"
        catalog_manager.register_nodes_batch(records)
        df = _read_table(catalog_manager, "meta_nodes")
        assert len(df) == 1
        assert df.iloc[0]["version_hash"] == "v2"

    def test_clears_cache_after_registration(self, catalog_manager):
        catalog_manager._nodes_cache = {"stale": {}}
        records = [
            {
                "pipeline_name": "p",
                "node_name": "n",
                "version_hash": "h",
                "type": "transform",
                "config_json": "{}",
            },
        ]
        catalog_manager.register_nodes_batch(records)
        assert catalog_manager._nodes_cache is None

    def test_failure_logs_warning(self, catalog_manager):
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(catalog_manager.engine, "write", side_effect=IOError("fail")):
                catalog_manager.register_nodes_batch(
                    [
                        {
                            "pipeline_name": "p",
                            "node_name": "n",
                            "version_hash": "h",
                            "type": "transform",
                            "config_json": "{}",
                        }
                    ]
                )
        mock_logger.warning.assert_called_once()
        assert "Failed to batch register nodes" in mock_logger.warning.call_args[0][0]


# ===========================================================================
# register_outputs_batch()
# ===========================================================================


class TestRegisterOutputsBatch:
    """Tests for CatalogManager.register_outputs_batch() Pandas paths."""

    def test_empty_records_no_op(self, catalog_manager):
        catalog_manager.register_outputs_batch([])

    def test_no_engine_no_op(self, bare_catalog):
        bare_catalog.register_outputs_batch(
            [
                {
                    "pipeline_name": "p",
                    "node_name": "n",
                    "output_type": "managed_table",
                    "last_run": datetime.now(timezone.utc),
                }
            ]
        )

    def test_registers_multiple_outputs(self, catalog_manager):
        now = datetime.now(timezone.utc)
        records = [
            {
                "pipeline_name": "pipe",
                "node_name": "load",
                "output_type": "external_table",
                "connection_name": "adls",
                "path": "/data/raw",
                "format": "parquet",
                "table_name": "raw_orders",
                "last_run": now,
                "row_count": 1000,
            },
            {
                "pipeline_name": "pipe",
                "node_name": "transform",
                "output_type": "managed_table",
                "last_run": now,
            },
        ]
        catalog_manager.register_outputs_batch(records)
        df = _read_table(catalog_manager, "meta_outputs")
        assert len(df) == 2
        assert set(df["node_name"]) == {"load", "transform"}

    def test_optional_fields_default_none(self, catalog_manager):
        now = datetime.now(timezone.utc)
        records = [
            {
                "pipeline_name": "pipe",
                "node_name": "n",
                "output_type": "managed_table",
                "last_run": now,
            },
        ]
        catalog_manager.register_outputs_batch(records)
        df = _read_table(catalog_manager, "meta_outputs")
        assert df.iloc[0]["connection_name"] is None
        assert df.iloc[0]["path"] is None

    def test_clears_cache_after_registration(self, catalog_manager):
        catalog_manager._outputs_cache = {"stale": {}}
        now = datetime.now(timezone.utc)
        records = [
            {
                "pipeline_name": "p",
                "node_name": "n",
                "output_type": "managed_table",
                "last_run": now,
            },
        ]
        catalog_manager.register_outputs_batch(records)
        assert catalog_manager._outputs_cache is None

    def test_failure_logs_warning(self, catalog_manager):
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(catalog_manager.engine, "write", side_effect=IOError("fail")):
                catalog_manager.register_outputs_batch(
                    [
                        {
                            "pipeline_name": "p",
                            "node_name": "n",
                            "output_type": "managed_table",
                            "last_run": datetime.now(timezone.utc),
                        }
                    ]
                )
        mock_logger.warning.assert_called_once()
        assert "Failed to batch register outputs" in mock_logger.warning.call_args[0][0]


# ===========================================================================
# log_pattern()
# ===========================================================================


class TestLogPattern:
    """Tests for CatalogManager.log_pattern()."""

    def test_pandas_engine_appends_record(self, catalog_manager):
        catalog_manager.log_pattern("orders", "fact", '{"keys": ["id"]}', 0.95)
        df = _read_table(catalog_manager, "meta_patterns")
        assert len(df) == 1
        assert df.iloc[0]["table_name"] == "orders"
        assert df.iloc[0]["pattern_type"] == "fact"
        assert df.iloc[0]["compliance_score"] == 0.95

    def test_no_engine_no_op(self, bare_catalog):
        bare_catalog.log_pattern("t", "dim", "{}", 1.0)

    def test_retry_failure_logs_warning(self, catalog_manager):
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(catalog_manager.engine, "write", side_effect=IOError("fail")):
                catalog_manager.log_pattern("t", "dim", "{}", 1.0)
        mock_logger.warning.assert_called_once()
        assert "Failed to log pattern" in mock_logger.warning.call_args[0][0]

    def test_multiple_patterns_append(self, catalog_manager):
        catalog_manager.log_pattern("a", "fact", "{}", 0.9)
        catalog_manager.log_pattern("b", "dimension", "{}", 1.0)
        df = _read_table(catalog_manager, "meta_patterns")
        assert len(df) == 2


# ===========================================================================
# register_asset()
# ===========================================================================


class TestRegisterAsset:
    """Tests for CatalogManager.register_asset()."""

    def test_pandas_engine_registers(self, catalog_manager):
        catalog_manager.register_asset(
            "proj", "customers", "/data/customers", "parquet", "dimension"
        )
        df = _read_table(catalog_manager, "meta_tables")
        assert len(df) == 1
        assert df.iloc[0]["table_name"] == "customers"
        assert df.iloc[0]["project"] == "proj"

    def test_upsert_updates_existing(self, catalog_manager):
        catalog_manager.register_asset("proj", "t", "/old", "parquet", "dim")
        catalog_manager.register_asset("proj", "t", "/new", "parquet", "dim")
        df = _read_table(catalog_manager, "meta_tables")
        assert len(df) == 1
        assert df.iloc[0]["path"] == "/new"

    def test_no_engine_no_op(self, bare_catalog):
        bare_catalog.register_asset("proj", "t", "/path", "parquet", "dim")

    def test_retry_failure_logs_warning(self, catalog_manager):
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(catalog_manager.engine, "write", side_effect=IOError("fail")):
                catalog_manager.register_asset("proj", "t", "/path", "parquet", "dim")
        mock_logger.warning.assert_called_once()
        assert "Failed to register asset" in mock_logger.warning.call_args[0][0]

    def test_default_schema_hash(self, catalog_manager):
        catalog_manager.register_asset("proj", "t", "/path", "parquet", "dim")
        df = _read_table(catalog_manager, "meta_tables")
        assert df.iloc[0]["schema_hash"] == ""


# ===========================================================================
# resolve_table_path()
# ===========================================================================


class TestResolveTablePath:
    """Tests for CatalogManager.resolve_table_path()."""

    def test_resolves_existing_table(self, catalog_manager):
        _seed_tables(
            catalog_manager, [{"table_name": "customers", "path": "/data/silver/customers"}]
        )
        assert catalog_manager.resolve_table_path("customers") == "/data/silver/customers"

    def test_returns_none_for_missing_table(self, catalog_manager):
        _seed_tables(catalog_manager, [{"table_name": "orders", "path": "/data/orders"}])
        assert catalog_manager.resolve_table_path("nonexistent") is None

    def test_empty_table_returns_none(self, catalog_manager):
        assert catalog_manager.resolve_table_path("anything") is None

    def test_no_engine_returns_none(self, bare_catalog):
        assert bare_catalog.resolve_table_path("anything") is None


# ===========================================================================
# get_pipeline_hash()
# ===========================================================================


class TestGetPipelineHash:
    """Tests for CatalogManager.get_pipeline_hash()."""

    def test_returns_hash_for_existing_pipeline(self, catalog_manager):
        _seed_pipelines(catalog_manager, [{"pipeline_name": "etl", "version_hash": "abc123"}])
        assert catalog_manager.get_pipeline_hash("etl") == "abc123"

    def test_returns_none_for_missing_pipeline(self, catalog_manager):
        _seed_pipelines(catalog_manager, [{"pipeline_name": "etl", "version_hash": "abc"}])
        assert catalog_manager.get_pipeline_hash("nonexistent") is None

    def test_empty_table_returns_none(self, catalog_manager):
        assert catalog_manager.get_pipeline_hash("anything") is None

    def test_no_engine_returns_none(self, bare_catalog):
        assert bare_catalog.get_pipeline_hash("anything") is None

    def test_returns_latest_with_duplicates(self, catalog_manager):
        """If multiple entries exist, returns the most recent one."""
        _seed_pipelines(
            catalog_manager,
            [
                {"pipeline_name": "etl", "version_hash": "old_hash"},
                {"pipeline_name": "etl", "version_hash": "new_hash"},
            ],
        )
        result = catalog_manager.get_pipeline_hash("etl")
        assert result in ("old_hash", "new_hash")


# ===========================================================================
# get_average_volume()
# ===========================================================================


class TestGetAverageVolume:
    """Tests for CatalogManager.get_average_volume()."""

    def test_returns_average_for_recent_runs(self, catalog_manager):
        now = datetime.now(timezone.utc)
        _seed_runs(
            catalog_manager,
            [
                {
                    "node_name": "load",
                    "status": "SUCCESS",
                    "rows_processed": 100,
                    "timestamp": now - timedelta(days=1),
                },
                {
                    "node_name": "load",
                    "status": "SUCCESS",
                    "rows_processed": 200,
                    "timestamp": now - timedelta(days=2),
                },
            ],
        )
        result = catalog_manager.get_average_volume("load", days=7)
        assert result == 150.0

    def test_excludes_failed_runs(self, catalog_manager):
        now = datetime.now(timezone.utc)
        _seed_runs(
            catalog_manager,
            [
                {
                    "node_name": "load",
                    "status": "SUCCESS",
                    "rows_processed": 100,
                    "timestamp": now - timedelta(days=1),
                },
                {
                    "node_name": "load",
                    "status": "FAILED",
                    "rows_processed": 0,
                    "timestamp": now - timedelta(days=1),
                },
            ],
        )
        result = catalog_manager.get_average_volume("load", days=7)
        assert result == 100.0

    def test_excludes_old_runs(self, catalog_manager):
        now = datetime.now(timezone.utc)
        _seed_runs(
            catalog_manager,
            [
                {
                    "node_name": "load",
                    "status": "SUCCESS",
                    "rows_processed": 100,
                    "timestamp": now - timedelta(days=1),
                },
                {
                    "node_name": "load",
                    "status": "SUCCESS",
                    "rows_processed": 9999,
                    "timestamp": now - timedelta(days=30),
                },
            ],
        )
        result = catalog_manager.get_average_volume("load", days=7)
        assert result == 100.0

    def test_no_matching_runs_returns_none(self, catalog_manager):
        now = datetime.now(timezone.utc)
        _seed_runs(catalog_manager, [{"node_name": "other", "status": "SUCCESS", "timestamp": now}])
        assert catalog_manager.get_average_volume("nonexistent", days=7) is None

    def test_empty_table_returns_none(self, catalog_manager):
        assert catalog_manager.get_average_volume("load") is None

    def test_no_engine_returns_none(self, bare_catalog):
        assert bare_catalog.get_average_volume("load") is None


# ===========================================================================
# get_average_duration()
# ===========================================================================


class TestGetAverageDuration:
    """Tests for CatalogManager.get_average_duration()."""

    def test_returns_average_in_seconds(self, catalog_manager):
        now = datetime.now(timezone.utc)
        _seed_runs(
            catalog_manager,
            [
                {
                    "node_name": "load",
                    "status": "SUCCESS",
                    "duration_ms": 2000,
                    "timestamp": now - timedelta(days=1),
                },
                {
                    "node_name": "load",
                    "status": "SUCCESS",
                    "duration_ms": 4000,
                    "timestamp": now - timedelta(days=2),
                },
            ],
        )
        result = catalog_manager.get_average_duration("load", days=7)
        assert result == 3.0  # (2000+4000)/2 / 1000

    def test_excludes_failed_runs(self, catalog_manager):
        now = datetime.now(timezone.utc)
        _seed_runs(
            catalog_manager,
            [
                {
                    "node_name": "load",
                    "status": "SUCCESS",
                    "duration_ms": 2000,
                    "timestamp": now - timedelta(days=1),
                },
                {
                    "node_name": "load",
                    "status": "FAILED",
                    "duration_ms": 99000,
                    "timestamp": now - timedelta(days=1),
                },
            ],
        )
        result = catalog_manager.get_average_duration("load", days=7)
        assert result == 2.0

    def test_no_matching_runs_returns_none(self, catalog_manager):
        assert catalog_manager.get_average_duration("nonexistent") is None

    def test_no_engine_returns_none(self, bare_catalog):
        assert bare_catalog.get_average_duration("load") is None


# ===========================================================================
# log_metrics()
# ===========================================================================


class TestLogMetrics:
    """Tests for CatalogManager.log_metrics()."""

    def test_pandas_engine_appends_record(self, catalog_manager):
        catalog_manager.log_metrics("revenue", "SUM(amount)", ["region", "product"], "gold.orders")
        df = _read_table(catalog_manager, "meta_metrics")
        assert len(df) == 1
        assert df.iloc[0]["metric_name"] == "revenue"
        assert df.iloc[0]["source_table"] == "gold.orders"
        assert json.loads(df.iloc[0]["dimensions"]) == ["region", "product"]

    def test_no_engine_no_op(self, bare_catalog):
        bare_catalog.log_metrics("m", "SQL", ["d"], "t")

    def test_retry_failure_logs_warning(self, catalog_manager):
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(catalog_manager.engine, "write", side_effect=IOError("fail")):
                catalog_manager.log_metrics("m", "SQL", ["d"], "t")
        mock_logger.warning.assert_called_once()
        assert "Failed to log metric" in mock_logger.warning.call_args[0][0]

    def test_multiple_metrics_append(self, catalog_manager):
        catalog_manager.log_metrics("m1", "SQL1", ["d1"], "t1")
        catalog_manager.log_metrics("m2", "SQL2", ["d2"], "t2")
        df = _read_table(catalog_manager, "meta_metrics")
        assert len(df) == 2

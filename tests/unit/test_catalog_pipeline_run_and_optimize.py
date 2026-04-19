"""Tests for CatalogManager pipeline run retrieval, optimize, bootstrap guard, and record helpers.

Covers: get_pipeline_run / _get_pipeline_run_pandas, optimize / _optimize_deltars,
        _bootstrapped guard, _prepare_pipeline_record, _prepare_node_record.

NOTE: Test names avoid 'spark' and 'delta' to prevent conftest.py from skipping on Windows.
"""

import json
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

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
    """CatalogManager with no engine and no spark (no backend)."""
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=None)
    return cm


def _read_table(cm, table_key):
    """Read a catalog table and return a pandas DataFrame."""
    dt = DeltaTable(cm.tables[table_key])
    return dt.to_pandas()


def _seed_pipeline_runs(cm, records):
    """Seed meta_pipeline_runs with pipeline run records via write_deltalake."""
    now = datetime.now(timezone.utc)
    schema = pa.schema(
        [
            ("run_id", pa.string()),
            ("project", pa.string()),
            ("pipeline_name", pa.string()),
            ("owner", pa.string()),
            ("layer", pa.string()),
            ("run_start_at", pa.timestamp("us", tz="UTC")),
            ("run_end_at", pa.timestamp("us", tz="UTC")),
            ("duration_ms", pa.int64()),
            ("status", pa.string()),
            ("nodes_total", pa.int64()),
            ("nodes_succeeded", pa.int64()),
            ("nodes_failed", pa.int64()),
            ("nodes_skipped", pa.int64()),
            ("rows_processed", pa.int64()),
            ("error_summary", pa.string()),
            ("terminal_nodes", pa.string()),
            ("environment", pa.string()),
            ("databricks_cluster_id", pa.string()),
            ("databricks_job_id", pa.string()),
            ("databricks_workspace_id", pa.string()),
            ("estimated_cost_usd", pa.float64()),
            ("actual_cost_usd", pa.float64()),
            ("cost_source", pa.string()),
            ("created_at", pa.timestamp("us", tz="UTC")),
        ]
    )
    df = pd.DataFrame(
        {
            "run_id": [r["run_id"] for r in records],
            "project": [r.get("project", "proj") for r in records],
            "pipeline_name": [r.get("pipeline_name", "pipe") for r in records],
            "owner": [r.get("owner", "owner") for r in records],
            "layer": [r.get("layer", "silver") for r in records],
            "run_start_at": [r.get("run_start_at", now) for r in records],
            "run_end_at": [r.get("run_end_at", now) for r in records],
            "duration_ms": [r.get("duration_ms", 1000) for r in records],
            "status": [r.get("status", "SUCCESS") for r in records],
            "nodes_total": [r.get("nodes_total", 3) for r in records],
            "nodes_succeeded": [r.get("nodes_succeeded", 3) for r in records],
            "nodes_failed": [r.get("nodes_failed", 0) for r in records],
            "nodes_skipped": [r.get("nodes_skipped", 0) for r in records],
            "rows_processed": [r.get("rows_processed", 100) for r in records],
            "error_summary": [r.get("error_summary") for r in records],
            "terminal_nodes": [r.get("terminal_nodes") for r in records],
            "environment": [r.get("environment") for r in records],
            "databricks_cluster_id": [None for _ in records],
            "databricks_job_id": [None for _ in records],
            "databricks_workspace_id": [None for _ in records],
            "estimated_cost_usd": [None for _ in records],
            "actual_cost_usd": [None for _ in records],
            "cost_source": [None for _ in records],
            "created_at": [r.get("created_at", now) for r in records],
        }
    )
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    write_deltalake(cm.tables["meta_pipeline_runs"], table, mode="overwrite", engine="rust")


# ===========================================================================
# get_pipeline_run() / _get_pipeline_run_pandas()
# ===========================================================================


class TestGetPipelineRun:
    """Tests for CatalogManager.get_pipeline_run() in Pandas mode."""

    def test_returns_record_when_found(self, catalog_manager):
        """get_pipeline_run returns a dict for an existing run_id."""
        _seed_pipeline_runs(catalog_manager, [{"run_id": "run-001", "status": "SUCCESS"}])
        result = catalog_manager.get_pipeline_run("run-001")
        assert result is not None
        assert result["run_id"] == "run-001"
        assert result["status"] == "SUCCESS"

    def test_returns_none_when_not_found(self, catalog_manager):
        """get_pipeline_run returns None for a non-existent run_id."""
        _seed_pipeline_runs(catalog_manager, [{"run_id": "run-001"}])
        result = catalog_manager.get_pipeline_run("nonexistent")
        assert result is None

    def test_returns_none_on_empty_table(self, catalog_manager):
        """get_pipeline_run returns None when the table has no rows."""
        result = catalog_manager.get_pipeline_run("any-id")
        assert result is None

    def test_returns_correct_fields(self, catalog_manager):
        """Returned dict contains expected pipeline run fields."""
        _seed_pipeline_runs(
            catalog_manager,
            [
                {
                    "run_id": "run-002",
                    "pipeline_name": "etl_orders",
                    "status": "FAILURE",
                    "duration_ms": 5000,
                    "nodes_total": 5,
                    "nodes_failed": 1,
                }
            ],
        )
        result = catalog_manager.get_pipeline_run("run-002")
        assert result["pipeline_name"] == "etl_orders"
        assert result["status"] == "FAILURE"
        assert result["duration_ms"] == 5000
        assert result["nodes_total"] == 5
        assert result["nodes_failed"] == 1

    def test_selects_correct_record_among_multiple(self, catalog_manager):
        """get_pipeline_run picks the right row when multiple runs exist."""
        _seed_pipeline_runs(
            catalog_manager,
            [
                {"run_id": "run-A", "pipeline_name": "pipe_a"},
                {"run_id": "run-B", "pipeline_name": "pipe_b"},
                {"run_id": "run-C", "pipeline_name": "pipe_c"},
            ],
        )
        result = catalog_manager.get_pipeline_run("run-B")
        assert result is not None
        assert result["pipeline_name"] == "pipe_b"

    def test_returns_none_with_no_backend(self, bare_catalog):
        """get_pipeline_run returns None when no engine/spark is set."""
        result = bare_catalog.get_pipeline_run("any-id")
        assert result is None


# ===========================================================================
# optimize() / _optimize_deltars()
# ===========================================================================


class TestOptimize:
    """Tests for CatalogManager.optimize() via the Pandas/delta-rs path."""

    def test_optimize_all_tables_succeeds(self, catalog_manager):
        """optimize() on all bootstrapped tables returns success for each."""
        results = catalog_manager.optimize()
        assert isinstance(results, dict)
        assert len(results) == len(catalog_manager.tables)
        for table_name, res in results.items():
            assert res["success"] is True, f"Table {table_name} failed: {res}"

    def test_optimize_specific_table_list(self, catalog_manager):
        """optimize() with a specific table list only processes those tables."""
        results = catalog_manager.optimize(tables=["meta_tables", "meta_runs"])
        assert set(results.keys()) == {"meta_tables", "meta_runs"}
        assert results["meta_tables"]["success"] is True
        assert results["meta_runs"]["success"] is True

    def test_optimize_unknown_table_returns_error(self, catalog_manager):
        """optimize() with an unknown table name returns success=False."""
        results = catalog_manager.optimize(tables=["nonexistent_table"])
        assert "nonexistent_table" in results
        assert results["nonexistent_table"]["success"] is False

    def test_optimize_no_engine_reports_error(self, bare_catalog):
        """optimize() with no engine returns 'No engine available' for each table."""
        results = bare_catalog.optimize(tables=["meta_tables"])
        assert results["meta_tables"]["success"] is False
        assert "No engine available" in results["meta_tables"]["error"]

    def test_optimize_returns_engine_key(self, catalog_manager):
        """Successful optimize results include engine='delta-rs'."""
        results = catalog_manager.optimize(tables=["meta_tables"])
        assert results["meta_tables"]["engine"] == "delta-rs"

    def test_optimize_custom_retention_hours(self, catalog_manager):
        """optimize() accepts a custom vacuum_retention_hours without error."""
        results = catalog_manager.optimize(tables=["meta_pipelines"], vacuum_retention_hours=336)
        assert results["meta_pipelines"]["success"] is True


# ===========================================================================
# _bootstrapped guard
# ===========================================================================


class TestBootstrappedGuard:
    """Tests for the _bootstrapped guard preventing redundant bootstrap calls."""

    def test_flag_set_after_first_bootstrap(self, tmp_path):
        """_bootstrapped should be True after bootstrap() succeeds."""
        config = SystemConfig(connection="local", path="_odibi_system")
        base_path = str(tmp_path / "_odibi_system")
        engine = PandasEngine(config={})
        cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
        assert cm._bootstrapped is False
        cm.bootstrap()
        assert cm._bootstrapped is True

    def test_second_bootstrap_is_noop(self, tmp_path, caplog):
        """Second bootstrap() call skips table checks (logged as debug)."""
        config = SystemConfig(connection="local", path="_odibi_system")
        base_path = str(tmp_path / "_odibi_system")
        engine = PandasEngine(config={})
        cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
        cm.bootstrap()

        with caplog.at_level(logging.DEBUG, logger="odibi.catalog"):
            cm.bootstrap()

        assert any("Bootstrap skipped" in msg for msg in caplog.messages)

    def test_ensure_table_not_called_on_second_bootstrap(self, tmp_path):
        """_ensure_table should not be called during a second bootstrap() call."""
        config = SystemConfig(connection="local", path="_odibi_system")
        base_path = str(tmp_path / "_odibi_system")
        engine = PandasEngine(config={})
        cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
        cm.bootstrap()

        with patch.object(cm, "_ensure_table") as mock_ensure:
            cm.bootstrap()
            mock_ensure.assert_not_called()


# ===========================================================================
# _prepare_pipeline_record()
# ===========================================================================


class TestPreparePipelineRecord:
    """Tests for CatalogManager._prepare_pipeline_record()."""

    def _make_pipeline_config(
        self, name="etl_orders", description="Orders ETL", layer="silver", tags=None
    ):
        """Create a mock pipeline config with the necessary attributes."""
        node1 = SimpleNamespace(name="read_orders", tags=tags or ["team:data", "priority:high"])
        node2 = SimpleNamespace(name="transform_orders", tags=["team:data"])
        return SimpleNamespace(
            pipeline=name,
            description=description,
            layer=layer,
            nodes=[node1, node2],
        )

    @patch("odibi.utils.hashing.calculate_pipeline_hash", return_value="abc123hash")
    def test_returns_correct_keys(self, mock_hash, catalog_manager):
        """Returned dict should have the expected keys."""
        config = self._make_pipeline_config()
        record = catalog_manager._prepare_pipeline_record(config)
        expected_keys = {
            "pipeline_name",
            "version_hash",
            "description",
            "layer",
            "schedule",
            "tags_json",
        }
        assert set(record.keys()) == expected_keys

    @patch("odibi.utils.hashing.calculate_pipeline_hash", return_value="abc123hash")
    def test_pipeline_name_from_config(self, mock_hash, catalog_manager):
        """pipeline_name should come from the config object."""
        config = self._make_pipeline_config(name="my_pipeline")
        record = catalog_manager._prepare_pipeline_record(config)
        assert record["pipeline_name"] == "my_pipeline"

    @patch("odibi.utils.hashing.calculate_pipeline_hash", return_value="hash_value_42")
    def test_version_hash_from_hashing_util(self, mock_hash, catalog_manager):
        """version_hash should be set by calculate_pipeline_hash."""
        config = self._make_pipeline_config()
        record = catalog_manager._prepare_pipeline_record(config)
        assert record["version_hash"] == "hash_value_42"
        mock_hash.assert_called_once_with(config)

    @patch("odibi.utils.hashing.calculate_pipeline_hash", return_value="h")
    def test_tags_aggregated_from_all_nodes(self, mock_hash, catalog_manager):
        """tags_json should contain the union of tags from all nodes."""
        config = self._make_pipeline_config(tags=["team:data", "priority:high"])
        record = catalog_manager._prepare_pipeline_record(config)
        tags = set(json.loads(record["tags_json"]))
        assert "team:data" in tags
        assert "priority:high" in tags

    @patch("odibi.utils.hashing.calculate_pipeline_hash", return_value="h")
    def test_empty_description_and_layer(self, mock_hash, catalog_manager):
        """description and layer default to empty strings when None."""
        config = self._make_pipeline_config(description=None, layer=None)
        record = catalog_manager._prepare_pipeline_record(config)
        assert record["description"] == ""
        assert record["layer"] == ""


# ===========================================================================
# _prepare_node_record()
# ===========================================================================


class TestPrepareNodeRecord:
    """Tests for CatalogManager._prepare_node_record()."""

    def _make_node_config(self, name="load_data", read=True, write=False, tags=None):
        """Create a mock node config with required attributes."""
        node = MagicMock()
        node.name = name
        node.read = read
        node.write = write
        node.tags = tags or []
        node.description = "test node"
        node.log_level = "INFO"
        node.model_dump.return_value = {
            "name": name,
            "read": read,
            "write": write,
        }
        return node

    @patch("odibi.utils.hashing.calculate_node_hash", return_value="node_hash_xyz")
    def test_returns_correct_keys(self, mock_hash, catalog_manager):
        """Returned dict should have the expected keys."""
        node = self._make_node_config()
        record = catalog_manager._prepare_node_record("my_pipeline", node)
        expected_keys = {"pipeline_name", "node_name", "version_hash", "type", "config_json"}
        assert set(record.keys()) == expected_keys

    @patch("odibi.utils.hashing.calculate_node_hash", return_value="node_hash_xyz")
    def test_node_type_read(self, mock_hash, catalog_manager):
        """Node with read=True should have type='read'."""
        node = self._make_node_config(read=True, write=False)
        record = catalog_manager._prepare_node_record("pipe", node)
        assert record["type"] == "read"

    @patch("odibi.utils.hashing.calculate_node_hash", return_value="node_hash_xyz")
    def test_node_type_write(self, mock_hash, catalog_manager):
        """Node with write=True should have type='write' (write overrides read)."""
        node = self._make_node_config(read=True, write=True)
        record = catalog_manager._prepare_node_record("pipe", node)
        assert record["type"] == "write"

    @patch("odibi.utils.hashing.calculate_node_hash", return_value="node_hash_xyz")
    def test_node_type_transform(self, mock_hash, catalog_manager):
        """Node with read=False and write=False should have type='transform'."""
        node = self._make_node_config(read=False, write=False)
        record = catalog_manager._prepare_node_record("pipe", node)
        assert record["type"] == "transform"

    @patch("odibi.utils.hashing.calculate_node_hash", return_value="nhash")
    def test_version_hash_from_hashing_util(self, mock_hash, catalog_manager):
        """version_hash should be set by calculate_node_hash."""
        node = self._make_node_config()
        record = catalog_manager._prepare_node_record("pipe", node)
        assert record["version_hash"] == "nhash"
        mock_hash.assert_called_once_with(node)

    @patch("odibi.utils.hashing.calculate_node_hash", return_value="h")
    def test_config_json_excludes_description_and_tags(self, mock_hash, catalog_manager):
        """config_json should call model_dump excluding description, tags, log_level."""
        node = self._make_node_config()
        catalog_manager._prepare_node_record("pipe", node)
        node.model_dump.assert_called_once_with(
            mode="json", exclude={"description", "tags", "log_level"}
        )

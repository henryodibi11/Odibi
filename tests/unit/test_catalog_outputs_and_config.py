"""Unit tests for CatalogManager output registration and config extraction.

Covers: get_node_output(), register_outputs_from_config(),
        _extract_node_output_info(), _get_all_outputs_cached().

NOTE: Test names avoid 'spark' and 'delta' to prevent conftest.py from skipping on Windows.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock

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
    """CatalogManager with no engine and no spark."""
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=None)
    return cm


def _seed_outputs(cm, records):
    """Seed meta_outputs with output records via write_deltalake."""
    schema = pa.schema(
        [
            ("pipeline_name", pa.string()),
            ("node_name", pa.string()),
            ("output_type", pa.string()),
            ("connection_name", pa.string()),
            ("path", pa.string()),
            ("format", pa.string()),
            ("table_name", pa.string()),
            ("last_run", pa.timestamp("us", tz="UTC")),
            ("row_count", pa.int64()),
            ("updated_at", pa.timestamp("us", tz="UTC")),
        ]
    )
    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "pipeline_name": [r["pipeline_name"] for r in records],
            "node_name": [r["node_name"] for r in records],
            "output_type": [r.get("output_type", "external_table") for r in records],
            "connection_name": [r.get("connection_name") for r in records],
            "path": [r.get("path") for r in records],
            "format": [r.get("format", "delta") for r in records],
            "table_name": [r.get("table_name") for r in records],
            "last_run": [r.get("last_run", now) for r in records],
            "row_count": [r.get("row_count", 0) for r in records],
            "updated_at": [now for _ in records],
        }
    )
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    write_deltalake(cm.tables["meta_outputs"], table, mode="overwrite", engine="rust")


def _make_write_config(
    connection="my_conn", path="/out/table", fmt="delta", register_table=None, table=None
):
    """Build a MagicMock that mimics WriteConfig."""
    w = MagicMock()
    w.connection = connection
    w.path = path
    w.format = fmt
    w.register_table = register_table
    w.table = table
    return w


def _make_transform_step(function, params=None):
    """Build a MagicMock that mimics TransformStep with function and params."""
    step = MagicMock()
    step.function = function
    step.params = params or {}
    return step


def _make_node_config(name, write=None, transform=None, transformer=None, params=None):
    """Build a MagicMock that mimics NodeConfig."""
    node = MagicMock()
    node.name = name
    node.write = write
    node.transform = transform
    node.transformer = transformer
    node.params = params
    return node


# ===========================================================================
# _extract_node_output_info()
# ===========================================================================


class TestExtractNodeOutputInfo:
    """Tests for CatalogManager._extract_node_output_info()."""

    def test_explicit_write_block_external(self, catalog_manager):
        """Node with write block and path is reported as external_table."""
        write = _make_write_config(
            connection="lake",
            path="/data/sales",
            fmt="parquet",
            register_table=None,
            table=None,
        )
        node = _make_node_config("sales_node", write=write)

        result = catalog_manager._extract_node_output_info(node)

        assert result is not None
        assert result["connection"] == "lake"
        assert result["path"] == "/data/sales"
        assert result["format"] == "parquet"
        assert result["output_type"] == "external_table"

    def test_explicit_write_block_managed(self, catalog_manager):
        """Node with write.table and no path is reported as managed_table."""
        write = _make_write_config(
            connection="warehouse",
            path=None,
            fmt="delta",
            register_table=None,
            table="dim_customer",
        )
        node = _make_node_config("dim_node", write=write)

        result = catalog_manager._extract_node_output_info(node)

        assert result is not None
        assert result["output_type"] == "managed_table"
        assert result["register_table"] == "dim_customer"

    def test_write_block_register_table_takes_precedence(self, catalog_manager):
        """register_table is preferred over table for the registered name."""
        write = _make_write_config(
            connection="conn",
            path="/p",
            fmt="delta",
            register_table="registered_name",
            table="raw_table",
        )
        node = _make_node_config("n", write=write)

        result = catalog_manager._extract_node_output_info(node)

        assert result["register_table"] == "registered_name"

    def test_write_block_format_enum(self, catalog_manager):
        """write.format with a .value attribute (enum) is serialised correctly."""
        fmt_enum = MagicMock()
        fmt_enum.value = "parquet"
        write = _make_write_config(connection="c", path="/p")
        write.format = fmt_enum
        node = _make_node_config("n", write=write)

        result = catalog_manager._extract_node_output_info(node)

        assert result["format"] == "parquet"

    def test_write_block_format_none_defaults_to_table_format(self, catalog_manager):
        """write.format=None defaults to 'delta'."""
        write = _make_write_config(connection="c", path="/p")
        write.format = None
        node = _make_node_config("n", write=write)

        result = catalog_manager._extract_node_output_info(node)

        assert result["format"] == "delta"

    def test_merge_in_transform_steps(self, catalog_manager):
        """merge step inside transform.steps is detected as output."""
        step = _make_transform_step(
            "merge", {"connection": "lake", "path": "/target/merged", "register_table": "tbl"}
        )
        transform = MagicMock()
        transform.steps = ["SELECT 1", step]
        node = _make_node_config("merge_node", write=None, transform=transform)

        result = catalog_manager._extract_node_output_info(node)

        assert result is not None
        assert result["connection"] == "lake"
        assert result["path"] == "/target/merged"
        assert result["output_type"] == "managed_table"

    def test_scd2_in_transform_steps(self, catalog_manager):
        """scd2 step inside transform.steps is detected as output."""
        step = _make_transform_step(
            "scd2", {"connection": "wh", "target": "/scd2/dim", "register_table": None}
        )
        transform = MagicMock()
        transform.steps = [step]
        node = _make_node_config("scd2_node", write=None, transform=transform)

        result = catalog_manager._extract_node_output_info(node)

        assert result is not None
        assert result["path"] == "/scd2/dim"
        assert result["output_type"] == "external_table"

    def test_top_level_merge_transformer(self, catalog_manager):
        """Top-level merge transformer with params is detected as output."""
        node = _make_node_config(
            "top_merge",
            write=None,
            transform=None,
            transformer="merge",
            params={"connection": "conn", "path": "/out", "register_table": "merged_tbl"},
        )

        result = catalog_manager._extract_node_output_info(node)

        assert result is not None
        assert result["connection"] == "conn"
        assert result["register_table"] == "merged_tbl"
        assert result["output_type"] == "managed_table"

    def test_top_level_scd2_transformer_with_target(self, catalog_manager):
        """Top-level scd2 transformer using 'target' key instead of 'path'."""
        node = _make_node_config(
            "top_scd2",
            write=None,
            transform=None,
            transformer="scd2",
            params={"connection": "wh", "target": "/scd2/out"},
        )

        result = catalog_manager._extract_node_output_info(node)

        assert result is not None
        assert result["path"] == "/scd2/out"

    def test_no_output_returns_none(self, catalog_manager):
        """Node with no write, no merge/scd2 returns None."""
        transform = MagicMock()
        transform.steps = ["SELECT * FROM input"]
        node = _make_node_config("passthrough", write=None, transform=transform)

        result = catalog_manager._extract_node_output_info(node)

        assert result is None

    def test_no_transform_no_transformer_returns_none(self, catalog_manager):
        """Node with write=None, transform=None, transformer=None returns None."""
        node = _make_node_config("empty", write=None, transform=None, transformer=None)

        result = catalog_manager._extract_node_output_info(node)

        assert result is None

    def test_merge_step_without_connection_returns_none(self, catalog_manager):
        """merge step missing connection is skipped."""
        step = _make_transform_step("merge", {"path": "/p"})
        transform = MagicMock()
        transform.steps = [step]
        node = _make_node_config("bad_merge", write=None, transform=transform)

        result = catalog_manager._extract_node_output_info(node)

        assert result is None


# ===========================================================================
# register_outputs_from_config()
# ===========================================================================


class TestRegisterOutputsFromConfig:
    """Tests for CatalogManager.register_outputs_from_config()."""

    def test_registers_nodes_with_write_blocks(self, catalog_manager):
        """Nodes with write blocks are registered to meta_outputs."""
        write1 = _make_write_config(connection="lake", path="/data/t1", fmt="delta")
        write2 = _make_write_config(connection="lake", path="/data/t2", fmt="parquet")

        node1 = _make_node_config("node_a", write=write1)
        node2 = _make_node_config("node_b", write=write2)
        node3 = _make_node_config("node_c", write=None, transform=None, transformer=None)

        pipeline_config = MagicMock()
        pipeline_config.pipeline = "ingest_pipeline"
        pipeline_config.nodes = [node1, node2, node3]

        count = catalog_manager.register_outputs_from_config(pipeline_config)

        assert count == 2
        # Verify records written to meta_outputs
        df = DeltaTable(catalog_manager.tables["meta_outputs"]).to_pandas()
        assert len(df) == 2
        assert set(df["node_name"]) == {"node_a", "node_b"}

    def test_returns_zero_when_no_outputs(self, catalog_manager):
        """Returns 0 when no node has output info."""
        node = _make_node_config("passthrough", write=None, transform=None, transformer=None)

        pipeline_config = MagicMock()
        pipeline_config.pipeline = "empty_pipeline"
        pipeline_config.nodes = [node]

        count = catalog_manager.register_outputs_from_config(pipeline_config)

        assert count == 0

    def test_invalidates_outputs_cache(self, catalog_manager):
        """Cache is cleared after registration so next lookup sees new data."""
        catalog_manager._outputs_cache = {"stale": {}}

        write = _make_write_config(connection="c", path="/p", fmt="delta")
        node = _make_node_config("n", write=write)
        pipeline_config = MagicMock()
        pipeline_config.pipeline = "pipe"
        pipeline_config.nodes = [node]

        catalog_manager.register_outputs_from_config(pipeline_config)

        assert catalog_manager._outputs_cache is None


# ===========================================================================
# get_node_output()
# ===========================================================================


class TestGetNodeOutput:
    """Tests for CatalogManager.get_node_output()."""

    def test_found_returns_dict(self, catalog_manager):
        """get_node_output returns metadata dict for a known node."""
        _seed_outputs(
            catalog_manager,
            [
                {
                    "pipeline_name": "bronze_ingest",
                    "node_name": "load_orders",
                    "output_type": "external_table",
                    "connection_name": "lake",
                    "path": "/bronze/orders",
                    "format": "delta",
                    "table_name": "orders",
                    "row_count": 5000,
                }
            ],
        )
        catalog_manager._outputs_cache = None  # force reload

        result = catalog_manager.get_node_output("bronze_ingest", "load_orders")

        assert result is not None
        assert result["pipeline_name"] == "bronze_ingest"
        assert result["node_name"] == "load_orders"
        assert result["path"] == "/bronze/orders"

    def test_not_found_returns_none(self, catalog_manager):
        """get_node_output returns None for an unknown pipeline.node."""
        result = catalog_manager.get_node_output("nonexistent", "nowhere")

        assert result is None

    def test_multiple_outputs_correct_lookup(self, catalog_manager):
        """Correct output is returned when multiple nodes are registered."""
        _seed_outputs(
            catalog_manager,
            [
                {
                    "pipeline_name": "pipe_a",
                    "node_name": "node_1",
                    "path": "/a/1",
                },
                {
                    "pipeline_name": "pipe_a",
                    "node_name": "node_2",
                    "path": "/a/2",
                },
                {
                    "pipeline_name": "pipe_b",
                    "node_name": "node_1",
                    "path": "/b/1",
                },
            ],
        )
        catalog_manager._outputs_cache = None

        result = catalog_manager.get_node_output("pipe_a", "node_2")
        assert result["path"] == "/a/2"

        result_b = catalog_manager.get_node_output("pipe_b", "node_1")
        assert result_b["path"] == "/b/1"


# ===========================================================================
# _get_all_outputs_cached()
# ===========================================================================


class TestGetAllOutputsCached:
    """Tests for CatalogManager._get_all_outputs_cached()."""

    def test_returns_empty_dict_without_engine(self, bare_catalog):
        """Returns empty dict when no engine and no spark are available."""
        result = bare_catalog._get_all_outputs_cached()

        assert result == {}

    def test_returns_populated_cache_from_seeded_data(self, catalog_manager):
        """Cache is populated from existing meta_outputs rows."""
        _seed_outputs(
            catalog_manager,
            [
                {
                    "pipeline_name": "ingest",
                    "node_name": "raw_load",
                    "output_type": "external_table",
                    "path": "/raw/data",
                    "format": "delta",
                },
            ],
        )
        catalog_manager._outputs_cache = None

        result = catalog_manager._get_all_outputs_cached()

        assert "ingest.raw_load" in result
        assert result["ingest.raw_load"]["path"] == "/raw/data"

    def test_cache_hit_returns_same_object(self, catalog_manager):
        """Second call returns the same cached dict object."""
        _seed_outputs(
            catalog_manager,
            [
                {"pipeline_name": "p", "node_name": "n", "path": "/x"},
            ],
        )
        catalog_manager._outputs_cache = None

        first = catalog_manager._get_all_outputs_cached()
        second = catalog_manager._get_all_outputs_cached()

        assert first is second

    def test_cache_invalidation_on_write(self, catalog_manager):
        """Writing new outputs invalidates the cache."""
        _seed_outputs(
            catalog_manager,
            [
                {"pipeline_name": "p", "node_name": "n1", "path": "/old"},
            ],
        )
        catalog_manager._outputs_cache = None

        # Populate cache
        first = catalog_manager._get_all_outputs_cached()
        assert "p.n1" in first

        # Write a new output via the batch API
        catalog_manager.register_outputs_batch(
            [
                {
                    "pipeline_name": "p",
                    "node_name": "n2",
                    "output_type": "external_table",
                    "connection_name": None,
                    "path": "/new",
                    "format": "delta",
                    "table_name": "",
                    "last_run": datetime.now(timezone.utc),
                    "row_count": 0,
                },
            ]
        )

        # Cache should have been invalidated
        assert catalog_manager._outputs_cache is None

        # Re-read should see both outputs
        refreshed = catalog_manager._get_all_outputs_cached()
        assert "p.n1" in refreshed
        assert "p.n2" in refreshed

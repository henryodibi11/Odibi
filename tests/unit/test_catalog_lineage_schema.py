"""Tests for CatalogManager lineage tracking and schema history.

Covers: _hash_schema, _get_latest_schema, track_schema, get_schema_history,
        record_lineage, record_lineage_batch, register_assets_batch,
        get_upstream, get_downstream  (issue #293).

NOTE: Test names avoid 'spark' and 'delta' to prevent conftest.py from skipping on Windows.
"""

import json
from datetime import datetime, timezone
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
    """CatalogManager with no engine and no spark (all operations should no-op)."""
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=None)
    return cm


def _read_table(cm, table_key):
    """Read a catalog table and return a pandas DataFrame."""
    from deltalake import DeltaTable

    dt = DeltaTable(cm.tables[table_key])
    return dt.to_pandas()


def _seed_schemas(cm, records):
    """Seed meta_schemas with schema version records."""
    now = datetime.now(timezone.utc)
    schema = pa.schema(
        [
            ("table_path", pa.string()),
            ("schema_version", pa.int64()),
            ("schema_hash", pa.string()),
            ("columns", pa.string()),
            ("captured_at", pa.timestamp("us", tz="UTC")),
            ("pipeline", pa.string()),
            ("node", pa.string()),
            ("run_id", pa.string()),
            ("columns_added", pa.string()),
            ("columns_removed", pa.string()),
            ("columns_type_changed", pa.string()),
        ]
    )
    df = pd.DataFrame(
        {
            "table_path": [r["table_path"] for r in records],
            "schema_version": [r["schema_version"] for r in records],
            "schema_hash": [r["schema_hash"] for r in records],
            "columns": [r["columns"] for r in records],
            "captured_at": [r.get("captured_at", now) for r in records],
            "pipeline": [r.get("pipeline", "pipe") for r in records],
            "node": [r.get("node", "node") for r in records],
            "run_id": [r.get("run_id", "run-001") for r in records],
            "columns_added": [r.get("columns_added") for r in records],
            "columns_removed": [r.get("columns_removed") for r in records],
            "columns_type_changed": [r.get("columns_type_changed") for r in records],
        }
    )
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    write_deltalake(cm.tables["meta_schemas"], table, mode="overwrite",
)


def _seed_lineage(cm, records):
    """Seed meta_lineage with lineage records."""
    now = datetime.now(timezone.utc)
    schema = pa.schema(
        [
            ("source_table", pa.string()),
            ("target_table", pa.string()),
            ("source_pipeline", pa.string()),
            ("source_node", pa.string()),
            ("target_pipeline", pa.string()),
            ("target_node", pa.string()),
            ("relationship", pa.string()),
            ("last_observed", pa.timestamp("us", tz="UTC")),
            ("run_id", pa.string()),
        ]
    )
    df = pd.DataFrame(
        {
            "source_table": [r["source_table"] for r in records],
            "target_table": [r["target_table"] for r in records],
            "source_pipeline": [r.get("source_pipeline") for r in records],
            "source_node": [r.get("source_node") for r in records],
            "target_pipeline": [r.get("target_pipeline", "pipe") for r in records],
            "target_node": [r.get("target_node", "node") for r in records],
            "relationship": [r.get("relationship", "feeds") for r in records],
            "last_observed": [r.get("last_observed", now) for r in records],
            "run_id": [r.get("run_id", "run-001") for r in records],
        }
    )
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    write_deltalake(cm.tables["meta_lineage"], table, mode="overwrite",
)


# ===========================================================================
# _hash_schema()
# ===========================================================================


class TestHashSchema:
    """Tests for CatalogManager._hash_schema()."""

    def test_deterministic_hash(self, catalog_manager):
        """Same schema always produces the same hash."""
        schema = {"id": "int", "name": "string"}
        h1 = catalog_manager._hash_schema(schema)
        h2 = catalog_manager._hash_schema(schema)
        assert h1 == h2
        assert len(h1) == 32  # MD5 hex digest

    def test_different_schemas_different_hash(self, catalog_manager):
        """Different schemas produce different hashes."""
        h1 = catalog_manager._hash_schema({"id": "int", "name": "string"})
        h2 = catalog_manager._hash_schema({"id": "int", "name": "string", "email": "string"})
        assert h1 != h2

    def test_key_order_independent(self, catalog_manager):
        """Key order doesn't matter (uses sorted keys)."""
        h1 = catalog_manager._hash_schema({"name": "string", "id": "int"})
        h2 = catalog_manager._hash_schema({"id": "int", "name": "string"})
        assert h1 == h2


# ===========================================================================
# _get_latest_schema()
# ===========================================================================


class TestGetLatestSchema:
    """Tests for CatalogManager._get_latest_schema()."""

    def test_empty_table_returns_none(self, catalog_manager):
        """Empty meta_schemas returns None."""
        result = catalog_manager._get_latest_schema("silver/customers")
        assert result is None

    def test_returns_latest_version(self, catalog_manager):
        """Returns the record with the highest schema_version."""
        schema = {"id": "int", "name": "string"}
        _seed_schemas(
            catalog_manager,
            [
                {
                    "table_path": "silver/customers",
                    "schema_version": 1,
                    "schema_hash": "hash1",
                    "columns": json.dumps(schema),
                },
                {
                    "table_path": "silver/customers",
                    "schema_version": 2,
                    "schema_hash": "hash2",
                    "columns": json.dumps({**schema, "email": "string"}),
                },
            ],
        )
        result = catalog_manager._get_latest_schema("silver/customers")
        assert result is not None
        assert result["schema_version"] == 2
        assert result["schema_hash"] == "hash2"

    def test_filters_by_table_path(self, catalog_manager):
        """Only returns records for the specified table_path."""
        _seed_schemas(
            catalog_manager,
            [
                {
                    "table_path": "silver/customers",
                    "schema_version": 1,
                    "schema_hash": "h1",
                    "columns": "{}",
                },
                {
                    "table_path": "silver/orders",
                    "schema_version": 1,
                    "schema_hash": "h2",
                    "columns": "{}",
                },
            ],
        )
        result = catalog_manager._get_latest_schema("silver/orders")
        assert result is not None
        assert result["table_path"] == "silver/orders"

    def test_no_engine_returns_none(self, bare_catalog):
        """No engine returns None."""
        assert bare_catalog._get_latest_schema("silver/customers") is None

    def test_nonexistent_table_returns_none(self, catalog_manager):
        """Table path not in data returns None."""
        _seed_schemas(
            catalog_manager,
            [
                {
                    "table_path": "silver/customers",
                    "schema_version": 1,
                    "schema_hash": "h1",
                    "columns": "{}",
                },
            ],
        )
        assert catalog_manager._get_latest_schema("silver/nonexistent") is None


# ===========================================================================
# track_schema()
# ===========================================================================


class TestTrackSchema:
    """Tests for CatalogManager.track_schema()."""

    def test_first_schema_is_version_1(self, catalog_manager):
        """First schema tracked for a table gets version 1."""
        schema = {"id": "int", "name": "string"}
        result = catalog_manager.track_schema("silver/customers", schema, "pipe", "node", "run-001")
        assert result["changed"] is True
        assert result["version"] == 1
        assert result["previous_version"] is None
        assert result["columns_added"] == []
        assert result["columns_removed"] == []

    def test_unchanged_schema_not_tracked(self, catalog_manager):
        """Same schema submitted twice is not tracked again."""
        schema = {"id": "int", "name": "string"}
        r1 = catalog_manager.track_schema("silver/customers", schema, "p", "n", "r1")
        r2 = catalog_manager.track_schema("silver/customers", schema, "p", "n", "r2")
        assert r1["changed"] is True
        assert r2["changed"] is False
        assert r2["version"] == 1
        # Only one record should exist
        df = _read_table(catalog_manager, "meta_schemas")
        assert len(df) == 1

    def test_column_added_detected(self, catalog_manager):
        """Adding a column is detected in the changes."""
        schema_v1 = {"id": "int", "name": "string"}
        schema_v2 = {"id": "int", "name": "string", "email": "string"}
        catalog_manager.track_schema("silver/customers", schema_v1, "p", "n", "r1")
        result = catalog_manager.track_schema("silver/customers", schema_v2, "p", "n", "r2")
        assert result["changed"] is True
        assert result["version"] == 2
        assert result["previous_version"] == 1
        assert "email" in result["columns_added"]
        assert result["columns_removed"] == []

    def test_column_removed_detected(self, catalog_manager):
        """Removing a column is detected in the changes."""
        schema_v1 = {"id": "int", "name": "string", "email": "string"}
        schema_v2 = {"id": "int", "name": "string"}
        catalog_manager.track_schema("silver/customers", schema_v1, "p", "n", "r1")
        result = catalog_manager.track_schema("silver/customers", schema_v2, "p", "n", "r2")
        assert result["changed"] is True
        assert "email" in result["columns_removed"]

    def test_column_type_changed_detected(self, catalog_manager):
        """Changing a column type is detected."""
        schema_v1 = {"id": "int", "name": "string"}
        schema_v2 = {"id": "bigint", "name": "string"}
        catalog_manager.track_schema("silver/customers", schema_v1, "p", "n", "r1")
        result = catalog_manager.track_schema("silver/customers", schema_v2, "p", "n", "r2")
        assert result["changed"] is True
        assert "id" in result["columns_type_changed"]

    def test_no_engine_returns_default(self, bare_catalog):
        """No engine returns default unchanged result."""
        result = bare_catalog.track_schema("t", {"id": "int"}, "p", "n", "r")
        assert result == {"changed": False, "version": 0}

    def test_failure_returns_error_dict(self, catalog_manager):
        """Write failure returns error dict with changed=False."""
        with patch.object(catalog_manager.engine, "write", side_effect=IOError("fail")):
            result = catalog_manager.track_schema("t", {"id": "int"}, "p", "n", "r")
        assert result["changed"] is False
        assert result["version"] == 0
        assert "error" in result

    def test_multiple_tables_independent(self, catalog_manager):
        """Schema tracking for different tables is independent."""
        schema = {"id": "int"}
        r1 = catalog_manager.track_schema("silver/a", schema, "p", "n", "r1")
        r2 = catalog_manager.track_schema("silver/b", schema, "p", "n", "r2")
        assert r1["version"] == 1
        assert r2["version"] == 1
        df = _read_table(catalog_manager, "meta_schemas")
        assert len(df) == 2

    def test_version_increments_correctly(self, catalog_manager):
        """Version increments with each schema change."""
        catalog_manager.track_schema("silver/t", {"id": "int"}, "p", "n", "r1")
        catalog_manager.track_schema("silver/t", {"id": "int", "a": "str"}, "p", "n", "r2")
        r3 = catalog_manager.track_schema(
            "silver/t", {"id": "int", "a": "str", "b": "str"}, "p", "n", "r3"
        )
        assert r3["version"] == 3


# ===========================================================================
# get_schema_history()
# ===========================================================================


class TestGetSchemaHistory:
    """Tests for CatalogManager.get_schema_history()."""

    def test_empty_returns_empty_list(self, catalog_manager):
        """No schema records → empty list."""
        assert catalog_manager.get_schema_history("silver/customers") == []

    def test_returns_versions_descending(self, catalog_manager):
        """Returns schema versions in descending order."""
        _seed_schemas(
            catalog_manager,
            [
                {
                    "table_path": "silver/customers",
                    "schema_version": 1,
                    "schema_hash": "h1",
                    "columns": "{}",
                },
                {
                    "table_path": "silver/customers",
                    "schema_version": 3,
                    "schema_hash": "h3",
                    "columns": "{}",
                },
                {
                    "table_path": "silver/customers",
                    "schema_version": 2,
                    "schema_hash": "h2",
                    "columns": "{}",
                },
            ],
        )
        history = catalog_manager.get_schema_history("silver/customers")
        assert len(history) == 3
        assert history[0]["schema_version"] == 3
        assert history[1]["schema_version"] == 2
        assert history[2]["schema_version"] == 1

    def test_limit_results(self, catalog_manager):
        """Limit parameter caps the number of results."""
        _seed_schemas(
            catalog_manager,
            [
                {
                    "table_path": "silver/t",
                    "schema_version": i,
                    "schema_hash": f"h{i}",
                    "columns": "{}",
                }
                for i in range(1, 6)
            ],
        )
        history = catalog_manager.get_schema_history("silver/t", limit=2)
        assert len(history) == 2
        assert history[0]["schema_version"] == 5

    def test_filters_by_table_path(self, catalog_manager):
        """Only returns history for the specified table."""
        _seed_schemas(
            catalog_manager,
            [
                {
                    "table_path": "silver/a",
                    "schema_version": 1,
                    "schema_hash": "h1",
                    "columns": "{}",
                },
                {
                    "table_path": "silver/b",
                    "schema_version": 1,
                    "schema_hash": "h2",
                    "columns": "{}",
                },
            ],
        )
        history = catalog_manager.get_schema_history("silver/a")
        assert len(history) == 1
        assert history[0]["table_path"] == "silver/a"

    def test_no_engine_returns_empty(self, bare_catalog):
        """No engine returns empty list."""
        assert bare_catalog.get_schema_history("silver/t") == []

    def test_nonexistent_table_returns_empty(self, catalog_manager):
        """Table path not in data returns empty list."""
        _seed_schemas(
            catalog_manager,
            [
                {
                    "table_path": "silver/a",
                    "schema_version": 1,
                    "schema_hash": "h1",
                    "columns": "{}",
                },
            ],
        )
        assert catalog_manager.get_schema_history("silver/nonexistent") == []


# ===========================================================================
# record_lineage()
# ===========================================================================


class TestRecordLineage:
    """Tests for CatalogManager.record_lineage()."""

    def test_records_lineage_relationship(self, catalog_manager):
        """Records a lineage relationship to meta_lineage."""
        catalog_manager.record_lineage(
            source_table="bronze/raw_orders",
            target_table="silver/orders",
            target_pipeline="etl_orders",
            target_node="transform_orders",
            run_id="run-001",
            source_pipeline="ingest",
            source_node="load_raw",
        )
        df = _read_table(catalog_manager, "meta_lineage")
        assert len(df) == 1
        assert df.iloc[0]["source_table"] == "bronze/raw_orders"
        assert df.iloc[0]["target_table"] == "silver/orders"
        assert df.iloc[0]["relationship"] == "feeds"

    def test_no_engine_no_op(self, bare_catalog):
        """No engine → no-op."""
        bare_catalog.record_lineage("a", "b", "p", "n", "r")

    def test_retry_failure_logs_warning(self, catalog_manager):
        """Write failure logs warning."""
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(catalog_manager.engine, "write", side_effect=IOError("fail")):
                catalog_manager.record_lineage("a", "b", "p", "n", "r")
        mock_logger.warning.assert_called_once()
        assert "Failed to record lineage" in mock_logger.warning.call_args[0][0]

    def test_default_relationship_is_feeds(self, catalog_manager):
        """Default relationship is 'feeds'."""
        catalog_manager.record_lineage("a", "b", "p", "n", "r")
        df = _read_table(catalog_manager, "meta_lineage")
        assert df.iloc[0]["relationship"] == "feeds"

    def test_custom_relationship(self, catalog_manager):
        """Custom relationship type is persisted."""
        catalog_manager.record_lineage("a", "b", "p", "n", "r", relationship="derived_from")
        df = _read_table(catalog_manager, "meta_lineage")
        assert df.iloc[0]["relationship"] == "derived_from"

    def test_optional_source_fields_none(self, catalog_manager):
        """Source pipeline and node can be None."""
        catalog_manager.record_lineage("a", "b", "p", "n", "r")
        df = _read_table(catalog_manager, "meta_lineage")
        assert df.iloc[0]["source_pipeline"] is None
        assert df.iloc[0]["source_node"] is None


# ===========================================================================
# record_lineage_batch()
# ===========================================================================


class TestRecordLineageBatch:
    """Tests for CatalogManager.record_lineage_batch()."""

    def test_empty_records_no_op(self, catalog_manager):
        """Empty list is a no-op."""
        catalog_manager.record_lineage_batch([])

    def test_no_engine_no_op(self, bare_catalog):
        """No engine → no-op."""
        bare_catalog.record_lineage_batch(
            [
                {
                    "source_table": "a",
                    "target_table": "b",
                    "target_pipeline": "p",
                    "target_node": "n",
                    "run_id": "r",
                }
            ]
        )

    def test_records_multiple_relationships(self, catalog_manager):
        """Batch records multiple lineage entries."""
        records = [
            {
                "source_table": "bronze/raw",
                "target_table": "silver/clean",
                "target_pipeline": "p1",
                "target_node": "n1",
                "run_id": "r1",
            },
            {
                "source_table": "silver/clean",
                "target_table": "gold/agg",
                "target_pipeline": "p2",
                "target_node": "n2",
                "run_id": "r1",
            },
        ]
        catalog_manager.record_lineage_batch(records)
        df = _read_table(catalog_manager, "meta_lineage")
        assert len(df) == 2
        assert set(df["target_table"]) == {"silver/clean", "gold/agg"}

    def test_default_relationship_feeds(self, catalog_manager):
        """Missing relationship defaults to 'feeds'."""
        records = [
            {
                "source_table": "a",
                "target_table": "b",
                "target_pipeline": "p",
                "target_node": "n",
                "run_id": "r",
            },
        ]
        catalog_manager.record_lineage_batch(records)
        df = _read_table(catalog_manager, "meta_lineage")
        assert df.iloc[0]["relationship"] == "feeds"

    def test_retry_failure_logs_warning(self, catalog_manager):
        """Write failure logs warning."""
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(catalog_manager.engine, "write", side_effect=IOError("fail")):
                catalog_manager.record_lineage_batch(
                    [
                        {
                            "source_table": "a",
                            "target_table": "b",
                            "target_pipeline": "p",
                            "target_node": "n",
                            "run_id": "r",
                        }
                    ]
                )
        mock_logger.warning.assert_called_once()
        assert "Failed to batch record lineage" in mock_logger.warning.call_args[0][0]

    def test_optional_fields_default_none(self, catalog_manager):
        """Optional source_pipeline and source_node default to None."""
        records = [
            {
                "source_table": "a",
                "target_table": "b",
                "target_pipeline": "p",
                "target_node": "n",
                "run_id": "r",
            },
        ]
        catalog_manager.record_lineage_batch(records)
        df = _read_table(catalog_manager, "meta_lineage")
        assert df.iloc[0]["source_pipeline"] is None
        assert df.iloc[0]["source_node"] is None


# ===========================================================================
# register_assets_batch()
# ===========================================================================


class TestRegisterAssetsBatch:
    """Tests for CatalogManager.register_assets_batch()."""

    def test_empty_records_no_op(self, catalog_manager):
        """Empty list is a no-op."""
        catalog_manager.register_assets_batch([])

    def test_no_engine_no_op(self, bare_catalog):
        """No engine → no-op."""
        bare_catalog.register_assets_batch(
            [
                {
                    "project": "p",
                    "table_name": "t",
                    "path": "/x",
                    "format": "parquet",
                    "pattern_type": "dimension",
                }
            ]
        )

    def test_registers_multiple_assets(self, catalog_manager):
        """Batch registers multiple assets to meta_tables."""
        records = [
            {
                "project": "proj",
                "table_name": "customers",
                "path": "/data/customers",
                "format": "parquet",
                "pattern_type": "dimension",
                "schema_hash": "abc123",
            },
            {
                "project": "proj",
                "table_name": "orders",
                "path": "/data/orders",
                "format": "parquet",
                "pattern_type": "fact",
            },
        ]
        catalog_manager.register_assets_batch(records)
        df = _read_table(catalog_manager, "meta_tables")
        assert len(df) == 2
        assert set(df["table_name"]) == {"customers", "orders"}

    def test_default_schema_hash_empty(self, catalog_manager):
        """Missing schema_hash defaults to empty string."""
        records = [
            {
                "project": "proj",
                "table_name": "t",
                "path": "/data/t",
                "format": "parquet",
                "pattern_type": "dimension",
            },
        ]
        catalog_manager.register_assets_batch(records)
        df = _read_table(catalog_manager, "meta_tables")
        assert df.iloc[0]["schema_hash"] == ""

    def test_retry_failure_logs_warning(self, catalog_manager):
        """Write failure logs warning."""
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(catalog_manager.engine, "write", side_effect=IOError("fail")):
                catalog_manager.register_assets_batch(
                    [
                        {
                            "project": "p",
                            "table_name": "t",
                            "path": "/x",
                            "format": "parquet",
                            "pattern_type": "dim",
                        }
                    ]
                )
        mock_logger.warning.assert_called_once()
        assert "Failed to batch register assets" in mock_logger.warning.call_args[0][0]

    def test_project_name_key_alias(self, catalog_manager):
        """Accepts 'project_name' as alias for 'project'."""
        records = [
            {
                "project_name": "my_proj",
                "table_name": "t",
                "path": "/x",
                "format": "parquet",
                "pattern_type": "dim",
            },
        ]
        catalog_manager.register_assets_batch(records)
        df = _read_table(catalog_manager, "meta_tables")
        assert df.iloc[0]["project"] == "my_proj"


# ===========================================================================
# get_upstream()
# ===========================================================================


class TestGetUpstream:
    """Tests for CatalogManager.get_upstream()."""

    def test_no_engine_returns_empty(self, bare_catalog):
        """No engine returns empty list."""
        assert bare_catalog.get_upstream("gold/report") == []

    def test_empty_lineage_returns_empty(self, catalog_manager):
        """No lineage data returns empty list."""
        assert catalog_manager.get_upstream("gold/report") == []

    def test_single_hop_upstream(self, catalog_manager):
        """Finds only direct upstream sources with depth=0."""
        _seed_lineage(
            catalog_manager,
            [
                {"source_table": "bronze/raw", "target_table": "silver/clean"},
                {"source_table": "silver/clean", "target_table": "gold/report"},
            ],
        )
        result = catalog_manager.get_upstream("gold/report", depth=0)
        assert len(result) == 1
        assert result[0]["source_table"] == "silver/clean"
        assert result[0]["depth"] == 0

    def test_multi_hop_upstream(self, catalog_manager):
        """Traverses multiple hops upstream (BFS)."""
        _seed_lineage(
            catalog_manager,
            [
                {"source_table": "bronze/raw", "target_table": "silver/clean"},
                {"source_table": "silver/clean", "target_table": "gold/report"},
            ],
        )
        result = catalog_manager.get_upstream("gold/report", depth=3)
        # Should find silver/clean (depth 0) and bronze/raw (depth 1)
        source_tables = [r["source_table"] for r in result]
        assert "silver/clean" in source_tables
        assert "bronze/raw" in source_tables
        # bronze/raw should be at depth 1
        bronze = [r for r in result if r["source_table"] == "bronze/raw"][0]
        assert bronze["depth"] == 1

    def test_depth_limit(self, catalog_manager):
        """Depth limit prevents traversal beyond specified depth."""
        _seed_lineage(
            catalog_manager,
            [
                {"source_table": "a", "target_table": "b"},
                {"source_table": "b", "target_table": "c"},
                {"source_table": "c", "target_table": "d"},
            ],
        )
        result = catalog_manager.get_upstream("d", depth=1)
        source_tables = [r["source_table"] for r in result]
        assert "c" in source_tables
        assert "b" in source_tables
        # 'a' should NOT be included (depth 2 > limit 1)
        assert "a" not in source_tables

    def test_cycle_prevention(self, catalog_manager):
        """Cycles in lineage don't cause infinite loops."""
        _seed_lineage(
            catalog_manager,
            [
                {"source_table": "a", "target_table": "b"},
                {"source_table": "b", "target_table": "a"},
            ],
        )
        result = catalog_manager.get_upstream("b", depth=10)
        # Should complete without hanging
        assert len(result) >= 1

    def test_multiple_upstream_sources(self, catalog_manager):
        """A table with multiple upstream sources finds all of them."""
        _seed_lineage(
            catalog_manager,
            [
                {"source_table": "bronze/customers", "target_table": "silver/combined"},
                {"source_table": "bronze/orders", "target_table": "silver/combined"},
            ],
        )
        result = catalog_manager.get_upstream("silver/combined")
        assert len(result) == 2
        sources = {r["source_table"] for r in result}
        assert sources == {"bronze/customers", "bronze/orders"}


# ===========================================================================
# get_downstream()
# ===========================================================================


class TestGetDownstream:
    """Tests for CatalogManager.get_downstream()."""

    def test_no_engine_returns_empty(self, bare_catalog):
        """No engine returns empty list."""
        assert bare_catalog.get_downstream("bronze/raw") == []

    def test_empty_lineage_returns_empty(self, catalog_manager):
        """No lineage data returns empty list."""
        assert catalog_manager.get_downstream("bronze/raw") == []

    def test_single_hop_downstream(self, catalog_manager):
        """Finds only direct downstream consumers with depth=0."""
        _seed_lineage(
            catalog_manager,
            [
                {"source_table": "bronze/raw", "target_table": "silver/clean"},
                {"source_table": "silver/clean", "target_table": "gold/report"},
            ],
        )
        result = catalog_manager.get_downstream("bronze/raw", depth=0)
        assert len(result) == 1
        assert result[0]["target_table"] == "silver/clean"
        assert result[0]["depth"] == 0

    def test_multi_hop_downstream(self, catalog_manager):
        """Traverses multiple hops downstream (BFS)."""
        _seed_lineage(
            catalog_manager,
            [
                {"source_table": "bronze/raw", "target_table": "silver/clean"},
                {"source_table": "silver/clean", "target_table": "gold/report"},
            ],
        )
        result = catalog_manager.get_downstream("bronze/raw", depth=3)
        targets = [r["target_table"] for r in result]
        assert "silver/clean" in targets
        assert "gold/report" in targets
        gold = [r for r in result if r["target_table"] == "gold/report"][0]
        assert gold["depth"] == 1

    def test_depth_limit(self, catalog_manager):
        """Depth limit prevents traversal beyond specified depth."""
        _seed_lineage(
            catalog_manager,
            [
                {"source_table": "a", "target_table": "b"},
                {"source_table": "b", "target_table": "c"},
                {"source_table": "c", "target_table": "d"},
            ],
        )
        result = catalog_manager.get_downstream("a", depth=1)
        targets = [r["target_table"] for r in result]
        assert "b" in targets
        assert "c" in targets
        # 'd' should NOT be included (depth 2 > limit 1)
        assert "d" not in targets

    def test_cycle_prevention(self, catalog_manager):
        """Cycles in lineage don't cause infinite loops."""
        _seed_lineage(
            catalog_manager,
            [
                {"source_table": "a", "target_table": "b"},
                {"source_table": "b", "target_table": "a"},
            ],
        )
        result = catalog_manager.get_downstream("a", depth=10)
        assert len(result) >= 1

    def test_multiple_downstream_consumers(self, catalog_manager):
        """A table with multiple downstream consumers finds all of them."""
        _seed_lineage(
            catalog_manager,
            [
                {"source_table": "silver/customers", "target_table": "gold/report_a"},
                {"source_table": "silver/customers", "target_table": "gold/report_b"},
            ],
        )
        result = catalog_manager.get_downstream("silver/customers")
        assert len(result) == 2
        targets = {r["target_table"] for r in result}
        assert targets == {"gold/report_a", "gold/report_b"}

    def test_diamond_pattern(self, catalog_manager):
        """Handles diamond-shaped lineage (A→B, A→C, B→D, C→D)."""
        _seed_lineage(
            catalog_manager,
            [
                {"source_table": "a", "target_table": "b"},
                {"source_table": "a", "target_table": "c"},
                {"source_table": "b", "target_table": "d"},
                {"source_table": "c", "target_table": "d"},
            ],
        )
        result = catalog_manager.get_downstream("a", depth=3)
        targets = [r["target_table"] for r in result]
        assert "b" in targets
        assert "c" in targets
        assert "d" in targets

"""Tests for CatalogManager cleanup, orphan removal, and state management.

Covers: remove_pipeline, remove_node, cleanup_orphans,
        clear_state_key, clear_state_pattern  (issue #290).
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.catalog import CatalogManager
from odibi.config import SystemConfig
from odibi.engine.pandas_engine import PandasEngine


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog_manager(tmp_path):
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    engine = PandasEngine(config={})
    cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
    cm.bootstrap()
    cm.project = "test_project"
    return cm


def _write_pipelines(cm, names):
    """Seed meta_pipelines directly via write_deltalake (bypass engine index bug)."""
    import pyarrow as pa
    from deltalake import write_deltalake

    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "pipeline_name": names,
            "version_hash": ["abc123"] * len(names),
            "description": ["test"] * len(names),
            "layer": ["bronze"] * len(names),
            "schedule": ["daily"] * len(names),
            "tags_json": [json.dumps({})] * len(names),
            "updated_at": [now] * len(names),
        }
    )
    table = pa.Table.from_pandas(df, preserve_index=False)
    write_deltalake(cm.tables["meta_pipelines"], table, mode="overwrite", engine="rust")


def _write_nodes(cm, entries):
    """Seed meta_nodes. *entries* is a list of (pipeline_name, node_name)."""
    import pyarrow as pa
    from deltalake import write_deltalake

    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "pipeline_name": [e[0] for e in entries],
            "node_name": [e[1] for e in entries],
            "version_hash": ["abc123"] * len(entries),
            "type": ["transform"] * len(entries),
            "config_json": [json.dumps({})] * len(entries),
            "updated_at": [now] * len(entries),
        }
    )
    table = pa.Table.from_pandas(df, preserve_index=False)
    write_deltalake(cm.tables["meta_nodes"], table, mode="overwrite", engine="rust")


def _write_state(cm, entries):
    """Seed meta_state. *entries* is a list of (key, value)."""
    import pyarrow as pa
    from deltalake import write_deltalake

    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "key": [e[0] for e in entries],
            "value": [e[1] for e in entries],
            "environment": ["test"] * len(entries),
            "updated_at": [now] * len(entries),
        }
    )
    table = pa.Table.from_pandas(df, preserve_index=False)
    write_deltalake(cm.tables["meta_state"], table, mode="overwrite", engine="rust")


def _make_config(*pipeline_defs):
    """Build a mock config matching cleanup_orphans expectations.

    Each *pipeline_defs* entry is ``(pipeline_name, [node_name, ...])``.
    """
    pipelines = []
    for p_name, node_names in pipeline_defs:
        nodes = [MagicMock() for _ in node_names]
        for mock_node, n_name in zip(nodes, node_names):
            mock_node.name = n_name
        pipeline = MagicMock()
        pipeline.pipeline = p_name
        pipeline.nodes = nodes
        pipelines.append(pipeline)
    cfg = MagicMock()
    cfg.pipelines = pipelines
    return cfg


def _patch_engine_write(cm):
    """Patch engine.write to use write_deltalake with preserve_index=False.

    Works around a pre-existing bug where the pandas engine passes the
    DataFrame index to write_deltalake, causing a schema field-count
    mismatch on overwrite.
    """
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake

    original_write = cm.engine.write

    def _fixed_write(df, connection=None, format=None, path=None, mode=None, **kwargs):
        if format == "delta" and mode == "overwrite":
            dt = DeltaTable(path)
            existing_schema = dt.schema().to_pyarrow()
            table = pa.Table.from_pandas(df, schema=existing_schema, preserve_index=False)
            write_deltalake(path, table, mode="overwrite", engine="rust")
            return {}
        return original_write(
            df, connection=connection, format=format, path=path, mode=mode, **kwargs
        )

    return patch.object(cm.engine, "write", side_effect=_fixed_write)


# ---------------------------------------------------------------------------
# remove_pipeline
# ---------------------------------------------------------------------------


class TestRemovePipeline:
    def test_no_engine_returns_zero(self, tmp_path):
        config = SystemConfig(connection="local", path="_odibi_system")
        cm = CatalogManager(
            spark=None, config=config, base_path=str(tmp_path / "_sys"), engine=None
        )
        assert cm.remove_pipeline("any") == 0

    def test_removes_pipeline_from_meta_pipelines(self, catalog_manager):
        cm = catalog_manager
        _write_pipelines(cm, ["p1", "p2"])

        with _patch_engine_write(cm):
            count = cm.remove_pipeline("p1")

        assert count >= 1
        df = cm._read_local_table(cm.tables["meta_pipelines"])
        assert "p1" not in df["pipeline_name"].values

    def test_removes_associated_nodes(self, catalog_manager):
        cm = catalog_manager
        _write_pipelines(cm, ["p1"])
        _write_nodes(cm, [("p1", "n1"), ("p1", "n2")])

        with _patch_engine_write(cm):
            cm.remove_pipeline("p1")

        df_nodes = cm._read_local_table(cm.tables["meta_nodes"])
        if not df_nodes.empty:
            assert "p1" not in df_nodes["pipeline_name"].values

    def test_returns_correct_deleted_count(self, catalog_manager):
        cm = catalog_manager
        _write_pipelines(cm, ["p1"])
        _write_nodes(cm, [("p1", "n1")])

        with _patch_engine_write(cm):
            count = cm.remove_pipeline("p1")

        # 1 pipeline + 1 node = 2
        assert count == 2

    def test_nonexistent_pipeline_returns_zero(self, catalog_manager):
        cm = catalog_manager
        _write_pipelines(cm, ["p1"])

        with _patch_engine_write(cm):
            count = cm.remove_pipeline("does_not_exist")

        assert count == 0

    def test_cache_invalidated_after_removal(self, catalog_manager):
        cm = catalog_manager
        _write_pipelines(cm, ["p1", "p2"])
        cm._pipelines_cache = {"stale": True}
        cm._nodes_cache = {"stale": True}

        with _patch_engine_write(cm):
            cm.remove_pipeline("p1")

        assert cm._pipelines_cache is None
        assert cm._nodes_cache is None

    def test_multiple_pipelines_only_named_one_removed(self, catalog_manager):
        cm = catalog_manager
        _write_pipelines(cm, ["keep_me", "remove_me"])
        _write_nodes(cm, [("keep_me", "n_keep"), ("remove_me", "n_remove")])

        with _patch_engine_write(cm):
            cm.remove_pipeline("remove_me")

        df_p = cm._read_local_table(cm.tables["meta_pipelines"])
        assert "keep_me" in df_p["pipeline_name"].values
        assert "remove_me" not in df_p["pipeline_name"].values

        df_n = cm._read_local_table(cm.tables["meta_nodes"])
        assert "n_keep" in df_n["node_name"].values
        remaining = df_n[df_n["pipeline_name"] == "remove_me"]
        assert remaining.empty


# ---------------------------------------------------------------------------
# remove_node
# ---------------------------------------------------------------------------


class TestRemoveNode:
    def test_no_engine_returns_zero(self, tmp_path):
        config = SystemConfig(connection="local", path="_odibi_system")
        cm = CatalogManager(
            spark=None, config=config, base_path=str(tmp_path / "_sys"), engine=None
        )
        assert cm.remove_node("p", "n") == 0

    def test_removes_specific_node(self, catalog_manager):
        cm = catalog_manager
        _write_nodes(cm, [("p1", "n1"), ("p1", "n2")])

        with _patch_engine_write(cm):
            count = cm.remove_node("p1", "n1")

        assert count == 1
        df = cm._read_local_table(cm.tables["meta_nodes"])
        matched = df[(df["pipeline_name"] == "p1") & (df["node_name"] == "n1")]
        assert matched.empty

    def test_other_nodes_preserved(self, catalog_manager):
        cm = catalog_manager
        _write_nodes(cm, [("p1", "n1"), ("p1", "n2")])

        with _patch_engine_write(cm):
            cm.remove_node("p1", "n1")

        df = cm._read_local_table(cm.tables["meta_nodes"])
        assert "n2" in df["node_name"].values

    def test_returns_correct_count(self, catalog_manager):
        cm = catalog_manager
        _write_nodes(cm, [("p1", "target")])

        with _patch_engine_write(cm):
            assert cm.remove_node("p1", "target") == 1

    def test_nonexistent_node_returns_zero(self, catalog_manager):
        cm = catalog_manager
        _write_nodes(cm, [("p1", "n1")])

        with _patch_engine_write(cm):
            assert cm.remove_node("p1", "ghost") == 0

    def test_nodes_cache_set_to_none(self, catalog_manager):
        cm = catalog_manager
        _write_nodes(cm, [("p1", "n1"), ("p1", "n2")])
        cm._nodes_cache = {"stale": True}

        with _patch_engine_write(cm):
            cm.remove_node("p1", "n1")

        assert cm._nodes_cache is None


# ---------------------------------------------------------------------------
# cleanup_orphans
# ---------------------------------------------------------------------------


class TestCleanupOrphans:
    def test_no_engine_returns_empty_dict(self, tmp_path):
        config = SystemConfig(connection="local", path="_odibi_system")
        cm = CatalogManager(
            spark=None, config=config, base_path=str(tmp_path / "_sys"), engine=None
        )
        assert cm.cleanup_orphans(MagicMock(pipelines=[])) == {}

    def test_removes_pipelines_not_in_config(self, catalog_manager):
        cm = catalog_manager
        _write_pipelines(cm, ["active", "orphan"])

        cfg = _make_config(("active", []))
        with _patch_engine_write(cm):
            results = cm.cleanup_orphans(cfg)

        assert results["meta_pipelines"] >= 1
        df = cm._read_local_table(cm.tables["meta_pipelines"])
        assert "orphan" not in df["pipeline_name"].values

    def test_removes_nodes_not_in_config(self, catalog_manager):
        cm = catalog_manager
        _write_pipelines(cm, ["p1"])
        _write_nodes(cm, [("p1", "keep_node"), ("p1", "orphan_node")])

        cfg = _make_config(("p1", ["keep_node"]))
        with _patch_engine_write(cm):
            results = cm.cleanup_orphans(cfg)

        assert results["meta_nodes"] >= 1
        df = cm._read_local_table(cm.tables["meta_nodes"])
        remaining_names = df["node_name"].tolist() if not df.empty else []
        assert "orphan_node" not in remaining_names

    def test_keeps_pipelines_in_config(self, catalog_manager):
        cm = catalog_manager
        _write_pipelines(cm, ["p1", "p2"])

        cfg = _make_config(("p1", []), ("p2", []))
        with _patch_engine_write(cm):
            results = cm.cleanup_orphans(cfg)

        assert results["meta_pipelines"] == 0
        df = cm._read_local_table(cm.tables["meta_pipelines"])
        assert set(df["pipeline_name"].tolist()) == {"p1", "p2"}

    def test_returns_counts_per_table(self, catalog_manager):
        cm = catalog_manager
        _write_pipelines(cm, ["p1", "orphan_p"])
        _write_nodes(cm, [("p1", "n1"), ("orphan_p", "orphan_n")])

        cfg = _make_config(("p1", ["n1"]))
        with _patch_engine_write(cm):
            results = cm.cleanup_orphans(cfg)

        assert "meta_pipelines" in results
        assert "meta_nodes" in results
        assert results["meta_pipelines"] == 1
        assert results["meta_nodes"] == 1

    def test_mock_config_structure(self, catalog_manager):
        cm = catalog_manager
        _write_pipelines(cm, ["sales"])
        _write_nodes(cm, [("sales", "extract")])

        cfg = _make_config(("sales", ["extract"]))
        with _patch_engine_write(cm):
            results = cm.cleanup_orphans(cfg)

        assert results["meta_pipelines"] == 0
        assert results["meta_nodes"] == 0

    def test_empty_config_removes_everything(self, catalog_manager):
        cm = catalog_manager
        _write_pipelines(cm, ["p1", "p2"])
        _write_nodes(cm, [("p1", "n1"), ("p2", "n2")])

        cfg = _make_config()  # empty pipelines list
        with _patch_engine_write(cm):
            results = cm.cleanup_orphans(cfg)

        assert results["meta_pipelines"] == 2
        assert results["meta_nodes"] == 2

    def test_cache_invalidated_after_cleanup(self, catalog_manager):
        cm = catalog_manager
        _write_pipelines(cm, ["p1"])
        cm._pipelines_cache = {"stale": True}
        cm._nodes_cache = {"stale": True}

        cfg = _make_config(("p1", []))
        with _patch_engine_write(cm):
            cm.cleanup_orphans(cfg)

        assert cm._pipelines_cache is None
        assert cm._nodes_cache is None


# ---------------------------------------------------------------------------
# clear_state_key
# ---------------------------------------------------------------------------


class TestClearStateKey:
    def test_no_engine_returns_false(self, tmp_path):
        config = SystemConfig(connection="local", path="_odibi_system")
        cm = CatalogManager(
            spark=None, config=config, base_path=str(tmp_path / "_sys"), engine=None
        )
        assert cm.clear_state_key("any") is False

    def test_removes_matching_key_returns_true(self, catalog_manager):
        cm = catalog_manager
        _write_state(cm, [("hwm_node1", "2024-01-01"), ("keep", "val")])

        with _patch_engine_write(cm):
            result = cm.clear_state_key("hwm_node1")

        assert result is True
        df = cm._read_local_table(cm.tables["meta_state"])
        assert "hwm_node1" not in df["key"].values

    def test_nonexistent_key_returns_false(self, catalog_manager):
        cm = catalog_manager
        _write_state(cm, [("existing_key", "value")])

        with _patch_engine_write(cm):
            assert cm.clear_state_key("no_such_key") is False

    def test_empty_state_table_returns_false(self, catalog_manager):
        cm = catalog_manager
        assert cm.clear_state_key("anything") is False

    def test_other_keys_preserved(self, catalog_manager):
        cm = catalog_manager
        _write_state(cm, [("keep_me", "v1"), ("delete_me", "v2")])

        with _patch_engine_write(cm):
            cm.clear_state_key("delete_me")

        df = cm._read_local_table(cm.tables["meta_state"])
        assert "keep_me" in df["key"].values
        assert "delete_me" not in df["key"].values


# ---------------------------------------------------------------------------
# clear_state_pattern
# ---------------------------------------------------------------------------


class TestClearStatePattern:
    def test_no_engine_returns_zero(self, tmp_path):
        config = SystemConfig(connection="local", path="_odibi_system")
        cm = CatalogManager(
            spark=None, config=config, base_path=str(tmp_path / "_sys"), engine=None
        )
        assert cm.clear_state_pattern("hwm_*") == 0

    def test_wildcard_matches_multiple_keys(self, catalog_manager):
        cm = catalog_manager
        _write_state(cm, [("hwm_node1", "v1"), ("hwm_node2", "v2"), ("other_key", "v3")])

        with _patch_engine_write(cm):
            count = cm.clear_state_pattern("hwm_*")

        assert count == 2
        df = cm._read_local_table(cm.tables["meta_state"])
        assert "other_key" in df["key"].values
        remaining_keys = df["key"].tolist()
        assert "hwm_node1" not in remaining_keys
        assert "hwm_node2" not in remaining_keys

    def test_exact_pattern_matches_single_key(self, catalog_manager):
        cm = catalog_manager
        _write_state(cm, [("exact_key", "v1"), ("other", "v2")])

        with _patch_engine_write(cm):
            count = cm.clear_state_pattern("exact_key")

        assert count == 1
        df = cm._read_local_table(cm.tables["meta_state"])
        assert "exact_key" not in df["key"].values
        assert "other" in df["key"].values

    def test_nonmatching_pattern_returns_zero(self, catalog_manager):
        cm = catalog_manager
        _write_state(cm, [("hwm_node1", "v1")])

        with _patch_engine_write(cm):
            count = cm.clear_state_pattern("zzz_*")

        assert count == 0

    def test_returns_correct_deleted_count(self, catalog_manager):
        cm = catalog_manager
        _write_state(cm, [("prefix_a", "1"), ("prefix_b", "2"), ("prefix_c", "3")])

        with _patch_engine_write(cm):
            count = cm.clear_state_pattern("prefix_*")

        assert count == 3

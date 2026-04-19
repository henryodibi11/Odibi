"""Coverage tests for odibi/story/lineage.py — uncovered branches and helpers."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from odibi.story.lineage import (
    LayerInfo,
    LineageEdge,
    LineageGenerator,
    LineageNode,
    LineageResult,
)


# ---------------------------------------------------------------------------
# Dataclass serialization
# ---------------------------------------------------------------------------


class TestDataclasses:
    def test_lineage_node_to_dict(self):
        n = LineageNode(id="a.b", type="table", layer="gold")
        assert n.to_dict() == {"id": "a.b", "type": "table", "layer": "gold"}

    def test_lineage_edge_to_dict(self):
        e = LineageEdge(from_node="x", to_node="y")
        assert e.to_dict() == {"from": "x", "to": "y"}

    def test_lineage_result_to_dict(self):
        r = LineageResult(
            generated_at="2026-01-01",
            date="2026-01-01",
            layers=[LayerInfo(name="bronze", story_path="p", status="success", duration=1.0)],
            nodes=[LineageNode(id="n1", type="table", layer="bronze")],
            edges=[LineageEdge(from_node="n1", to_node="n2")],
        )
        d = r.to_dict()
        assert d["date"] == "2026-01-01"
        assert len(d["layers"]) == 1
        assert d["layers"][0]["name"] == "bronze"
        assert len(d["nodes"]) == 1
        assert len(d["edges"]) == 1


# ---------------------------------------------------------------------------
# Helper methods
# ---------------------------------------------------------------------------


class TestHelperMethods:
    def test_sanitize_id(self):
        g = LineageGenerator(stories_path="/tmp/stories")
        assert g._sanitize_id("a.b-c d") == "a_b_c_d"

    def test_infer_layer_bronze(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._infer_layer("my_bronze_table") == "bronze"

    def test_infer_layer_silver(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._infer_layer("silver_customers") == "silver"

    def test_infer_layer_gold(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._infer_layer("gold_fact_orders") == "gold"

    def test_infer_layer_raw(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._infer_layer("raw_data") == "raw"

    def test_infer_layer_semantic_vw(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._infer_layer("vw_orders") == "semantic"

    def test_infer_layer_semantic_keyword(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._infer_layer("my_semantic_view") == "semantic"

    def test_infer_layer_unknown(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._infer_layer("foobar_table") == "unknown"

    def test_infer_layer_from_path_bronze(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._infer_layer_from_path("/data/bronze/table.parquet") == "bronze"

    def test_infer_layer_from_path_unknown(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._infer_layer_from_path("/data/foo/table.parquet") == "unknown"

    def test_normalize_node_name_slash(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._normalize_node_name("Sales/gold/fact_orders") == "fact_orders"

    def test_normalize_node_name_dot(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._normalize_node_name("sales.fact_orders") == "fact_orders"

    def test_normalize_node_name_simple(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._normalize_node_name("My_Table") == "my_table"

    def test_layer_sort_key_known(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._layer_sort_key("bronze") < g._layer_sort_key("gold")

    def test_layer_sort_key_unknown(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._layer_sort_key("something_else") == len(g.LAYER_ORDER)

    def test_layer_sort_key_none(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._layer_sort_key(None) == len(g.LAYER_ORDER)

    def test_get_layer_class_bronze(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._get_layer_class("Bronze") == "layer-bronze"

    def test_get_layer_class_silver(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._get_layer_class("silver") == "layer-silver"

    def test_get_layer_class_gold(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._get_layer_class("GOLD") == "layer-gold"

    def test_get_layer_class_semantic(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._get_layer_class("semantic") == "layer-semantic"

    def test_get_layer_class_none(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._get_layer_class(None) == ""

    def test_get_layer_class_unknown(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g._get_layer_class("random") == ""

    def test_result_property_none(self):
        g = LineageGenerator(stories_path="/tmp")
        assert g.result is None

    def test_is_remote(self):
        g = LineageGenerator(stories_path="abfs://container@account/path")
        assert g.is_remote is True

    def test_is_local(self):
        g = LineageGenerator(stories_path="/tmp/stories")
        assert g.is_remote is False


# ---------------------------------------------------------------------------
# render_json / render_html — no result
# ---------------------------------------------------------------------------


class TestRenderNoResult:
    def test_render_json_no_result(self):
        g = LineageGenerator(stories_path="/tmp")
        with pytest.raises(ValueError, match="No lineage result"):
            g.render_json()

    def test_render_html_no_result(self):
        g = LineageGenerator(stories_path="/tmp")
        with pytest.raises(ValueError, match="No lineage result"):
            g.render_html()

    def test_render_json_with_result(self):
        g = LineageGenerator(stories_path="/tmp")
        r = LineageResult(generated_at="now", date="2026-01-01", layers=[], nodes=[], edges=[])
        j = g.render_json(r)
        assert '"date": "2026-01-01"' in j

    def test_render_html_with_result(self):
        g = LineageGenerator(stories_path="/tmp")
        r = LineageResult(
            generated_at="now",
            date="2026-01-01",
            layers=[
                LayerInfo(
                    name="bronze",
                    story_path="p.html",
                    status="success",
                    duration=1.0,
                    pipeline_layer="bronze",
                )
            ],
            nodes=[LineageNode(id="n1", type="table", layer="bronze")],
            edges=[LineageEdge(from_node="n1", to_node="n2")],
        )
        html = g.render_html(r)
        assert "Data Lineage" in html
        assert "2026-01-01" in html


# ---------------------------------------------------------------------------
# _find_story_files — local
# ---------------------------------------------------------------------------


class TestFindStoryFilesLocal:
    def test_stories_path_not_exists(self, tmp_path):
        g = LineageGenerator(stories_path=str(tmp_path / "nonexistent"))
        files = g._find_story_files("2026-01-01")
        assert files == []

    def test_skips_non_dir_and_excluded(self, tmp_path):
        date = "2026-01-01"
        # Create a file (not directory)
        (tmp_path / "notadir.txt").write_text("hi")
        # Create excluded dirs
        (tmp_path / "lineage" / date).mkdir(parents=True)
        (tmp_path / "__pycache__" / date).mkdir(parents=True)
        # Create valid pipeline dir without date subdir
        (tmp_path / "pipeline_a").mkdir()

        g = LineageGenerator(stories_path=str(tmp_path))
        files = g._find_story_files(date)
        assert files == []

    def test_finds_latest_json(self, tmp_path):
        date = "2026-01-01"
        pipeline_dir = tmp_path / "bronze" / date
        pipeline_dir.mkdir(parents=True)
        (pipeline_dir / "run_10-00-00.json").write_text("{}")
        (pipeline_dir / "run_14-30-00.json").write_text("{}")

        g = LineageGenerator(stories_path=str(tmp_path))
        files = g._find_story_files(date)
        assert len(files) == 1
        assert "run_14-30-00.json" in files[0]

    def test_no_json_in_date_dir(self, tmp_path):
        date = "2026-01-01"
        pipeline_dir = tmp_path / "bronze" / date
        pipeline_dir.mkdir(parents=True)
        (pipeline_dir / "readme.txt").write_text("not json")

        g = LineageGenerator(stories_path=str(tmp_path))
        files = g._find_story_files(date)
        assert files == []


# ---------------------------------------------------------------------------
# _find_remote_story_files
# ---------------------------------------------------------------------------


class TestFindRemoteStoryFiles:
    def test_import_error(self):
        g = LineageGenerator(stories_path="abfs://container@acct/stories")
        import sys

        original = sys.modules.get("fsspec")
        sys.modules["fsspec"] = None
        try:
            files = g._find_remote_story_files("2026-01-01")
        except (ImportError, TypeError):
            files = []
        finally:
            if original is not None:
                sys.modules["fsspec"] = original
            else:
                sys.modules.pop("fsspec", None)
        assert files == []

    def test_remote_path_not_exists(self):
        g = LineageGenerator(stories_path="abfs://container@acct/stories")
        mock_fs = MagicMock()
        mock_fs.exists.return_value = False

        with patch("fsspec.core.url_to_fs", return_value=(mock_fs, "path")):
            files = g._find_remote_story_files("2026-01-01")
        assert files == []

    def test_remote_finds_json_files(self):
        g = LineageGenerator(stories_path="abfs://container@acct/stories")
        mock_fs = MagicMock()
        mock_fs.exists.side_effect = lambda p: True  # all paths exist
        mock_fs.ls.side_effect = [
            ["path/bronze"],  # top-level listing
            ["path/bronze/2026-01-01/run_14-30-00.json"],  # date dir listing
        ]
        mock_fs.isdir.return_value = True

        with patch("fsspec.core.url_to_fs", return_value=(mock_fs, "path")):
            files = g._find_remote_story_files("2026-01-01")
        assert len(files) == 1

    def test_remote_general_exception(self):
        g = LineageGenerator(stories_path="abfs://container@acct/stories")
        with patch("fsspec.core.url_to_fs", side_effect=RuntimeError("boom")):
            files = g._find_remote_story_files("2026-01-01")
        assert files == []


# ---------------------------------------------------------------------------
# _load_story
# ---------------------------------------------------------------------------


class TestLoadStory:
    def test_load_local_success(self, tmp_path):
        story = {"pipeline_name": "test", "completed_nodes": 1}
        f = tmp_path / "story.json"
        f.write_text(json.dumps(story))

        g = LineageGenerator(stories_path=str(tmp_path))
        result = g._load_story(str(f))
        assert result["pipeline_name"] == "test"

    def test_load_local_failure_returns_none(self, tmp_path):
        g = LineageGenerator(stories_path=str(tmp_path))
        result = g._load_story(str(tmp_path / "nonexistent.json"), max_retries=1, retry_delay=0)
        assert result is None

    def test_load_remote_with_full_url(self):
        g = LineageGenerator(stories_path="abfs://container@acct/stories")
        mock_file = MagicMock()
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__ = MagicMock(return_value=False)

        import io

        io.StringIO('{"test": true}')
        with patch("fsspec.open", return_value=mock_file):
            with patch("json.load", return_value={"test": True}):
                result = g._load_story("abfs://container@acct/stories/b/2026/run.json")
        assert result == {"test": True}

    def test_load_remote_relative_path(self):
        g = LineageGenerator(stories_path="abfs://container@acct/stories")
        mock_fs = MagicMock()
        mock_file = MagicMock()
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__ = MagicMock(return_value=False)
        mock_fs.open.return_value = mock_file

        with patch("fsspec.core.url_to_fs", return_value=(mock_fs, "base")):
            with patch("json.load", return_value={"ok": True}):
                result = g._load_story("container/stories/run.json")
        assert result == {"ok": True}


# ---------------------------------------------------------------------------
# _extract_layer_info
# ---------------------------------------------------------------------------


class TestExtractLayerInfo:
    def test_success_status(self, tmp_path):
        g = LineageGenerator(stories_path=str(tmp_path))
        data = {"pipeline_name": "test", "completed_nodes": 3, "failed_nodes": 0, "duration": 10.0}
        info = g._extract_layer_info(data, str(tmp_path / "test" / "2026" / "run.json"))
        assert info.status == "success"
        assert info.name == "test"

    def test_failed_status(self, tmp_path):
        g = LineageGenerator(stories_path=str(tmp_path))
        data = {"pipeline_name": "test", "completed_nodes": 3, "failed_nodes": 1, "duration": 5.0}
        info = g._extract_layer_info(data, str(tmp_path / "p.json"))
        assert info.status == "failed"

    def test_unknown_status(self, tmp_path):
        g = LineageGenerator(stories_path=str(tmp_path))
        data = {"pipeline_name": "test", "completed_nodes": 0, "failed_nodes": 0, "duration": 0.0}
        info = g._extract_layer_info(data, str(tmp_path / "p.json"))
        assert info.status == "unknown"

    def test_views_failed_status(self, tmp_path):
        g = LineageGenerator(stories_path=str(tmp_path))
        data = {"pipeline_name": "test", "views_failed": 2, "duration": 0.0}
        info = g._extract_layer_info(data, str(tmp_path / "p.json"))
        assert info.status == "failed"

    def test_views_created_success(self, tmp_path):
        g = LineageGenerator(stories_path=str(tmp_path))
        data = {"pipeline_name": "test", "views_created": 5, "duration": 2.0}
        info = g._extract_layer_info(data, str(tmp_path / "p.json"))
        assert info.status == "success"

    def test_relative_path_fallback(self):
        g = LineageGenerator(stories_path="/nonexistent_base")
        data = {"pipeline_name": "test", "completed_nodes": 1, "duration": 1.0}
        info = g._extract_layer_info(data, "/other/path/run.json")
        assert info.story_path.endswith(".html")

    def test_name_from_name_field(self, tmp_path):
        g = LineageGenerator(stories_path=str(tmp_path))
        data = {"name": "my_pipeline", "completed_nodes": 1, "duration": 0.0}
        info = g._extract_layer_info(data, str(tmp_path / "p.json"))
        assert info.name == "my_pipeline"


# ---------------------------------------------------------------------------
# _stitch_cross_layer_edges
# ---------------------------------------------------------------------------


class TestStitchCrossLayerEdges:
    def test_creates_edges_between_layers(self):
        g = LineageGenerator(stories_path="/tmp")
        nodes = {
            "bronze/fact_orders": LineageNode(
                id="bronze/fact_orders", type="table", layer="bronze"
            ),
            "gold/fact_orders": LineageNode(id="gold/fact_orders", type="table", layer="gold"),
        }
        edges = []
        edge_set = set()
        new_edges = g._stitch_cross_layer_edges(nodes, edges, edge_set)
        assert len(new_edges) == 1
        assert new_edges[0].from_node == "bronze/fact_orders"
        assert new_edges[0].to_node == "gold/fact_orders"

    def test_no_stitching_single_layer(self):
        g = LineageGenerator(stories_path="/tmp")
        nodes = {
            "bronze/a": LineageNode(id="bronze/a", type="table", layer="bronze"),
            "bronze/b": LineageNode(id="bronze/b", type="table", layer="bronze"),
        }
        new_edges = g._stitch_cross_layer_edges(nodes, [], set())
        assert len(new_edges) == 0

    def test_skips_existing_edges(self):
        g = LineageGenerator(stories_path="/tmp")
        nodes = {
            "bronze/t": LineageNode(id="bronze/t", type="table", layer="bronze"),
            "gold/t": LineageNode(id="gold/t", type="table", layer="gold"),
        }
        edge_set = {("bronze/t", "gold/t")}
        new_edges = g._stitch_cross_layer_edges(nodes, [], edge_set)
        assert len(new_edges) == 0

    def test_same_layer_no_edge(self):
        g = LineageGenerator(stories_path="/tmp")
        nodes = {
            "a/fact_orders": LineageNode(id="a/fact_orders", type="table", layer="gold"),
            "b/fact_orders": LineageNode(id="b/fact_orders", type="table", layer="gold"),
        }
        new_edges = g._stitch_cross_layer_edges(nodes, [], set())
        assert len(new_edges) == 0


# ---------------------------------------------------------------------------
# _inherit_layers_from_matches
# ---------------------------------------------------------------------------


class TestInheritLayersFromMatches:
    def test_unknown_inherits_gold(self):
        g = LineageGenerator(stories_path="/tmp")
        nodes = {
            "gold/fact_orders": LineageNode(id="gold/fact_orders", type="table", layer="gold"),
            "sales.fact_orders": LineageNode(id="sales.fact_orders", type="table", layer="unknown"),
        }
        g._inherit_layers_from_matches(nodes)
        assert nodes["sales.fact_orders"].layer == "gold"

    def test_no_update_if_already_definitive(self):
        g = LineageGenerator(stories_path="/tmp")
        nodes = {
            "bronze/tbl": LineageNode(id="bronze/tbl", type="table", layer="bronze"),
            "gold/tbl": LineageNode(id="gold/tbl", type="table", layer="gold"),
        }
        g._inherit_layers_from_matches(nodes)
        # bronze should NOT be overridden to gold (both are definitive)
        assert nodes["bronze/tbl"].layer == "bronze"


# ---------------------------------------------------------------------------
# save
# ---------------------------------------------------------------------------


class TestSave:
    def test_save_no_result(self):
        g = LineageGenerator(stories_path="/tmp")
        with pytest.raises(ValueError, match="No lineage result"):
            g.save()

    def test_save_local(self, tmp_path):
        g = LineageGenerator(stories_path=str(tmp_path))
        r = LineageResult(generated_at="now", date="2026-01-01", layers=[], nodes=[], edges=[])
        paths = g.save(r)
        assert "json" in paths
        assert "html" in paths
        assert Path(paths["json"]).exists()
        assert Path(paths["html"]).exists()

    def test_save_with_write_file(self, tmp_path):
        g = LineageGenerator(stories_path=str(tmp_path))
        r = LineageResult(generated_at="now", date="2026-01-01", layers=[], nodes=[], edges=[])
        written = {}

        def writer(path, content):
            written[path] = content

        g.save(r, write_file=writer)
        assert len(written) == 2

    def test_save_remote_path_format(self):
        g = LineageGenerator(stories_path="abfs://container@acct/stories")
        r = LineageResult(generated_at="now", date="2026-01-01", layers=[], nodes=[], edges=[])
        written = {}

        def writer(path, content):
            written[path] = content

        paths = g.save(r, write_file=writer)
        assert "lineage/2026-01-01" in paths["json"]


# ---------------------------------------------------------------------------
# _generate_mermaid_diagram
# ---------------------------------------------------------------------------


class TestMermaidDiagram:
    def test_basic_diagram(self):
        g = LineageGenerator(stories_path="/tmp")
        r = LineageResult(
            generated_at="now",
            date="2026-01-01",
            layers=[],
            nodes=[
                LineageNode(id="raw.src", type="source", layer="raw"),
                LineageNode(id="bronze.tbl", type="table", layer="bronze"),
            ],
            edges=[LineageEdge(from_node="raw.src", to_node="bronze.tbl")],
        )
        mermaid = g._generate_mermaid_diagram(r)
        assert "graph LR" in mermaid
        assert "raw_src" in mermaid  # sanitized
        assert "bronze_tbl" in mermaid
        assert "-->" in mermaid

    def test_view_node_shape(self):
        g = LineageGenerator(stories_path="/tmp")
        r = LineageResult(
            generated_at="now",
            date="2026-01-01",
            layers=[],
            nodes=[LineageNode(id="vw_sales", type="view", layer="semantic")],
            edges=[],
        )
        mermaid = g._generate_mermaid_diagram(r)
        # View nodes use square brackets, tables use stadium shape
        assert 'vw_sales["vw_sales"]' in mermaid


# ---------------------------------------------------------------------------
# _generate_layers_table
# ---------------------------------------------------------------------------


class TestLayersTable:
    def test_empty_layers(self):
        g = LineageGenerator(stories_path="/tmp")
        r = LineageResult(generated_at="now", date="2026-01-01", layers=[], nodes=[], edges=[])
        html = g._generate_layers_table(r)
        assert "No pipeline layers" in html

    def test_with_layers(self):
        g = LineageGenerator(stories_path="/tmp")
        r = LineageResult(
            generated_at="now",
            date="2026-01-01",
            layers=[
                LayerInfo(
                    name="bronze",
                    story_path="p.html",
                    status="success",
                    duration=5.0,
                    pipeline_layer="bronze",
                ),
                LayerInfo(
                    name="gold",
                    story_path="g.html",
                    status="failed",
                    duration=10.0,
                    pipeline_layer="gold",
                ),
            ],
            nodes=[],
            edges=[],
        )
        html = g._generate_layers_table(r)
        assert "bronze" in html
        assert "gold" in html
        assert "success" in html
        assert "failed" in html


# ---------------------------------------------------------------------------
# generate() — node processing edge cases
# ---------------------------------------------------------------------------


class TestGenerateEdgeCases:
    def test_empty_node_id_skipped(self, tmp_path):
        date = "2026-01-01"
        pipeline_dir = tmp_path / "test" / date
        pipeline_dir.mkdir(parents=True)
        story = {
            "pipeline_name": "test",
            "pipeline_layer": "bronze",
            "completed_nodes": 1,
            "failed_nodes": 0,
            "duration": 1.0,
            "graph_data": {
                "nodes": [
                    {"id": "", "type": "table"},  # empty id
                    {"id": "valid", "type": "table"},
                ],
                "edges": [],
            },
        }
        (pipeline_dir / "run.json").write_text(json.dumps(story))

        g = LineageGenerator(stories_path=str(tmp_path))
        result = g.generate(date=date)
        node_ids = [n.id for n in result.nodes]
        assert "" not in node_ids
        assert "valid" in node_ids

    def test_source_node_unknown_layer_defaults_raw(self, tmp_path):
        date = "2026-01-01"
        pipeline_dir = tmp_path / "test" / date
        pipeline_dir.mkdir(parents=True)
        story = {
            "pipeline_name": "test",
            "pipeline_layer": "bronze",
            "completed_nodes": 1,
            "failed_nodes": 0,
            "duration": 1.0,
            "graph_data": {
                "nodes": [{"id": "ext_table", "type": "source"}],
                "edges": [],
            },
        }
        (pipeline_dir / "run.json").write_text(json.dumps(story))

        g = LineageGenerator(stories_path=str(tmp_path))
        result = g.generate(date=date)
        src_node = [n for n in result.nodes if n.id == "ext_table"][0]
        assert src_node.layer == "raw"

    def test_output_node_inherits_story_layer(self, tmp_path):
        date = "2026-01-01"
        pipeline_dir = tmp_path / "test" / date
        pipeline_dir.mkdir(parents=True)
        story = {
            "pipeline_name": "test",
            "pipeline_layer": "silver",
            "completed_nodes": 1,
            "failed_nodes": 0,
            "duration": 1.0,
            "graph_data": {
                "nodes": [{"id": "my_output", "type": "table"}],
                "edges": [],
            },
        }
        (pipeline_dir / "run.json").write_text(json.dumps(story))

        g = LineageGenerator(stories_path=str(tmp_path))
        result = g.generate(date=date)
        out_node = [n for n in result.nodes if n.id == "my_output"][0]
        assert out_node.layer == "silver"

    def test_edge_source_target_format(self, tmp_path):
        date = "2026-01-01"
        pipeline_dir = tmp_path / "test" / date
        pipeline_dir.mkdir(parents=True)
        story = {
            "pipeline_name": "test",
            "pipeline_layer": "bronze",
            "completed_nodes": 1,
            "failed_nodes": 0,
            "duration": 1.0,
            "graph_data": {
                "nodes": [
                    {"id": "a", "type": "table"},
                    {"id": "b", "type": "table"},
                ],
                "edges": [{"source": "a", "target": "b"}],  # alternative format
            },
        }
        (pipeline_dir / "run.json").write_text(json.dumps(story))

        g = LineageGenerator(stories_path=str(tmp_path))
        result = g.generate(date=date)
        assert len(result.edges) >= 1
        edge = result.edges[0]
        assert edge.from_node == "a"
        assert edge.to_node == "b"

    def test_duplicate_node_id_overwritten_by_table(self, tmp_path):
        date = "2026-01-01"
        pipeline_dir = tmp_path / "pipe1" / date
        pipeline_dir.mkdir(parents=True)
        story1 = {
            "pipeline_name": "pipe1",
            "pipeline_layer": "bronze",
            "completed_nodes": 1,
            "failed_nodes": 0,
            "duration": 1.0,
            "graph_data": {
                "nodes": [
                    {"id": "shared_table", "type": "source", "layer": "raw"},
                ],
                "edges": [],
            },
        }
        (pipeline_dir / "run.json").write_text(json.dumps(story1))

        pipe2_dir = tmp_path / "pipe2" / date
        pipe2_dir.mkdir(parents=True)
        story2 = {
            "pipeline_name": "pipe2",
            "pipeline_layer": "silver",
            "completed_nodes": 1,
            "failed_nodes": 0,
            "duration": 1.0,
            "graph_data": {
                "nodes": [
                    {"id": "shared_table", "type": "table"},  # overrides
                ],
                "edges": [],
            },
        }
        (pipe2_dir / "run.json").write_text(json.dumps(story2))

        g = LineageGenerator(stories_path=str(tmp_path))
        result = g.generate(date=date)
        shared = [n for n in result.nodes if n.id == "shared_table"]
        assert len(shared) == 1
        assert shared[0].layer == "silver"

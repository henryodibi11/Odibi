"""Tests for LineageGenerator (V2 Phase 4)."""

import json
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from odibi.story.lineage import (
    LayerInfo,
    LineageEdge,
    LineageGenerator,
    LineageNode,
    LineageResult,
)


@pytest.fixture
def sample_stories_dir():
    """Create a temporary directory with sample story JSON files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        today = datetime.now().strftime("%Y-%m-%d")

        bronze_dir = Path(tmpdir) / "bronze_pipeline" / today
        bronze_dir.mkdir(parents=True)

        bronze_story = {
            "pipeline_name": "bronze_pipeline",
            "pipeline_layer": "bronze",
            "completed_nodes": 3,
            "failed_nodes": 0,
            "duration": 45.2,
            "graph_data": {
                "nodes": [
                    {"id": "raw.equipment_logs", "type": "table", "layer": "raw"},
                    {"id": "bronze.equipment", "type": "table", "layer": "bronze"},
                ],
                "edges": [
                    {"from": "raw.equipment_logs", "to": "bronze.equipment"},
                ],
            },
        }
        (bronze_dir / "run_14-30-00.json").write_text(json.dumps(bronze_story))

        silver_dir = Path(tmpdir) / "silver_pipeline" / today
        silver_dir.mkdir(parents=True)

        silver_story = {
            "pipeline_name": "silver_pipeline",
            "pipeline_layer": "silver",
            "completed_nodes": 2,
            "failed_nodes": 0,
            "duration": 32.1,
            "graph_data": {
                "nodes": [
                    {"id": "bronze.equipment", "type": "table", "layer": "bronze"},
                    {"id": "silver.equipment_clean", "type": "table", "layer": "silver"},
                ],
                "edges": [
                    {"from": "bronze.equipment", "to": "silver.equipment_clean"},
                ],
            },
        }
        (silver_dir / "run_14-35-00.json").write_text(json.dumps(silver_story))

        gold_dir = Path(tmpdir) / "gold_pipeline" / today
        gold_dir.mkdir(parents=True)

        gold_story = {
            "pipeline_name": "gold_pipeline",
            "pipeline_layer": "gold",
            "completed_nodes": 1,
            "failed_nodes": 0,
            "duration": 28.5,
            "graph_data": {
                "nodes": [
                    {"id": "silver.equipment_clean", "type": "table", "layer": "silver"},
                    {"id": "gold.oee_fact", "type": "table", "layer": "gold"},
                ],
                "edges": [
                    {"from": "silver.equipment_clean", "to": "gold.oee_fact"},
                ],
            },
        }
        (gold_dir / "run_14-40-00.json").write_text(json.dumps(gold_story))

        semantic_dir = Path(tmpdir) / "oee_semantic" / today
        semantic_dir.mkdir(parents=True)

        semantic_story = {
            "name": "oee_semantic",
            "views_created": 2,
            "views_failed": 0,
            "duration": 5.3,
            "graph_data": {
                "nodes": [
                    {"id": "gold.oee_fact", "type": "table", "layer": "gold"},
                    {"id": "vw_oee_daily", "type": "view", "layer": "semantic"},
                    {"id": "vw_oee_monthly", "type": "view", "layer": "semantic"},
                ],
                "edges": [
                    {"from": "gold.oee_fact", "to": "vw_oee_daily"},
                    {"from": "gold.oee_fact", "to": "vw_oee_monthly"},
                ],
            },
        }
        (semantic_dir / "run_14-45-00.json").write_text(json.dumps(semantic_story))

        yield tmpdir


class TestLineageNode:
    """Test LineageNode dataclass."""

    def test_create_node(self):
        """Test creating a lineage node."""
        node = LineageNode(id="gold.fact", type="table", layer="gold")
        assert node.id == "gold.fact"
        assert node.type == "table"
        assert node.layer == "gold"

    def test_to_dict(self):
        """Test node serialization."""
        node = LineageNode(id="vw_test", type="view", layer="semantic")
        d = node.to_dict()
        assert d == {"id": "vw_test", "type": "view", "layer": "semantic"}


class TestLineageEdge:
    """Test LineageEdge dataclass."""

    def test_create_edge(self):
        """Test creating a lineage edge."""
        edge = LineageEdge(from_node="bronze.a", to_node="silver.b")
        assert edge.from_node == "bronze.a"
        assert edge.to_node == "silver.b"

    def test_to_dict(self):
        """Test edge serialization."""
        edge = LineageEdge(from_node="a", to_node="b")
        d = edge.to_dict()
        assert d == {"from": "a", "to": "b"}


class TestLayerInfo:
    """Test LayerInfo dataclass."""

    def test_create_layer_info(self):
        """Test creating layer info."""
        layer = LayerInfo(
            name="bronze_pipeline",
            story_path="bronze_pipeline/2025-01-02/run.html",
            status="success",
            duration=45.2,
            pipeline_layer="bronze",
        )
        assert layer.name == "bronze_pipeline"
        assert layer.status == "success"
        assert layer.duration == 45.2
        assert layer.pipeline_layer == "bronze"


class TestLineageResult:
    """Test LineageResult dataclass."""

    def test_create_result(self):
        """Test creating a lineage result."""
        result = LineageResult(
            generated_at="2025-01-02T15:00:00",
            date="2025-01-02",
            layers=[
                LayerInfo(
                    name="bronze",
                    story_path="bronze/run.html",
                    status="success",
                    duration=10.0,
                )
            ],
            nodes=[LineageNode(id="a", type="table", layer="bronze")],
            edges=[LineageEdge(from_node="a", to_node="b")],
        )
        assert result.date == "2025-01-02"
        assert len(result.layers) == 1
        assert len(result.nodes) == 1
        assert len(result.edges) == 1

    def test_to_dict(self):
        """Test result serialization."""
        result = LineageResult(
            generated_at="2025-01-02T15:00:00",
            date="2025-01-02",
            layers=[],
            nodes=[LineageNode(id="a", type="table", layer="bronze")],
            edges=[LineageEdge(from_node="a", to_node="b")],
        )
        d = result.to_dict()
        assert d["date"] == "2025-01-02"
        assert len(d["nodes"]) == 1
        assert len(d["edges"]) == 1


class TestLineageGenerator:
    """Test LineageGenerator class."""

    def test_init(self, sample_stories_dir):
        """Test initializing lineage generator."""
        gen = LineageGenerator(stories_path=sample_stories_dir)
        assert gen.stories_path == sample_stories_dir
        assert not gen.is_remote

    def test_generate_stitches_multiple_stories(self, sample_stories_dir):
        """Test that generate() stitches graph_data from multiple stories."""
        gen = LineageGenerator(stories_path=sample_stories_dir)
        result = gen.generate()

        assert result is not None
        assert len(result.layers) == 4

        node_ids = {n.id for n in result.nodes}
        assert "raw.equipment_logs" in node_ids
        assert "bronze.equipment" in node_ids
        assert "silver.equipment_clean" in node_ids
        assert "gold.oee_fact" in node_ids
        assert "vw_oee_daily" in node_ids
        assert "vw_oee_monthly" in node_ids

    def test_generate_deduplicates_nodes(self, sample_stories_dir):
        """Test that nodes shared between stories are not duplicated."""
        gen = LineageGenerator(stories_path=sample_stories_dir)
        result = gen.generate()

        node_ids = [n.id for n in result.nodes]
        assert node_ids.count("bronze.equipment") == 1
        assert node_ids.count("silver.equipment_clean") == 1
        assert node_ids.count("gold.oee_fact") == 1

    def test_generate_deduplicates_edges(self, sample_stories_dir):
        """Test that duplicate edges are not included."""
        gen = LineageGenerator(stories_path=sample_stories_dir)
        result = gen.generate()

        edge_tuples = [(e.from_node, e.to_node) for e in result.edges]
        assert len(edge_tuples) == len(set(edge_tuples))

    def test_generate_captures_layer_info(self, sample_stories_dir):
        """Test that layer info is captured from stories."""
        gen = LineageGenerator(stories_path=sample_stories_dir)
        result = gen.generate()

        layer_names = [layer.name for layer in result.layers]
        assert "bronze_pipeline" in layer_names
        assert "silver_pipeline" in layer_names
        assert "gold_pipeline" in layer_names
        assert "oee_semantic" in layer_names

    def test_generate_sorts_layers_by_order(self, sample_stories_dir):
        """Test that layers are sorted by medallion order."""
        gen = LineageGenerator(stories_path=sample_stories_dir)
        result = gen.generate()

        layer_names = [layer.pipeline_layer or layer.name for layer in result.layers]
        expected_order = ["bronze", "silver", "gold", "oee_semantic"]
        for i, expected in enumerate(expected_order):
            assert expected in layer_names[i] or layer_names[i] == expected

    def test_render_json(self, sample_stories_dir):
        """Test JSON rendering."""
        gen = LineageGenerator(stories_path=sample_stories_dir)
        result = gen.generate()

        json_str = gen.render_json(result)
        data = json.loads(json_str)

        assert "date" in data
        assert "nodes" in data
        assert "edges" in data
        assert "layers" in data

    def test_render_html(self, sample_stories_dir):
        """Test HTML rendering with Mermaid diagram."""
        gen = LineageGenerator(stories_path=sample_stories_dir)
        result = gen.generate()

        html = gen.render_html(result)

        assert "<!DOCTYPE html>" in html
        assert "Data Lineage" in html
        assert "mermaid" in html
        assert "graph LR" in html

    def test_save_creates_files(self, sample_stories_dir):
        """Test that save() creates JSON and HTML files."""
        gen = LineageGenerator(stories_path=sample_stories_dir)
        result = gen.generate()

        paths = gen.save(result)

        assert "json" in paths
        assert "html" in paths
        assert Path(paths["json"]).exists()
        assert Path(paths["html"]).exists()

    def test_save_updates_result_paths(self, sample_stories_dir):
        """Test that save() updates the result with file paths."""
        gen = LineageGenerator(stories_path=sample_stories_dir)
        result = gen.generate()

        gen.save(result)

        assert result.json_path is not None
        assert result.html_path is not None

    def test_generate_with_specific_date(self, sample_stories_dir):
        """Test generating lineage for a specific date."""
        gen = LineageGenerator(stories_path=sample_stories_dir)
        today = datetime.now().strftime("%Y-%m-%d")

        result = gen.generate(date=today)

        assert result.date == today
        assert len(result.layers) > 0

    def test_generate_empty_for_missing_date(self, sample_stories_dir):
        """Test that generate() returns empty result for date with no stories."""
        gen = LineageGenerator(stories_path=sample_stories_dir)

        result = gen.generate(date="1999-01-01")

        assert result.date == "1999-01-01"
        assert len(result.layers) == 0
        assert len(result.nodes) == 0
        assert len(result.edges) == 0

    def test_infer_layer_from_node_id(self, sample_stories_dir):
        """Test layer inference from node ID."""
        gen = LineageGenerator(stories_path=sample_stories_dir)

        assert gen._infer_layer("raw.data") == "raw"
        assert gen._infer_layer("bronze.table") == "bronze"
        assert gen._infer_layer("silver.clean") == "silver"
        assert gen._infer_layer("gold.fact") == "gold"
        assert gen._infer_layer("vw_daily") == "semantic"
        assert gen._infer_layer("unknown_table") == "unknown"

    def test_sanitize_id_for_mermaid(self, sample_stories_dir):
        """Test ID sanitization for Mermaid compatibility."""
        gen = LineageGenerator(stories_path=sample_stories_dir)

        assert gen._sanitize_id("gold.oee_fact") == "gold_oee_fact"
        assert gen._sanitize_id("my-table") == "my_table"
        assert gen._sanitize_id("table with spaces") == "table_with_spaces"

    def test_result_property(self, sample_stories_dir):
        """Test that result property returns last generated result."""
        gen = LineageGenerator(stories_path=sample_stories_dir)

        assert gen.result is None

        gen.generate()

        assert gen.result is not None
        assert len(gen.result.nodes) > 0


class TestLineageGeneratorWithCustomWriteFile:
    """Test LineageGenerator with custom write_file callable."""

    def test_save_with_custom_write_file(self, sample_stories_dir):
        """Test save() with custom write_file callable."""
        gen = LineageGenerator(stories_path=sample_stories_dir)
        result = gen.generate()

        written_files = {}

        def write_file(path, content):
            written_files[path] = content

        gen.save(result, write_file=write_file)

        assert len(written_files) == 2
        assert any(".json" in p for p in written_files)
        assert any(".html" in p for p in written_files)


class TestLineageGeneratorEdgeCases:
    """Test edge cases for LineageGenerator."""

    def test_empty_stories_directory(self):
        """Test behavior with empty stories directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            gen = LineageGenerator(stories_path=tmpdir)
            result = gen.generate()

            assert len(result.layers) == 0
            assert len(result.nodes) == 0
            assert len(result.edges) == 0

    def test_nonexistent_stories_directory(self):
        """Test behavior with non-existent directory."""
        gen = LineageGenerator(stories_path="/nonexistent/path")
        result = gen.generate()

        assert len(result.layers) == 0

    def test_story_without_graph_data(self):
        """Test handling stories without graph_data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            today = datetime.now().strftime("%Y-%m-%d")
            pipeline_dir = Path(tmpdir) / "pipeline" / today
            pipeline_dir.mkdir(parents=True)

            story = {
                "pipeline_name": "test",
                "completed_nodes": 1,
                "failed_nodes": 0,
                "duration": 1.0,
            }
            (pipeline_dir / "run.json").write_text(json.dumps(story))

            gen = LineageGenerator(stories_path=tmpdir)
            result = gen.generate()

            assert len(result.layers) == 1
            assert len(result.nodes) == 0

    def test_failed_pipeline_status(self):
        """Test that failed pipeline status is captured."""
        with tempfile.TemporaryDirectory() as tmpdir:
            today = datetime.now().strftime("%Y-%m-%d")
            pipeline_dir = Path(tmpdir) / "pipeline" / today
            pipeline_dir.mkdir(parents=True)

            story = {
                "pipeline_name": "test",
                "completed_nodes": 2,
                "failed_nodes": 1,
                "duration": 10.0,
                "graph_data": {"nodes": [], "edges": []},
            }
            (pipeline_dir / "run.json").write_text(json.dumps(story))

            gen = LineageGenerator(stories_path=tmpdir)
            result = gen.generate()

            assert result.layers[0].status == "failed"

"""Tests for story generator graph data building, especially cross-pipeline dependencies."""

import pytest

from odibi.story.generator import StoryGenerator
from odibi.story.metadata import NodeExecutionMetadata, PipelineStoryMetadata


class TestBuildGraphDataCrossPipeline:
    """Tests for _build_graph_data cross-pipeline dependency detection."""

    @pytest.fixture
    def generator(self, tmp_path):
        """Create a generator for testing."""
        return StoryGenerator(
            pipeline_name="silver_pipeline",
            output_path=str(tmp_path),
        )

    @pytest.fixture
    def metadata_with_inputs(self):
        """Create metadata with nodes that have inputs referencing other pipelines."""
        metadata = PipelineStoryMetadata(
            pipeline_name="silver_pipeline",
            run_id="test-run-123",
            started_at=None,
            completed_at=None,
            duration=10.0,
            total_nodes=2,
            completed_nodes=2,
            failed_nodes=0,
            skipped_nodes=0,
        )

        # Node that references bronze pipeline via inputs
        node_with_inputs = NodeExecutionMetadata(
            node_name="process_events",
            operation="transform",
            status="success",
            duration=5.0,
            config_snapshot={
                "name": "process_events",
                "inputs": {
                    "events": "$bronze_pipeline.raw_events",
                    "calendar": "$bronze_pipeline.calendar_data",
                },
                "transform": {"steps": []},
            },
        )

        # Node that uses depends_on (intra-pipeline)
        node_with_depends = NodeExecutionMetadata(
            node_name="aggregate_events",
            operation="transform",
            status="success",
            duration=3.0,
            config_snapshot={
                "name": "aggregate_events",
                "depends_on": ["process_events"],
                "transform": {"steps": []},
            },
        )

        metadata.nodes = [node_with_inputs, node_with_depends]
        return metadata

    @pytest.fixture
    def metadata_no_inputs(self):
        """Create metadata with nodes that don't use inputs."""
        metadata = PipelineStoryMetadata(
            pipeline_name="simple_pipeline",
            run_id="test-run-456",
            started_at=None,
            completed_at=None,
            duration=5.0,
            total_nodes=2,
            completed_nodes=2,
            failed_nodes=0,
            skipped_nodes=0,
        )

        node1 = NodeExecutionMetadata(
            node_name="load_data",
            operation="read",
            status="success",
            duration=2.0,
            config_snapshot={
                "name": "load_data",
                "read": {"path": "/data/input.csv"},
            },
        )

        node2 = NodeExecutionMetadata(
            node_name="transform_data",
            operation="transform",
            status="success",
            duration=3.0,
            config_snapshot={
                "name": "transform_data",
                "depends_on": ["load_data"],
                "transform": {"steps": []},
            },
        )

        metadata.nodes = [node1, node2]
        return metadata

    def test_fallback_path_detects_cross_pipeline_inputs(self, generator, metadata_with_inputs):
        """Fallback path should detect cross-pipeline dependencies from inputs block."""
        # No graph_data or config provided - triggers fallback path
        result = generator._build_graph_data(
            metadata=metadata_with_inputs,
            graph_data=None,
            config=None,
        )

        # Should have 4 nodes: 2 internal + 2 external (raw_events, calendar_data)
        assert len(result["nodes"]) == 4

        # Find external nodes
        external_nodes = [n for n in result["nodes"] if n.get("is_external")]
        assert len(external_nodes) == 2

        external_ids = {n["id"] for n in external_nodes}
        assert "raw_events" in external_ids
        assert "calendar_data" in external_ids

        # Check external nodes have pipeline.node format labels
        external_labels = {n["label"] for n in external_nodes}
        assert "bronze_pipeline.raw_events" in external_labels
        assert "bronze_pipeline.calendar_data" in external_labels

        # Check source_pipeline is set
        for ext_node in external_nodes:
            assert ext_node["source_pipeline"] == "bronze_pipeline"

        # Check edges include cross-pipeline references
        edge_sources = {e["source"] for e in result["edges"]}
        assert "raw_events" in edge_sources
        assert "calendar_data" in edge_sources

        # Check dependencies are populated for process_events node
        process_node = next(n for n in result["nodes"] if n["id"] == "process_events")
        assert "bronze_pipeline.raw_events" in process_node["dependencies"]
        assert "bronze_pipeline.calendar_data" in process_node["dependencies"]

    def test_fallback_path_handles_depends_on(self, generator, metadata_with_inputs):
        """Fallback path should handle depends_on for intra-pipeline edges."""
        result = generator._build_graph_data(
            metadata=metadata_with_inputs,
            graph_data=None,
            config=None,
        )

        # Check that aggregate_events depends on process_events
        edges = result["edges"]
        intra_pipeline_edge = next(
            (
                e
                for e in edges
                if e["source"] == "process_events" and e["target"] == "aggregate_events"
            ),
            None,
        )
        assert intra_pipeline_edge is not None

    def test_fallback_path_no_inputs(self, generator, metadata_no_inputs):
        """Fallback path should work when no inputs block is present."""
        result = generator._build_graph_data(
            metadata=metadata_no_inputs,
            graph_data=None,
            config=None,
        )

        # Should have 2 nodes only (no external dependencies)
        assert len(result["nodes"]) == 2

        # No external nodes
        external_nodes = [n for n in result["nodes"] if n.get("is_external")]
        assert len(external_nodes) == 0

        # Check internal edge
        edges = result["edges"]
        assert any(e["source"] == "load_data" and e["target"] == "transform_data" for e in edges)

    def test_config_path_detects_cross_pipeline_inputs(self, generator):
        """Config path should detect cross-pipeline dependencies from inputs block."""
        metadata = PipelineStoryMetadata(
            pipeline_name="silver_pipeline",
            run_id="test-run",
            started_at=None,
            completed_at=None,
            duration=5.0,
            total_nodes=1,
            completed_nodes=1,
            failed_nodes=0,
            skipped_nodes=0,
        )
        metadata.nodes = [
            NodeExecutionMetadata(
                node_name="process",
                operation="transform",
                status="success",
                duration=1.0,
            )
        ]

        config = {
            "nodes": [
                {
                    "name": "process",
                    "inputs": {
                        "data": "$bronze.raw_data",
                    },
                }
            ]
        }

        result = generator._build_graph_data(
            metadata=metadata,
            graph_data=None,
            config=config,
        )

        # Should have 2 nodes: 1 internal + 1 external (raw_data)
        assert len(result["nodes"]) == 2

        external_nodes = [n for n in result["nodes"] if n.get("is_external")]
        assert len(external_nodes) == 1
        assert external_nodes[0]["id"] == "raw_data"

    def test_external_node_status_is_external(self, generator, metadata_with_inputs):
        """External nodes should have status 'external'."""
        result = generator._build_graph_data(
            metadata=metadata_with_inputs,
            graph_data=None,
            config=None,
        )

        external_nodes = [n for n in result["nodes"] if n.get("is_external")]
        for node in external_nodes:
            assert node["status"] == "external"

    def test_input_with_no_dot_reference(self, generator):
        """Input references without dots should still create edges."""
        metadata = PipelineStoryMetadata(
            pipeline_name="test",
            run_id="test",
            started_at=None,
            completed_at=None,
            duration=1.0,
            total_nodes=1,
            completed_nodes=1,
            failed_nodes=0,
            skipped_nodes=0,
        )
        metadata.nodes = [
            NodeExecutionMetadata(
                node_name="process",
                operation="transform",
                status="success",
                duration=1.0,
                config_snapshot={
                    "name": "process",
                    "inputs": {
                        "data": "$upstream_node",  # No dot - same pipeline ref
                    },
                },
            )
        ]

        result = generator._build_graph_data(
            metadata=metadata,
            graph_data=None,
            config=None,
        )

        # Should create edge from upstream_node to process
        edges = result["edges"]
        assert any(e["source"] == "upstream_node" and e["target"] == "process" for e in edges)

        # upstream_node should be external (not in current pipeline)
        external_nodes = [n for n in result["nodes"] if n.get("is_external")]
        assert len(external_nodes) == 1
        assert external_nodes[0]["id"] == "upstream_node"

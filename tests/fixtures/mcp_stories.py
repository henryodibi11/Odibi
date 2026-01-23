# tests/fixtures/mcp_stories.py
"""Mock story fixtures for MCP integration tests."""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any

from odibi_mcp.contracts.schema import SchemaResponse, ColumnSpec
from odibi_mcp.contracts.graph import GraphData, GraphNode, GraphEdge


@dataclass
class MockNodeInfo:
    """Mock node information."""

    name: str
    operation: str
    status: str
    duration: float
    validations: List[str]
    schema: Optional[SchemaResponse] = None


@dataclass
class MockStory:
    """Mock pipeline story for testing."""

    pipeline: str
    project: str
    run_id: str
    status: str
    duration_seconds: float
    nodes: List[MockNodeInfo]
    schema_changes: List[Any]
    graph_data: Optional[GraphData]
    row_count: int = 0

    def node_info(self, node: str) -> Dict[str, Any]:
        """Get info for a specific node."""
        for n in self.nodes:
            if n.name == node:
                return {
                    "operation": n.operation,
                    "validations": n.validations,
                    "schema": n.schema,
                    "duration": n.duration,
                }
        raise KeyError(f"Node {node} not found")


def create_mock_story(
    pipeline: str = "test_pipeline",
    project: str = "test_project",
    status: str = "SUCCESS",
) -> MockStory:
    """Create a mock story for testing."""
    return MockStory(
        pipeline=pipeline,
        project=project,
        run_id="run_001",
        status=status,
        duration_seconds=10.5,
        nodes=[
            MockNodeInfo(
                name="source_node",
                operation="read",
                status="SUCCESS",
                duration=2.0,
                validations=[],
                schema=SchemaResponse(
                    columns=[
                        ColumnSpec(name="id", dtype="int", nullable=False),
                        ColumnSpec(name="name", dtype="string", nullable=True),
                    ],
                    row_count=100,
                    partition_columns=[],
                ),
            ),
            MockNodeInfo(
                name="transform_node",
                operation="transform",
                status="SUCCESS",
                duration=5.0,
                validations=["not_null", "unique"],
            ),
            MockNodeInfo(
                name="sink_node",
                operation="write",
                status="SUCCESS",
                duration=3.5,
                validations=[],
            ),
        ],
        schema_changes=[],
        graph_data=GraphData(
            nodes=[
                GraphNode(id="source_node", type="source", label="Source"),
                GraphNode(id="transform_node", type="transform", label="Transform"),
                GraphNode(id="sink_node", type="sink", label="Sink"),
            ],
            edges=[
                GraphEdge(from_node="source_node", to_node="transform_node"),
                GraphEdge(from_node="transform_node", to_node="sink_node"),
            ],
        ),
        row_count=100,
    )


class MockStoryLoader:
    """Mock story loader for testing."""

    def __init__(self, stories: Optional[Dict[str, MockStory]] = None):
        self.stories = stories or {}

    def load(self, pipeline: str, run_selector) -> MockStory:
        """Load a mock story."""
        if pipeline in self.stories:
            return self.stories[pipeline]
        return create_mock_story(pipeline=pipeline)

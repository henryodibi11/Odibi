# odibi_mcp/tools/lineage.py
"""Lineage-related MCP tools - wired to real pipeline config."""

import logging
from dataclasses import dataclass
from typing import List

from odibi_mcp.contracts.resources import ResourceRef
from odibi_mcp.contracts.graph import GraphData, GraphNode, GraphEdge
from odibi_mcp.context import get_project_context

logger = logging.getLogger(__name__)


@dataclass
class LineageResult:
    """Result of lineage query."""

    source: ResourceRef
    related: List[ResourceRef]
    depth: int


def _build_dag_from_pipeline(pipeline_config: dict) -> dict:
    """Build a DAG representation from pipeline config."""
    nodes = {}
    edges = []

    for node in pipeline_config.get("nodes", []):
        node_name = node.get("name")
        if not node_name:
            continue

        # Track node
        nodes[node_name] = {
            "name": node_name,
            "inputs": [],
            "outputs": [],
            "depends_on": node.get("depends_on", []),
        }

        # Extract inputs (connections to external sources)
        for input_name, input_config in node.get("inputs", {}).items():
            if isinstance(input_config, dict):
                nodes[node_name]["inputs"].append(
                    {
                        "name": input_name,
                        "connection": input_config.get("connection"),
                        "path": input_config.get("path"),
                    }
                )

        # Extract outputs
        for output_name, output_config in node.get("outputs", {}).items():
            if isinstance(output_config, dict):
                nodes[node_name]["outputs"].append(
                    {
                        "name": output_name,
                        "connection": output_config.get("connection"),
                        "path": output_config.get("path"),
                    }
                )

        # Build edges from depends_on
        for dep in node.get("depends_on", []):
            edges.append((dep, node_name))

    return {"nodes": nodes, "edges": edges}


def lineage_upstream(
    pipeline: str,
    node: str,
    depth: int = 3,
) -> LineageResult:
    """
    Get upstream lineage (data sources) for a node.

    Traces dependencies from the pipeline config.
    """
    ctx = get_project_context()
    if not ctx or ctx.is_exploration_mode():
        return LineageResult(
            source=ResourceRef(
                kind="node", logical_name=f"{pipeline}/{node}", connection="unknown"
            ),
            related=[],
            depth=depth,
        )

    pipeline_config = ctx.get_pipeline(pipeline)
    if not pipeline_config:
        return LineageResult(
            source=ResourceRef(
                kind="node", logical_name=f"{pipeline}/{node}", connection="unknown"
            ),
            related=[],
            depth=depth,
        )

    dag = _build_dag_from_pipeline(pipeline_config)

    # Find upstream nodes
    def get_upstream(node_name: str, current_depth: int) -> List[str]:
        if current_depth <= 0:
            return []

        upstream = []
        for dep in dag["nodes"].get(node_name, {}).get("depends_on", []):
            upstream.append(dep)
            upstream.extend(get_upstream(dep, current_depth - 1))
        return upstream

    upstream_nodes = list(set(get_upstream(node, depth)))

    # Build ResourceRef list
    related = []
    for up_node in upstream_nodes:
        node_info = dag["nodes"].get(up_node, {})

        # Add the node itself
        related.append(
            ResourceRef(
                kind="node",
                logical_name=f"{pipeline}/{up_node}",
                connection="pipeline",
            )
        )

        # Add its inputs (external sources)
        for inp in node_info.get("inputs", []):
            related.append(
                ResourceRef(
                    kind="file" if inp.get("path") else "table",
                    logical_name=inp.get("path", inp.get("name", "unknown")),
                    connection=inp.get("connection", "unknown"),
                )
            )

    # Also add direct inputs of the target node
    target_node_info = dag["nodes"].get(node, {})
    for inp in target_node_info.get("inputs", []):
        related.append(
            ResourceRef(
                kind="file" if inp.get("path") else "table",
                logical_name=inp.get("path", inp.get("name", "unknown")),
                connection=inp.get("connection", "unknown"),
            )
        )

    return LineageResult(
        source=ResourceRef(kind="node", logical_name=f"{pipeline}/{node}", connection="pipeline"),
        related=related,
        depth=depth,
    )


def lineage_downstream(
    pipeline: str,
    node: str,
    depth: int = 3,
) -> LineageResult:
    """
    Get downstream lineage (consumers) for a node.

    Finds nodes that depend on this node.
    """
    ctx = get_project_context()
    if not ctx or ctx.is_exploration_mode():
        return LineageResult(
            source=ResourceRef(
                kind="node", logical_name=f"{pipeline}/{node}", connection="unknown"
            ),
            related=[],
            depth=depth,
        )

    pipeline_config = ctx.get_pipeline(pipeline)
    if not pipeline_config:
        return LineageResult(
            source=ResourceRef(
                kind="node", logical_name=f"{pipeline}/{node}", connection="unknown"
            ),
            related=[],
            depth=depth,
        )

    dag = _build_dag_from_pipeline(pipeline_config)

    # Build reverse dependency map
    reverse_deps = {}
    for node_name, node_info in dag["nodes"].items():
        for dep in node_info.get("depends_on", []):
            if dep not in reverse_deps:
                reverse_deps[dep] = []
            reverse_deps[dep].append(node_name)

    # Find downstream nodes
    def get_downstream(node_name: str, current_depth: int) -> List[str]:
        if current_depth <= 0:
            return []

        downstream = []
        for consumer in reverse_deps.get(node_name, []):
            downstream.append(consumer)
            downstream.extend(get_downstream(consumer, current_depth - 1))
        return downstream

    downstream_nodes = list(set(get_downstream(node, depth)))

    # Build ResourceRef list
    related = []
    for down_node in downstream_nodes:
        node_info = dag["nodes"].get(down_node, {})

        related.append(
            ResourceRef(
                kind="node",
                logical_name=f"{pipeline}/{down_node}",
                connection="pipeline",
            )
        )

        # Add outputs (where data goes)
        for out in node_info.get("outputs", []):
            related.append(
                ResourceRef(
                    kind="file" if out.get("path") else "table",
                    logical_name=out.get("path", out.get("name", "unknown")),
                    connection=out.get("connection", "unknown"),
                )
            )

    return LineageResult(
        source=ResourceRef(kind="node", logical_name=f"{pipeline}/{node}", connection="pipeline"),
        related=related,
        depth=depth,
    )


def lineage_graph(
    pipeline: str,
    include_external: bool = False,
) -> GraphData:
    """
    Get full lineage graph for a pipeline.

    Returns nodes and edges for visualization.
    """
    ctx = get_project_context()
    if not ctx or ctx.is_exploration_mode():
        return GraphData(nodes=[], edges=[])

    pipeline_config = ctx.get_pipeline(pipeline)
    if not pipeline_config:
        return GraphData(nodes=[], edges=[])

    dag = _build_dag_from_pipeline(pipeline_config)

    graph_nodes = []
    graph_edges = []

    # Add pipeline nodes
    for node_name, node_info in dag["nodes"].items():
        graph_nodes.append(
            GraphNode(
                id=node_name,
                label=node_name,
                type="transform",
            )
        )

        # Add edges for dependencies
        for dep in node_info.get("depends_on", []):
            graph_edges.append(
                GraphEdge(
                    from_node=dep,
                    to_node=node_name,
                    edge_type="dependency",
                )
            )

        if include_external:
            # Add input sources
            for inp in node_info.get("inputs", []):
                source_id = (
                    f"src_{inp.get('connection')}_{inp.get('path', inp.get('name', 'unknown'))}"
                )
                source_id = source_id.replace("/", "_").replace(".", "_")

                graph_nodes.append(
                    GraphNode(
                        id=source_id,
                        label=inp.get("path", inp.get("name", "source")),
                        type="source",
                    )
                )

                graph_edges.append(
                    GraphEdge(
                        from_node=source_id,
                        to_node=node_name,
                        edge_type="data_flow",
                    )
                )

            # Add output sinks
            for out in node_info.get("outputs", []):
                sink_id = (
                    f"sink_{out.get('connection')}_{out.get('path', out.get('name', 'unknown'))}"
                )
                sink_id = sink_id.replace("/", "_").replace(".", "_")

                graph_nodes.append(
                    GraphNode(
                        id=sink_id,
                        label=out.get("path", out.get("name", "sink")),
                        type="sink",
                    )
                )

                graph_edges.append(
                    GraphEdge(
                        from_node=node_name,
                        to_node=sink_id,
                        edge_type="data_flow",
                    )
                )

    return GraphData(nodes=graph_nodes, edges=graph_edges)

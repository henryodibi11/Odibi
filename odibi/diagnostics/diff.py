"""
ODIBI Diff Tools
================

Compare nodes and runs to identify changes in logic, data, or performance.
"""

from typing import Dict, List, Optional
from dataclasses import dataclass

from odibi.story.metadata import NodeExecutionMetadata, PipelineStoryMetadata


@dataclass
class NodeDiffResult:
    """Difference between two node executions."""

    node_name: str

    # Status
    status_change: Optional[str] = None  # e.g. "success -> failed"

    # Data
    rows_change_diff: int = 0  # Difference in rows_change (drift)

    # Schema
    schema_change: bool = False

    # Logic
    sql_diff: bool = False
    sql_changes: List[str] = None

    # Versioning
    delta_version_change: Optional[str] = None  # "v1 -> v2"


def diff_nodes(node_a: NodeExecutionMetadata, node_b: NodeExecutionMetadata) -> NodeDiffResult:
    """
    Compare two executions of the same node.

    Args:
        node_a: Baseline execution
        node_b: Current execution

    Returns:
        NodeDiffResult
    """
    result = NodeDiffResult(node_name=node_a.node_name)

    # Status check
    if node_a.status != node_b.status:
        result.status_change = f"{node_a.status} -> {node_b.status}"

    # Data check
    rows_a = node_a.rows_out or 0
    rows_b = node_b.rows_out or 0
    result.rows_change_diff = rows_b - rows_a

    # Schema check
    schema_a = set(node_a.schema_out or [])
    schema_b = set(node_b.schema_out or [])
    if schema_a != schema_b:
        result.schema_change = True

    # SQL Logic check
    if node_a.executed_sql != node_b.executed_sql:
        result.sql_diff = True
        # Simple text diff could be added here

    # Delta Version check
    ver_a = node_a.delta_info.version if node_a.delta_info else "N/A"
    ver_b = node_b.delta_info.version if node_b.delta_info else "N/A"

    if ver_a != ver_b:
        result.delta_version_change = f"v{ver_a} -> v{ver_b}"

    return result


def diff_runs(
    run_a: PipelineStoryMetadata, run_b: PipelineStoryMetadata
) -> Dict[str, NodeDiffResult]:
    """
    Compare two pipeline runs node by node.

    Args:
        run_a: Baseline run
        run_b: Current run

    Returns:
        Dictionary of node_name -> NodeDiffResult
    """
    results = {}

    # Index nodes by name
    nodes_a = {n.node_name: n for n in run_a.nodes}
    nodes_b = {n.node_name: n for n in run_b.nodes}

    all_nodes = set(nodes_a.keys()) | set(nodes_b.keys())

    for name in all_nodes:
        if name in nodes_a and name in nodes_b:
            results[name] = diff_nodes(nodes_a[name], nodes_b[name])
        # Handle missing nodes (added/removed) logic if needed

    return results

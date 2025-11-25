"""
ODIBI Diff Tools
================

Compare nodes and runs to identify changes in logic, data, or performance.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from odibi.story.metadata import NodeExecutionMetadata, PipelineStoryMetadata


@dataclass
class NodeDiffResult:
    """Difference between two node executions."""

    node_name: str

    # Status
    status_change: Optional[str] = None  # e.g. "success -> failed"

    # Data
    rows_out_a: int = 0
    rows_out_b: int = 0
    rows_diff: int = 0  # b - a

    # Schema
    schema_change: bool = False
    columns_added: List[str] = field(default_factory=list)
    columns_removed: List[str] = field(default_factory=list)

    # Logic
    sql_changed: bool = False
    config_changed: bool = False
    transformation_changed: bool = False

    # Versioning
    delta_version_change: Optional[str] = None  # "v1 -> v2"

    @property
    def has_drift(self) -> bool:
        """Check if any significant drift occurred."""
        return (
            self.status_change is not None
            or self.schema_change
            or self.sql_changed
            or self.config_changed
            or self.transformation_changed
        )


@dataclass
class RunDiffResult:
    """Difference between two pipeline runs."""

    run_id_a: str
    run_id_b: str

    node_diffs: Dict[str, NodeDiffResult] = field(default_factory=dict)
    nodes_added: List[str] = field(default_factory=list)
    nodes_removed: List[str] = field(default_factory=list)

    # Impact Analysis
    drift_source_nodes: List[str] = field(default_factory=list)
    impacted_downstream_nodes: List[str] = field(default_factory=list)


def diff_nodes(node_a: NodeExecutionMetadata, node_b: NodeExecutionMetadata) -> NodeDiffResult:
    """
    Compare two executions of the same node.

    Args:
        node_a: Baseline execution (Run A)
        node_b: Current execution (Run B)

    Returns:
        NodeDiffResult
    """
    result = NodeDiffResult(
        node_name=node_a.node_name, rows_out_a=node_a.rows_out or 0, rows_out_b=node_b.rows_out or 0
    )

    result.rows_diff = result.rows_out_b - result.rows_out_a

    # Status check
    if node_a.status != node_b.status:
        result.status_change = f"{node_a.status} -> {node_b.status}"

    # Schema check
    schema_a = set(node_a.schema_out or [])
    schema_b = set(node_b.schema_out or [])

    if schema_a != schema_b:
        result.schema_change = True
        result.columns_added = list(schema_b - schema_a)
        result.columns_removed = list(schema_a - schema_b)

    # Logic check (SQL)
    # Prefer Hash comparison if available
    if node_a.sql_hash and node_b.sql_hash:
        if node_a.sql_hash != node_b.sql_hash:
            result.sql_changed = True
    elif node_a.executed_sql != node_b.executed_sql:
        # Fallback to list comparison
        result.sql_changed = True

    # Transformation Stack Check
    if node_a.transformation_stack != node_b.transformation_stack:
        result.transformation_changed = True

    # Config check
    # Note: dict comparison handles order if python >= 3.7
    if node_a.config_snapshot and node_b.config_snapshot:
        # Deep compare
        # We might want to exclude timestamps or dynamic fields if they leak into config
        if node_a.config_snapshot != node_b.config_snapshot:
            result.config_changed = True

    # Delta Version check
    ver_a = node_a.delta_info.version if node_a.delta_info else None
    ver_b = node_b.delta_info.version if node_b.delta_info else None

    if ver_a is not None and ver_b is not None and ver_a != ver_b:
        result.delta_version_change = f"v{ver_a} -> v{ver_b}"

    return result


def diff_runs(run_a: PipelineStoryMetadata, run_b: PipelineStoryMetadata) -> RunDiffResult:
    """
    Compare two pipeline runs node by node.

    Args:
        run_a: Baseline run (Previous)
        run_b: Current run (New)

    Returns:
        RunDiffResult
    """
    result = RunDiffResult(
        run_id_a=getattr(run_a, "run_id", "unknown"), run_id_b=getattr(run_b, "run_id", "unknown")
    )

    # Index nodes by name
    nodes_a = {n.node_name: n for n in run_a.nodes}
    nodes_b = {n.node_name: n for n in run_b.nodes}

    set_a = set(nodes_a.keys())
    set_b = set(nodes_b.keys())

    result.nodes_added = list(set_b - set_a)
    result.nodes_removed = list(set_a - set_b)

    common_nodes = set_a.intersection(set_b)

    for name in common_nodes:
        diff = diff_nodes(nodes_a[name], nodes_b[name])
        result.node_diffs[name] = diff

        if diff.has_drift or diff.sql_changed or diff.config_changed:
            # logic change implies source of drift
            if diff.sql_changed or diff.config_changed:
                result.drift_source_nodes.append(name)
            else:
                # just data drift/impact
                result.impacted_downstream_nodes.append(name)

    return result

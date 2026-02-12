"""
Unit tests for odibi/diagnostics/diff.py
"""

import pytest

from odibi.diagnostics.diff import (
    NodeDiffResult,
    RunDiffResult,
    diff_nodes,
    diff_runs,
)
from odibi.story.metadata import (
    DeltaWriteInfo,
    NodeExecutionMetadata,
    PipelineStoryMetadata,
)


class TestNodeDiffResult:
    """Tests for NodeDiffResult dataclass."""

    def test_node_diff_result_structure(self):
        """Test that NodeDiffResult can be instantiated with required fields."""
        result = NodeDiffResult(
            node_name="test_node",
            status_change="success -> failed",
            rows_out_a=100,
            rows_out_b=150,
            rows_diff=50,
        )

        assert result.node_name == "test_node"
        assert result.status_change == "success -> failed"
        assert result.rows_out_a == 100
        assert result.rows_out_b == 150
        assert result.rows_diff == 50

    def test_has_drift_property_no_drift(self):
        """Test has_drift property returns False when no drift."""
        result = NodeDiffResult(node_name="test_node")

        assert result.has_drift is False

    def test_has_drift_property_status_change(self):
        """Test has_drift property returns True for status change."""
        result = NodeDiffResult(node_name="test_node", status_change="success -> failed")

        assert result.has_drift is True

    def test_has_drift_property_schema_change(self):
        """Test has_drift property returns True for schema change."""
        result = NodeDiffResult(node_name="test_node", schema_change=True)

        assert result.has_drift is True

    def test_has_drift_property_sql_change(self):
        """Test has_drift property returns True for SQL change."""
        result = NodeDiffResult(node_name="test_node", sql_changed=True)

        assert result.has_drift is True

    def test_has_drift_property_config_change(self):
        """Test has_drift property returns True for config change."""
        result = NodeDiffResult(node_name="test_node", config_changed=True)

        assert result.has_drift is True

    def test_has_drift_property_transformation_change(self):
        """Test has_drift property returns True for transformation change."""
        result = NodeDiffResult(node_name="test_node", transformation_changed=True)

        assert result.has_drift is True


class TestDiffNodes:
    """Tests for diff_nodes function."""

    def test_diff_nodes_basic(self):
        """Test basic diff_nodes functionality with row count changes."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            rows_out=100,
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.5,
            rows_out=150,
        )

        result = diff_nodes(node_a, node_b)

        assert result.node_name == "test_node"
        assert result.rows_out_a == 100
        assert result.rows_out_b == 150
        assert result.rows_diff == 50
        assert result.status_change is None

    def test_diff_nodes_status_change(self):
        """Test that status changes are detected."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="failed",
            duration=0.5,
        )

        result = diff_nodes(node_a, node_b)

        assert result.status_change == "success -> failed"
        assert result.has_drift is True

    def test_diff_nodes_schema_changes(self):
        """Test that schema changes are detected."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            schema_out=["id", "name", "old_col"],
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            schema_out=["id", "name", "new_col"],
        )

        result = diff_nodes(node_a, node_b)

        assert result.schema_change is True
        assert "new_col" in result.columns_added
        assert "old_col" in result.columns_removed
        assert result.has_drift is True

    def test_diff_nodes_no_schema_change(self):
        """Test that no schema change is detected when schemas are identical."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            schema_out=["id", "name"],
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            schema_out=["id", "name"],
        )

        result = diff_nodes(node_a, node_b)

        assert result.schema_change is False
        assert len(result.columns_added) == 0
        assert len(result.columns_removed) == 0

    def test_diff_nodes_sql_change_with_hash(self):
        """Test that SQL changes are detected using hash comparison."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            sql_hash="abc123",
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            sql_hash="def456",
        )

        result = diff_nodes(node_a, node_b)

        assert result.sql_changed is True
        assert result.has_drift is True

    def test_diff_nodes_sql_change_fallback(self):
        """Test SQL change detection using list comparison when no hash."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            executed_sql=["SELECT * FROM table_a"],
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            executed_sql=["SELECT * FROM table_b"],
        )

        result = diff_nodes(node_a, node_b)

        assert result.sql_changed is True

    def test_diff_nodes_transformation_change(self):
        """Test that transformation stack changes are detected."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            transformation_stack=["transform_1", "transform_2"],
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            transformation_stack=["transform_1", "transform_3"],
        )

        result = diff_nodes(node_a, node_b)

        assert result.transformation_changed is True
        assert result.has_drift is True

    def test_diff_nodes_config_change(self):
        """Test that config changes are detected."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            config_snapshot={"param1": "value1", "param2": 10},
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            config_snapshot={"param1": "value2", "param2": 10},
        )

        result = diff_nodes(node_a, node_b)

        assert result.config_changed is True
        assert result.has_drift is True

    def test_diff_nodes_delta_version_change(self):
        """Test that Delta version changes are detected."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            delta_info=DeltaWriteInfo(version=1),
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            delta_info=DeltaWriteInfo(version=2),
        )

        result = diff_nodes(node_a, node_b)

        assert result.delta_version_change == "v1 -> v2"

    def test_diff_nodes_no_delta_version_when_missing(self):
        """Test that no Delta version change is reported when delta_info is None."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            delta_info=None,
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            delta_info=None,
        )

        result = diff_nodes(node_a, node_b)

        assert result.delta_version_change is None

    def test_diff_nodes_handles_none_rows_out(self):
        """Test that diff_nodes handles None values for rows_out."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            rows_out=None,
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            rows_out=None,
        )

        result = diff_nodes(node_a, node_b)

        assert result.rows_out_a == 0
        assert result.rows_out_b == 0
        assert result.rows_diff == 0


class TestRunDiffResult:
    """Tests for RunDiffResult dataclass."""

    def test_run_diff_result_structure(self):
        """Test that RunDiffResult can be instantiated."""
        result = RunDiffResult(
            run_id_a="run_123",
            run_id_b="run_456",
            nodes_added=["new_node"],
            nodes_removed=["old_node"],
        )

        assert result.run_id_a == "run_123"
        assert result.run_id_b == "run_456"
        assert "new_node" in result.nodes_added
        assert "old_node" in result.nodes_removed


class TestDiffRuns:
    """Tests for diff_runs function."""

    def test_diff_runs_basic(self):
        """Test basic diff_runs functionality."""
        node_a = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            rows_out=100,
        )

        node_b = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.5,
            rows_out=150,
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_a])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_b])

        # Add run_id attribute
        run_a.run_id = "run_123"
        run_b.run_id = "run_456"

        result = diff_runs(run_a, run_b)

        assert result.run_id_a == "run_123"
        assert result.run_id_b == "run_456"
        assert "node1" in result.node_diffs
        assert result.node_diffs["node1"].rows_diff == 50

    def test_diff_runs_added_nodes(self):
        """Test that added nodes are detected."""
        node_a = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
        )

        node_b1 = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
        )

        node_b2 = NodeExecutionMetadata(
            node_name="node2",
            operation="run",
            status="success",
            duration=1.0,
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_a])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_b1, node_b2])

        run_a.run_id = "run_123"
        run_b.run_id = "run_456"

        result = diff_runs(run_a, run_b)

        assert "node2" in result.nodes_added
        assert len(result.nodes_removed) == 0

    def test_diff_runs_removed_nodes(self):
        """Test that removed nodes are detected."""
        node_a1 = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
        )

        node_a2 = NodeExecutionMetadata(
            node_name="node2",
            operation="run",
            status="success",
            duration=1.0,
        )

        node_b = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_a1, node_a2])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_b])

        run_a.run_id = "run_123"
        run_b.run_id = "run_456"

        result = diff_runs(run_a, run_b)

        assert "node2" in result.nodes_removed
        assert len(result.nodes_added) == 0

    def test_diff_runs_drift_source_detection(self):
        """Test that drift source nodes are identified for SQL changes."""
        node_a = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            sql_hash="abc123",
        )

        node_b = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            sql_hash="def456",
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_a])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_b])

        run_a.run_id = "run_123"
        run_b.run_id = "run_456"

        result = diff_runs(run_a, run_b)

        assert "node1" in result.drift_source_nodes
        assert result.node_diffs["node1"].sql_changed is True

    def test_diff_runs_config_change_drift_source(self):
        """Test that config changes are identified as drift sources."""
        node_a = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            config_snapshot={"param": "value1"},
        )

        node_b = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            config_snapshot={"param": "value2"},
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_a])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_b])

        run_a.run_id = "run_123"
        run_b.run_id = "run_456"

        result = diff_runs(run_a, run_b)

        assert "node1" in result.drift_source_nodes

    def test_diff_runs_impacted_downstream_nodes(self):
        """Test that data drift without logic change is flagged as impacted downstream."""
        node_a = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            schema_out=["id", "name", "old_col"],
        )

        node_b = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            schema_out=["id", "name", "new_col"],
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_a])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_b])

        run_a.run_id = "run_123"
        run_b.run_id = "run_456"

        result = diff_runs(run_a, run_b)

        # Schema change without SQL/config change = impacted downstream
        assert "node1" in result.impacted_downstream_nodes

    def test_diff_runs_no_run_id_uses_unknown(self):
        """Test that diff_runs handles missing run_id gracefully."""
        node_a = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
        )

        node_b = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_a])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_b])

        # Don't set run_id - but note that PipelineStoryMetadata has a default factory
        # So run_id will be generated automatically. Let's test that the function works
        # even when run_id is dynamically generated.

        result = diff_runs(run_a, run_b)

        # The run_ids should be auto-generated timestamps, so just verify they exist
        assert result.run_id_a is not None
        assert result.run_id_b is not None
        assert len(result.run_id_a) > 0
        assert len(result.run_id_b) > 0

"""Unit tests for odibi.diagnostics.diff module."""

from odibi.diagnostics.diff import (
    NodeDiffResult,
    RunDiffResult,
    diff_nodes,
    diff_runs,
)
from odibi.story.metadata import NodeExecutionMetadata, PipelineStoryMetadata


class TestNodeDiffResult:
    """Test NodeDiffResult dataclass."""

    def test_initialization_defaults(self):
        """Test that NodeDiffResult can be initialized with defaults."""
        result = NodeDiffResult(node_name="test_node")

        assert result.node_name == "test_node"
        assert result.status_change is None
        assert result.rows_out_a == 0
        assert result.rows_out_b == 0
        assert result.rows_diff == 0
        assert result.schema_change is False
        assert result.columns_added == []
        assert result.columns_removed == []
        assert result.sql_changed is False
        assert result.config_changed is False
        assert result.transformation_changed is False
        assert result.delta_version_change is None

    def test_has_drift_no_changes(self):
        """Test that has_drift returns False when there are no changes."""
        result = NodeDiffResult(node_name="test_node")
        assert result.has_drift is False

    def test_has_drift_status_change(self):
        """Test that has_drift returns True when status changes."""
        result = NodeDiffResult(node_name="test_node", status_change="success -> failed")
        assert result.has_drift is True

    def test_has_drift_schema_change(self):
        """Test that has_drift returns True when schema changes."""
        result = NodeDiffResult(node_name="test_node", schema_change=True)
        assert result.has_drift is True

    def test_has_drift_sql_change(self):
        """Test that has_drift returns True when SQL changes."""
        result = NodeDiffResult(node_name="test_node", sql_changed=True)
        assert result.has_drift is True

    def test_has_drift_config_change(self):
        """Test that has_drift returns True when config changes."""
        result = NodeDiffResult(node_name="test_node", config_changed=True)
        assert result.has_drift is True

    def test_has_drift_transformation_change(self):
        """Test that has_drift returns True when transformation changes."""
        result = NodeDiffResult(node_name="test_node", transformation_changed=True)
        assert result.has_drift is True


class TestRunDiffResult:
    """Test RunDiffResult dataclass."""

    def test_initialization_defaults(self):
        """Test that RunDiffResult can be initialized with defaults."""
        result = RunDiffResult(run_id_a="run1", run_id_b="run2")

        assert result.run_id_a == "run1"
        assert result.run_id_b == "run2"
        assert result.node_diffs == {}
        assert result.nodes_added == []
        assert result.nodes_removed == []
        assert result.drift_source_nodes == []
        assert result.impacted_downstream_nodes == []


class TestDiffNodes:
    """Test diff_nodes function."""

    def test_diff_nodes_no_changes(self):
        """Test that diff_nodes detects no changes when nodes are identical."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            rows_out=100,
            schema_out=["id", "name"],
        )
        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            rows_out=100,
            schema_out=["id", "name"],
        )

        result = diff_nodes(node_a, node_b)

        assert result.node_name == "test_node"
        assert result.rows_out_a == 100
        assert result.rows_out_b == 100
        assert result.rows_diff == 0
        assert result.status_change is None
        assert result.schema_change is False
        assert result.has_drift is False

    def test_diff_nodes_status_change(self):
        """Test that diff_nodes detects status changes."""
        node_a = NodeExecutionMetadata(
            node_name="test_node", operation="run", status="success", duration=1.0
        )
        node_b = NodeExecutionMetadata(
            node_name="test_node", operation="run", status="failed", duration=1.0
        )

        result = diff_nodes(node_a, node_b)

        assert result.status_change == "success -> failed"
        assert result.has_drift is True

    def test_diff_nodes_row_count_change(self):
        """Test that diff_nodes detects row count changes."""
        node_a = NodeExecutionMetadata(
            node_name="test_node", operation="run", status="success", duration=1.0, rows_out=100
        )
        node_b = NodeExecutionMetadata(
            node_name="test_node", operation="run", status="success", duration=1.0, rows_out=150
        )

        result = diff_nodes(node_a, node_b)

        assert result.rows_out_a == 100
        assert result.rows_out_b == 150
        assert result.rows_diff == 50

    def test_diff_nodes_schema_added(self):
        """Test that diff_nodes detects schema additions."""
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
            schema_out=["id", "name", "email"],
        )

        result = diff_nodes(node_a, node_b)

        assert result.schema_change is True
        assert "email" in result.columns_added
        assert result.columns_removed == []
        assert result.has_drift is True

    def test_diff_nodes_schema_removed(self):
        """Test that diff_nodes detects schema removals."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            schema_out=["id", "name", "phone"],
        )
        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            schema_out=["id", "name"],
        )

        result = diff_nodes(node_a, node_b)

        assert result.schema_change is True
        assert result.columns_added == []
        assert "phone" in result.columns_removed
        assert result.has_drift is True

    def test_diff_nodes_sql_change_by_hash(self):
        """Test that diff_nodes detects SQL changes using hash."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            sql_hash="abc123",
            executed_sql=["SELECT * FROM table"],
        )
        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            sql_hash="def456",
            executed_sql=["SELECT * FROM table WHERE id > 10"],
        )

        result = diff_nodes(node_a, node_b)

        assert result.sql_changed is True
        assert result.has_drift is True

    def test_diff_nodes_sql_change_by_content(self):
        """Test that diff_nodes detects SQL changes using content when hash not available."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            executed_sql=["SELECT * FROM table"],
        )
        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            executed_sql=["SELECT * FROM table WHERE id > 10"],
        )

        result = diff_nodes(node_a, node_b)

        assert result.sql_changed is True
        assert result.has_drift is True

    def test_diff_nodes_transformation_change(self):
        """Test that diff_nodes detects transformation stack changes."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            transformation_stack=["filter", "join"],
        )
        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            transformation_stack=["filter", "join", "aggregate"],
        )

        result = diff_nodes(node_a, node_b)

        assert result.transformation_changed is True
        assert result.has_drift is True

    def test_diff_nodes_config_change(self):
        """Test that diff_nodes detects config changes."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            config_snapshot={"option1": "value1"},
        )
        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            config_snapshot={"option1": "value2"},
        )

        result = diff_nodes(node_a, node_b)

        assert result.config_changed is True
        assert result.has_drift is True

    def test_diff_nodes_delta_version_change(self):
        """Test that diff_nodes detects Delta version changes."""
        from dataclasses import dataclass

        @dataclass
        class DeltaInfo:
            version: int

        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            delta_info=DeltaInfo(version=1),
        )
        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            delta_info=DeltaInfo(version=2),
        )

        result = diff_nodes(node_a, node_b)

        assert result.delta_version_change == "v1 -> v2"

    def test_diff_nodes_null_rows(self):
        """Test that diff_nodes handles None values for rows_out."""
        node_a = NodeExecutionMetadata(
            node_name="test_node", operation="run", status="success", duration=1.0, rows_out=None
        )
        node_b = NodeExecutionMetadata(
            node_name="test_node", operation="run", status="success", duration=1.0, rows_out=None
        )

        result = diff_nodes(node_a, node_b)

        assert result.rows_out_a == 0
        assert result.rows_out_b == 0
        assert result.rows_diff == 0


class TestDiffRuns:
    """Test diff_runs function."""

    def test_diff_runs_no_changes(self):
        """Test that diff_runs detects no changes when runs are identical."""
        node_a = NodeExecutionMetadata(
            node_name="node1", operation="run", status="success", duration=1.0, rows_out=100
        )
        node_b = NodeExecutionMetadata(
            node_name="node1", operation="run", status="success", duration=1.0, rows_out=100
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_a])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_b])

        result = diff_runs(run_a, run_b)

        assert len(result.node_diffs) == 1
        assert "node1" in result.node_diffs
        assert result.nodes_added == []
        assert result.nodes_removed == []
        assert result.drift_source_nodes == []
        assert result.impacted_downstream_nodes == []

    def test_diff_runs_node_added(self):
        """Test that diff_runs detects added nodes."""
        node_a = NodeExecutionMetadata(
            node_name="node1", operation="run", status="success", duration=1.0
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_a])
        run_b = PipelineStoryMetadata(
            pipeline_name="test_pipeline",
            nodes=[
                node_a,
                NodeExecutionMetadata(
                    node_name="node2", operation="run", status="success", duration=1.0
                ),
            ],
        )

        result = diff_runs(run_a, run_b)

        assert "node2" in result.nodes_added
        assert result.nodes_removed == []

    def test_diff_runs_node_removed(self):
        """Test that diff_runs detects removed nodes."""
        node_a = NodeExecutionMetadata(
            node_name="node1", operation="run", status="success", duration=1.0
        )
        node_b = NodeExecutionMetadata(
            node_name="node2", operation="run", status="success", duration=1.0
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_a, node_b])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_a])

        result = diff_runs(run_a, run_b)

        assert result.nodes_added == []
        assert "node2" in result.nodes_removed

    def test_diff_runs_drift_source_sql_change(self):
        """Test that diff_runs identifies drift source nodes (SQL change)."""
        node_a1 = NodeExecutionMetadata(
            node_name="node1", operation="run", status="success", duration=1.0, sql_hash="abc123"
        )
        node_b1 = NodeExecutionMetadata(
            node_name="node1", operation="run", status="success", duration=1.0, sql_hash="def456"
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_a1])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_b1])

        result = diff_runs(run_a, run_b)

        assert "node1" in result.drift_source_nodes
        assert "node1" not in result.impacted_downstream_nodes

    def test_diff_runs_drift_source_config_change(self):
        """Test that diff_runs identifies drift source nodes (config change)."""
        node_a1 = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            config_snapshot={"key": "value1"},
        )
        node_b1 = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            config_snapshot={"key": "value2"},
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_a1])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_b1])

        result = diff_runs(run_a, run_b)

        assert "node1" in result.drift_source_nodes

    def test_diff_runs_impacted_downstream_data_drift(self):
        """Test that diff_runs identifies impacted downstream nodes (data drift only)."""
        node_a1 = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            rows_out=100,
            schema_out=["id", "name"],
            sql_hash="abc123",
        )
        node_b1 = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            rows_out=100,
            schema_out=["id", "name", "new_col"],  # Schema change but no SQL change
            sql_hash="abc123",
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_a1])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node_b1])

        result = diff_runs(run_a, run_b)

        # Schema change without SQL/config change => impacted downstream
        assert "node1" in result.impacted_downstream_nodes
        assert "node1" not in result.drift_source_nodes

    def test_diff_runs_multiple_nodes_mixed_changes(self):
        """Test that diff_runs correctly categorizes multiple nodes with different changes."""
        # Node1: SQL change (drift source)
        node_a1 = NodeExecutionMetadata(
            node_name="node1", operation="run", status="success", duration=1.0, sql_hash="abc123"
        )
        node_b1 = NodeExecutionMetadata(
            node_name="node1", operation="run", status="success", duration=1.0, sql_hash="def456"
        )

        # Node2: Schema change only (impacted downstream)
        node_a2 = NodeExecutionMetadata(
            node_name="node2",
            operation="run",
            status="success",
            duration=1.0,
            schema_out=["id", "name"],
            sql_hash="xyz789",
        )
        node_b2 = NodeExecutionMetadata(
            node_name="node2",
            operation="run",
            status="success",
            duration=1.0,
            schema_out=["id", "name", "email"],
            sql_hash="xyz789",
        )

        # Node3: No change
        node_a3 = NodeExecutionMetadata(
            node_name="node3", operation="run", status="success", duration=1.0, rows_out=100
        )
        node_b3 = NodeExecutionMetadata(
            node_name="node3", operation="run", status="success", duration=1.0, rows_out=100
        )

        run_a = PipelineStoryMetadata(
            pipeline_name="test_pipeline", nodes=[node_a1, node_a2, node_a3]
        )
        run_b = PipelineStoryMetadata(
            pipeline_name="test_pipeline", nodes=[node_b1, node_b2, node_b3]
        )

        result = diff_runs(run_a, run_b)

        assert "node1" in result.drift_source_nodes
        assert "node2" in result.impacted_downstream_nodes
        assert "node3" not in result.drift_source_nodes
        assert "node3" not in result.impacted_downstream_nodes

    def test_diff_runs_run_id_extraction(self):
        """Test that diff_runs correctly extracts run IDs."""
        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[])

        # Set run_id if available
        run_a.run_id = "run_2026_01_01"
        run_b.run_id = "run_2026_01_02"

        result = diff_runs(run_a, run_b)

        assert result.run_id_a == "run_2026_01_01"
        assert result.run_id_b == "run_2026_01_02"

    def test_diff_runs_missing_run_id(self):
        """Test that diff_runs handles missing run_id gracefully."""
        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[])

        result = diff_runs(run_a, run_b)

        # The run_id is auto-generated by PipelineStoryMetadata, so check that it exists
        assert result.run_id_a is not None
        assert result.run_id_b is not None

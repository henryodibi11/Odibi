"""
Unit tests for odibi.diagnostics.diff module.

Tests NodeDiffResult, RunDiffResult dataclasses,
diff_nodes, and diff_runs functions.
"""



from odibi.diagnostics.diff import NodeDiffResult, RunDiffResult, diff_nodes, diff_runs
from odibi.story.metadata import DeltaWriteInfo, NodeExecutionMetadata, PipelineStoryMetadata


class TestNodeDiffResult:
    """Tests for NodeDiffResult dataclass."""

    def test_nodediffresult_basic_fields(self):
        """Test NodeDiffResult instantiation with basic fields."""
        result = NodeDiffResult(
            node_name="test_node", rows_out_a=100, rows_out_b=150, rows_diff=50
        )

        assert result.node_name == "test_node"
        assert result.rows_out_a == 100
        assert result.rows_out_b == 150
        assert result.rows_diff == 50

    def test_nodediffresult_optional_fields(self):
        """Test NodeDiffResult with optional fields."""
        result = NodeDiffResult(
            node_name="test_node",
            status_change="success -> failed",
            schema_change=True,
            columns_added=["new_col"],
            columns_removed=["old_col"],
            sql_changed=True,
            config_changed=True,
            transformation_changed=True,
            delta_version_change="v1 -> v2",
        )

        assert result.status_change == "success -> failed"
        assert result.schema_change is True
        assert result.columns_added == ["new_col"]
        assert result.columns_removed == ["old_col"]
        assert result.sql_changed is True
        assert result.config_changed is True
        assert result.transformation_changed is True
        assert result.delta_version_change == "v1 -> v2"

    def test_nodediffresult_has_drift_property(self):
        """Test has_drift property returns True when drift is detected."""
        # No drift
        result = NodeDiffResult(node_name="test_node")
        assert result.has_drift is False

        # Status change
        result = NodeDiffResult(node_name="test_node", status_change="success -> failed")
        assert result.has_drift is True

        # Schema change
        result = NodeDiffResult(node_name="test_node", schema_change=True)
        assert result.has_drift is True

        # SQL change
        result = NodeDiffResult(node_name="test_node", sql_changed=True)
        assert result.has_drift is True

        # Config change
        result = NodeDiffResult(node_name="test_node", config_changed=True)
        assert result.has_drift is True

        # Transformation change
        result = NodeDiffResult(node_name="test_node", transformation_changed=True)
        assert result.has_drift is True


class TestRunDiffResult:
    """Tests for RunDiffResult dataclass."""

    def test_rundiffresult_basic_fields(self):
        """Test RunDiffResult instantiation with basic fields."""
        result = RunDiffResult(run_id_a="run_001", run_id_b="run_002")

        assert result.run_id_a == "run_001"
        assert result.run_id_b == "run_002"
        assert result.node_diffs == {}
        assert result.nodes_added == []
        assert result.nodes_removed == []
        assert result.drift_source_nodes == []
        assert result.impacted_downstream_nodes == []

    def test_rundiffresult_with_node_diffs(self):
        """Test RunDiffResult with node diffs."""
        node_diff = NodeDiffResult(node_name="node1", sql_changed=True)
        result = RunDiffResult(
            run_id_a="run_001",
            run_id_b="run_002",
            node_diffs={"node1": node_diff},
            nodes_added=["node2"],
            nodes_removed=["node3"],
            drift_source_nodes=["node1"],
            impacted_downstream_nodes=["node4"],
        )

        assert "node1" in result.node_diffs
        assert result.nodes_added == ["node2"]
        assert result.nodes_removed == ["node3"]
        assert result.drift_source_nodes == ["node1"]
        assert result.impacted_downstream_nodes == ["node4"]


class TestDiffNodes:
    """Tests for diff_nodes function."""

    def test_diff_nodes_no_changes(self):
        """Test diff_nodes when nodes are identical."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            rows_out=100,
            schema_out=["col1", "col2"],
            executed_sql=["SELECT * FROM table"],
            transformation_stack=["transform1"],
            config_snapshot={"key": "value"},
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.5,
            rows_out=100,
            schema_out=["col1", "col2"],
            executed_sql=["SELECT * FROM table"],
            transformation_stack=["transform1"],
            config_snapshot={"key": "value"},
        )

        result = diff_nodes(node_a, node_b)

        assert result.node_name == "test_node"
        assert result.rows_diff == 0
        assert result.status_change is None
        assert result.schema_change is False
        assert result.sql_changed is False
        assert result.config_changed is False
        assert result.transformation_changed is False

    def test_diff_nodes_status_change(self):
        """Test diff_nodes detects status changes."""
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
        """Test diff_nodes tracks row count changes."""
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

    def test_diff_nodes_schema_change(self):
        """Test diff_nodes detects schema changes."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            schema_out=["col1", "col2"],
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            schema_out=["col1", "col3"],
        )

        result = diff_nodes(node_a, node_b)

        assert result.schema_change is True
        assert "col3" in result.columns_added
        assert "col2" in result.columns_removed

    def test_diff_nodes_sql_change_by_hash(self):
        """Test diff_nodes detects SQL changes via hash comparison."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            sql_hash="hash1",
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            sql_hash="hash2",
        )

        result = diff_nodes(node_a, node_b)

        assert result.sql_changed is True

    def test_diff_nodes_sql_change_by_content(self):
        """Test diff_nodes detects SQL changes via content comparison."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            executed_sql=["SELECT * FROM table1"],
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            executed_sql=["SELECT * FROM table2"],
        )

        result = diff_nodes(node_a, node_b)

        assert result.sql_changed is True

    def test_diff_nodes_transformation_change(self):
        """Test diff_nodes detects transformation stack changes."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            transformation_stack=["transform1", "transform2"],
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            transformation_stack=["transform1", "transform3"],
        )

        result = diff_nodes(node_a, node_b)

        assert result.transformation_changed is True

    def test_diff_nodes_config_change(self):
        """Test diff_nodes detects config changes."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            config_snapshot={"key": "value1"},
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            config_snapshot={"key": "value2"},
        )

        result = diff_nodes(node_a, node_b)

        assert result.config_changed is True

    def test_diff_nodes_delta_version_change(self):
        """Test diff_nodes detects Delta version changes."""
        node_a = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            delta_info=DeltaWriteInfo(version=1, operation="WRITE"),
        )

        node_b = NodeExecutionMetadata(
            node_name="test_node",
            operation="run",
            status="success",
            duration=1.0,
            delta_info=DeltaWriteInfo(version=2, operation="WRITE"),
        )

        result = diff_nodes(node_a, node_b)

        assert result.delta_version_change == "v1 -> v2"


class TestDiffRuns:
    """Tests for diff_runs function."""

    def test_diff_runs_identical_runs(self):
        """Test diff_runs with identical runs."""
        node = NodeExecutionMetadata(
            node_name="node1", operation="run", status="success", duration=1.0
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node])

        result = diff_runs(run_a, run_b)

        assert len(result.node_diffs) == 1
        assert "node1" in result.node_diffs
        assert result.nodes_added == []
        assert result.nodes_removed == []

    def test_diff_runs_added_node(self):
        """Test diff_runs detects added nodes."""
        node1 = NodeExecutionMetadata(
            node_name="node1", operation="run", status="success", duration=1.0
        )
        node2 = NodeExecutionMetadata(
            node_name="node2", operation="run", status="success", duration=1.0
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node1])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node1, node2])

        result = diff_runs(run_a, run_b)

        assert "node2" in result.nodes_added
        assert result.nodes_removed == []

    def test_diff_runs_removed_node(self):
        """Test diff_runs detects removed nodes."""
        node1 = NodeExecutionMetadata(
            node_name="node1", operation="run", status="success", duration=1.0
        )
        node2 = NodeExecutionMetadata(
            node_name="node2", operation="run", status="success", duration=1.0
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node1, node2])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node1])

        result = diff_runs(run_a, run_b)

        assert result.nodes_added == []
        assert "node2" in result.nodes_removed

    def test_diff_runs_drift_source_detection(self):
        """Test diff_runs identifies drift source nodes (SQL/config changes)."""
        node1_a = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            executed_sql=["SELECT * FROM table1"],
        )

        node1_b = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            executed_sql=["SELECT * FROM table2"],
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node1_a])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node1_b])

        result = diff_runs(run_a, run_b)

        assert "node1" in result.drift_source_nodes
        assert "node1" not in result.impacted_downstream_nodes

    def test_diff_runs_impacted_downstream_detection(self):
        """Test diff_runs identifies impacted downstream nodes (data drift only)."""
        node1_a = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            rows_out=100,
            schema_out=["col1"],
        )

        node1_b = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            rows_out=100,
            schema_out=["col1", "col2"],  # Schema changed, but not SQL/config
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node1_a])
        run_b = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node1_b])

        result = diff_runs(run_a, run_b)

        # Schema change is drift, but not SQL/config change
        assert "node1" in result.impacted_downstream_nodes

    def test_diff_runs_multiple_nodes(self):
        """Test diff_runs with multiple nodes and various changes."""
        node1_a = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            executed_sql=["SELECT 1"],
        )
        node2_a = NodeExecutionMetadata(
            node_name="node2", operation="run", status="success", duration=1.0
        )

        node1_b = NodeExecutionMetadata(
            node_name="node1",
            operation="run",
            status="success",
            duration=1.0,
            executed_sql=["SELECT 2"],  # SQL changed
        )
        node2_b = NodeExecutionMetadata(
            node_name="node2",
            operation="run",
            status="success",
            duration=1.0,
            schema_out=["col1"],  # Schema changed
        )
        node3_b = NodeExecutionMetadata(
            node_name="node3", operation="run", status="success", duration=1.0
        )

        run_a = PipelineStoryMetadata(pipeline_name="test_pipeline", nodes=[node1_a, node2_a])
        run_b = PipelineStoryMetadata(
            pipeline_name="test_pipeline", nodes=[node1_b, node2_b, node3_b]
        )

        result = diff_runs(run_a, run_b)

        assert "node3" in result.nodes_added
        assert "node1" in result.drift_source_nodes  # SQL changed
        assert len(result.node_diffs) == 2  # node1 and node2

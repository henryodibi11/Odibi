"""Tests for story metadata tracking."""

from odibi.story.metadata import NodeExecutionMetadata, PipelineStoryMetadata


class TestNodeExecutionMetadata:
    """Tests for NodeExecutionMetadata class."""

    def test_create_basic_metadata(self):
        """Should create node metadata with basic info."""
        metadata = NodeExecutionMetadata(
            node_name="load_data", operation="read", status="success", duration=1.234
        )

        assert metadata.node_name == "load_data"
        assert metadata.operation == "read"
        assert metadata.status == "success"
        assert metadata.duration == 1.234

    def test_calculate_row_change(self):
        """Should calculate row count changes."""
        metadata = NodeExecutionMetadata(
            node_name="filter",
            operation="sql",
            status="success",
            duration=0.5,
            rows_in=1000,
            rows_out=850,
        )

        metadata.calculate_row_change()

        assert metadata.rows_change == -150
        assert metadata.rows_change_pct == -15.0

    def test_calculate_row_change_increase(self):
        """Should handle row count increases."""
        metadata = NodeExecutionMetadata(
            node_name="join",
            operation="join",
            status="success",
            duration=0.8,
            rows_in=100,
            rows_out=150,
        )

        metadata.calculate_row_change()

        assert metadata.rows_change == 50
        assert metadata.rows_change_pct == 50.0

    def test_calculate_row_change_from_zero(self):
        """Should handle calculation when starting from zero rows."""
        metadata = NodeExecutionMetadata(
            node_name="generate",
            operation="custom",
            status="success",
            duration=0.3,
            rows_in=0,
            rows_out=100,
        )

        metadata.calculate_row_change()

        assert metadata.rows_change == 100
        assert metadata.rows_change_pct == 100.0

    def test_calculate_schema_changes(self):
        """Should detect schema changes."""
        metadata = NodeExecutionMetadata(
            node_name="transform",
            operation="sql",
            status="success",
            duration=0.6,
            schema_in=["ID", "Name", "OldField"],
            schema_out=["ID", "Name", "NewField", "Calculated"],
        )

        metadata.calculate_schema_changes()

        assert "NewField" in metadata.columns_added
        assert "Calculated" in metadata.columns_added
        assert "OldField" in metadata.columns_removed

    def test_to_dict(self):
        """Should convert to dictionary."""
        metadata = NodeExecutionMetadata(
            node_name="test_node",
            operation="pivot",
            status="success",
            duration=1.5,
            rows_in=100,
            rows_out=50,
        )
        metadata.calculate_row_change()

        result = metadata.to_dict()

        assert result["node_name"] == "test_node"
        assert result["operation"] == "pivot"
        assert result["rows_change"] == -50
        assert "duration" in result


class TestPipelineStoryMetadata:
    """Tests for PipelineStoryMetadata class."""

    def test_create_pipeline_metadata(self):
        """Should create pipeline metadata."""
        metadata = PipelineStoryMetadata(pipeline_name="etl_pipeline", pipeline_layer="bronze")

        assert metadata.pipeline_name == "etl_pipeline"
        assert metadata.pipeline_layer == "bronze"
        assert metadata.total_nodes == 0

    def test_add_successful_node(self):
        """Should track successful nodes."""
        metadata = PipelineStoryMetadata(pipeline_name="test")

        node = NodeExecutionMetadata(
            node_name="node1", operation="read", status="success", duration=1.0
        )

        metadata.add_node(node)

        assert metadata.total_nodes == 1
        assert metadata.completed_nodes == 1
        assert metadata.failed_nodes == 0
        assert metadata.skipped_nodes == 0

    def test_add_failed_node(self):
        """Should track failed nodes."""
        metadata = PipelineStoryMetadata(pipeline_name="test")

        node = NodeExecutionMetadata(
            node_name="node1",
            operation="sql",
            status="failed",
            duration=0.5,
            error_message="Division by zero",
            error_type="ZeroDivisionError",
        )

        metadata.add_node(node)

        assert metadata.total_nodes == 1
        assert metadata.completed_nodes == 0
        assert metadata.failed_nodes == 1

    def test_add_skipped_node(self):
        """Should track skipped nodes."""
        metadata = PipelineStoryMetadata(pipeline_name="test")

        node = NodeExecutionMetadata(
            node_name="node1", operation="write", status="skipped", duration=0.0
        )

        metadata.add_node(node)

        assert metadata.total_nodes == 1
        assert metadata.skipped_nodes == 1

    def test_add_multiple_nodes(self):
        """Should track multiple nodes correctly."""
        metadata = PipelineStoryMetadata(pipeline_name="test")

        # Add 3 successful, 1 failed, 1 skipped
        for i in range(3):
            metadata.add_node(
                NodeExecutionMetadata(
                    node_name=f"node{i}", operation="read", status="success", duration=1.0
                )
            )

        metadata.add_node(
            NodeExecutionMetadata(
                node_name="failed_node", operation="sql", status="failed", duration=0.5
            )
        )

        metadata.add_node(
            NodeExecutionMetadata(
                node_name="skipped_node", operation="write", status="skipped", duration=0.0
            )
        )

        assert metadata.total_nodes == 5
        assert metadata.completed_nodes == 3
        assert metadata.failed_nodes == 1
        assert metadata.skipped_nodes == 1

    def test_get_success_rate(self):
        """Should calculate success rate."""
        metadata = PipelineStoryMetadata(pipeline_name="test")

        # 3 success, 1 failed = 75% success rate
        for i in range(3):
            metadata.add_node(
                NodeExecutionMetadata(
                    node_name=f"success{i}", operation="read", status="success", duration=1.0
                )
            )

        metadata.add_node(
            NodeExecutionMetadata(
                node_name="failed", operation="sql", status="failed", duration=0.5
            )
        )

        assert metadata.get_success_rate() == 75.0

    def test_get_success_rate_empty(self):
        """Should return 0% for empty pipeline."""
        metadata = PipelineStoryMetadata(pipeline_name="test")
        assert metadata.get_success_rate() == 0.0

    def test_get_total_rows_processed(self):
        """Should calculate total rows processed."""
        metadata = PipelineStoryMetadata(pipeline_name="test")

        metadata.add_node(
            NodeExecutionMetadata(
                node_name="node1", operation="read", status="success", duration=1.0, rows_out=1000
            )
        )

        metadata.add_node(
            NodeExecutionMetadata(
                node_name="node2", operation="filter", status="success", duration=0.5, rows_out=800
            )
        )

        assert metadata.get_total_rows_processed() == 1800

    def test_to_dict_complete(self):
        """Should convert complete metadata to dictionary."""
        metadata = PipelineStoryMetadata(
            pipeline_name="test_pipeline",
            pipeline_layer="silver",
            project="Test Project",
            plant="NKC",
            asset="Dryer 1",
        )

        metadata.add_node(
            NodeExecutionMetadata(
                node_name="load", operation="read", status="success", duration=1.0, rows_out=500
            )
        )

        result = metadata.to_dict()

        assert result["pipeline_name"] == "test_pipeline"
        assert result["pipeline_layer"] == "silver"
        assert result["project"] == "Test Project"
        assert result["plant"] == "NKC"
        assert result["total_nodes"] == 1
        assert result["success_rate"] == 100.0
        assert result["total_rows_processed"] == 500
        assert len(result["nodes"]) == 1

    def test_project_context(self):
        """Should store project context."""
        metadata = PipelineStoryMetadata(
            pipeline_name="test",
            project="Manufacturing Data",
            plant="NKC",
            asset="Germ Dryer 1",
            business_unit="Operations",
        )

        assert metadata.project == "Manufacturing Data"
        assert metadata.plant == "NKC"
        assert metadata.asset == "Germ Dryer 1"
        assert metadata.business_unit == "Operations"

    def test_get_total_rows_in(self):
        """Should calculate total input rows."""
        metadata = PipelineStoryMetadata(pipeline_name="test")

        metadata.add_node(
            NodeExecutionMetadata(
                node_name="node1",
                operation="read",
                status="success",
                duration=1.0,
                rows_in=1000,
                rows_out=1000,
            )
        )

        metadata.add_node(
            NodeExecutionMetadata(
                node_name="node2",
                operation="filter",
                status="success",
                duration=0.5,
                rows_in=1000,
                rows_out=800,
            )
        )

        assert metadata.get_total_rows_in() == 2000

    def test_get_rows_dropped(self):
        """Should calculate total rows dropped (filtered)."""
        metadata = PipelineStoryMetadata(pipeline_name="test")

        metadata.add_node(
            NodeExecutionMetadata(
                node_name="node1",
                operation="filter",
                status="success",
                duration=0.5,
                rows_in=1000,
                rows_out=800,
            )
        )

        metadata.add_node(
            NodeExecutionMetadata(
                node_name="node2",
                operation="clean",
                status="success",
                duration=0.3,
                rows_in=800,
                rows_out=750,
            )
        )

        # 200 dropped in node1, 50 dropped in node2 = 250 total
        assert metadata.get_rows_dropped() == 250

    def test_get_rows_dropped_ignores_increases(self):
        """Should not count row increases as drops."""
        metadata = PipelineStoryMetadata(pipeline_name="test")

        metadata.add_node(
            NodeExecutionMetadata(
                node_name="join",
                operation="join",
                status="success",
                duration=0.5,
                rows_in=100,
                rows_out=500,  # More rows out than in
            )
        )

        assert metadata.get_rows_dropped() == 0

    def test_get_final_output_rows(self):
        """Should get row count from last successful node."""
        metadata = PipelineStoryMetadata(pipeline_name="test")

        metadata.add_node(
            NodeExecutionMetadata(
                node_name="node1",
                operation="read",
                status="success",
                duration=1.0,
                rows_out=1000,
            )
        )

        metadata.add_node(
            NodeExecutionMetadata(
                node_name="node2",
                operation="filter",
                status="success",
                duration=0.5,
                rows_out=850,
            )
        )

        assert metadata.get_final_output_rows() == 850

    def test_get_alert_summary(self):
        """Should return a summary dict for alerts."""
        metadata = PipelineStoryMetadata(pipeline_name="test")

        metadata.add_node(
            NodeExecutionMetadata(
                node_name="read",
                operation="read",
                status="success",
                duration=1.0,
                rows_in=1000,
                rows_out=1000,
            )
        )

        metadata.add_node(
            NodeExecutionMetadata(
                node_name="filter",
                operation="filter",
                status="success",
                duration=0.5,
                rows_in=1000,
                rows_out=900,
            )
        )

        summary = metadata.get_alert_summary()

        assert summary["total_rows_processed"] == 1900
        assert summary["total_rows_in"] == 2000
        assert summary["rows_dropped"] == 100
        assert summary["final_output_rows"] == 900
        assert summary["success_rate"] == 100.0
        assert summary["completed_nodes"] == 2
        assert summary["failed_nodes"] == 0

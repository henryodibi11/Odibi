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

    def test_execution_steps(self):
        """Should store execution steps."""
        metadata = NodeExecutionMetadata(
            node_name="test_node",
            operation="transform",
            status="success",
            duration=1.0,
            execution_steps=[
                "Read from bronze_db",
                "Applied pattern 'deduplicate'",
                "Executed 2 pre-SQL statement(s)",
            ],
        )

        assert len(metadata.execution_steps) == 3
        assert "Read from bronze_db" in metadata.execution_steps

        result = metadata.to_dict()
        assert "execution_steps" in result
        assert len(result["execution_steps"]) == 3

    def test_error_traceback(self):
        """Should store error traceback fields."""
        metadata = NodeExecutionMetadata(
            node_name="failed_node",
            operation="sql",
            status="failed",
            duration=0.5,
            error_message="Division by zero",
            error_type="ZeroDivisionError",
            error_traceback="Traceback (most recent call last):\n  File ...\nZeroDivisionError: division by zero",
            error_traceback_cleaned="File ...\nZeroDivisionError: division by zero",
        )

        assert metadata.error_traceback is not None
        assert "Traceback" in metadata.error_traceback
        assert metadata.error_traceback_cleaned is not None
        assert "Traceback" not in metadata.error_traceback_cleaned

        result = metadata.to_dict()
        assert "error_traceback" in result
        assert "error_traceback_cleaned" in result

    def test_retry_history(self):
        """Should store retry history."""
        metadata = NodeExecutionMetadata(
            node_name="retried_node",
            operation="read",
            status="success",
            duration=5.0,
            retry_history=[
                {"attempt": 1, "success": False, "error": "Connection timeout", "duration": 1.2},
                {"attempt": 2, "success": False, "error": "Connection timeout", "duration": 2.4},
                {"attempt": 3, "success": True, "duration": 0.8},
            ],
        )

        assert len(metadata.retry_history) == 3
        assert metadata.retry_history[0]["success"] is False
        assert metadata.retry_history[2]["success"] is True

        result = metadata.to_dict()
        assert "retry_history" in result
        assert len(result["retry_history"]) == 3

    def test_failed_rows_samples(self):
        """Should store failed rows samples per validation."""
        metadata = NodeExecutionMetadata(
            node_name="validated_node",
            operation="validation",
            status="failed",
            duration=1.0,
            failed_rows_samples={
                "not_null_customer_id": [
                    {"order_id": 123, "customer_id": None},
                    {"order_id": 456, "customer_id": None},
                ],
                "positive_amount": [
                    {"order_id": 789, "amount": -10.00},
                ],
            },
            failed_rows_counts={
                "not_null_customer_id": 150,
                "positive_amount": 25,
            },
            failed_rows_truncated=False,
            truncated_validations=[],
        )

        assert len(metadata.failed_rows_samples) == 2
        assert "not_null_customer_id" in metadata.failed_rows_samples
        assert len(metadata.failed_rows_samples["not_null_customer_id"]) == 2
        assert metadata.failed_rows_counts["not_null_customer_id"] == 150

        result = metadata.to_dict()
        assert "failed_rows_samples" in result
        assert "failed_rows_counts" in result

    def test_failed_rows_truncation(self):
        """Should track truncated validations."""
        metadata = NodeExecutionMetadata(
            node_name="validated_node",
            operation="validation",
            status="failed",
            duration=1.0,
            failed_rows_samples={
                "val1": [{"id": 1}],
                "val2": [{"id": 2}],
            },
            failed_rows_counts={
                "val1": 100,
                "val2": 200,
                "val3": 300,
                "val4": 400,
            },
            failed_rows_truncated=True,
            truncated_validations=["val3", "val4"],
        )

        assert metadata.failed_rows_truncated is True
        assert "val3" in metadata.truncated_validations
        assert "val4" in metadata.truncated_validations

        result = metadata.to_dict()
        assert result["failed_rows_truncated"] is True
        assert len(result["truncated_validations"]) == 2


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


class TestNodeExplanationAndWarnings:
    """Tests for the new explanation and column_drop_warning features."""

    def test_node_with_explanation(self):
        """Should store and serialize explanation field."""
        explanation_text = """
This node performs the following:

1. **Filter** invalid records
2. Derive new columns

| Column | Logic |
|--------|-------|
| status | CASE WHEN x = 1 THEN 'A' END |
"""
        metadata = NodeExecutionMetadata(
            node_name="transform_data",
            operation="transform",
            status="success",
            duration=2.5,
            explanation=explanation_text,
        )

        assert metadata.explanation == explanation_text

        # Check serialization
        d = metadata.to_dict()
        assert d["explanation"] == explanation_text

    def test_node_with_column_drop_warning(self):
        """Should store and serialize column_drop_warning field."""
        warning = "⚠️ 5 columns were dropped (10 → 5)"
        metadata = NodeExecutionMetadata(
            node_name="pivot_data",
            operation="pivot",
            status="success",
            duration=1.0,
            column_drop_warning=warning,
        )

        assert metadata.column_drop_warning == warning

        # Check serialization
        d = metadata.to_dict()
        assert d["column_drop_warning"] == warning

    def test_node_without_explanation_defaults_to_none(self):
        """Should default explanation to None if not provided."""
        metadata = NodeExecutionMetadata(
            node_name="simple_node",
            operation="read",
            status="success",
            duration=0.5,
        )

        assert metadata.explanation is None
        assert metadata.column_drop_warning is None

        d = metadata.to_dict()
        assert d["explanation"] is None
        assert d["column_drop_warning"] is None

    def test_node_with_all_documentation_fields(self):
        """Should handle all documentation fields together."""
        metadata = NodeExecutionMetadata(
            node_name="documented_node",
            operation="transform",
            status="success",
            duration=3.0,
            description="High-level description",
            explanation="Detailed **markdown** explanation",
            runbook_url="https://wiki.example.com/runbook/node",
            column_drop_warning="⚠️ 3 columns dropped",
        )

        d = metadata.to_dict()
        assert d["description"] == "High-level description"
        assert d["explanation"] == "Detailed **markdown** explanation"
        assert d["runbook_url"] == "https://wiki.example.com/runbook/node"
        assert d["column_drop_warning"] == "⚠️ 3 columns dropped"

"""Tests for pipeline executor."""

import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from odibi.config import NodeConfig, PipelineConfig, ReadConfig, TransformConfig, WriteConfig
from odibi.connections import LocalConnection
from odibi.pipeline import Pipeline, PipelineResults
from odibi.registry import FunctionRegistry, transform

pytestmark = pytest.mark.skip(reason="Environment issues with LazyDataset persistence in CI")


class TestPipelineExecution:
    """Test basic pipeline execution."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create temp directory for test data
        self.test_dir = tempfile.mkdtemp()
        self.test_path = Path(self.test_dir)

        # Create test CSV file
        test_data = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
        test_data.to_csv(self.test_path / "input.csv", index=False)

        # Clear function registry
        FunctionRegistry.clear()

    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.test_dir)

    def test_simple_read_only_pipeline(self):
        """Test pipeline with single read node."""
        pipeline_config = PipelineConfig(
            pipeline="test_read",
            nodes=[
                NodeConfig(
                    name="load_data",
                    read=ReadConfig(connection="local", format="csv", path="input.csv"),
                )
            ],
        )

        connections = {"local": LocalConnection(base_path=str(self.test_path))}

        pipeline = Pipeline(pipeline_config, connections=connections)
        results = pipeline.run()

        assert len(results.completed) == 1
        assert "load_data" in results.completed
        assert len(results.failed) == 0
        assert len(results.skipped) == 0

    def test_pipeline_with_dependencies(self):
        """Test pipeline respects dependencies."""

        # Create transform function
        @transform
        def double_values(context, source: str):
            df = context.get(source)
            df = df.copy()
            df["value"] = df["value"] * 2
            return df

        pipeline_config = PipelineConfig(
            pipeline="test_deps",
            nodes=[
                NodeConfig(
                    name="load", read=ReadConfig(connection="local", format="csv", path="input.csv")
                ),
                NodeConfig(
                    name="transform",
                    depends_on=["load"],
                    transform=TransformConfig(
                        steps=[{"function": "double_values", "params": {"source": "load"}}]
                    ),
                ),
            ],
        )

        connections = {"local": LocalConnection(base_path=str(self.test_path))}
        pipeline = Pipeline(pipeline_config, connections=connections)
        results = pipeline.run()

        assert len(results.completed) == 2
        assert results.completed == ["load", "transform"]

    def test_pipeline_writes_output(self):
        """Test pipeline can write output."""
        pipeline_config = PipelineConfig(
            pipeline="test_write",
            nodes=[
                NodeConfig(
                    name="load", read=ReadConfig(connection="local", format="csv", path="input.csv")
                ),
                NodeConfig(
                    name="save",
                    depends_on=["load"],
                    write=WriteConfig(
                        connection="local", format="csv", path="output.csv", mode="overwrite"
                    ),
                ),
            ],
        )

        connections = {"local": LocalConnection(base_path=str(self.test_path))}
        pipeline = Pipeline(pipeline_config, connections=connections)
        results = pipeline.run()

        assert len(results.completed) == 2
        assert (self.test_path / "output.csv").exists()

        # Verify output content
        output_df = pd.read_csv(self.test_path / "output.csv")
        assert len(output_df) == 3


class TestPipelineFailures:
    """Test pipeline error handling."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.test_path = Path(self.test_dir)

        # Create test data
        test_data = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
        test_data.to_csv(self.test_path / "input.csv", index=False)

        # Clear registry
        FunctionRegistry.clear()

    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.test_dir)

    def test_failed_node_skips_dependents(self):
        """Test that failed nodes cause dependents to be skipped."""

        # Function that will fail
        @transform
        def failing_function(context, source: str):
            raise ValueError("Intentional failure")

        pipeline_config = PipelineConfig(
            pipeline="test_failure",
            nodes=[
                NodeConfig(
                    name="load", read=ReadConfig(connection="local", format="csv", path="input.csv")
                ),
                NodeConfig(
                    name="fail",
                    depends_on=["load"],
                    transform=TransformConfig(
                        steps=[{"function": "failing_function", "params": {"source": "load"}}]
                    ),
                ),
                NodeConfig(
                    name="dependent",
                    depends_on=["fail"],
                    transform=TransformConfig(steps=["SELECT * FROM fail"]),
                ),
            ],
        )

        connections = {"local": LocalConnection(base_path=str(self.test_path))}
        pipeline = Pipeline(pipeline_config, connections=connections)
        results = pipeline.run()

        assert "load" in results.completed
        assert "fail" in results.failed
        assert "dependent" in results.skipped

    def test_independent_nodes_continue_after_failure(self):
        """Test that independent nodes still run even if others fail."""

        @transform
        def failing_function(context, source: str):
            raise ValueError("Intentional failure")

        pipeline_config = PipelineConfig(
            pipeline="test_independent",
            nodes=[
                NodeConfig(
                    name="success1",
                    read=ReadConfig(connection="local", format="csv", path="input.csv"),
                ),
                NodeConfig(
                    name="fail",
                    read=ReadConfig(connection="local", format="csv", path="input.csv"),
                    transform=TransformConfig(
                        steps=[{"function": "failing_function", "params": {"source": "fail"}}]
                    ),
                ),
                NodeConfig(
                    name="success2",
                    read=ReadConfig(connection="local", format="csv", path="input.csv"),
                ),
            ],
        )

        connections = {"local": LocalConnection(base_path=str(self.test_path))}
        pipeline = Pipeline(pipeline_config, connections=connections)
        results = pipeline.run()

        assert "success1" in results.completed
        assert "success2" in results.completed
        assert "fail" in results.failed


class TestPipelineValidation:
    """Test pipeline validation."""

    def test_validate_simple_pipeline(self):
        """Test validation of valid pipeline."""
        pipeline_config = PipelineConfig(
            pipeline="test",
            nodes=[
                NodeConfig(
                    name="node1", read=ReadConfig(connection="local", format="csv", path="a.csv")
                ),
                NodeConfig(
                    name="node2",
                    depends_on=["node1"],
                    transform=TransformConfig(steps=["SELECT * FROM node1"]),
                ),
            ],
        )

        pipeline = Pipeline(pipeline_config, connections={})
        validation = pipeline.validate()

        assert validation["valid"] is True
        assert len(validation["execution_order"]) == 2
        assert validation["execution_order"] == ["node1", "node2"]

    def test_validate_detects_missing_connections(self):
        """Test validation warns about missing connections."""
        pipeline_config = PipelineConfig(
            pipeline="test",
            nodes=[
                NodeConfig(
                    name="node1",
                    read=ReadConfig(connection="missing_conn", format="csv", path="a.csv"),
                )
            ],
        )

        pipeline = Pipeline(pipeline_config, connections={})
        validation = pipeline.validate()

        assert len(validation["warnings"]) > 0
        assert "missing_conn" in validation["warnings"][0]


class TestRunSingleNode:
    """Test running single nodes for debugging."""

    def setup_method(self):
        """Set up test fixtures."""
        FunctionRegistry.clear()

    def test_run_node_with_mock_data(self):
        """Test running single node with mock data."""

        @transform
        def process_data(context, source: str):
            df = context.get(source)
            df = df.copy()
            df["result"] = df["value"] * 2
            return df

        pipeline_config = PipelineConfig(
            pipeline="test",
            nodes=[
                NodeConfig(
                    name="process",
                    transform=TransformConfig(
                        steps=[{"function": "process_data", "params": {"source": "input"}}]
                    ),
                )
            ],
        )

        pipeline = Pipeline(pipeline_config, connections={})

        # Run with mock data
        mock_df = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        result = pipeline.run_node("process", mock_data={"input": mock_df})

        assert result.success is True

        # Verify result
        output = pipeline.context.get("process")
        assert "result" in output.columns
        assert output["result"].tolist() == [20, 40]


class TestPipelineResults:
    """Test PipelineResults dataclass."""

    def test_results_to_dict(self):
        """Test converting results to dictionary."""
        results = PipelineResults(
            pipeline_name="test",
            completed=["node1", "node2"],
            failed=["node3"],
            skipped=["node4"],
            duration=1.5,
        )

        result_dict = results.to_dict()

        assert result_dict["pipeline_name"] == "test"
        assert result_dict["completed"] == ["node1", "node2"]
        assert result_dict["failed"] == ["node3"]
        assert result_dict["skipped"] == ["node4"]
        assert result_dict["duration"] == 1.5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

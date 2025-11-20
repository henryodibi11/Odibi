import pytest
from unittest.mock import MagicMock, patch
from odibi.config import (
    NodeConfig,
    ReadConfig,
    WriteConfig,
    PipelineConfig,
)
from odibi.context import PandasContext
from odibi.pipeline import Pipeline
from odibi.node import Node
import pandas as pd


class TestCheckpointing:
    @pytest.fixture
    def context(self):
        return PandasContext()

    @pytest.fixture
    def engine(self):
        engine = MagicMock()
        # Mock read to return a DataFrame
        engine.read.return_value = pd.DataFrame({"a": [1]})
        engine.get_schema.return_value = ["a"]
        engine.count_rows.return_value = 1
        return engine

    @pytest.fixture
    def connections(self):
        return {"local": MagicMock()}

    def test_node_restore_success(self, context, engine, connections):
        """Test that Node.restore() works when write config exists."""
        config = NodeConfig(
            name="node_a",
            # Node A writes to output.csv
            read=ReadConfig(connection="local", format="csv", path="input.csv"),
            write=WriteConfig(connection="local", format="csv", path="output.csv"),
        )

        node = Node(config, context, engine, connections)

        # When restoring, it should call engine.read with output parameters
        restored = node.restore()

        assert restored is True
        assert context.has("node_a")
        # Verify read was called (to restore)
        assert engine.read.called

    def test_node_restore_fail_no_write(self, context, engine, connections):
        """Test that Node.restore() fails if no write config."""
        config = NodeConfig(
            name="node_a",
            read=ReadConfig(connection="local", format="csv", path="input.csv"),
            # No write
        )

        node = Node(config, context, engine, connections)
        restored = node.restore()

        assert restored is False
        assert not context.has("node_a")

    def test_pipeline_resume_skips_successful_node(self, context, engine, connections):
        """Test Pipeline.run logic for skipping nodes."""

        # Mock StateManager
        with patch("odibi.pipeline.StateManager") as MockStateManager:
            state_instance = MockStateManager.return_value

            # Setup: Node A succeeded last time
            def get_status(pipeline, node):
                if node == "node_a":
                    return True
                return False

            state_instance.get_last_run_status.side_effect = get_status

            # Pipeline config
            pipeline_config = PipelineConfig(
                pipeline="test_pipe",
                nodes=[
                    NodeConfig(
                        name="node_a",
                        read=ReadConfig(connection="local", format="csv", path="in.csv"),
                        write=WriteConfig(connection="local", format="csv", path="out_a.csv"),
                    ),
                    NodeConfig(
                        name="node_b",
                        depends_on=["node_a"],
                        write=WriteConfig(connection="local", format="csv", path="out_b.csv"),
                    ),
                ],
            )

            # Use real Pipeline class but mock engine/connections
            pipeline = Pipeline(pipeline_config, engine="pandas", connections=connections)
            pipeline.engine = engine
            pipeline.context = context

            # Execute with resume
            results = pipeline.run(resume_from_failure=True)

            # Node A should be skipped
            assert "node_a" in results.completed
            assert results.node_results["node_a"].metadata.get("skipped") is True

            # Node B should run (not skipped)
            assert "node_b" in results.completed
            assert not results.node_results["node_b"].metadata.get("skipped")

            # Context should have node_a (restored) and node_b (executed)
            assert context.has("node_a")
            assert context.has("node_b")

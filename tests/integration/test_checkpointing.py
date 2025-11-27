from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.config import (
    NodeConfig,
    PipelineConfig,
    ReadConfig,
    WriteConfig,
)
from odibi.context import PandasContext
from odibi.node import Node
from odibi.pipeline import Pipeline


class TestCheckpointing:
    @pytest.fixture
    def context(self):
        return PandasContext()

    @pytest.fixture
    def engine(self):
        engine = MagicMock()
        # Mock read to return a DataFrame
        engine.read.return_value = pd.DataFrame({"a": [1]})
        engine.get_schema.return_value = {"a": "int64"}
        engine.count_rows.return_value = 1
        # Mock other methods used in reporting to avoid MagicMock comparisons
        engine.validate_data.return_value = []
        engine.profile_nulls.return_value = {"a": 0.0}
        engine.get_source_files.return_value = []
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
        import hashlib
        import json

        # Mock StateManager
        with patch("odibi.pipeline.StateManager") as MockStateManager:
            state_instance = MockStateManager.return_value

            # Pipeline config
            node_a_config = NodeConfig(
                name="node_a",
                read=ReadConfig(connection="local", format="csv", path="in.csv"),
                write=WriteConfig(connection="local", format="csv", path="out_a.csv"),
            )
            node_b_config = NodeConfig(
                name="node_b",
                depends_on=["node_a"],
                write=WriteConfig(connection="local", format="csv", path="out_b.csv"),
            )

            pipeline_config = PipelineConfig(
                pipeline="test_pipe",
                nodes=[node_a_config, node_b_config],
            )

            # Calculate expected hash for node_a to mock successful previous run
            dump = node_a_config.model_dump(
                mode="json", exclude={"description", "tags", "log_level"}
            )
            dump_str = json.dumps(dump, sort_keys=True)
            node_a_hash = hashlib.md5(dump_str.encode("utf-8")).hexdigest()

            # Setup: Node A succeeded last time with matching hash
            def get_info(pipeline, node):
                if node == "node_a":
                    return {
                        "success": True,
                        "metadata": {"version_hash": node_a_hash},
                    }
                return None

            state_instance.get_last_run_info.side_effect = get_info

            # Use real Pipeline class but mock engine/connections
            pipeline = Pipeline(pipeline_config, engine="pandas", connections=connections)
            pipeline.engine = engine
            pipeline.context = context

            # Inject mock ProjectConfig for state management
            from odibi.config import ProjectConfig, SystemConfig, StoryConfig

            pipeline.project_config = ProjectConfig(
                project="test_project",
                engine="pandas",
                connections={"local": {"type": "local", "base_path": "./data"}},
                pipelines=[pipeline_config],
                story=StoryConfig(connection="local", path="stories"),
                system=SystemConfig(connection="local"),
            )

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

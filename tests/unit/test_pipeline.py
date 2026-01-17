"""Unit tests for Pipeline and PipelineManager classes."""

from unittest.mock import MagicMock, patch

import pytest

pytest.skip("Pipeline functionality not implemented", allow_module_level=True)

from odibi.config import NodeConfig, PipelineConfig, ProjectConfig  # noqa: E402
from odibi.pipeline import Pipeline, PipelineManager, PipelineResults  # noqa: E402


@pytest.fixture
def mock_context():
    return MagicMock()


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.name = "pandas"
    return engine


@pytest.fixture
def connections():
    return {"src": MagicMock(), "dst": MagicMock()}


@pytest.fixture
def basic_pipeline_config():
    return PipelineConfig(
        pipeline="test_pipeline",
        nodes=[
            NodeConfig(
                name="test_node",
                read={"connection": "src", "format": "csv", "path": "input.csv"},
                write={"connection": "dst", "format": "csv", "path": "output.csv"},
            )
        ],
    )


@pytest.fixture
def basic_project_config():
    return ProjectConfig(
        project="test_project",
        engine="pandas",
        connections={"src": {"type": "local"}, "dst": {"type": "local"}},
        story={"connection": "dst", "path": "stories"},
        system={"connection": "dst", "path": "_system"},
        pipelines=[],
    )


class TestPipelineResults:
    """Tests for PipelineResults dataclass."""

    def test_init_basic(self):
        """Test basic PipelineResults initialization."""
        results = PipelineResults(pipeline_name="test_pipeline")

        assert results.pipeline_name == "test_pipeline"
        assert results.completed == []
        assert results.failed == []
        assert results.skipped == []
        assert results.node_results == {}
        assert results.duration == 0.0
        assert results.start_time is None
        assert results.end_time is None
        assert results.story_path is None

    def test_to_dict(self):
        """Test to_dict method."""
        results = PipelineResults(
            pipeline_name="test_pipeline",
            completed=["node1"],
            failed=["node2"],
            duration=10.5,
            start_time="2024-01-01T00:00:00",
            end_time="2024-01-01T00:00:10",
        )

        result_dict = results.to_dict()

        assert result_dict["pipeline_name"] == "test_pipeline"
        assert result_dict["completed"] == ["node1"]
        assert result_dict["failed"] == ["node2"]
        assert result_dict["duration"] == 10.5
        assert result_dict["start_time"] == "2024-01-01T00:00:00"
        assert result_dict["end_time"] == "2024-01-01T00:00:10"
        assert result_dict["node_count"] == 0

    def test_debug_summary_success(self):
        """Test debug_summary for successful pipeline."""
        results = PipelineResults(pipeline_name="test_pipeline")

        summary = results.debug_summary()

        assert summary == ""

    def test_debug_summary_failure(self):
        """Test debug_summary for failed pipeline."""
        results = PipelineResults(
            pipeline_name="test_pipeline",
            failed=["failed_node"],
            story_path="/path/to/story",
        )

        summary = results.debug_summary()

        assert "❌ Pipeline 'test_pipeline' failed" in summary
        assert "failed_node" in summary
        assert "odibi story show /path/to/story" in summary


class TestPipeline:
    """Tests for Pipeline class."""

    def test_init_basic(self, basic_pipeline_config, mock_engine, connections):
        """Test basic Pipeline initialization."""
        with patch("odibi.pipeline.create_context") as mock_create_context:
            mock_context = MagicMock()
            mock_create_context.return_value = mock_context

            with patch("odibi.pipeline.DependencyGraph") as mock_dep_graph:
                mock_graph = MagicMock()
                mock_dep_graph.return_value = mock_graph

                pipeline = Pipeline(
                    pipeline_config=basic_pipeline_config,
                    engine="pandas",
                    connections=connections,
                )

                assert pipeline.config == basic_pipeline_config
                assert pipeline.engine_type == "pandas"
                assert pipeline.connections == connections
                assert pipeline.context == mock_context
                assert pipeline.graph == mock_graph

    def test_register_outputs_no_catalog(self, basic_pipeline_config, mock_engine, connections):
        """Test register_outputs when no catalog manager configured."""
        with patch("odibi.pipeline.create_context"):
            with patch("odibi.pipeline.DependencyGraph"):
                pipeline = Pipeline(
                    pipeline_config=basic_pipeline_config,
                    engine="pandas",
                    connections=connections,
                )

                count = pipeline.register_outputs()
                assert count == 0

    def test_register_outputs_with_catalog(self, basic_pipeline_config, mock_engine, connections):
        """Test register_outputs with catalog manager."""
        catalog_manager = MagicMock()
        catalog_manager.register_outputs_from_config.return_value = 5

        with patch("odibi.pipeline.create_context"):
            with patch("odibi.pipeline.DependencyGraph"):
                pipeline = Pipeline(
                    pipeline_config=basic_pipeline_config,
                    engine="pandas",
                    connections=connections,
                    catalog_manager=catalog_manager,
                )

                count = pipeline.register_outputs()
                assert count == 5
                catalog_manager.register_outputs_from_config.assert_called_once_with(
                    basic_pipeline_config
                )

    def test_run_basic(self, basic_pipeline_config, mock_engine, connections):
        """Test basic pipeline run."""
        with patch("odibi.pipeline.create_context") as mock_create_context:
            mock_context = MagicMock()
            mock_create_context.return_value = mock_context

            with patch("odibi.pipeline.DependencyGraph") as mock_dep_graph:
                mock_graph = MagicMock()
                mock_graph.topological_sort.return_value = ["test_node"]
                mock_graph.get_execution_layers.return_value = [["test_node"]]
                mock_dep_graph.return_value = mock_graph

                pipeline = Pipeline(
                    pipeline_config=basic_pipeline_config,
                    engine="pandas",
                    connections=connections,
                )

                # Mock the process_node function
                with patch.object(pipeline, "_ctx"):
                    with patch("odibi.pipeline.time.time", side_effect=[0.0, 1.0]):
                        with patch("odibi.pipeline.datetime") as mock_datetime:
                            mock_datetime.now.return_value.isoformat.return_value = (
                                "2024-01-01T00:00:00"
                            )

                            # Mock the inner process_node logic
                            def mock_process_node(node_name):
                                return MagicMock(success=True, duration=0.5, failed=[], metadata={})

                            with patch.object(
                                pipeline, "process_node", side_effect=mock_process_node
                            ):
                                results = pipeline.run()

                                assert isinstance(results, PipelineResults)
                                assert results.pipeline_name == "test_pipeline"
                                assert results.duration == 1.0
                                assert results.completed == ["test_node"]

    def test_run_node_basic(self, basic_pipeline_config, mock_engine, connections):
        """Test run_node method."""
        with patch("odibi.pipeline.create_context"):
            with patch("odibi.pipeline.DependencyGraph"):
                pipeline = Pipeline(
                    pipeline_config=basic_pipeline_config,
                    engine="pandas",
                    connections=connections,
                )

                # Mock Node execution
                mock_result = MagicMock()
                with patch("odibi.pipeline.Node") as mock_node_class:
                    mock_node_instance = MagicMock()
                    mock_node_instance.execute.return_value = mock_result
                    mock_node_class.return_value = mock_node_instance

                    result = pipeline.run_node("test_node")

                    assert result == mock_result
                    mock_node_class.assert_called_once()
                    mock_node_instance.execute.assert_called_once()

    def test_run_node_not_found(self, basic_pipeline_config, mock_engine, connections):
        """Test run_node with non-existent node."""
        with patch("odibi.pipeline.create_context"):
            with patch("odibi.pipeline.DependencyGraph"):
                pipeline = Pipeline(
                    pipeline_config=basic_pipeline_config,
                    engine="pandas",
                    connections=connections,
                )

                with pytest.raises(ValueError, match="Node 'nonexistent' not found"):
                    pipeline.run_node("nonexistent")

    def test_validate_basic(self, basic_pipeline_config, mock_engine, connections):
        """Test basic pipeline validation."""
        with patch("odibi.pipeline.create_context"):
            with patch("odibi.pipeline.DependencyGraph") as mock_dep_graph:
                mock_graph = MagicMock()
                mock_graph.topological_sort.return_value = ["test_node"]
                mock_dep_graph.return_value = mock_graph

                pipeline = Pipeline(
                    pipeline_config=basic_pipeline_config,
                    engine="pandas",
                    connections=connections,
                )

                with patch.object(pipeline, "_ctx"):
                    validation = pipeline.validate()

                    assert validation["valid"] is True
                    assert validation["node_count"] == 1
                    assert validation["execution_order"] == ["test_node"]

    def test_get_execution_layers(self, basic_pipeline_config, mock_engine, connections):
        """Test get_execution_layers method."""
        with patch("odibi.pipeline.create_context"):
            with patch("odibi.pipeline.DependencyGraph") as mock_dep_graph:
                mock_graph = MagicMock()
                mock_graph.get_execution_layers.return_value = [["node1"], ["node2"]]
                mock_dep_graph.return_value = mock_graph

                pipeline = Pipeline(
                    pipeline_config=basic_pipeline_config,
                    engine="pandas",
                    connections=connections,
                )

                layers = pipeline.get_execution_layers()
                assert layers == [["node1"], ["node2"]]
                mock_graph.get_execution_layers.assert_called_once()

    def test_visualize(self, basic_pipeline_config, mock_engine, connections):
        """Test visualize method."""
        with patch("odibi.pipeline.create_context"):
            with patch("odibi.pipeline.DependencyGraph") as mock_dep_graph:
                mock_graph = MagicMock()
                mock_graph.visualize.return_value = "graph visualization"
                mock_dep_graph.return_value = mock_graph

                pipeline = Pipeline(
                    pipeline_config=basic_pipeline_config,
                    engine="pandas",
                    connections=connections,
                )

                viz = pipeline.visualize()
                assert viz == "graph visualization"
                mock_graph.visualize.assert_called_once()


class TestPipelineManager:
    """Tests for PipelineManager class."""

    def test_from_yaml_basic(self, basic_project_config, tmp_path):
        """Test basic PipelineManager creation from YAML."""
        # Create a temporary YAML file
        yaml_content = """
        project: test_project
        engine: pandas
        connections:
          src:
            type: local
          dst:
            type: local
        story:
          connection: dst
          path: stories
        system:
          connection: dst
          path: _system
        pipelines:
          - pipeline: test_pipeline
            nodes:
              - name: test_node
                read:
                  connection: src
                  format: csv
                  path: input.csv
                write:
                  connection: dst
                  format: csv
                  path: output.csv
        """

        yaml_path = tmp_path / "test_config.yaml"
        yaml_path.write_text(yaml_content)

        with patch("odibi.pipeline.PipelineManager._build_connections") as mock_build_conn:
            mock_build_conn.return_value = {"src": MagicMock(), "dst": MagicMock()}

            with patch("odibi.pipeline.configure_logging"):
                with patch("odibi.pipeline.OpenLineageAdapter"):
                    with patch("odibi.pipeline.Pipeline"):
                        manager = PipelineManager.from_yaml(str(yaml_path))

                        assert isinstance(manager, PipelineManager)
                        assert manager.project_config.project == "test_project"

    def test_list_pipelines(self, basic_project_config):
        """Test list_pipelines method."""
        with patch("odibi.pipeline.configure_logging"):
            with patch("odibi.pipeline.OpenLineageAdapter"):
                manager = PipelineManager(basic_project_config, {})

                pipelines = manager.list_pipelines()
                assert pipelines == []

    def test_get_pipeline(self, basic_project_config):
        """Test get_pipeline method."""
        with patch("odibi.pipeline.configure_logging"):
            with patch("odibi.pipeline.OpenLineageAdapter"):
                manager = PipelineManager(basic_project_config, {})

                with pytest.raises(ValueError, match="Pipeline 'nonexistent' not found"):
                    manager.get_pipeline("nonexistent")

    def test_deploy_no_catalog(self, basic_project_config):
        """Test deploy when no catalog manager configured."""
        with patch("odibi.pipeline.configure_logging"):
            with patch("odibi.pipeline.OpenLineageAdapter"):
                manager = PipelineManager(basic_project_config, {})

                result = manager.deploy()
                assert result is False

    def test_run_basic(self, basic_project_config):
        """Test basic run method."""
        with patch("odibi.pipeline.configure_logging"):
            with patch("odibi.pipeline.OpenLineageAdapter"):
                manager = PipelineManager(basic_project_config, {})

                # Should return empty dict since no pipelines
                results = manager.run()
                assert results == {}

    def test_run_single_pipeline(self, basic_project_config):
        """Test run method with single pipeline."""
        # Add a pipeline to the config
        pipeline_config = PipelineConfig(
            pipeline="test_pipeline",
            nodes=[
                NodeConfig(
                    name="test_node",
                    read={"connection": "src", "format": "csv", "path": "input.csv"},
                    write={"connection": "dst", "format": "csv", "path": "output.csv"},
                )
            ],
        )
        basic_project_config.pipelines = [pipeline_config]

        with patch("odibi.pipeline.configure_logging"):
            with patch("odibi.pipeline.OpenLineageAdapter"):
                with patch("odibi.pipeline.Pipeline") as mock_pipeline_class:
                    mock_pipeline = MagicMock()
                    mock_pipeline.config = pipeline_config
                    mock_pipeline.run.return_value = PipelineResults(pipeline_name="test_pipeline")
                    mock_pipeline_class.return_value = mock_pipeline

                    manager = PipelineManager(basic_project_config, {})

                    result = manager.run("test_pipeline")

                    assert isinstance(result, PipelineResults)
                    assert result.pipeline_name == "test_pipeline"
                    mock_pipeline.run.assert_called_once()

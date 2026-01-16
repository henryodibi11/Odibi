"""Tests for SemanticLayerRunner."""

from unittest.mock import MagicMock, patch

import pytest

from odibi.semantics.runner import SemanticConfig, SemanticLayerRunner, run_semantic_layer


@pytest.fixture
def mock_project_config():
    """Create a mock ProjectConfig."""
    config = MagicMock()
    config.project = "test_project"
    config.semantic = {
        "connection": "sql_gold",
        "sql_output_path": "gold/views/",
        "metrics": [
            {
                "name": "revenue",
                "expr": "SUM(amount)",
                "source": "fact_orders",
            }
        ],
        "dimensions": [
            {"name": "region"},
        ],
        "views": [
            {
                "name": "vw_revenue_by_region",
                "metrics": ["revenue"],
                "dimensions": ["region"],
            }
        ],
    }
    config.connections = {}
    config.story = MagicMock()
    config.story.auto_generate = False
    config.story.generate_lineage = False
    config.story.path = "./stories"
    config.story.connection = None
    return config


class TestSemanticConfig:
    """Test SemanticConfig class."""

    def test_init_parses_connection(self):
        """Test that connection is parsed."""
        config_dict = {
            "connection": "sql_gold",
            "metrics": [],
        }
        sem_config = SemanticConfig(config_dict)

        assert sem_config.connection == "sql_gold"

    def test_init_parses_sql_output_path(self):
        """Test that sql_output_path is parsed."""
        config_dict = {
            "sql_output_path": "gold/views/",
            "metrics": [],
        }
        sem_config = SemanticConfig(config_dict)

        assert sem_config.sql_output_path == "gold/views/"

    def test_init_parses_layer_config(self):
        """Test that layer_config is parsed."""
        config_dict = {
            "metrics": [
                {"name": "revenue", "expr": "SUM(amount)", "source": "fact_orders"}
            ],
            "dimensions": [{"name": "region"}],
        }
        sem_config = SemanticConfig(config_dict)

        assert len(sem_config.layer_config.metrics) == 1
        assert len(sem_config.layer_config.dimensions) == 1


class TestSemanticLayerRunnerInit:
    """Test SemanticLayerRunner initialization."""

    def test_init_stores_project_config(self, mock_project_config):
        """Test that project config is stored."""
        runner = SemanticLayerRunner(mock_project_config)
        assert runner.project_config is mock_project_config

    def test_init_default_name(self, mock_project_config):
        """Test default name generation."""
        runner = SemanticLayerRunner(mock_project_config)
        assert runner.name == "test_project_semantic"

    def test_init_custom_name(self, mock_project_config):
        """Test custom name."""
        runner = SemanticLayerRunner(mock_project_config, name="custom_semantic")
        assert runner.name == "custom_semantic"


class TestSemanticLayerRunnerProperties:
    """Test SemanticLayerRunner properties."""

    def test_semantic_ext_property(self, mock_project_config):
        """Test semantic_ext property."""
        runner = SemanticLayerRunner(mock_project_config)

        ext = runner.semantic_ext
        assert ext.connection == "sql_gold"

    def test_semantic_config_property(self, mock_project_config):
        """Test semantic_config property."""
        runner = SemanticLayerRunner(mock_project_config)

        config = runner.semantic_config
        assert len(config.metrics) == 1

    def test_connection_name_property(self, mock_project_config):
        """Test connection_name property."""
        runner = SemanticLayerRunner(mock_project_config)
        assert runner.connection_name == "sql_gold"

    def test_sql_output_path_property(self, mock_project_config):
        """Test sql_output_path property."""
        runner = SemanticLayerRunner(mock_project_config)
        assert runner.sql_output_path == "gold/views/"


class TestSemanticLayerRunnerRun:
    """Test SemanticLayerRunner run method."""

    def test_run_no_views_returns_empty(self, mock_project_config):
        """Test run returns empty when no views defined."""
        mock_project_config.semantic = {"metrics": [], "views": []}

        runner = SemanticLayerRunner(mock_project_config)
        result = runner.run(execute_sql=lambda x: None)

        assert result["views_created"] == []
        assert result["views_failed"] == []

    def test_run_requires_execute_sql_or_connection(self, mock_project_config):
        """Test run requires execute_sql or connection."""
        mock_project_config.semantic = {
            "metrics": [{"name": "rev", "expr": "SUM(x)", "source": "t"}],
            "views": [{"name": "v", "metrics": ["rev"], "dimensions": ["a"]}],
        }
        mock_project_config.connections = {}

        runner = SemanticLayerRunner(mock_project_config)

        with pytest.raises(ValueError, match="execute_sql"):
            runner.run()

    @patch("odibi.semantics.runner.SemanticStoryGenerator")
    def test_run_with_execute_sql(self, mock_story_gen, mock_project_config):
        """Test run with execute_sql provided."""
        mock_metadata = MagicMock()
        mock_metadata.views = []
        mock_metadata.duration = 1.5
        mock_story_gen.return_value.execute_with_story.return_value = mock_metadata

        runner = SemanticLayerRunner(mock_project_config)

        execute_sql = MagicMock()
        result = runner.run(execute_sql=execute_sql)

        assert result["duration"] == 1.5
        mock_story_gen.assert_called_once()


class TestSemanticLayerRunnerStorageOptions:
    """Test SemanticLayerRunner storage options methods."""

    def test_get_storage_options_no_connection(self, mock_project_config):
        """Test _get_storage_options returns empty when no connection."""
        runner = SemanticLayerRunner(mock_project_config)

        with patch("odibi.semantics.runner.get_storage_options") as mock_get:
            mock_get.return_value = {}
            options = runner._get_storage_options()

            assert options == {}


class TestRunSemanticLayerFunction:
    """Test run_semantic_layer convenience function."""

    @patch("odibi.semantics.runner.SemanticLayerRunner")
    def test_creates_runner_and_runs(self, mock_runner_class, mock_project_config):
        """Test that function creates runner and calls run."""
        mock_runner = MagicMock()
        mock_runner.run.return_value = {"views_created": [], "views_failed": []}
        mock_runner_class.return_value = mock_runner

        execute_sql = MagicMock()
        result = run_semantic_layer(mock_project_config, execute_sql)

        mock_runner_class.assert_called_once_with(mock_project_config)
        mock_runner.run.assert_called_once()
        assert "views_created" in result

    @patch("odibi.semantics.runner.SemanticLayerRunner")
    def test_passes_all_args(self, mock_runner_class, mock_project_config):
        """Test that all arguments are passed through."""
        mock_runner = MagicMock()
        mock_runner.run.return_value = {}
        mock_runner_class.return_value = mock_runner

        execute_sql = MagicMock()
        write_file = MagicMock()

        run_semantic_layer(
            mock_project_config,
            execute_sql,
            save_sql_to="path/",
            write_file=write_file,
            generate_story=True,
            generate_lineage=True,
        )

        call_kwargs = mock_runner.run.call_args[1]
        assert call_kwargs["execute_sql"] is execute_sql
        assert call_kwargs["save_sql_to"] == "path/"
        assert call_kwargs["write_file"] is write_file
        assert call_kwargs["generate_story"] is True
        assert call_kwargs["generate_lineage"] is True


class TestSemanticLayerRunnerMetadata:
    """Test SemanticLayerRunner metadata property."""

    def test_metadata_initially_none(self, mock_project_config):
        """Test metadata is None before run."""
        runner = SemanticLayerRunner(mock_project_config)
        assert runner.metadata is None

    @patch("odibi.semantics.runner.SemanticStoryGenerator")
    def test_metadata_set_after_run(self, mock_story_gen, mock_project_config):
        """Test metadata is set after run."""
        mock_metadata = MagicMock()
        mock_metadata.views = []
        mock_metadata.duration = 1.0
        mock_story_gen.return_value.execute_with_story.return_value = mock_metadata

        runner = SemanticLayerRunner(mock_project_config)
        runner.run(execute_sql=MagicMock())

        assert runner.metadata is mock_metadata

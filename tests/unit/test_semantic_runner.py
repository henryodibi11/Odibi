"""Tests for odibi.semantics.runner module."""

from unittest.mock import MagicMock, patch

import pytest

from odibi.semantics.runner import SemanticConfig, SemanticLayerRunner, run_semantic_layer


@pytest.fixture
def mock_project_config():
    """Create a mock ProjectConfig with semantic section."""
    config = MagicMock()
    config.project = "test_project"
    config.semantic = {
        "connection": "sql_gold",
        "sql_output_path": "gold/views/",
        "metrics": [
            {"name": "revenue", "expr": "SUM(amount)", "source": "fact_orders"},
        ],
        "dimensions": [{"name": "region"}],
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
    config.story.docs = None
    return config


# ---------- SemanticConfig ----------


class TestSemanticConfigInit:
    """Test SemanticConfig.__init__ parsing."""

    def test_parses_connection(self):
        sc = SemanticConfig({"connection": "my_conn", "metrics": []})
        assert sc.connection == "my_conn"

    def test_parses_sql_output_path(self):
        sc = SemanticConfig({"sql_output_path": "gold/views/", "metrics": []})
        assert sc.sql_output_path == "gold/views/"

    def test_parses_layer_config(self):
        sc = SemanticConfig(
            {
                "metrics": [
                    {"name": "rev", "expr": "SUM(x)", "source": "t"},
                ],
                "dimensions": [{"name": "region"}],
            }
        )
        assert len(sc.layer_config.metrics) == 1
        assert len(sc.layer_config.dimensions) == 1

    def test_defaults_when_keys_missing(self):
        sc = SemanticConfig({})
        assert sc.connection is None
        assert sc.sql_output_path is None


# ---------- SemanticLayerRunner.__init__ ----------


class TestSemanticLayerRunnerInit:
    """Test SemanticLayerRunner initialization."""

    def test_default_name_generation(self, mock_project_config):
        runner = SemanticLayerRunner(mock_project_config)
        assert runner.name == "test_project_semantic"

    def test_custom_name(self, mock_project_config):
        runner = SemanticLayerRunner(mock_project_config, name="my_layer")
        assert runner.name == "my_layer"

    def test_stores_project_config(self, mock_project_config):
        runner = SemanticLayerRunner(mock_project_config)
        assert runner.project_config is mock_project_config


# ---------- _parse_semantic_config ----------


class TestParseSemanticConfig:
    """Test SemanticLayerRunner._parse_semantic_config."""

    def test_with_semantic_dict(self, mock_project_config):
        runner = SemanticLayerRunner(mock_project_config)
        ext = runner._parse_semantic_config()
        assert ext.connection == "sql_gold"
        assert ext.sql_output_path == "gold/views/"
        assert len(ext.layer_config.views) == 1

    def test_without_semantic_dict(self, mock_project_config):
        mock_project_config.semantic = None
        runner = SemanticLayerRunner(mock_project_config)
        ext = runner._parse_semantic_config()
        assert ext.connection is None
        assert ext.layer_config.views == []

    def test_empty_semantic_dict(self, mock_project_config):
        mock_project_config.semantic = {}
        runner = SemanticLayerRunner(mock_project_config)
        ext = runner._parse_semantic_config()
        assert ext.connection is None


# ---------- Properties ----------


class TestSemanticLayerRunnerProperties:
    """Test SemanticLayerRunner properties."""

    def test_semantic_ext_cached(self, mock_project_config):
        runner = SemanticLayerRunner(mock_project_config)
        ext1 = runner.semantic_ext
        ext2 = runner.semantic_ext
        assert ext1 is ext2

    def test_semantic_config(self, mock_project_config):
        runner = SemanticLayerRunner(mock_project_config)
        assert len(runner.semantic_config.metrics) == 1

    def test_connection_name(self, mock_project_config):
        runner = SemanticLayerRunner(mock_project_config)
        assert runner.connection_name == "sql_gold"

    def test_sql_output_path(self, mock_project_config):
        runner = SemanticLayerRunner(mock_project_config)
        assert runner.sql_output_path == "gold/views/"


# ---------- run ----------


class TestSemanticLayerRunnerRun:
    """Test SemanticLayerRunner.run method."""

    def test_no_views_returns_early(self, mock_project_config):
        mock_project_config.semantic = {"metrics": [], "views": []}
        runner = SemanticLayerRunner(mock_project_config)
        result = runner.run(execute_sql=lambda x: None)
        assert result["views_created"] == []
        assert result["views_failed"] == []
        assert result["duration"] == 0.0

    @patch("odibi.semantics.runner.SemanticStoryGenerator")
    @patch("odibi.semantics.runner.get_storage_options", return_value={})
    def test_run_calls_execute_with_story(self, mock_storage, mock_story_gen, mock_project_config):
        mock_view_ok = MagicMock()
        mock_view_ok.view_name = "vw_ok"
        mock_view_ok.status = "success"

        mock_view_fail = MagicMock()
        mock_view_fail.view_name = "vw_fail"
        mock_view_fail.status = "failed"

        mock_metadata = MagicMock()
        mock_metadata.views = [mock_view_ok, mock_view_fail]
        mock_metadata.duration = 2.5
        mock_story_gen.return_value.execute_with_story.return_value = mock_metadata

        runner = SemanticLayerRunner(mock_project_config)
        execute_sql = MagicMock()
        result = runner.run(execute_sql=execute_sql)

        mock_story_gen.return_value.execute_with_story.assert_called_once()
        assert result["views_created"] == ["vw_ok"]
        assert result["views_failed"] == ["vw_fail"]
        assert result["duration"] == 2.5


# ---------- _get_execute_sql_from_connection ----------


class TestGetExecuteSqlFromConnection:
    """Test SemanticLayerRunner._get_execute_sql_from_connection."""

    def test_no_connection_raises(self, mock_project_config):
        mock_project_config.semantic = {"metrics": []}
        runner = SemanticLayerRunner(mock_project_config)
        with pytest.raises(ValueError, match="No execute_sql"):
            runner._get_execute_sql_from_connection()

    def test_connection_not_found_raises(self, mock_project_config):
        mock_project_config.connections = {"other": MagicMock()}
        runner = SemanticLayerRunner(mock_project_config)
        with pytest.raises(ValueError, match="not found"):
            runner._get_execute_sql_from_connection()

    @patch("odibi.connections.azure_sql.AzureSQL")
    def test_valid_connection_returns_callable(self, mock_azure_sql_cls, mock_project_config):
        conn = MagicMock()
        conn.type = MagicMock()
        conn.type.value = "azure_sql"
        conn.host = "server.database.windows.net"
        conn.database = "mydb"
        conn.port = 1433
        conn.auth = None
        conn.username = None
        conn.password = None
        mock_project_config.connections = {"sql_gold": conn}

        mock_instance = MagicMock()
        mock_azure_sql_cls.return_value = mock_instance

        runner = SemanticLayerRunner(mock_project_config)
        result = runner._get_execute_sql_from_connection()

        assert result is mock_instance.execute
        mock_azure_sql_cls.assert_called_once()


# ---------- _get_write_file_from_story_connection ----------


class TestGetWriteFileFromStoryConnection:
    """Test SemanticLayerRunner._get_write_file_from_story_connection."""

    @patch("odibi.semantics.runner.get_storage_options", return_value={})
    def test_no_story_connection_returns_none(self, mock_storage, mock_project_config):
        mock_project_config.story.connection = "missing"
        mock_project_config.connections = {}
        runner = SemanticLayerRunner(mock_project_config)
        assert runner._get_write_file_from_story_connection() is None

    @patch("odibi.semantics.runner.get_storage_options", return_value={})
    def test_local_connection_returns_callable(self, mock_storage, mock_project_config):
        conn = MagicMock()
        conn.type = MagicMock()
        conn.type.value = "local"
        conn.base_path = "./data"
        mock_project_config.story.connection = "local_data"
        mock_project_config.connections = {"local_data": conn}
        runner = SemanticLayerRunner(mock_project_config)
        result = runner._get_write_file_from_story_connection()
        assert callable(result)

    @patch("odibi.semantics.runner.get_storage_options")
    def test_azure_blob_connection_returns_callable(self, mock_storage, mock_project_config):
        mock_storage.return_value = {"account_key": "key123"}
        conn = MagicMock()
        conn.type = MagicMock()
        conn.type.value = "azure_blob"
        conn.account_name = "myaccount"
        conn.container = "mycontainer"
        mock_project_config.story.connection = "az_conn"
        mock_project_config.connections = {"az_conn": conn}
        runner = SemanticLayerRunner(mock_project_config)
        result = runner._get_write_file_from_story_connection()
        assert callable(result)

    @patch("odibi.semantics.runner.get_storage_options", return_value={})
    def test_unsupported_type_returns_none(self, mock_storage, mock_project_config):
        conn = MagicMock()
        conn.type = MagicMock()
        conn.type.value = "http"
        mock_project_config.story.connection = "http_conn"
        mock_project_config.connections = {"http_conn": conn}
        runner = SemanticLayerRunner(mock_project_config)
        assert runner._get_write_file_from_story_connection() is None


# ---------- _generate_docs ----------


class TestGenerateDocs:
    """Test SemanticLayerRunner._generate_docs."""

    def test_no_story_config_returns_early(self, mock_project_config):
        mock_project_config.story = None
        runner = SemanticLayerRunner(mock_project_config)
        runner._generate_docs(MagicMock())

    def test_no_docs_config_returns_early(self, mock_project_config):
        mock_project_config.story.docs = None
        runner = SemanticLayerRunner(mock_project_config)
        runner._generate_docs(MagicMock())

    def test_docs_disabled_returns_early(self, mock_project_config):
        docs = MagicMock()
        docs.enabled = False
        mock_project_config.story.docs = docs
        runner = SemanticLayerRunner(mock_project_config)
        runner._generate_docs(MagicMock())

    @patch("odibi.semantics.runner.get_storage_options", return_value={})
    @patch("odibi.semantics.runner.get_full_stories_path", return_value="./stories")
    @patch("odibi.story.doc_generator.DocGenerator")
    def test_docs_success(
        self,
        mock_doc_gen_cls,
        mock_stories_path,
        mock_storage,
        mock_project_config,
    ):
        from odibi.config import DocsConfig

        docs_config = DocsConfig(enabled=True, output_path="docs/gen/")
        mock_project_config.story.docs = docs_config

        mock_doc_gen = MagicMock()
        mock_doc_gen.generate.return_value = {"readme": "path"}
        mock_doc_gen_cls.return_value = mock_doc_gen

        metadata = MagicMock()
        metadata.views = []
        metadata.started_at = "2025-01-01"
        metadata.completed_at = "2025-01-01"
        metadata.duration = 1.0
        metadata.views_total = 0
        metadata.views_created = 0
        metadata.views_failed = 0

        runner = SemanticLayerRunner(mock_project_config)
        runner._generate_docs(metadata, story_html_path="story.html")

        mock_doc_gen.generate.assert_called_once()


# ---------- run_semantic_layer convenience function ----------


class TestRunSemanticLayerFunction:
    """Test run_semantic_layer convenience function."""

    @patch("odibi.semantics.runner.SemanticLayerRunner")
    def test_creates_runner_and_runs(self, mock_runner_cls, mock_project_config):
        mock_runner = MagicMock()
        mock_runner.run.return_value = {"views_created": ["vw1"]}
        mock_runner_cls.return_value = mock_runner

        execute_sql = MagicMock()
        result = run_semantic_layer(mock_project_config, execute_sql)

        mock_runner_cls.assert_called_once_with(mock_project_config)
        mock_runner.run.assert_called_once()
        assert result["views_created"] == ["vw1"]

    @patch("odibi.semantics.runner.SemanticLayerRunner")
    def test_passes_all_arguments(self, mock_runner_cls, mock_project_config):
        mock_runner = MagicMock()
        mock_runner.run.return_value = {}
        mock_runner_cls.return_value = mock_runner

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

        kwargs = mock_runner.run.call_args[1]
        assert kwargs["execute_sql"] is execute_sql
        assert kwargs["save_sql_to"] == "path/"
        assert kwargs["write_file"] is write_file
        assert kwargs["generate_story"] is True
        assert kwargs["generate_lineage"] is True

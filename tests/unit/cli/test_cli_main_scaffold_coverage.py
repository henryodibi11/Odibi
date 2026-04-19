"""Tests for odibi.cli.main — command dispatch branches (lines 412-498)."""

from unittest.mock import patch

from odibi.cli.main import main


class TestMainDispatchScaffoldBranch:
    """Cover the scaffold dispatch (lines 418-425)."""

    def test_scaffold_project_dispatch(self):
        args = [
            "odibi",
            "scaffold",
            "project",
            "my_proj",
        ]
        with (
            patch("sys.argv", args),
            patch("odibi.cli.main.cmd_scaffold_project", return_value=0) as mock_fn,
        ):
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_scaffold_sql_pipeline_dispatch(self):
        args = [
            "odibi",
            "scaffold",
            "sql-pipeline",
            "ingest",
            "--source",
            "src",
            "--target",
            "tgt",
            "--tables",
            "t1,t2",
        ]
        with (
            patch("sys.argv", args),
            patch("odibi.cli.main.cmd_scaffold_sql_pipeline", return_value=0) as mock_fn,
        ):
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_scaffold_no_subcommand_prints_help(self):
        args = ["odibi", "scaffold"]
        with patch("sys.argv", args):
            result = main()
        assert result == 1


class TestMainDispatchDeferredCommands:
    """Cover deferred-import command branches (lines 432-494)."""

    def test_init_pipeline_dispatch(self):
        args = ["odibi", "init-pipeline", "my_proj"]
        with (
            patch("sys.argv", args),
            patch("odibi.cli.init_pipeline.init_pipeline_command", return_value=0) as mock_fn,
        ):
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_story_dispatch(self):
        args = ["odibi", "story", "list"]
        with (
            patch("sys.argv", args),
            patch("odibi.cli.story.story_command", return_value=0) as mock_fn,
        ):
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_catalog_dispatch(self):
        args = ["odibi", "catalog", "runs", "config.yaml"]
        with (
            patch("sys.argv", args),
            patch("odibi.cli.catalog.catalog_command", return_value=0) as mock_fn,
        ):
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_lineage_dispatch(self):
        args = ["odibi", "lineage", "upstream", "gold/customers"]
        with (
            patch("sys.argv", args),
            patch("odibi.cli.lineage.lineage_command", return_value=0) as mock_fn,
        ):
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_schema_dispatch(self):
        args = ["odibi", "schema", "history", "my_table"]
        with (
            patch("sys.argv", args),
            patch("odibi.cli.schema.schema_command", return_value=0) as mock_fn,
        ):
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_secrets_dispatch(self):
        args = ["odibi", "secrets", "init", "config.yaml"]
        with (
            patch("sys.argv", args),
            patch("odibi.cli.secrets.secrets_command", return_value=0) as mock_fn,
        ):
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_system_dispatch(self):
        args = ["odibi", "system", "sync", "config.yaml"]
        with (
            patch("sys.argv", args),
            patch("odibi.cli.system.system_command", return_value=0) as mock_fn,
        ):
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_templates_dispatch(self):
        args = ["odibi", "templates", "list"]
        with (
            patch("sys.argv", args),
            patch("odibi.cli.templates.templates_command", return_value=0) as mock_fn,
        ):
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_list_dispatch(self):
        args = ["odibi", "list", "transformers"]
        with (
            patch("sys.argv", args),
            patch("odibi.cli.list_cmd.list_command", return_value=0) as mock_fn,
        ):
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_explain_dispatch(self):
        args = ["odibi", "explain", "my_transform"]
        with (
            patch("sys.argv", args),
            patch("odibi.cli.list_cmd.explain_command", return_value=0) as mock_fn,
        ):
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_export_dispatch(self):
        args = ["odibi", "export", "--target", "airflow", "--out", "out.py"]
        with (
            patch("sys.argv", args),
            patch("odibi.cli.export.export_command", return_value=0) as mock_fn,
        ):
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_ui_dispatch(self):
        args = ["odibi", "ui", "config.yaml"]
        with patch("sys.argv", args), patch("odibi.cli.ui.ui_command", return_value=0) as mock_fn:
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_graph_dispatch(self):
        args = ["odibi", "graph", "config.yaml"]
        with (
            patch("sys.argv", args),
            patch("odibi.cli.graph.graph_command", return_value=0) as mock_fn,
        ):
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_deploy_dispatch(self):
        args = ["odibi", "deploy", "config.yaml"]
        with (
            patch("sys.argv", args),
            patch("odibi.cli.deploy.deploy_command", return_value=0) as mock_fn,
        ):
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_test_dispatch(self):
        args = ["odibi", "test"]
        with (
            patch("sys.argv", args),
            patch("odibi.cli.test.test_command", return_value=0) as mock_fn,
        ):
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_run_dispatch(self):
        args = ["odibi", "run", "pipeline.yaml"]
        with patch("sys.argv", args), patch("odibi.cli.run.run_command", return_value=0) as mock_fn:
            result = main()
        assert result == 0
        mock_fn.assert_called_once()

    def test_no_command_prints_help(self):
        args = ["odibi"]
        with patch("sys.argv", args):
            result = main()
        assert result == 1

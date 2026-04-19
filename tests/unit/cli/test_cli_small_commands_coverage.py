"""Tests for small CLI modules: templates, run, deploy, export, validate, ui."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _make_args(**kwargs):
    args = MagicMock()
    for k, v in kwargs.items():
        setattr(args, k, v)
    return args


# =========================================================================
# cli/templates.py
# =========================================================================


class TestTemplatesListCommand:
    @patch("odibi.tools.templates.list_templates")
    def test_list_json(self, mock_list, capsys):
        from odibi.cli.templates import templates_list_command

        mock_list.return_value = {"connections": ["local", "azure_blob"]}
        args = _make_args(format="json", category=None)
        ret = templates_list_command(args)
        assert ret == 0
        assert "local" in capsys.readouterr().out

    @patch("odibi.tools.templates.list_templates")
    def test_list_category_valid(self, mock_list, capsys):
        from odibi.cli.templates import templates_list_command

        mock_list.return_value = {"connections": ["local", "azure_blob"], "patterns": ["dimension"]}
        args = _make_args(format="table", category="connections")
        ret = templates_list_command(args)
        assert ret == 0
        out = capsys.readouterr().out
        assert "local" in out

    @patch("odibi.tools.templates.list_templates")
    def test_list_category_invalid(self, mock_list, capsys):
        from odibi.cli.templates import templates_list_command

        mock_list.return_value = {"connections": []}
        args = _make_args(format="table", category="bogus")
        ret = templates_list_command(args)
        assert ret == 1

    @patch("odibi.tools.templates.list_templates")
    def test_list_all(self, mock_list, capsys):
        from odibi.cli.templates import templates_list_command

        mock_list.return_value = {"connections": ["local"], "patterns": ["dim"]}
        args = _make_args(format="table", category=None)
        ret = templates_list_command(args)
        assert ret == 0
        out = capsys.readouterr().out
        assert "Connections" in out
        assert "Patterns" in out


class TestTemplatesShowCommand:
    @patch("odibi.tools.templates.show_template")
    def test_show_success(self, mock_show, capsys):
        from odibi.cli.templates import templates_show_command

        mock_show.return_value = "# Template YAML"
        args = _make_args(name="azure_blob", required_only=False, no_comments=False)
        ret = templates_show_command(args)
        assert ret == 0
        assert "Template YAML" in capsys.readouterr().out

    @patch("odibi.tools.templates.show_template")
    def test_show_error(self, mock_show, capsys):
        from odibi.cli.templates import templates_show_command

        mock_show.side_effect = ValueError("not found")
        args = _make_args(name="bogus", required_only=False, no_comments=False)
        ret = templates_show_command(args)
        assert ret == 1


class TestTemplatesTransformerCommand:
    @patch("odibi.tools.templates.show_transformer")
    def test_success(self, mock_show, capsys):
        from odibi.cli.templates import templates_transformer_command

        mock_show.return_value = "# SCD2 docs"
        args = _make_args(name="scd2", no_example=False)
        ret = templates_transformer_command(args)
        assert ret == 0

    @patch("odibi.tools.templates.show_transformer")
    def test_error(self, mock_show):
        from odibi.cli.templates import templates_transformer_command

        mock_show.side_effect = ValueError("unknown")
        args = _make_args(name="bogus", no_example=False)
        ret = templates_transformer_command(args)
        assert ret == 1


class TestTemplatesSchemaCommand:
    @patch("odibi.tools.templates.generate_json_schema")
    def test_success(self, mock_gen, capsys):
        from odibi.cli.templates import templates_schema_command

        mock_gen.return_value = {"$defs": {"A": {}, "B": {}}}
        args = _make_args(output=None, no_transformers=False, verbose=True)
        ret = templates_schema_command(args)
        assert ret == 0
        out = capsys.readouterr().out
        assert "odibi.schema.json" in out
        assert "Definitions: 2" in out

    @patch("odibi.tools.templates.generate_json_schema")
    def test_error(self, mock_gen):
        from odibi.cli.templates import templates_schema_command

        mock_gen.side_effect = Exception("schema fail")
        args = _make_args(output="out.json", no_transformers=False, verbose=False)
        ret = templates_schema_command(args)
        assert ret == 1


class TestTemplatesCommand:
    def test_dispatch_list(self):
        from odibi.cli.templates import templates_command

        with patch("odibi.cli.templates.templates_list_command", return_value=0) as m:
            args = _make_args(templates_command="list")
            ret = templates_command(args)
            assert ret == 0
            m.assert_called_once()

    def test_dispatch_show(self):
        from odibi.cli.templates import templates_command

        with patch("odibi.cli.templates.templates_show_command", return_value=0):
            args = _make_args(templates_command="show")
            assert templates_command(args) == 0

    def test_dispatch_transformer(self):
        from odibi.cli.templates import templates_command

        with patch("odibi.cli.templates.templates_transformer_command", return_value=0):
            args = _make_args(templates_command="transformer")
            assert templates_command(args) == 0

    def test_dispatch_schema(self):
        from odibi.cli.templates import templates_command

        with patch("odibi.cli.templates.templates_schema_command", return_value=0):
            args = _make_args(templates_command="schema")
            assert templates_command(args) == 0

    def test_dispatch_none(self, capsys):
        from odibi.cli.templates import templates_command

        args = _make_args(templates_command=None)
        ret = templates_command(args)
        assert ret == 1


class TestAddTemplatesParser:
    def test_parser_creation(self):
        import argparse
        from odibi.cli.templates import add_templates_parser

        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        add_templates_parser(sub)
        # Just verify no error on parse
        result = parser.parse_args(["templates", "list"])
        assert result.templates_command == "list"


# =========================================================================
# cli/validate.py
# =========================================================================


class TestValidateCommand:
    @patch("odibi.pipeline.PipelineManager")
    def test_all_valid(self, mock_pm_cls, capsys):
        from odibi.cli.validate import validate_command

        mock_manager = MagicMock()
        mock_manager._pipelines = {
            "p1": MagicMock(
                validate=MagicMock(return_value={"valid": True, "warnings": [], "errors": []})
            ),
        }
        mock_pm_cls.from_yaml.return_value = mock_manager
        args = _make_args(config="test.yaml", env=None)
        ret = validate_command(args)
        assert ret == 0
        assert "valid" in capsys.readouterr().out.lower()

    @patch("odibi.pipeline.PipelineManager")
    def test_invalid(self, mock_pm_cls, capsys):
        from odibi.cli.validate import validate_command

        mock_manager = MagicMock()
        mock_manager._pipelines = {
            "p1": MagicMock(
                validate=MagicMock(
                    return_value={
                        "valid": False,
                        "warnings": ["warn1"],
                        "errors": ["err1"],
                    }
                )
            ),
        }
        mock_pm_cls.from_yaml.return_value = mock_manager
        args = _make_args(config="test.yaml", env=None)
        ret = validate_command(args)
        assert ret == 1
        out = capsys.readouterr().out
        assert "err1" in out
        assert "warn1" in out

    @patch("odibi.pipeline.PipelineManager")
    def test_exception(self, mock_pm_cls, capsys):
        from odibi.cli.validate import validate_command

        mock_pm_cls.from_yaml.side_effect = Exception("bad yaml")
        args = _make_args(config="test.yaml", env=None)
        ret = validate_command(args)
        assert ret == 1


# =========================================================================
# cli/ui.py
# =========================================================================


class TestUiCommand:
    def test_import_error(self):
        from odibi.cli.ui import ui_command

        args = _make_args(config="test.yaml", port=8000, host="127.0.0.1")
        # Remove uvicorn from sys.modules and prevent import
        import sys

        saved = sys.modules.pop("uvicorn", None)
        try:
            with patch.dict("sys.modules", {"uvicorn": None}):
                ret = ui_command(args)
                assert ret == 1
        finally:
            if saved is not None:
                sys.modules["uvicorn"] = saved

    def test_add_ui_parser(self):
        import argparse
        from odibi.cli.ui import add_ui_parser

        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        add_ui_parser(sub)
        result = parser.parse_args(["ui", "test.yaml", "--port", "9000"])
        assert result.port == 9000


# =========================================================================
# cli/export.py
# =========================================================================


class TestExportCommand:
    def test_config_not_found(self, capsys):
        from odibi.cli.export import export_command

        args = _make_args(config="nonexistent.yaml", target="airflow", pipeline="p1", out="out.py")
        ret = export_command(args)
        assert ret == 1
        assert "not found" in capsys.readouterr().out

    @patch("odibi.cli.export.load_config_from_file")
    def test_load_config_error(self, mock_load, capsys, tmp_path):
        from odibi.cli.export import export_command

        cfg = tmp_path / "odibi.yaml"
        cfg.write_text("x: 1")
        mock_load.side_effect = Exception("parse error")
        args = _make_args(config=str(cfg), target="airflow", pipeline="p1", out="out.py")
        ret = export_command(args)
        assert ret == 1

    @patch("odibi.cli.export.AirflowExporter")
    @patch("odibi.cli.export.load_config_from_file")
    def test_airflow_no_pipeline(self, mock_load, mock_exp, capsys, tmp_path):
        from odibi.cli.export import export_command

        cfg = tmp_path / "odibi.yaml"
        cfg.write_text("x: 1")
        mock_load.return_value = MagicMock()
        args = _make_args(config=str(cfg), target="airflow", pipeline=None, out="out.py")
        ret = export_command(args)
        assert ret == 1

    @patch("odibi.cli.export.AirflowExporter")
    @patch("odibi.cli.export.load_config_from_file")
    def test_airflow_success(self, mock_load, mock_exp, capsys, tmp_path):
        from odibi.cli.export import export_command

        cfg = tmp_path / "odibi.yaml"
        cfg.write_text("x: 1")
        mock_load.return_value = MagicMock()
        mock_exp.return_value.generate_code.return_value = "# DAG code"
        out = tmp_path / "dag.py"
        args = _make_args(config=str(cfg), target="airflow", pipeline="p1", out=str(out))
        ret = export_command(args)
        assert ret == 0
        assert out.read_text().strip() == "# DAG code"

    @patch("odibi.cli.export.AirflowExporter")
    @patch("odibi.cli.export.load_config_from_file")
    def test_airflow_generate_error(self, mock_load, mock_exp, capsys, tmp_path):
        from odibi.cli.export import export_command

        cfg = tmp_path / "odibi.yaml"
        cfg.write_text("x: 1")
        mock_load.return_value = MagicMock()
        mock_exp.return_value.generate_code.side_effect = ValueError("bad pipeline")
        out = tmp_path / "dag.py"
        args = _make_args(config=str(cfg), target="airflow", pipeline="p1", out=str(out))
        ret = export_command(args)
        assert ret == 1

    @patch("odibi.cli.export.load_config_from_file")
    def test_dagster_export(self, mock_load, capsys, tmp_path):
        from odibi.cli.export import export_command

        cfg = tmp_path / "odibi.yaml"
        cfg.write_text("x: 1")
        mock_load.return_value = MagicMock()
        out = tmp_path / "definitions.py"
        args = _make_args(config=str(cfg), target="dagster", pipeline=None, out=str(out))
        ret = export_command(args)
        assert ret == 0
        content = out.read_text()
        assert "DagsterFactory" in content

    def test_add_export_parser(self):
        import argparse
        from odibi.cli.export import add_export_parser

        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        add_export_parser(sub)
        result = parser.parse_args(
            ["export", "--target", "airflow", "--out", "dag.py", "--pipeline", "p1"]
        )
        assert result.target == "airflow"


# =========================================================================
# cli/run.py
# =========================================================================


class TestRunCommand:
    @patch("odibi.cli.run.load_extensions")
    @patch("odibi.cli.run.PipelineManager")
    def test_success(self, mock_pm, mock_ext, tmp_path):
        from odibi.cli.run import run_command

        cfg = tmp_path / "odibi.yaml"
        cfg.write_text("x: 1")
        mock_result = MagicMock()
        mock_result.failed = []
        mock_result.story_path = str(tmp_path / "story.html")
        mock_result.pipeline_name = "p1"
        mock_result.duration = 1.5
        mock_pm.from_yaml.return_value.run.return_value = {"p1": mock_result}
        args = _make_args(
            config=str(cfg),
            env=None,
            dry_run=False,
            resume=False,
            parallel=False,
            workers=1,
            on_error="fail",
            pipeline_name=None,
            tag=None,
            node_name=None,
        )
        ret = run_command(args)
        assert ret == 0

    @patch("odibi.cli.run.load_extensions")
    @patch("odibi.cli.run.PipelineManager")
    def test_failure(self, mock_pm, mock_ext, tmp_path):
        from odibi.cli.run import run_command

        cfg = tmp_path / "odibi.yaml"
        cfg.write_text("x: 1")
        node_result = MagicMock()
        node_result.error = MagicMock()
        node_result.error.suggestions = ["Try this"]
        mock_result = MagicMock()
        mock_result.failed = ["node1"]
        mock_result.node_results = {"node1": node_result}
        mock_result.debug_summary.return_value = "Debug info"
        mock_pm.from_yaml.return_value.run.return_value = {"p1": mock_result}
        args = _make_args(
            config=str(cfg),
            env=None,
            dry_run=False,
            resume=False,
            parallel=False,
            workers=1,
            on_error="fail",
            pipeline_name=None,
            tag=None,
            node_name=None,
        )
        ret = run_command(args)
        assert ret == 1

    @patch("odibi.cli.run.load_extensions")
    @patch("odibi.cli.run.PipelineManager")
    def test_exception(self, mock_pm, mock_ext, tmp_path):
        from odibi.cli.run import run_command

        cfg = tmp_path / "odibi.yaml"
        cfg.write_text("x: 1")
        mock_pm.from_yaml.side_effect = Exception("yaml error")
        args = _make_args(
            config=str(cfg),
            env=None,
            dry_run=False,
            resume=False,
            parallel=False,
            workers=1,
            on_error="fail",
        )
        ret = run_command(args)
        assert ret == 1

    @patch("odibi.cli.run.load_extensions")
    @patch("odibi.cli.run.PipelineManager")
    def test_failure_no_suggestions(self, mock_pm, mock_ext, tmp_path):
        from odibi.cli.run import run_command

        cfg = tmp_path / "odibi.yaml"
        cfg.write_text("x: 1")
        node_result = MagicMock()
        node_result.error = MagicMock(spec=[])  # no suggestions attr
        node_result.error.original_error = MagicMock(spec=[])  # no suggestions attr
        mock_result = MagicMock()
        mock_result.failed = ["node1"]
        mock_result.node_results = {"node1": node_result}
        mock_result.debug_summary.return_value = None
        mock_pm.from_yaml.return_value.run.return_value = {"p1": mock_result}
        args = _make_args(
            config=str(cfg),
            env=None,
            dry_run=False,
            resume=False,
            parallel=False,
            workers=1,
            on_error="fail",
            pipeline_name=None,
            tag=None,
            node_name=None,
        )
        ret = run_command(args)
        assert ret == 1


# =========================================================================
# cli/deploy.py
# =========================================================================


class TestDeployCommand:
    @patch("odibi.cli.deploy.load_extensions")
    @patch("odibi.cli.deploy.PipelineManager")
    def test_success(self, mock_pm, mock_ext, tmp_path):
        from odibi.cli.deploy import deploy_command

        cfg = tmp_path / "odibi.yaml"
        cfg.write_text("x: 1")
        node = MagicMock()
        node.name = "n1"
        pipeline = MagicMock()
        pipeline.pipeline = "p1"
        pipeline.nodes = [node]
        mock_manager = MagicMock()
        mock_manager.catalog_manager = MagicMock()
        mock_manager.catalog_manager.base_path = str(tmp_path)
        mock_manager.project_config.pipelines = [pipeline]
        mock_pm.from_yaml.return_value = mock_manager
        args = _make_args(config=str(cfg), env=None)
        ret = deploy_command(args)
        assert ret == 0
        mock_manager.catalog_manager.register_pipeline.assert_called_once()
        mock_manager.catalog_manager.register_node.assert_called_once()

    @patch("odibi.cli.deploy.load_extensions")
    @patch("odibi.cli.deploy.PipelineManager")
    def test_no_catalog(self, mock_pm, mock_ext, tmp_path):
        from odibi.cli.deploy import deploy_command

        cfg = tmp_path / "odibi.yaml"
        cfg.write_text("x: 1")
        mock_manager = MagicMock()
        mock_manager.catalog_manager = None
        mock_pm.from_yaml.return_value = mock_manager
        args = _make_args(config=str(cfg), env=None)
        ret = deploy_command(args)
        assert ret == 1

    @patch("odibi.cli.deploy.load_extensions")
    @patch("odibi.cli.deploy.PipelineManager")
    def test_exception(self, mock_pm, mock_ext, tmp_path):
        from odibi.cli.deploy import deploy_command

        cfg = tmp_path / "odibi.yaml"
        cfg.write_text("x: 1")
        mock_pm.from_yaml.side_effect = Exception("bad config")
        args = _make_args(config=str(cfg), env=None)
        ret = deploy_command(args)
        assert ret == 1

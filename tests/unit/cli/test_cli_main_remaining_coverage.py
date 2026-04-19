"""Tests for odibi.cli.main — cmd_discover, cmd_scaffold_project, cmd_scaffold_sql_pipeline."""

import json
from argparse import Namespace
from unittest.mock import MagicMock, patch

from odibi.cli.main import cmd_discover, cmd_scaffold_project, cmd_scaffold_sql_pipeline


# ---------------------------------------------------------------------------
# cmd_discover
# ---------------------------------------------------------------------------


class TestCmdDiscover:
    def test_discover_json_format(self, capsys):
        args = Namespace(
            config="test.yaml",
            connection="my_conn",
            dataset="my_db",
            profile=True,
            schema=True,
            format="json",
        )
        mock_pm = MagicMock()
        mock_pm.discover.return_value = {"tables": ["t1", "t2"]}

        with patch("odibi.pipeline.PipelineManager") as mock_cls:
            mock_cls.from_yaml.return_value = mock_pm
            result = cmd_discover(args)

        assert result == 0
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["tables"] == ["t1", "t2"]

    def test_discover_auto_format(self, capsys):
        args = Namespace(
            config="test.yaml",
            connection="my_conn",
            dataset="my_db",
            profile=False,
            schema=False,
            format="auto",
        )
        mock_pm = MagicMock()
        mock_pm.discover.return_value = {"tables": ["t1"]}

        with patch("odibi.pipeline.PipelineManager") as mock_cls:
            mock_cls.from_yaml.return_value = mock_pm
            result = cmd_discover(args)

        assert result == 0
        output = capsys.readouterr().out
        assert "my_conn" in output
        assert "1 tables" in output

    def test_discover_auto_format_no_dataset(self, capsys):
        args = Namespace(
            config="test.yaml",
            connection="my_conn",
            dataset=None,
            profile=False,
            schema=False,
            format="auto",
        )
        mock_pm = MagicMock()
        mock_pm.discover.return_value = {"tables": []}

        with patch("odibi.pipeline.PipelineManager") as mock_cls:
            mock_cls.from_yaml.return_value = mock_pm
            result = cmd_discover(args)

        assert result == 0
        output = capsys.readouterr().out
        assert "Dataset" not in output

    def test_discover_exception_returns_1(self, capsys):
        args = Namespace(
            config="test.yaml",
            connection="my_conn",
            dataset=None,
            profile=False,
            schema=False,
            format="auto",
        )
        with patch("odibi.pipeline.PipelineManager") as mock_cls:
            mock_cls.from_yaml.side_effect = RuntimeError("connection failed")
            result = cmd_discover(args)

        assert result == 1
        assert "connection failed" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# cmd_scaffold_project
# ---------------------------------------------------------------------------


class TestCmdScaffoldProject:
    def test_scaffold_project_success(self, tmp_path, capsys, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = Namespace(name="my_project", force=False)
        mock_pm = MagicMock()
        mock_pm.scaffold_project.return_value = "project: my_project\n"

        with patch("odibi.pipeline.PipelineManager", return_value=mock_pm):
            result = cmd_scaffold_project(args)

        assert result == 0
        assert (tmp_path / "my_project.yaml").exists()
        assert "Generated project scaffold" in capsys.readouterr().out

    def test_scaffold_project_file_exists_no_force(self, tmp_path, capsys, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "existing.yaml").write_text("old")
        args = Namespace(name="existing", force=False)
        mock_pm = MagicMock()
        mock_pm.scaffold_project.return_value = "new content"

        with patch("odibi.pipeline.PipelineManager", return_value=mock_pm):
            result = cmd_scaffold_project(args)

        assert result == 1
        assert "already exists" in capsys.readouterr().err

    def test_scaffold_project_file_exists_with_force(self, tmp_path, capsys, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "overwrite.yaml").write_text("old")
        args = Namespace(name="overwrite", force=True)
        mock_pm = MagicMock()
        mock_pm.scaffold_project.return_value = "new content"

        with patch("odibi.pipeline.PipelineManager", return_value=mock_pm):
            result = cmd_scaffold_project(args)

        assert result == 0
        assert (tmp_path / "overwrite.yaml").read_text() == "new content"

    def test_scaffold_project_exception_returns_1(self, capsys):
        args = Namespace(name="fail_proj", force=False)

        with patch("odibi.pipeline.PipelineManager", side_effect=RuntimeError("boom")):
            result = cmd_scaffold_project(args)

        assert result == 1
        assert "boom" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# cmd_scaffold_sql_pipeline
# ---------------------------------------------------------------------------


class TestCmdScaffoldSqlPipeline:
    def test_scaffold_sql_pipeline_success(self, tmp_path, capsys, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = Namespace(
            config="test.yaml",
            name="ingest",
            source="src_conn",
            target="tgt_conn",
            tables="t1,t2,t3",
            force=False,
        )
        mock_pm = MagicMock()
        mock_pm.scaffold_sql_pipeline.return_value = "pipeline: ingest\n"

        with patch("odibi.pipeline.PipelineManager") as mock_cls:
            mock_cls.from_yaml.return_value = mock_pm
            result = cmd_scaffold_sql_pipeline(args)

        assert result == 0
        assert (tmp_path / "ingest_pipeline.yaml").exists()
        output = capsys.readouterr().out
        assert "Generated SQL pipeline scaffold" in output
        assert "t1" in output

    def test_scaffold_sql_pipeline_file_exists_no_force(self, tmp_path, capsys, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "dup_pipeline.yaml").write_text("old")
        args = Namespace(
            config="test.yaml",
            name="dup",
            source="src",
            target="tgt",
            tables="t1",
            force=False,
        )
        mock_pm = MagicMock()
        mock_pm.scaffold_sql_pipeline.return_value = "new"

        with patch("odibi.pipeline.PipelineManager") as mock_cls:
            mock_cls.from_yaml.return_value = mock_pm
            result = cmd_scaffold_sql_pipeline(args)

        assert result == 1
        assert "already exists" in capsys.readouterr().err

    def test_scaffold_sql_pipeline_file_exists_with_force(self, tmp_path, capsys, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "overwrite_pipeline.yaml").write_text("old")
        args = Namespace(
            config="test.yaml",
            name="overwrite",
            source="src",
            target="tgt",
            tables="t1",
            force=True,
        )
        mock_pm = MagicMock()
        mock_pm.scaffold_sql_pipeline.return_value = "new"

        with patch("odibi.pipeline.PipelineManager") as mock_cls:
            mock_cls.from_yaml.return_value = mock_pm
            result = cmd_scaffold_sql_pipeline(args)

        assert result == 0
        assert (tmp_path / "overwrite_pipeline.yaml").read_text() == "new"

    def test_scaffold_sql_pipeline_empty_tables(self, tmp_path, capsys, monkeypatch):
        monkeypatch.chdir(tmp_path)
        args = Namespace(
            config="test.yaml",
            name="empty",
            source="src",
            target="tgt",
            tables="",
            force=False,
        )
        mock_pm = MagicMock()
        mock_pm.scaffold_sql_pipeline.return_value = "pipeline: empty\n"

        with patch("odibi.pipeline.PipelineManager") as mock_cls:
            mock_cls.from_yaml.return_value = mock_pm
            result = cmd_scaffold_sql_pipeline(args)

        assert result == 0
        mock_pm.scaffold_sql_pipeline.assert_called_once_with(
            pipeline_name="empty",
            source_connection="src",
            target_connection="tgt",
            tables=[],
        )

    def test_scaffold_sql_pipeline_exception_returns_1(self, capsys):
        args = Namespace(
            config="test.yaml",
            name="fail",
            source="src",
            target="tgt",
            tables="t1",
            force=False,
        )
        with patch("odibi.pipeline.PipelineManager") as mock_cls:
            mock_cls.from_yaml.side_effect = RuntimeError("nope")
            result = cmd_scaffold_sql_pipeline(args)

        assert result == 1
        assert "nope" in capsys.readouterr().err

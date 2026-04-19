"""Tests for odibi.cli.story — full coverage of CLI story commands."""

import json
import os
import time

from unittest.mock import MagicMock, patch, mock_open

from odibi.cli.story import (
    story_command,
    generate_command,
    diff_command,
    list_command,
    last_command,
    show_command,
    add_story_parser,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_args(**overrides):
    args = MagicMock()
    for k, v in overrides.items():
        setattr(args, k, v)
    return args


def _write_json(path, data):
    path.write_text(json.dumps(data))


def _story_data(**overrides):
    base = {
        "pipeline_name": "test_pipeline",
        "duration": 5.0,
        "success_rate": 100.0,
        "total_rows_processed": 1000,
        "success": True,
        "nodes": [
            {
                "node_name": "load",
                "duration": 2.0,
                "rows_in": 0,
                "rows_out": 1000,
                "status": "success",
            }
        ],
    }
    base.update(overrides)
    return base


# ===========================================================================
# 1. story_command dispatcher
# ===========================================================================


class TestStoryCommandDispatcher:
    @patch("odibi.cli.story.generate_command", return_value=0)
    def test_dispatches_generate(self, mock_gen):
        args = _make_args(story_command="generate")
        assert story_command(args) == 0
        mock_gen.assert_called_once_with(args)

    @patch("odibi.cli.story.diff_command", return_value=0)
    def test_dispatches_diff(self, mock_diff):
        args = _make_args(story_command="diff")
        assert story_command(args) == 0
        mock_diff.assert_called_once_with(args)

    @patch("odibi.cli.story.list_command", return_value=0)
    def test_dispatches_list(self, mock_list):
        args = _make_args(story_command="list")
        assert story_command(args) == 0
        mock_list.assert_called_once_with(args)

    @patch("odibi.cli.story.last_command", return_value=0)
    def test_dispatches_last(self, mock_last):
        args = _make_args(story_command="last")
        assert story_command(args) == 0
        mock_last.assert_called_once_with(args)

    @patch("odibi.cli.story.show_command", return_value=0)
    def test_dispatches_show(self, mock_show):
        args = _make_args(story_command="show")
        assert story_command(args) == 0
        mock_show.assert_called_once_with(args)

    def test_unknown_subcommand_returns_1(self, capsys):
        args = _make_args(story_command="bogus")
        assert story_command(args) == 1
        out = capsys.readouterr().out
        assert "Unknown story command" in out
        assert "bogus" in out


# ===========================================================================
# 2. generate_command
# ===========================================================================


class TestGenerateCommand:
    def test_file_not_found_returns_1(self, capsys):
        args = _make_args(
            config="nonexistent.yaml",
            output=None,
            format="html",
            no_validate=False,
            no_diagram=False,
            theme="default",
            verbose=False,
        )
        result = generate_command(args)
        assert result == 1
        assert "not found" in capsys.readouterr().out

    @patch("odibi.cli.story.DocStoryGenerator")
    @patch("odibi.cli.story.ProjectConfig")
    @patch("odibi.cli.story.yaml.safe_load")
    @patch("builtins.open", new_callable=mock_open, read_data="dummy")
    def test_valid_config_generates(self, m_open, m_yaml, m_config, m_gen, capsys):
        pipeline = MagicMock()
        pipeline.pipeline = "my_pipe"
        m_config.return_value = MagicMock(pipelines=[pipeline])
        m_yaml.return_value = {"pipelines": [{}]}
        m_gen.return_value.generate.return_value = "out.html"

        args = _make_args(
            config="ok.yaml",
            output="out.html",
            format="html",
            no_validate=False,
            no_diagram=False,
            theme=None,
            verbose=False,
        )
        result = generate_command(args)
        assert result == 0
        m_gen.return_value.generate.assert_called_once()
        assert "Documentation generated" in capsys.readouterr().out

    @patch("odibi.cli.story.ProjectConfig")
    @patch("odibi.cli.story.yaml.safe_load")
    @patch("builtins.open", new_callable=mock_open, read_data="dummy")
    def test_no_pipelines_returns_1(self, m_open, m_yaml, m_config, capsys):
        m_config.return_value = MagicMock(pipelines=[])
        m_yaml.return_value = {}

        args = _make_args(
            config="ok.yaml",
            output=None,
            format="html",
            no_validate=False,
            no_diagram=False,
            theme="default",
            verbose=False,
        )
        result = generate_command(args)
        assert result == 1
        assert "No pipelines" in capsys.readouterr().out

    @patch("odibi.cli.story.DocStoryGenerator")
    @patch("odibi.cli.story.ProjectConfig")
    @patch("odibi.cli.story.yaml.safe_load")
    @patch("builtins.open", new_callable=mock_open, read_data="dummy")
    def test_value_error_returns_1(self, m_open, m_yaml, m_config, m_gen, capsys):
        pipeline = MagicMock()
        pipeline.pipeline = "p"
        m_config.return_value = MagicMock(pipelines=[pipeline])
        m_yaml.return_value = {}
        m_gen.return_value.generate.side_effect = ValueError("bad config")

        args = _make_args(
            config="ok.yaml",
            output="out.html",
            format="html",
            no_validate=False,
            no_diagram=False,
            theme=None,
            verbose=False,
        )
        result = generate_command(args)
        assert result == 1
        assert "Validation error" in capsys.readouterr().out

    @patch("odibi.cli.story.DocStoryGenerator")
    @patch("odibi.cli.story.ProjectConfig")
    @patch("odibi.cli.story.yaml.safe_load")
    @patch("builtins.open", new_callable=mock_open, read_data="dummy")
    def test_generic_exception_verbose_prints_traceback(
        self, m_open, m_yaml, m_config, m_gen, capsys
    ):
        pipeline = MagicMock()
        pipeline.pipeline = "p"
        m_config.return_value = MagicMock(pipelines=[pipeline])
        m_yaml.return_value = {}
        m_gen.return_value.generate.side_effect = RuntimeError("boom")

        args = _make_args(
            config="ok.yaml",
            output="out.html",
            format="html",
            no_validate=False,
            no_diagram=False,
            theme=None,
            verbose=True,
        )
        result = generate_command(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "Error generating documentation" in captured.out
        assert "Traceback" in captured.err

    @patch("odibi.cli.story.get_theme", create=True)
    @patch("odibi.cli.story.DocStoryGenerator")
    @patch("odibi.cli.story.ProjectConfig")
    @patch("odibi.cli.story.yaml.safe_load")
    @patch("builtins.open", new_callable=mock_open, read_data="dummy")
    def test_html_format_with_theme(self, m_open, m_yaml, m_config, m_gen, m_theme, capsys):
        pipeline = MagicMock()
        pipeline.pipeline = "p"
        m_config.return_value = MagicMock(pipelines=[pipeline])
        m_yaml.return_value = {}
        theme_obj = MagicMock(name="corporate")
        m_theme.return_value = theme_obj
        m_gen.return_value.generate.return_value = "out.html"

        with patch("odibi.story.themes.get_theme", m_theme):
            args = _make_args(
                config="ok.yaml",
                output="out.html",
                format="html",
                no_validate=False,
                no_diagram=False,
                theme="corporate",
                verbose=False,
            )
            result = generate_command(args)

        assert result == 0

    @patch("odibi.cli.story.DocStoryGenerator")
    @patch("odibi.cli.story.ProjectConfig")
    @patch("odibi.cli.story.yaml.safe_load")
    @patch("builtins.open", new_callable=mock_open, read_data="dummy")
    def test_output_path_auto_generation(self, m_open, m_yaml, m_config, m_gen, capsys):
        pipeline = MagicMock()
        pipeline.pipeline = "sales"
        m_config.return_value = MagicMock(pipelines=[pipeline])
        m_yaml.return_value = {}
        m_gen.return_value.generate.return_value = "docs/sales_documentation.json"

        args = _make_args(
            config="ok.yaml",
            output=None,
            format="json",
            no_validate=False,
            no_diagram=False,
            theme=None,
            verbose=False,
        )
        result = generate_command(args)
        assert result == 0
        call_kwargs = m_gen.return_value.generate.call_args
        assert "sales_documentation.json" in call_kwargs.kwargs.get(
            "output_path", call_kwargs[1].get("output_path", "")
        ) or "sales_documentation.json" in str(call_kwargs)


# ===========================================================================
# 3. diff_command
# ===========================================================================


class TestDiffCommand:
    def test_two_valid_json_returns_0(self, tmp_path, capsys):
        s1 = tmp_path / "s1.json"
        s2 = tmp_path / "s2.json"
        _write_json(s1, _story_data(duration=5.0))
        _write_json(s2, _story_data(duration=4.0))

        args = _make_args(story1=str(s1), story2=str(s2), detailed=False, verbose=False)
        assert diff_command(args) == 0
        out = capsys.readouterr().out
        assert "Comparison Results" in out

    def test_file_not_found_returns_1(self, capsys):
        args = _make_args(
            story1="missing1.json",
            story2="missing2.json",
            detailed=False,
            verbose=False,
        )
        assert diff_command(args) == 1
        assert "not found" in capsys.readouterr().out

    def test_invalid_json_returns_1(self, tmp_path, capsys):
        bad = tmp_path / "bad.json"
        bad.write_text("{not json")
        good = tmp_path / "good.json"
        _write_json(good, _story_data())

        args = _make_args(story1=str(bad), story2=str(good), detailed=False, verbose=False)
        assert diff_command(args) == 1
        assert "Invalid JSON" in capsys.readouterr().out

    def test_detailed_mode_node_comparison(self, tmp_path, capsys):
        s1 = tmp_path / "s1.json"
        s2 = tmp_path / "s2.json"
        _write_json(
            s1,
            _story_data(
                nodes=[
                    {"node_name": "load", "duration": 2.0, "rows_out": 100, "status": "success"},
                ]
            ),
        )
        _write_json(
            s2,
            _story_data(
                nodes=[
                    {"node_name": "load", "duration": 1.5, "rows_out": 200, "status": "success"},
                ]
            ),
        )

        args = _make_args(story1=str(s1), story2=str(s2), detailed=True, verbose=False)
        assert diff_command(args) == 0
        out = capsys.readouterr().out
        assert "Node-Level Details" in out
        assert "load" in out

    def test_time_diff_slower(self, tmp_path, capsys):
        s1 = tmp_path / "s1.json"
        s2 = tmp_path / "s2.json"
        _write_json(s1, _story_data(duration=3.0))
        _write_json(s2, _story_data(duration=5.0))

        args = _make_args(story1=str(s1), story2=str(s2), detailed=False, verbose=False)
        diff_command(args)
        assert "slower" in capsys.readouterr().out

    def test_time_diff_faster(self, tmp_path, capsys):
        s1 = tmp_path / "s1.json"
        s2 = tmp_path / "s2.json"
        _write_json(s1, _story_data(duration=5.0))
        _write_json(s2, _story_data(duration=3.0))

        args = _make_args(story1=str(s1), story2=str(s2), detailed=False, verbose=False)
        diff_command(args)
        assert "faster" in capsys.readouterr().out

    def test_time_diff_zero(self, tmp_path, capsys):
        s1 = tmp_path / "s1.json"
        s2 = tmp_path / "s2.json"
        _write_json(s1, _story_data(duration=5.0))
        _write_json(s2, _story_data(duration=5.0))

        args = _make_args(story1=str(s1), story2=str(s2), detailed=False, verbose=False)
        diff_command(args)
        assert "No change" in capsys.readouterr().out

    def test_row_diff_display(self, tmp_path, capsys):
        s1 = tmp_path / "s1.json"
        s2 = tmp_path / "s2.json"
        _write_json(s1, _story_data(total_rows_processed=1000))
        _write_json(s2, _story_data(total_rows_processed=1500))

        args = _make_args(story1=str(s1), story2=str(s2), detailed=False, verbose=False)
        diff_command(args)
        out = capsys.readouterr().out
        assert "+500" in out

    def test_nodes_added_in_detailed(self, tmp_path, capsys):
        s1 = tmp_path / "s1.json"
        s2 = tmp_path / "s2.json"
        _write_json(s1, _story_data(nodes=[]))
        _write_json(
            s2,
            _story_data(
                nodes=[
                    {"node_name": "new_node", "duration": 1.0, "rows_out": 10, "status": "success"},
                ]
            ),
        )

        args = _make_args(story1=str(s1), story2=str(s2), detailed=True, verbose=False)
        diff_command(args)
        out = capsys.readouterr().out
        assert "Added in Story 2" in out

    def test_nodes_removed_in_detailed(self, tmp_path, capsys):
        s1 = tmp_path / "s1.json"
        s2 = tmp_path / "s2.json"
        _write_json(
            s1,
            _story_data(
                nodes=[
                    {"node_name": "old_node", "duration": 1.0, "rows_out": 10, "status": "success"},
                ]
            ),
        )
        _write_json(s2, _story_data(nodes=[]))

        args = _make_args(story1=str(s1), story2=str(s2), detailed=True, verbose=False)
        diff_command(args)
        out = capsys.readouterr().out
        assert "Removed in Story 2" in out

    def test_status_change_detection(self, tmp_path, capsys):
        s1 = tmp_path / "s1.json"
        s2 = tmp_path / "s2.json"
        _write_json(
            s1,
            _story_data(
                nodes=[
                    {"node_name": "n", "duration": 1.0, "rows_out": 10, "status": "success"},
                ]
            ),
        )
        _write_json(
            s2,
            _story_data(
                nodes=[
                    {"node_name": "n", "duration": 1.0, "rows_out": 10, "status": "failed"},
                ]
            ),
        )

        args = _make_args(story1=str(s1), story2=str(s2), detailed=True, verbose=False)
        diff_command(args)
        out = capsys.readouterr().out
        assert "Status changed" in out
        assert "success" in out
        assert "failed" in out


# ===========================================================================
# 4. list_command
# ===========================================================================


class TestListCommand:
    def test_directory_not_found_returns_1(self, capsys):
        args = _make_args(directory="nonexistent_dir", limit=10)
        assert list_command(args) == 1
        assert "not found" in capsys.readouterr().out

    def test_no_story_files_returns_0(self, tmp_path, capsys):
        args = _make_args(directory=str(tmp_path), limit=10)
        assert list_command(args) == 0
        assert "No story files found" in capsys.readouterr().out

    def test_files_found_prints_sorted(self, tmp_path, capsys):
        f1 = tmp_path / "a.json"
        f1.write_text("{}")
        time.sleep(0.05)
        f2 = tmp_path / "b.json"
        f2.write_text("{}")

        args = _make_args(directory=str(tmp_path), limit=10)
        assert list_command(args) == 0
        out = capsys.readouterr().out
        assert "a.json" in out
        assert "b.json" in out
        # newest first
        assert out.index("b.json") < out.index("a.json")

    def test_limit_applied(self, tmp_path, capsys):
        for i in range(5):
            (tmp_path / f"story_{i}.json").write_text("{}")

        args = _make_args(directory=str(tmp_path), limit=2)
        assert list_command(args) == 0
        out = capsys.readouterr().out
        assert "and 3 more" in out

    def test_more_files_than_limit_shows_count(self, tmp_path, capsys):
        for i in range(4):
            (tmp_path / f"s{i}.html").write_text("<html></html>")

        args = _make_args(directory=str(tmp_path), limit=1)
        assert list_command(args) == 0
        out = capsys.readouterr().out
        assert "and 3 more" in out

    def test_size_formatting_bytes(self, tmp_path, capsys):
        f = tmp_path / "tiny.json"
        f.write_text("{}")  # a few bytes

        args = _make_args(directory=str(tmp_path), limit=10)
        list_command(args)
        out = capsys.readouterr().out
        assert "B" in out

    def test_size_formatting_kb(self, tmp_path, capsys):
        f = tmp_path / "medium.json"
        f.write_text("x" * 2048)  # ~2 KB

        args = _make_args(directory=str(tmp_path), limit=10)
        list_command(args)
        out = capsys.readouterr().out
        assert "KB" in out

    def test_size_formatting_mb(self, tmp_path, capsys):
        f = tmp_path / "large.json"
        f.write_text("x" * (1024 * 1024 + 1))  # >1 MB

        args = _make_args(directory=str(tmp_path), limit=10)
        list_command(args)
        out = capsys.readouterr().out
        assert "MB" in out


# ===========================================================================
# 5. last_command
# ===========================================================================


class TestLastCommand:
    def test_no_story_files_returns_1(self, tmp_path, capsys):
        orig = os.getcwd()
        os.chdir(tmp_path)
        try:
            args = _make_args(node=None)
            assert last_command(args) == 1
            assert "No story files found" in capsys.readouterr().out
        finally:
            os.chdir(orig)

    @patch("webbrowser.open")
    def test_html_file_opens_browser(self, m_wb_open, tmp_path, capsys):
        stories_dir = tmp_path / "stories"
        stories_dir.mkdir()
        html_file = stories_dir / "run.html"
        html_file.write_text("<html></html>")

        orig = os.getcwd()
        os.chdir(tmp_path)
        try:
            args = _make_args(node=None)
            assert last_command(args) == 0
            m_wb_open.assert_called_once()
            assert "Opening in browser" in capsys.readouterr().out
        finally:
            os.chdir(orig)

    def test_json_file_prints_path(self, tmp_path, capsys):
        stories_dir = tmp_path / "stories"
        stories_dir.mkdir()
        json_file = stories_dir / "run.json"
        _write_json(json_file, _story_data())

        orig = os.getcwd()
        os.chdir(tmp_path)
        try:
            args = _make_args(node=None)
            assert last_command(args) == 0
            out = capsys.readouterr().out
            assert "run.json" in out
        finally:
            os.chdir(orig)

    def test_node_filter_json_found(self, tmp_path, capsys):
        stories_dir = tmp_path / "stories"
        stories_dir.mkdir()
        json_file = stories_dir / "run.json"
        _write_json(
            json_file,
            _story_data(
                nodes=[
                    {
                        "node_name": "dim_customer",
                        "status": "success",
                        "duration": 1.5,
                        "rows_in": 100,
                        "rows_out": 100,
                    }
                ]
            ),
        )

        orig = os.getcwd()
        os.chdir(tmp_path)
        try:
            args = _make_args(node="dim_customer")
            assert last_command(args) == 0
            out = capsys.readouterr().out
            assert "dim_customer" in out
            assert "Status" in out
        finally:
            os.chdir(orig)

    def test_node_filter_json_not_found_returns_1(self, tmp_path, capsys):
        stories_dir = tmp_path / "stories"
        stories_dir.mkdir()
        json_file = stories_dir / "run.json"
        _write_json(json_file, _story_data())

        orig = os.getcwd()
        os.chdir(tmp_path)
        try:
            args = _make_args(node="nonexistent_node")
            assert last_command(args) == 1
            out = capsys.readouterr().out
            assert "not found" in out
        finally:
            os.chdir(orig)

    def test_node_filter_html_prints_message(self, tmp_path, capsys):
        stories_dir = tmp_path / "stories"
        stories_dir.mkdir()
        html_file = stories_dir / "run.html"
        html_file.write_text("<html></html>")

        orig = os.getcwd()
        os.chdir(tmp_path)
        try:
            args = _make_args(node="some_node")
            assert last_command(args) == 0
            out = capsys.readouterr().out
            assert "Node filtering only works with JSON" in out
        finally:
            os.chdir(orig)

    def test_node_with_error_and_paths(self, tmp_path, capsys):
        stories_dir = tmp_path / "stories"
        stories_dir.mkdir()
        json_file = stories_dir / "run.json"
        _write_json(
            json_file,
            _story_data(
                nodes=[
                    {
                        "node_name": "failing_node",
                        "status": "failed",
                        "duration": 0.5,
                        "rows_in": 0,
                        "rows_out": 0,
                        "error": "Something went wrong",
                        "source_path": "/data/source",
                        "target_path": "/data/target",
                    }
                ]
            ),
        )

        orig = os.getcwd()
        os.chdir(tmp_path)
        try:
            args = _make_args(node="failing_node")
            assert last_command(args) == 0
            out = capsys.readouterr().out
            assert "Something went wrong" in out
            assert "/data/source" in out
            assert "/data/target" in out
        finally:
            os.chdir(orig)


# ===========================================================================
# 6. show_command
# ===========================================================================


class TestShowCommand:
    def test_file_not_found_returns_1(self, capsys):
        args = _make_args(path="nonexistent.json")
        assert show_command(args) == 1
        assert "not found" in capsys.readouterr().out

    @patch("webbrowser.open")
    def test_html_opens_browser(self, m_wb_open, tmp_path, capsys):
        html = tmp_path / "story.html"
        html.write_text("<html></html>")

        args = _make_args(path=str(html))
        assert show_command(args) == 0
        m_wb_open.assert_called_once()
        assert "Opening in browser" in capsys.readouterr().out

    def test_json_prints_summary(self, tmp_path, capsys):
        f = tmp_path / "story.json"
        _write_json(
            f,
            _story_data(
                pipeline_name="sales_pipe",
                duration=12.34,
                success=True,
                nodes=[
                    {"node_name": "node_a", "status": "success", "duration": 3.0},
                    {"node_name": "node_b", "status": "failed", "duration": 1.0},
                ],
            ),
        )

        args = _make_args(path=str(f))
        assert show_command(args) == 0
        out = capsys.readouterr().out
        assert "sales_pipe" in out
        assert "12.34" in out
        assert "node_a" in out
        assert "node_b" in out
        assert "Nodes: 2" in out

    def test_other_file_type_prints_editor_message(self, tmp_path, capsys):
        f = tmp_path / "story.txt"
        f.write_text("plain text")

        args = _make_args(path=str(f))
        assert show_command(args) == 0
        assert "text editor" in capsys.readouterr().out


# ===========================================================================
# 7. add_story_parser
# ===========================================================================


class TestAddStoryParser:
    def test_creates_subparsers_without_error(self):
        import argparse

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        result = add_story_parser(subparsers)
        assert result is not None

        # All subcommands parseable
        for cmd_line in [
            ["story", "generate", "c.yaml"],
            ["story", "diff", "a.json", "b.json"],
            ["story", "list"],
            ["story", "last"],
            ["story", "show", "x.json"],
        ]:
            args = parser.parse_args(cmd_line)
            assert args.command == "story"

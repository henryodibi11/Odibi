"""Tests for story CLI commands."""

import json
from argparse import Namespace

from odibi.cli.story import diff_command, list_command


class TestGenerateCommand:
    """Tests for story generate CLI command."""

    def test_generate_command_help(self):
        """Should have generate command available."""
        import argparse

        from odibi.cli.story import add_story_parser

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_story_parser(subparsers)

        # Should be able to parse story generate
        args = parser.parse_args(["story", "generate", "config.yaml"])
        assert args.command == "story"
        assert args.story_command == "generate"
        assert args.config == "config.yaml"

    def test_generate_command_with_output(self):
        """Should accept output parameter."""
        import argparse

        from odibi.cli.story import add_story_parser

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_story_parser(subparsers)

        args = parser.parse_args(
            ["story", "generate", "config.yaml", "--output", "docs/my_doc.html"]
        )

        assert args.output == "docs/my_doc.html"

    def test_generate_command_with_format(self):
        """Should accept format parameter."""
        import argparse

        from odibi.cli.story import add_story_parser

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_story_parser(subparsers)

        args = parser.parse_args(["story", "generate", "config.yaml", "--format", "markdown"])

        assert args.format == "markdown"

    def test_generate_command_no_validate_flag(self):
        """Should support --no-validate flag."""
        import argparse

        from odibi.cli.story import add_story_parser

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_story_parser(subparsers)

        args = parser.parse_args(["story", "generate", "config.yaml", "--no-validate"])

        assert args.no_validate is True

    def test_generate_command_with_theme(self):
        """Should accept theme parameter."""
        import argparse

        from odibi.cli.story import add_story_parser

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_story_parser(subparsers)

        args = parser.parse_args(["story", "generate", "config.yaml", "--theme", "dark"])

        assert args.theme == "dark"


class TestDiffCommand:
    """Tests for story diff CLI command."""

    def test_diff_command_help(self):
        """Should have diff command available."""
        import argparse

        from odibi.cli.story import add_story_parser

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_story_parser(subparsers)

        args = parser.parse_args(["story", "diff", "story1.json", "story2.json"])

        assert args.command == "story"
        assert args.story_command == "diff"
        assert args.story1 == "story1.json"
        assert args.story2 == "story2.json"

    def test_diff_command_detailed_flag(self):
        """Should support --detailed flag."""
        import argparse

        from odibi.cli.story import add_story_parser

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_story_parser(subparsers)

        args = parser.parse_args(["story", "diff", "story1.json", "story2.json", "--detailed"])

        assert args.detailed is True

    def test_diff_command_execution(self, tmp_path, capsys):
        """Should compare two story files."""
        # Create sample story files
        story1 = {
            "pipeline_name": "test_pipeline",
            "duration": 5.0,
            "success_rate": 100.0,
            "total_rows_processed": 1000,
            "nodes": [
                {"node_name": "load", "duration": 2.0, "rows_out": 1000, "status": "success"}
            ],
        }

        story2 = {
            "pipeline_name": "test_pipeline",
            "duration": 4.5,
            "success_rate": 100.0,
            "total_rows_processed": 1200,
            "nodes": [
                {"node_name": "load", "duration": 1.8, "rows_out": 1200, "status": "success"}
            ],
        }

        story1_path = tmp_path / "story1.json"
        story2_path = tmp_path / "story2.json"

        with open(story1_path, "w") as f:
            json.dump(story1, f)

        with open(story2_path, "w") as f:
            json.dump(story2, f)

        # Run diff command
        args = Namespace(
            story1=str(story1_path), story2=str(story2_path), detailed=False, verbose=False
        )

        result = diff_command(args)

        assert result == 0

        captured = capsys.readouterr()
        assert "test_pipeline" in captured.out
        assert "5.00s" in captured.out
        assert "4.50s" in captured.out


class TestListCommand:
    """Tests for story list CLI command."""

    def test_list_command_help(self):
        """Should have list command available."""
        import argparse

        from odibi.cli.story import add_story_parser

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_story_parser(subparsers)

        args = parser.parse_args(["story", "list"])

        assert args.command == "story"
        assert args.story_command == "list"

    def test_list_command_with_directory(self):
        """Should accept directory parameter."""
        import argparse

        from odibi.cli.story import add_story_parser

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_story_parser(subparsers)

        args = parser.parse_args(["story", "list", "--directory", "custom/path"])

        assert args.directory == "custom/path"

    def test_list_command_execution(self, tmp_path, capsys):
        """Should list story files in directory."""
        # Create sample story files
        (tmp_path / "story1.json").write_text("{}")
        (tmp_path / "story2.html").write_text("<html></html>")
        (tmp_path / "story3.md").write_text("# Story")

        args = Namespace(directory=str(tmp_path), limit=10)

        result = list_command(args)

        assert result == 0

        captured = capsys.readouterr()
        assert "story1.json" in captured.out or "story2.html" in captured.out

    def test_list_command_empty_directory(self, tmp_path, capsys):
        """Should handle empty directory gracefully."""
        args = Namespace(directory=str(tmp_path), limit=10)

        result = list_command(args)

        assert result == 0

        captured = capsys.readouterr()
        assert "No story files found" in captured.out

    def test_list_command_nonexistent_directory(self, capsys):
        """Should handle nonexistent directory."""
        args = Namespace(directory="nonexistent/path", limit=10)

        result = list_command(args)

        assert result == 1

        captured = capsys.readouterr()
        assert "not found" in captured.out


class TestStoryParserIntegration:
    """Integration tests for story parser."""

    def test_all_story_commands_registered(self):
        """Should register all story subcommands."""
        import argparse

        from odibi.cli.story import add_story_parser

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_story_parser(subparsers)

        # Test generate
        args = parser.parse_args(["story", "generate", "test.yaml"])
        assert args.story_command == "generate"

        # Test diff
        args = parser.parse_args(["story", "diff", "s1.json", "s2.json"])
        assert args.story_command == "diff"

        # Test list
        args = parser.parse_args(["story", "list"])
        assert args.story_command == "list"

        # Test last
        args = parser.parse_args(["story", "last"])
        assert args.story_command == "last"

        # Test last with --node
        args = parser.parse_args(["story", "last", "--node", "dim_customer"])
        assert args.story_command == "last"
        assert args.node == "dim_customer"

        # Test show
        args = parser.parse_args(["story", "show", "stories/run.json"])
        assert args.story_command == "show"
        assert args.path == "stories/run.json"


class TestLastCommand:
    """Tests for story last CLI command."""

    def test_last_command_no_stories(self, tmp_path, capsys):
        """last should show error when no stories exist."""
        import os

        from odibi.cli.story import last_command

        # Change to empty temp dir
        original_cwd = os.getcwd()
        os.chdir(tmp_path)

        try:
            args = Namespace(node=None)
            result = last_command(args)

            assert result == 1
            captured = capsys.readouterr()
            assert "No story files found" in captured.out
        finally:
            os.chdir(original_cwd)

    def test_last_command_finds_most_recent(self, tmp_path, capsys):
        """last should find the most recent story file."""
        import os
        import time

        from odibi.cli.story import last_command

        # Create stories dir with files
        stories_dir = tmp_path / "stories"
        stories_dir.mkdir()

        # Create older story
        old_story = stories_dir / "old.json"
        old_story.write_text('{"pipeline_name": "old"}')

        time.sleep(0.1)  # Ensure different mtime

        # Create newer story
        new_story = stories_dir / "new.json"
        new_story.write_text('{"pipeline_name": "new", "nodes": []}')

        original_cwd = os.getcwd()
        os.chdir(tmp_path)

        try:
            args = Namespace(node=None)
            result = last_command(args)

            assert result == 0
            captured = capsys.readouterr()
            assert "new.json" in captured.out
        finally:
            os.chdir(original_cwd)


class TestShowCommand:
    """Tests for story show CLI command."""

    def test_show_command_file_not_found(self, capsys):
        """show should error when file doesn't exist."""
        from odibi.cli.story import show_command

        args = Namespace(path="nonexistent.json")
        result = show_command(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "not found" in captured.out

    def test_show_command_json_story(self, tmp_path, capsys):
        """show should display JSON story summary."""
        from odibi.cli.story import show_command

        story_file = tmp_path / "test_story.json"
        story_data = {
            "pipeline_name": "test_pipeline",
            "duration": 5.5,
            "success": True,
            "nodes": [
                {"node_name": "node1", "status": "success", "duration": 1.0},
                {"node_name": "node2", "status": "success", "duration": 2.0},
            ],
        }
        story_file.write_text(json.dumps(story_data))

        args = Namespace(path=str(story_file))
        result = show_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "test_pipeline" in captured.out
        assert "5.5" in captured.out
        assert "node1" in captured.out
        assert "node2" in captured.out

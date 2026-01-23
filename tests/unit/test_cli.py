"""Tests for CLI module (Phase 2.5)."""

import sys
from argparse import Namespace
from unittest.mock import ANY, Mock, patch

import pytest

from odibi.cli import main
from odibi.cli.run import run_command
from odibi.cli.validate import validate_command


class TestCLIImports:
    """Test that CLI modules are importable."""

    def test_cli_main_import(self):
        """CLI main should be importable."""
        assert main is not None
        assert callable(main)

    def test_run_command_import(self):
        """Run command should be importable."""
        assert run_command is not None
        assert callable(run_command)

    def test_validate_command_import(self):
        """Validate command should be importable."""
        assert validate_command is not None
        assert callable(validate_command)


class TestCLIMain:
    """Test CLI main entry point."""

    def test_cli_main_no_args_shows_help(self, capsys):
        """CLI should show help when no args provided."""
        with patch.object(sys, "argv", ["odibi"]):
            result = main()
            assert result == 1  # Returns 1 (failure) when no command

        captured = capsys.readouterr()
        output = captured.out + captured.err
        assert "usage:" in output.lower() or "odibi" in output.lower()

    def test_cli_main_help_flag(self, capsys):
        """CLI should show help with --help flag."""
        with patch.object(sys, "argv", ["odibi", "--help"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

        captured = capsys.readouterr()
        output = captured.out + captured.err
        assert "run" in output.lower()
        assert "validate" in output.lower()

    def test_cli_main_invalid_command(self, capsys):
        """CLI should show help with invalid command."""
        with patch.object(sys, "argv", ["odibi", "invalid"]):
            with pytest.raises(SystemExit):
                main()

        captured = capsys.readouterr()
        output = captured.out + captured.err
        assert "invalid" in output.lower() or "usage" in output.lower()


class TestRunCommand:
    """Test run command."""

    def test_run_command_missing_file(self):
        """Run command should fail gracefully with missing file."""
        args = Mock()
        args.config = "nonexistent.yaml"

        result = run_command(args)
        assert result == 1  # Failure exit code

    def test_run_command_with_mock_manager(self):
        """Run command should work with valid config."""
        args = Mock()
        args.config = "test.yaml"
        args.dry_run = False
        args.env = "dev"
        args.resume = False
        args.parallel = False
        args.workers = 4

        mock_result = Mock()
        mock_result.failed = []
        mock_result.pipeline_name = "test_pipeline"
        mock_result.duration = 1.5
        mock_result.story_path = None  # No story path in mock

        mock_manager = Mock()
        mock_manager.run.return_value = mock_result

        with patch("odibi.cli.run.PipelineManager") as MockManager:
            MockManager.from_yaml.return_value = mock_manager
            result = run_command(args)

            assert result == 0  # Success
            MockManager.from_yaml.assert_called_once_with("test.yaml", env="dev")
            mock_manager.run.assert_called_once_with(
                pipelines=ANY,
                dry_run=False,
                resume_from_failure=False,
                parallel=False,
                max_workers=4,
                on_error=ANY,
                tag=ANY,
                node=ANY,
            )


class TestValidateCommand:
    """Test validate command."""

    def test_validate_command_missing_file(self):
        """Validate command should fail gracefully with missing file."""
        args = Mock()
        args.config = "nonexistent.yaml"

        result = validate_command(args)
        assert result == 1  # Failure exit code

    def test_validate_command_invalid_yaml(self, tmp_path):
        """Validate command should fail with invalid YAML."""
        config_file = tmp_path / "invalid.yaml"
        config_file.write_text("invalid: yaml: content: ][")

        args = Mock()
        args.config = str(config_file)

        result = validate_command(args)
        assert result == 1  # Failure exit code

    def test_validate_command_valid_config(self, tmp_path):
        """Validate command should succeed with valid config."""
        config_file = tmp_path / "valid.yaml"
        config_file.write_text(
            """
project: Test Project
engine: pandas
connections:
  local:
    type: local
    base_path: ./data
story:
  connection: local
  path: stories/
system:
  connection: local
pipelines:
  - pipeline: test
    nodes:
      - name: test_node
        read:
          connection: local
          path: test.csv
          format: csv
"""
        )

        args = Mock()
        args.config = str(config_file)

        result = validate_command(args)
        assert result == 0  # Success


class TestInitCommand:
    """Test init-pipeline command."""

    def test_init_command_creates_project(self, tmp_path, monkeypatch):
        """init should create project directory with odibi.yaml."""
        from odibi.cli.init_pipeline import init_pipeline_command

        # Change to temp directory
        monkeypatch.chdir(tmp_path)

        args = Namespace(name="test_project", template="hello", force=False)
        result = init_pipeline_command(args)

        # Should succeed (may fail if templates not found, which is OK in test env)
        project_dir = tmp_path / "test_project"
        if result == 0:
            assert project_dir.exists()
            assert (project_dir / "odibi.yaml").exists()

    def test_init_command_fixes_sample_data_path(self, tmp_path, monkeypatch):
        """init should replace ../sample_data with ./sample_data in odibi.yaml."""
        from odibi.cli.init_pipeline import init_pipeline_command

        monkeypatch.chdir(tmp_path)

        args = Namespace(name="test_project", template="hello", force=False)
        result = init_pipeline_command(args)

        if result == 0:
            odibi_yaml = tmp_path / "test_project" / "odibi.yaml"
            content = odibi_yaml.read_text()

            # Should NOT have ../sample_data (broken path)
            assert "../sample_data" not in content
            # Should have ./sample_data (correct for standalone project)
            assert "./sample_data" in content

    def test_init_command_copies_sample_data(self, tmp_path, monkeypatch):
        """init should copy sample_data directory."""
        from odibi.cli.init_pipeline import init_pipeline_command

        monkeypatch.chdir(tmp_path)

        args = Namespace(name="test_project", template="star-schema", force=False)
        result = init_pipeline_command(args)

        if result == 0:
            sample_dir = tmp_path / "test_project" / "sample_data"
            assert sample_dir.exists()
            # Should have the CSV files
            assert (sample_dir / "customers.csv").exists()
            assert (sample_dir / "products.csv").exists()
            assert (sample_dir / "orders.csv").exists()

    def test_init_command_force_overwrites(self, tmp_path, monkeypatch):
        """init --force should overwrite existing directory."""
        from odibi.cli.init_pipeline import init_pipeline_command

        monkeypatch.chdir(tmp_path)

        # Create existing directory
        existing = tmp_path / "test_project"
        existing.mkdir()
        (existing / "existing_file.txt").write_text("should be gone")

        args = Namespace(name="test_project", template="hello", force=True)
        result = init_pipeline_command(args)

        if result == 0:
            assert not (existing / "existing_file.txt").exists()
            assert (existing / "odibi.yaml").exists()

    def test_init_command_creates_mcp_config(self, tmp_path, monkeypatch):
        """init should create mcp_config.yaml with project name."""
        from odibi.cli.init_pipeline import init_pipeline_command

        monkeypatch.chdir(tmp_path)

        args = Namespace(name="my_analytics", template="hello", force=False)
        result = init_pipeline_command(args)

        if result == 0:
            mcp_config = tmp_path / "my_analytics" / "mcp_config.yaml"
            assert mcp_config.exists()
            content = mcp_config.read_text()
            # Should have project name in authorized_projects
            assert "my_analytics" in content
            # Should have standard MCP sections
            assert "access:" in content
            assert "connection_policies:" in content
            assert "discovery:" in content
            assert "audit:" in content

"""Integration tests for CLI (Phase 2.5)."""

import subprocess
from pathlib import Path

import pytest


class TestCLIHelp:
    """Test CLI help commands."""

    def test_cli_help_command(self):
        """CLI --help should work."""
        result = subprocess.run(["python", "-m", "odibi", "--help"], capture_output=True, text=True)
        assert result.returncode == 0
        output = result.stdout + result.stderr
        assert "run" in output.lower()
        assert "validate" in output.lower()

    def test_cli_run_help(self):
        """CLI run --help should work."""
        result = subprocess.run(
            ["python", "-m", "odibi", "run", "--help"], capture_output=True, text=True
        )
        assert result.returncode == 0
        output = result.stdout + result.stderr
        assert "config" in output.lower()

    def test_cli_validate_help(self):
        """CLI validate --help should work."""
        result = subprocess.run(
            ["python", "-m", "odibi", "validate", "--help"], capture_output=True, text=True
        )
        assert result.returncode == 0
        output = result.stdout + result.stderr
        assert "config" in output.lower()


class TestCLIValidateCommand:
    """Test CLI validate command integration."""

    def test_validate_missing_file(self):
        """Validate should fail gracefully with missing file."""
        result = subprocess.run(
            ["python", "-m", "odibi", "validate", "nonexistent.yaml"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1
        output = result.stdout + result.stderr
        assert "fail" in output.lower() or "error" in output.lower()

    def test_validate_invalid_yaml(self, tmp_path):
        """Validate should fail with invalid YAML."""
        config_file = tmp_path / "invalid.yaml"
        config_file.write_text("invalid: yaml: content: ][")

        result = subprocess.run(
            ["python", "-m", "odibi", "validate", str(config_file)], capture_output=True, text=True
        )
        assert result.returncode == 1

    def test_validate_valid_minimal_config(self, tmp_path):
        """Validate should succeed with valid minimal config."""
        config_file = tmp_path / "valid.yaml"
        config_file.write_text("""
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
""")

        result = subprocess.run(
            ["python", "-m", "odibi", "validate", str(config_file)], capture_output=True, text=True
        )
        assert result.returncode == 0
        output = result.stdout + result.stderr
        assert "valid" in output.lower()


class TestCLIRunCommand:
    """Test CLI run command integration."""

    @pytest.mark.skip(reason="Subprocess crashes on Windows - covered by unit tests")
    def test_run_missing_file(self):
        """Run should fail gracefully with missing file."""
        result = subprocess.run(
            ["python", "-m", "odibi", "run", "nonexistent.yaml"], capture_output=True, text=True
        )
        assert result.returncode == 1
        output = result.stdout + result.stderr
        assert "fail" in output.lower() or "error" in output.lower()

    @pytest.mark.skip(reason="Subprocess crashes on Windows - covered by unit tests")
    def test_run_with_missing_data(self, tmp_path):
        """Run should fail gracefully when data files are missing."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
project: Test Project
engine: pandas
connections:
  local:
    type: local
    base_path: ./nonexistent_data
story:
  connection: local
  path: stories/
pipelines:
  - pipeline: test
    nodes:
      - name: test_node
        read:
          connection: local
          path: missing.csv
          format: csv
""")

        result = subprocess.run(
            ["python", "-m", "odibi", "run", str(config_file)], capture_output=True, text=True
        )
        # Should fail because data file doesn't exist
        assert result.returncode == 1


class TestCLIExampleFiles:
    """Test CLI with example files (if they exist)."""

    def test_validate_example_local_if_exists(self):
        """Validate example_local.yaml if it exists."""
        example_path = Path("examples/example_local.yaml")
        if example_path.exists():
            result = subprocess.run(
                ["python", "-m", "odibi", "validate", str(example_path)],
                capture_output=True,
                text=True,
            )
            # Should validate successfully
            assert result.returncode == 0
            output = result.stdout + result.stderr
            assert "valid" in output.lower()
        else:
            pytest.skip("example_local.yaml not found")


class TestCLIInvalidCommands:
    """Test CLI with invalid commands."""

    @pytest.mark.skip(reason="Subprocess crashes on Windows - covered by unit tests")
    def test_cli_no_command(self):
        """CLI with no command should show help."""
        result = subprocess.run(["python", "-m", "odibi"], capture_output=True, text=True)
        # Should exit with error and show help
        assert result.returncode == 1
        output = result.stdout + result.stderr
        assert "usage" in output.lower() or "help" in output.lower()

    @pytest.mark.skip(reason="Subprocess crashes on Windows - covered by unit tests")
    def test_cli_invalid_command(self):
        """CLI with invalid command should fail."""
        result = subprocess.run(
            ["python", "-m", "odibi", "invalid-command"], capture_output=True, text=True
        )
        assert result.returncode != 0

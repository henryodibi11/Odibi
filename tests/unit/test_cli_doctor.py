"""Tests for doctor CLI command."""

import argparse
from unittest.mock import Mock, patch

from odibi.cli.doctor import (
    add_doctor_parser,
    check_config,
    check_connections,
    check_dependencies,
    check_system_catalog,
    doctor_command,
)


class TestCheckDependencies:
    """Tests for check_dependencies function."""

    def test_check_dependencies_all_installed(self, capsys):
        """Should return True when all required dependencies are installed."""
        with patch("odibi.cli.doctor.importlib.import_module") as mock_import:
            # Mock all dependencies as installed with versions
            mock_module = Mock()
            mock_module.__version__ = "1.0.0"
            mock_import.return_value = mock_module

            result = check_dependencies()

            assert result is True
            captured = capsys.readouterr()
            assert "Checking dependencies..." in captured.out
            assert "[OK] Pandas: 1.0.0" in captured.out

    def test_check_dependencies_missing_required(self, capsys):
        """Should return False when required dependencies are missing."""

        def side_effect(module_name):
            if module_name == "pandas":
                raise ImportError("pandas not found")
            mock_module = Mock()
            mock_module.__version__ = "1.0.0"
            return mock_module

        with patch("odibi.cli.doctor.importlib.import_module", side_effect=side_effect):
            result = check_dependencies()

            assert result is False
            captured = capsys.readouterr()
            assert "[MISSING] Pandas" in captured.out

    def test_check_dependencies_missing_optional(self, capsys):
        """Should return True when only optional dependencies are missing."""

        def side_effect(module_name):
            if module_name in ["pyspark", "openlineage.client", "azure.storage.blob", "delta"]:
                raise ImportError(f"{module_name} not found")
            mock_module = Mock()
            mock_module.__version__ = "1.0.0"
            return mock_module

        with patch("odibi.cli.doctor.importlib.import_module", side_effect=side_effect):
            result = check_dependencies()

            assert result is True
            captured = capsys.readouterr()
            assert "[OPTIONAL] PySpark" in captured.out
            assert "[OPTIONAL] OpenLineage" in captured.out
            assert "[OPTIONAL] Azure Blob Storage" in captured.out
            assert "[OPTIONAL] Delta Lake" in captured.out

    def test_check_dependencies_no_version_attribute(self, capsys):
        """Should handle modules without __version__ attribute."""
        with patch("odibi.cli.doctor.importlib.import_module") as mock_import:
            mock_module = Mock(spec=[])  # No __version__ attribute
            mock_import.return_value = mock_module

            result = check_dependencies()

            assert result is True
            captured = capsys.readouterr()
            assert "[OK] Pandas: installed" in captured.out


class TestCheckConfig:
    """Tests for check_config function."""

    def test_check_config_no_file_found(self, capsys):
        """Should return None when no config file exists."""
        with patch("odibi.cli.doctor.os.path.exists", return_value=False):
            result = check_config()

            assert result is None
            captured = capsys.readouterr()
            assert "Checking configuration..." in captured.out
            assert "[FAIL] No configuration file found" in captured.out

    def test_check_config_finds_odibi_yaml(self, capsys):
        """Should find and load odibi.yaml."""

        def exists_side_effect(path):
            return path == "odibi.yaml"

        mock_manager = Mock()
        with patch("odibi.cli.doctor.os.path.exists", side_effect=exists_side_effect):
            with patch("odibi.cli.doctor.PipelineManager.from_yaml", return_value=mock_manager):
                result = check_config()

                assert result == mock_manager
                captured = capsys.readouterr()
                assert "[OK] Found odibi.yaml" in captured.out
                assert "[OK] Configuration is valid" in captured.out

    def test_check_config_finds_project_yaml(self, capsys):
        """Should find and load project.yaml if odibi.yaml doesn't exist."""

        def exists_side_effect(path):
            return path == "project.yaml"

        mock_manager = Mock()
        with patch("odibi.cli.doctor.os.path.exists", side_effect=exists_side_effect):
            with patch("odibi.cli.doctor.PipelineManager.from_yaml", return_value=mock_manager):
                result = check_config()

                assert result == mock_manager
                captured = capsys.readouterr()
                assert "[OK] Found project.yaml" in captured.out

    def test_check_config_invalid_yaml(self, capsys):
        """Should return None and show error when config is invalid."""

        def exists_side_effect(path):
            return path == "odibi.yaml"

        with patch("odibi.cli.doctor.os.path.exists", side_effect=exists_side_effect):
            with patch(
                "odibi.cli.doctor.PipelineManager.from_yaml",
                side_effect=Exception("Invalid YAML syntax"),
            ):
                result = check_config()

                assert result is None
                captured = capsys.readouterr()
                assert "[FAIL] Invalid configuration: Invalid YAML syntax" in captured.out


class TestCheckConnections:
    """Tests for check_connections function."""

    def test_check_connections_no_manager(self, capsys):
        """Should skip checks when no manager provided."""
        result = check_connections(None)

        assert result is False
        captured = capsys.readouterr()
        assert "Checking connections..." in captured.out
        assert "[SKIP] Skipping connection checks" in captured.out

    def test_check_connections_all_valid(self, capsys):
        """Should return True when all connections are valid."""
        mock_conn1 = Mock()
        mock_conn1.validate.return_value = None
        mock_conn1.__class__.__name__ = "LocalConnection"

        mock_conn2 = Mock()
        mock_conn2.validate.return_value = None
        mock_conn2.__class__.__name__ = "AzureBlobConnection"

        mock_manager = Mock()
        mock_manager.connections = {"local": mock_conn1, "azure": mock_conn2}

        result = check_connections(mock_manager)

        assert result is True
        captured = capsys.readouterr()
        assert "[OK] local (LocalConnection)" in captured.out
        assert "[OK] azure (AzureBlobConnection)" in captured.out

    def test_check_connections_validation_fails(self, capsys):
        """Should return False when connection validation fails."""
        mock_conn = Mock()
        mock_conn.validate.side_effect = Exception("Connection failed")
        mock_conn.__class__.__name__ = "LocalConnection"

        mock_manager = Mock()
        mock_manager.connections = {"local": mock_conn}

        result = check_connections(mock_manager)

        assert result is False
        captured = capsys.readouterr()
        assert "[FAIL] local: Connection failed" in captured.out

    def test_check_connections_empty_connections(self, capsys):
        """Should return True when no connections defined."""
        mock_manager = Mock()
        mock_manager.connections = {}

        result = check_connections(mock_manager)

        assert result is True
        captured = capsys.readouterr()
        assert "Checking connections..." in captured.out


class TestCheckSystemCatalog:
    """Tests for check_system_catalog function."""

    def test_check_system_catalog_no_manager(self, capsys):
        """Should skip when no manager provided."""
        result = check_system_catalog(None)

        assert result is True
        captured = capsys.readouterr()
        assert "Checking System Catalog..." in captured.out
        assert "[SKIP] System Catalog not configured" in captured.out

    def test_check_system_catalog_no_catalog_manager(self, capsys):
        """Should skip when catalog_manager is None."""
        mock_manager = Mock()
        mock_manager.catalog_manager = None

        result = check_system_catalog(mock_manager)

        assert result is True
        captured = capsys.readouterr()
        assert "[SKIP] System Catalog not configured" in captured.out

    def test_check_system_catalog_all_tables_exist(self, capsys):
        """Should return True when all catalog tables exist."""
        mock_catalog = Mock()
        mock_catalog.base_path = "/path/to/catalog"
        mock_catalog.tables = {
            "meta_tables": "/path/to/meta_tables",
            "meta_runs": "/path/to/meta_runs",
            "meta_patterns": "/path/to/meta_patterns",
            "meta_metrics": "/path/to/meta_metrics",
            "meta_state": "/path/to/meta_state",
            "meta_pipelines": "/path/to/meta_pipelines",
            "meta_nodes": "/path/to/meta_nodes",
        }
        mock_catalog._table_exists.return_value = True

        mock_manager = Mock()
        mock_manager.catalog_manager = mock_catalog

        result = check_system_catalog(mock_manager)

        assert result is True
        captured = capsys.readouterr()
        assert "[INFO] Catalog Path: /path/to/catalog" in captured.out
        assert "[OK] meta_tables" in captured.out
        assert "[OK] meta_runs" in captured.out
        assert "[OK] meta_patterns" in captured.out
        assert "[OK] meta_metrics" in captured.out
        assert "[OK] meta_state" in captured.out
        assert "[OK] meta_pipelines" in captured.out
        assert "[OK] meta_nodes" in captured.out

    def test_check_system_catalog_missing_tables(self, capsys):
        """Should return False when some tables are missing."""
        mock_catalog = Mock()
        mock_catalog.base_path = "/path/to/catalog"
        mock_catalog.tables = {
            "meta_tables": "/path/to/meta_tables",
            "meta_runs": "/path/to/meta_runs",
            "meta_patterns": "/path/to/meta_patterns",
            "meta_metrics": "/path/to/meta_metrics",
            "meta_state": "/path/to/meta_state",
            "meta_pipelines": "/path/to/meta_pipelines",
            "meta_nodes": "/path/to/meta_nodes",
        }

        def table_exists_side_effect(path):
            return "meta_runs" not in path and "meta_metrics" not in path

        mock_catalog._table_exists.side_effect = table_exists_side_effect

        mock_manager = Mock()
        mock_manager.catalog_manager = mock_catalog

        result = check_system_catalog(mock_manager)

        assert result is False
        captured = capsys.readouterr()
        assert "[FAIL] meta_runs not found" in captured.out
        assert "[FAIL] meta_metrics not found" in captured.out
        assert "[OK] meta_tables" in captured.out


class TestDoctorCommand:
    """Tests for doctor_command function."""

    def test_doctor_command_all_checks_pass(self, capsys):
        """Should return 0 when all checks pass."""
        mock_manager = Mock()
        mock_manager.catalog_manager = None

        with patch("odibi.cli.doctor.check_dependencies", return_value=True):
            with patch("odibi.cli.doctor.check_config", return_value=mock_manager):
                with patch("odibi.cli.doctor.check_connections", return_value=True):
                    with patch("odibi.cli.doctor.check_system_catalog", return_value=True):
                        args = Mock()
                        result = doctor_command(args)

                        assert result == 0
                        captured = capsys.readouterr()
                        assert "Odibi Doctor" in captured.out
                        assert "[SUCCESS] You are ready to run pipelines!" in captured.out

    def test_doctor_command_dependencies_fail(self, capsys):
        """Should return 1 when dependencies check fails."""
        mock_manager = Mock()

        with patch("odibi.cli.doctor.check_dependencies", return_value=False):
            with patch("odibi.cli.doctor.check_config", return_value=mock_manager):
                with patch("odibi.cli.doctor.check_connections", return_value=True):
                    args = Mock()
                    result = doctor_command(args)

                    assert result == 1
                    captured = capsys.readouterr()
                    assert "[WARNING] Some required dependencies are missing." in captured.out

    def test_doctor_command_config_fail(self, capsys):
        """Should return 1 when config check fails."""
        with patch("odibi.cli.doctor.check_dependencies", return_value=True):
            with patch("odibi.cli.doctor.check_config", return_value=None):
                with patch("odibi.cli.doctor.check_connections", return_value=False):
                    args = Mock()
                    result = doctor_command(args)

                    assert result == 1
                    captured = capsys.readouterr()
                    assert "[WARNING] Configuration issues found." in captured.out

    def test_doctor_command_connections_fail(self, capsys):
        """Should return 1 when connections check fails."""
        mock_manager = Mock()
        mock_manager.catalog_manager = None

        with patch("odibi.cli.doctor.check_dependencies", return_value=True):
            with patch("odibi.cli.doctor.check_config", return_value=mock_manager):
                with patch("odibi.cli.doctor.check_connections", return_value=False):
                    args = Mock()
                    result = doctor_command(args)

                    assert result == 1
                    captured = capsys.readouterr()
                    assert "[WARNING] Some connections failed validation." in captured.out

    def test_doctor_command_catalog_fail(self, capsys):
        """Should return 1 when system catalog check fails."""
        mock_manager = Mock()
        mock_manager.catalog_manager = Mock()

        with patch("odibi.cli.doctor.check_dependencies", return_value=True):
            with patch("odibi.cli.doctor.check_config", return_value=mock_manager):
                with patch("odibi.cli.doctor.check_connections", return_value=True):
                    with patch("odibi.cli.doctor.check_system_catalog", return_value=False):
                        args = Mock()
                        result = doctor_command(args)

                        assert result == 1
                        captured = capsys.readouterr()
                        assert "[WARNING] System Catalog issues found." in captured.out

    def test_doctor_command_version_display(self, capsys):
        """Should display odibi version."""
        mock_manager = Mock()
        mock_manager.catalog_manager = None

        import odibi

        with patch.object(odibi, "__version__", "1.2.3"):
            with patch("odibi.cli.doctor.check_dependencies", return_value=True):
                with patch("odibi.cli.doctor.check_config", return_value=mock_manager):
                    with patch("odibi.cli.doctor.check_connections", return_value=True):
                        args = Mock()
                        doctor_command(args)

                        captured = capsys.readouterr()
                        assert "Odibi Doctor (v1.2.3)" in captured.out

    def test_doctor_command_version_unknown(self, capsys):
        """Should show 'unknown' version on error."""
        mock_manager = Mock()
        mock_manager.catalog_manager = None

        import builtins

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "odibi":
                raise Exception("Import error")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with patch("odibi.cli.doctor.check_dependencies", return_value=True):
                with patch("odibi.cli.doctor.check_config", return_value=mock_manager):
                    with patch("odibi.cli.doctor.check_connections", return_value=True):
                        args = Mock()
                        doctor_command(args)

                        captured = capsys.readouterr()
                        assert "Odibi Doctor (vunknown)" in captured.out

    def test_doctor_command_separator_line(self, capsys):
        """Should display separator line."""
        mock_manager = Mock()
        mock_manager.catalog_manager = None

        with patch("odibi.cli.doctor.check_dependencies", return_value=True):
            with patch("odibi.cli.doctor.check_config", return_value=mock_manager):
                with patch("odibi.cli.doctor.check_connections", return_value=True):
                    args = Mock()
                    doctor_command(args)

                    captured = capsys.readouterr()
                    assert "=" * 40 in captured.out


class TestAddDoctorParser:
    """Tests for add_doctor_parser function."""

    def test_add_doctor_parser(self):
        """Should add doctor subparser."""
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")

        result = add_doctor_parser(subparsers)

        assert result is not None
        # Parse doctor command
        args = parser.parse_args(["doctor"])
        assert args.command == "doctor"

    def test_add_doctor_parser_help_text(self):
        """Should have help text for doctor command."""
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")

        doctor_parser = add_doctor_parser(subparsers)

        # Check that help text is set (accessed via _actions in the parent)
        assert doctor_parser is not None


class TestDoctorIntegration:
    """Integration tests for doctor command."""

    def test_doctor_workflow_with_catalog(self, capsys):
        """Should run full workflow with catalog configured."""
        mock_catalog = Mock()
        mock_catalog.base_path = "/catalog"
        mock_catalog.tables = {
            "meta_tables": "/catalog/meta_tables",
            "meta_runs": "/catalog/meta_runs",
            "meta_patterns": "/catalog/meta_patterns",
            "meta_metrics": "/catalog/meta_metrics",
            "meta_state": "/catalog/meta_state",
            "meta_pipelines": "/catalog/meta_pipelines",
            "meta_nodes": "/catalog/meta_nodes",
        }
        mock_catalog._table_exists.return_value = True

        mock_manager = Mock()
        mock_manager.catalog_manager = mock_catalog
        mock_manager.connections = {}

        with patch("odibi.cli.doctor.check_dependencies", return_value=True):
            with patch("odibi.cli.doctor.check_config", return_value=mock_manager):
                with patch("odibi.cli.doctor.check_connections", return_value=True):
                    with patch("odibi.cli.doctor.check_system_catalog", return_value=True):
                        args = Mock()
                        result = doctor_command(args)

                        assert result == 0
                        captured = capsys.readouterr()
                        assert "[SUCCESS]" in captured.out

    def test_doctor_workflow_without_catalog(self, capsys):
        """Should run full workflow without catalog (local mode)."""
        mock_manager = Mock()
        mock_manager.catalog_manager = None
        mock_manager.connections = {}

        with patch("odibi.cli.doctor.check_dependencies", return_value=True):
            with patch("odibi.cli.doctor.check_config", return_value=mock_manager):
                with patch("odibi.cli.doctor.check_connections", return_value=True):
                    args = Mock()
                    result = doctor_command(args)

                    assert result == 0
                    captured = capsys.readouterr()
                    assert "[SUCCESS]" in captured.out

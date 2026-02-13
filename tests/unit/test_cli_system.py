"""Tests for system CLI command."""

import argparse
from datetime import date
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from odibi.cli.system import (
    _cleanup_command,
    _count_records_pandas,
    _count_records_spark,
    _count_records_sql_server,
    _count_records_to_delete,
    _delete_old_records,
    _delete_records_pandas,
    _delete_records_spark,
    _delete_records_sql_server,
    _rebuild_summaries_command,
    _sync_command,
    add_system_parser,
    system_command,
)


class TestSystemCLIImports:
    """Test that system CLI modules are importable."""

    def test_system_command_import(self):
        """System command should be importable."""
        assert system_command is not None
        assert callable(system_command)

    def test_add_system_parser_import(self):
        """add_system_parser should be importable."""
        assert add_system_parser is not None
        assert callable(add_system_parser)


class TestSystemParserSetup:
    """Test system parser configuration."""

    def test_add_system_parser(self):
        """System parser should add subcommands."""
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_system_parser(subparsers)

        # Parse system sync command
        args = parser.parse_args(["system", "sync", "config.yaml"])
        assert args.command == "system"
        assert args.system_command == "sync"
        assert args.config == "config.yaml"

    def test_system_sync_options(self):
        """Sync subcommand should accept all options."""
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_system_parser(subparsers)

        args = parser.parse_args(
            [
                "system",
                "sync",
                "config.yaml",
                "--env",
                "prod",
                "--tables",
                "runs",
                "state",
                "--dry-run",
            ]
        )
        assert args.system_command == "sync"
        assert args.env == "prod"
        assert args.tables == ["runs", "state"]
        assert args.dry_run is True

    def test_system_rebuild_summaries_options(self):
        """Rebuild-summaries subcommand should accept all options."""
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_system_parser(subparsers)

        args = parser.parse_args(
            [
                "system",
                "rebuild-summaries",
                "config.yaml",
                "--env",
                "dev",
                "--pipeline",
                "etl",
                "--since",
                "2024-01-01",
                "--max-age-minutes",
                "60",
            ]
        )
        assert args.system_command == "rebuild-summaries"
        assert args.env == "dev"
        assert args.pipeline == "etl"
        assert args.since == "2024-01-01"
        assert args.max_age_minutes == 60

    def test_system_rebuild_summaries_all_flag(self):
        """Rebuild-summaries should support --all flag."""
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_system_parser(subparsers)

        args = parser.parse_args(
            ["system", "rebuild-summaries", "config.yaml", "--all", "--since", "2024-01-01"]
        )
        assert args.rebuild_all is True

    def test_system_cleanup_options(self):
        """Cleanup subcommand should accept all options."""
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_system_parser(subparsers)

        args = parser.parse_args(["system", "cleanup", "config.yaml", "--env", "qat", "--dry-run"])
        assert args.system_command == "cleanup"
        assert args.env == "qat"
        assert args.dry_run is True


class TestSystemCommand:
    """Test system command dispatcher."""

    def test_system_no_subcommand(self, caplog):
        """System without subcommand shows help."""
        args = Mock()
        args.system_command = None

        result = system_command(args)

        assert result == 1
        # Help message is printed to stdout, not logged

    def test_system_unknown_subcommand(self, caplog):
        """Unknown subcommand shows error."""
        args = Mock()
        args.system_command = "unknown"

        result = system_command(args)

        assert result == 1
        # Error message is printed to stdout, not logged


class TestSyncCommand:
    """Test sync command."""

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    def test_sync_no_system_config(self, mock_manager_class, mock_load_ext, caplog):
        """Should fail if system catalog not configured."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None
        args.tables = None
        args.dry_run = False

        mock_manager = Mock()
        mock_manager.config.system = None
        mock_manager_class.from_yaml.return_value = mock_manager

        result = _sync_command(args)

        assert result == 1
        assert "not configured" in caplog.text

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    def test_sync_no_sync_from_config(self, mock_manager_class, mock_load_ext, caplog):
        """Should fail if sync_from not configured."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None
        args.tables = None
        args.dry_run = False

        mock_system = Mock()
        mock_system.sync_from = None

        mock_manager = Mock()
        mock_manager.config.system = mock_system
        mock_manager_class.from_yaml.return_value = mock_manager

        result = _sync_command(args)

        assert result == 1
        assert "No sync_from configured" in caplog.text

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    def test_sync_dry_run(self, mock_manager_class, mock_load_ext, capsys):
        """Dry run should show what would be synced."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None
        args.tables = ["runs", "state"]
        args.dry_run = True

        mock_sync_from = Mock()
        mock_sync_from.connection = "source_db"
        mock_system = Mock()
        mock_system.sync_from = mock_sync_from
        mock_system.connection = "target_db"

        mock_manager = Mock()
        mock_manager.config.system = mock_system
        mock_manager.config.connections = {}
        mock_manager_class.from_yaml.return_value = mock_manager

        with patch("odibi.cli.system.create_sync_source_backend"):
            with patch("odibi.cli.system.create_state_backend"):
                result = _sync_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out
        assert "source_db" in captured.out
        assert "target_db" in captured.out

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    @patch("odibi.cli.system.create_sync_source_backend")
    @patch("odibi.cli.system.create_state_backend")
    @patch("odibi.cli.system.sync_system_data")
    def test_sync_success(
        self,
        mock_sync_data,
        mock_create_state,
        mock_create_source,
        mock_manager_class,
        mock_load_ext,
        capsys,
    ):
        """Successful sync should show results."""
        args = Mock()
        args.config = "test.yaml"
        args.env = "prod"
        args.tables = None
        args.dry_run = False

        mock_sync_from = Mock()
        mock_sync_from.connection = "source_db"
        mock_system = Mock()
        mock_system.sync_from = mock_sync_from
        mock_system.connection = "target_db"

        mock_manager = Mock()
        mock_manager.config.system = mock_system
        mock_manager.config.connections = {}
        mock_manager_class.from_yaml.return_value = mock_manager

        mock_sync_data.return_value = {"runs": 100, "state": 50}

        result = _sync_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "complete" in captured.out
        assert "100" in captured.out
        assert "50" in captured.out

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    def test_sync_exception_handling(self, mock_manager_class, mock_load_ext, caplog):
        """Should handle exceptions gracefully."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None

        mock_manager_class.from_yaml.side_effect = Exception("Config load failed")

        result = _sync_command(args)

        assert result == 1
        assert "failed" in caplog.text


class TestRebuildSummariesCommand:
    """Test rebuild-summaries command."""

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    def test_rebuild_no_system_config(self, mock_manager_class, mock_load_ext, caplog):
        """Should fail if system catalog not configured."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None
        args.pipeline = "etl"
        args.rebuild_all = False
        args.since = "2024-01-01"
        args.max_age_minutes = None

        mock_manager = Mock()
        mock_manager.config.system = None
        mock_manager_class.from_yaml.return_value = mock_manager

        result = _rebuild_summaries_command(args)

        assert result == 1
        assert "not configured" in caplog.text

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    def test_rebuild_no_pipeline_or_all(self, mock_manager_class, mock_load_ext, caplog):
        """Should fail if neither --pipeline nor --all specified."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None
        args.pipeline = None
        args.rebuild_all = False
        args.since = "2024-01-01"
        args.max_age_minutes = None

        mock_manager = Mock()
        mock_manager.config.system = Mock()
        mock_manager_class.from_yaml.return_value = mock_manager

        result = _rebuild_summaries_command(args)

        assert result == 1
        assert "Must specify either --pipeline or --all" in caplog.text

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    def test_rebuild_both_pipeline_and_all(self, mock_manager_class, mock_load_ext, caplog):
        """Should fail if both --pipeline and --all specified."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None
        args.pipeline = "etl"
        args.rebuild_all = True
        args.since = "2024-01-01"
        args.max_age_minutes = None

        mock_manager = Mock()
        mock_manager.config.system = Mock()
        mock_manager_class.from_yaml.return_value = mock_manager

        result = _rebuild_summaries_command(args)

        assert result == 1
        assert "mutually exclusive" in caplog.text

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    def test_rebuild_invalid_date_format(self, mock_manager_class, mock_load_ext, caplog):
        """Should fail with invalid date format."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None
        args.pipeline = "etl"
        args.rebuild_all = False
        args.since = "invalid-date"
        args.max_age_minutes = None

        mock_manager = Mock()
        mock_manager.config.system = Mock()
        mock_manager_class.from_yaml.return_value = mock_manager

        result = _rebuild_summaries_command(args)

        assert result == 1
        assert "Invalid date format" in caplog.text

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    def test_rebuild_no_catalog_manager(self, mock_manager_class, mock_load_ext, caplog):
        """Should fail if catalog manager not available."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None
        args.pipeline = "etl"
        args.rebuild_all = False
        args.since = "2024-01-01"
        args.max_age_minutes = None

        mock_manager = Mock()
        mock_manager.config.system = Mock()
        mock_manager.get_catalog.return_value = None
        mock_manager_class.from_yaml.return_value = mock_manager

        result = _rebuild_summaries_command(args)

        assert result == 1
        assert "CatalogManager not available" in caplog.text

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    @patch("odibi.derived_updater.DerivedUpdater")
    def test_rebuild_no_runs_found(
        self, mock_updater_class, mock_manager_class, mock_load_ext, capsys
    ):
        """Should show message when no runs found."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None
        args.pipeline = "etl"
        args.rebuild_all = False
        args.since = "2024-01-01"
        args.max_age_minutes = None

        mock_catalog = Mock()
        mock_catalog.get_run_ids.return_value = []

        mock_manager = Mock()
        mock_manager.config.system = Mock()
        mock_manager.get_catalog.return_value = mock_catalog
        mock_manager_class.from_yaml.return_value = mock_manager

        result = _rebuild_summaries_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "No runs found" in captured.out

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    @patch("odibi.derived_updater.DerivedUpdater")
    def test_rebuild_success(self, mock_updater_class, mock_manager_class, mock_load_ext, capsys):
        """Should rebuild summaries successfully."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None
        args.pipeline = "etl"
        args.rebuild_all = False
        args.since = "2024-01-01"
        args.max_age_minutes = 60

        mock_catalog = Mock()
        mock_catalog.get_run_ids.return_value = ["run-1", "run-2"]
        mock_catalog.get_pipeline_run.side_effect = [
            {
                "pipeline_name": "etl",
                "freshness_sla": None,
            },
            {
                "pipeline_name": "etl",
                "freshness_sla": None,
            },
        ]

        mock_updater = Mock()
        mock_updater.reclaim_for_rebuild.return_value = "token-123"
        mock_updater_class.return_value = mock_updater

        mock_manager = Mock()
        mock_manager.config.system = Mock()
        mock_manager.get_catalog.return_value = mock_catalog
        mock_manager_class.from_yaml.return_value = mock_manager

        result = _rebuild_summaries_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Rebuild complete" in captured.out
        assert "Rebuilt:" in captured.out

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    def test_rebuild_exception_propagates(self, mock_manager_class, mock_load_ext):
        """Should propagate exceptions."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None
        args.pipeline = "etl"
        args.rebuild_all = False
        args.since = "2024-01-01"
        args.max_age_minutes = None

        mock_manager_class.from_yaml.side_effect = RuntimeError("Config error")

        with pytest.raises(RuntimeError, match="Config error"):
            _rebuild_summaries_command(args)


class TestCleanupCommand:
    """Test cleanup command."""

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    def test_cleanup_no_system_config(self, mock_manager_class, mock_load_ext, caplog):
        """Should fail if system catalog not configured."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None
        args.dry_run = False

        mock_manager = Mock()
        mock_manager.config.system = None
        mock_manager_class.from_yaml.return_value = mock_manager

        result = _cleanup_command(args)

        assert result == 1
        assert "not configured" in caplog.text

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    def test_cleanup_no_catalog_manager(self, mock_manager_class, mock_load_ext, caplog):
        """Should fail if catalog manager not available."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None
        args.dry_run = False

        mock_manager = Mock()
        mock_manager.config.system = Mock()
        mock_manager.get_catalog.return_value = None
        mock_manager_class.from_yaml.return_value = mock_manager

        result = _cleanup_command(args)

        assert result == 1
        assert "CatalogManager not available" in caplog.text

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    @patch("odibi.cli.system._count_records_to_delete")
    def test_cleanup_dry_run(self, mock_count, mock_manager_class, mock_load_ext, capsys):
        """Dry run should show what would be deleted."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None
        args.dry_run = True

        mock_retention = Mock()
        mock_retention.daily_stats = 90
        mock_retention.failures = 30
        mock_retention.observability_errors = 7

        mock_system = Mock()
        mock_system.retention_days = mock_retention

        mock_catalog = Mock()

        mock_manager = Mock()
        mock_manager.config.system = mock_system
        mock_manager.get_catalog.return_value = mock_catalog
        mock_manager_class.from_yaml.return_value = mock_manager

        mock_count.return_value = {
            "meta_daily_stats": 100,
            "meta_failures": 50,
            "meta_observability_errors": 25,
        }

        result = _cleanup_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out
        assert "100 records" in captured.out

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    @patch("odibi.cli.system._delete_old_records")
    def test_cleanup_success(self, mock_delete, mock_manager_class, mock_load_ext, capsys):
        """Successful cleanup should show results."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None
        args.dry_run = False

        mock_retention = Mock()
        mock_retention.daily_stats = 90
        mock_retention.failures = 30
        mock_retention.observability_errors = 7

        mock_system = Mock()
        mock_system.retention_days = mock_retention

        mock_catalog = Mock()

        mock_manager = Mock()
        mock_manager.config.system = mock_system
        mock_manager.get_catalog.return_value = mock_catalog
        mock_manager_class.from_yaml.return_value = mock_manager

        mock_delete.return_value = {
            "meta_daily_stats": 75,
            "meta_failures": 20,
            "meta_observability_errors": 10,
        }

        result = _cleanup_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Cleanup complete" in captured.out
        assert "75 records deleted" in captured.out

    @patch("odibi.cli.system.load_extensions")
    @patch("odibi.cli.system.PipelineManager")
    def test_cleanup_exception_propagates(self, mock_manager_class, mock_load_ext):
        """Should propagate exceptions."""
        args = Mock()
        args.config = "test.yaml"
        args.env = None
        args.dry_run = False

        mock_manager_class.from_yaml.side_effect = RuntimeError("Config error")

        with pytest.raises(RuntimeError, match="Config error"):
            _cleanup_command(args)


class TestCountRecordsToDelete:
    """Test _count_records_to_delete dispatcher."""

    def test_count_spark_mode(self):
        """Should use Spark counter when in Spark mode."""
        mock_catalog = Mock()
        mock_catalog.is_spark_mode = True
        mock_catalog.is_pandas_mode = False
        mock_catalog.is_sql_server_mode = False

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        with patch("odibi.cli.system._count_records_spark") as mock_spark:
            mock_spark.return_value = {"meta_daily_stats": 100}
            result = _count_records_to_delete(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 100}
        mock_spark.assert_called_once_with(mock_catalog, cutoffs)

    def test_count_pandas_mode(self):
        """Should use Pandas counter when in Pandas mode."""
        mock_catalog = Mock()
        mock_catalog.is_spark_mode = False
        mock_catalog.is_pandas_mode = True
        mock_catalog.is_sql_server_mode = False

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        with patch("odibi.cli.system._count_records_pandas") as mock_pandas:
            mock_pandas.return_value = {"meta_daily_stats": 50}
            result = _count_records_to_delete(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 50}
        mock_pandas.assert_called_once_with(mock_catalog, cutoffs)

    def test_count_sql_server_mode(self):
        """Should use SQL Server counter when in SQL Server mode."""
        mock_catalog = Mock()
        mock_catalog.is_spark_mode = False
        mock_catalog.is_pandas_mode = False
        mock_catalog.is_sql_server_mode = True

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        with patch("odibi.cli.system._count_records_sql_server") as mock_sql:
            mock_sql.return_value = {"meta_daily_stats": 75}
            result = _count_records_to_delete(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 75}
        mock_sql.assert_called_once_with(mock_catalog, cutoffs)

    def test_count_no_backend(self, caplog):
        """Should handle case where no backend is available."""
        mock_catalog = Mock()
        mock_catalog.is_spark_mode = False
        mock_catalog.is_pandas_mode = False
        mock_catalog.is_sql_server_mode = False

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        result = _count_records_to_delete(mock_catalog, cutoffs)

        assert result == {}
        assert "No backend available" in caplog.text


class TestCountRecordsSpark:
    """Test _count_records_spark function."""

    def test_count_spark_success(self):
        """Should count records using Spark."""
        mock_functions = Mock()
        mock_pyspark_sql = Mock()
        mock_pyspark_sql.functions = mock_functions

        with patch.dict(
            "sys.modules",
            {"pyspark": Mock(), "pyspark.sql": mock_pyspark_sql},
        ):
            mock_catalog = Mock()
            mock_catalog.tables = {"meta_daily_stats": "/path/to/table"}

            mock_filtered = Mock()
            mock_filtered.count.return_value = 75

            mock_df = Mock()
            mock_df.filter.return_value = mock_filtered

            mock_catalog.spark.read.format.return_value.load.return_value = mock_df

            cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

            result = _count_records_spark(mock_catalog, cutoffs)

            assert result == {"meta_daily_stats": 75}

    def test_count_spark_exception_handling(self, caplog):
        """Should handle exceptions gracefully."""
        with patch.dict("sys.modules", {"pyspark": Mock(), "pyspark.sql": Mock()}):
            with patch("pyspark.sql.functions", Mock()):
                mock_catalog = Mock()
                mock_catalog.tables = {"meta_daily_stats": "/path/to/table"}
                mock_catalog.spark.read.format.side_effect = Exception("Spark error")

                cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

                result = _count_records_spark(mock_catalog, cutoffs)

                assert result == {"meta_daily_stats": 0}
                assert "Failed to count" in caplog.text


class TestCountRecordsPandas:
    """Test _count_records_pandas function."""

    @patch("deltalake.DeltaTable")
    def test_count_pandas_success(self, mock_delta_table):
        """Should count records using Pandas/delta-rs."""
        mock_catalog = Mock()
        mock_catalog.tables = {"meta_daily_stats": "/path/to/table"}
        mock_catalog._get_storage_options.return_value = {"key": "value"}

        cutoff = date(2024, 1, 1)
        df_data = pd.DataFrame(
            {
                "date": pd.to_datetime(["2023-12-15", "2024-01-15", "2024-02-01"]).date,
                "value": [1, 2, 3],
            }
        )

        mock_dt = Mock()
        mock_dt.to_pandas.return_value = df_data
        mock_delta_table.return_value = mock_dt

        cutoffs = {"meta_daily_stats": cutoff}

        result = _count_records_pandas(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 1}  # Only 2023-12-15 is before cutoff

    def test_count_pandas_no_deltalake(self, caplog):
        """Should handle missing deltalake library."""
        mock_catalog = Mock()
        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        # Mock the import to raise ImportError
        import builtins

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "deltalake":
                raise ImportError("No module named deltalake")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            result = _count_records_pandas(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 0}
        assert "not available" in caplog.text

    @patch("deltalake.DeltaTable")
    def test_count_pandas_exception_handling(self, mock_delta_table, caplog):
        """Should handle exceptions gracefully."""
        mock_catalog = Mock()
        mock_catalog.tables = {"meta_daily_stats": "/path/to/table"}
        mock_catalog._get_storage_options.return_value = None

        mock_delta_table.side_effect = Exception("Delta error")

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        result = _count_records_pandas(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 0}
        assert "Failed to count" in caplog.text


class TestCountRecordsSQLServer:
    """Test _count_records_sql_server function."""

    def test_count_sql_server_success(self):
        """Should count records using SQL Server."""
        mock_catalog = Mock()
        mock_catalog.config.schema_name = "odibi_system"
        mock_catalog.connection.execute.return_value = [(42,)]

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        result = _count_records_sql_server(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 42}

    def test_count_sql_server_default_schema(self):
        """Should use default schema when not configured."""
        mock_catalog = Mock()
        mock_catalog.config = Mock(spec=[])  # No schema_name attribute
        mock_catalog.connection.execute.return_value = [(10,)]

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        result = _count_records_sql_server(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 10}

    def test_count_sql_server_exception_handling(self, caplog):
        """Should handle exceptions gracefully."""
        mock_catalog = Mock()
        mock_catalog.config.schema_name = "odibi_system"
        mock_catalog.connection.execute.side_effect = Exception("SQL error")

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        result = _count_records_sql_server(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 0}
        assert "Failed to count" in caplog.text


class TestDeleteOldRecords:
    """Test _delete_old_records dispatcher."""

    def test_delete_spark_mode(self):
        """Should use Spark deleter when in Spark mode."""
        mock_catalog = Mock()
        mock_catalog.is_spark_mode = True
        mock_catalog.is_pandas_mode = False
        mock_catalog.is_sql_server_mode = False

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        with patch("odibi.cli.system._delete_records_spark") as mock_spark:
            mock_spark.return_value = {"meta_daily_stats": 100}
            result = _delete_old_records(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 100}
        mock_spark.assert_called_once_with(mock_catalog, cutoffs)

    def test_delete_pandas_mode(self):
        """Should use Pandas deleter when in Pandas mode."""
        mock_catalog = Mock()
        mock_catalog.is_spark_mode = False
        mock_catalog.is_pandas_mode = True
        mock_catalog.is_sql_server_mode = False

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        with patch("odibi.cli.system._delete_records_pandas") as mock_pandas:
            mock_pandas.return_value = {"meta_daily_stats": 50}
            result = _delete_old_records(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 50}
        mock_pandas.assert_called_once_with(mock_catalog, cutoffs)

    def test_delete_sql_server_mode(self):
        """Should use SQL Server deleter when in SQL Server mode."""
        mock_catalog = Mock()
        mock_catalog.is_spark_mode = False
        mock_catalog.is_pandas_mode = False
        mock_catalog.is_sql_server_mode = True

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        with patch("odibi.cli.system._delete_records_sql_server") as mock_sql:
            mock_sql.return_value = {"meta_daily_stats": 75}
            result = _delete_old_records(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 75}
        mock_sql.assert_called_once_with(mock_catalog, cutoffs)

    def test_delete_no_backend(self, caplog):
        """Should handle case where no backend is available."""
        mock_catalog = Mock()
        mock_catalog.is_spark_mode = False
        mock_catalog.is_pandas_mode = False
        mock_catalog.is_sql_server_mode = False

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        result = _delete_old_records(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 0}
        assert "No backend available" in caplog.text


class TestDeleteRecordsSpark:
    """Test _delete_records_spark function."""

    def test_delete_spark_success(self):
        """Should delete records using Spark."""
        mock_catalog = Mock()
        mock_catalog.tables = {"meta_daily_stats": "/path/to/table"}

        mock_df_before = Mock()
        mock_df_before.count.return_value = 200

        mock_df_after = Mock()
        mock_df_after.count.return_value = 125

        mock_catalog.spark.read.format.return_value.load.side_effect = [
            mock_df_before,
            mock_df_after,
        ]
        mock_catalog.spark.sql.return_value = None

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        result = _delete_records_spark(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 75}
        mock_catalog.spark.sql.assert_called_once()

    def test_delete_spark_exception_handling(self, caplog):
        """Should handle exceptions gracefully."""
        mock_catalog = Mock()
        mock_catalog.tables = {"meta_daily_stats": "/path/to/table"}
        mock_catalog.spark.read.format.side_effect = Exception("Spark error")

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        result = _delete_records_spark(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 0}
        assert "Failed to delete" in caplog.text


class TestDeleteRecordsPandas:
    """Test _delete_records_pandas function."""

    @patch("deltalake.DeltaTable")
    def test_delete_pandas_success(self, mock_delta_table):
        """Should delete records using Pandas/delta-rs."""
        mock_catalog = Mock()
        mock_catalog.tables = {"meta_daily_stats": "/path/to/table"}
        mock_catalog._get_storage_options.return_value = {"key": "value"}

        df_before = pd.DataFrame({"date": ["2023-12-15", "2024-01-15"], "value": [1, 2]})
        df_after = pd.DataFrame({"date": ["2024-01-15"], "value": [2]})

        mock_dt_before = Mock()
        mock_dt_before.to_pandas.return_value = df_before
        mock_dt_before.delete.return_value = None

        mock_dt_after = Mock()
        mock_dt_after.to_pandas.return_value = df_after

        mock_delta_table.side_effect = [mock_dt_before, mock_dt_after]

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        result = _delete_records_pandas(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 1}

    def test_delete_pandas_no_deltalake(self):
        """Should raise NotImplementedError when deltalake not available."""
        mock_catalog = Mock()
        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        # Mock the import to raise ImportError
        import builtins

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "deltalake":
                raise ImportError("No module named deltalake")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(NotImplementedError, match="not available"):
                _delete_records_pandas(mock_catalog, cutoffs)

    @patch("deltalake.DeltaTable")
    def test_delete_pandas_predicate_not_supported(self, mock_delta_table):
        """Should raise NotImplementedError when predicate delete not supported."""
        mock_catalog = Mock()
        mock_catalog.tables = {"meta_daily_stats": "/path/to/table"}
        mock_catalog._get_storage_options.return_value = None

        mock_dt = Mock()
        mock_dt.to_pandas.return_value = pd.DataFrame({"date": [], "value": []})
        mock_dt.delete.side_effect = Exception("Predicate not supported")
        mock_delta_table.return_value = mock_dt

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        with pytest.raises(NotImplementedError, match="not supported"):
            _delete_records_pandas(mock_catalog, cutoffs)

    @patch("deltalake.DeltaTable")
    def test_delete_pandas_exception_handling(self, mock_delta_table, caplog):
        """Should handle non-NotImplementedError exceptions gracefully."""
        mock_catalog = Mock()
        mock_catalog.tables = {"meta_daily_stats": "/path/to/table"}
        mock_catalog._get_storage_options.return_value = None

        mock_delta_table.side_effect = RuntimeError("Unexpected error")

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        result = _delete_records_pandas(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 0}
        assert "Failed to delete" in caplog.text


class TestDeleteRecordsSQLServer:
    """Test _delete_records_sql_server function."""

    def test_delete_sql_server_success(self):
        """Should delete records using SQL Server."""
        mock_catalog = Mock()
        mock_catalog.config.schema_name = "odibi_system"
        mock_catalog.connection.execute.side_effect = [
            [(30,)],  # COUNT query result
            None,  # DELETE query result
        ]

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        result = _delete_records_sql_server(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 30}
        assert mock_catalog.connection.execute.call_count == 2

    def test_delete_sql_server_default_schema(self):
        """Should use default schema when not configured."""
        mock_catalog = Mock()
        mock_catalog.config = Mock(spec=[])  # No schema_name attribute
        mock_catalog.connection.execute.side_effect = [
            [(15,)],
            None,
        ]

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        result = _delete_records_sql_server(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 15}

    def test_delete_sql_server_exception_handling(self, caplog):
        """Should handle exceptions gracefully."""
        mock_catalog = Mock()
        mock_catalog.config.schema_name = "odibi_system"
        mock_catalog.connection.execute.side_effect = Exception("SQL error")

        cutoffs = {"meta_daily_stats": date(2024, 1, 1)}

        result = _delete_records_sql_server(mock_catalog, cutoffs)

        assert result == {"meta_daily_stats": 0}
        assert "Failed to delete" in caplog.text

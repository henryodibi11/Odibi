"""Tests for catalog sync CLI commands: _sync_command, _sync_status_command, _sync_purge_command."""

from argparse import Namespace
from unittest.mock import MagicMock, patch


from odibi.cli.catalog import _sync_command, _sync_status_command, _sync_purge_command


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_sync_config(**overrides):
    """Create a mock SyncToConfig-like object."""
    cfg = MagicMock()
    cfg.connection = "sql_prod"
    cfg.mode = "incremental"
    cfg.on = "after_run"
    cfg.async_sync = True
    cfg.tables = None
    cfg.schema_name = "odibi_system"
    cfg.model_copy = MagicMock(return_value=cfg)
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def _make_project_config(system=True, sync_to=True, **overrides):
    """Create a mock ProjectConfig with optional system/sync_to."""
    project = MagicMock()
    if not system:
        project.system = None
    else:
        project.system = MagicMock()
        project.system.connection = "adls_prod"
        project.system.path = "_odibi_system"
        project.system.environment = "dev"
        if not sync_to:
            project.system.sync_to = None
        else:
            project.system.sync_to = _make_sync_config(**overrides)
    return project


# ---------------------------------------------------------------------------
# _sync_command
# ---------------------------------------------------------------------------


class TestSyncCommand:
    """Tests for _sync_command (catalog sync)."""

    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_no_system_config_returns_1(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls, capsys
    ):
        mock_load.return_value = _make_project_config(system=False)
        args = Namespace(config="test.yaml", tables=None, mode=None, dry_run=False)
        result = _sync_command(args)
        assert result == 1
        assert "No 'system'" in capsys.readouterr().out

    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_no_sync_to_returns_1_with_instructions(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls, capsys
    ):
        mock_load.return_value = _make_project_config(system=True, sync_to=False)
        args = Namespace(config="test.yaml", tables=None, mode=None, dry_run=False)
        result = _sync_command(args)
        assert result == 1
        out = capsys.readouterr().out
        assert "No 'sync_to'" in out
        assert "sync_to:" in out

    @patch("odibi.catalog_sync.ALL_SYNC_TABLES", ["meta_runs", "meta_tables"])
    @patch("odibi.catalog_sync.DEFAULT_SYNC_TABLES", ["meta_runs"])
    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_invalid_tables_returns_1(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls, capsys
    ):
        mock_load.return_value = _make_project_config()
        args = Namespace(config="test.yaml", tables="bad_table", mode=None, dry_run=False)
        result = _sync_command(args)
        assert result == 1
        assert "Unknown tables" in capsys.readouterr().out

    @patch("odibi.catalog_sync.ALL_SYNC_TABLES", ["meta_runs", "meta_tables"])
    @patch("odibi.catalog_sync.DEFAULT_SYNC_TABLES", ["meta_runs"])
    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_mode_override_updates_sync_config(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls
    ):
        project = _make_project_config()
        updated_cfg = _make_sync_config()
        project.system.sync_to.model_copy.return_value = updated_cfg
        mock_load.return_value = project
        mock_gcm.return_value = MagicMock()
        pm_instance = MagicMock()
        pm_instance.connections = {"sql_prod": MagicMock()}
        mock_pm.return_value = pm_instance
        syncer_instance = MagicMock()
        syncer_instance.sync.return_value = {"meta_runs": {"success": True, "rows": 10}}
        mock_syncer_cls.return_value = syncer_instance

        args = Namespace(config="test.yaml", tables=None, mode="full", dry_run=False)
        _sync_command(args)

        project.system.sync_to.model_copy.assert_called_once_with(update={"mode": "full"})

    @patch("odibi.catalog_sync.ALL_SYNC_TABLES", ["meta_runs", "meta_tables"])
    @patch("odibi.catalog_sync.DEFAULT_SYNC_TABLES", ["meta_runs"])
    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_dry_run_returns_0(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls, capsys
    ):
        mock_load.return_value = _make_project_config()
        args = Namespace(config="test.yaml", tables=None, mode=None, dry_run=True)
        result = _sync_command(args)
        assert result == 0
        out = capsys.readouterr().out
        assert "Dry Run" in out
        assert "sql_prod" in out

    @patch("odibi.catalog_sync.ALL_SYNC_TABLES", ["meta_runs", "meta_tables"])
    @patch("odibi.catalog_sync.DEFAULT_SYNC_TABLES", ["meta_runs"])
    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_catalog_manager_none_returns_1(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls, capsys
    ):
        mock_load.return_value = _make_project_config()
        mock_gcm.return_value = None
        args = Namespace(config="test.yaml", tables=None, mode=None, dry_run=False)
        result = _sync_command(args)
        assert result == 1
        assert "Could not initialize" in capsys.readouterr().out

    @patch("odibi.catalog_sync.ALL_SYNC_TABLES", ["meta_runs", "meta_tables"])
    @patch("odibi.catalog_sync.DEFAULT_SYNC_TABLES", ["meta_runs"])
    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_target_connection_not_found_returns_1(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls, capsys
    ):
        mock_load.return_value = _make_project_config()
        mock_gcm.return_value = MagicMock()
        pm_instance = MagicMock()
        pm_instance.connections = {}
        mock_pm.return_value = pm_instance
        args = Namespace(config="test.yaml", tables=None, mode=None, dry_run=False)
        result = _sync_command(args)
        assert result == 1
        assert "not found" in capsys.readouterr().out

    @patch("odibi.catalog_sync.ALL_SYNC_TABLES", ["meta_runs", "meta_tables"])
    @patch("odibi.catalog_sync.DEFAULT_SYNC_TABLES", ["meta_runs"])
    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_successful_sync_returns_0(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls, capsys
    ):
        mock_load.return_value = _make_project_config()
        mock_gcm.return_value = MagicMock()
        pm_instance = MagicMock()
        pm_instance.connections = {"sql_prod": MagicMock()}
        mock_pm.return_value = pm_instance
        syncer = MagicMock()
        syncer.sync.return_value = {
            "meta_runs": {"success": True, "rows": 50},
            "meta_tables": {"success": True, "rows": 10},
        }
        mock_syncer_cls.return_value = syncer

        args = Namespace(config="test.yaml", tables=None, mode=None, dry_run=False)
        result = _sync_command(args)
        assert result == 0
        out = capsys.readouterr().out
        assert "Sync Results" in out
        assert "2/2" in out

    @patch("odibi.catalog_sync.ALL_SYNC_TABLES", ["meta_runs", "meta_tables"])
    @patch("odibi.catalog_sync.DEFAULT_SYNC_TABLES", ["meta_runs"])
    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_partial_failure_returns_1(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls, capsys
    ):
        mock_load.return_value = _make_project_config()
        mock_gcm.return_value = MagicMock()
        pm_instance = MagicMock()
        pm_instance.connections = {"sql_prod": MagicMock()}
        mock_pm.return_value = pm_instance
        syncer = MagicMock()
        syncer.sync.return_value = {
            "meta_runs": {"success": True, "rows": 50},
            "meta_tables": {"success": False, "error": "Connection lost"},
        }
        mock_syncer_cls.return_value = syncer

        args = Namespace(config="test.yaml", tables=None, mode=None, dry_run=False)
        result = _sync_command(args)
        assert result == 1
        out = capsys.readouterr().out
        assert "1/2" in out
        assert "Connection lost" in out

    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_exception_returns_1(self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls):
        mock_load.side_effect = RuntimeError("boom")
        args = Namespace(config="test.yaml", tables=None, mode=None, dry_run=False)
        result = _sync_command(args)
        assert result == 1


# ---------------------------------------------------------------------------
# _sync_status_command
# ---------------------------------------------------------------------------


class TestSyncStatusCommand:
    """Tests for _sync_status_command."""

    @patch("odibi.config.load_config_from_file")
    def test_no_system_config_returns_1(self, mock_load, capsys):
        mock_load.return_value = _make_project_config(system=False)
        args = Namespace(config="test.yaml")
        result = _sync_status_command(args)
        assert result == 1
        assert "No 'system'" in capsys.readouterr().out

    @patch("odibi.config.load_config_from_file")
    def test_no_sync_to_returns_0(self, mock_load, capsys):
        mock_load.return_value = _make_project_config(system=True, sync_to=False)
        args = Namespace(config="test.yaml")
        result = _sync_status_command(args)
        assert result == 0
        out = capsys.readouterr().out
        assert "No sync_to configured" in out

    @patch("odibi.catalog_sync.DEFAULT_SYNC_TABLES", ["meta_runs", "meta_tables"])
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.config.load_config_from_file")
    def test_with_sync_to_shows_config_and_timestamps(self, mock_load, mock_gcm, capsys):
        mock_load.return_value = _make_project_config()
        catalog = MagicMock()
        catalog.get_state.side_effect = lambda key: (
            "2026-04-19T10:00:00" if "meta_runs" in key else None
        )
        mock_gcm.return_value = catalog
        args = Namespace(config="test.yaml")
        result = _sync_status_command(args)
        assert result == 0
        out = capsys.readouterr().out
        assert "Sync Target: sql_prod" in out
        assert "Mode: incremental" in out
        assert "2026-04-19T10:00:00" in out
        assert "never synced" in out

    @patch("odibi.catalog_sync.DEFAULT_SYNC_TABLES", ["meta_runs"])
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.config.load_config_from_file")
    def test_timestamp_exception_shows_unknown(self, mock_load, mock_gcm, capsys):
        mock_load.return_value = _make_project_config()
        catalog = MagicMock()
        catalog.get_state.side_effect = RuntimeError("fail")
        mock_gcm.return_value = catalog
        args = Namespace(config="test.yaml")
        result = _sync_status_command(args)
        assert result == 0
        assert "unknown" in capsys.readouterr().out

    @patch("odibi.config.load_config_from_file")
    def test_exception_returns_1(self, mock_load):
        mock_load.side_effect = RuntimeError("boom")
        args = Namespace(config="test.yaml")
        result = _sync_status_command(args)
        assert result == 1

    @patch("odibi.catalog_sync.DEFAULT_SYNC_TABLES", ["meta_runs"])
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.config.load_config_from_file")
    def test_with_explicit_tables_in_config(self, mock_load, mock_gcm, capsys):
        project = _make_project_config()
        project.system.sync_to.tables = ["meta_runs", "meta_tables"]
        mock_load.return_value = project
        catalog = MagicMock()
        catalog.get_state.return_value = "2026-01-01"
        mock_gcm.return_value = catalog
        args = Namespace(config="test.yaml")
        result = _sync_status_command(args)
        assert result == 0
        out = capsys.readouterr().out
        assert "Tables: meta_runs, meta_tables" in out

    @patch("odibi.catalog_sync.DEFAULT_SYNC_TABLES", ["meta_runs"])
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.config.load_config_from_file")
    def test_catalog_none_skips_timestamps(self, mock_load, mock_gcm, capsys):
        mock_load.return_value = _make_project_config()
        mock_gcm.return_value = None
        args = Namespace(config="test.yaml")
        result = _sync_status_command(args)
        assert result == 0
        out = capsys.readouterr().out
        assert "Last Sync Timestamps" not in out


# ---------------------------------------------------------------------------
# _sync_purge_command
# ---------------------------------------------------------------------------


class TestSyncPurgeCommand:
    """Tests for _sync_purge_command."""

    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_no_system_or_sync_to_returns_1(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls, capsys
    ):
        mock_load.return_value = _make_project_config(system=True, sync_to=False)
        args = Namespace(config="test.yaml", days=90, dry_run=False)
        result = _sync_purge_command(args)
        assert result == 1
        assert "No 'sync_to'" in capsys.readouterr().out

    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_no_system_returns_1(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls, capsys
    ):
        mock_load.return_value = _make_project_config(system=False)
        args = Namespace(config="test.yaml", days=90, dry_run=False)
        result = _sync_purge_command(args)
        assert result == 1

    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_catalog_manager_none_returns_1(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls, capsys
    ):
        mock_load.return_value = _make_project_config()
        mock_gcm.return_value = None
        args = Namespace(config="test.yaml", days=90, dry_run=False)
        result = _sync_purge_command(args)
        assert result == 1
        assert "Could not initialize" in capsys.readouterr().out

    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_target_connection_not_found_returns_1(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls, capsys
    ):
        mock_load.return_value = _make_project_config()
        mock_gcm.return_value = MagicMock()
        pm_instance = MagicMock()
        pm_instance.connections = {}
        mock_pm.return_value = pm_instance
        args = Namespace(config="test.yaml", days=90, dry_run=False)
        result = _sync_purge_command(args)
        assert result == 1
        assert "not found" in capsys.readouterr().out

    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_target_not_sql_server_returns_1(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls, capsys
    ):
        mock_load.return_value = _make_project_config()
        mock_gcm.return_value = MagicMock()
        pm_instance = MagicMock()
        pm_instance.connections = {"sql_prod": MagicMock()}
        mock_pm.return_value = pm_instance
        syncer = MagicMock()
        syncer.target_type = "delta"
        mock_syncer_cls.return_value = syncer
        args = Namespace(config="test.yaml", days=90, dry_run=False)
        result = _sync_purge_command(args)
        assert result == 1
        assert "only supported for SQL Server" in capsys.readouterr().out

    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_dry_run_returns_0(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls, capsys
    ):
        mock_load.return_value = _make_project_config()
        mock_gcm.return_value = MagicMock()
        pm_instance = MagicMock()
        pm_instance.connections = {"sql_prod": MagicMock()}
        mock_pm.return_value = pm_instance
        syncer = MagicMock()
        syncer.target_type = "sql_server"
        mock_syncer_cls.return_value = syncer
        args = Namespace(config="test.yaml", days=90, dry_run=True)
        result = _sync_purge_command(args)
        assert result == 0
        out = capsys.readouterr().out
        assert "Dry Run" in out
        assert "90 days" in out

    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_successful_purge_returns_0(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls, capsys
    ):
        mock_load.return_value = _make_project_config()
        mock_gcm.return_value = MagicMock()
        pm_instance = MagicMock()
        pm_instance.connections = {"sql_prod": MagicMock()}
        mock_pm.return_value = pm_instance
        syncer = MagicMock()
        syncer.target_type = "sql_server"
        syncer.purge_sql_tables.return_value = {
            "meta_runs": {"success": True},
            "meta_failures": {"success": True},
        }
        mock_syncer_cls.return_value = syncer
        args = Namespace(config="test.yaml", days=90, dry_run=False)
        result = _sync_purge_command(args)
        assert result == 0
        out = capsys.readouterr().out
        assert "Purge Results" in out
        assert "2/2" in out

    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_partial_purge_failure_returns_1(
        self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls, capsys
    ):
        mock_load.return_value = _make_project_config()
        mock_gcm.return_value = MagicMock()
        pm_instance = MagicMock()
        pm_instance.connections = {"sql_prod": MagicMock()}
        mock_pm.return_value = pm_instance
        syncer = MagicMock()
        syncer.target_type = "sql_server"
        syncer.purge_sql_tables.return_value = {
            "meta_runs": {"success": True},
            "meta_failures": {"success": False, "error": "timeout"},
        }
        mock_syncer_cls.return_value = syncer
        args = Namespace(config="test.yaml", days=90, dry_run=False)
        result = _sync_purge_command(args)
        assert result == 1
        out = capsys.readouterr().out
        assert "1/2" in out
        assert "timeout" in out

    @patch("odibi.catalog_sync.CatalogSyncer", new_callable=MagicMock)
    @patch("odibi.config.load_config_from_file")
    @patch("odibi.cli.catalog._get_catalog_manager")
    @patch("odibi.cli.catalog.PipelineManager")
    @patch("odibi.cli.catalog.load_extensions")
    def test_exception_returns_1(self, mock_ext, mock_pm, mock_gcm, mock_load, mock_syncer_cls):
        mock_load.side_effect = RuntimeError("boom")
        args = Namespace(config="test.yaml", days=90, dry_run=False)
        result = _sync_purge_command(args)
        assert result == 1

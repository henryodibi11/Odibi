"""Tests for odibi.cli.schema — _schema_history, _schema_diff, _get_catalog_manager."""

import json
from argparse import Namespace
from datetime import datetime
from unittest.mock import MagicMock, patch

from odibi.cli.schema import _get_catalog_manager, _schema_diff, _schema_history


# ---------------------------------------------------------------------------
# _get_catalog_manager
# ---------------------------------------------------------------------------


class TestGetCatalogManager:
    def test_no_config_returns_none(self, capsys):
        result = _get_catalog_manager(None)
        assert result is None
        assert "Error: --config is required" in capsys.readouterr().out

    def test_file_not_found_returns_none(self, capsys):
        result = _get_catalog_manager("nonexistent_file_xyz.yaml")
        assert result is None
        assert "Config file not found" in capsys.readouterr().out

    @patch("odibi.cli.schema.load_config_from_file", side_effect=RuntimeError("bad config"))
    def test_generic_exception_returns_none(self, _mock, capsys):
        result = _get_catalog_manager("some.yaml")
        assert result is None
        assert "Error loading config" in capsys.readouterr().out

    @patch("odibi.cli.schema.load_config_from_file")
    def test_success_with_base_path(self, mock_load):
        mock_config = MagicMock()
        mock_config.engine = "local"
        mock_system_conn = MagicMock()
        mock_system_conn.base_path = "/data/"
        mock_config.system.path = "catalog"
        mock_config.system.connection = "sys_conn"
        mock_config.connections.get.return_value = mock_system_conn
        mock_load.return_value = mock_config

        with (
            patch("odibi.catalog.CatalogManager") as mock_cm,
            patch("odibi.engine.get_engine", create=True) as mock_engine,
        ):
            mock_engine.return_value = MagicMock()
            mock_cm.return_value = MagicMock()
            result = _get_catalog_manager("test.yaml")

        assert result is not None
        mock_cm.assert_called_once()
        call_kwargs = mock_cm.call_args[1]
        assert call_kwargs["base_path"] == "/data/catalog"

    @patch("odibi.cli.schema.load_config_from_file")
    def test_success_without_base_path(self, mock_load):
        mock_config = MagicMock()
        mock_config.engine = "local"
        mock_system_conn = MagicMock(spec=[])  # no base_path attribute
        mock_config.system.path = "catalog"
        mock_config.system.connection = "sys_conn"
        mock_config.connections.get.return_value = mock_system_conn
        mock_load.return_value = mock_config

        with (
            patch("odibi.catalog.CatalogManager") as mock_cm,
            patch("odibi.engine.get_engine", create=True) as mock_engine,
        ):
            mock_engine.return_value = MagicMock()
            mock_cm.return_value = MagicMock()
            result = _get_catalog_manager("test.yaml")

        assert result is not None
        call_kwargs = mock_cm.call_args[1]
        assert call_kwargs["base_path"] == "catalog"


# ---------------------------------------------------------------------------
# _schema_history
# ---------------------------------------------------------------------------


class TestSchemaHistory:
    def test_no_config_returns_1(self, capsys):
        args = Namespace(config=None, table="my_table", limit=10, format="table")
        result = _schema_history(args)
        assert result == 1
        assert "Error" in capsys.readouterr().out

    @patch("odibi.cli.schema._get_catalog_manager", return_value=None)
    def test_catalog_none_returns_1(self, _mock):
        args = Namespace(config="test.yaml", table="my_table", limit=10, format="table")
        result = _schema_history(args)
        assert result == 1

    @patch("odibi.cli.schema._get_catalog_manager")
    def test_no_history_prints_message(self, mock_gcm, capsys):
        catalog = MagicMock()
        catalog.get_schema_history.return_value = []
        mock_gcm.return_value = catalog

        args = Namespace(config="test.yaml", table="silver/customers", limit=10, format="table")
        result = _schema_history(args)
        assert result == 0
        assert "No schema history found" in capsys.readouterr().out

    @patch("odibi.cli.schema._get_catalog_manager")
    def test_history_json_format(self, mock_gcm, capsys):
        catalog = MagicMock()
        catalog.get_schema_history.return_value = [
            {"schema_version": 1, "captured_at": "2024-01-01", "columns": "{}"},
        ]
        mock_gcm.return_value = catalog

        args = Namespace(config="test.yaml", table="my_table", limit=10, format="json")
        result = _schema_history(args)
        assert result == 0
        parsed = json.loads(capsys.readouterr().out)
        assert parsed[0]["schema_version"] == 1

    @patch("odibi.cli.schema._get_catalog_manager")
    def test_history_table_format_initial_schema(self, mock_gcm, capsys):
        catalog = MagicMock()
        catalog.get_schema_history.return_value = [
            {
                "schema_version": 1,
                "captured_at": datetime(2024, 1, 1, 12, 0, 0),
                "columns": '{"id": "int", "name": "string"}',
                "columns_added": None,
                "columns_removed": None,
                "columns_type_changed": None,
            },
        ]
        mock_gcm.return_value = catalog

        args = Namespace(config="test.yaml", table="my_table", limit=10, format="table")
        result = _schema_history(args)
        assert result == 0
        output = capsys.readouterr().out
        assert "v1" in output
        assert "2024-01-01" in output
        assert "Initial schema (2 columns)" in output

    @patch("odibi.cli.schema._get_catalog_manager")
    def test_history_table_format_with_changes(self, mock_gcm, capsys):
        catalog = MagicMock()
        catalog.get_schema_history.return_value = [
            {
                "schema_version": 2,
                "captured_at": "2024-02-01",
                "columns": "{}",
                "columns_added": ["col_a", "col_b"],
                "columns_removed": ["col_c"],
                "columns_type_changed": ["col_d"],
            },
        ]
        mock_gcm.return_value = catalog

        args = Namespace(config="test.yaml", table="my_table", limit=10, format="table")
        result = _schema_history(args)
        assert result == 0
        output = capsys.readouterr().out
        assert "+col_a, col_b" in output
        assert "-col_c" in output
        assert "~col_d" in output

    @patch("odibi.cli.schema._get_catalog_manager")
    def test_history_table_format_no_changes_non_v1(self, mock_gcm, capsys):
        catalog = MagicMock()
        catalog.get_schema_history.return_value = [
            {
                "schema_version": 3,
                "captured_at": "2024-03-01",
                "columns": "{}",
                "columns_added": [],
                "columns_removed": [],
                "columns_type_changed": [],
            },
        ]
        mock_gcm.return_value = catalog

        args = Namespace(config="test.yaml", table="my_table", limit=10, format="table")
        result = _schema_history(args)
        assert result == 0
        output = capsys.readouterr().out
        assert "(no changes detected)" in output

    @patch("odibi.cli.schema._get_catalog_manager")
    def test_history_table_format_truncated_changes(self, mock_gcm, capsys):
        catalog = MagicMock()
        catalog.get_schema_history.return_value = [
            {
                "schema_version": 2,
                "captured_at": "2024-02-01",
                "columns": "{}",
                "columns_added": ["a", "b", "c", "d"],
                "columns_removed": ["e", "f", "g", "h"],
                "columns_type_changed": ["i", "j", "k", "l"],
            },
        ]
        mock_gcm.return_value = catalog

        args = Namespace(config="test.yaml", table="my_table", limit=10, format="table")
        result = _schema_history(args)
        assert result == 0
        output = capsys.readouterr().out
        assert "..." in output


# ---------------------------------------------------------------------------
# _schema_diff
# ---------------------------------------------------------------------------


class TestSchemaDiff:
    def test_no_config_returns_1(self, capsys):
        args = Namespace(config=None, table="my_table", from_version=1, to_version=2)
        result = _schema_diff(args)
        assert result == 1

    @patch("odibi.cli.schema._get_catalog_manager", return_value=None)
    def test_catalog_none_returns_1(self, _mock):
        args = Namespace(config="test.yaml", table="my_table", from_version=1, to_version=2)
        result = _schema_diff(args)
        assert result == 1

    @patch("odibi.cli.schema._get_catalog_manager")
    def test_no_history_returns_1(self, mock_gcm, capsys):
        catalog = MagicMock()
        catalog.get_schema_history.return_value = []
        mock_gcm.return_value = catalog

        args = Namespace(config="test.yaml", table="my_table", from_version=1, to_version=2)
        result = _schema_diff(args)
        assert result == 1
        assert "No schema history found" in capsys.readouterr().out

    @patch("odibi.cli.schema._get_catalog_manager")
    def test_default_versions_with_less_than_2_returns_1(self, mock_gcm, capsys):
        catalog = MagicMock()
        catalog.get_schema_history.return_value = [
            {"schema_version": 1, "columns": '{"id": "int"}'},
        ]
        mock_gcm.return_value = catalog

        args = Namespace(config="test.yaml", table="my_table", from_version=None, to_version=None)
        result = _schema_diff(args)
        assert result == 1
        assert "Need at least 2 versions" in capsys.readouterr().out

    @patch("odibi.cli.schema._get_catalog_manager")
    def test_from_version_not_found_returns_1(self, mock_gcm, capsys):
        catalog = MagicMock()
        catalog.get_schema_history.return_value = [
            {"schema_version": 2, "columns": '{"id": "int"}'},
        ]
        mock_gcm.return_value = catalog

        args = Namespace(config="test.yaml", table="my_table", from_version=99, to_version=2)
        result = _schema_diff(args)
        assert result == 1
        assert "v99" in capsys.readouterr().out

    @patch("odibi.cli.schema._get_catalog_manager")
    def test_to_version_not_found_returns_1(self, mock_gcm, capsys):
        catalog = MagicMock()
        catalog.get_schema_history.return_value = [
            {"schema_version": 1, "columns": '{"id": "int"}'},
        ]
        mock_gcm.return_value = catalog

        args = Namespace(config="test.yaml", table="my_table", from_version=1, to_version=99)
        result = _schema_diff(args)
        assert result == 1
        assert "v99" in capsys.readouterr().out

    @patch("odibi.cli.schema._get_catalog_manager")
    def test_diff_with_additions_removals_changes(self, mock_gcm, capsys):
        catalog = MagicMock()
        catalog.get_schema_history.return_value = [
            {"schema_version": 1, "columns": '{"id": "int", "old_col": "string"}'},
            {"schema_version": 2, "columns": '{"id": "bigint", "new_col": "float"}'},
        ]
        mock_gcm.return_value = catalog

        args = Namespace(config="test.yaml", table="my_table", from_version=1, to_version=2)
        result = _schema_diff(args)
        assert result == 0
        output = capsys.readouterr().out
        assert "+" in output  # new_col added
        assert "-" in output  # old_col removed
        assert "~" in output  # id type changed
        assert "new_col" in output
        assert "old_col" in output

    @patch("odibi.cli.schema._get_catalog_manager")
    def test_diff_default_versions_uses_latest_two(self, mock_gcm, capsys):
        catalog = MagicMock()
        catalog.get_schema_history.return_value = [
            {"schema_version": 3, "columns": '{"id": "int", "name": "string"}'},
            {"schema_version": 2, "columns": '{"id": "int"}'},
            {"schema_version": 1, "columns": '{"id": "int"}'},
        ]
        mock_gcm.return_value = catalog

        args = Namespace(config="test.yaml", table="my_table", from_version=None, to_version=None)
        result = _schema_diff(args)
        assert result == 0
        output = capsys.readouterr().out
        assert "v2" in output
        assert "v3" in output
        assert "name" in output

    @patch("odibi.cli.schema._get_catalog_manager")
    def test_diff_unchanged_columns(self, mock_gcm, capsys):
        catalog = MagicMock()
        catalog.get_schema_history.return_value = [
            {"schema_version": 1, "columns": '{"id": "int"}'},
            {"schema_version": 2, "columns": '{"id": "int"}'},
        ]
        mock_gcm.return_value = catalog

        args = Namespace(config="test.yaml", table="my_table", from_version=1, to_version=2)
        result = _schema_diff(args)
        assert result == 0
        output = capsys.readouterr().out
        assert "(unchanged)" in output

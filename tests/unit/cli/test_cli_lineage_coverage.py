"""Tests for _get_catalog_manager, _lineage_upstream, _lineage_downstream, _lineage_impact."""

import json
from argparse import Namespace
from unittest.mock import MagicMock, patch

from odibi.cli.lineage import (
    _get_catalog_manager,
    _lineage_downstream,
    _lineage_impact,
    _lineage_upstream,
)


# ────────────────────────────────────────────────────────────────────
#  _get_catalog_manager
# ────────────────────────────────────────────────────────────────────


class TestGetCatalogManager:
    """Tests for _get_catalog_manager."""

    def test_config_path_none_returns_none(self, capsys):
        result = _get_catalog_manager(None)
        assert result is None
        assert "Error: --config is required" in capsys.readouterr().out

    @patch("odibi.cli.lineage.load_config_from_file", side_effect=FileNotFoundError)
    def test_config_file_not_found_returns_none(self, _mock_load, capsys):
        result = _get_catalog_manager("missing.yaml")
        assert result is None
        assert "Config file not found" in capsys.readouterr().out

    @patch("odibi.cli.lineage.load_config_from_file", side_effect=RuntimeError("boom"))
    def test_other_exception_returns_none(self, _mock_load, capsys):
        result = _get_catalog_manager("bad.yaml")
        assert result is None
        assert "Error loading config: boom" in capsys.readouterr().out

    @patch("odibi.cli.lineage.load_config_from_file")
    def test_success_returns_catalog(self, mock_load):
        mock_system = MagicMock()
        mock_system.connection = "sys_conn"
        mock_system.path = "catalog"

        mock_conn = MagicMock()
        mock_conn.base_path = "/mnt/data/"

        mock_config = MagicMock()
        mock_config.system = mock_system
        mock_config.connections = {"sys_conn": mock_conn}
        mock_config.engine = MagicMock()

        mock_load.return_value = mock_config

        mock_cm_cls = MagicMock()
        mock_get_engine = MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "odibi.catalog": MagicMock(CatalogManager=mock_cm_cls),
                "odibi.engine": MagicMock(get_engine=mock_get_engine),
            },
        ):
            result = _get_catalog_manager("good.yaml")

        assert result is not None
        mock_load.assert_called_once_with("good.yaml")
        mock_cm_cls.assert_called_once()

    @patch("odibi.cli.lineage.load_config_from_file")
    def test_success_no_base_path(self, mock_load):
        mock_system = MagicMock()
        mock_system.connection = "sys_conn"
        mock_system.path = "catalog"

        mock_conn = MagicMock(spec=[])  # no base_path attribute

        mock_config = MagicMock()
        mock_config.system = mock_system
        mock_config.connections = {"sys_conn": mock_conn}
        mock_config.engine = MagicMock()

        mock_load.return_value = mock_config

        mock_cm_cls = MagicMock()
        mock_get_engine = MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "odibi.catalog": MagicMock(CatalogManager=mock_cm_cls),
                "odibi.engine": MagicMock(get_engine=mock_get_engine),
            },
        ):
            result = _get_catalog_manager("good.yaml")

        assert result is not None
        call_kwargs = mock_cm_cls.call_args[1]
        assert call_kwargs["base_path"] == "catalog"


# ────────────────────────────────────────────────────────────────────
#  _lineage_upstream
# ────────────────────────────────────────────────────────────────────


class TestLineageUpstream:
    """Tests for _lineage_upstream."""

    @patch("odibi.cli.lineage._get_catalog_manager", return_value=None)
    def test_catalog_none_returns_1(self, _mock_cm):
        args = Namespace(table="gold/customers", config="test.yaml", depth=3, format="tree")
        assert _lineage_upstream(args) == 1

    @patch("odibi.cli.lineage._get_catalog_manager")
    def test_no_upstream_found(self, mock_cm, capsys):
        mock_catalog = MagicMock()
        mock_catalog.get_upstream.return_value = []
        mock_cm.return_value = mock_catalog

        args = Namespace(table="gold/customers", config="test.yaml", depth=3, format="tree")
        result = _lineage_upstream(args)

        assert result == 0
        assert "No upstream lineage found" in capsys.readouterr().out

    @patch("odibi.cli.lineage._get_catalog_manager")
    def test_json_format(self, mock_cm, capsys):
        mock_catalog = MagicMock()
        mock_catalog.get_upstream.return_value = [{"depth": 0, "source_table": "bronze/raw"}]
        mock_cm.return_value = mock_catalog

        args = Namespace(table="gold/customers", config="test.yaml", depth=3, format="json")
        result = _lineage_upstream(args)

        assert result == 0
        output = capsys.readouterr().out
        parsed = json.loads(output)
        assert parsed[0]["source_table"] == "bronze/raw"

    @patch("odibi.cli.lineage._get_catalog_manager")
    def test_tree_format(self, mock_cm, capsys):
        mock_catalog = MagicMock()
        mock_catalog.get_upstream.return_value = [{"depth": 0, "source_table": "bronze/raw"}]
        mock_cm.return_value = mock_catalog

        args = Namespace(table="gold/customers", config="test.yaml", depth=3, format="tree")
        result = _lineage_upstream(args)

        assert result == 0
        output = capsys.readouterr().out
        assert "Upstream Lineage: gold/customers" in output
        assert "bronze/raw" in output


# ────────────────────────────────────────────────────────────────────
#  _lineage_downstream
# ────────────────────────────────────────────────────────────────────


class TestLineageDownstream:
    """Tests for _lineage_downstream."""

    @patch("odibi.cli.lineage._get_catalog_manager", return_value=None)
    def test_catalog_none_returns_1(self, _mock_cm):
        args = Namespace(table="bronze/raw", config="test.yaml", depth=3, format="tree")
        assert _lineage_downstream(args) == 1

    @patch("odibi.cli.lineage._get_catalog_manager")
    def test_no_downstream_found(self, mock_cm, capsys):
        mock_catalog = MagicMock()
        mock_catalog.get_downstream.return_value = []
        mock_cm.return_value = mock_catalog

        args = Namespace(table="bronze/raw", config="test.yaml", depth=3, format="tree")
        result = _lineage_downstream(args)

        assert result == 0
        assert "No downstream lineage found" in capsys.readouterr().out

    @patch("odibi.cli.lineage._get_catalog_manager")
    def test_json_format(self, mock_cm, capsys):
        mock_catalog = MagicMock()
        mock_catalog.get_downstream.return_value = [{"depth": 0, "target_table": "gold/report"}]
        mock_cm.return_value = mock_catalog

        args = Namespace(table="bronze/raw", config="test.yaml", depth=3, format="json")
        result = _lineage_downstream(args)

        assert result == 0
        output = capsys.readouterr().out
        parsed = json.loads(output)
        assert parsed[0]["target_table"] == "gold/report"

    @patch("odibi.cli.lineage._get_catalog_manager")
    def test_tree_format(self, mock_cm, capsys):
        mock_catalog = MagicMock()
        mock_catalog.get_downstream.return_value = [{"depth": 0, "target_table": "gold/report"}]
        mock_cm.return_value = mock_catalog

        args = Namespace(table="bronze/raw", config="test.yaml", depth=3, format="tree")
        result = _lineage_downstream(args)

        assert result == 0
        output = capsys.readouterr().out
        assert "Downstream Lineage: bronze/raw" in output
        assert "gold/report" in output


# ────────────────────────────────────────────────────────────────────
#  _lineage_impact
# ────────────────────────────────────────────────────────────────────


class TestLineageImpact:
    """Tests for _lineage_impact."""

    @patch("odibi.cli.lineage._get_catalog_manager", return_value=None)
    def test_catalog_none_returns_1(self, _mock_cm):
        args = Namespace(table="bronze/raw", config="test.yaml", depth=3)
        assert _lineage_impact(args) == 1

    @patch("odibi.cli.lineage._get_catalog_manager")
    def test_no_downstream_deps(self, mock_cm, capsys):
        mock_catalog = MagicMock()
        mock_catalog.get_downstream.return_value = []
        mock_cm.return_value = mock_catalog

        args = Namespace(table="bronze/raw", config="test.yaml", depth=3)
        result = _lineage_impact(args)

        assert result == 0
        assert "No downstream dependencies found" in capsys.readouterr().out

    @patch("odibi.cli.lineage._get_catalog_manager")
    def test_impact_with_affected_tables_and_pipelines(self, mock_cm, capsys):
        mock_catalog = MagicMock()
        mock_catalog.get_downstream.return_value = [
            {"depth": 0, "target_table": "silver/cleaned", "target_pipeline": "etl_silver"},
            {"depth": 1, "target_table": "gold/report", "target_pipeline": "etl_gold"},
        ]
        mock_cm.return_value = mock_catalog

        args = Namespace(table="bronze/raw", config="test.yaml", depth=3)
        result = _lineage_impact(args)

        assert result == 0
        output = capsys.readouterr().out
        assert "Impact Analysis: bronze/raw" in output
        assert "silver/cleaned" in output
        assert "gold/report" in output
        assert "2 downstream table(s)" in output
        assert "2 pipeline(s)" in output

    @patch("odibi.cli.lineage._get_catalog_manager")
    def test_impact_with_no_pipeline_info(self, mock_cm, capsys):
        mock_catalog = MagicMock()
        mock_catalog.get_downstream.return_value = [
            {"depth": 0, "target_table": "silver/cleaned"},
        ]
        mock_cm.return_value = mock_catalog

        args = Namespace(table="bronze/raw", config="test.yaml", depth=3)
        result = _lineage_impact(args)

        assert result == 0
        output = capsys.readouterr().out
        assert "1 downstream table(s)" in output
        assert "0 pipeline(s)" in output

"""Tests for odibi.cli.schema, odibi.cli.secrets, and odibi.cli.lineage."""

import os
from unittest.mock import MagicMock, patch

import pytest

from odibi.cli.lineage import _build_tree, _print_tree, lineage_command
from odibi.cli.schema import schema_command
from odibi.cli.secrets import (
    SimpleConnection,
    extract_env_vars,
    init_command,
    secrets_command,
    validate_command,
)


# ────────────────────────────────────────────────────────────────────
#  extract_env_vars
# ────────────────────────────────────────────────────────────────────


class TestExtractEnvVars:
    """Tests for extract_env_vars."""

    def test_dollar_brace_patterns(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("host: ${DB_HOST}\nport: ${DB_PORT}\n")
        result = extract_env_vars(str(cfg))
        assert result == {"DB_HOST", "DB_PORT"}

    def test_env_colon_patterns(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("key: ${env:SECRET_KEY}\ntoken: ${env:API_TOKEN}\n")
        result = extract_env_vars(str(cfg))
        assert result == {"SECRET_KEY", "API_TOKEN"}

    def test_mixed_patterns(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("a: ${VAR_A}\nb: ${env:VAR_B}\n")
        result = extract_env_vars(str(cfg))
        assert result == {"VAR_A", "VAR_B"}

    def test_no_patterns_returns_empty(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("host: localhost\nport: 5432\n")
        result = extract_env_vars(str(cfg))
        assert result == set()

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            extract_env_vars("/no/such/file.yaml")

    def test_duplicate_vars_deduplicated(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("a: ${MY_VAR}\nb: ${MY_VAR}\nc: ${env:MY_VAR}\n")
        result = extract_env_vars(str(cfg))
        assert result == {"MY_VAR"}


# ────────────────────────────────────────────────────────────────────
#  init_command
# ────────────────────────────────────────────────────────────────────


class TestInitCommand:
    """Tests for init_command."""

    def test_creates_template(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("host: ${DB_HOST}\nport: ${DB_PORT}\n")
        output = tmp_path / ".env.template"

        args = MagicMock()
        args.config = str(cfg)
        args.output = str(output)
        args.force = False

        result = init_command(args)

        assert result == 0
        assert output.exists()
        content = output.read_text()
        assert "DB_HOST=" in content
        assert "DB_PORT=" in content

    def test_no_vars_returns_0(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("host: localhost\n")
        output = tmp_path / ".env.template"

        args = MagicMock()
        args.config = str(cfg)
        args.output = str(output)
        args.force = False

        result = init_command(args)

        assert result == 0
        assert not output.exists()

    def test_existing_output_without_force_returns_1(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("host: ${DB_HOST}\n")
        output = tmp_path / ".env.template"
        output.write_text("existing")

        args = MagicMock()
        args.config = str(cfg)
        args.output = str(output)
        args.force = False

        result = init_command(args)

        assert result == 1
        assert output.read_text() == "existing"

    def test_existing_output_with_force_overwrites(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("host: ${DB_HOST}\n")
        output = tmp_path / ".env.template"
        output.write_text("old content")

        args = MagicMock()
        args.config = str(cfg)
        args.output = str(output)
        args.force = True

        result = init_command(args)

        assert result == 0
        content = output.read_text()
        assert "DB_HOST=" in content
        assert "old content" not in content

    def test_template_vars_sorted(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("z: ${ZEBRA}\na: ${ALPHA}\nm: ${MIDDLE}\n")
        output = tmp_path / ".env.template"

        args = MagicMock()
        args.config = str(cfg)
        args.output = str(output)
        args.force = False

        init_command(args)

        lines = output.read_text().strip().splitlines()
        var_lines = [line for line in lines if "=" in line and not line.startswith("#")]
        assert var_lines == ["ALPHA=", "MIDDLE=", "ZEBRA="]


# ────────────────────────────────────────────────────────────────────
#  SimpleConnection
# ────────────────────────────────────────────────────────────────────


class TestSimpleConnection:
    """Tests for SimpleConnection."""

    def test_flat_structure(self):
        conn = SimpleConnection(
            "test",
            {
                "key_vault_name": "myvault",
                "secret_name": "mysecret",
                "account_name": "myaccount",
            },
        )
        assert conn.name == "test"
        assert conn.key_vault_name == "myvault"
        assert conn.secret_name == "mysecret"
        assert conn.account == "myaccount"

    def test_auth_dict_structure(self):
        conn = SimpleConnection(
            "test",
            {
                "auth": {
                    "key_vault_name": "vault2",
                    "secret_name": "secret2",
                }
            },
        )
        assert conn.key_vault_name == "vault2"
        assert conn.secret_name == "secret2"

    def test_missing_fields_none(self):
        conn = SimpleConnection("test", {})
        assert conn.key_vault_name is None
        assert conn.secret_name is None

    def test_account_fallback(self):
        conn = SimpleConnection("test", {"account": "fallback_acct"})
        assert conn.account == "fallback_acct"

    def test_account_default_unknown(self):
        conn = SimpleConnection("test", {})
        assert conn.account == "unknown"

    def test_flat_overrides_auth(self):
        conn = SimpleConnection(
            "test",
            {
                "key_vault_name": "flat_vault",
                "auth": {"key_vault_name": "auth_vault"},
            },
        )
        assert conn.key_vault_name == "flat_vault"


# ────────────────────────────────────────────────────────────────────
#  secrets_command
# ────────────────────────────────────────────────────────────────────


class TestSecretsCommand:
    """Tests for secrets_command dispatcher."""

    @patch("odibi.cli.secrets.init_command", return_value=0)
    def test_dispatches_to_init(self, mock_init):
        args = MagicMock()
        args.secrets_command = "init"
        result = secrets_command(args)
        assert result == 0
        mock_init.assert_called_once_with(args)

    @patch("odibi.cli.secrets.validate_command", return_value=0)
    def test_dispatches_to_validate(self, mock_validate):
        args = MagicMock()
        args.secrets_command = "validate"
        result = secrets_command(args)
        assert result == 0
        mock_validate.assert_called_once_with(args)

    def test_no_command_returns_1(self):
        args = MagicMock()
        args.secrets_command = None
        result = secrets_command(args)
        assert result == 1

    def test_unknown_command_returns_1(self):
        args = MagicMock()
        args.secrets_command = "unknown"
        result = secrets_command(args)
        assert result == 1


# ────────────────────────────────────────────────────────────────────
#  validate_command
# ────────────────────────────────────────────────────────────────────


class TestValidateCommand:
    """Tests for validate_command."""

    @patch("odibi.cli.secrets.check_keyvault_access", return_value=True)
    @patch("odibi.cli.secrets.load_dotenv")
    def test_all_vars_set_returns_0(self, _mock_dotenv, _mock_kv, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("host: ${TEST_VAR_XYZ}\n")

        args = MagicMock()
        args.config = str(cfg)

        with patch.dict(os.environ, {"TEST_VAR_XYZ": "value"}):
            result = validate_command(args)

        assert result == 0

    @patch("odibi.cli.secrets.load_dotenv")
    def test_missing_vars_returns_1(self, _mock_dotenv, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("host: ${MISSING_VAR_UNIQUE_ABC}\n")

        args = MagicMock()
        args.config = str(cfg)

        env = os.environ.copy()
        env.pop("MISSING_VAR_UNIQUE_ABC", None)
        with patch.dict(os.environ, env, clear=True):
            result = validate_command(args)

        assert result == 1

    @patch("odibi.cli.secrets.check_keyvault_access", return_value=True)
    @patch("odibi.cli.secrets.load_dotenv")
    def test_no_vars_returns_0(self, _mock_dotenv, _mock_kv, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("host: localhost\n")

        args = MagicMock()
        args.config = str(cfg)

        result = validate_command(args)
        assert result == 0


# ────────────────────────────────────────────────────────────────────
#  _build_tree
# ────────────────────────────────────────────────────────────────────


class TestBuildTree:
    """Tests for _build_tree."""

    def test_upstream_builds_correct_tree(self):
        records = [
            {"depth": 0, "source_table": "bronze/raw_data"},
            {"depth": 0, "source_table": "bronze/raw_other"},
        ]
        tree = _build_tree(records, "silver/customers", direction="upstream")

        assert tree["name"] == "silver/customers"
        assert len(tree["children"]) == 2
        child_names = [c["name"] for c in tree["children"]]
        assert "bronze/raw_data" in child_names
        assert "bronze/raw_other" in child_names

    def test_downstream_builds_correct_tree(self):
        records = [
            {"depth": 0, "target_table": "gold/agg_table"},
        ]
        tree = _build_tree(records, "silver/customers", direction="downstream")

        assert tree["name"] == "silver/customers"
        assert len(tree["children"]) == 1
        assert tree["children"][0]["name"] == "gold/agg_table"

    def test_empty_records_root_no_children(self):
        tree = _build_tree([], "silver/customers", direction="upstream")
        assert tree["name"] == "silver/customers"
        assert tree["children"] == []

    def test_records_with_pipeline_info_upstream(self):
        records = [
            {
                "depth": 0,
                "source_table": "bronze/raw",
                "source_pipeline": "ingest",
                "source_node": "load_raw",
            },
        ]
        tree = _build_tree(records, "silver/out", direction="upstream")

        child_name = tree["children"][0]["name"]
        assert "bronze/raw" in child_name
        assert "(ingest.load_raw)" in child_name

    def test_records_with_pipeline_info_downstream(self):
        records = [
            {
                "depth": 0,
                "target_table": "gold/report",
                "target_pipeline": "reporting",
                "target_node": "build_report",
            },
        ]
        tree = _build_tree(records, "silver/out", direction="downstream")

        child_name = tree["children"][0]["name"]
        assert "gold/report" in child_name
        assert "(reporting.build_report)" in child_name

    def test_deeper_records_ignored_by_depth0(self):
        records = [
            {"depth": 0, "source_table": "bronze/raw"},
            {"depth": 1, "source_table": "landing/file"},
        ]
        tree = _build_tree(records, "silver/out", direction="upstream")
        assert len(tree["children"]) == 1
        assert tree["children"][0]["name"] == "bronze/raw"


# ────────────────────────────────────────────────────────────────────
#  _print_tree
# ────────────────────────────────────────────────────────────────────


class TestPrintTree:
    """Tests for _print_tree."""

    def test_root_printed_at_depth_0(self, capsys):
        node = {"name": "root_table", "children": []}
        _print_tree(node)
        captured = capsys.readouterr()
        assert "root_table" in captured.out
        assert "├" not in captured.out
        assert "└" not in captured.out

    def test_children_use_connectors(self, capsys):
        node = {
            "name": "root",
            "children": [
                {"name": "child1", "children": []},
                {"name": "child2", "children": []},
            ],
        }
        _print_tree(node)
        captured = capsys.readouterr()
        assert "├── child1" in captured.out
        assert "└── child2" in captured.out

    def test_single_child_uses_last_connector(self, capsys):
        node = {
            "name": "root",
            "children": [{"name": "only_child", "children": []}],
        }
        _print_tree(node)
        captured = capsys.readouterr()
        assert "└── only_child" in captured.out

    def test_nested_children(self, capsys):
        node = {
            "name": "root",
            "children": [
                {
                    "name": "child",
                    "children": [{"name": "grandchild", "children": []}],
                },
            ],
        }
        _print_tree(node)
        captured = capsys.readouterr()
        assert "grandchild" in captured.out


# ────────────────────────────────────────────────────────────────────
#  lineage_command
# ────────────────────────────────────────────────────────────────────


class TestLineageCommand:
    """Tests for lineage_command dispatcher."""

    def test_no_command_returns_1(self):
        args = MagicMock()
        args.lineage_command = None
        result = lineage_command(args)
        assert result == 1

    def test_missing_attribute_returns_1(self):
        args = MagicMock(spec=[])
        result = lineage_command(args)
        assert result == 1

    def test_unknown_command_returns_1(self):
        args = MagicMock()
        args.lineage_command = "bogus"
        result = lineage_command(args)
        assert result == 1

    @patch("odibi.cli.lineage._lineage_upstream", return_value=0)
    def test_dispatches_upstream(self, mock_up):
        args = MagicMock()
        args.lineage_command = "upstream"
        result = lineage_command(args)
        assert result == 0
        mock_up.assert_called_once_with(args)

    @patch("odibi.cli.lineage._lineage_downstream", return_value=0)
    def test_dispatches_downstream(self, mock_down):
        args = MagicMock()
        args.lineage_command = "downstream"
        result = lineage_command(args)
        assert result == 0
        mock_down.assert_called_once_with(args)

    @patch("odibi.cli.lineage._lineage_impact", return_value=0)
    def test_dispatches_impact(self, mock_impact):
        args = MagicMock()
        args.lineage_command = "impact"
        result = lineage_command(args)
        assert result == 0
        mock_impact.assert_called_once_with(args)


# ────────────────────────────────────────────────────────────────────
#  schema_command
# ────────────────────────────────────────────────────────────────────


class TestSchemaCommand:
    """Tests for schema_command dispatcher."""

    def test_no_command_returns_1(self):
        args = MagicMock()
        args.schema_command = None
        result = schema_command(args)
        assert result == 1

    def test_missing_attribute_returns_1(self):
        args = MagicMock(spec=[])
        result = schema_command(args)
        assert result == 1

    def test_unknown_command_returns_1(self):
        args = MagicMock()
        args.schema_command = "bogus"
        result = schema_command(args)
        assert result == 1

    @patch("odibi.cli.schema._schema_history", return_value=0)
    def test_dispatches_history(self, mock_hist):
        args = MagicMock()
        args.schema_command = "history"
        result = schema_command(args)
        assert result == 0
        mock_hist.assert_called_once_with(args)

    @patch("odibi.cli.schema._schema_diff", return_value=0)
    def test_dispatches_diff(self, mock_diff):
        args = MagicMock()
        args.schema_command = "diff"
        result = schema_command(args)
        assert result == 0
        mock_diff.assert_called_once_with(args)

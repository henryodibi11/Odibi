"""Tests for odibi.cli.list_cmd — list_command, explain, helpers, and sub-commands."""

import json
from argparse import Namespace
from unittest.mock import MagicMock, patch

from odibi.cli.list_cmd import (
    _extract_pattern_params,
    _get_first_line,
    _serialize_default,
    explain_command,
    list_command,
    list_connections_command,
    list_patterns_command,
    list_recipes_command,
    list_transformers_command,
    _ensure_initialized,
)


# ────────────────────────────────────────────────────────────────────
#  _get_first_line
# ────────────────────────────────────────────────────────────────────


class TestGetFirstLine:
    def test_none_returns_none(self):
        assert _get_first_line(None) is None

    def test_empty_returns_none(self):
        assert _get_first_line("") is None

    def test_multiline_returns_first(self):
        assert _get_first_line("first line\nsecond line\nthird") == "first line"

    def test_single_line(self):
        assert _get_first_line("only line") == "only line"

    def test_whitespace_stripped(self):
        assert _get_first_line("  padded  \nnext") == "padded"


# ────────────────────────────────────────────────────────────────────
#  _serialize_default
# ────────────────────────────────────────────────────────────────────


class TestSerializeDefault:
    def test_none(self):
        assert _serialize_default(None) is None

    def test_str(self):
        assert _serialize_default("hello") == "hello"

    def test_int(self):
        assert _serialize_default(42) == 42

    def test_bool(self):
        assert _serialize_default(True) is True

    def test_list(self):
        assert _serialize_default([1, 2]) == [1, 2]

    def test_dict(self):
        assert _serialize_default({"a": 1}) == {"a": 1}

    def test_object_returns_str(self):
        obj = object()
        assert _serialize_default(obj) == str(obj)


# ────────────────────────────────────────────────────────────────────
#  _extract_pattern_params
# ────────────────────────────────────────────────────────────────────


class TestExtractPatternParams:
    def test_parses_docstring_config_section(self):
        cls = MagicMock()
        cls.__name__ = "CustomPattern"
        cls.__doc__ = (
            "Some pattern.\n\n"
            "Configuration Options:\n"
            "- **key** (str, required): The key column\n"
            "- **mode** (str): Optional mode\n"
            "\nOther section.\n"
        )
        params = _extract_pattern_params(cls)
        assert len(params) == 2
        assert params[0] == {"name": "key", "required": True}
        assert params[1] == {"name": "mode", "required": False}

    def test_fallback_dimension_pattern(self):
        cls = MagicMock()
        cls.__name__ = "DimensionPattern"
        cls.__doc__ = "No config section here."
        params = _extract_pattern_params(cls)
        assert any(p["name"] == "natural_key" for p in params)

    def test_fallback_fact_pattern(self):
        cls = MagicMock()
        cls.__name__ = "FactPattern"
        cls.__doc__ = ""
        params = _extract_pattern_params(cls)
        assert any(p["name"] == "grain" for p in params)

    def test_fallback_scd2_pattern(self):
        cls = MagicMock()
        cls.__name__ = "SCD2Pattern"
        cls.__doc__ = ""
        params = _extract_pattern_params(cls)
        assert any(p["name"] == "natural_key" for p in params)

    def test_fallback_merge_pattern(self):
        cls = MagicMock()
        cls.__name__ = "MergePattern"
        cls.__doc__ = ""
        params = _extract_pattern_params(cls)
        assert any(p["name"] == "merge_key" for p in params)

    def test_fallback_aggregation_pattern(self):
        cls = MagicMock()
        cls.__name__ = "AggregationPattern"
        cls.__doc__ = ""
        params = _extract_pattern_params(cls)
        assert any(p["name"] == "group_by" for p in params)

    def test_fallback_date_dimension_pattern(self):
        cls = MagicMock()
        cls.__name__ = "DateDimensionPattern"
        cls.__doc__ = ""
        params = _extract_pattern_params(cls)
        assert any(p["name"] == "start_date" for p in params)

    def test_unknown_pattern_no_docstring_returns_empty(self):
        cls = MagicMock()
        cls.__name__ = "UnknownPattern"
        cls.__doc__ = ""
        params = _extract_pattern_params(cls)
        assert params == []


# ────────────────────────────────────────────────────────────────────
#  _ensure_initialized
# ────────────────────────────────────────────────────────────────────


class TestEnsureInitialized:
    @patch("odibi.cli.list_cmd.register_builtins")
    @patch("odibi.cli.list_cmd.register_standard_library")
    @patch("odibi.cli.list_cmd.FunctionRegistry")
    @patch("odibi.cli.list_cmd._CONNECTION_FACTORIES", {})
    def test_registers_when_empty(self, mock_reg, mock_std, mock_builtins):
        mock_reg.list_functions.return_value = []
        _ensure_initialized()
        mock_std.assert_called_once()
        mock_builtins.assert_called_once()

    @patch("odibi.cli.list_cmd.register_builtins")
    @patch("odibi.cli.list_cmd.register_standard_library")
    @patch("odibi.cli.list_cmd.FunctionRegistry")
    @patch("odibi.cli.list_cmd._CONNECTION_FACTORIES", {"local": object()})
    def test_skips_when_populated(self, mock_reg, mock_std, mock_builtins):
        mock_reg.list_functions.return_value = ["rename_columns"]
        _ensure_initialized()
        mock_std.assert_not_called()
        mock_builtins.assert_not_called()


# ────────────────────────────────────────────────────────────────────
#  list_command (dispatcher)
# ────────────────────────────────────────────────────────────────────


class TestListCommand:
    @patch("odibi.cli.list_cmd.list_transformers_command", return_value=0)
    def test_dispatches_transformers(self, mock_fn):
        args = Namespace(list_command="transformers", format="table")
        assert list_command(args) == 0
        mock_fn.assert_called_once_with(args)

    @patch("odibi.cli.list_cmd.list_patterns_command", return_value=0)
    def test_dispatches_patterns(self, mock_fn):
        args = Namespace(list_command="patterns", format="table")
        assert list_command(args) == 0
        mock_fn.assert_called_once_with(args)

    @patch("odibi.cli.list_cmd.list_connections_command", return_value=0)
    def test_dispatches_connections(self, mock_fn):
        args = Namespace(list_command="connections", format="table")
        assert list_command(args) == 0
        mock_fn.assert_called_once_with(args)

    @patch("odibi.cli.list_cmd.list_recipes_command", return_value=0)
    def test_dispatches_recipes(self, mock_fn):
        args = Namespace(list_command="recipes", format="table")
        assert list_command(args) == 0
        mock_fn.assert_called_once_with(args)

    def test_unknown_returns_1(self, capsys):
        args = Namespace(list_command=None, format="table")
        assert list_command(args) == 1
        assert "Usage:" in capsys.readouterr().out


# ────────────────────────────────────────────────────────────────────
#  list_transformers_command
# ────────────────────────────────────────────────────────────────────


class TestListTransformersCommand:
    @patch("odibi.cli.list_cmd._ensure_initialized")
    @patch("odibi.cli.list_cmd.FunctionRegistry")
    def test_table_format(self, mock_reg, _mock_init, capsys):
        mock_reg.list_functions.return_value = ["rename_columns", "drop_columns"]
        mock_reg.get_function_info.return_value = {
            "docstring": "Some doc",
            "parameters": {},
        }

        args = Namespace(format="table")
        result = list_transformers_command(args)

        assert result == 0
        output = capsys.readouterr().out
        assert "Available Transformers (2):" in output
        assert "rename_columns" in output

    @patch("odibi.cli.list_cmd._ensure_initialized")
    @patch("odibi.cli.list_cmd.FunctionRegistry")
    def test_json_format(self, mock_reg, _mock_init, capsys):
        mock_reg.list_functions.return_value = ["rename_columns"]
        mock_reg.get_function_info.return_value = {
            "docstring": "Rename columns\nMore details.",
            "parameters": {
                "current": {"required": True},
                "mapping": {"required": True, "default": None},
            },
        }

        args = Namespace(format="json")
        result = list_transformers_command(args)

        assert result == 0
        output = capsys.readouterr().out
        parsed = json.loads(output)
        assert len(parsed) == 1
        assert parsed[0]["name"] == "rename_columns"
        # "current" param should be excluded
        param_names = [p["name"] for p in parsed[0]["parameters"]]
        assert "current" not in param_names
        assert "mapping" in param_names


# ────────────────────────────────────────────────────────────────────
#  list_patterns_command
# ────────────────────────────────────────────────────────────────────


class TestListPatternsCommand:
    @patch(
        "odibi.cli.list_cmd._PATTERNS",
        {"scd2": MagicMock(__name__="SCD2Pattern", __doc__="SCD2 docs")},
    )
    def test_table_format(self, capsys):
        args = Namespace(format="table")
        result = list_patterns_command(args)

        assert result == 0
        output = capsys.readouterr().out
        assert "Available Patterns (1):" in output
        assert "scd2" in output

    @patch("odibi.cli.list_cmd._extract_pattern_params", return_value=[])
    @patch(
        "odibi.cli.list_cmd._PATTERNS",
        {"scd2": MagicMock(__name__="SCD2Pattern", __doc__="SCD2 pattern\nMore.")},
    )
    def test_json_format(self, _mock_extract, capsys):
        args = Namespace(format="json")
        result = list_patterns_command(args)

        assert result == 0
        parsed = json.loads(capsys.readouterr().out)
        assert len(parsed) == 1
        assert parsed[0]["name"] == "scd2"
        assert parsed[0]["class"] == "SCD2Pattern"


# ────────────────────────────────────────────────────────────────────
#  list_connections_command
# ────────────────────────────────────────────────────────────────────


class TestListConnectionsCommand:
    @patch("odibi.cli.list_cmd._ensure_initialized")
    @patch("odibi.cli.list_cmd._CONNECTION_FACTORIES", {"local": object(), "http": object()})
    def test_table_format(self, _mock_init, capsys):
        args = Namespace(format="table")
        result = list_connections_command(args)

        assert result == 0
        output = capsys.readouterr().out
        assert "Available Connection Types (2):" in output
        assert "local" in output
        assert "http" in output

    @patch("odibi.cli.list_cmd._ensure_initialized")
    @patch("odibi.cli.list_cmd._CONNECTION_FACTORIES", {"local": object()})
    def test_json_format(self, _mock_init, capsys):
        args = Namespace(format="json")
        result = list_connections_command(args)

        assert result == 0
        parsed = json.loads(capsys.readouterr().out)
        assert len(parsed) == 1
        assert parsed[0]["name"] == "local"
        assert "Local filesystem" in parsed[0]["description"]


# ────────────────────────────────────────────────────────────────────
#  list_recipes_command
# ────────────────────────────────────────────────────────────────────


class TestListRecipesCommand:
    @patch("odibi.recipes.get_recipe_registry")
    def test_no_recipes(self, mock_registry, capsys):
        mock_reg = MagicMock()
        mock_reg.list_recipes.return_value = {}
        mock_registry.return_value = mock_reg

        args = Namespace(format="table")
        result = list_recipes_command(args)

        assert result == 0
        assert "No recipes available." in capsys.readouterr().out

    @patch("odibi.recipes.get_recipe_registry")
    def test_table_format(self, mock_registry, capsys):
        recipe = MagicMock()
        recipe.description = "A bronze recipe"
        mock_reg = MagicMock()
        mock_reg.list_recipes.return_value = {"bronze_ingest": recipe}
        mock_registry.return_value = mock_reg

        args = Namespace(format="table")
        result = list_recipes_command(args)

        assert result == 0
        output = capsys.readouterr().out
        assert "Available Recipes (1):" in output
        assert "bronze_ingest" in output

    @patch("odibi.recipes.get_recipe_registry")
    def test_json_format(self, mock_registry, capsys):
        recipe = MagicMock()
        recipe.description = "A bronze recipe"
        recipe.required_vars = ["source"]
        recipe.optional_vars = {"format": "parquet"}
        recipe.template = {"input": "val"}
        mock_reg = MagicMock()
        mock_reg.list_recipes.return_value = {"bronze_ingest": recipe}
        mock_registry.return_value = mock_reg

        args = Namespace(format="json")
        result = list_recipes_command(args)

        assert result == 0
        parsed = json.loads(capsys.readouterr().out)
        assert len(parsed) == 1
        assert parsed[0]["name"] == "bronze_ingest"
        assert parsed[0]["required_vars"] == ["source"]


# ────────────────────────────────────────────────────────────────────
#  explain_command
# ────────────────────────────────────────────────────────────────────


class TestExplainCommand:
    @patch("odibi.recipes.get_recipe_registry")
    @patch("odibi.cli.list_cmd._ensure_initialized")
    @patch("odibi.cli.list_cmd._CONNECTION_FACTORIES", {})
    @patch("odibi.cli.list_cmd._PATTERNS", {})
    @patch("odibi.cli.list_cmd.FunctionRegistry")
    def test_transformer_found(self, mock_reg, _mock_init, mock_recipe_reg, capsys):
        mock_reg.has_function.return_value = True
        mock_reg.get_function_info.return_value = {
            "docstring": "Rename columns in a DataFrame.",
            "parameters": {
                "current": {"required": True},
                "mapping": {"required": True, "default": None},
                "prefix": {"required": False, "default": "col_"},
            },
        }
        mock_recipe_reg.return_value.get.return_value = None

        args = Namespace(name="rename_columns")
        result = explain_command(args)

        assert result == 0
        output = capsys.readouterr().out
        assert "Transformer: rename_columns" in output
        assert "mapping: required" in output
        assert "prefix: optional (default: col_)" in output

    @patch("odibi.recipes.get_recipe_registry")
    @patch("odibi.cli.list_cmd._ensure_initialized")
    @patch("odibi.cli.list_cmd._CONNECTION_FACTORIES", {})
    @patch(
        "odibi.cli.list_cmd._extract_pattern_params",
        return_value=[{"name": "natural_key", "required": True}],
    )
    @patch("odibi.cli.list_cmd.FunctionRegistry")
    def test_pattern_found(self, mock_reg, _mock_extract, _mock_init, mock_recipe_reg, capsys):
        mock_reg.has_function.return_value = False
        mock_recipe_reg.return_value.get.return_value = None

        mock_cls = MagicMock()
        mock_cls.__name__ = "DimensionPattern"
        mock_cls.__doc__ = "Dimension pattern docs."

        with patch("odibi.cli.list_cmd._PATTERNS", {"dimension": mock_cls}):
            args = Namespace(name="dimension")
            result = explain_command(args)

        assert result == 0
        output = capsys.readouterr().out
        assert "Pattern: dimension" in output

    @patch("odibi.recipes.get_recipe_registry")
    @patch("odibi.cli.list_cmd._ensure_initialized")
    @patch("odibi.cli.list_cmd._PATTERNS", {})
    @patch("odibi.cli.list_cmd.FunctionRegistry")
    def test_connection_found(self, mock_reg, _mock_init, mock_recipe_reg, capsys):
        mock_reg.has_function.return_value = False
        mock_recipe_reg.return_value.get.return_value = None

        with patch("odibi.cli.list_cmd._CONNECTION_FACTORIES", {"local": object()}):
            args = Namespace(name="local")
            result = explain_command(args)

        assert result == 0
        output = capsys.readouterr().out
        assert "Connection: local" in output

    @patch("odibi.cli.list_cmd._ensure_initialized")
    @patch("odibi.cli.list_cmd._CONNECTION_FACTORIES", {})
    @patch("odibi.cli.list_cmd._PATTERNS", {})
    @patch("odibi.cli.list_cmd.FunctionRegistry")
    def test_recipe_found(self, mock_reg, _mock_init, capsys):
        mock_reg.has_function.return_value = False

        recipe = MagicMock()
        recipe.description = "A test recipe"
        recipe.required_vars = ["source"]
        recipe.optional_vars = {"fmt": "parquet"}
        recipe.template = {"input": "val"}

        mock_registry = MagicMock()
        mock_registry.get.return_value = recipe
        mock_registry.list_recipes.return_value = {"test_recipe": recipe}

        with patch("odibi.recipes.get_recipe_registry", return_value=mock_registry):
            args = Namespace(name="test_recipe")
            result = explain_command(args)

        assert result == 0
        output = capsys.readouterr().out
        assert "Recipe: test_recipe" in output
        assert "Required Variables:" in output

    @patch("odibi.recipes.get_recipe_registry")
    @patch("odibi.cli.list_cmd._ensure_initialized")
    @patch("odibi.cli.list_cmd._CONNECTION_FACTORIES", {})
    @patch("odibi.cli.list_cmd._PATTERNS", {})
    @patch("odibi.cli.list_cmd.FunctionRegistry")
    def test_not_found(self, mock_reg, _mock_init, mock_recipe_reg, capsys):
        mock_reg.has_function.return_value = False
        mock_reg.list_functions.return_value = ["rename_columns"]
        mock_recipe_reg.return_value.get.return_value = None
        mock_recipe_reg.return_value.list_recipes.return_value = {}

        args = Namespace(name="nonexistent")
        result = explain_command(args)

        assert result == 1
        output = capsys.readouterr().out
        assert "Unknown: 'nonexistent'" in output
        assert "Try one of:" in output

"""Unit tests for odibi/utils/console.py."""

from unittest.mock import MagicMock, patch


from odibi.utils import console


class TestIsRichAvailable:
    """Tests for is_rich_available function."""

    def test_is_rich_available_with_rich_installed(self):
        """Test when Rich is installed."""
        with patch("importlib.util.find_spec", return_value=MagicMock()):
            console._RICH_AVAILABLE = None  # Reset cache
            result = console.is_rich_available()
            assert result is True

    def test_is_rich_available_without_rich_installed(self):
        """Test when Rich is not installed."""
        with patch("importlib.util.find_spec", return_value=None):
            console._RICH_AVAILABLE = None  # Reset cache
            result = console.is_rich_available()
            assert result is False

    def test_is_rich_available_import_error(self):
        """Test when import fails."""
        with patch("importlib.util.find_spec", side_effect=ImportError()):
            console._RICH_AVAILABLE = None  # Reset cache
            result = console.is_rich_available()
            assert result is False

    def test_is_rich_available_caching(self):
        """Test that result is cached."""
        console._RICH_AVAILABLE = True
        with patch("importlib.util.find_spec", return_value=None):
            # Should return cached value, not call find_spec
            result = console.is_rich_available()
            assert result is True


class TestGetConsole:
    """Tests for get_console function."""

    def test_get_console_when_rich_available(self):
        """Test getting console when Rich is available."""
        with patch("odibi.utils.console.is_rich_available", return_value=True):
            mock_console = MagicMock()
            with patch("rich.console.Console", return_value=mock_console):
                result = console.get_console()
                assert result is mock_console

    def test_get_console_when_rich_not_available(self):
        """Test getting console when Rich is not available."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            result = console.get_console()
            assert result is None


class TestIsNotebookEnvironment:
    """Tests for _is_notebook_environment function."""

    def test_is_notebook_environment_zmq_shell(self):
        """Test detection of ZMQInteractiveShell (Jupyter)."""
        mock_shell = MagicMock()
        mock_shell.__class__.__name__ = "ZMQInteractiveShell"
        mock_get_ipython = MagicMock(return_value=mock_shell)

        with patch.dict("sys.modules", {"IPython": MagicMock(get_ipython=mock_get_ipython)}):
            result = console._is_notebook_environment()
            assert result is True

    def test_is_notebook_environment_databricks_shell(self):
        """Test detection of DatabricksShell."""
        mock_shell = MagicMock()
        mock_shell.__class__.__name__ = "DatabricksShell"
        mock_get_ipython = MagicMock(return_value=mock_shell)

        with patch.dict("sys.modules", {"IPython": MagicMock(get_ipython=mock_get_ipython)}):
            result = console._is_notebook_environment()
            assert result is True

    def test_is_notebook_environment_generic_shell(self):
        """Test detection of generic Shell."""
        mock_shell = MagicMock()
        mock_shell.__class__.__name__ = "Shell"
        mock_get_ipython = MagicMock(return_value=mock_shell)

        with patch.dict("sys.modules", {"IPython": MagicMock(get_ipython=mock_get_ipython)}):
            result = console._is_notebook_environment()
            assert result is True

    def test_is_notebook_environment_none_shell(self):
        """Test when get_ipython returns None."""
        mock_get_ipython = MagicMock(return_value=None)

        with patch.dict("sys.modules", {"IPython": MagicMock(get_ipython=mock_get_ipython)}):
            result = console._is_notebook_environment()
            assert result is False

    def test_is_notebook_environment_no_ipython(self):
        """Test when IPython is not installed."""
        result = console._is_notebook_environment()
        # In this environment, IPython is not installed by default
        # so this should return False
        assert result is False

    def test_is_notebook_environment_terminal_shell(self):
        """Test non-notebook shell."""
        mock_shell = MagicMock()
        mock_shell.__class__.__name__ = "TerminalInteractiveShell"
        mock_get_ipython = MagicMock(return_value=mock_shell)

        with patch.dict("sys.modules", {"IPython": MagicMock(get_ipython=mock_get_ipython)}):
            result = console._is_notebook_environment()
            assert result is False


class TestSuccess:
    """Tests for success function."""

    def test_success_with_rich(self, capsys):
        """Test success message with Rich available."""
        with patch("odibi.utils.console.is_rich_available", return_value=True):
            mock_console = MagicMock()
            with patch("odibi.utils.console.get_console", return_value=mock_console):
                console.success("Test message")
                mock_console.print.assert_called_once_with("[green]✓[/green] Test message")

    def test_success_without_rich(self, capsys):
        """Test success message without Rich."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.success("Test message")
            captured = capsys.readouterr()
            assert captured.out == "✓ Test message\n"

    def test_success_custom_prefix(self, capsys):
        """Test success message with custom prefix."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.success("Test message", prefix="[OK]")
            captured = capsys.readouterr()
            assert captured.out == "[OK] Test message\n"

    def test_success_empty_message(self, capsys):
        """Test success with empty message."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.success("")
            captured = capsys.readouterr()
            assert captured.out == "✓ \n"

    def test_success_special_characters(self, capsys):
        """Test success with special characters."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.success("Test™ & <message>")
            captured = capsys.readouterr()
            assert captured.out == "✓ Test™ & <message>\n"


class TestError:
    """Tests for error function."""

    def test_error_with_rich(self, capsys):
        """Test error message with Rich available."""
        with patch("odibi.utils.console.is_rich_available", return_value=True):
            mock_console = MagicMock()
            with patch("odibi.utils.console.get_console", return_value=mock_console):
                console.error("Test error")
                mock_console.print.assert_called_once_with("[red]✗[/red] Test error")

    def test_error_without_rich(self, capsys):
        """Test error message without Rich."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.error("Test error")
            captured = capsys.readouterr()
            assert captured.out == "✗ Test error\n"

    def test_error_custom_prefix(self, capsys):
        """Test error message with custom prefix."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.error("Test error", prefix="[ERR]")
            captured = capsys.readouterr()
            assert captured.out == "[ERR] Test error\n"


class TestWarning:
    """Tests for warning function."""

    def test_warning_with_rich(self, capsys):
        """Test warning message with Rich available."""
        with patch("odibi.utils.console.is_rich_available", return_value=True):
            mock_console = MagicMock()
            with patch("odibi.utils.console.get_console", return_value=mock_console):
                console.warning("Test warning")
                mock_console.print.assert_called_once_with("[yellow]⚠[/yellow] Test warning")

    def test_warning_without_rich(self, capsys):
        """Test warning message without Rich."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.warning("Test warning")
            captured = capsys.readouterr()
            assert captured.out == "⚠ Test warning\n"

    def test_warning_custom_prefix(self, capsys):
        """Test warning message with custom prefix."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.warning("Test warning", prefix="[WARN]")
            captured = capsys.readouterr()
            assert captured.out == "[WARN] Test warning\n"


class TestInfo:
    """Tests for info function."""

    def test_info_with_rich(self, capsys):
        """Test info message with Rich available."""
        with patch("odibi.utils.console.is_rich_available", return_value=True):
            mock_console = MagicMock()
            with patch("odibi.utils.console.get_console", return_value=mock_console):
                console.info("Test info")
                mock_console.print.assert_called_once_with("[blue]ℹ[/blue] Test info")

    def test_info_without_rich(self, capsys):
        """Test info message without Rich."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.info("Test info")
            captured = capsys.readouterr()
            assert captured.out == "ℹ Test info\n"

    def test_info_custom_prefix(self, capsys):
        """Test info message with custom prefix."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.info("Test info", prefix="[INFO]")
            captured = capsys.readouterr()
            assert captured.out == "[INFO] Test info\n"


class TestPrintTable:
    """Tests for print_table function."""

    def test_print_table_empty_data(self, capsys):
        """Test print_table with empty data."""
        console.print_table([])
        captured = capsys.readouterr()
        assert captured.out == "(empty)\n"

    def test_print_table_none_data(self, capsys):
        """Test print_table with None data."""
        console.print_table(None)
        captured = capsys.readouterr()
        assert captured.out == "(empty)\n"

    def test_print_table_list_of_dicts_without_rich(self, capsys):
        """Test print_table with list of dicts without Rich."""
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.print_table(data)
            captured = capsys.readouterr()
            assert "Alice" in captured.out
            assert "Bob" in captured.out
            assert "name" in captured.out
            assert "age" in captured.out

    def test_print_table_list_of_dicts_with_rich(self):
        """Test print_table with list of dicts with Rich."""
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]
        with patch("odibi.utils.console.is_rich_available", return_value=True):
            mock_console = MagicMock()
            with patch("odibi.utils.console.get_console", return_value=mock_console):
                with patch("rich.table.Table") as mock_table_class:
                    mock_table = MagicMock()
                    mock_table_class.return_value = mock_table
                    console.print_table(data)
                    mock_table.add_column.assert_any_call("name")
                    mock_table.add_column.assert_any_call("age")
                    assert mock_table.add_row.call_count == 2

    def test_print_table_with_title_without_rich(self, capsys):
        """Test print_table with title without Rich."""
        data = [{"name": "Alice"}]
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.print_table(data, title="Users")
            captured = capsys.readouterr()
            assert "Users" in captured.out

    def test_print_table_with_custom_columns(self, capsys):
        """Test print_table with custom columns."""
        data = [
            {"name": "Alice", "age": 30, "city": "NYC"},
            {"name": "Bob", "age": 25, "city": "LA"},
        ]
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.print_table(data, columns=["name", "city"])
            captured = capsys.readouterr()
            assert "Alice" in captured.out
            assert "NYC" in captured.out
            # age should not be included
            assert "age" not in captured.out or "30" not in captured.out

    def test_print_table_with_dataframe_mock(self):
        """Test print_table with DataFrame-like object."""
        mock_df = MagicMock()
        mock_df.to_dict.return_value = [{"name": "Alice", "age": 30}]
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.print_table(mock_df)
            mock_df.to_dict.assert_called_once_with("records")

    def test_print_table_with_invalid_data(self, capsys):
        """Test print_table with invalid data type."""
        console.print_table(12345)
        captured = capsys.readouterr()
        assert "12345" in captured.out

    def test_print_table_empty_rows_after_conversion(self, capsys):
        """Test print_table with empty rows after conversion."""
        mock_df = MagicMock()
        mock_df.to_dict.return_value = []
        console.print_table(mock_df)
        captured = capsys.readouterr()
        assert "(empty)" in captured.out

    def test_print_table_special_characters(self, capsys):
        """Test print_table with special characters."""
        data = [{"name": "Test™", "value": "<>&"}]
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.print_table(data)
            captured = capsys.readouterr()
            assert "Test™" in captured.out
            assert "<>&" in captured.out


class TestPrintPanel:
    """Tests for print_panel function."""

    def test_print_panel_without_rich(self, capsys):
        """Test print_panel without Rich."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.print_panel("Test content")
            captured = capsys.readouterr()
            assert "Test content" in captured.out
            assert "┌" in captured.out
            assert "└" in captured.out

    def test_print_panel_with_rich(self):
        """Test print_panel with Rich."""
        with patch("odibi.utils.console.is_rich_available", return_value=True):
            mock_console = MagicMock()
            with patch("odibi.utils.console.get_console", return_value=mock_console):
                with patch("rich.panel.Panel") as mock_panel_class:
                    mock_panel = MagicMock()
                    mock_panel_class.return_value = mock_panel
                    console.print_panel("Test content", title="Title")
                    mock_panel_class.assert_called_once()
                    mock_console.print.assert_called_once_with(mock_panel)

    def test_print_panel_with_title_without_rich(self, capsys):
        """Test print_panel with title without Rich."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.print_panel("Test content", title="Title")
            captured = capsys.readouterr()
            assert "Title" in captured.out
            assert "Test content" in captured.out

    def test_print_panel_multiline_content(self, capsys):
        """Test print_panel with multiline content."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.print_panel("Line 1\nLine 2\nLine 3")
            captured = capsys.readouterr()
            assert "Line 1" in captured.out
            assert "Line 2" in captured.out
            assert "Line 3" in captured.out

    def test_print_panel_empty_content(self, capsys):
        """Test print_panel with empty content."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.print_panel("")
            captured = capsys.readouterr()
            assert "┌" in captured.out
            assert "└" in captured.out


class TestPrintRule:
    """Tests for print_rule function."""

    def test_print_rule_without_rich(self, capsys):
        """Test print_rule without Rich."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.print_rule()
            captured = capsys.readouterr()
            assert "─" in captured.out

    def test_print_rule_with_rich(self):
        """Test print_rule with Rich."""
        with patch("odibi.utils.console.is_rich_available", return_value=True):
            mock_console = MagicMock()
            with patch("odibi.utils.console.get_console", return_value=mock_console):
                with patch("rich.rule.Rule") as mock_rule_class:
                    mock_rule = MagicMock()
                    mock_rule_class.return_value = mock_rule
                    console.print_rule()
                    mock_rule_class.assert_called_once()
                    mock_console.print.assert_called_once_with(mock_rule)

    def test_print_rule_with_title_without_rich(self, capsys):
        """Test print_rule with title without Rich."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.print_rule(title="Section")
            captured = capsys.readouterr()
            assert "Section" in captured.out
            assert "─" in captured.out

    def test_print_rule_with_title_and_style(self):
        """Test print_rule with title and style using Rich."""
        with patch("odibi.utils.console.is_rich_available", return_value=True):
            mock_console = MagicMock()
            with patch("odibi.utils.console.get_console", return_value=mock_console):
                with patch("rich.rule.Rule") as mock_rule_class:
                    console.print_rule(title="Section", style="red")
                    mock_rule_class.assert_called_once_with("Section", style="red")

    def test_print_rule_empty_title(self, capsys):
        """Test print_rule with empty title."""
        with patch("odibi.utils.console.is_rich_available", return_value=False):
            console.print_rule(title=None)
            captured = capsys.readouterr()
            assert "─" in captured.out

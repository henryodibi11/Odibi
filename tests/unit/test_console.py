import pytest
import odibi.utils.console as console_mod


@pytest.fixture(autouse=True)
def force_plain_fallback(monkeypatch):
    monkeypatch.setattr(console_mod, "_RICH_AVAILABLE", False)
    yield
    console_mod._RICH_AVAILABLE = None


class TestIsNotebookEnvironment:
    def test_returns_false_in_test_context(self):
        assert console_mod._is_notebook_environment() is False


class TestSuccess:
    def test_default_prefix(self, capsys):
        console_mod.success("done")
        assert capsys.readouterr().out == "✓ done\n"

    def test_custom_prefix(self, capsys):
        console_mod.success("ok", prefix="OK")
        assert capsys.readouterr().out == "OK ok\n"


class TestError:
    def test_default_prefix(self, capsys):
        console_mod.error("fail")
        assert capsys.readouterr().out == "✗ fail\n"

    def test_custom_prefix(self, capsys):
        console_mod.error("bad", prefix="ERR")
        assert capsys.readouterr().out == "ERR bad\n"


class TestWarning:
    def test_default_prefix(self, capsys):
        console_mod.warning("careful")
        assert capsys.readouterr().out == "⚠ careful\n"

    def test_custom_prefix(self, capsys):
        console_mod.warning("hmm", prefix="WARN")
        assert capsys.readouterr().out == "WARN hmm\n"


class TestInfo:
    def test_default_prefix(self, capsys):
        console_mod.info("note")
        assert capsys.readouterr().out == "ℹ note\n"

    def test_custom_prefix(self, capsys):
        console_mod.info("hey", prefix="INFO")
        assert capsys.readouterr().out == "INFO hey\n"


class TestPrintTable:
    def test_list_of_dicts(self, capsys):
        data = [{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}]
        console_mod.print_table(data)
        out = capsys.readouterr().out
        assert "Alice" in out
        assert "Bob" in out
        assert "name" in out
        assert "age" in out

    def test_list_of_dicts_with_title(self, capsys):
        data = [{"x": "1"}]
        console_mod.print_table(data, title="My Table")
        out = capsys.readouterr().out
        assert "My Table" in out
        lines = out.strip().split("\n")
        assert lines[1].startswith("-")

    def test_empty_list(self, capsys):
        console_mod.print_table([])
        assert capsys.readouterr().out == "(empty)\n"

    def test_none_data(self, capsys):
        console_mod.print_table(None)
        assert capsys.readouterr().out == "(empty)\n"

    def test_dataframe_like(self, capsys):
        class FakeDF:
            def to_dict(self, orient):
                return [{"col1": "a", "col2": "b"}]

        console_mod.print_table(FakeDF())
        out = capsys.readouterr().out
        assert "col1" in out
        assert "a" in out

    def test_dataframe_like_empty_records(self, capsys):
        class FakeDF:
            def to_dict(self, orient):
                return []

        console_mod.print_table(FakeDF())
        assert capsys.readouterr().out == "(empty)\n"

    def test_non_list_non_dataframe(self, capsys):
        console_mod.print_table("just a string")
        assert capsys.readouterr().out == "just a string\n"

    def test_columns_filter(self, capsys):
        data = [{"a": "1", "b": "2", "c": "3"}]
        console_mod.print_table(data, columns=["a", "c"])
        out = capsys.readouterr().out
        assert "a" in out
        assert "c" in out
        assert "b" not in out


class TestPrintPanel:
    def test_without_title(self, capsys):
        console_mod.print_panel("hello world")
        out = capsys.readouterr().out
        assert "┌" in out
        assert "└" in out
        assert "hello world" in out

    def test_with_title(self, capsys):
        console_mod.print_panel("content", title="Title")
        out = capsys.readouterr().out
        assert "Title" in out
        assert "content" in out
        assert "├" in out

    def test_multiline_content(self, capsys):
        console_mod.print_panel("line1\nline2")
        out = capsys.readouterr().out
        assert "line1" in out
        assert "line2" in out


class TestPrintRule:
    def test_without_title(self, capsys):
        console_mod.print_rule()
        out = capsys.readouterr().out
        assert "─" * 40 in out

    def test_with_title(self, capsys):
        console_mod.print_rule(title="Section")
        out = capsys.readouterr().out
        assert "Section" in out
        assert "─" in out


class TestIsRichAvailable:
    def test_returns_cached_false(self):
        assert console_mod.is_rich_available() is False

    def test_redetects_when_reset(self, monkeypatch):
        console_mod._RICH_AVAILABLE = None
        result = console_mod.is_rich_available()
        assert isinstance(result, bool)


class TestGetConsole:
    def test_returns_none_when_rich_unavailable(self):
        assert console_mod.get_console() is None


class TestRichPath:
    @pytest.fixture(autouse=True)
    def enable_rich(self, monkeypatch):
        monkeypatch.setattr(console_mod, "_RICH_AVAILABLE", True)

    def test_get_console_returns_console(self):
        c = console_mod.get_console()
        assert c is not None
        assert type(c).__name__ == "Console"

    def test_success_uses_rich(self, capsys):
        console_mod.success("yay")

    def test_error_uses_rich(self, capsys):
        console_mod.error("bad")

    def test_warning_uses_rich(self, capsys):
        console_mod.warning("hmm")

    def test_info_uses_rich(self, capsys):
        console_mod.info("note")

    def test_print_table_uses_rich(self, capsys):
        console_mod.print_table([{"a": "1"}])

    def test_print_panel_uses_rich(self, capsys):
        console_mod.print_panel("content", title="T")

    def test_print_rule_uses_rich(self, capsys):
        console_mod.print_rule(title="Section")

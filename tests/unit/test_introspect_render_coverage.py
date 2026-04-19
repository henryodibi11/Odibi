"""Tests for odibi.introspect — rendering and generate_docs coverage gaps."""

from pathlib import Path
from unittest.mock import patch


from odibi.introspect import (
    FieldDoc,
    ModelDoc,
    TYPE_ALIASES,
    generate_docs,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _model(
    name,
    fields=None,
    group="Core",
    docstring=None,
    category=None,
    function_name=None,
    function_doc=None,
    used_in=None,
):
    return ModelDoc(
        name=name,
        module="test.module",
        summary=docstring.split("\n")[0] if docstring else None,
        docstring=docstring,
        fields=fields or [],
        group=group,
        category=category,
        function_name=function_name,
        function_doc=function_doc,
        used_in=used_in or [],
    )


def _field(name="field1", type_hint="str", required=True, default=None, description=None):
    return FieldDoc(
        name=name,
        type_hint=type_hint,
        required=required,
        default=default,
        description=description,
    )


# ---------------------------------------------------------------------------
# generate_docs with synthetic models — isolated rendering
# ---------------------------------------------------------------------------


class TestRenderEmptyModels:
    """Empty models list → minimal output with header only."""

    @patch("odibi.introspect.discover_modules", return_value=[])
    @patch("odibi.introspect.scan_module_for_models", return_value=[])
    def test_empty_models_produces_header(self, mock_scan, mock_disc, tmp_path):
        output = str(tmp_path / "empty.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        assert "# Odibi Configuration Reference" in content
        assert content.endswith("\n")
        # No section headers when no models
        assert "## " not in content


class TestRenderCoreGroup:
    """Core group models → standard headers and field tables."""

    @patch("odibi.introspect.discover_modules", return_value=["fake"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_core_model_renders_heading_and_table(self, mock_scan, mock_disc, tmp_path):
        model = _model(
            "TestCoreModel",
            fields=[_field("name", "str", True, description="The name")],
            group="Core",
            docstring="A core model for testing.",
        )
        mock_scan.return_value = [model]
        output = str(tmp_path / "core.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        assert "## Project Structure" in content
        assert "### `TestCoreModel`" in content
        assert "| **name** |" in content
        assert "| Yes |" in content
        assert "A core model for testing." in content

    @patch("odibi.introspect.discover_modules", return_value=["fake"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_core_model_no_fields_no_table(self, mock_scan, mock_disc, tmp_path):
        model = _model("EmptyCore", group="Core", docstring="No fields.")
        mock_scan.return_value = [model]
        output = str(tmp_path / "nf.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        assert "### `EmptyCore`" in content
        assert "| Field |" not in content.split("### `EmptyCore`")[1].split("---")[0]


class TestRenderTransformationGroup:
    """Transformation group → category sorting + function doc rendering."""

    @patch("odibi.introspect.discover_modules", return_value=["fake"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_transformation_category_headers(self, mock_scan, mock_disc, tmp_path):
        models = [
            _model(
                "FilterParams",
                group="Transformation",
                category="Common Operations",
                function_name="filter",
                function_doc="Filters rows.",
            ),
            _model(
                "JoinParams",
                group="Transformation",
                category="Relational Algebra",
                function_name="join",
                function_doc="Joins tables.",
            ),
        ]
        mock_scan.return_value = models
        output = str(tmp_path / "transform.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        assert "## Transformation Reference" in content
        assert "### 📂 Common Operations" in content
        assert "### 📂 Relational Algebra" in content
        # Common Operations should appear before Relational Algebra
        assert content.index("Common Operations") < content.index("Relational Algebra")

    @patch("odibi.introspect.discover_modules", return_value=["fake"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_transformation_function_header(self, mock_scan, mock_disc, tmp_path):
        model = _model(
            "FilterParams",
            group="Transformation",
            category="Common Operations",
            function_name="filter",
            function_doc="Filter docs.",
            docstring="Config for filter.",
            fields=[_field("column", "str", True)],
        )
        mock_scan.return_value = [model]
        output = str(tmp_path / "func.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        # Function name in header
        assert "`filter` (FilterParams)" in content
        assert "Filter docs." in content
        assert "Config for filter." in content
        assert "[Back to Catalog](#nodeconfig)" in content

    @patch("odibi.introspect.discover_modules", return_value=["fake"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_transformation_duplicate_docstring_skipped(self, mock_scan, mock_disc, tmp_path):
        model = _model(
            "DupParams",
            group="Transformation",
            category="Common Operations",
            function_name="dup_func",
            function_doc="Same doc.",
            docstring="Same doc.",
        )
        mock_scan.return_value = [model]
        output = str(tmp_path / "dup.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        # The docstring "Same doc." should appear only once (from function_doc)
        section = content.split("#### ")[1]
        assert section.count("Same doc.") == 1


class TestRenderConnectionGroup:
    """Connection group models render correctly."""

    @patch("odibi.introspect.discover_modules", return_value=["fake"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_connection_section(self, mock_scan, mock_disc, tmp_path):
        model = _model(
            "LocalConnectionConfig",
            group="Connection",
            docstring="Local connection.",
            fields=[_field("path", "str", True, description="Path to data")],
        )
        mock_scan.return_value = [model]
        output = str(tmp_path / "conn.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        assert "## Connections" in content
        assert "### `LocalConnectionConfig`" in content


class TestRenderCrossLinking:
    """Model name in type_hint → linked with markdown anchor."""

    @patch("odibi.introspect.discover_modules", return_value=["fake"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_cross_link_in_field_type(self, mock_scan, mock_disc, tmp_path):
        child = _model("WriteConfig", group="Operation", fields=[_field("mode", "str", True)])
        parent = _model("NodeConfig", group="Core", fields=[_field("write", "WriteConfig", False)])
        mock_scan.return_value = [parent, child]
        output = str(tmp_path / "xlink.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        assert "[WriteConfig](#writeconfig)" in content

    @patch("odibi.introspect.discover_modules", return_value=["fake"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_cross_link_in_list_type(self, mock_scan, mock_disc, tmp_path):
        child = _model("FieldDef", group="Core", fields=[])
        parent = _model(
            "ParentModel", group="Core", fields=[_field("fields", "List[FieldDef]", True)]
        )
        mock_scan.return_value = [parent, child]
        output = str(tmp_path / "xlist.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        assert "[FieldDef](#fielddef)" in content


class TestRenderTypeAliasExpansion:
    """Type alias → expands to Options: links in description."""

    @patch("odibi.introspect.discover_modules", return_value=["fake"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_alias_expands_in_description(self, mock_scan, mock_disc, tmp_path):
        # Use a real alias (ConnectionConfig) with a component model present
        component_name = TYPE_ALIASES["ConnectionConfig"][0]  # LocalConnectionConfig
        component = _model(component_name, group="Connection")
        parent = _model(
            "ProjectConfig",
            group="Core",
            fields=[
                _field("connection", "ConnectionConfig", True, description="Connection to use")
            ],
        )
        mock_scan.return_value = [parent, component]
        output = str(tmp_path / "alias.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        assert "**Options:**" in content
        assert f"[{component_name}](#{component_name.lower()})" in content


class TestRenderUsedInReverseIndex:
    """used_in list renders as '> *Used in:' quote."""

    @patch("odibi.introspect.discover_modules", return_value=["fake"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_used_in_rendered(self, mock_scan, mock_disc, tmp_path):
        child = _model("ReadConfig", group="Operation", fields=[_field("source", "str", True)])
        parent = _model("NodeConfig", group="Core", fields=[_field("read", "ReadConfig", False)])
        mock_scan.return_value = [parent, child]
        output = str(tmp_path / "usedin.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        assert "> *Used in:" in content
        assert "[NodeConfig](#nodeconfig)" in content


class TestRenderDocstringCodeFence:
    """Docstring ending with code fence → extra blank line."""

    @patch("odibi.introspect.discover_modules", return_value=["fake"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_code_fence_ending_gets_extra_blank(self, mock_scan, mock_disc, tmp_path):
        docstring = "Example:\n```yaml\nkey: value\n```"
        model = _model("FencedModel", group="Core", docstring=docstring)
        mock_scan.return_value = [model]
        output = str(tmp_path / "fence.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        # After the code fence, there should be two blank lines (the extra one)
        idx = content.index("```\n", content.index("key: value"))
        after_fence = content[idx + 3 :]  # skip the "```"
        # Should have at least two consecutive newlines
        assert after_fence.startswith("\n\n")

    @patch("odibi.introspect.discover_modules", return_value=["fake"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_code_fence_in_transformation_group(self, mock_scan, mock_disc, tmp_path):
        docstring = "Usage:\n```python\nfilter(df)\n```"
        model = _model(
            "CodeModel",
            group="Transformation",
            category="Common Operations",
            docstring=docstring,
            function_name="code_func",
        )
        mock_scan.return_value = [model]
        output = str(tmp_path / "tfence.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        idx = content.index("```\n", content.index("filter(df)"))
        after_fence = content[idx + 3 :]
        assert after_fence.startswith("\n\n")


class TestGenerateDocsPipeline:
    """generate_docs — discover + scan + render full pipeline."""

    @patch("odibi.introspect.discover_modules")
    @patch("odibi.introspect.scan_module_for_models")
    def test_pipeline_calls_discover_and_scan(self, mock_scan, mock_disc, tmp_path):
        mock_disc.return_value = ["mod_a", "mod_b"]
        mock_scan.return_value = []
        output = str(tmp_path / "pipe.md")
        generate_docs(output_path=output)
        mock_disc.assert_called_once()
        assert mock_scan.call_count == 2

    @patch("odibi.introspect.discover_modules", return_value=["mod"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_creates_parent_directories(self, mock_scan, mock_disc, tmp_path):
        mock_scan.return_value = []
        output = str(tmp_path / "deep" / "nested" / "out.md")
        generate_docs(output_path=output)
        assert Path(output).exists()

    @patch("odibi.introspect.discover_modules", return_value=["mod"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_file_ends_with_newline(self, mock_scan, mock_disc, tmp_path):
        mock_scan.return_value = [_model("X", group="Core")]
        output = str(tmp_path / "nl.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        assert content.endswith("\n")

    @patch("odibi.introspect.discover_modules", return_value=["mod"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_trailing_whitespace_stripped(self, mock_scan, mock_disc, tmp_path):
        model = _model("TrimModel", group="Core", docstring="Has trailing   ")
        mock_scan.return_value = [model]
        output = str(tmp_path / "trim.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        for line in content.split("\n"):
            assert line == line.rstrip()


class TestRenderFieldDefaults:
    """Field default values render correctly."""

    @patch("odibi.introspect.discover_modules", return_value=["mod"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_field_with_default_shows_backticks(self, mock_scan, mock_disc, tmp_path):
        model = _model(
            "DefModel", group="Core", fields=[_field("mode", "str", False, default="append")]
        )
        mock_scan.return_value = [model]
        output = str(tmp_path / "def.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        assert "`append`" in content

    @patch("odibi.introspect.discover_modules", return_value=["mod"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_field_no_default_shows_dash(self, mock_scan, mock_disc, tmp_path):
        model = _model("NoDef", group="Core", fields=[_field("name", "str", True)])
        mock_scan.return_value = [model]
        output = str(tmp_path / "nodef.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        # The default column should show "-"
        assert "| - |" in content


class TestRenderPipeEscape:
    """Pipe characters in descriptions and type hints are escaped."""

    @patch("odibi.introspect.discover_modules", return_value=["mod"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_pipe_in_description_escaped(self, mock_scan, mock_disc, tmp_path):
        model = _model(
            "PipeModel",
            group="Core",
            fields=[_field("col", "str", True, description="A | B choice")],
        )
        mock_scan.return_value = [model]
        output = str(tmp_path / "pipe.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        assert "A \\| B choice" in content

    @patch("odibi.introspect.discover_modules", return_value=["mod"])
    @patch("odibi.introspect.scan_module_for_models")
    def test_pipe_in_type_hint_escaped(self, mock_scan, mock_disc, tmp_path):
        model = _model("UnionModel", group="Core", fields=[_field("val", "str | int", True)])
        mock_scan.return_value = [model]
        output = str(tmp_path / "union.md")
        generate_docs(output_path=output)
        content = Path(output).read_text(encoding="utf-8")
        assert "str \\| int" in content

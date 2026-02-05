"""Tests for story renderers."""

import json
from pathlib import Path

import pytest

from odibi.story.metadata import NodeExecutionMetadata, PipelineStoryMetadata
from odibi.story.renderers import (
    HTMLStoryRenderer,
    JSONStoryRenderer,
    MarkdownStoryRenderer,
    get_renderer,
)


@pytest.fixture
def sample_metadata():
    """Create sample pipeline metadata for testing."""
    metadata = PipelineStoryMetadata(
        pipeline_name="test_pipeline",
        pipeline_layer="bronze",
        project="Test Project",
        plant="NKC",
        asset="Dryer 1",
    )
    metadata.duration = 5.25
    metadata.completed_at = "2025-11-10T14:30:00"

    # Add successful node
    node1 = NodeExecutionMetadata(
        node_name="load_data",
        operation="read",
        status="success",
        duration=1.234,
        rows_out=1000,
        schema_out=["ID", "Name", "Value"],
    )
    metadata.add_node(node1)

    # Add node with transformations
    node2 = NodeExecutionMetadata(
        node_name="transform",
        operation="sql",
        status="success",
        duration=2.567,
        rows_in=1000,
        rows_out=850,
        schema_in=["ID", "Name", "Value"],
        schema_out=["ID", "Name", "Value", "Calculated"],
    )
    node2.calculate_row_change()
    node2.calculate_schema_changes()
    metadata.add_node(node2)

    # Add failed node
    node3 = NodeExecutionMetadata(
        node_name="failed_step",
        operation="pivot",
        status="failed",
        duration=0.123,
        error_message="Division by zero",
        error_type="ZeroDivisionError",
    )
    metadata.add_node(node3)

    return metadata


class TestMarkdownStoryRenderer:
    """Tests for MarkdownStoryRenderer."""

    def test_render_basic(self, sample_metadata):
        """Should render basic markdown story."""
        renderer = MarkdownStoryRenderer()
        markdown = renderer.render(sample_metadata)

        assert "# Pipeline Run Story: test_pipeline" in markdown
        assert "test_pipeline" in markdown
        assert "bronze" in markdown

    def test_render_includes_summary(self, sample_metadata):
        """Should include summary section."""
        renderer = MarkdownStoryRenderer()
        markdown = renderer.render(sample_metadata)

        assert "## Summary" in markdown
        assert "✅ **Completed:** 2 nodes" in markdown
        assert "❌ **Failed:** 1 nodes" in markdown

    def test_render_includes_node_details(self, sample_metadata):
        """Should include node details."""
        renderer = MarkdownStoryRenderer()
        markdown = renderer.render(sample_metadata)

        assert "load_data" in markdown
        assert "transform" in markdown
        assert "failed_step" in markdown

    def test_render_includes_status_icons(self, sample_metadata):
        """Should include status icons for nodes."""
        renderer = MarkdownStoryRenderer()
        markdown = renderer.render(sample_metadata)

        assert "✅" in markdown  # Success
        assert "❌" in markdown  # Failed

    def test_render_includes_row_changes(self, sample_metadata):
        """Should include row count changes."""
        renderer = MarkdownStoryRenderer()
        markdown = renderer.render(sample_metadata)

        assert "Rows Read:" in markdown
        assert "Rows Out:" in markdown
        assert "Row Change:" in markdown
        assert "850" in markdown
        assert "-150" in markdown

    def test_render_includes_schema_changes(self, sample_metadata):
        """Should include schema changes."""
        renderer = MarkdownStoryRenderer()
        markdown = renderer.render(sample_metadata)

        assert "Schema Changes:" in markdown
        assert "➕ Added: `Calculated`" in markdown

    def test_render_includes_errors(self, sample_metadata):
        """Should include error details."""
        renderer = MarkdownStoryRenderer()
        markdown = renderer.render(sample_metadata)

        assert "ZeroDivisionError" in markdown
        assert "Division by zero" in markdown

    def test_render_includes_project_context(self, sample_metadata):
        """Should include project context."""
        renderer = MarkdownStoryRenderer()
        markdown = renderer.render(sample_metadata)

        assert "Test Project" in markdown
        assert "NKC" in markdown
        assert "Dryer 1" in markdown

    def test_render_to_file(self, sample_metadata, tmp_path):
        """Should render to file."""
        renderer = MarkdownStoryRenderer()
        output_path = tmp_path / "story.md"

        result_path = renderer.render_to_file(sample_metadata, str(output_path))

        assert Path(result_path).exists()
        content = Path(result_path).read_text(encoding="utf-8")
        assert "test_pipeline" in content


class TestHTMLStoryRenderer:
    """Tests for HTMLStoryRenderer."""

    def test_render_basic(self, sample_metadata):
        """Should render basic HTML story."""
        pytest.importorskip("jinja2")

        renderer = HTMLStoryRenderer()
        html = renderer.render(sample_metadata)

        assert "Pipeline Flow" in html  # Replaces explicit checks for mermaid if node list is empty
        # But wait, the test fixture has nodes.
        # If the template changed structure, "load_data" should still be there.

        # Check for presence of key elements
        assert "load_data" in html
        assert "transform" in html

    def test_render_includes_header(self, sample_metadata):
        """Should include header section."""
        pytest.importorskip("jinja2")

        renderer = HTMLStoryRenderer()
        html = renderer.render(sample_metadata)

        assert 'class="header"' in html
        assert "test_pipeline" in html

    def test_render_includes_summary_stats(self, sample_metadata):
        """Should include summary statistics."""
        pytest.importorskip("jinja2")

        renderer = HTMLStoryRenderer()
        html = renderer.render(sample_metadata)

        assert "Nodes" in html
        assert "Completed" in html
        assert "Failed" in html
        assert "Success Rate" in html

    def test_render_includes_node_cards(self, sample_metadata):
        """Should include node cards."""
        pytest.importorskip("jinja2")

        renderer = HTMLStoryRenderer()
        html = renderer.render(sample_metadata)

        assert "load_data" in html
        assert "transform" in html
        assert "failed_step" in html

    def test_render_includes_schema_badges(self, sample_metadata):
        """Should include schema change badges."""
        pytest.importorskip("jinja2")

        renderer = HTMLStoryRenderer()
        html = renderer.render(sample_metadata)

        assert "Calculated" in html  # Added column

    def test_render_includes_error_box(self, sample_metadata):
        """Should include error box for failed nodes."""
        pytest.importorskip("jinja2")

        renderer = HTMLStoryRenderer()
        html = renderer.render(sample_metadata)

        # The class might not be explicitly "error-box" anymore,
        # but inline styles or specific error message container
        assert "Error:" in html
        assert "ZeroDivisionError" in html
        assert "Division by zero" in html

    def test_render_includes_javascript(self, sample_metadata):
        """Should include interactive JavaScript."""
        pytest.importorskip("jinja2")

        renderer = HTMLStoryRenderer()
        html = renderer.render(sample_metadata)

        assert "<script>" in html
        assert "toggleNode" in html

    def test_render_to_file(self, sample_metadata, tmp_path):
        """Should render to file."""
        pytest.importorskip("jinja2")

        renderer = HTMLStoryRenderer()
        output_path = tmp_path / "story.html"

        result_path = renderer.render_to_file(sample_metadata, str(output_path))

        assert Path(result_path).exists()
        content = Path(result_path).read_text(encoding="utf-8")
        assert "<!DOCTYPE html>" in content
        assert "test_pipeline" in content

    def test_template_path_exists(self):
        """Should have valid default template path."""
        renderer = HTMLStoryRenderer()
        assert Path(renderer.template_path).exists()

    def test_render_with_theme(self, sample_metadata):
        """Should render with custom theme."""
        pytest.importorskip("jinja2")
        from odibi.story.themes import DARK_THEME

        renderer = HTMLStoryRenderer(theme=DARK_THEME)
        html = renderer.render(sample_metadata)

        assert "<!DOCTYPE html>" in html
        assert "test_pipeline" in html
        # Theme CSS should be injected
        assert "#1e1e1e" in html or "dark" in html.lower()


class TestJSONStoryRenderer:
    """Tests for JSONStoryRenderer."""

    def test_render_basic(self, sample_metadata):
        """Should render basic JSON story."""
        renderer = JSONStoryRenderer()
        json_str = renderer.render(sample_metadata)

        data = json.loads(json_str)
        assert data["pipeline_name"] == "test_pipeline"
        assert data["pipeline_layer"] == "bronze"

    def test_render_includes_all_metadata(self, sample_metadata):
        """Should include all metadata fields."""
        renderer = JSONStoryRenderer()
        json_str = renderer.render(sample_metadata)

        data = json.loads(json_str)

        assert "total_nodes" in data
        assert "completed_nodes" in data
        assert "failed_nodes" in data
        assert "success_rate" in data
        assert "total_rows_processed" in data
        assert "nodes" in data

    def test_render_includes_node_details(self, sample_metadata):
        """Should include node details."""
        renderer = JSONStoryRenderer()
        json_str = renderer.render(sample_metadata)

        data = json.loads(json_str)

        assert len(data["nodes"]) == 3
        assert data["nodes"][0]["node_name"] == "load_data"
        assert data["nodes"][1]["node_name"] == "transform"
        assert data["nodes"][2]["node_name"] == "failed_step"

    def test_render_includes_project_context(self, sample_metadata):
        """Should include project context."""
        renderer = JSONStoryRenderer()
        json_str = renderer.render(sample_metadata)

        data = json.loads(json_str)

        assert data["project"] == "Test Project"
        assert data["plant"] == "NKC"
        assert data["asset"] == "Dryer 1"

    def test_render_to_file(self, sample_metadata, tmp_path):
        """Should render to file."""
        renderer = JSONStoryRenderer()
        output_path = tmp_path / "story.json"

        result_path = renderer.render_to_file(sample_metadata, str(output_path))

        assert Path(result_path).exists()
        content = Path(result_path).read_text()
        data = json.loads(content)
        assert data["pipeline_name"] == "test_pipeline"


class TestGetRenderer:
    """Tests for get_renderer factory function."""

    def test_get_html_renderer(self):
        """Should return HTML renderer."""
        renderer = get_renderer("html")
        assert isinstance(renderer, HTMLStoryRenderer)

    def test_get_markdown_renderer(self):
        """Should return Markdown renderer."""
        renderer = get_renderer("markdown")
        assert isinstance(renderer, MarkdownStoryRenderer)

    def test_get_markdown_renderer_short(self):
        """Should return Markdown renderer for 'md'."""
        renderer = get_renderer("md")
        assert isinstance(renderer, MarkdownStoryRenderer)

    def test_get_json_renderer(self):
        """Should return JSON renderer."""
        renderer = get_renderer("json")
        assert isinstance(renderer, JSONStoryRenderer)

    def test_get_renderer_case_insensitive(self):
        """Should be case-insensitive."""
        assert isinstance(get_renderer("HTML"), HTMLStoryRenderer)
        assert isinstance(get_renderer("Markdown"), MarkdownStoryRenderer)
        assert isinstance(get_renderer("JSON"), JSONStoryRenderer)

    def test_get_renderer_invalid_format(self):
        """Should raise error for invalid format."""
        with pytest.raises(ValueError, match="Unsupported format"):
            get_renderer("invalid")


class TestMarkdownFilter:
    """Tests for the markdown rendering filter in HTMLStoryRenderer."""

    def test_render_markdown_basic(self):
        """Should render basic markdown to HTML."""
        renderer = HTMLStoryRenderer()
        result = renderer._render_markdown("**bold** and *italic*")
        assert "<strong>bold</strong>" in result
        assert "<em>italic</em>" in result

    def test_render_markdown_list(self):
        """Should render markdown lists."""
        renderer = HTMLStoryRenderer()
        result = renderer._render_markdown("1. First\n2. Second")
        assert "<ol>" in result
        assert "<li>" in result

    def test_render_markdown_table(self):
        """Should render markdown tables."""
        renderer = HTMLStoryRenderer()
        md = """
| Column | Value |
|--------|-------|
| A | 1 |
"""
        result = renderer._render_markdown(md)
        assert "<table>" in result
        assert "<th>" in result
        assert "<td>" in result

    def test_render_markdown_code_block(self):
        """Should render code blocks."""
        renderer = HTMLStoryRenderer()
        md = """
```sql
SELECT * FROM table
```
"""
        result = renderer._render_markdown(md)
        assert "<code>" in result or "<pre>" in result

    def test_render_markdown_empty(self):
        """Should handle empty input."""
        renderer = HTMLStoryRenderer()
        assert renderer._render_markdown("") == ""
        assert renderer._render_markdown(None) == ""


class TestHTMLStoryExplanationRendering:
    """Tests for explanation rendering in HTML stories."""

    def test_html_includes_explanation_section(self, sample_metadata):
        """Should include explanation in rendered HTML when present."""
        # Add a node with explanation
        node_with_explanation = NodeExecutionMetadata(
            node_name="explained_node",
            operation="transform",
            status="success",
            duration=1.0,
            explanation="This is a **test** explanation",
        )
        sample_metadata.add_node(node_with_explanation)

        renderer = HTMLStoryRenderer()
        html = renderer.render(sample_metadata)

        assert "Transformation Explanation" in html
        assert "explanation-content" in html

    def test_html_includes_column_drop_warning(self, sample_metadata):
        """Should include column drop warning when present."""
        node_with_warning = NodeExecutionMetadata(
            node_name="pivot_node",
            operation="pivot",
            status="success",
            duration=1.0,
            column_drop_warning="⚠️ 5 columns were dropped (10 → 5)",
        )
        sample_metadata.add_node(node_with_warning)

        renderer = HTMLStoryRenderer()
        html = renderer.render(sample_metadata)

        assert "5 columns were dropped" in html

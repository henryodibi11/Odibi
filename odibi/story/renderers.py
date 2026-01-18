"""
Story Renderers
===============

Renders pipeline stories in different output formats (HTML, Markdown, JSON).
"""

import json
from pathlib import Path
from typing import Optional

import odibi
from odibi.story.metadata import PipelineStoryMetadata
from odibi.utils.logging_context import get_logging_context


class HTMLStoryRenderer:
    """
    Renders pipeline stories as HTML.

    Creates professional, interactive HTML reports with:
    - Responsive design
    - Collapsible sections
    - Status indicators
    - Summary statistics
    """

    def __init__(self, template_path: Optional[str] = None, theme=None):
        """
        Initialize HTML renderer.

        Args:
            template_path: Path to custom template (uses default if None)
            theme: StoryTheme instance or None for default
        """
        self.template_path = template_path or self._default_template_path()
        self.theme = theme

    def _default_template_path(self) -> Path:
        """Get path to default HTML template."""
        return Path(__file__).parent / "templates" / "run_story.html"

    def render(self, metadata: PipelineStoryMetadata) -> str:
        """
        Render story as HTML.

        Args:
            metadata: Pipeline story metadata

        Returns:
            HTML string
        """
        ctx = get_logging_context()
        ctx.debug(
            "Rendering HTML story",
            template=str(self.template_path),
            pipeline=metadata.pipeline_name,
        )

        try:
            from jinja2 import Template
        except ImportError as e:
            ctx.error("jinja2 not installed for HTML rendering")
            raise ImportError(
                "jinja2 is required for HTML rendering. Install with: pip install jinja2"
            ) from e

        try:
            # Load template
            with open(self.template_path, "r", encoding="utf-8") as f:
                template_content = f.read()

            # Apply theme if provided
            if self.theme:
                # Inject theme CSS variables into template
                theme_css = self.theme.to_css_string()
                template_content = template_content.replace(
                    ":root {", theme_css.split("}")[0] + "}"
                )

            template = Template(template_content)

            # Register custom filters
            # Note: Template creates its own environment, so we attach to that
            template.environment.filters["to_yaml"] = self._to_yaml
            template.environment.filters["format_run_id"] = self._format_run_id

            # Render with metadata, theme, and version
            html = template.render(
                metadata=metadata,
                theme=self.theme,
                odibi_version=odibi.__version__,
            )

            ctx.debug(
                "HTML story rendered",
                output_format="html",
                size=len(html),
                nodes=len(metadata.nodes),
            )

            return html
        except Exception as e:
            ctx.error(
                "HTML template rendering failed",
                template=str(self.template_path),
                error=str(e),
            )
            raise

    def _to_yaml(self, value) -> str:
        """Convert value to YAML string."""
        import yaml

        if not value:
            return ""
        return yaml.dump(value, sort_keys=False, default_flow_style=False).strip()

    def _format_run_id(self, run_id: str) -> str:
        """Format run_id (YYYYMMDD_HHMMSS) to human-readable format."""
        from datetime import datetime

        if not run_id or len(run_id) < 15:
            return run_id or ""

        try:
            dt = datetime.strptime(run_id[:15], "%Y%m%d_%H%M%S")
            return dt.strftime("%b %d, %I:%M %p UTC").replace(" 0", " ").lstrip("0")
        except ValueError:
            return run_id

    def render_to_file(self, metadata: PipelineStoryMetadata, output_path: str) -> str:
        """
        Render story and save to file.

        Args:
            metadata: Pipeline story metadata
            output_path: Path to save HTML file

        Returns:
            Path to saved file
        """
        ctx = get_logging_context()
        html = self.render(metadata)

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(html)
            ctx.debug(
                "HTML story written to file",
                path=str(output_file),
                size=len(html),
            )
        except Exception as e:
            ctx.error("Failed to write HTML story", path=str(output_file), error=str(e))
            raise

        return str(output_file)


class MarkdownStoryRenderer:
    """
    Renders pipeline stories as Markdown.

    Creates clean, readable Markdown documentation with:
    - GitHub-flavored markdown
    - Tables for data
    - Code blocks for errors
    - Emoji indicators
    """

    def render(self, metadata: PipelineStoryMetadata) -> str:
        """
        Render story as Markdown.

        Args:
            metadata: Pipeline story metadata

        Returns:
            Markdown string
        """
        ctx = get_logging_context()
        ctx.debug(
            "Rendering Markdown story",
            output_format="markdown",
            pipeline=metadata.pipeline_name,
        )

        lines = []

        # Header
        lines.append(f"# Pipeline Run Story: {metadata.pipeline_name}")
        lines.append("")

        # Execution info
        lines.append(f"**Started:** {metadata.started_at}")
        lines.append(f"**Completed:** {metadata.completed_at or 'Running...'}")
        lines.append(f"**Duration:** {metadata.duration:.2f}s")

        # Status
        if metadata.failed_nodes > 0:
            status = f"❌ Failed ({metadata.failed_nodes} node{'s' if metadata.failed_nodes != 1 else ''})"
        elif metadata.completed_nodes == metadata.total_nodes:
            status = f"✅ Success ({metadata.completed_nodes}/{metadata.total_nodes} nodes)"
        else:
            status = f"⚠️ Partial ({metadata.completed_nodes}/{metadata.total_nodes} completed)"

        lines.append(f"**Status:** {status}")
        lines.append("")

        # Project context
        if metadata.pipeline_layer:
            lines.append(f"**Layer:** {metadata.pipeline_layer}")

        if metadata.project:
            context_parts = [f"**Project:** {metadata.project}"]
            if metadata.plant:
                context_parts.append(f"**Plant:** {metadata.plant}")
            if metadata.asset:
                context_parts.append(f"**Asset:** {metadata.asset}")
            lines.append(" | ".join(context_parts))
            lines.append("")

        lines.append("---")
        lines.append("")

        # Summary
        lines.append("## Summary")
        lines.append("")
        lines.append(f"- ✅ **Completed:** {metadata.completed_nodes} nodes")
        lines.append(f"- ❌ **Failed:** {metadata.failed_nodes} nodes")
        lines.append(f"- Skipped: {metadata.skipped_nodes} nodes")
        lines.append(f"- Success Rate: {metadata.get_success_rate():.1f}%")
        lines.append(f"- Total Rows Processed: {metadata.get_total_rows_processed():,}")
        lines.append(f"- Duration: {metadata.duration:.2f}s")
        lines.append("")
        lines.append("---")
        lines.append("")

        # Node details
        lines.append("## Node Execution Details")
        lines.append("")

        for node in metadata.nodes:
            # Node header
            status_icon = {"success": "✅", "failed": "❌", "skipped": "⏭️"}.get(node.status, "❓")
            lines.append(f"### {status_icon} {node.node_name}")
            lines.append("")
            lines.append(f"**Operation:** `{node.operation}`")
            lines.append(f"**Duration:** {node.duration:.4f}s")

            # Historical Context
            if node.historical_avg_duration and node.duration is not None:
                diff = node.duration - node.historical_avg_duration
                icon = "^" if diff > 0 else "v"
                lines.append(
                    f"**Avg Duration (7d):** {node.historical_avg_duration:.4f}s ({icon} {abs(diff):.4f}s)"
                )

            # Data metrics
            if node.rows_in is not None:
                lines.append(f"**Rows Read:** {node.rows_in:,}")
            if node.rows_out is not None:
                lines.append(f"**Rows Out:** {node.rows_out:,}")
            if node.rows_written is not None:
                if node.rows_in is not None and node.rows_written == 0 and node.rows_in > 0:
                    lines.append(f"**Rows Written:** {node.rows_written:,} (no changes detected)")
                else:
                    lines.append(f"**Rows Written:** {node.rows_written:,}")

                # Historical Rows
                if node.historical_avg_rows is not None and node.rows_out is not None:
                    diff = node.rows_out - node.historical_avg_rows
                    icon = "🔼" if diff > 0 else "🔽"
                    lines.append(
                        f"**Avg Rows (7d):** ~{int(node.historical_avg_rows):,} ({icon} {int(abs(diff)):,})"
                    )

            if node.rows_change is not None:
                change_sign = "+" if node.rows_change > 0 else ""
                lines.append(
                    f"**Row Change:** {change_sign}{node.rows_change:,} "
                    f"({node.rows_change_pct:+.1f}%)"
                )

            # Validation Warnings
            if node.validation_warnings:
                lines.append("")
                lines.append("**Validation Warnings:**")
                for warning in node.validation_warnings:
                    lines.append(f"- ⚠️ {warning}")

            # Schema changes
            if node.columns_added or node.columns_removed:
                lines.append("")
                lines.append("**Schema Changes:**")
                for col in node.columns_added:
                    lines.append(f"- ➕ Added: `{col}`")
                for col in node.columns_removed:
                    lines.append(f"- ➖ Removed: `{col}`")

            # Error details
            if node.error_message:
                lines.append("")
                lines.append(f"**Error:** {node.error_type or 'Exception'}")
                lines.append("```")
                lines.append(node.error_message)
                lines.append("```")

            lines.append("")
            lines.append("---")
            lines.append("")

        result = "\n".join(lines)
        ctx.debug(
            "Markdown story rendered",
            output_format="markdown",
            size=len(result),
            nodes=len(metadata.nodes),
        )
        return result

    def render_to_file(self, metadata: PipelineStoryMetadata, output_path: str) -> str:
        """
        Render story and save to file.

        Args:
            metadata: Pipeline story metadata
            output_path: Path to save Markdown file

        Returns:
            Path to saved file
        """
        ctx = get_logging_context()
        markdown = self.render(metadata)

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(markdown)
            ctx.debug(
                "Markdown story written to file",
                path=str(output_file),
                size=len(markdown),
            )
        except Exception as e:
            ctx.error("Failed to write Markdown story", path=str(output_file), error=str(e))
            raise

        return str(output_file)


class JSONStoryRenderer:
    """
    Renders pipeline stories as JSON.

    Creates machine-readable JSON output for:
    - API integration
    - Programmatic analysis
    - Data storage/archival
    """

    def render(self, metadata: PipelineStoryMetadata) -> str:
        """
        Render story as JSON.

        Args:
            metadata: Pipeline story metadata

        Returns:
            JSON string
        """
        ctx = get_logging_context()
        ctx.debug(
            "Rendering JSON story",
            output_format="json",
            pipeline=metadata.pipeline_name,
        )

        result = json.dumps(metadata.to_dict(), indent=2, default=str)

        ctx.debug(
            "JSON story rendered",
            output_format="json",
            size=len(result),
            nodes=len(metadata.nodes),
        )
        return result

    def render_to_file(self, metadata: PipelineStoryMetadata, output_path: str) -> str:
        """
        Render story and save to file.

        Args:
            metadata: Pipeline story metadata
            output_path: Path to save JSON file

        Returns:
            Path to saved file
        """
        ctx = get_logging_context()
        json_str = self.render(metadata)

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(json_str)
            ctx.debug(
                "JSON story written to file",
                path=str(output_file),
                size=len(json_str),
            )
        except Exception as e:
            ctx.error("Failed to write JSON story", path=str(output_file), error=str(e))
            raise

        return str(output_file)


def get_renderer(format: str):
    """
    Get renderer for specified format.

    Args:
        format: Output format ("html", "markdown", "json")

    Returns:
        Renderer instance

    Raises:
        ValueError: If format is not supported
    """
    renderers = {
        "html": HTMLStoryRenderer,
        "markdown": MarkdownStoryRenderer,
        "md": MarkdownStoryRenderer,
        "json": JSONStoryRenderer,
    }

    renderer_class = renderers.get(format.lower())
    if not renderer_class:
        raise ValueError(
            f"Unsupported format: {format}. Supported formats: {', '.join(renderers.keys())}"
        )

    return renderer_class()

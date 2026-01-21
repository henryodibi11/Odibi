"""
Documentation Generator
=======================

Generates structured markdown documentation from Story artifacts.

- README.md: Stakeholder-facing project overview
- TECHNICAL_DETAILS.md: Engineer-facing exhaustive details
- NODE_CARDS/*.md: Per-node transformation documentation
- RUN_MEMO.md: Per-run summary (what happened, what changed)

Project-level docs update only on successful runs.
RUN_MEMO generates for every run.
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import yaml

from odibi.config import DocsConfig
from odibi.story.metadata import NodeExecutionMetadata, PipelineStoryMetadata
from odibi.utils.logging_context import get_logging_context


class DocGenerator:
    """
    Generates markdown documentation from Story metadata.

    Example:
        ```python
        generator = DocGenerator(
            config=DocsConfig(enabled=True, output_path="docs/generated/"),
            pipeline_name="sales_pipeline",
        )
        generator.generate(metadata, story_html_path="stories/sales/2026-01-21/run.html")
        ```
    """

    def __init__(
        self,
        config: DocsConfig,
        pipeline_name: str,
        workspace_root: Optional[str] = None,
    ):
        """
        Initialize documentation generator.

        Args:
            config: Documentation configuration
            pipeline_name: Name of the pipeline
            workspace_root: Root directory for output paths (defaults to cwd)
        """
        self.config = config
        self.pipeline_name = pipeline_name
        self.workspace_root = Path(workspace_root) if workspace_root else Path.cwd()
        self.output_path = self.workspace_root / config.output_path

        ctx = get_logging_context()
        ctx.debug(
            "DocGenerator initialized",
            pipeline=pipeline_name,
            output_path=str(self.output_path),
            enabled=config.enabled,
        )

    def generate(
        self,
        metadata: PipelineStoryMetadata,
        story_html_path: Optional[str] = None,
        story_json_path: Optional[str] = None,
        run_memo_path: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Generate documentation from Story metadata.

        Args:
            metadata: Pipeline story metadata from completed run
            story_html_path: Path to the Story HTML file (for linking)
            story_json_path: Path to the Story JSON file (for linking)
            run_memo_path: Override path for RUN_MEMO.md (default: alongside story)

        Returns:
            Dictionary of artifact name -> output path
        """
        ctx = get_logging_context()

        if not self.config.enabled:
            ctx.debug("Documentation generation disabled")
            return {}

        # Resolve story link URL
        story_link_url = self._resolve_story_url(story_html_path)

        outputs = self.config.outputs
        generated: Dict[str, str] = {}

        # Determine if this was a successful run
        is_success = metadata.failed_nodes == 0

        # Project-level docs only on success
        if is_success:
            self.output_path.mkdir(parents=True, exist_ok=True)

            if outputs.readme:
                path = self._generate_readme(metadata, story_link_url)
                generated["readme"] = path

            if outputs.technical_details:
                path = self._generate_technical_details(metadata)
                generated["technical_details"] = path

            if outputs.node_cards:
                paths = self._generate_node_cards(metadata)
                generated["node_cards"] = str(self.output_path / "node_cards")
                for name, path in paths.items():
                    generated[f"node_card:{name}"] = path
        else:
            ctx.info(
                "Skipping project docs update (run failed)",
                failed_nodes=metadata.failed_nodes,
            )

        # RUN_MEMO always generated
        if outputs.run_memo:
            memo_output = run_memo_path
            if not memo_output and story_html_path and "://" not in story_html_path:
                # Place alongside story HTML (local paths only)
                story_file = Path(story_html_path)
                memo_output = str(story_file.with_name(story_file.stem + "_memo.md"))
            elif not memo_output:
                # Fallback to output_path
                memo_output = str(self.output_path / f"run_memo_{metadata.run_id}.md")

            path = self._generate_run_memo(metadata, memo_output, story_link_url)
            generated["run_memo"] = path

        ctx.info(
            "Documentation generated",
            artifacts=len(generated),
            is_success=is_success,
        )

        return generated

    def _generate_readme(
        self,
        metadata: PipelineStoryMetadata,
        story_html_path: Optional[str] = None,
    ) -> str:
        """Generate README.md for the pipeline."""
        ctx = get_logging_context()

        lines = [
            f"# {metadata.pipeline_name}",
            "",
        ]

        # Layer badge
        if metadata.pipeline_layer:
            layer = metadata.pipeline_layer.lower()
            badge_color = {
                "bronze": "CD7F32",
                "silver": "C0C0C0",
                "gold": "FFD700",
                "semantic": "9B59B6",
            }.get(layer, "808080")
            lines.append(f"![Layer](https://img.shields.io/badge/layer-{layer}-{badge_color})")
            lines.append("")

        # Last run status
        status_emoji = "✅" if metadata.failed_nodes == 0 else "❌"
        lines.extend(
            [
                "## Last Run",
                "",
                "| Metric | Value |",
                "|--------|-------|",
                f"| Status | {status_emoji} {'Success' if metadata.failed_nodes == 0 else 'Failed'} |",
                f"| Run ID | `{metadata.run_id}` |",
                f"| Timestamp | {metadata.started_at} |",
                f"| Duration | {metadata.duration:.2f}s |",
                f"| Nodes | {metadata.completed_nodes}/{metadata.total_nodes} completed |",
                f"| Success Rate | {metadata.get_success_rate():.1f}% |",
                "",
            ]
        )

        # Project context
        if metadata.project or metadata.plant or metadata.asset:
            lines.extend(
                [
                    "## Project Context",
                    "",
                    "| Field | Value |",
                    "|-------|-------|",
                ]
            )
            if metadata.project:
                lines.append(f"| Project | {metadata.project} |")
            if metadata.plant:
                lines.append(f"| Plant | {metadata.plant} |")
            if metadata.asset:
                lines.append(f"| Asset | {metadata.asset} |")
            if metadata.business_unit:
                lines.append(f"| Business Unit | {metadata.business_unit} |")
            lines.append("")

        # Node summary
        lines.extend(
            [
                "## Pipeline Nodes",
                "",
                "| Node | Operation | Status | Duration | Rows Out |",
                "|------|-----------|--------|----------|----------|",
            ]
        )

        for node in metadata.nodes:
            status_icon = {"success": "✅", "failed": "❌", "skipped": "⏭️"}.get(node.status, "❓")
            rows = f"{node.rows_out:,}" if node.rows_out is not None else "-"
            node_link = (
                f"[{node.node_name}](node_cards/{self._sanitize_filename(node.node_name)}.md)"
            )
            lines.append(
                f"| {node_link} | `{node.operation}` | {status_icon} | {node.duration:.2f}s | {rows} |"
            )

        lines.append("")

        # Quick links
        lines.extend(
            [
                "## Documentation",
                "",
                "- [Technical Details](TECHNICAL_DETAILS.md)",
                "- [Node Cards](node_cards/)",
            ]
        )

        if story_html_path:
            lines.append(f"- [Full Story Report]({story_html_path})")

        lines.append("")

        # Footer
        lines.extend(
            [
                "---",
                "",
                f"*Generated by Odibi on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
            ]
        )

        content = "\n".join(lines)
        output_file = self.output_path / "README.md"
        output_file.write_text(content, encoding="utf-8")

        ctx.debug("README.md generated", path=str(output_file))
        return str(output_file)

    def _generate_technical_details(self, metadata: PipelineStoryMetadata) -> str:
        """Generate TECHNICAL_DETAILS.md with exhaustive information."""
        ctx = get_logging_context()

        lines = [
            f"# Technical Details: {metadata.pipeline_name}",
            "",
            "## Execution Summary",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Pipeline | {metadata.pipeline_name} |",
            f"| Layer | {metadata.pipeline_layer or 'N/A'} |",
            f"| Run ID | `{metadata.run_id}` |",
            f"| Started | {metadata.started_at} |",
            f"| Completed | {metadata.completed_at} |",
            f"| Duration | {metadata.duration:.2f}s |",
            f"| Total Nodes | {metadata.total_nodes} |",
            f"| Completed | {metadata.completed_nodes} |",
            f"| Failed | {metadata.failed_nodes} |",
            f"| Skipped | {metadata.skipped_nodes} |",
            f"| Total Rows Processed | {metadata.get_total_rows_processed():,} |",
            "",
        ]

        # Git info
        if metadata.git_info:
            lines.extend(
                [
                    "## Git Information",
                    "",
                    "| Field | Value |",
                    "|-------|-------|",
                ]
            )
            git = metadata.git_info
            if git.get("commit"):
                lines.append(f"| Commit | `{git['commit'][:8]}` |")
            if git.get("branch"):
                lines.append(f"| Branch | {git['branch']} |")
            if git.get("author"):
                lines.append(f"| Author | {git['author']} |")
            if git.get("message"):
                lines.append(f"| Message | {git['message'][:50]}... |")
            lines.append("")

        # Data quality summary
        quality = metadata.get_data_quality_summary()
        if quality["has_quality_issues"]:
            lines.extend(
                [
                    "## Data Quality Summary",
                    "",
                    f"- **Validation Failures:** {quality['total_validations_failed']}",
                    f"- **Failed Rows:** {quality['total_failed_rows']:,}",
                ]
            )
            if quality["nodes_with_warnings"]:
                lines.append(
                    f"- **Nodes with Warnings:** {', '.join(quality['nodes_with_warnings'])}"
                )
            lines.append("")

            if quality["top_null_columns"]:
                lines.extend(
                    [
                        "### Top Null Columns",
                        "",
                        "| Node | Column | Null % |",
                        "|------|--------|--------|",
                    ]
                )
                for col in quality["top_null_columns"][:10]:
                    lines.append(f"| {col['node']} | {col['column']} | {col['null_pct']:.1f}% |")
                lines.append("")

        # Schema summary (final output)
        final_node = None
        for node in reversed(metadata.nodes):
            if node.status == "success" and node.schema_out:
                final_node = node
                break

        if final_node and final_node.schema_out and self.config.include.schema_tables:
            lines.extend(
                [
                    "## Output Schema",
                    "",
                    f"*From node: {final_node.node_name}*",
                    "",
                    "| Column | Type |",
                    "|--------|------|",
                ]
            )
            for col in final_node.schema_out:
                # Schema format is typically "name: type" or just "name"
                if ": " in col:
                    name, dtype = col.split(": ", 1)
                else:
                    name, dtype = col, "unknown"
                lines.append(f"| {name} | `{dtype}` |")
            lines.append("")

        # Anomalies
        anomalous = metadata.get_anomalous_nodes()
        if anomalous:
            lines.extend(
                [
                    "## Anomalies Detected",
                    "",
                    "| Node | Reasons |",
                    "|------|---------|",
                ]
            )
            for node in anomalous:
                reasons = ", ".join(node.anomaly_reasons) if node.anomaly_reasons else "Unknown"
                lines.append(f"| {node.node_name} | {reasons} |")
            lines.append("")

        # Footer
        lines.extend(
            [
                "---",
                "",
                f"*Generated by Odibi on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
            ]
        )

        content = "\n".join(lines)
        output_file = self.output_path / "TECHNICAL_DETAILS.md"
        output_file.write_text(content, encoding="utf-8")

        ctx.debug("TECHNICAL_DETAILS.md generated", path=str(output_file))
        return str(output_file)

    def _generate_node_cards(self, metadata: PipelineStoryMetadata) -> Dict[str, str]:
        """Generate NODE_CARDS/*.md for each node."""
        ctx = get_logging_context()

        cards_dir = self.output_path / "node_cards"
        cards_dir.mkdir(parents=True, exist_ok=True)

        generated: Dict[str, str] = {}

        for node in metadata.nodes:
            if node.status == "skipped":
                continue

            content = self._render_node_card(node)
            filename = f"{self._sanitize_filename(node.node_name)}.md"
            output_file = cards_dir / filename
            output_file.write_text(content, encoding="utf-8")
            generated[node.node_name] = str(output_file)

        ctx.debug("Node cards generated", count=len(generated))
        return generated

    def _render_node_card(self, node: NodeExecutionMetadata) -> str:
        """Render a single node card."""
        lines = [
            f"# {node.node_name}",
            "",
        ]

        # Description
        if node.description:
            lines.extend([node.description, ""])

        # Summary table
        status_icon = {"success": "✅", "failed": "❌", "skipped": "⏭️"}.get(node.status, "❓")

        lines.extend(
            [
                "## Summary",
                "",
                "| Metric | Value |",
                "|--------|-------|",
                f"| Operation | `{node.operation}` |",
                f"| Status | {status_icon} {node.status} |",
                f"| Duration | {node.duration:.4f}s |",
            ]
        )

        if node.rows_in is not None:
            lines.append(f"| Rows In | {node.rows_in:,} |")
        if node.rows_out is not None:
            lines.append(f"| Rows Out | {node.rows_out:,} |")
        if node.rows_written is not None:
            lines.append(f"| Rows Written | {node.rows_written:,} |")
        if node.rows_change is not None:
            sign = "+" if node.rows_change > 0 else ""
            lines.append(
                f"| Row Change | {sign}{node.rows_change:,} ({node.rows_change_pct:+.1f}%) |"
            )

        lines.append("")

        # Historical context
        if node.historical_avg_duration or node.historical_avg_rows:
            lines.extend(
                [
                    "## Historical Context",
                    "",
                    "| Metric | Current | Avg (7d) |",
                    "|--------|---------|----------|",
                ]
            )
            if node.historical_avg_duration:
                diff = node.duration - node.historical_avg_duration
                icon = "🔺" if diff > 0 else "🔻"
                lines.append(
                    f"| Duration | {node.duration:.2f}s | {node.historical_avg_duration:.2f}s {icon} |"
                )
            if node.historical_avg_rows and node.rows_out is not None:
                diff = node.rows_out - int(node.historical_avg_rows)
                icon = "🔺" if diff > 0 else "🔻"
                lines.append(
                    f"| Rows | {node.rows_out:,} | {int(node.historical_avg_rows):,} {icon} |"
                )
            lines.append("")

        # Schema changes
        if node.columns_added or node.columns_removed:
            lines.extend(
                [
                    "## Schema Changes",
                    "",
                ]
            )
            if node.columns_added:
                lines.append("**Added:**")
                for col in node.columns_added:
                    lines.append(f"- `{col}`")
                lines.append("")
            if node.columns_removed:
                lines.append("**Removed:**")
                for col in node.columns_removed:
                    lines.append(f"- `{col}`")
                lines.append("")

        # Schema in/out
        if self.config.include.schema_tables:
            if node.schema_in:
                lines.extend(
                    [
                        "## Schema In",
                        "",
                        "| Column | Type |",
                        "|--------|------|",
                    ]
                )
                for col in node.schema_in[:20]:  # Limit to 20
                    if ": " in col:
                        name, dtype = col.split(": ", 1)
                    else:
                        name, dtype = col, "-"
                    lines.append(f"| {name} | `{dtype}` |")
                if len(node.schema_in) > 20:
                    lines.append(f"| ... | *{len(node.schema_in) - 20} more columns* |")
                lines.append("")

            if node.schema_out:
                lines.extend(
                    [
                        "## Schema Out",
                        "",
                        "| Column | Type |",
                        "|--------|------|",
                    ]
                )
                for col in node.schema_out[:20]:
                    if ": " in col:
                        name, dtype = col.split(": ", 1)
                    else:
                        name, dtype = col, "-"
                    lines.append(f"| {name} | `{dtype}` |")
                if len(node.schema_out) > 20:
                    lines.append(f"| ... | *{len(node.schema_out) - 20} more columns* |")
                lines.append("")

        # Transformation stack
        if node.transformation_stack:
            lines.extend(
                [
                    "## Transformations Applied",
                    "",
                ]
            )
            for i, transform in enumerate(node.transformation_stack, 1):
                lines.append(f"{i}. `{transform}`")
            lines.append("")

        # Executed SQL
        if node.executed_sql and self.config.include.sql:
            lines.extend(
                [
                    "## Executed SQL",
                    "",
                ]
            )
            for i, sql in enumerate(node.executed_sql):
                if len(node.executed_sql) > 1:
                    lines.append(f"### Query {i + 1}")
                    lines.append("")
                lines.append("```sql")
                lines.append(sql.strip())
                lines.append("```")
                lines.append("")

        # Config snapshot
        if node.config_snapshot and self.config.include.config_snapshot:
            lines.extend(
                [
                    "## Configuration",
                    "",
                    "```yaml",
                    yaml.dump(
                        node.config_snapshot, sort_keys=False, default_flow_style=False
                    ).strip(),
                    "```",
                    "",
                ]
            )

        # Validation warnings
        if node.validation_warnings:
            lines.extend(
                [
                    "## Validation Warnings",
                    "",
                ]
            )
            for warning in node.validation_warnings:
                lines.append(f"- ⚠️ {warning}")
            lines.append("")

        # Error details
        if node.error_message:
            lines.extend(
                [
                    "## Error Details",
                    "",
                    f"**Type:** `{node.error_type or 'Exception'}`",
                    "",
                    "```",
                    node.error_message,
                    "```",
                    "",
                ]
            )

            if node.error_suggestions:
                lines.append("**Suggestions:**")
                for suggestion in node.error_suggestions:
                    lines.append(f"- {suggestion}")
                lines.append("")

        # Runbook link
        if node.runbook_url:
            lines.extend(
                [
                    "## Runbook",
                    "",
                    f"[View Runbook]({node.runbook_url})",
                    "",
                ]
            )

        # Footer
        lines.extend(
            [
                "---",
                "",
                f"*Generated by Odibi on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
            ]
        )

        return "\n".join(lines)

    def _generate_run_memo(
        self,
        metadata: PipelineStoryMetadata,
        output_path: str,
        story_html_path: Optional[str] = None,
    ) -> str:
        """Generate RUN_MEMO.md for this specific run."""
        ctx = get_logging_context()

        status_emoji = "✅" if metadata.failed_nodes == 0 else "❌"
        status_text = "Success" if metadata.failed_nodes == 0 else "Failed"

        lines = [
            f"# Run Memo: {metadata.pipeline_name}",
            "",
            f"**Status:** {status_emoji} {status_text}",
            f"**Run ID:** `{metadata.run_id}`",
            f"**Timestamp:** {metadata.started_at}",
            f"**Duration:** {metadata.duration:.2f}s",
            "",
        ]

        # Quick stats
        lines.extend(
            [
                "## Summary",
                "",
                f"- **Completed:** {metadata.completed_nodes}/{metadata.total_nodes} nodes",
                f"- **Failed:** {metadata.failed_nodes} nodes",
                f"- **Skipped:** {metadata.skipped_nodes} nodes",
                f"- **Rows Processed:** {metadata.get_total_rows_processed():,}",
                "",
            ]
        )

        # What changed from last run
        if metadata.change_summary:
            lines.extend(
                [
                    "## What Changed",
                    "",
                ]
            )
            for change in metadata.change_summary:
                lines.append(f"- {change}")
            lines.append("")
        else:
            # Check individual nodes for changes
            changed_nodes = [n for n in metadata.nodes if n.changed_from_last_success]
            if changed_nodes:
                lines.extend(
                    [
                        "## What Changed",
                        "",
                        "| Node | Changes |",
                        "|------|---------|",
                    ]
                )
                for node in changed_nodes:
                    changes = (
                        ", ".join(node.changes_detected) if node.changes_detected else "Unknown"
                    )
                    lines.append(f"| {node.node_name} | {changes} |")
                lines.append("")

        # Anomalies
        anomalous = metadata.get_anomalous_nodes()
        if anomalous:
            lines.extend(
                [
                    "## ⚠️ Anomalies Detected",
                    "",
                ]
            )
            for node in anomalous:
                reasons = (
                    ", ".join(node.anomaly_reasons)
                    if node.anomaly_reasons
                    else "Performance deviation"
                )
                lines.append(f"- **{node.node_name}:** {reasons}")
            lines.append("")

        # Failed nodes
        failed_nodes = [n for n in metadata.nodes if n.status == "failed"]
        if failed_nodes:
            lines.extend(
                [
                    "## ❌ Failed Nodes",
                    "",
                ]
            )
            for node in failed_nodes:
                lines.append(f"### {node.node_name}")
                lines.append("")
                lines.append(f"**Error:** `{node.error_type or 'Exception'}`")
                lines.append("")
                lines.append("```")
                lines.append(node.error_message or "No error message")
                lines.append("```")
                lines.append("")

                if node.error_suggestions:
                    lines.append("**Suggestions:**")
                    for suggestion in node.error_suggestions:
                        lines.append(f"- {suggestion}")
                    lines.append("")

        # Data quality issues
        quality = metadata.get_data_quality_summary()
        if quality["has_quality_issues"]:
            lines.extend(
                [
                    "## Data Quality Issues",
                    "",
                    f"- **Validation Failures:** {quality['total_validations_failed']}",
                    f"- **Failed Rows:** {quality['total_failed_rows']:,}",
                    "",
                ]
            )

        # Link to full story
        if story_html_path:
            lines.extend(
                [
                    "## Full Report",
                    "",
                    f"[View Full Story Report]({story_html_path})",
                    "",
                ]
            )

        # Footer
        lines.extend(
            [
                "---",
                "",
                f"*Generated by Odibi on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
            ]
        )

        content = "\n".join(lines)
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(content, encoding="utf-8")

        ctx.debug("RUN_MEMO.md generated", path=str(output_file))
        return str(output_file)

    def _resolve_story_url(self, story_html_path: Optional[str]) -> Optional[str]:
        """
        Resolve the URL/path to use for story links.

        Returns the path as-is for display. For remote paths (abfss://, https://),
        the link may not be clickable in a browser but shows where the file is stored.

        Args:
            story_html_path: Path to story HTML file

        Returns:
            Path string for display, or None if not provided
        """
        return story_html_path

    def _sanitize_filename(self, name: str) -> str:
        """Sanitize node name for use as filename."""
        return (
            name.lower()
            .replace(" ", "_")
            .replace("/", "_")
            .replace("\\", "_")
            .replace(".", "_")
            .replace(":", "_")
        )

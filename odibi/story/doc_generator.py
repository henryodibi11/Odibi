"""
Documentation Generator
=======================

Generates structured markdown documentation from Story artifacts.

Project-level docs (aggregate all pipelines):
- README.md: Stakeholder-facing project overview with all pipelines
- TECHNICAL_DETAILS.md: Engineer-facing exhaustive details for all pipelines
- NODE_CARDS/{pipeline}/*.md: Per-node transformation documentation
- RUN_HISTORY.md: Consolidated run memos (newest first)

Project-level docs update only on successful runs.
RUN_HISTORY updates every run, prepending new entries.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import yaml

from odibi.config import DocsConfig
from odibi.story.metadata import NodeExecutionMetadata, PipelineStoryMetadata
from odibi.utils.logging_context import get_logging_context


# Layer ordering for display
LAYER_ORDER = {"bronze": 1, "silver": 2, "gold": 3, "semantic": 4}


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
        storage_options: Optional[Dict[str, Any]] = None,
        write_file: Optional[Callable[[str, str], None]] = None,
    ):
        """
        Initialize documentation generator.

        Args:
            config: Documentation configuration
            pipeline_name: Name of the pipeline
            workspace_root: Root directory for output paths (defaults to cwd).
                For remote storage, this should be the full remote path (e.g., abfss://...).
            storage_options: Credentials for remote storage (e.g., ADLS). Used with fsspec.
            write_file: Optional callable to write files. If provided, used instead of
                fsspec/local writes. Signature: (path: str, content: str) -> None
        """
        self.config = config
        self.pipeline_name = pipeline_name
        self.storage_options = storage_options or {}
        self.write_file_callback = write_file

        # Detect if output is remote (cloud storage)
        output_path_str = config.output_path
        if workspace_root and "://" in workspace_root:
            # Remote workspace root - combine with output path
            self.is_remote = True
            self.output_path_str = f"{workspace_root.rstrip('/')}/{output_path_str.lstrip('/')}"
            self.output_path = None  # Don't use Path for remote
        elif "://" in output_path_str:
            # Output path itself is remote
            self.is_remote = True
            self.output_path_str = output_path_str
            self.output_path = None
        else:
            # Local filesystem
            self.is_remote = False
            self.workspace_root = Path(workspace_root) if workspace_root else Path.cwd()
            self.output_path = self.workspace_root / output_path_str
            self.output_path_str = str(self.output_path)

        ctx = get_logging_context()
        ctx.debug(
            "DocGenerator initialized",
            pipeline=pipeline_name,
            output_path=self.output_path_str,
            is_remote=self.is_remote,
            enabled=config.enabled,
        )

    def _get_state_path(self) -> str:
        """Get path to the pipeline state file."""
        if self.is_remote:
            return f"{self.output_path_str.rstrip('/')}/.pipelines.json"
        return str(self.output_path / ".pipelines.json")

    def _write_file(self, path: str, content: str) -> None:
        """Write content to file, handling remote storage if configured."""
        ctx = get_logging_context()

        # Use callback if provided
        if self.write_file_callback:
            self.write_file_callback(path, content)
            ctx.debug("File written via callback", path=path, size=len(content))
            return

        if self.is_remote or "://" in path:
            self._write_remote(path, content)
        else:
            # Local filesystem
            local_path = Path(path)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_text(content, encoding="utf-8")
            ctx.debug("File written locally", path=path, size=len(content))

    def _write_remote(self, path: str, content: str) -> None:
        """Write content to remote path using fsspec."""
        ctx = get_logging_context()
        try:
            import fsspec

            with fsspec.open(path, "w", encoding="utf-8", **self.storage_options) as f:
                f.write(content)
            ctx.debug("Remote file written", path=path, size=len(content))
        except ImportError:
            # Fallback for Databricks environments without fsspec
            try:
                from pyspark.dbutils import DBUtils
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.getOrCreate()
                dbutils = DBUtils(spark)
                dbutils.fs.put(path, content, True)
                ctx.debug("Remote file written via dbutils", path=path, size=len(content))
            except Exception as e:
                ctx.error("Failed to write remote file", path=path, error=str(e))
                raise RuntimeError(
                    f"Could not write to {path}. Install 'fsspec' or 'adlfs'."
                ) from e

    def _read_file(self, path: str) -> Optional[str]:
        """Read content from file, handling remote storage if configured."""
        if self.is_remote or "://" in path:
            return self._read_remote(path)
        else:
            local_path = Path(path)
            if local_path.exists():
                return local_path.read_text(encoding="utf-8")
            return None

    def _read_remote(self, path: str) -> Optional[str]:
        """Read content from remote path using fsspec."""
        ctx = get_logging_context()
        try:
            import fsspec

            try:
                with fsspec.open(path, "r", encoding="utf-8", **self.storage_options) as f:
                    return f.read()
            except FileNotFoundError:
                return None
        except ImportError:
            # Fallback for Databricks
            try:
                from pyspark.dbutils import DBUtils
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.getOrCreate()
                dbutils = DBUtils(spark)
                try:
                    return dbutils.fs.head(path, 1024 * 1024)  # 1MB max
                except Exception:
                    return None
            except Exception as e:
                ctx.debug("Failed to read remote file", path=path, error=str(e))
                return None

    def _load_pipeline_state(self) -> Dict[str, Any]:
        """Load aggregated pipeline state from storage."""
        state_path = self._get_state_path()
        content = self._read_file(state_path)
        if content:
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                return {"pipelines": {}, "project": None}
        return {"pipelines": {}, "project": None}

    def _save_pipeline_state(self, state: Dict[str, Any]) -> None:
        """Save aggregated pipeline state to storage."""
        state_path = self._get_state_path()
        self._write_file(state_path, json.dumps(state, indent=2))

    def _update_pipeline_state(
        self,
        metadata: PipelineStoryMetadata,
        story_html_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Update state with current pipeline run and return full state."""
        state = self._load_pipeline_state()

        # Update project info if available
        if metadata.project:
            state["project"] = metadata.project

        # Update this pipeline's entry
        pipeline_entry = {
            "name": metadata.pipeline_name,
            "layer": metadata.pipeline_layer,
            "last_run_id": metadata.run_id,
            "last_run_time": metadata.started_at,
            "last_status": "success" if metadata.failed_nodes == 0 else "failed",
            "total_nodes": metadata.total_nodes,
            "completed_nodes": metadata.completed_nodes,
            "failed_nodes": metadata.failed_nodes,
            "duration": metadata.duration,
            "story_path": story_html_path,
            "nodes": [
                {
                    "name": n.node_name,
                    "operation": n.operation,
                    "status": n.status,
                    "duration": n.duration,
                    "rows_out": n.rows_out,
                }
                for n in metadata.nodes
            ],
        }

        state["pipelines"][metadata.pipeline_name] = pipeline_entry
        self._save_pipeline_state(state)
        return state

    def generate(
        self,
        metadata: PipelineStoryMetadata,
        story_html_path: Optional[str] = None,
        story_json_path: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Generate documentation from Story metadata.

        Updates project-level docs that aggregate all pipelines.

        Args:
            metadata: Pipeline story metadata from completed run
            story_html_path: Path to the Story HTML file (for linking)
            story_json_path: Path to the Story JSON file (for linking)

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

        # Always update pipeline state (for aggregation)
        # For local paths, ensure directory exists
        if not self.is_remote and self.output_path:
            self.output_path.mkdir(parents=True, exist_ok=True)
        project_state = self._update_pipeline_state(metadata, story_link_url)

        # Project-level docs only on success
        if is_success:
            if outputs.readme:
                path = self._generate_readme(metadata, story_link_url, project_state)
                generated["readme"] = path

            if outputs.technical_details:
                path = self._generate_technical_details(metadata, project_state)
                generated["technical_details"] = path

            if outputs.node_cards:
                paths = self._generate_node_cards(metadata)
                generated["node_cards"] = (
                    f"{self.output_path_str.rstrip('/')}/node_cards/{self.pipeline_name}"
                )
                for name, path in paths.items():
                    generated[f"node_card:{name}"] = path
        else:
            ctx.info(
                "Skipping project docs update (run failed)",
                failed_nodes=metadata.failed_nodes,
            )

        # RUN_MEMO always generated - consolidated into single RUN_HISTORY.md
        if outputs.run_memo:
            history_path = f"{self.output_path_str.rstrip('/')}/RUN_HISTORY.md"
            path = self._generate_run_memo(metadata, history_path, story_link_url)
            generated["run_memo"] = path

        ctx.info(
            "Documentation generated",
            artifacts=len(generated),
            is_success=is_success,
            pipelines_tracked=len(project_state.get("pipelines", {})),
        )

        return generated

    def _generate_readme(
        self,
        metadata: PipelineStoryMetadata,
        story_html_path: Optional[str],
        project_state: Dict[str, Any],
    ) -> str:
        """Generate project-level README.md showing all pipelines."""
        ctx = get_logging_context()

        project_name = project_state.get("project") or "Data Project"
        pipelines = project_state.get("pipelines", {})

        lines = [
            f"# {project_name}",
            "",
        ]

        # Run Health Summary (quick triage info)
        health = metadata.get_run_health_summary()
        lines.extend(
            [
                "## Run Health",
                "",
                "| Metric | Value |",
                "|--------|-------|",
                f"| Status | {'✅ Success' if not health['has_failures'] else '❌ Failed'} |",
                f"| Failed Nodes | {health['failed_count']} |",
                f"| Anomalies | {health['anomaly_count']} |",
            ]
        )
        if health["first_failure_node"]:
            lines.append(f"| First Failure | {health['first_failure_node']} |")
        lines.append("")

        # Data Freshness
        freshness = metadata.get_freshness_info()
        if freshness:
            lines.extend(
                [
                    "## Data Freshness",
                    "",
                    f"**Latest data:** {freshness['formatted']} (from `{freshness['column']}` in {freshness['node']})",
                    "",
                ]
            )

        # Key Metrics
        lines.extend(
            [
                "## Key Metrics",
                "",
                "| Metric | Value |",
                "|--------|-------|",
                f"| Total Rows In | {metadata.get_total_rows_in():,} |",
                f"| Total Rows Processed | {metadata.get_total_rows_processed():,} |",
                f"| Rows Dropped/Filtered | {metadata.get_rows_dropped():,} |",
            ]
        )
        final_rows = metadata.get_final_output_rows()
        if final_rows is not None:
            lines.append(f"| Final Output Rows | {final_rows:,} |")
        lines.append("")

        # Git info in README (moved from just TECHNICAL_DETAILS)
        if metadata.git_info:
            git = metadata.git_info
            lines.extend(
                [
                    "## Version",
                    "",
                    f"**Branch:** {git.get('branch', 'N/A')} | **Commit:** `{git.get('commit', 'N/A')[:8]}`",
                    "",
                ]
            )

        # Project context from current metadata
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

        # Pipeline DAG (Mermaid diagram from graph_data)
        if metadata.graph_data:
            mermaid_lines = self._render_mermaid_dag(metadata.graph_data)
            if mermaid_lines:
                lines.extend(
                    [
                        "## Pipeline DAG",
                        "",
                        "```mermaid",
                    ]
                )
                lines.extend(mermaid_lines)
                lines.extend(
                    [
                        "```",
                        "",
                    ]
                )

        # Pipelines summary table
        lines.extend(
            [
                "## Pipelines",
                "",
                "| Pipeline | Layer | Last Run | Status | Nodes | Duration |",
                "|----------|-------|----------|--------|-------|----------|",
            ]
        )

        # Sort pipelines by layer order, then by name
        sorted_pipelines = sorted(
            pipelines.values(),
            key=lambda p: (
                LAYER_ORDER.get((p.get("layer") or "").lower(), 99),
                p.get("name", ""),
            ),
        )

        for p in sorted_pipelines:
            status_icon = "✅" if p.get("last_status") == "success" else "❌"
            layer = p.get("layer") or "-"
            layer_badge = self._get_layer_badge(layer)
            nodes = f"{p.get('completed_nodes', 0)}/{p.get('total_nodes', 0)}"
            duration = f"{p.get('duration', 0):.2f}s"
            last_run = p.get("last_run_time", "-")
            if isinstance(last_run, str) and "T" in last_run:
                last_run = last_run.split("T")[0]  # Just the date

            pipeline_link = f"[{p['name']}](#{p['name'].lower().replace('_', '-')})"
            lines.append(
                f"| {pipeline_link} | {layer_badge} | {last_run} | {status_icon} | {nodes} | {duration} |"
            )

        lines.append("")

        # Per-pipeline details
        for p in sorted_pipelines:
            pipeline_name = p["name"]
            layer = p.get("layer")
            nodes = p.get("nodes", [])

            lines.extend(
                [
                    f"## {pipeline_name}",
                    "",
                ]
            )

            if layer:
                lines.append(self._get_layer_badge_image(layer))
                lines.append("")

            lines.extend(
                [
                    "| Node | Operation | Status | Duration | Rows Out |",
                    "|------|-----------|--------|----------|----------|",
                ]
            )

            for node in nodes:
                status_icon = {"success": "✅", "failed": "❌", "skipped": "⏭️"}.get(
                    node.get("status", ""), "❓"
                )
                rows = f"{node['rows_out']:,}" if node.get("rows_out") is not None else "-"
                node_link = f"[{node['name']}](node_cards/{pipeline_name}/{self._sanitize_filename(node['name'])}.md)"
                duration = f"{node.get('duration', 0):.2f}s"
                lines.append(
                    f"| {node_link} | `{node.get('operation', '-')}` | {status_icon} | {duration} | {rows} |"
                )

            # Story link if available
            if p.get("story_path"):
                lines.append("")
                lines.append(f"📊 [Full Story Report]({p['story_path']})")

            lines.append("")

        # Quick links
        lines.extend(
            [
                "## Documentation",
                "",
                "- [Technical Details](TECHNICAL_DETAILS.md)",
                "- [Run History](RUN_HISTORY.md)",
                "- [Node Cards](node_cards/)",
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
        output_file = f"{self.output_path_str.rstrip('/')}/README.md"
        self._write_file(output_file, content)

        ctx.debug("README.md generated", path=output_file, pipelines=len(pipelines))
        return output_file

    def _get_layer_badge(self, layer: str) -> str:
        """Get a simple text badge for a layer."""
        return layer.lower() if layer else "-"

    def _get_layer_badge_image(self, layer: str) -> str:
        """Get a shields.io badge for a layer."""
        layer_lower = layer.lower()
        badge_color = {
            "bronze": "CD7F32",
            "silver": "C0C0C0",
            "gold": "FFD700",
            "semantic": "9B59B6",
        }.get(layer_lower, "808080")
        return f"![Layer](https://img.shields.io/badge/layer-{layer_lower}-{badge_color})"

    def _generate_technical_details(
        self,
        metadata: PipelineStoryMetadata,
        project_state: Dict[str, Any],
    ) -> str:
        """Generate TECHNICAL_DETAILS.md with exhaustive information for all pipelines."""
        ctx = get_logging_context()

        project_name = project_state.get("project") or "Data Project"
        pipelines = project_state.get("pipelines", {})

        lines = [
            f"# Technical Details: {project_name}",
            "",
            "## Project Summary",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Project | {project_name} |",
            f"| Pipelines | {len(pipelines)} |",
            f"| Last Updated | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} |",
            "",
        ]

        # Sort pipelines by layer
        sorted_pipelines = sorted(
            pipelines.values(),
            key=lambda p: (
                LAYER_ORDER.get((p.get("layer") or "").lower(), 99),
                p.get("name", ""),
            ),
        )

        # Generate section for each pipeline
        for p in sorted_pipelines:
            pipeline_name = p["name"]
            lines.extend(
                [
                    f"## {pipeline_name}",
                    "",
                ]
            )

            if p.get("layer"):
                lines.append(self._get_layer_badge_image(p["layer"]))
                lines.append("")

            lines.extend(
                [
                    "### Execution Summary",
                    "",
                    "| Metric | Value |",
                    "|--------|-------|",
                    f"| Pipeline | {pipeline_name} |",
                    f"| Layer | {p.get('layer') or 'N/A'} |",
                    f"| Run ID | `{p.get('last_run_id', 'N/A')}` |",
                    f"| Last Run | {p.get('last_run_time', 'N/A')} |",
                    f"| Duration | {p.get('duration', 0):.2f}s |",
                    f"| Total Nodes | {p.get('total_nodes', 0)} |",
                    f"| Completed | {p.get('completed_nodes', 0)} |",
                    f"| Failed | {p.get('failed_nodes', 0)} |",
                    "",
                ]
            )

        # Now add current pipeline's detailed info (SQL, schemas, etc.)
        lines.extend(
            [
                f"## Current Run Details: {metadata.pipeline_name}",
                "",
            ]
        )

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

        # Output schemas - show all nodes that write data (actual project outputs)
        if self.config.include.schema_tables:
            output_nodes = [
                node
                for node in metadata.nodes
                if node.status == "success" and node.schema_out and node.rows_written is not None
            ]

            if output_nodes:
                lines.extend(
                    [
                        "## Output Schemas",
                        "",
                        "Tables written by this pipeline:",
                        "",
                    ]
                )

                for node in output_nodes:
                    lines.extend(
                        [
                            f"### {node.node_name}",
                            "",
                        ]
                    )

                    if node.rows_written is not None:
                        lines.append(f"*{node.rows_written:,} rows written*")
                        lines.append("")

                    lines.extend(
                        [
                            "| Column | Type |",
                            "|--------|------|",
                        ]
                    )

                    for col in node.schema_out:
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
        output_file = f"{self.output_path_str.rstrip('/')}/TECHNICAL_DETAILS.md"
        self._write_file(output_file, content)

        ctx.debug("TECHNICAL_DETAILS.md generated", path=output_file)
        return output_file

    def _generate_node_cards(self, metadata: PipelineStoryMetadata) -> Dict[str, str]:
        """Generate NODE_CARDS/{pipeline}/*.md for each node."""
        ctx = get_logging_context()

        # Use pipeline-specific subfolder
        cards_dir = f"{self.output_path_str.rstrip('/')}/node_cards/{self.pipeline_name}"

        # For local paths, ensure directory exists
        if not self.is_remote:
            Path(cards_dir).mkdir(parents=True, exist_ok=True)

        generated: Dict[str, str] = {}

        for node in metadata.nodes:
            if node.status == "skipped":
                continue

            content = self._render_node_card(node)
            filename = f"{self._sanitize_filename(node.node_name)}.md"
            output_file = f"{cards_dir}/{filename}"
            self._write_file(output_file, content)
            generated[node.node_name] = output_file

        ctx.debug("Node cards generated", count=len(generated), pipeline=self.pipeline_name)
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
        if node.columns_added or node.columns_removed or node.columns_renamed:
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
            if node.columns_renamed:
                lines.append("**Renamed:**")
                for col in node.columns_renamed:
                    lines.append(f"- `{col}`")
                lines.append("")

        # Schema in/out
        if self.config.include.schema_tables:
            if node.schema_in:
                schema_in = self._normalize_schema(node.schema_in)
                if schema_in:
                    lines.extend(
                        [
                            "## Schema In",
                            "",
                            "| Column | Type |",
                            "|--------|------|",
                        ]
                    )
                    for col in schema_in[:20]:  # Limit to 20
                        if ": " in col:
                            name, dtype = col.split(": ", 1)
                        else:
                            name, dtype = col, "-"
                        lines.append(f"| {name} | `{dtype}` |")
                    if len(schema_in) > 20:
                        lines.append(f"| ... | *{len(schema_in) - 20} more columns* |")
                    lines.append("")

            if node.schema_out:
                schema_out = self._normalize_schema(node.schema_out)
                if schema_out:
                    lines.extend(
                        [
                            "## Schema Out",
                            "",
                            "| Column | Type |",
                            "|--------|------|",
                        ]
                    )
                    for col in schema_out[:20]:
                        if ": " in col:
                            name, dtype = col.split(": ", 1)
                        else:
                            name, dtype = col, "-"
                        lines.append(f"| {name} | `{dtype}` |")
                    if len(schema_out) > 20:
                        lines.append(f"| ... | *{len(schema_out) - 20} more columns* |")
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

            # Use cleaned traceback if available (easier to read)
            traceback_to_show = node.error_traceback_cleaned or node.error_traceback
            if traceback_to_show:
                lines.extend(
                    [
                        "**Traceback:**",
                        "",
                        "```",
                        traceback_to_show,
                        "```",
                        "",
                    ]
                )

            if node.error_suggestions:
                lines.append("**Suggestions:**")
                for suggestion in node.error_suggestions:
                    lines.append(f"- {suggestion}")
                lines.append("")

            # Error context (additional debugging info)
            if node.error_context:
                lines.extend(
                    [
                        "**Context:**",
                        "",
                    ]
                )
                for key, val in node.error_context.items():
                    lines.append(f"- **{key}:** {val}")
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

        # Sample Data (for LLM to understand what the data looks like)
        if node.sample_data:
            lines.extend(
                [
                    "## Sample Data",
                    "",
                ]
            )
            sample_lines = self._render_sample_data_table(node.sample_data, max_rows=5)
            lines.extend(sample_lines)
            lines.append("")

        # Null Profile (data quality insight)
        if node.null_profile:
            null_lines = self._render_null_profile(node.null_profile)
            if null_lines:
                lines.extend(
                    [
                        "## Null Profile",
                        "",
                    ]
                )
                lines.extend(null_lines)
                lines.append("")

        # Column Statistics (data profiling)
        if node.column_statistics:
            stats_lines = self._render_column_statistics(node.column_statistics)
            if stats_lines:
                lines.extend(
                    [
                        "## Column Statistics",
                        "",
                    ]
                )
                lines.extend(stats_lines)
                lines.append("")

        # Failed Rows Samples (actual bad data for debugging)
        if node.failed_rows_samples:
            lines.extend(
                [
                    "## Failed Rows",
                    "",
                ]
            )
            lines.extend(self._render_failed_rows_samples(node))

        # Source Files (input lineage)
        if node.source_files:
            lines.extend(
                [
                    "## Source Files",
                    "",
                ]
            )
            for src in node.source_files[:10]:  # Limit to 10
                lines.append(f"- `{src}`")
            if len(node.source_files) > 10:
                lines.append(f"- *... {len(node.source_files) - 10} more files*")
            lines.append("")

        # Delta Info (version and write metrics)
        if node.delta_info:
            lines.extend(
                [
                    "## Delta Write Info",
                    "",
                    "| Metric | Value |",
                    "|--------|-------|",
                    f"| Version | {node.delta_info.version} |",
                    f"| Operation | {node.delta_info.operation or 'N/A'} |",
                ]
            )
            if node.delta_info.read_version is not None:
                lines.append(f"| Read Version | {node.delta_info.read_version} |")
            # Add operation metrics if available
            if node.delta_info.operation_metrics:
                for key, val in node.delta_info.operation_metrics.items():
                    lines.append(f"| {key} | {val} |")
            lines.append("")

        # Duration History (performance trend)
        if node.duration_history:
            lines.extend(
                [
                    "## Duration History",
                    "",
                    "| Run | Duration |",
                    "|-----|----------|",
                ]
            )
            for entry in node.duration_history[-7:]:  # Last 7 runs
                run_id = entry.get("run_id", "N/A")
                dur = entry.get("duration", 0)
                lines.append(f"| {run_id} | {dur:.2f}s |")
            lines.append("")

        # Cross-run Comparison (what changed from last success)
        if node.changed_from_last_success:
            lines.extend(
                [
                    "## Changes from Last Success",
                    "",
                ]
            )
            if node.changes_detected:
                lines.append(f"**Changed:** {', '.join(node.changes_detected)}")
                lines.append("")
            if node.previous_rows_out is not None and node.rows_out is not None:
                diff = node.rows_out - node.previous_rows_out
                icon = "🔺" if diff > 0 else "🔻"
                lines.append(
                    f"- Rows: {node.previous_rows_out:,} → {node.rows_out:,} ({icon}{diff:+,})"
                )
            if node.previous_duration is not None:
                diff = node.duration - node.previous_duration
                icon = "🔺" if diff > 0 else "🔻"
                lines.append(
                    f"- Duration: {node.previous_duration:.2f}s → {node.duration:.2f}s ({icon}{diff:+.2f}s)"
                )
            if node.previous_sql_hash and node.sql_hash and node.previous_sql_hash != node.sql_hash:
                lines.append("- SQL logic changed")
            lines.append("")

        # Execution Steps (troubleshooting)
        if node.execution_steps:
            lines.extend(
                [
                    "## Execution Steps",
                    "",
                ]
            )
            for i, step in enumerate(node.execution_steps, 1):
                lines.append(f"{i}. {step}")
            lines.append("")

        # Retry History (debugging)
        if node.retry_history:
            lines.extend(
                [
                    "## Retry History",
                    "",
                    "| Attempt | Error | Timestamp |",
                    "|---------|-------|-----------|",
                ]
            )
            for attempt in node.retry_history:
                attempt_num = attempt.get("attempt", "?")
                error = attempt.get("error", "Unknown")[:50]
                ts = attempt.get("timestamp", "N/A")
                lines.append(f"| {attempt_num} | {error} | {ts} |")
            lines.append("")

        # Environment (execution context)
        if node.environment:
            lines.extend(
                [
                    "## Environment",
                    "",
                    "| Key | Value |",
                    "|-----|-------|",
                ]
            )
            for key, val in list(node.environment.items())[:10]:
                lines.append(f"| {key} | {val} |")
            lines.append("")

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
        """Generate run memo and prepend to RUN_HISTORY.md (newest first)."""
        ctx = get_logging_context()

        status_emoji = "✅" if metadata.failed_nodes == 0 else "❌"
        status_text = "Success" if metadata.failed_nodes == 0 else "Failed"

        lines = [
            f"## {metadata.run_id} - {status_emoji} {status_text}",
            "",
            f"**Pipeline:** {metadata.pipeline_name}",
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

        # Separator between runs
        lines.extend(
            [
                "",
                "---",
                "",
            ]
        )

        new_entry = "\n".join(lines)

        # Prepend new entry to existing history (newest first)
        header = f"# Run History: {metadata.pipeline_name}\n\n"
        existing = self._read_file(output_path)
        if existing:
            # Strip old header if present
            if existing.startswith("# Run History:"):
                existing = existing.split("\n", 2)[-1].lstrip("\n")
            content = header + new_entry + existing
        else:
            content = header + new_entry

        self._write_file(output_path, content)

        ctx.debug("RUN_HISTORY.md updated", path=output_path)
        return output_path

    def _normalize_schema(self, schema: Any) -> List[str]:
        """
        Normalize schema to a list of "column: type" strings.

        Handles both list format and dict format.
        """
        if schema is None:
            return []

        if isinstance(schema, list):
            return schema

        if isinstance(schema, dict):
            # Convert dict to list of "col: type" strings
            return [f"{k}: {v}" for k, v in schema.items()]

        # Unknown format, return empty
        return []

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

    def _render_mermaid_dag(self, graph_data: Dict[str, Any]) -> List[str]:
        """Render graph_data as Mermaid flowchart lines."""
        nodes = graph_data.get("nodes", [])
        edges = graph_data.get("edges", [])

        if not nodes:
            return []

        lines = ["flowchart LR"]

        # Define nodes with labels
        for node in nodes:
            node_id = node.get("id", "").replace("-", "_").replace(" ", "_")
            node_type = node.get("type", "")
            label = node.get("id", node_id)

            # Style based on type
            if node_type == "source":
                lines.append(f"    {node_id}[({label})]")  # Stadium shape for sources
            elif node_type == "sink":
                lines.append(f"    {node_id}[/{label}/]")  # Parallelogram for sinks
            else:
                lines.append(f"    {node_id}[{label}]")  # Rectangle for transforms

        # Define edges
        for edge in edges:
            from_id = edge.get("from", "").replace("-", "_").replace(" ", "_")
            to_id = edge.get("to", "").replace("-", "_").replace(" ", "_")
            if from_id and to_id:
                lines.append(f"    {from_id} --> {to_id}")

        return lines

    def _render_sample_data_table(
        self, sample_data: List[Dict[str, Any]], max_rows: int = 5
    ) -> List[str]:
        """Render sample data as a markdown table."""
        if not sample_data:
            return []

        lines = []
        sample = sample_data[:max_rows]

        # Get all columns from all rows
        all_cols = []
        for row in sample:
            for col in row.keys():
                if col not in all_cols:
                    all_cols.append(col)

        if not all_cols:
            return []

        # Header
        lines.append("| " + " | ".join(all_cols) + " |")
        lines.append("|" + "|".join(["---"] * len(all_cols)) + "|")

        # Data rows
        for row in sample:
            values = []
            for col in all_cols:
                val = row.get(col, "")
                # Truncate long values
                val_str = str(val) if val is not None else ""
                if len(val_str) > 30:
                    val_str = val_str[:27] + "..."
                values.append(val_str)
            lines.append("| " + " | ".join(values) + " |")

        if len(sample_data) > max_rows:
            lines.append(
                f"| *... {len(sample_data) - max_rows} more rows* |" + " |" * (len(all_cols) - 1)
            )

        return lines

    def _render_null_profile(self, null_profile: Dict[str, float]) -> List[str]:
        """Render null profile as markdown table."""
        if not null_profile:
            return []

        # Filter to columns with nulls and sort by null percentage
        cols_with_nulls = [(col, pct) for col, pct in null_profile.items() if pct and pct > 0]
        if not cols_with_nulls:
            return []

        cols_with_nulls.sort(key=lambda x: x[1], reverse=True)

        lines = [
            "| Column | Null % |",
            "|--------|--------|",
        ]
        for col, pct in cols_with_nulls[:15]:  # Top 15
            lines.append(f"| {col} | {pct:.1f}% |")

        if len(cols_with_nulls) > 15:
            lines.append(f"| *... {len(cols_with_nulls) - 15} more columns* | |")

        return lines

    def _render_column_statistics(self, stats: Dict[str, Dict[str, Any]]) -> List[str]:
        """Render column statistics as markdown table."""
        if not stats:
            return []

        lines = [
            "| Column | Min | Max | Mean | StdDev |",
            "|--------|-----|-----|------|--------|",
        ]

        for col, col_stats in list(stats.items())[:20]:  # Top 20 columns
            min_val = col_stats.get("min", "-")
            max_val = col_stats.get("max", "-")
            mean_val = col_stats.get("mean", "-")
            stddev_val = col_stats.get("stddev", "-")

            # Format numeric values
            if isinstance(mean_val, float):
                mean_val = f"{mean_val:.2f}"
            if isinstance(stddev_val, float):
                stddev_val = f"{stddev_val:.2f}"

            lines.append(f"| {col} | {min_val} | {max_val} | {mean_val} | {stddev_val} |")

        return lines

    def _render_failed_rows_samples(self, node: NodeExecutionMetadata) -> List[str]:
        """Render failed rows samples for each validation."""
        if not node.failed_rows_samples:
            return []

        lines = []
        for validation_name, samples in node.failed_rows_samples.items():
            count = node.failed_rows_counts.get(validation_name, len(samples))
            lines.extend(
                [
                    f"### {validation_name}",
                    "",
                    f"*{count:,} rows failed*",
                    "",
                ]
            )
            if samples:
                lines.extend(self._render_sample_data_table(samples, max_rows=3))
            lines.append("")

        if node.failed_rows_truncated:
            lines.append(
                f"*Note: Sample data truncated. Affected validations: {', '.join(node.truncated_validations)}*"
            )
            lines.append("")

        return lines

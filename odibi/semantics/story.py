"""
Semantic Story Generation
=========================

Generate execution stories for semantic layer view creation.

Stories capture:
- View execution metadata (success/failure, duration, SQL)
- Graph data for lineage visualization
- HTML and JSON outputs for documentation
"""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from odibi.semantics.metrics import SemanticLayerConfig
from odibi.semantics.views import ViewExecutionResult, ViewGenerator
from odibi.utils.logging_context import get_logging_context


@dataclass
class ViewExecutionMetadata:
    """
    Metadata for a single view execution.

    Captures execution details for documentation and debugging.
    """

    view_name: str
    source_table: str
    status: str  # "success", "failed"
    duration: float
    sql_generated: str
    sql_file_path: Optional[str] = None
    error_message: Optional[str] = None
    row_count: Optional[int] = None
    metrics_included: List[str] = field(default_factory=list)
    dimensions_included: List[str] = field(default_factory=list)


@dataclass
class GraphNode:
    """Node in the lineage graph."""

    id: str
    type: str  # "table", "view"
    layer: str  # "gold", "semantic"


@dataclass
class GraphEdge:
    """Edge in the lineage graph."""

    from_node: str
    to_node: str


@dataclass
class SemanticStoryMetadata:
    """
    Complete metadata for a semantic layer execution story.

    Contains all information needed to generate story outputs.
    """

    name: str
    started_at: str
    completed_at: str
    duration: float
    views: List[ViewExecutionMetadata]
    views_created: int
    views_failed: int
    sql_files_saved: List[str]
    graph_data: Dict[str, Any]
    pipeline_layer: str = "semantic"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "pipeline_layer": self.pipeline_layer,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration": self.duration,
            "views": [asdict(v) for v in self.views],
            "views_created": self.views_created,
            "views_failed": self.views_failed,
            "sql_files_saved": self.sql_files_saved,
            "graph_data": self.graph_data,
        }


class SemanticStoryGenerator:
    """
    Generate execution stories for semantic layer operations.

    Creates both JSON and HTML outputs documenting:
    - View creation status and timing
    - Generated SQL for each view
    - Lineage graph connecting source tables to views
    """

    def __init__(
        self,
        config: SemanticLayerConfig,
        name: str = "semantic_layer",
        output_path: str = "stories/",
        storage_options: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize story generator.

        Args:
            config: Semantic layer configuration
            name: Name for this semantic layer execution
            output_path: Directory for story output
            storage_options: Credentials for remote storage
        """
        self.config = config
        self.name = name
        self.output_path_str = output_path
        self.is_remote = "://" in output_path
        self.storage_options = storage_options or {}

        self._view_generator = ViewGenerator(config)
        self._metadata: Optional[SemanticStoryMetadata] = None

        if not self.is_remote:
            self.output_path = Path(output_path)
        else:
            self.output_path = None

    def execute_with_story(
        self,
        execute_sql: Callable[[str], None],
        save_sql_to: Optional[str] = None,
        write_file: Optional[Callable[[str, str], None]] = None,
    ) -> SemanticStoryMetadata:
        """
        Execute all views and generate a story.

        Args:
            execute_sql: Callable that executes SQL against the database
            save_sql_to: Optional path to save SQL files
            write_file: Optional callable to write files

        Returns:
            SemanticStoryMetadata with execution details
        """
        ctx = get_logging_context()
        started_at = datetime.now()
        ctx.info("Starting semantic layer execution with story", name=self.name)

        execution_result = self._view_generator.execute_all_views(
            execute_sql=execute_sql,
            save_sql_to=save_sql_to,
            write_file=write_file,
        )

        completed_at = datetime.now()
        duration = (completed_at - started_at).total_seconds()

        view_metadata = self._build_view_metadata(execution_result)
        graph_data = self._build_graph_data(execution_result)

        self._metadata = SemanticStoryMetadata(
            name=self.name,
            started_at=started_at.isoformat(),
            completed_at=completed_at.isoformat(),
            duration=duration,
            views=view_metadata,
            views_created=len(execution_result.views_created),
            views_failed=len(execution_result.errors),
            sql_files_saved=execution_result.sql_files_saved,
            graph_data=graph_data,
        )

        ctx.info(
            "Semantic layer execution complete",
            views_created=self._metadata.views_created,
            views_failed=self._metadata.views_failed,
            duration=duration,
        )

        return self._metadata

    def _build_view_metadata(
        self, execution_result: ViewExecutionResult
    ) -> List[ViewExecutionMetadata]:
        """Build metadata for each view execution."""
        view_metadata = []

        for result in execution_result.results:
            view_config = self._view_generator.get_view(result.name)
            source_table = ""
            if view_config:
                try:
                    source_table = self._view_generator._get_source_table(view_config)
                except ValueError:
                    source_table = "unknown"

            metadata = ViewExecutionMetadata(
                view_name=result.name,
                source_table=source_table,
                status="success" if result.success else "failed",
                duration=0.0,
                sql_generated=result.sql,
                sql_file_path=result.sql_file_path,
                error_message=result.error,
                metrics_included=view_config.metrics if view_config else [],
                dimensions_included=view_config.dimensions if view_config else [],
            )
            view_metadata.append(metadata)

        return view_metadata

    def _build_graph_data(self, execution_result: ViewExecutionResult) -> Dict[str, Any]:
        """Build lineage graph data."""
        nodes = []
        edges = []
        seen_sources = set()

        for result in execution_result.results:
            view_config = self._view_generator.get_view(result.name)
            if not view_config:
                continue

            nodes.append(
                {
                    "id": result.name,
                    "type": "view",
                    "layer": "semantic",
                }
            )

            try:
                source_table = self._view_generator._get_source_table(view_config)
                if source_table not in seen_sources:
                    # Source tables are inputs - mark as type "source"
                    # Layer will be inferred by lineage stitcher from matching nodes
                    nodes.append(
                        {
                            "id": source_table,
                            "type": "source",
                        }
                    )
                    seen_sources.add(source_table)

                edges.append(
                    {
                        "from": source_table,
                        "to": result.name,
                    }
                )
            except ValueError:
                pass

        return {"nodes": nodes, "edges": edges}

    def save_story(
        self,
        write_file: Optional[Callable[[str, str], None]] = None,
    ) -> Dict[str, str]:
        """
        Save story as JSON and HTML files.

        Args:
            write_file: Optional callable to write files (for remote storage)

        Returns:
            Dict with paths to saved files
        """
        if not self._metadata:
            raise ValueError("No story metadata. Call execute_with_story first.")

        ctx = get_logging_context()
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("run_%H-%M-%S")

        if self.is_remote:
            base_path = f"{self.output_path_str.rstrip('/')}/{self.name}/{date_str}"
        else:
            base_path = self.output_path / self.name / date_str
            base_path = str(base_path)

        json_path = f"{base_path}/{time_str}.json"
        html_path = f"{base_path}/{time_str}.html"

        json_content = self.render_json()
        html_content = self.render_html()

        if write_file:
            write_file(json_path, json_content)
            write_file(html_path, html_content)
        elif not self.is_remote:
            Path(base_path).mkdir(parents=True, exist_ok=True)
            Path(json_path).write_text(json_content, encoding="utf-8")
            Path(html_path).write_text(html_content, encoding="utf-8")

        ctx.info("Story saved", json_path=json_path, html_path=html_path)

        return {"json": json_path, "html": html_path}

    def render_json(self) -> str:
        """Render story as JSON string."""
        if not self._metadata:
            raise ValueError("No story metadata. Call execute_with_story first.")
        return json.dumps(self._metadata.to_dict(), indent=2)

    def render_html(self) -> str:
        """Render story as HTML string."""
        if not self._metadata:
            raise ValueError("No story metadata. Call execute_with_story first.")

        meta = self._metadata

        status_class = "success" if meta.views_failed == 0 else "warning"
        status_text = "Success" if meta.views_failed == 0 else "Partial Failure"

        views_html = []
        for view in meta.views:
            view_status = "✅" if view.status == "success" else "❌"
            error_html = ""
            if view.error_message:
                error_html = f'<div class="error">{view.error_message}</div>'

            sql_html = f"<pre><code>{self._escape_html(view.sql_generated)}</code></pre>"

            views_html.append(f"""
            <div class="view-card {view.status}">
                <h3>{view_status} {view.view_name}</h3>
                <div class="view-details">
                    <p><strong>Source:</strong> {view.source_table}</p>
                    <p><strong>Metrics:</strong> {", ".join(view.metrics_included)}</p>
                    <p><strong>Dimensions:</strong> {", ".join(view.dimensions_included)}</p>
                    {error_html}
                </div>
                <details>
                    <summary>Generated SQL</summary>
                    {sql_html}
                </details>
            </div>
            """)

        mermaid_code = self._generate_mermaid_diagram()

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Semantic Layer Story: {meta.name}</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <style>
        :root {{
            --primary: #2563eb;
            --success: #16a34a;
            --warning: #dc2626;
            --bg: #f8fafc;
            --card-bg: #ffffff;
            --text: #1e293b;
            --border: #e2e8f0;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg);
            color: var(--text);
            margin: 0;
            padding: 20px;
            line-height: 1.6;
        }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        h1 {{ color: var(--primary); margin-bottom: 0; }}
        .subtitle {{ color: #64748b; margin-top: 5px; }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .stat {{
            background: var(--card-bg);
            padding: 15px;
            border-radius: 8px;
            border: 1px solid var(--border);
            text-align: center;
        }}
        .stat-value {{ font-size: 24px; font-weight: bold; color: var(--primary); }}
        .stat-label {{ font-size: 12px; color: #64748b; text-transform: uppercase; }}
        .status-badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 14px;
            font-weight: 500;
        }}
        .status-badge.success {{ background: #dcfce7; color: var(--success); }}
        .status-badge.warning {{ background: #fee2e2; color: var(--warning); }}
        .view-card {{
            background: var(--card-bg);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 15px;
            margin: 15px 0;
        }}
        .view-card.success {{ border-left: 4px solid var(--success); }}
        .view-card.failed {{ border-left: 4px solid var(--warning); }}
        .view-card h3 {{ margin: 0 0 10px 0; }}
        .view-details p {{ margin: 5px 0; }}
        .error {{ background: #fee2e2; color: var(--warning); padding: 10px; border-radius: 4px; margin-top: 10px; }}
        details {{ margin-top: 10px; }}
        summary {{ cursor: pointer; font-weight: 500; color: var(--primary); }}
        pre {{
            background: #1e293b;
            color: #e2e8f0;
            padding: 15px;
            border-radius: 8px;
            overflow-x: auto;
            font-size: 13px;
        }}
        .lineage {{ background: var(--card-bg); padding: 20px; border-radius: 8px; border: 1px solid var(--border); margin: 20px 0; }}
        .mermaid {{ text-align: center; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🔷 {meta.name}</h1>
        <p class="subtitle">Semantic Layer Execution Story</p>

        <div class="summary">
            <div class="stat">
                <div class="stat-value">{meta.views_created}</div>
                <div class="stat-label">Views Created</div>
            </div>
            <div class="stat">
                <div class="stat-value">{meta.views_failed}</div>
                <div class="stat-label">Failed</div>
            </div>
            <div class="stat">
                <div class="stat-value">{meta.duration:.2f}s</div>
                <div class="stat-label">Duration</div>
            </div>
            <div class="stat">
                <span class="status-badge {status_class}">{status_text}</span>
            </div>
        </div>

        <h2>📊 Lineage</h2>
        <div class="lineage">
            <div class="mermaid">
{mermaid_code}
            </div>
        </div>

        <h2>📋 Views</h2>
        {"".join(views_html)}

        <footer style="text-align: center; color: #94a3b8; margin-top: 40px; font-size: 12px;">
            Generated: {meta.completed_at} | Duration: {meta.duration:.2f}s
        </footer>
    </div>
    <script>mermaid.initialize({{startOnLoad: true, theme: 'neutral'}});</script>
</body>
</html>"""

        return html

    def _generate_mermaid_diagram(self) -> str:
        """Generate Mermaid flowchart from graph data."""
        if not self._metadata:
            return "graph LR\n    A[No Data]"

        lines = ["graph LR"]

        for node in self._metadata.graph_data.get("nodes", []):
            node_id = node["id"].replace(".", "_").replace("-", "_")
            if node["type"] == "table":
                lines.append(f'    {node_id}[("{node["id"]}")]')
            else:
                lines.append(f'    {node_id}["{node["id"]}"]')

        for edge in self._metadata.graph_data.get("edges", []):
            from_id = edge["from"].replace(".", "_").replace("-", "_")
            to_id = edge["to"].replace(".", "_").replace("-", "_")
            lines.append(f"    {from_id} --> {to_id}")

        return "\n".join(lines)

    def _escape_html(self, text: str) -> str:
        """Escape HTML special characters."""
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )

    @property
    def metadata(self) -> Optional[SemanticStoryMetadata]:
        """Get the last generated metadata."""
        return self._metadata

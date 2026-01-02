"""
Lineage Stitcher
================

Generates end-to-end lineage by stitching graph_data from multiple pipeline stories.

This module reads story JSON files from a pipeline run date and combines their
lineage graphs into a unified view showing data flow from raw → bronze → silver
→ gold → semantic layers.

Features:
- Read stories from multiple pipelines for a given date
- Stitch graph_data (nodes + edges) into combined lineage
- Generate lineage JSON with all nodes/edges and story links
- Generate interactive HTML with Mermaid diagram
"""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from odibi.utils.logging_context import get_logging_context


@dataclass
class LayerInfo:
    """Information about a single layer's story."""

    name: str
    story_path: str
    status: str
    duration: float
    pipeline_layer: Optional[str] = None


@dataclass
class LineageNode:
    """Node in the combined lineage graph."""

    id: str
    type: str
    layer: str

    def to_dict(self) -> Dict[str, Any]:
        return {"id": self.id, "type": self.type, "layer": self.layer}


@dataclass
class LineageEdge:
    """Edge in the combined lineage graph."""

    from_node: str
    to_node: str

    def to_dict(self) -> Dict[str, Any]:
        return {"from": self.from_node, "to": self.to_node}


@dataclass
class LineageResult:
    """Result of lineage generation."""

    generated_at: str
    date: str
    layers: List[LayerInfo]
    nodes: List[LineageNode]
    edges: List[LineageEdge]
    json_path: Optional[str] = None
    html_path: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "date": self.date,
            "layers": [
                {
                    "name": layer.name,
                    "story_path": layer.story_path,
                    "status": layer.status,
                    "duration": layer.duration,
                    "pipeline_layer": layer.pipeline_layer,
                }
                for layer in self.layers
            ],
            "nodes": [node.to_dict() for node in self.nodes],
            "edges": [edge.to_dict() for edge in self.edges],
        }


class LineageGenerator:
    """
    Generate combined lineage from multiple pipeline stories.

    Reads all story JSON files for a given date, extracts their graph_data,
    and stitches them into a unified lineage view.

    Example:
        ```python
        generator = LineageGenerator(stories_path="stories/")
        result = generator.generate(date="2025-01-02")
        generator.save(result)
        ```
    """

    LAYER_ORDER = ["raw", "bronze", "silver", "gold", "semantic"]

    def __init__(
        self,
        stories_path: str,
        storage_options: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize lineage generator.

        Args:
            stories_path: Base path for story files (local or remote)
            storage_options: Credentials for remote storage (e.g., ADLS)
        """
        self.stories_path = stories_path
        self.storage_options = storage_options or {}
        self.is_remote = "://" in stories_path
        self._result: Optional[LineageResult] = None

    def generate(self, date: Optional[str] = None) -> LineageResult:
        """
        Generate lineage from all stories for a given date.

        Args:
            date: Date string (YYYY-MM-DD), defaults to today

        Returns:
            LineageResult with combined graph and links to stories
        """
        ctx = get_logging_context()

        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        ctx.info("Generating lineage", date=date, stories_path=self.stories_path)

        story_files = self._find_story_files(date)
        ctx.debug("Found story files", count=len(story_files))

        layers: List[LayerInfo] = []
        all_nodes: Dict[str, LineageNode] = {}
        all_edges: List[LineageEdge] = []
        edge_set: set = set()

        for story_path in story_files:
            story_data = self._load_story(story_path)
            if story_data is None:
                continue

            layer_info = self._extract_layer_info(story_data, story_path)
            layers.append(layer_info)

            graph_data = story_data.get("graph_data", {})
            nodes_data = graph_data.get("nodes", [])
            edges_data = graph_data.get("edges", [])

            for node_data in nodes_data:
                node_id = node_data.get("id", "")
                if node_id and node_id not in all_nodes:
                    all_nodes[node_id] = LineageNode(
                        id=node_id,
                        type=node_data.get("type", "table"),
                        layer=node_data.get("layer", self._infer_layer(node_id)),
                    )

            for edge_data in edges_data:
                from_node = edge_data.get("from", "")
                to_node = edge_data.get("to", "")
                edge_key = (from_node, to_node)
                if from_node and to_node and edge_key not in edge_set:
                    all_edges.append(LineageEdge(from_node=from_node, to_node=to_node))
                    edge_set.add(edge_key)

        layers.sort(key=lambda x: self._layer_sort_key(x.pipeline_layer or x.name))
        nodes_list = sorted(
            all_nodes.values(),
            key=lambda x: (self._layer_sort_key(x.layer), x.id),
        )

        self._result = LineageResult(
            generated_at=datetime.now().isoformat(),
            date=date,
            layers=layers,
            nodes=nodes_list,
            edges=all_edges,
        )

        ctx.info(
            "Lineage generated",
            layers=len(layers),
            nodes=len(nodes_list),
            edges=len(all_edges),
        )

        return self._result

    def save(
        self,
        result: Optional[LineageResult] = None,
        write_file: Optional[Callable[[str, str], None]] = None,
    ) -> Dict[str, str]:
        """
        Save lineage as JSON and HTML files.

        Args:
            result: LineageResult to save (uses last generated if not provided)
            write_file: Optional callable to write files (for remote storage)

        Returns:
            Dict with paths to saved files
        """
        if result is None:
            result = self._result

        if result is None:
            raise ValueError("No lineage result. Call generate() first.")

        ctx = get_logging_context()

        if self.is_remote:
            base_path = f"{self.stories_path.rstrip('/')}/lineage"
        else:
            base_path = Path(self.stories_path) / "lineage"
            base_path.mkdir(parents=True, exist_ok=True)
            base_path = str(base_path)

        json_path = f"{base_path}/{result.date}.json"
        html_path = f"{base_path}/{result.date}.html"

        json_content = self.render_json(result)
        html_content = self.render_html(result)

        if write_file:
            write_file(json_path, json_content)
            write_file(html_path, html_content)
        elif not self.is_remote:
            Path(json_path).write_text(json_content, encoding="utf-8")
            Path(html_path).write_text(html_content, encoding="utf-8")

        result.json_path = json_path
        result.html_path = html_path

        ctx.info("Lineage saved", json_path=json_path, html_path=html_path)

        return {"json": json_path, "html": html_path}

    def render_json(self, result: Optional[LineageResult] = None) -> str:
        """Render lineage as JSON string."""
        if result is None:
            result = self._result
        if result is None:
            raise ValueError("No lineage result. Call generate() first.")
        return json.dumps(result.to_dict(), indent=2)

    def render_html(self, result: Optional[LineageResult] = None) -> str:
        """Render lineage as interactive HTML with Mermaid diagram."""
        if result is None:
            result = self._result
        if result is None:
            raise ValueError("No lineage result. Call generate() first.")

        mermaid_code = self._generate_mermaid_diagram(result)
        layers_html = self._generate_layers_table(result)

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Lineage: {result.date}</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <style>
        :root {{
            --primary: #2563eb;
            --success: #16a34a;
            --warning: #dc2626;
            --bronze: #cd7f32;
            --silver: #c0c0c0;
            --gold: #ffd700;
            --semantic: #9333ea;
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
        .container {{ max-width: 1400px; margin: 0 auto; }}
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
        .lineage {{
            background: var(--card-bg);
            padding: 20px;
            border-radius: 8px;
            border: 1px solid var(--border);
            margin: 20px 0;
            overflow-x: auto;
        }}
        .mermaid {{ text-align: center; min-height: 200px; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            background: var(--card-bg);
            border-radius: 8px;
            overflow: hidden;
        }}
        th, td {{
            padding: 12px 16px;
            text-align: left;
            border-bottom: 1px solid var(--border);
        }}
        th {{
            background: #f1f5f9;
            font-weight: 600;
            color: #475569;
        }}
        tr:hover {{ background: #f8fafc; }}
        a {{ color: var(--primary); text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .status-badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 500;
        }}
        .status-badge.success {{ background: #dcfce7; color: var(--success); }}
        .status-badge.failed {{ background: #fee2e2; color: var(--warning); }}
        .layer-badge {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
        }}
        .layer-bronze {{ background: #fef3c7; color: #92400e; }}
        .layer-silver {{ background: #f1f5f9; color: #475569; }}
        .layer-gold {{ background: #fef9c3; color: #854d0e; }}
        .layer-semantic {{ background: #f3e8ff; color: #7c3aed; }}
        .legend {{
            display: flex;
            gap: 20px;
            flex-wrap: wrap;
            margin-bottom: 15px;
            padding: 10px;
            background: #f8fafc;
            border-radius: 8px;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 13px;
        }}
        .legend-color {{
            width: 16px;
            height: 16px;
            border-radius: 4px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🔗 Data Lineage</h1>
        <p class="subtitle">End-to-end data flow for {result.date}</p>

        <div class="summary">
            <div class="stat">
                <div class="stat-value">{len(result.layers)}</div>
                <div class="stat-label">Layers</div>
            </div>
            <div class="stat">
                <div class="stat-value">{len(result.nodes)}</div>
                <div class="stat-label">Nodes</div>
            </div>
            <div class="stat">
                <div class="stat-value">{len(result.edges)}</div>
                <div class="stat-label">Edges</div>
            </div>
            <div class="stat">
                <div class="stat-value">{sum(1 for layer in result.layers if layer.status == "success")}/{len(result.layers)}</div>
                <div class="stat-label">Successful</div>
            </div>
        </div>

        <h2>📊 Lineage Graph</h2>
        <div class="lineage">
            <div class="legend">
                <div class="legend-item">
                    <div class="legend-color" style="background: #f59e0b;"></div>
                    <span>Bronze (Raw Ingestion)</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #6b7280;"></div>
                    <span>Silver (Cleaned)</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #eab308;"></div>
                    <span>Gold (Aggregated)</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #8b5cf6;"></div>
                    <span>Semantic (Views)</span>
                </div>
            </div>
            <div class="mermaid">
{mermaid_code}
            </div>
        </div>

        <h2>📋 Pipeline Layers</h2>
        {layers_html}

        <footer style="text-align: center; color: #94a3b8; margin-top: 40px; font-size: 12px;">
            Generated: {result.generated_at}
        </footer>
    </div>
    <script>
        mermaid.initialize({{
            startOnLoad: true,
            theme: 'neutral',
            flowchart: {{
                useMaxWidth: true,
                htmlLabels: true,
                curve: 'basis'
            }}
        }});
    </script>
</body>
</html>"""

        return html

    def _find_story_files(self, date: str) -> List[str]:
        """Find the latest story JSON file per pipeline for the given date.

        If a pipeline ran multiple times on the same date, only the most recent
        run (by filename timestamp) is included in the lineage.
        """
        ctx = get_logging_context()

        if self.is_remote:
            return self._find_remote_story_files(date)

        story_files = []
        stories_path = Path(self.stories_path)

        if not stories_path.exists():
            ctx.warning("Stories path does not exist", path=str(stories_path))
            return []

        for pipeline_dir in stories_path.iterdir():
            if not pipeline_dir.is_dir():
                continue
            if pipeline_dir.name in ("lineage", "__pycache__"):
                continue

            date_dir = pipeline_dir / date
            if not date_dir.exists():
                continue

            json_files = sorted(date_dir.glob("*.json"), reverse=True)
            if json_files:
                story_files.append(str(json_files[0]))
                ctx.debug(
                    "Selected latest story for pipeline",
                    pipeline=pipeline_dir.name,
                    file=json_files[0].name,
                    total_runs=len(json_files),
                )

        return story_files

    def _find_remote_story_files(self, date: str) -> List[str]:
        """Find the latest story file per pipeline in remote storage."""
        ctx = get_logging_context()

        try:
            import fsspec

            fs, path_prefix = fsspec.core.url_to_fs(self.stories_path, **self.storage_options)

            if not fs.exists(path_prefix):
                ctx.warning("Remote stories path does not exist", path=self.stories_path)
                return []

            story_files = []
            for item in fs.ls(path_prefix, detail=False):
                if fs.isdir(item) and not item.endswith(("lineage", "__pycache__")):
                    date_path = f"{item}/{date}"
                    if fs.exists(date_path):
                        json_files = sorted(
                            [f for f in fs.ls(date_path, detail=False) if f.endswith(".json")],
                            reverse=True,
                        )
                        if json_files:
                            story_files.append(json_files[0])

            return story_files

        except ImportError:
            ctx.error("fsspec not available for remote storage")
            return []
        except Exception as e:
            ctx.error(f"Error finding remote story files: {e}")
            return []

    def _load_story(self, story_path: str) -> Optional[Dict[str, Any]]:
        """Load a story JSON file."""
        ctx = get_logging_context()

        try:
            if self.is_remote:
                import fsspec

                fs, _ = fsspec.core.url_to_fs(story_path, **self.storage_options)
                with fs.open(story_path, "r") as f:
                    return json.load(f)
            else:
                with open(story_path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            ctx.warning(f"Failed to load story: {story_path}", error=str(e))
            return None

    def _extract_layer_info(self, story_data: Dict[str, Any], story_path: str) -> LayerInfo:
        """Extract layer info from story data."""
        name = story_data.get("pipeline_name") or story_data.get("name", "unknown")
        pipeline_layer = story_data.get("pipeline_layer")

        completed_nodes = story_data.get("completed_nodes", 0)
        failed_nodes = story_data.get("failed_nodes", 0)
        views_created = story_data.get("views_created", 0)
        views_failed = story_data.get("views_failed", 0)

        if failed_nodes > 0 or views_failed > 0:
            status = "failed"
        elif completed_nodes > 0 or views_created > 0:
            status = "success"
        else:
            status = "unknown"

        duration = story_data.get("duration", 0.0)

        relative_path = story_path
        if not self.is_remote:
            try:
                relative_path = str(Path(story_path).relative_to(Path(self.stories_path)))
            except ValueError:
                pass
        relative_path = relative_path.replace(".json", ".html")

        return LayerInfo(
            name=name,
            story_path=relative_path,
            status=status,
            duration=duration,
            pipeline_layer=pipeline_layer,
        )

    def _infer_layer(self, node_id: str) -> str:
        """Infer layer from node ID."""
        node_lower = node_id.lower()
        if "raw" in node_lower:
            return "raw"
        elif "bronze" in node_lower:
            return "bronze"
        elif "silver" in node_lower:
            return "silver"
        elif "gold" in node_lower:
            return "gold"
        elif node_lower.startswith("vw_") or "semantic" in node_lower:
            return "semantic"
        else:
            return "unknown"

    def _layer_sort_key(self, layer: str) -> int:
        """Get sort key for layer ordering."""
        layer_lower = layer.lower() if layer else ""
        for idx, layer_name in enumerate(self.LAYER_ORDER):
            if layer_name in layer_lower:
                return idx
        return len(self.LAYER_ORDER)

    def _generate_mermaid_diagram(self, result: LineageResult) -> str:
        """Generate Mermaid flowchart from lineage result."""
        lines = ["graph LR"]

        layer_styles = {
            "raw": "fill:#fef3c7,stroke:#f59e0b,color:#92400e",
            "bronze": "fill:#fef3c7,stroke:#f59e0b,color:#92400e",
            "silver": "fill:#f1f5f9,stroke:#6b7280,color:#374151",
            "gold": "fill:#fef9c3,stroke:#eab308,color:#854d0e",
            "semantic": "fill:#f3e8ff,stroke:#8b5cf6,color:#6b21a8",
            "unknown": "fill:#f1f5f9,stroke:#94a3b8,color:#475569",
        }

        for node in result.nodes:
            node_id = self._sanitize_id(node.id)
            label = node.id

            if node.type == "view":
                lines.append(f'    {node_id}["{label}"]')
            else:
                lines.append(f'    {node_id}[("{label}")]')

        for edge in result.edges:
            from_id = self._sanitize_id(edge.from_node)
            to_id = self._sanitize_id(edge.to_node)
            lines.append(f"    {from_id} --> {to_id}")

        for layer, style in layer_styles.items():
            lines.append(f"    classDef {layer}Style {style}")

        for node in result.nodes:
            node_id = self._sanitize_id(node.id)
            layer = node.layer if node.layer in layer_styles else "unknown"
            lines.append(f"    class {node_id} {layer}Style")

        return "\n".join(lines)

    def _generate_layers_table(self, result: LineageResult) -> str:
        """Generate HTML table for layers."""
        if not result.layers:
            return "<p>No pipeline layers found for this date.</p>"

        rows = []
        for layer in result.layers:
            status_class = "success" if layer.status == "success" else "failed"
            layer_class = self._get_layer_class(layer.pipeline_layer or layer.name)

            rows.append(
                f"""
            <tr>
                <td>{layer.name}</td>
                <td><span class="layer-badge {layer_class}">{layer.pipeline_layer or "-"}</span></td>
                <td><span class="status-badge {status_class}">{layer.status}</span></td>
                <td>{layer.duration:.2f}s</td>
            </tr>
            """
            )

        return f"""
        <table>
            <thead>
                <tr>
                    <th>Pipeline</th>
                    <th>Layer</th>
                    <th>Status</th>
                    <th>Duration</th>
                </tr>
            </thead>
            <tbody>
                {"".join(rows)}
            </tbody>
        </table>
        """

    def _get_layer_class(self, layer: str) -> str:
        """Get CSS class for layer badge."""
        if not layer:
            return ""
        layer_lower = layer.lower()
        if "bronze" in layer_lower:
            return "layer-bronze"
        elif "silver" in layer_lower:
            return "layer-silver"
        elif "gold" in layer_lower:
            return "layer-gold"
        elif "semantic" in layer_lower:
            return "layer-semantic"
        return ""

    def _sanitize_id(self, node_id: str) -> str:
        """Sanitize node ID for Mermaid compatibility."""
        return node_id.replace(".", "_").replace("-", "_").replace(" ", "_")

    @property
    def result(self) -> Optional[LineageResult]:
        """Get the last generated lineage result."""
        return self._result

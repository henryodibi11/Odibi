"""
Documentation Story Generator
==============================

Generates stakeholder-ready documentation from pipeline configurations.
Automatically extracts explanations from registered operations.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

import odibi
from odibi.config import NodeConfig, PipelineConfig, ProjectConfig
from odibi.validation import ExplanationLinter


class DocStoryGenerator:
    """
    Generates documentation stories for pipelines.

    Creates stakeholder-ready documentation by:
    - Extracting operation explanations from the registry
    - Building context from pipeline/project config
    - Validating explanation quality
    - Rendering to HTML or Markdown
    """

    def __init__(
        self,
        pipeline_config: PipelineConfig,
        project_config: Optional[ProjectConfig] = None,
    ):
        """
        Initialize doc story generator.

        Args:
            pipeline_config: Pipeline configuration
            project_config: Optional project configuration for context
        """
        self.pipeline_config = pipeline_config
        self.project_config = project_config
        self.registry = None  # Registry logic removed/deprecated in cleanup
        self.linter = ExplanationLinter()

    def generate(
        self,
        output_path: str,
        format: str = "html",
        validate: bool = True,
        include_flow_diagram: bool = True,
        theme=None,
    ) -> str:
        """
        Generate documentation story.

        Args:
            output_path: Path to save documentation
            format: Output format ("html" or "markdown")
            validate: Whether to validate explanation quality
            include_flow_diagram: Whether to include flow diagram
            theme: StoryTheme instance for HTML rendering (optional)

        Returns:
            Path to generated documentation file

        Raises:
            ValueError: If validation fails and validate=True
        """
        # Build documentation structure
        doc_data = {
            "title": self._generate_title(),
            "overview": self._generate_overview(),
            "operations": self._generate_operation_details(validate),
            "expected_outputs": self._generate_outputs(),
        }

        if include_flow_diagram:
            doc_data["flow_diagram"] = self._generate_flow_diagram()

        # Render to requested format
        if format.lower() == "html":
            return self._render_html(doc_data, output_path, theme)
        elif format.lower() in ["markdown", "md"]:
            return self._render_markdown(doc_data, output_path)
        elif format.lower() == "json":
            return self._render_json(doc_data, output_path)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def _generate_title(self) -> str:
        """Generate documentation title."""
        title = f"Pipeline Documentation: {self.pipeline_config.pipeline}"

        if self.project_config:
            if self.project_config.project:
                title = f"{self.project_config.project} - {title}"

        return title

    def _generate_overview(self) -> Dict[str, Any]:
        """Generate pipeline overview section."""
        overview = {
            "pipeline_name": self.pipeline_config.pipeline,
            "description": self.pipeline_config.description or "No description provided",
            "total_nodes": len(self.pipeline_config.nodes),
        }

        # Add project context if available
        if self.project_config:
            overview["project"] = self.project_config.project
            overview["plant"] = getattr(self.project_config, "plant", None)
            overview["asset"] = getattr(self.project_config, "asset", None)
            overview["business_unit"] = getattr(self.project_config, "business_unit", None)

        # Add layer info if available
        if hasattr(self.pipeline_config, "layer"):
            overview["layer"] = self.pipeline_config.layer

        return overview

    def _generate_operation_details(self, validate: bool) -> List[Dict[str, Any]]:
        """
        Generate detailed operation explanations.

        Args:
            validate: Whether to validate explanation quality

        Returns:
            List of operation details with explanations
        """
        operations = []

        for node in self.pipeline_config.nodes:
            operation_detail = {
                "node_name": node.name,
                "operation_name": self._get_operation_name(node),
                "params": {},
                "explanation": self._get_node_description(node),
            }

            operations.append(operation_detail)

        return operations

    def _get_operation_name(self, node: NodeConfig) -> str:
        """Get operation name from node config."""
        if node.read:
            return "read"
        elif node.transform:
            return (
                f"transform ({node.transform.operation})"
                if hasattr(node.transform, "operation")
                else "transform"
            )
        elif node.write:
            return "write"
        return "unknown"

    def _get_node_description(self, node: NodeConfig) -> str:
        """Get description for node."""
        if node.description:
            return node.description

        # Build basic description from operations
        desc_parts = []
        if node.read:
            desc_parts.append(f"Reads from `{node.read.connection}`")
        if node.transform:
            if hasattr(node.transform, "operation"):
                desc_parts.append(f"Transforms using `{node.transform.operation}`")
            else:
                desc_parts.append("Transforms data")
        if node.write:
            desc_parts.append(f"Writes to `{node.write.connection}`")

        return " → ".join(desc_parts) if desc_parts else "No description available"

    def _build_context(self, node: NodeConfig) -> Dict[str, Any]:
        """
        Build context dictionary for explanation.

        Args:
            node: Node configuration

        Returns:
            Context dictionary
        """
        context = {
            "node": node.name,
            "operation": self._get_operation_name(node),
            "pipeline": self.pipeline_config.pipeline,
        }

        # Add project-level context if available
        if self.project_config:
            context["project"] = self.project_config.project
            context["plant"] = getattr(self.project_config, "plant", None)
            context["asset"] = getattr(self.project_config, "asset", None)
            context["business_unit"] = getattr(self.project_config, "business_unit", None)

        return context

    def _generate_outputs(self) -> Dict[str, Any]:
        """Generate expected outputs section."""
        # Get final nodes (nodes with no dependents)
        all_dependencies = set()
        for node in self.pipeline_config.nodes:
            if node.depends_on:
                all_dependencies.update(node.depends_on)

        final_nodes = [
            node.name for node in self.pipeline_config.nodes if node.name not in all_dependencies
        ]

        return {
            "final_nodes": final_nodes,
            "description": f"This pipeline produces {len(final_nodes)} final output(s)",
        }

    def _generate_flow_diagram(self) -> str:
        """
        Generate ASCII flow diagram.

        Returns:
            ASCII art flow diagram
        """
        lines = []
        lines.append("Pipeline Flow:")
        lines.append("")

        for i, node in enumerate(self.pipeline_config.nodes):
            # Node representation
            lines.append(f"{i + 1}. [{node.name}]")
            lines.append(f"   Operation: {self._get_operation_name(node)}")

            if node.depends_on:
                lines.append(f"   Depends on: {', '.join(node.depends_on)}")

            lines.append("")

        return "\n".join(lines)

    def _render_html(self, doc_data: Dict[str, Any], output_path: str, theme=None) -> str:
        """
        Render documentation as HTML.

        Args:
            doc_data: Documentation data
            output_path: Output file path
            theme: Optional StoryTheme for customization

        Returns:
            Path to generated file
        """
        try:
            from jinja2 import Template
        except ImportError:
            raise ImportError(
                "jinja2 is required for HTML rendering. Install with: pip install jinja2"
            )

        # HTML template for doc story
        template_str = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ doc.title }}</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            line-height: 1.6;
            max-width: 1000px;
            margin: 0 auto;
            padding: 20px;
            color: #333;
        }
        h1 { color: #0066cc; border-bottom: 3px solid #0066cc; padding-bottom: 10px; }
        h2 { color: #0066cc; margin-top: 30px; }
        h3 { color: #555; }
        .overview { background: #f5f5f5; padding: 20px; border-radius: 8px; margin: 20px 0; }
        .overview-item { margin: 10px 0; }
        .operation-card {
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 20px;
            margin: 20px 0;
            background: white;
        }
        .operation-header {
            background: #0066cc;
            color: white;
            padding: 10px 15px;
            border-radius: 4px;
            margin: -20px -20px 15px -20px;
        }
        .params {
            background: #f9f9f9;
            padding: 10px;
            border-left: 3px solid #0066cc;
            margin: 10px 0;
        }
        .explanation { margin: 15px 0; }
        pre {
            background: #f5f5f5;
            padding: 15px;
            border-radius: 4px;
            overflow-x: auto;
        }
        code {
            background: #f5f5f5;
            padding: 2px 6px;
            border-radius: 3px;
        }
        .flow-diagram {
            background: #f9f9f9;
            padding: 20px;
            border-radius: 8px;
            font-family: monospace;
            white-space: pre;
        }
    </style>
</head>
<body>
    <!-- Nigerian accent bar -->
    <div style="height: 4px; background: linear-gradient(to right, #008751 33%, #fff 33%, #fff 66%, #008751 66%); margin-bottom: 15px; border-radius: 2px;"></div>

    <p style="color: #008751; font-size: 0.85em; margin: 0 0 5px 0; font-weight: 500;">Ndewo — Welcome to your data story</p>
    <h1>{{ doc.title }}</h1>

    <div class="overview">
        <h2>Overview</h2>
        <div class="overview-item"><strong>Pipeline:</strong> {{ doc.overview.pipeline_name }}</div>
        <div class="overview-item"><strong>Description:</strong> {{ doc.overview.description }}</div>
        <div class="overview-item"><strong>Total Operations:</strong> {{ doc.overview.total_nodes }}</div>

        {% if doc.overview.project %}
        <div class="overview-item"><strong>Project:</strong> {{ doc.overview.project }}</div>
        {% endif %}

        {% if doc.overview.plant %}
        <div class="overview-item"><strong>Plant:</strong> {{ doc.overview.plant }}</div>
        {% endif %}

        {% if doc.overview.asset %}
        <div class="overview-item"><strong>Asset:</strong> {{ doc.overview.asset }}</div>
        {% endif %}

        {% if doc.overview.layer %}
        <div class="overview-item"><strong>Layer:</strong> {{ doc.overview.layer }}</div>
        {% endif %}
    </div>

    {% if doc.flow_diagram %}
    <h2>Pipeline Flow</h2>
    <div class="flow-diagram">{{ doc.flow_diagram }}</div>
    {% endif %}

    <h2>Operations</h2>

    {% for op in doc.operations %}
    <div class="operation-card">
        <div class="operation-header">
            <h3 style="margin: 0; color: white;">{{ op.node_name }}</h3>
            <div style="opacity: 0.9; font-size: 14px;">Operation: <code style="background: rgba(255,255,255,0.2);">{{ op.operation_name }}</code></div>
        </div>

        {% if op.params %}
        <div class="params">
            <strong>Parameters:</strong>
            <ul>
            {% for key, value in op.params.items() %}
                <li><code>{{ key }}</code>: {{ value }}</li>
            {% endfor %}
            </ul>
        </div>
        {% endif %}

        <div class="explanation">
            {% if op.explanation %}
                {{ op.explanation|safe }}
            {% elif op.explanation_error %}
                <p style="color: #dc3545;"><strong>Error generating explanation:</strong> {{ op.explanation_error }}</p>
            {% else %}
                <p style="color: #666;"><em>No explanation available</em></p>
            {% endif %}
        </div>
    </div>
    {% endfor %}

    <h2>Expected Outputs</h2>
    <p>{{ doc.expected_outputs.description }}</p>
    <ul>
    {% for node in doc.expected_outputs.final_nodes %}
        <li><strong>{{ node }}</strong></li>
    {% endfor %}
    </ul>

    <hr style="margin-top: 40px; border: none; border-top: 1px solid #ddd;">
    <p style="text-align: center; color: #666; font-size: 14px; font-style: italic;">
        "Where others saw gaps, I built bridges."
    </p>
    <p style="text-align: center; color: #888; font-size: 12px;">
        Odibi v{{ odibi_version }} · Henry Odibi
        <svg style="vertical-align: middle; margin-left: 4px;" width="20" height="14" viewBox="0 0 3 2">
            <rect width="1" height="2" x="0" fill="#008751"/>
            <rect width="1" height="2" x="1" fill="#ffffff"/>
            <rect width="1" height="2" x="2" fill="#008751"/>
        </svg>
    </p>
</body>
</html>
        """

        # Apply theme if provided
        if theme:
            theme_css = theme.to_css_string()
            # Replace default :root styles with theme
            template_str = template_str.replace(
                "body {\n            font-family: -apple-system",
                f"{theme_css}\n        body {{\n            font-family: var(--font-family)",
            )

        template = Template(template_str)
        html = template.render(doc=doc_data, theme=theme, odibi_version=odibi.__version__)

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html)

        return str(output_file)

    def _render_markdown(self, doc_data: Dict[str, Any], output_path: str) -> str:
        """
        Render documentation as Markdown.

        Args:
            doc_data: Documentation data
            output_path: Output file path

        Returns:
            Path to generated file
        """
        lines = []

        # Title
        lines.append(f"# {doc_data['title']}")
        lines.append("")

        # Overview
        lines.append("## Overview")
        lines.append("")
        overview = doc_data["overview"]
        lines.append(f"**Pipeline:** {overview['pipeline_name']}")
        lines.append(f"**Description:** {overview['description']}")
        lines.append(f"**Total Operations:** {overview['total_nodes']}")

        if overview.get("project"):
            lines.append(f"**Project:** {overview['project']}")
        if overview.get("plant"):
            lines.append(f"**Plant:** {overview['plant']}")
        if overview.get("asset"):
            lines.append(f"**Asset:** {overview['asset']}")
        if overview.get("layer"):
            lines.append(f"**Layer:** {overview['layer']}")

        lines.append("")
        lines.append("---")
        lines.append("")

        # Flow diagram
        if "flow_diagram" in doc_data:
            lines.append("## Pipeline Flow")
            lines.append("")
            lines.append("```")
            lines.append(doc_data["flow_diagram"])
            lines.append("```")
            lines.append("")

        # Operations
        lines.append("## Operations")
        lines.append("")

        for op in doc_data["operations"]:
            lines.append(f"### {op['node_name']}")
            lines.append("")
            lines.append(f"**Operation:** `{op['operation_name']}`")
            lines.append("")

            if op["params"]:
                lines.append("**Parameters:**")
                for key, value in op["params"].items():
                    lines.append(f"- `{key}`: {value}")
                lines.append("")

            if op.get("explanation"):
                lines.append(op["explanation"])
            elif op.get("explanation_error"):
                lines.append(f"*Error generating explanation: {op['explanation_error']}*")
            else:
                lines.append("*No explanation available*")

            lines.append("")
            lines.append("---")
            lines.append("")

        # Expected outputs
        lines.append("## Expected Outputs")
        lines.append("")
        lines.append(doc_data["expected_outputs"]["description"])
        lines.append("")
        for node in doc_data["expected_outputs"]["final_nodes"]:
            lines.append(f"- **{node}**")

        lines.append("")
        lines.append("---")
        lines.append("")
        lines.append('*"Where others saw gaps, I built bridges."*')
        lines.append("")
        lines.append(f"Odibi v{odibi.__version__} · Henry Odibi 🌍")

        markdown = "\n".join(lines)

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(markdown)

        return str(output_file)

    def _render_json(self, doc_data: Dict[str, Any], output_path: str) -> str:
        """
        Render documentation as JSON.

        Args:
            doc_data: Documentation data
            output_path: Output file path

        Returns:
            Path to generated file
        """
        import json

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(doc_data, f, indent=2)

        return str(output_file)

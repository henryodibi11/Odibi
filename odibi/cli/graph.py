"""
Graph CLI Command
=================

Visualizes the pipeline dependency graph.
"""

from odibi.graph import DependencyGraph
from odibi.pipeline import PipelineManager


def graph_command(args):
    """
    Handle graph subcommand.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code
    """
    try:
        # Load pipeline manager
        manager = PipelineManager.from_yaml(args.config)

        # Determine which pipeline to graph
        pipeline_name = args.pipeline
        if not pipeline_name:
            # Default to first pipeline if not specified
            pipeline_names = manager.list_pipelines()
            if not pipeline_names:
                print("❌ No pipelines found in configuration")
                return 1
            pipeline_name = pipeline_names[0]

        # Get the pipeline
        try:
            pipeline = manager.get_pipeline(pipeline_name)
        except ValueError:
            print(f"❌ Pipeline '{pipeline_name}' not found")
            return 1

        # Generate visualization
        if args.format == "ascii":
            print(pipeline.visualize())
        elif args.format == "dot":
            print(_generate_dot(pipeline.graph, pipeline_name, manager.catalog_manager))
        elif args.format == "mermaid":
            print(_generate_mermaid(pipeline.graph, pipeline_name, manager.catalog_manager))

        return 0

    except Exception as e:
        print(f"❌ Error generating graph: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1


def _generate_dot(graph: DependencyGraph, pipeline_name: str, catalog_manager=None) -> str:
    """Generate DOT (Graphviz) representation."""
    lines = []
    lines.append(f'digraph "{pipeline_name}" {{')
    lines.append("    rankdir=LR;")
    lines.append('    node [shape=box, style=rounded, fontname="Helvetica"];')
    lines.append('    edge [fontname="Helvetica"];')
    lines.append("")

    for node_name in graph.nodes:
        # Add node
        node = graph.nodes[node_name]
        op_type = "unknown"
        if node.read:
            op_type = "read"
            color = "lightblue"
        elif node.write:
            op_type = "write"
            color = "lightgreen"
        elif node.transform:
            op_type = "transform"
            color = "lightyellow"

        # Enrich with Catalog Stats
        stats_text = ""
        if catalog_manager:
            try:
                avg_rows = catalog_manager.get_average_volume(node_name)
                if avg_rows is not None:
                    stats_text = f"\\n~{int(avg_rows)} rows"
            except Exception:
                pass

        label = f"{node_name}\\n({op_type}){stats_text}"
        lines.append(f'    "{node_name}" [label="{label}", style="filled", fillcolor="{color}"];')

        # Add edges
        for dep in node.depends_on:
            lines.append(f'    "{dep}" -> "{node_name}";')

    lines.append("}")
    return "\n".join(lines)


def _generate_mermaid(graph: DependencyGraph, pipeline_name: str, catalog_manager=None) -> str:
    """Generate Mermaid diagram."""
    lines = []
    lines.append("graph LR")

    # Define styles
    lines.append("    classDef read fill:lightblue,stroke:#333,stroke-width:1px;")
    lines.append("    classDef write fill:lightgreen,stroke:#333,stroke-width:1px;")
    lines.append("    classDef transform fill:lightyellow,stroke:#333,stroke-width:1px;")

    for node_name in graph.nodes:
        node = graph.nodes[node_name]
        style_class = "transform"

        # Node styling based on type
        if node.read:
            shape = "(("  # Circle
            end_shape = "))"
            style_class = "read"
        elif node.write:
            shape = "[/"  # Parallelogram
            end_shape = "/]"
            style_class = "write"
        else:
            shape = "["  # Box
            end_shape = "]"

        # Enrich with Catalog Stats
        label = node_name
        if catalog_manager:
            try:
                avg_rows = catalog_manager.get_average_volume(node_name)
                if avg_rows is not None:
                    label = f"{node_name}<br/>~{int(avg_rows)} rows"
            except Exception:
                pass

        lines.append(f'    {node_name}{shape}"{label}"{end_shape}:::{style_class}')

        # Edges
        for dep in node.depends_on:
            lines.append(f"    {dep} --> {node_name}")

    return "\n".join(lines)

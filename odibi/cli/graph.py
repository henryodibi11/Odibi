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
            print(_generate_dot(pipeline.graph, pipeline_name))
        elif args.format == "mermaid":
            print(_generate_mermaid(pipeline.graph, pipeline_name))

        return 0

    except Exception as e:
        print(f"❌ Error generating graph: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1


def _generate_dot(graph: DependencyGraph, pipeline_name: str) -> str:
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

        label = f"{node_name}\\n({op_type})"
        lines.append(f'    "{node_name}" [label="{label}", style="filled", fillcolor="{color}"];')

        # Add edges
        for dep in node.depends_on:
            lines.append(f'    "{dep}" -> "{node_name}";')

    lines.append("}")
    return "\n".join(lines)


def _generate_mermaid(graph: DependencyGraph, pipeline_name: str) -> str:
    """Generate Mermaid diagram."""
    lines = []
    lines.append("graph LR")

    for node_name in graph.nodes:
        node = graph.nodes[node_name]

        # Node styling based on type
        if node.read:
            shape = "(("  # Circle
            end_shape = "))"
        elif node.write:
            shape = "[/"  # Parallelogram
            end_shape = "/]"
        else:
            shape = "["  # Box
            end_shape = "]"

        lines.append(f"    {node_name}{shape}{node_name}{end_shape}")

        # Edges
        for dep in node.depends_on:
            lines.append(f"    {dep} --> {node_name}")

    return "\n".join(lines)

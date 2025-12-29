"""Dependency graph builder and analyzer."""

from collections import defaultdict, deque
from typing import Dict, List, Optional, Set

from odibi.config import NodeConfig
from odibi.exceptions import DependencyError
from odibi.utils.logging import logger
from odibi.utils.logging_context import get_logging_context


class DependencyGraph:
    """Builds and analyzes dependency graph from node configurations."""

    def __init__(self, nodes: List[NodeConfig]):
        """Initialize dependency graph.

        Args:
            nodes: List of node configurations
        """
        ctx = get_logging_context()
        ctx.log_graph_operation("init_start", node_count=len(nodes))
        logger.debug(f"Initializing dependency graph with {len(nodes)} nodes")

        self.nodes = {node.name: node for node in nodes}
        self.adjacency_list: Dict[str, List[str]] = defaultdict(list)
        self.reverse_adjacency_list: Dict[str, List[str]] = defaultdict(list)

        self._build_graph()
        self._validate_graph()

        ctx.log_graph_operation("init_complete", node_count=len(self.nodes), status="success")

    def _build_graph(self) -> None:
        """Build adjacency lists from node dependencies."""
        ctx = get_logging_context()
        edge_count = 0

        logger.debug("Building adjacency lists from node dependencies")

        for node in self.nodes.values():
            for dependency in node.depends_on:
                self.adjacency_list[dependency].append(node.name)
                self.reverse_adjacency_list[node.name].append(dependency)
                edge_count += 1
                logger.debug(f"Added edge: {dependency} -> {node.name}")

        ctx.log_graph_operation(
            "build_complete",
            node_count=len(self.nodes),
            edge_count=edge_count,
        )
        logger.debug(f"Graph built with {len(self.nodes)} nodes and {edge_count} edges")

    def _validate_graph(self) -> None:
        """Validate the dependency graph.

        Raises:
            DependencyError: If validation fails
        """
        ctx = get_logging_context()
        ctx.log_graph_operation("validate_start", node_count=len(self.nodes))
        logger.debug("Starting graph validation")

        try:
            self._check_missing_dependencies()
            self._check_cycles()
            ctx.log_graph_operation("validate_complete", status="success")
            logger.debug("Graph validation completed successfully")
        except DependencyError as e:
            ctx.error(f"Graph validation failed: {e}")
            raise

    def _check_missing_dependencies(self) -> None:
        """Check that all dependencies exist as nodes.

        Raises:
            DependencyError: If any dependency doesn't exist
        """
        ctx = get_logging_context()
        logger.debug("Checking for missing dependencies")
        missing_deps = []

        for node in self.nodes.values():
            for dependency in node.depends_on:
                if dependency not in self.nodes:
                    missing_deps.append((node.name, dependency))
                    logger.debug(
                        f"Missing dependency detected: node '{node.name}' "
                        f"depends on '{dependency}' which doesn't exist"
                    )

        if missing_deps:
            errors = [
                f"Node '{node}' depends on '{dep}' which doesn't exist"
                for node, dep in missing_deps
            ]
            error_msg = "Missing dependencies found:\n  " + "\n  ".join(errors)
            ctx.error(
                error_msg,
                missing_count=len(missing_deps),
                missing_deps=missing_deps,
            )
            raise DependencyError(error_msg)

        logger.debug(f"No missing dependencies found across {len(self.nodes)} nodes")

    def _check_cycles(self) -> None:
        """Check for circular dependencies.

        Raises:
            DependencyError: If cycle detected
        """
        ctx = get_logging_context()
        logger.debug("Checking for circular dependencies")

        visited = set()
        rec_stack = set()

        def visit(node: str, path: List[str]) -> Optional[List[str]]:
            """DFS to detect cycles.

            Returns:
                Cycle path if found, None otherwise
            """
            if node in rec_stack:
                cycle_start = path.index(node)
                return path[cycle_start:] + [node]

            if node in visited:
                return None

            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for dependent in self.adjacency_list[node]:
                cycle = visit(dependent, path[:])
                if cycle:
                    return cycle

            rec_stack.remove(node)
            return None

        for node_name in self.nodes.keys():
            if node_name not in visited:
                cycle = visit(node_name, [])
                if cycle:
                    cycle_path = " -> ".join(cycle)
                    ctx.error(
                        f"Circular dependency detected: {cycle_path}",
                        cycle=cycle,
                        cycle_length=len(cycle),
                    )
                    raise DependencyError("Circular dependency detected", cycle=cycle)

        logger.debug(f"No circular dependencies found across {len(self.nodes)} nodes")

    def topological_sort(self) -> List[str]:
        """Return nodes in topological order (dependencies first).

        Uses Kahn's algorithm.

        Returns:
            List of node names in execution order
        """
        ctx = get_logging_context()
        ctx.log_graph_operation("topological_sort_start", node_count=len(self.nodes))
        logger.debug("Starting topological sort using Kahn's algorithm")

        in_degree = {name: 0 for name in self.nodes.keys()}
        for node in self.nodes.values():
            for dependency in node.depends_on:
                in_degree[node.name] += 1

        queue = deque([name for name, degree in in_degree.items() if degree == 0])
        sorted_nodes = []

        logger.debug(f"Initial nodes with no dependencies: {list(queue)}")

        while queue:
            node_name = queue.popleft()
            sorted_nodes.append(node_name)
            logger.debug(f"Processing node: {node_name} (position {len(sorted_nodes)})")

            for dependent in self.adjacency_list[node_name]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
                    logger.debug(f"Node '{dependent}' ready for processing")

        if len(sorted_nodes) != len(self.nodes):
            error_msg = "Failed to create topological sort (likely cycle)"
            ctx.error(
                error_msg,
                sorted_count=len(sorted_nodes),
                total_count=len(self.nodes),
            )
            raise DependencyError(error_msg)

        ctx.log_graph_operation(
            "topological_sort_complete",
            node_count=len(sorted_nodes),
            execution_order=sorted_nodes,
        )
        logger.debug(f"Topological sort complete. Execution order: {sorted_nodes}")

        return sorted_nodes

    def get_execution_layers(self) -> List[List[str]]:
        """Group nodes into execution layers for parallel execution.

        Nodes in the same layer have no dependencies on each other
        and can run in parallel.

        Returns:
            List of layers, where each layer is a list of node names
        """
        ctx = get_logging_context()
        ctx.log_graph_operation("execution_layers_start", node_count=len(self.nodes))
        logger.debug("Creating execution layers for parallel execution")

        in_degree = {name: len(node.depends_on) for name, node in self.nodes.items()}

        layers = []
        remaining = set(self.nodes.keys())

        while remaining:
            current_layer = [name for name in remaining if in_degree[name] == 0]

            if not current_layer:
                error_msg = "Cannot create execution layers (likely cycle)"
                ctx.error(
                    error_msg,
                    remaining_nodes=list(remaining),
                    layers_created=len(layers),
                )
                raise DependencyError(error_msg)

            layer_num = len(layers) + 1
            logger.debug(f"Layer {layer_num}: {current_layer}")
            layers.append(current_layer)

            for node_name in current_layer:
                remaining.remove(node_name)

                for dependent in self.adjacency_list[node_name]:
                    if dependent in remaining:
                        in_degree[dependent] -= 1

        ctx.log_graph_operation(
            "execution_layers_complete",
            node_count=len(self.nodes),
            layer_count=len(layers),
            layers=[{"layer": i + 1, "nodes": layer} for i, layer in enumerate(layers)],
        )
        logger.debug(f"Created {len(layers)} execution layers")

        return layers

    def get_dependencies(self, node_name: str) -> Set[str]:
        """Get all dependencies (direct and transitive) for a node.

        Args:
            node_name: Name of node

        Returns:
            Set of all dependency node names
        """
        logger.debug(f"Getting all dependencies for node '{node_name}'")

        if node_name not in self.nodes:
            logger.error(f"Node '{node_name}' not found in graph")
            raise ValueError(f"Node '{node_name}' not found")

        dependencies = set()
        queue = deque([node_name])

        while queue:
            current = queue.popleft()
            for dependency in self.reverse_adjacency_list[current]:
                if dependency not in dependencies:
                    dependencies.add(dependency)
                    queue.append(dependency)

        logger.debug(f"Node '{node_name}' has {len(dependencies)} dependencies: {dependencies}")
        return dependencies

    def get_dependents(self, node_name: str) -> Set[str]:
        """Get all dependents (direct and transitive) for a node.

        Args:
            node_name: Name of node

        Returns:
            Set of all dependent node names
        """
        logger.debug(f"Getting all dependents for node '{node_name}'")

        if node_name not in self.nodes:
            logger.error(f"Node '{node_name}' not found in graph")
            raise ValueError(f"Node '{node_name}' not found")

        dependents = set()
        queue = deque([node_name])

        while queue:
            current = queue.popleft()
            for dependent in self.adjacency_list[current]:
                if dependent not in dependents:
                    dependents.add(dependent)
                    queue.append(dependent)

        logger.debug(f"Node '{node_name}' has {len(dependents)} dependents: {dependents}")
        return dependents

    def get_independent_nodes(self) -> List[str]:
        """Get nodes that have no dependencies.

        Returns:
            List of node names with no dependencies
        """
        independent = [name for name, node in self.nodes.items() if not node.depends_on]
        logger.debug(f"Found {len(independent)} independent nodes: {independent}")
        return independent

    def visualize(self) -> str:
        """Generate a text visualization of the graph.

        Returns:
            String representation of the graph
        """
        logger.debug("Generating text visualization of dependency graph")
        lines = ["Dependency Graph:", ""]

        layers = self.get_execution_layers()
        for i, layer in enumerate(layers):
            lines.append(f"Layer {i + 1}:")
            for node_name in sorted(layer):
                node = self.nodes[node_name]
                deps = (
                    f" (depends on: {', '.join(sorted(node.depends_on))})"
                    if node.depends_on
                    else ""
                )
                lines.append(f"  - {node_name}{deps}")
            lines.append("")

        return "\n".join(lines)

    def to_dict(self) -> Dict[str, any]:
        """Export graph as a dictionary for JSON serialization.

        Returns:
            Dictionary with nodes and edges suitable for visualization libraries.
            Includes cross-pipeline dependencies from inputs block.
        """
        nodes = []
        edges = []
        existing_node_ids = set()

        for node_name, node_config in self.nodes.items():
            existing_node_ids.add(node_name)
            nodes.append(
                {
                    "id": node_name,
                    "label": node_name,
                    "type": node_config.type if hasattr(node_config, "type") else "transform",
                }
            )

            # Add edges from depends_on (intra-pipeline dependencies)
            for dependency in node_config.depends_on:
                edges.append(
                    {
                        "source": dependency,
                        "target": node_name,
                    }
                )

            # Add edges from inputs block (cross-pipeline dependencies)
            # Track full reference for external node labels
            if node_config.inputs:
                for input_name, input_val in node_config.inputs.items():
                    if isinstance(input_val, str) and input_val.startswith("$"):
                        ref = input_val[1:]  # Remove $
                        if "." in ref:
                            pipeline_name, node_ref = ref.split(".", 1)
                            edges.append(
                                {
                                    "source": node_ref,
                                    "target": node_name,
                                    "source_pipeline": pipeline_name,
                                }
                            )
                        else:
                            edges.append({"source": ref, "target": node_name})

        # Find cross-pipeline dependencies (edge sources that don't exist as nodes)
        # Build a map of node_ref -> pipeline_name for labeling
        external_node_pipelines: Dict[str, str] = {}
        cross_pipeline_deps = set()
        for edge in edges:
            if edge["source"] not in existing_node_ids:
                cross_pipeline_deps.add(edge["source"])
                # Track the pipeline name if available
                if "source_pipeline" in edge:
                    external_node_pipelines[edge["source"]] = edge["source_pipeline"]

        # Add placeholder nodes for cross-pipeline dependencies
        for dep_id in cross_pipeline_deps:
            pipeline_name = external_node_pipelines.get(dep_id)
            label = f"{pipeline_name}.{dep_id}" if pipeline_name else dep_id
            nodes.append(
                {
                    "id": dep_id,
                    "label": label,
                    "type": "external",
                    "source_pipeline": pipeline_name,
                }
            )

        return {
            "nodes": nodes,
            "edges": edges,
        }

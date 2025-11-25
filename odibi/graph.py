"""Dependency graph builder and analyzer."""

from collections import defaultdict, deque
from typing import Dict, List, Optional, Set

from odibi.config import NodeConfig
from odibi.exceptions import DependencyError


class DependencyGraph:
    """Builds and analyzes dependency graph from node configurations."""

    def __init__(self, nodes: List[NodeConfig]):
        """Initialize dependency graph.

        Args:
            nodes: List of node configurations
        """
        self.nodes = {node.name: node for node in nodes}
        self.adjacency_list: Dict[str, List[str]] = defaultdict(list)
        self.reverse_adjacency_list: Dict[str, List[str]] = defaultdict(list)

        self._build_graph()
        self._validate_graph()

    def _build_graph(self) -> None:
        """Build adjacency lists from node dependencies."""
        for node in self.nodes.values():
            for dependency in node.depends_on:
                # Edge from dependency to node (dependency -> node)
                self.adjacency_list[dependency].append(node.name)
                self.reverse_adjacency_list[node.name].append(dependency)

    def _validate_graph(self) -> None:
        """Validate the dependency graph.

        Raises:
            DependencyError: If validation fails
        """
        # Check for missing dependencies
        self._check_missing_dependencies()

        # Check for cycles
        self._check_cycles()

    def _check_missing_dependencies(self) -> None:
        """Check that all dependencies exist as nodes.

        Raises:
            DependencyError: If any dependency doesn't exist
        """
        missing_deps = []

        for node in self.nodes.values():
            for dependency in node.depends_on:
                if dependency not in self.nodes:
                    missing_deps.append((node.name, dependency))

        if missing_deps:
            errors = [
                f"Node '{node}' depends on '{dep}' which doesn't exist"
                for node, dep in missing_deps
            ]
            raise DependencyError("Missing dependencies found:\n  " + "\n  ".join(errors))

    def _check_cycles(self) -> None:
        """Check for circular dependencies.

        Raises:
            DependencyError: If cycle detected
        """
        # Track visited nodes and current path
        visited = set()
        rec_stack = set()

        def visit(node: str, path: List[str]) -> Optional[List[str]]:
            """DFS to detect cycles.

            Returns:
                Cycle path if found, None otherwise
            """
            if node in rec_stack:
                # Found a cycle
                cycle_start = path.index(node)
                return path[cycle_start:] + [node]

            if node in visited:
                return None

            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            # Visit all dependents
            for dependent in self.adjacency_list[node]:
                cycle = visit(dependent, path[:])
                if cycle:
                    return cycle

            rec_stack.remove(node)
            return None

        # Check from each node
        for node_name in self.nodes.keys():
            if node_name not in visited:
                cycle = visit(node_name, [])
                if cycle:
                    raise DependencyError("Circular dependency detected", cycle=cycle)

    def topological_sort(self) -> List[str]:
        """Return nodes in topological order (dependencies first).

        Uses Kahn's algorithm.

        Returns:
            List of node names in execution order
        """
        # Calculate in-degree for each node
        in_degree = {name: 0 for name in self.nodes.keys()}
        for node in self.nodes.values():
            for dependency in node.depends_on:
                in_degree[node.name] += 1

        # Queue of nodes with no dependencies
        queue = deque([name for name, degree in in_degree.items() if degree == 0])
        sorted_nodes = []

        while queue:
            # Process node with no remaining dependencies
            node_name = queue.popleft()
            sorted_nodes.append(node_name)

            # Reduce in-degree for all dependents
            for dependent in self.adjacency_list[node_name]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        # If we didn't process all nodes, there's a cycle
        if len(sorted_nodes) != len(self.nodes):
            raise DependencyError("Failed to create topological sort (likely cycle)")

        return sorted_nodes

    def get_execution_layers(self) -> List[List[str]]:
        """Group nodes into execution layers for parallel execution.

        Nodes in the same layer have no dependencies on each other
        and can run in parallel.

        Returns:
            List of layers, where each layer is a list of node names
        """
        # Calculate in-degree for each node
        in_degree = {name: len(node.depends_on) for name, node in self.nodes.items()}

        layers = []
        remaining = set(self.nodes.keys())

        while remaining:
            # Find all nodes with no remaining dependencies
            current_layer = [name for name in remaining if in_degree[name] == 0]

            if not current_layer:
                raise DependencyError("Cannot create execution layers (likely cycle)")

            layers.append(current_layer)

            # Remove current layer from remaining
            for node_name in current_layer:
                remaining.remove(node_name)

                # Reduce in-degree for dependents
                for dependent in self.adjacency_list[node_name]:
                    if dependent in remaining:
                        in_degree[dependent] -= 1

        return layers

    def get_dependencies(self, node_name: str) -> Set[str]:
        """Get all dependencies (direct and transitive) for a node.

        Args:
            node_name: Name of node

        Returns:
            Set of all dependency node names
        """
        if node_name not in self.nodes:
            raise ValueError(f"Node '{node_name}' not found")

        dependencies = set()
        queue = deque([node_name])

        while queue:
            current = queue.popleft()
            for dependency in self.reverse_adjacency_list[current]:
                if dependency not in dependencies:
                    dependencies.add(dependency)
                    queue.append(dependency)

        return dependencies

    def get_dependents(self, node_name: str) -> Set[str]:
        """Get all dependents (direct and transitive) for a node.

        Args:
            node_name: Name of node

        Returns:
            Set of all dependent node names
        """
        if node_name not in self.nodes:
            raise ValueError(f"Node '{node_name}' not found")

        dependents = set()
        queue = deque([node_name])

        while queue:
            current = queue.popleft()
            for dependent in self.adjacency_list[current]:
                if dependent not in dependents:
                    dependents.add(dependent)
                    queue.append(dependent)

        return dependents

    def get_independent_nodes(self) -> List[str]:
        """Get nodes that have no dependencies.

        Returns:
            List of node names with no dependencies
        """
        return [name for name, node in self.nodes.items() if not node.depends_on]

    def visualize(self) -> str:
        """Generate a text visualization of the graph.

        Returns:
            String representation of the graph
        """
        lines = ["Dependency Graph:", ""]

        # Show execution layers
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

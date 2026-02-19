import pytest
from odibi.graph import DependencyGraph
from odibi.config import NodeConfig
from odibi.exceptions import DependencyError


def make_node(name, depends_on=None, inputs=None, type_=None):
    """
    Helper function to create a NodeConfig instance for testing.
    Assumes minimal required fields for DependencyGraph.
    """
    node = NodeConfig(
        name=name,
        read={"connection": "dummy_conn", "format": "dummy_format", "path": "dummy_path"},
        write={"connection": "dummy_conn", "format": "dummy_format", "path": "dummy_path"},
        depends_on=depends_on or [],
        inputs=inputs or {},
    )
    if type_:
        # Simulate an optional 'type' attribute
        setattr(node, "type", type_)
    return node


class TestDependencyGraph:
    def test_empty_graph_topological_and_layers(self):
        """test_topological_sort_empty_graph_returns_empty_and_layers_empty."""
        graph = DependencyGraph([])
        # For empty graph, topological sort and execution layers should be empty lists.
        assert graph.topological_sort() == []
        assert graph.get_execution_layers() == []
        assert graph.get_independent_nodes() == []
        viz = graph.visualize()
        assert "Dependency Graph:" in viz
        graph_dict = graph.to_dict()
        assert graph_dict["nodes"] == []
        assert graph_dict["edges"] == []

    def test_topological_sort_valid_order(self):
        """test_topological_sort_chain_valid_order."""
        # Create a chain: A -> B -> C.
        node_a = make_node("A")
        node_b = make_node("B", depends_on=["A"])
        node_c = make_node("C", depends_on=["B"])
        graph = DependencyGraph([node_a, node_b, node_c])
        order = graph.topological_sort()
        # Order should ensure A comes before B and B before C.
        assert order.index("A") < order.index("B")
        assert order.index("B") < order.index("C")

    def test_execution_layers_linear(self):
        """test_get_execution_layers_chain_returns_sequential_layers."""
        # Chain: A -> B -> C; expect three sequential layers.
        node_a = make_node("A")
        node_b = make_node("B", depends_on=["A"])
        node_c = make_node("C", depends_on=["B"])
        graph = DependencyGraph([node_a, node_b, node_c])
        layers = graph.get_execution_layers()
        assert layers == [["A"], ["B"], ["C"]]

    def test_execution_layers_parallel(self):
        """test_get_execution_layers_parallel_nodes_grouped_together."""
        # Graph: A independent; B and C both depend on A.
        node_a = make_node("A")
        node_b = make_node("B", depends_on=["A"])
        node_c = make_node("C", depends_on=["A"])
        graph = DependencyGraph([node_a, node_b, node_c])
        layers = graph.get_execution_layers()
        # First layer must contain A; second layer contains B and C (order can vary).
        assert "A" in layers[0]
        assert set(layers[1]) == {"B", "C"}

    def test_missing_dependency_raises_error(self):
        """test_init_missing_dependency_raises_DependencyError."""
        # Node B depends on a non-existent node "X".
        node_a = make_node("A")
        node_b = make_node("B", depends_on=["X"])
        with pytest.raises(DependencyError) as exc_info:
            DependencyGraph([node_a, node_b])
        assert "Missing dependencies" in str(exc_info.value)

    def test_cycle_detection_raises_error(self):
        """test_init_cycle_detection_raises_DependencyError."""
        # Create a cycle: A depends on B and B depends on A.
        node_a = make_node("A", depends_on=["B"])
        node_b = make_node("B", depends_on=["A"])
        with pytest.raises(DependencyError) as exc_info:
            DependencyGraph([node_a, node_b])
        assert "Circular dependency detected" in str(exc_info.value)

    def test_get_dependencies_returns_all_transitive_deps(self):
        """test_get_dependencies_chain_returns_correct_dependency_set."""
        # Graph: A -> B -> C, and additionally D depends on A, E depends on B.
        node_a = make_node("A")
        node_b = make_node("B", depends_on=["A"])
        node_c = make_node("C", depends_on=["B"])
        node_d = make_node("D", depends_on=["A"])
        node_e = make_node("E", depends_on=["B"])
        graph = DependencyGraph([node_a, node_b, node_c, node_d, node_e])
        deps_c = graph.get_dependencies("C")
        assert deps_c == {"A", "B"}
        deps_e = graph.get_dependencies("E")
        assert deps_e == {"A", "B"}
        deps_a = graph.get_dependencies("A")
        assert deps_a == set()

    def test_get_dependents_returns_all_transitive_dependents(self):
        """test_get_dependents_chain_returns_correct_dependents_set."""
        # Using same graph as above.
        node_a = make_node("A")
        node_b = make_node("B", depends_on=["A"])
        node_c = make_node("C", depends_on=["B"])
        node_d = make_node("D", depends_on=["A"])
        node_e = make_node("E", depends_on=["B"])
        graph = DependencyGraph([node_a, node_b, node_c, node_d, node_e])
        dependents_a = graph.get_dependents("A")
        assert dependents_a == {"B", "C", "D", "E"}
        dependents_b = graph.get_dependents("B")
        assert dependents_b == {"C", "E"}
        dependents_c = graph.get_dependents("C")
        assert dependents_c == set()

    def test_get_independent_nodes_returns_correct_nodes(self):
        """test_get_independent_nodes_returns_only_nodes_without_dependencies."""
        # In this graph, nodes with no dependencies: A and C.
        node_a = make_node("A")
        node_b = make_node("B", depends_on=["A"])
        node_c = make_node("C")
        graph = DependencyGraph([node_a, node_b, node_c])
        independent = graph.get_independent_nodes()
        assert set(independent) == {"A", "C"}

    def test_visualize_generates_expected_output(self):
        """test_visualize_returns_text_representation_of_graph."""
        # Simple graph: A -> B.
        node_a = make_node("A")
        node_b = make_node("B", depends_on=["A"])
        graph = DependencyGraph([node_a, node_b])
        output = graph.visualize()
        assert "Dependency Graph:" in output
        assert "Layer" in output
        assert "A" in output
        assert "B" in output

    def test_to_dict_includes_cross_pipeline_edges_and_external_nodes(self):
        """test_to_dict_returns_cross_pipeline_edges_and_creates_external_placeholder."""
        # Create a node with an input referencing an external dependency.
        # Node B has an input that references "$pipeline1.A"
        from odibi.config import NodeConfig

        node_b = NodeConfig.model_construct(
            name="B",
            write={"connection": "dummy_conn", "format": "dummy_format", "path": "dummy_path"},
            depends_on=[],
            inputs={"input1": "$pipeline1.A"},
        )
        node_c = make_node("C", depends_on=["B"])
        graph = DependencyGraph([node_b, node_c])
        graph_dict = graph.to_dict()
        edges = graph_dict["edges"]
        found = False
        for edge in edges:
            if edge.get("source_pipeline") == "pipeline1":
                if edge["source"] == "A" and edge["target"] == "B":
                    found = True
        assert found, "Cross-pipeline edge not found in to_dict output"
        nodes = graph_dict["nodes"]
        external_node = next(
            (n for n in nodes if n["id"] == "A" and n.get("type") == "external"), None
        )
        assert external_node is not None, "External node placeholder not added"

    def test_get_dependencies_invalid_node_raises_ValueError(self):
        """test_get_dependencies_invalid_node_raises_ValueError."""
        node_a = make_node("A")
        graph = DependencyGraph([node_a])
        with pytest.raises(ValueError) as exc_info:
            graph.get_dependencies("NonExistent")
        assert "not found" in str(exc_info.value)

    def test_get_dependents_invalid_node_raises_ValueError(self):
        """test_get_dependents_invalid_node_raises_ValueError."""
        node_a = make_node("A")
        graph = DependencyGraph([node_a])
        with pytest.raises(ValueError) as exc_info:
            graph.get_dependents("NonExistent")
        assert "not found" in str(exc_info.value)

    def test_cross_pipeline_no_cycle(self):
        """No error when cross-pipeline deps are acyclic."""
        node_a = NodeConfig.model_construct(
            name="A",
            depends_on=[],
            inputs={"src": "$pipeline2.X"},
        )
        node_x = make_node("X")
        DependencyGraph.check_cross_pipeline_cycles(
            {
                "pipeline1": [node_a],
                "pipeline2": [node_x],
            }
        )

    def test_cross_pipeline_cycle_detected(self):
        """Cross-pipeline cycle raises DependencyError."""
        node_a = NodeConfig.model_construct(
            name="A",
            depends_on=[],
            inputs={"src": "$pipeline2.X"},
        )
        node_x = NodeConfig.model_construct(
            name="X",
            depends_on=[],
            inputs={"src": "$pipeline1.A"},
        )
        with pytest.raises(DependencyError, match="Cross-pipeline circular dependency"):
            DependencyGraph.check_cross_pipeline_cycles(
                {
                    "pipeline1": [node_a],
                    "pipeline2": [node_x],
                }
            )

    def test_cross_pipeline_transitive_cycle(self):
        """Transitive cross-pipeline cycle (A->B->C->A) detected."""
        node_a = NodeConfig.model_construct(
            name="A",
            depends_on=[],
            inputs={"src": "$pipeline2.X"},
        )
        node_x = NodeConfig.model_construct(
            name="X",
            depends_on=[],
            inputs={"src": "$pipeline3.Y"},
        )
        node_y = NodeConfig.model_construct(
            name="Y",
            depends_on=[],
            inputs={"src": "$pipeline1.A"},
        )
        with pytest.raises(DependencyError, match="Cross-pipeline circular dependency"):
            DependencyGraph.check_cross_pipeline_cycles(
                {
                    "pipeline1": [node_a],
                    "pipeline2": [node_x],
                    "pipeline3": [node_y],
                }
            )

    def test_cross_pipeline_self_ref_ignored(self):
        """References within same pipeline are not cross-pipeline deps."""
        node_a = NodeConfig.model_construct(
            name="A",
            depends_on=[],
            inputs={"src": "$pipeline1.B"},
        )
        node_b = make_node("B")
        DependencyGraph.check_cross_pipeline_cycles(
            {
                "pipeline1": [node_a, node_b],
            }
        )

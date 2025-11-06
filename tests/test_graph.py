"""Tests for dependency graph builder."""

import pytest

from odibi.config import NodeConfig, ReadConfig, TransformConfig
from odibi.graph import DependencyGraph
from odibi.exceptions import DependencyError


class TestDependencyGraph:
    """Test dependency graph construction and validation."""
    
    def test_simple_linear_graph(self):
        """Test linear dependency chain."""
        nodes = [
            NodeConfig(
                name="node1",
                read=ReadConfig(connection="local", format="csv", path="a.csv")
            ),
            NodeConfig(
                name="node2",
                depends_on=["node1"],
                transform=TransformConfig(steps=["SELECT * FROM node1"])
            ),
            NodeConfig(
                name="node3",
                depends_on=["node2"],
                transform=TransformConfig(steps=["SELECT * FROM node2"])
            )
        ]
        
        graph = DependencyGraph(nodes)
        order = graph.topological_sort()
        
        assert order == ["node1", "node2", "node3"]
    
    def test_parallel_nodes(self):
        """Test nodes with no dependencies can run in parallel."""
        nodes = [
            NodeConfig(
                name="parallel1",
                read=ReadConfig(connection="local", format="csv", path="a.csv")
            ),
            NodeConfig(
                name="parallel2",
                read=ReadConfig(connection="local", format="csv", path="b.csv")
            ),
            NodeConfig(
                name="join",
                depends_on=["parallel1", "parallel2"],
                transform=TransformConfig(steps=["SELECT * FROM parallel1"])
            )
        ]
        
        graph = DependencyGraph(nodes)
        layers = graph.get_execution_layers()
        
        # First layer should have both parallel nodes
        assert len(layers) == 2
        assert set(layers[0]) == {"parallel1", "parallel2"}
        assert layers[1] == ["join"]
    
    def test_complex_diamond_graph(self):
        """Test diamond-shaped dependency graph."""
        nodes = [
            NodeConfig(name="root", read=ReadConfig(connection="local", format="csv", path="a.csv")),
            NodeConfig(name="left", depends_on=["root"], transform=TransformConfig(steps=["SELECT * FROM root"])),
            NodeConfig(name="right", depends_on=["root"], transform=TransformConfig(steps=["SELECT * FROM root"])),
            NodeConfig(name="merge", depends_on=["left", "right"], transform=TransformConfig(steps=["SELECT * FROM left"]))
        ]
        
        graph = DependencyGraph(nodes)
        order = graph.topological_sort()
        
        # Root must be first
        assert order[0] == "root"
        # Merge must be last
        assert order[3] == "merge"
        # Left and right can be in any order
        assert set(order[1:3]) == {"left", "right"}


class TestCycleDetection:
    """Test circular dependency detection."""
    
    def test_direct_cycle(self):
        """Test detection of direct circular dependency."""
        nodes = [
            NodeConfig(
                name="node1",
                depends_on=["node2"],
                read=ReadConfig(connection="local", format="csv", path="a.csv")
            ),
            NodeConfig(
                name="node2",
                depends_on=["node1"],
                transform=TransformConfig(steps=["SELECT * FROM node1"])
            )
        ]
        
        with pytest.raises(DependencyError) as exc_info:
            DependencyGraph(nodes)
        
        assert "Circular dependency" in str(exc_info.value)
    
    def test_indirect_cycle(self):
        """Test detection of indirect circular dependency."""
        nodes = [
            NodeConfig(name="node1", depends_on=["node3"], read=ReadConfig(connection="local", format="csv", path="a.csv")),
            NodeConfig(name="node2", depends_on=["node1"], transform=TransformConfig(steps=["SELECT * FROM node1"])),
            NodeConfig(name="node3", depends_on=["node2"], transform=TransformConfig(steps=["SELECT * FROM node2"]))
        ]
        
        with pytest.raises(DependencyError) as exc_info:
            DependencyGraph(nodes)
        
        assert "Circular dependency" in str(exc_info.value)
    
    def test_self_dependency(self):
        """Test detection of node depending on itself."""
        nodes = [
            NodeConfig(
                name="node1",
                depends_on=["node1"],
                read=ReadConfig(connection="local", format="csv", path="a.csv")
            )
        ]
        
        with pytest.raises(DependencyError) as exc_info:
            DependencyGraph(nodes)
        
        assert "Circular dependency" in str(exc_info.value)


class TestMissingDependencies:
    """Test validation of missing dependencies."""
    
    def test_missing_dependency(self):
        """Test error when dependency doesn't exist."""
        nodes = [
            NodeConfig(
                name="node1",
                depends_on=["nonexistent"],
                read=ReadConfig(connection="local", format="csv", path="a.csv")
            )
        ]
        
        with pytest.raises(DependencyError) as exc_info:
            DependencyGraph(nodes)
        
        error_msg = str(exc_info.value)
        assert "Missing dependencies" in error_msg
        assert "nonexistent" in error_msg
    
    def test_multiple_missing_dependencies(self):
        """Test error with multiple missing dependencies."""
        nodes = [
            NodeConfig(
                name="node1",
                depends_on=["missing1", "missing2"],
                read=ReadConfig(connection="local", format="csv", path="a.csv")
            )
        ]
        
        with pytest.raises(DependencyError) as exc_info:
            DependencyGraph(nodes)
        
        error_msg = str(exc_info.value)
        assert "missing1" in error_msg or "missing2" in error_msg


class TestGraphAnalysis:
    """Test graph analysis methods."""
    
    def test_get_dependencies(self):
        """Test getting all dependencies of a node."""
        nodes = [
            NodeConfig(name="root", read=ReadConfig(connection="local", format="csv", path="a.csv")),
            NodeConfig(name="middle", depends_on=["root"], transform=TransformConfig(steps=["SELECT * FROM root"])),
            NodeConfig(name="leaf", depends_on=["middle"], transform=TransformConfig(steps=["SELECT * FROM middle"]))
        ]
        
        graph = DependencyGraph(nodes)
        
        # Leaf depends on middle and root (transitive)
        deps = graph.get_dependencies("leaf")
        assert deps == {"root", "middle"}
        
        # Middle depends only on root
        deps = graph.get_dependencies("middle")
        assert deps == {"root"}
        
        # Root has no dependencies
        deps = graph.get_dependencies("root")
        assert deps == set()
    
    def test_get_dependents(self):
        """Test getting all dependents of a node."""
        nodes = [
            NodeConfig(name="root", read=ReadConfig(connection="local", format="csv", path="a.csv")),
            NodeConfig(name="middle", depends_on=["root"], transform=TransformConfig(steps=["SELECT * FROM root"])),
            NodeConfig(name="leaf", depends_on=["middle"], transform=TransformConfig(steps=["SELECT * FROM middle"]))
        ]
        
        graph = DependencyGraph(nodes)
        
        # Root has middle and leaf as dependents (transitive)
        dependents = graph.get_dependents("root")
        assert dependents == {"middle", "leaf"}
        
        # Middle has only leaf
        dependents = graph.get_dependents("middle")
        assert dependents == {"leaf"}
        
        # Leaf has no dependents
        dependents = graph.get_dependents("leaf")
        assert dependents == set()
    
    def test_get_independent_nodes(self):
        """Test finding nodes with no dependencies."""
        nodes = [
            NodeConfig(name="independent1", read=ReadConfig(connection="local", format="csv", path="a.csv")),
            NodeConfig(name="independent2", read=ReadConfig(connection="local", format="csv", path="b.csv")),
            NodeConfig(
                name="dependent",
                depends_on=["independent1"],
                transform=TransformConfig(steps=["SELECT * FROM independent1"])
            )
        ]
        
        graph = DependencyGraph(nodes)
        independent = graph.get_independent_nodes()
        
        assert set(independent) == {"independent1", "independent2"}


class TestExecutionLayers:
    """Test parallel execution layer creation."""
    
    def test_single_layer(self):
        """Test all independent nodes in one layer."""
        nodes = [
            NodeConfig(name="node1", read=ReadConfig(connection="local", format="csv", path="a.csv")),
            NodeConfig(name="node2", read=ReadConfig(connection="local", format="csv", path="b.csv")),
            NodeConfig(name="node3", read=ReadConfig(connection="local", format="csv", path="c.csv"))
        ]
        
        graph = DependencyGraph(nodes)
        layers = graph.get_execution_layers()
        
        assert len(layers) == 1
        assert set(layers[0]) == {"node1", "node2", "node3"}
    
    def test_multiple_layers(self):
        """Test nodes grouped into correct layers."""
        nodes = [
            NodeConfig(name="l1_a", read=ReadConfig(connection="local", format="csv", path="a.csv")),
            NodeConfig(name="l1_b", read=ReadConfig(connection="local", format="csv", path="b.csv")),
            NodeConfig(name="l2_a", depends_on=["l1_a"], transform=TransformConfig(steps=["SELECT * FROM l1_a"])),
            NodeConfig(name="l2_b", depends_on=["l1_b"], transform=TransformConfig(steps=["SELECT * FROM l1_b"])),
            NodeConfig(
                name="l3",
                depends_on=["l2_a", "l2_b"],
                transform=TransformConfig(steps=["SELECT * FROM l2_a"])
            )
        ]
        
        graph = DependencyGraph(nodes)
        layers = graph.get_execution_layers()
        
        assert len(layers) == 3
        assert set(layers[0]) == {"l1_a", "l1_b"}
        assert set(layers[1]) == {"l2_a", "l2_b"}
        assert layers[2] == ["l3"]


class TestVisualization:
    """Test graph visualization."""
    
    def test_visualize_simple_graph(self):
        """Test text visualization output."""
        nodes = [
            NodeConfig(name="load", read=ReadConfig(connection="local", format="csv", path="a.csv")),
            NodeConfig(name="transform", depends_on=["load"], transform=TransformConfig(steps=["SELECT * FROM load"]))
        ]
        
        graph = DependencyGraph(nodes)
        viz = graph.visualize()
        
        assert "Dependency Graph" in viz
        assert "load" in viz
        assert "transform" in viz
        assert "depends on" in viz


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

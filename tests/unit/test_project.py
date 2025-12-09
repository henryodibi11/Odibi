"""Unit tests for Project class - unified semantic layer integration."""

import pandas as pd
import pytest
import yaml

from odibi.project import Project, SourceResolver
from odibi.config import ProjectConfig, LocalConnectionConfig, ConnectionType
from odibi.semantics.metrics import (
    MetricDefinition,
    DimensionDefinition,
    SemanticLayerConfig,
)


class TestSourceResolver:
    """Test SourceResolver class."""

    def test_resolve_with_connection_prefix(self):
        """Test resolving source with connection.table format."""
        connections = {
            "gold": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path="/data/gold"),
            "silver": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path="/data/silver"),
        }
        resolver = SourceResolver(connections)

        conn_name, full_path = resolver.resolve("gold.fact_orders")

        assert conn_name == "gold"
        assert "fact_orders" in full_path
        assert "gold" in full_path

    def test_resolve_without_prefix_single_connection(self):
        """Test resolving source without prefix when only one connection exists."""
        connections = {
            "warehouse": LocalConnectionConfig(
                type=ConnectionType.LOCAL, base_path="/data/warehouse"
            ),
        }
        resolver = SourceResolver(connections)

        conn_name, full_path = resolver.resolve("fact_orders")

        assert conn_name == "warehouse"
        assert "fact_orders" in full_path

    def test_resolve_without_prefix_prefers_gold(self):
        """Test that resolver prefers 'gold' connection when no prefix given."""
        connections = {
            "bronze": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path="/data/bronze"),
            "silver": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path="/data/silver"),
            "gold": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path="/data/gold"),
        }
        resolver = SourceResolver(connections)

        conn_name, full_path = resolver.resolve("fact_orders")

        assert conn_name == "gold"

    def test_resolve_unknown_connection_raises_error(self):
        """Test that unknown connection raises ValueError."""
        connections = {
            "gold": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path="/data/gold"),
        }
        resolver = SourceResolver(connections)

        with pytest.raises(ValueError, match="not found"):
            resolver.resolve("unknown.fact_orders")

    def test_resolve_with_base_path(self):
        """Test resolving with relative base path."""
        connections = {
            "gold": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path="./data/gold"),
        }
        resolver = SourceResolver(connections, base_path="/project")

        conn_name, full_path = resolver.resolve("gold.fact_orders")

        assert "/project" in full_path or "\\project" in full_path

    def test_resolve_node_reference(self):
        """Test resolving $pipeline.node reference."""
        connections = {
            "gold": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path="/data/gold"),
        }
        pipelines = [
            {
                "pipeline": "build_warehouse",
                "nodes": [
                    {
                        "name": "fact_orders",
                        "write": {"connection": "gold", "table": "fact_orders"},
                    }
                ],
            }
        ]
        resolver = SourceResolver(connections, pipelines=pipelines)

        conn_name, full_path = resolver.resolve("$build_warehouse.fact_orders")

        assert conn_name == "gold"
        assert "fact_orders" in full_path

    def test_resolve_node_reference_not_found(self):
        """Test that unknown node reference raises ValueError."""
        connections = {
            "gold": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path="/data/gold"),
        }
        resolver = SourceResolver(connections, pipelines=[])

        with pytest.raises(ValueError, match="not found"):
            resolver.resolve("$build_warehouse.unknown_node")

    def test_resolve_node_reference_no_write(self):
        """Test that node without write config raises ValueError."""
        connections = {
            "gold": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path="/data/gold"),
        }
        pipelines = [
            {
                "pipeline": "build_warehouse",
                "nodes": [{"name": "read_only", "read": {"connection": "gold"}}],
            }
        ]
        resolver = SourceResolver(connections, pipelines=pipelines)

        with pytest.raises(ValueError, match="no 'write' config"):
            resolver.resolve("$build_warehouse.read_only")


class TestProjectWithInlineSemantics:
    """Test Project class with inline semantic configuration."""

    @pytest.fixture
    def sample_data(self):
        """Create sample fact data."""
        return pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4, 5],
                "region": ["East", "West", "East", "West", "East"],
                "product": ["A", "A", "B", "B", "A"],
                "total_amount": [100, 200, 150, 250, 300],
            }
        )

    @pytest.fixture
    def project_with_data(self, sample_data, tmp_path):
        """Create a Project with sample data pre-registered."""
        config_dict = {
            "project": "test_project",
            "engine": "pandas",
            "connections": {
                "gold": {"type": "local", "base_path": str(tmp_path / "gold")},
            },
            "story": {"connection": "gold", "path": "stories"},
            "system": {"connection": "gold", "path": "_system"},
            "pipelines": [
                {
                    "pipeline": "test",
                    "nodes": [
                        {
                            "name": "test_node",
                            "read": {"connection": "gold", "path": "test", "format": "parquet"},
                            "write": {
                                "connection": "gold",
                                "path": "test_out",
                                "format": "parquet",
                            },
                        }
                    ],
                }
            ],
            "semantic": {
                "metrics": [
                    {
                        "name": "revenue",
                        "expr": "SUM(total_amount)",
                        "source": "fact_orders",
                    },
                    {
                        "name": "order_count",
                        "expr": "COUNT(*)",
                        "source": "fact_orders",
                    },
                ],
                "dimensions": [
                    {"name": "region", "source": "fact_orders", "column": "region"},
                    {"name": "product", "source": "fact_orders", "column": "product"},
                ],
            },
        }

        config = ProjectConfig(**config_dict)
        semantic_config = SemanticLayerConfig(
            metrics=[
                MetricDefinition(name="revenue", expr="SUM(total_amount)", source="fact_orders"),
                MetricDefinition(name="order_count", expr="COUNT(*)", source="fact_orders"),
            ],
            dimensions=[
                DimensionDefinition(name="region", source="fact_orders", column="region"),
                DimensionDefinition(name="product", source="fact_orders", column="product"),
            ],
        )

        project = Project(config=config, semantic_config=semantic_config)
        project.register("fact_orders", sample_data)

        return project

    def test_query_simple(self, project_with_data):
        """Test simple query execution."""
        result = project_with_data.query("revenue BY region")

        assert result.row_count == 2  # East and West
        assert "revenue" in result.df.columns
        assert "region" in result.df.columns

    def test_query_multiple_metrics(self, project_with_data):
        """Test query with multiple metrics."""
        result = project_with_data.query("revenue, order_count BY region")

        assert "revenue" in result.df.columns
        assert "order_count" in result.df.columns

    def test_query_multiple_dimensions(self, project_with_data):
        """Test query with multiple dimensions."""
        result = project_with_data.query("revenue BY region, product")

        assert result.row_count == 4  # East-A, East-B, West-A, West-B
        assert "region" in result.df.columns
        assert "product" in result.df.columns

    def test_query_no_grouping(self, project_with_data):
        """Test query without grouping (grand total)."""
        result = project_with_data.query("revenue")

        assert result.row_count == 1
        assert result.df["revenue"].iloc[0] == 1000  # Sum of all amounts

    def test_metrics_property(self, project_with_data):
        """Test metrics property returns available metrics."""
        metrics = project_with_data.metrics

        assert "revenue" in metrics
        assert "order_count" in metrics

    def test_dimensions_property(self, project_with_data):
        """Test dimensions property returns available dimensions."""
        dimensions = project_with_data.dimensions

        assert "region" in dimensions
        assert "product" in dimensions

    def test_describe(self, project_with_data):
        """Test describe returns project info."""
        desc = project_with_data.describe()

        assert desc["project"] == "test_project"
        assert desc["engine"] == "pandas"
        assert "gold" in desc["connections"]
        assert len(desc["metrics"]) == 2
        assert len(desc["dimensions"]) == 2


class TestProjectLoadFromYAML:
    """Test Project.load() from YAML files."""

    @pytest.fixture
    def config_files(self, tmp_path):
        """Create temporary config files."""
        gold_path = tmp_path / "gold"
        gold_path.mkdir()
        stories_path = gold_path / "stories"
        stories_path.mkdir()

        odibi_config = {
            "project": "test_project",
            "engine": "pandas",
            "connections": {
                "gold": {"type": "local", "base_path": str(gold_path)},
            },
            "story": {"connection": "gold", "path": "stories"},
            "system": {"connection": "gold", "path": "_system"},
            "pipelines": [
                {
                    "pipeline": "test",
                    "nodes": [
                        {
                            "name": "test_node",
                            "read": {"connection": "gold", "path": "test", "format": "parquet"},
                            "write": {
                                "connection": "gold",
                                "path": "test_out",
                                "format": "parquet",
                            },
                        }
                    ],
                }
            ],
            "semantic": {"config": "semantic_config.yaml"},
        }

        semantic_config = {
            "metrics": [
                {
                    "name": "revenue",
                    "expr": "SUM(total_amount)",
                    "source": "fact_orders",
                },
            ],
            "dimensions": [
                {"name": "region", "source": "fact_orders", "column": "region"},
            ],
        }

        odibi_path = tmp_path / "odibi.yaml"
        semantic_path = tmp_path / "semantic_config.yaml"

        with open(odibi_path, "w") as f:
            yaml.dump(odibi_config, f)

        with open(semantic_path, "w") as f:
            yaml.dump(semantic_config, f)

        return {"odibi": str(odibi_path), "semantic": str(semantic_path), "tmp": tmp_path}

    def test_load_with_external_semantic_file(self, config_files):
        """Test loading project with external semantic config file."""
        project = Project.load(config_files["odibi"])

        assert project.config.project == "test_project"
        assert project.semantic_config is not None
        assert len(project.metrics) == 1
        assert "revenue" in project.metrics

    def test_load_with_inline_semantic(self, tmp_path):
        """Test loading project with inline semantic config."""
        gold_path = tmp_path / "gold"
        gold_path.mkdir()
        stories_path = gold_path / "stories"
        stories_path.mkdir()

        config = {
            "project": "inline_test",
            "engine": "pandas",
            "connections": {
                "gold": {"type": "local", "base_path": str(gold_path)},
            },
            "story": {"connection": "gold", "path": "stories"},
            "system": {"connection": "gold", "path": "_system"},
            "pipelines": [
                {
                    "pipeline": "test",
                    "nodes": [
                        {
                            "name": "test_node",
                            "read": {"connection": "gold", "path": "test", "format": "parquet"},
                            "write": {
                                "connection": "gold",
                                "path": "test_out",
                                "format": "parquet",
                            },
                        }
                    ],
                }
            ],
            "semantic": {
                "metrics": [
                    {"name": "orders", "expr": "COUNT(*)", "source": "fact_orders"},
                ],
                "dimensions": [],
            },
        }

        config_path = tmp_path / "odibi.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        project = Project.load(str(config_path))

        assert project.config.project == "inline_test"
        assert "orders" in project.metrics

    def test_load_without_semantic(self, tmp_path):
        """Test loading project without semantic configuration."""
        gold_path = tmp_path / "gold"
        gold_path.mkdir()
        stories_path = gold_path / "stories"
        stories_path.mkdir()

        config = {
            "project": "no_semantic",
            "engine": "pandas",
            "connections": {
                "gold": {"type": "local", "base_path": str(gold_path)},
            },
            "story": {"connection": "gold", "path": "stories"},
            "system": {"connection": "gold", "path": "_system"},
            "pipelines": [
                {
                    "pipeline": "test",
                    "nodes": [
                        {
                            "name": "test_node",
                            "read": {"connection": "gold", "path": "test", "format": "parquet"},
                            "write": {
                                "connection": "gold",
                                "path": "test_out",
                                "format": "parquet",
                            },
                        }
                    ],
                }
            ],
        }

        config_path = tmp_path / "odibi.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        project = Project.load(str(config_path))

        assert project.semantic_config is None
        assert project.metrics == []

    def test_query_without_semantic_raises_error(self, tmp_path):
        """Test that querying without semantic config raises helpful error."""
        gold_path = tmp_path / "gold"
        gold_path.mkdir()
        stories_path = gold_path / "stories"
        stories_path.mkdir()

        config = {
            "project": "no_semantic",
            "engine": "pandas",
            "connections": {
                "gold": {"type": "local", "base_path": str(gold_path)},
            },
            "story": {"connection": "gold", "path": "stories"},
            "system": {"connection": "gold", "path": "_system"},
            "pipelines": [
                {
                    "pipeline": "test",
                    "nodes": [
                        {
                            "name": "test_node",
                            "read": {"connection": "gold", "path": "test", "format": "parquet"},
                            "write": {
                                "connection": "gold",
                                "path": "test_out",
                                "format": "parquet",
                            },
                        }
                    ],
                }
            ],
        }

        config_path = tmp_path / "odibi.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        project = Project.load(str(config_path))

        with pytest.raises(ValueError, match="Semantic layer not configured"):
            project.query("revenue BY region")


class TestProjectSourceResolution:
    """Test Project source resolution and table loading."""

    def test_get_sources_for_query(self, tmp_path):
        """Test extracting sources needed for a query."""
        gold_path = tmp_path / "gold"
        gold_path.mkdir()

        config = ProjectConfig(
            project="test",
            engine="pandas",
            connections={
                "gold": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path=str(gold_path))
            },
            story={"connection": "gold", "path": "stories"},
            system={"connection": "gold", "path": "_system"},
            pipelines=[
                {
                    "pipeline": "test",
                    "nodes": [
                        {
                            "name": "test_node",
                            "read": {"connection": "gold", "path": "test", "format": "parquet"},
                            "write": {
                                "connection": "gold",
                                "path": "test_out",
                                "format": "parquet",
                            },
                        }
                    ],
                }
            ],
        )

        semantic_config = SemanticLayerConfig(
            metrics=[
                MetricDefinition(
                    name="revenue", expr="SUM(total_amount)", source="gold.fact_orders"
                ),
            ],
            dimensions=[
                DimensionDefinition(name="region", source="gold.dim_customer", column="region"),
            ],
        )

        project = Project(config=config, semantic_config=semantic_config)
        sources = project._get_sources_for_query("revenue BY region")

        assert "gold.fact_orders" in sources
        assert "gold.dim_customer" in sources


class TestProjectRegister:
    """Test manual table registration."""

    def test_register_table(self, tmp_path):
        """Test manually registering a table."""
        gold_path = tmp_path / "gold"
        gold_path.mkdir()

        config = ProjectConfig(
            project="test",
            engine="pandas",
            connections={
                "gold": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path=str(gold_path))
            },
            story={"connection": "gold", "path": "stories"},
            system={"connection": "gold", "path": "_system"},
            pipelines=[
                {
                    "pipeline": "test",
                    "nodes": [
                        {
                            "name": "test_node",
                            "read": {"connection": "gold", "path": "test", "format": "parquet"},
                            "write": {
                                "connection": "gold",
                                "path": "test_out",
                                "format": "parquet",
                            },
                        }
                    ],
                }
            ],
        )

        semantic_config = SemanticLayerConfig(
            metrics=[
                MetricDefinition(name="revenue", expr="SUM(amount)", source="my_table"),
            ],
            dimensions=[],
        )

        project = Project(config=config, semantic_config=semantic_config)

        df = pd.DataFrame({"id": [1, 2], "amount": [100, 200]})
        project.register("my_table", df)

        result = project.query("revenue")
        assert result.df["revenue"].iloc[0] == 300

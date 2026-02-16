"""Unit tests for Project class - unified semantic layer integration."""

import logging

logging.getLogger("odibi").propagate = False

import pandas as pd  # noqa: E402
import pytest  # noqa: E402
import yaml  # noqa: E402
from unittest.mock import MagicMock  # noqa: E402

from odibi.project import Project, SourceResolver  # noqa: E402
from odibi.config import ProjectConfig, LocalConnectionConfig, ConnectionType  # noqa: E402
from odibi.semantics.metrics import (  # noqa: E402
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


# ============================================================
# New coverage tests
# ============================================================


class TestSourceResolverBuildPath:
    """Test SourceResolver._build_path edge cases."""

    def test_build_path_with_catalog_and_schema(self):
        """Unity Catalog connection returns catalog.schema.table_name."""
        mock_conn = MagicMock()
        mock_conn.model_dump.return_value = {
            "catalog": "my_catalog",
            "schema": "my_schema",
        }
        connections = {"uc": mock_conn}
        resolver = SourceResolver(connections)

        path = resolver._build_path(mock_conn, "fact_orders")

        assert path == "my_catalog.my_schema.fact_orders"

    def test_build_path_with_path_field(self):
        """Connection with 'path' instead of 'base_path'."""
        mock_conn = MagicMock()
        mock_conn.model_dump.return_value = {"path": "/mnt/data"}
        connections = {"store": mock_conn}
        resolver = SourceResolver(connections)

        path = resolver._build_path(mock_conn, "orders")

        assert path.replace("\\", "/") == "/mnt/data/orders"

    def test_build_path_no_base_no_path_no_catalog(self):
        """Connection with none of base_path, path, or catalog returns just table_name."""
        mock_conn = MagicMock()
        mock_conn.model_dump.return_value = {"type": "local"}
        connections = {"bare": mock_conn}
        resolver = SourceResolver(connections)

        path = resolver._build_path(mock_conn, "my_table")

        assert path == "my_table"

    def test_build_path_absolute_base_path_skips_join(self):
        """Absolute base_path is not joined with resolver base_path."""
        connections = {
            "abs": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path="/absolute/gold"),
        }
        resolver = SourceResolver(connections, base_path="/project")

        _, full_path = resolver.resolve("abs.orders")

        assert "/project" not in full_path.replace("\\", "/")
        assert "absolute" in full_path


class TestSourceResolverFindDefaultConnection:
    """Test _find_default_connection priority logic."""

    def _make_conn(self):
        return LocalConnectionConfig(type=ConnectionType.LOCAL, base_path="/data")

    def test_silver_priority_when_no_gold(self):
        """Silver is chosen when gold is absent."""
        connections = {"bronze": self._make_conn(), "silver": self._make_conn()}
        resolver = SourceResolver(connections)

        conn_name, _ = resolver.resolve("orders")

        assert conn_name == "silver"

    def test_bronze_priority_when_no_gold_silver(self):
        """Bronze is chosen when gold and silver are absent."""
        connections = {
            "bronze": self._make_conn(),
            "staging": self._make_conn(),
        }
        resolver = SourceResolver(connections)

        conn_name, _ = resolver.resolve("orders")

        assert conn_name == "bronze"

    def test_warehouse_priority(self):
        """'warehouse' is chosen when gold/silver/bronze are absent."""
        connections = {
            "warehouse": self._make_conn(),
            "archive": self._make_conn(),
        }
        resolver = SourceResolver(connections)

        conn_name, _ = resolver.resolve("orders")

        assert conn_name == "warehouse"

    def test_default_priority(self):
        """'default' is chosen when gold/silver/bronze/warehouse are absent."""
        connections = {
            "default": self._make_conn(),
            "other": self._make_conn(),
        }
        resolver = SourceResolver(connections)

        conn_name, _ = resolver.resolve("orders")

        assert conn_name == "default"

    def test_fallback_to_first_connection(self):
        """Falls back to first connection when no priority names match."""
        connections = {
            "custom_a": self._make_conn(),
            "custom_b": self._make_conn(),
        }
        resolver = SourceResolver(connections)

        conn_name, _ = resolver.resolve("orders")

        assert conn_name == "custom_a"


class TestSourceResolverNodeReferenceEdgeCases:
    """Test _resolve_node_reference edge cases."""

    def _base_connections(self):
        return {
            "gold": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path="/data/gold"),
        }

    def test_node_write_no_connection(self):
        """Node write config without 'connection' raises ValueError."""
        pipelines = [
            {
                "pipeline": "p1",
                "nodes": [
                    {"name": "n1", "write": {"table": "t1"}},
                ],
            }
        ]
        resolver = SourceResolver(self._base_connections(), pipelines=pipelines)

        with pytest.raises(ValueError, match="no 'connection'"):
            resolver.resolve("$p1.n1")

    def test_node_write_no_table_no_path(self):
        """Node write config without 'table' or 'path' raises ValueError."""
        pipelines = [
            {
                "pipeline": "p1",
                "nodes": [
                    {"name": "n1", "write": {"connection": "gold"}},
                ],
            }
        ]
        resolver = SourceResolver(self._base_connections(), pipelines=pipelines)

        with pytest.raises(ValueError, match="no 'table' or 'path'"):
            resolver.resolve("$p1.n1")

    def test_node_write_connection_not_found(self):
        """Node write config referencing unknown connection raises ValueError."""
        pipelines = [
            {
                "pipeline": "p1",
                "nodes": [
                    {
                        "name": "n1",
                        "write": {"connection": "missing", "table": "t1"},
                    },
                ],
            }
        ]
        resolver = SourceResolver(self._base_connections(), pipelines=pipelines)

        with pytest.raises(ValueError, match="not found"):
            resolver.resolve("$p1.n1")

    def test_node_write_uses_path_instead_of_table(self):
        """Node write config with 'path' instead of 'table' resolves correctly."""
        pipelines = [
            {
                "pipeline": "p1",
                "nodes": [
                    {
                        "name": "n1",
                        "write": {"connection": "gold", "path": "output/orders"},
                    },
                ],
            }
        ]
        resolver = SourceResolver(self._base_connections(), pipelines=pipelines)

        conn_name, full_path = resolver.resolve("$p1.n1")

        assert conn_name == "gold"
        assert "output" in full_path
        assert "orders" in full_path


class TestProjectProperties:
    """Test Project property methods."""

    @pytest.fixture
    def minimal_config(self, tmp_path):
        gold_path = tmp_path / "gold"
        gold_path.mkdir()
        return ProjectConfig(
            project="props_test",
            engine="pandas",
            connections={
                "gold": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path=str(gold_path)),
            },
            story={"connection": "gold", "path": "stories"},
            system={"connection": "gold", "path": "_system"},
            pipelines=[
                {
                    "pipeline": "test",
                    "nodes": [
                        {
                            "name": "n",
                            "read": {
                                "connection": "gold",
                                "path": "r",
                                "format": "parquet",
                            },
                            "write": {
                                "connection": "gold",
                                "path": "w",
                                "format": "parquet",
                            },
                        }
                    ],
                }
            ],
        )

    def test_connections_property(self, minimal_config):
        """connections property returns the config connections dict."""
        project = Project(config=minimal_config)

        assert "gold" in project.connections
        assert project.connections is minimal_config.connections

    def test_metrics_without_semantic(self, minimal_config):
        """metrics returns empty list when no semantic config."""
        project = Project(config=minimal_config)

        assert project.metrics == []

    def test_dimensions_without_semantic(self, minimal_config):
        """dimensions returns empty list when no semantic config."""
        project = Project(config=minimal_config)

        assert project.dimensions == []

    def test_describe_without_semantic(self, minimal_config):
        """describe() returns empty metrics/dimensions when no semantic config."""
        project = Project(config=minimal_config)
        desc = project.describe()

        assert desc["project"] == "props_test"
        assert desc["engine"] == "pandas"
        assert desc["metrics"] == []
        assert desc["dimensions"] == []
        assert "gold" in desc["connections"]


class TestProjectReadPandas:
    """Test Project._read_pandas file reading."""

    def test_read_csv(self, tmp_path):
        """Reads .csv files via pandas."""
        csv_path = tmp_path / "data.csv"
        pd.DataFrame({"a": [1, 2], "b": [3, 4]}).to_csv(csv_path, index=False)

        gold_path = tmp_path / "gold"
        gold_path.mkdir()
        config = ProjectConfig(
            project="csv_test",
            engine="pandas",
            connections={
                "gold": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path=str(tmp_path))
            },
            story={"connection": "gold", "path": "stories"},
            system={"connection": "gold", "path": "_system"},
            pipelines=[
                {
                    "pipeline": "t",
                    "nodes": [
                        {
                            "name": "n",
                            "read": {
                                "connection": "gold",
                                "path": "r",
                                "format": "parquet",
                            },
                            "write": {
                                "connection": "gold",
                                "path": "w",
                                "format": "parquet",
                            },
                        }
                    ],
                }
            ],
        )
        project = Project(config=config)
        conn = config.connections["gold"]
        conn_dict = conn.model_dump()

        df = project._read_pandas(conn_dict, str(csv_path), "local")

        assert list(df.columns) == ["a", "b"]
        assert len(df) == 2

    def test_read_parquet(self, tmp_path):
        """Reads .parquet files via pandas."""
        pq_path = tmp_path / "data.parquet"
        pd.DataFrame({"x": [10, 20]}).to_parquet(pq_path, index=False)

        gold_path = tmp_path / "gold"
        gold_path.mkdir()
        config = ProjectConfig(
            project="pq_test",
            engine="pandas",
            connections={
                "gold": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path=str(tmp_path))
            },
            story={"connection": "gold", "path": "stories"},
            system={"connection": "gold", "path": "_system"},
            pipelines=[
                {
                    "pipeline": "t",
                    "nodes": [
                        {
                            "name": "n",
                            "read": {
                                "connection": "gold",
                                "path": "r",
                                "format": "parquet",
                            },
                            "write": {
                                "connection": "gold",
                                "path": "w",
                                "format": "parquet",
                            },
                        }
                    ],
                }
            ],
        )
        project = Project(config=config)
        conn_dict = config.connections["gold"].model_dump()

        df = project._read_pandas(conn_dict, str(pq_path), "local")

        assert list(df.columns) == ["x"]
        assert len(df) == 2

    def test_read_directory_as_parquet(self, tmp_path):
        """Reads a directory (treated as parquet dataset)."""
        data_dir = tmp_path / "partitioned"
        data_dir.mkdir()
        pd.DataFrame({"v": [1]}).to_parquet(data_dir / "part0.parquet", index=False)

        gold_path = tmp_path / "gold"
        gold_path.mkdir()
        config = ProjectConfig(
            project="dir_test",
            engine="pandas",
            connections={
                "gold": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path=str(tmp_path))
            },
            story={"connection": "gold", "path": "stories"},
            system={"connection": "gold", "path": "_system"},
            pipelines=[
                {
                    "pipeline": "t",
                    "nodes": [
                        {
                            "name": "n",
                            "read": {
                                "connection": "gold",
                                "path": "r",
                                "format": "parquet",
                            },
                            "write": {
                                "connection": "gold",
                                "path": "w",
                                "format": "parquet",
                            },
                        }
                    ],
                }
            ],
        )
        project = Project(config=config)
        conn_dict = config.connections["gold"].model_dump()

        df = project._read_pandas(conn_dict, str(data_dir), "local")

        assert "v" in df.columns
        assert len(df) == 1


class TestProjectReadPolars:
    """Test Project._read_polars file reading."""

    def test_read_csv_polars(self, tmp_path):
        """Reads .csv files via polars."""
        pl = pytest.importorskip("polars")

        csv_path = tmp_path / "data.csv"
        pd.DataFrame({"a": [1, 2], "b": [3, 4]}).to_csv(csv_path, index=False)

        gold_path = tmp_path / "gold"
        gold_path.mkdir()
        config = ProjectConfig(
            project="pl_csv",
            engine="polars",
            connections={
                "gold": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path=str(tmp_path))
            },
            story={"connection": "gold", "path": "stories"},
            system={"connection": "gold", "path": "_system"},
            pipelines=[
                {
                    "pipeline": "t",
                    "nodes": [
                        {
                            "name": "n",
                            "read": {
                                "connection": "gold",
                                "path": "r",
                                "format": "parquet",
                            },
                            "write": {
                                "connection": "gold",
                                "path": "w",
                                "format": "parquet",
                            },
                        }
                    ],
                }
            ],
        )
        project = Project(config=config)
        conn_dict = config.connections["gold"].model_dump()

        df = project._read_polars(conn_dict, str(csv_path), "local")

        assert isinstance(df, pl.DataFrame)
        assert df.columns == ["a", "b"]
        assert len(df) == 2

    def test_read_parquet_polars(self, tmp_path):
        """Reads .parquet files via polars."""
        pl = pytest.importorskip("polars")

        pq_path = tmp_path / "data.parquet"
        pd.DataFrame({"x": [10, 20]}).to_parquet(pq_path, index=False)

        gold_path = tmp_path / "gold"
        gold_path.mkdir()
        config = ProjectConfig(
            project="pl_pq",
            engine="polars",
            connections={
                "gold": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path=str(tmp_path))
            },
            story={"connection": "gold", "path": "stories"},
            system={"connection": "gold", "path": "_system"},
            pipelines=[
                {
                    "pipeline": "t",
                    "nodes": [
                        {
                            "name": "n",
                            "read": {
                                "connection": "gold",
                                "path": "r",
                                "format": "parquet",
                            },
                            "write": {
                                "connection": "gold",
                                "path": "w",
                                "format": "parquet",
                            },
                        }
                    ],
                }
            ],
        )
        project = Project(config=config)
        conn_dict = config.connections["gold"].model_dump()

        df = project._read_polars(conn_dict, str(pq_path), "local")

        assert isinstance(df, pl.DataFrame)
        assert df.columns == ["x"]
        assert len(df) == 2

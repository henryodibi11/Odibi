"""Tests for semantic story generation (V2 Phase 3)."""

import json

import pytest

from odibi.semantics.metrics import (
    DimensionDefinition,
    MetricDefinition,
    MetricType,
    SemanticLayerConfig,
    TimeGrain,
    ViewConfig,
)
from odibi.semantics.story import (
    SemanticStoryGenerator,
    SemanticStoryMetadata,
    ViewExecutionMetadata,
)


@pytest.fixture
def sample_config():
    """Create a sample semantic layer config for testing."""
    return SemanticLayerConfig(
        metrics=[
            MetricDefinition(
                name="total_revenue",
                expr="SUM(revenue)",
                source="gold.sales_fact",
            ),
            MetricDefinition(
                name="total_cost",
                expr="SUM(cost)",
                source="gold.sales_fact",
            ),
            MetricDefinition(
                name="profit_margin",
                type=MetricType.DERIVED,
                components=["total_revenue", "total_cost"],
                formula="(total_revenue - total_cost) / total_revenue",
                description="Profit margin percentage",
            ),
        ],
        dimensions=[
            DimensionDefinition(name="date", column="order_date"),
            DimensionDefinition(name="month", column="order_date", grain=TimeGrain.MONTH),
            DimensionDefinition(name="region", column="region_id"),
        ],
        views=[
            ViewConfig(
                name="vw_sales_daily",
                description="Daily sales metrics",
                metrics=["total_revenue", "total_cost"],
                dimensions=["date", "region"],
            ),
            ViewConfig(
                name="vw_profit_monthly",
                description="Monthly profit margin",
                metrics=["profit_margin"],
                dimensions=["month", "region"],
            ),
        ],
    )


class TestViewExecutionMetadata:
    """Test ViewExecutionMetadata dataclass."""

    def test_create_metadata(self):
        """Test creating view execution metadata."""
        metadata = ViewExecutionMetadata(
            view_name="vw_test",
            source_table="gold.fact",
            status="success",
            duration=1.5,
            sql_generated="CREATE VIEW...",
            metrics_included=["revenue"],
            dimensions_included=["date"],
        )
        assert metadata.view_name == "vw_test"
        assert metadata.status == "success"
        assert metadata.error_message is None


class TestSemanticStoryMetadata:
    """Test SemanticStoryMetadata dataclass."""

    def test_to_dict(self):
        """Test converting metadata to dictionary."""
        view_meta = ViewExecutionMetadata(
            view_name="vw_test",
            source_table="gold.fact",
            status="success",
            duration=1.0,
            sql_generated="CREATE VIEW...",
        )
        metadata = SemanticStoryMetadata(
            name="test_semantic",
            started_at="2026-01-02T10:00:00",
            completed_at="2026-01-02T10:00:01",
            duration=1.0,
            views=[view_meta],
            views_created=1,
            views_failed=0,
            sql_files_saved=[],
            graph_data={"nodes": [], "edges": []},
        )

        result = metadata.to_dict()

        assert result["name"] == "test_semantic"
        assert result["views_created"] == 1
        assert len(result["views"]) == 1
        assert "graph_data" in result


class TestSemanticStoryGenerator:
    """Test SemanticStoryGenerator class."""

    def test_execute_with_story_success(self, sample_config):
        """Test executing views and generating story."""
        generator = SemanticStoryGenerator(sample_config, name="test_layer")
        executed_sqls = []

        def mock_execute(sql):
            executed_sqls.append(sql)

        metadata = generator.execute_with_story(mock_execute)

        assert metadata.name == "test_layer"
        assert metadata.views_created == 2
        assert metadata.views_failed == 0
        assert len(metadata.views) == 2
        assert len(executed_sqls) == 4  # 2 schema + 2 views

    def test_execute_with_story_partial_failure(self, sample_config):
        """Test story generation with some view failures."""
        generator = SemanticStoryGenerator(sample_config, name="test_layer")
        call_count = [0]

        def mock_execute_with_failure(sql):
            call_count[0] += 1
            # Fail on second call (first view DDL, after first schema creation)
            if call_count[0] == 2:
                raise Exception("Database error")

        metadata = generator.execute_with_story(mock_execute_with_failure)

        assert metadata.views_created == 1
        assert metadata.views_failed == 1
        failed_view = next(v for v in metadata.views if v.status == "failed")
        assert "Database error" in failed_view.error_message

    def test_graph_data_structure(self, sample_config):
        """Test that graph data has correct structure."""
        generator = SemanticStoryGenerator(sample_config, name="test_layer")

        def mock_execute(sql):
            pass

        metadata = generator.execute_with_story(mock_execute)

        assert "nodes" in metadata.graph_data
        assert "edges" in metadata.graph_data

        nodes = metadata.graph_data["nodes"]
        edges = metadata.graph_data["edges"]

        view_nodes = [n for n in nodes if n["type"] == "view"]
        table_nodes = [n for n in nodes if n["type"] == "table"]

        assert len(view_nodes) == 2
        assert len(table_nodes) == 1
        assert table_nodes[0]["id"] == "gold.sales_fact"
        assert len(edges) == 2

    def test_graph_data_edges_correct(self, sample_config):
        """Test that edges connect source to views."""
        generator = SemanticStoryGenerator(sample_config, name="test_layer")

        def mock_execute(sql):
            pass

        metadata = generator.execute_with_story(mock_execute)
        edges = metadata.graph_data["edges"]

        for edge in edges:
            assert edge["from"] == "gold.sales_fact"
            assert edge["to"] in ["vw_sales_daily", "vw_profit_monthly"]


class TestStoryRendering:
    """Test story rendering to JSON and HTML."""

    def test_render_json(self, sample_config):
        """Test JSON rendering."""
        generator = SemanticStoryGenerator(sample_config)

        def mock_execute(sql):
            pass

        generator.execute_with_story(mock_execute)
        json_str = generator.render_json()

        parsed = json.loads(json_str)
        assert "name" in parsed
        assert "views" in parsed
        assert "graph_data" in parsed

    def test_render_html(self, sample_config):
        """Test HTML rendering."""
        generator = SemanticStoryGenerator(sample_config)

        def mock_execute(sql):
            pass

        generator.execute_with_story(mock_execute)
        html = generator.render_html()

        assert "<!DOCTYPE html>" in html
        assert "Semantic Layer" in html
        assert "vw_sales_daily" in html
        assert "vw_profit_monthly" in html
        assert "mermaid" in html

    def test_html_contains_mermaid_diagram(self, sample_config):
        """Test that HTML includes mermaid lineage diagram."""
        generator = SemanticStoryGenerator(sample_config)

        def mock_execute(sql):
            pass

        generator.execute_with_story(mock_execute)
        html = generator.render_html()

        assert "graph LR" in html
        assert "gold_sales_fact" in html

    def test_html_contains_sql(self, sample_config):
        """Test that HTML includes generated SQL."""
        generator = SemanticStoryGenerator(sample_config)

        def mock_execute(sql):
            pass

        generator.execute_with_story(mock_execute)
        html = generator.render_html()

        assert "CREATE OR ALTER VIEW" in html
        assert "SUM(revenue)" in html

    def test_render_without_execute_raises(self, sample_config):
        """Test that rendering without execution raises error."""
        generator = SemanticStoryGenerator(sample_config)

        with pytest.raises(ValueError, match="No story metadata"):
            generator.render_json()

        with pytest.raises(ValueError, match="No story metadata"):
            generator.render_html()


class TestStorySaving:
    """Test story file saving."""

    def test_save_story_local(self, sample_config, tmp_path):
        """Test saving story to local filesystem."""
        generator = SemanticStoryGenerator(
            sample_config,
            name="test_layer",
            output_path=str(tmp_path),
        )

        def mock_execute(sql):
            pass

        generator.execute_with_story(mock_execute)
        paths = generator.save_story()

        assert "json" in paths
        assert "html" in paths
        assert paths["json"].endswith(".json")
        assert paths["html"].endswith(".html")

    def test_save_story_with_write_callback(self, sample_config):
        """Test saving story with custom write callback."""
        generator = SemanticStoryGenerator(
            sample_config,
            name="test_layer",
            output_path="abfss://container@storage/stories",
        )

        def mock_execute(sql):
            pass

        saved_files = {}

        def mock_write(path, content):
            saved_files[path] = content

        generator.execute_with_story(mock_execute)
        generator.save_story(write_file=mock_write)

        assert len(saved_files) == 2
        assert any(".json" in p for p in saved_files.keys())
        assert any(".html" in p for p in saved_files.keys())

    def test_save_without_execute_raises(self, sample_config):
        """Test that saving without execution raises error."""
        generator = SemanticStoryGenerator(sample_config)

        with pytest.raises(ValueError, match="No story metadata"):
            generator.save_story()


class TestStoryMetadataAccess:
    """Test accessing story metadata."""

    def test_metadata_property(self, sample_config):
        """Test accessing metadata property."""
        generator = SemanticStoryGenerator(sample_config)

        assert generator.metadata is None

        def mock_execute(sql):
            pass

        generator.execute_with_story(mock_execute)

        assert generator.metadata is not None
        assert generator.metadata.name == "semantic_layer"

    def test_view_metadata_includes_details(self, sample_config):
        """Test that view metadata includes all details."""
        generator = SemanticStoryGenerator(sample_config)

        def mock_execute(sql):
            pass

        generator.execute_with_story(mock_execute)

        view = next(v for v in generator.metadata.views if v.view_name == "vw_sales_daily")

        assert view.source_table == "gold.sales_fact"
        assert "total_revenue" in view.metrics_included
        assert "date" in view.dimensions_included
        assert view.status == "success"

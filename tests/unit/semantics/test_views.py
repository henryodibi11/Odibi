"""Tests for semantic view generation (V2 Phase 2)."""

import pytest

from odibi.semantics.metrics import (
    DimensionDefinition,
    MetricDefinition,
    MetricType,
    SemanticLayerConfig,
    TimeGrain,
    ViewConfig,
)
from odibi.semantics.views import ViewGenerator


class TestViewConfig:
    """Test ViewConfig validation."""

    def test_valid_view_config(self):
        """Test creating a valid view config."""
        view = ViewConfig(
            name="vw_sales_daily",
            description="Daily sales metrics",
            metrics=["revenue", "order_count"],
            dimensions=["date", "region"],
        )
        assert view.name == "vw_sales_daily"
        assert view.db_schema == "semantic"

    def test_view_requires_name(self):
        """Test that empty name raises error."""
        with pytest.raises(ValueError, match="empty"):
            ViewConfig(
                name="",
                metrics=["revenue"],
                dimensions=["date"],
            )

    def test_view_requires_metrics(self):
        """Test that empty metrics raises error."""
        with pytest.raises(ValueError, match="metric"):
            ViewConfig(
                name="vw_test",
                metrics=[],
                dimensions=["date"],
            )

    def test_view_requires_dimensions(self):
        """Test that empty dimensions raises error."""
        with pytest.raises(ValueError, match="dimension"):
            ViewConfig(
                name="vw_test",
                metrics=["revenue"],
                dimensions=[],
            )


class TestViewGenerator:
    """Test ViewGenerator DDL generation."""

    @pytest.fixture
    def config_with_views(self):
        """Create a config with metrics, dimensions, and views."""
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
                    name="order_count",
                    expr="COUNT(*)",
                    source="gold.sales_fact",
                ),
                MetricDefinition(
                    name="profit_margin",
                    type=MetricType.DERIVED,
                    components=["total_revenue", "total_cost"],
                    formula="(total_revenue - total_cost) / total_revenue",
                    description="Profit margin = (Revenue - Cost) / Revenue",
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
                    description="Daily sales metrics by region",
                    metrics=["total_revenue", "order_count"],
                    dimensions=["date", "region"],
                ),
                ViewConfig(
                    name="vw_profit_monthly",
                    description="Monthly profit margin rollup",
                    metrics=["total_revenue", "total_cost", "profit_margin"],
                    dimensions=["month", "region"],
                ),
            ],
        )

    def test_generate_simple_view_ddl(self, config_with_views):
        """Test generating DDL for a simple view."""
        generator = ViewGenerator(config_with_views)
        view_config = generator.get_view("vw_sales_daily")

        ddl = generator.generate_view_ddl(view_config)

        assert "CREATE OR ALTER VIEW semantic.vw_sales_daily AS" in ddl
        assert "SUM(revenue) AS total_revenue" in ddl
        assert "COUNT(*) AS order_count" in ddl
        assert "order_date AS date" in ddl
        assert "region_id AS region" in ddl
        assert "gold.sales_fact" in ddl
        assert "GROUP BY" in ddl

    def test_generate_derived_view_ddl(self, config_with_views):
        """Test generating DDL with derived metrics."""
        generator = ViewGenerator(config_with_views)
        view_config = generator.get_view("vw_profit_monthly")

        ddl = generator.generate_view_ddl(view_config)

        assert "CREATE OR ALTER VIEW semantic.vw_profit_monthly AS" in ddl
        assert "SUM(revenue)" in ddl
        assert "SUM(cost)" in ddl
        assert "NULLIF" in ddl
        assert "profit_margin" in ddl
        assert "DATETRUNC(month, order_date)" in ddl

    def test_ddl_header_contains_documentation(self, config_with_views):
        """Test that DDL includes documentation header."""
        generator = ViewGenerator(config_with_views)
        view_config = generator.get_view("vw_profit_monthly")

        ddl = generator.generate_view_ddl(view_config)

        assert "View: semantic.vw_profit_monthly" in ddl
        assert "Description: Monthly profit margin rollup" in ddl
        assert "Generated:" in ddl
        assert "Metrics included:" in ddl
        assert "profit_margin:" in ddl
        assert "Formula:" in ddl

    def test_time_grain_transformation(self, config_with_views):
        """Test that time grain dimensions use DATETRUNC."""
        generator = ViewGenerator(config_with_views)
        view_config = generator.get_view("vw_profit_monthly")

        ddl = generator.generate_view_ddl(view_config)

        assert "DATETRUNC(month, order_date) AS month" in ddl

    def test_nullif_in_derived_metrics(self, config_with_views):
        """Test that division is protected with NULLIF."""
        generator = ViewGenerator(config_with_views)
        view_config = generator.get_view("vw_profit_monthly")

        ddl = generator.generate_view_ddl(view_config)

        assert "NULLIF(" in ddl
        assert ", 0)" in ddl

    def test_list_views(self, config_with_views):
        """Test listing all configured views."""
        generator = ViewGenerator(config_with_views)
        views = generator.list_views()

        assert len(views) == 2
        assert views[0]["name"] == "vw_sales_daily"
        assert views[1]["name"] == "vw_profit_monthly"

    def test_get_view(self, config_with_views):
        """Test getting a view by name."""
        generator = ViewGenerator(config_with_views)

        view = generator.get_view("vw_sales_daily")
        assert view is not None
        assert view.name == "vw_sales_daily"

        view = generator.get_view("nonexistent")
        assert view is None


class TestViewExecution:
    """Test view execution."""

    @pytest.fixture
    def simple_config(self):
        """Create a simple config for testing execution."""
        return SemanticLayerConfig(
            metrics=[
                MetricDefinition(
                    name="revenue",
                    expr="SUM(amount)",
                    source="gold.sales",
                ),
            ],
            dimensions=[
                DimensionDefinition(name="region", column="region"),
            ],
            views=[
                ViewConfig(
                    name="vw_test",
                    metrics=["revenue"],
                    dimensions=["region"],
                ),
            ],
        )

    def test_execute_view_success(self, simple_config):
        """Test successful view execution."""
        generator = ViewGenerator(simple_config)
        view_config = generator.get_view("vw_test")

        executed_sql = []

        def mock_execute(sql):
            executed_sql.append(sql)

        result = generator.execute_view(view_config, mock_execute)

        assert result.success
        assert result.name == "vw_test"
        assert "CREATE OR ALTER VIEW" in result.sql
        assert len(executed_sql) == 2  # schema creation + view DDL
        assert "CREATE SCHEMA" in executed_sql[0]
        assert "CREATE OR ALTER VIEW" in executed_sql[1]

    def test_execute_view_with_save(self, simple_config):
        """Test view execution with SQL file save."""
        generator = ViewGenerator(simple_config)
        view_config = generator.get_view("vw_test")

        saved_files = {}

        def mock_execute(sql):
            pass

        def mock_write(path, content):
            saved_files[path] = content

        result = generator.execute_view(
            view_config,
            mock_execute,
            save_sql_to="/gold/views",
            write_file=mock_write,
        )

        assert result.success
        assert result.sql_file_path == "/gold/views/vw_test.sql"
        assert "/gold/views/vw_test.sql" in saved_files

    def test_execute_view_failure(self, simple_config):
        """Test view execution handles errors."""
        generator = ViewGenerator(simple_config)
        view_config = generator.get_view("vw_test")

        def mock_execute(sql):
            raise Exception("Database error")

        result = generator.execute_view(view_config, mock_execute)

        assert not result.success
        assert "Database error" in result.error

    def test_execute_all_views(self, simple_config):
        """Test executing all views."""
        simple_config.views.append(
            ViewConfig(
                name="vw_test2",
                metrics=["revenue"],
                dimensions=["region"],
            )
        )

        generator = ViewGenerator(simple_config)
        executed = []

        def mock_execute(sql):
            executed.append(sql)

        result = generator.execute_all_views(mock_execute)

        assert len(result.views_created) == 2
        assert "vw_test" in result.views_created
        assert "vw_test2" in result.views_created
        assert len(result.errors) == 0


class TestTimeGrainTransformations:
    """Test different time grain transformations."""

    @pytest.fixture
    def grain_config(self):
        """Create config with various time grains."""
        return SemanticLayerConfig(
            metrics=[
                MetricDefinition(name="count", expr="COUNT(*)", source="fact"),
            ],
            dimensions=[
                DimensionDefinition(name="day", column="ts", grain=TimeGrain.DAY),
                DimensionDefinition(name="week", column="ts", grain=TimeGrain.WEEK),
                DimensionDefinition(name="month", column="ts", grain=TimeGrain.MONTH),
                DimensionDefinition(name="quarter", column="ts", grain=TimeGrain.QUARTER),
                DimensionDefinition(name="year", column="ts", grain=TimeGrain.YEAR),
            ],
        )

    def test_day_grain(self, grain_config):
        """Test day grain transformation."""
        view = ViewConfig(name="vw", metrics=["count"], dimensions=["day"])
        generator = ViewGenerator(grain_config)
        ddl = generator.generate_view_ddl(view)

        assert "DATETRUNC(day, ts) AS day" in ddl

    def test_week_grain(self, grain_config):
        """Test week grain transformation."""
        view = ViewConfig(name="vw", metrics=["count"], dimensions=["week"])
        generator = ViewGenerator(grain_config)
        ddl = generator.generate_view_ddl(view)

        assert "DATETRUNC(week, ts) AS week" in ddl

    def test_month_grain(self, grain_config):
        """Test month grain transformation."""
        view = ViewConfig(name="vw", metrics=["count"], dimensions=["month"])
        generator = ViewGenerator(grain_config)
        ddl = generator.generate_view_ddl(view)

        assert "DATETRUNC(month, ts) AS month" in ddl

    def test_quarter_grain(self, grain_config):
        """Test quarter grain transformation."""
        view = ViewConfig(name="vw", metrics=["count"], dimensions=["quarter"])
        generator = ViewGenerator(grain_config)
        ddl = generator.generate_view_ddl(view)

        assert "DATETRUNC(quarter, ts) AS quarter" in ddl

    def test_year_grain(self, grain_config):
        """Test year grain transformation."""
        view = ViewConfig(name="vw", metrics=["count"], dimensions=["year"])
        generator = ViewGenerator(grain_config)
        ddl = generator.generate_view_ddl(view)

        assert "DATETRUNC(year, ts) AS year" in ddl

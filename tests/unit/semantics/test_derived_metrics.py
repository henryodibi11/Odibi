"""Tests for derived metrics in the semantic layer (V2 Phase 1)."""

import pandas as pd
import pytest

from odibi.context import EngineContext, PandasContext

try:
    import polars as pl
    from odibi.context import PolarsContext

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    pl = None

from odibi.enums import EngineType
from odibi.semantics.metrics import (
    DimensionDefinition,
    MetricDefinition,
    MetricType,
    SemanticLayerConfig,
)
from odibi.semantics.query import SemanticQuery


def create_pandas_context(df):
    """Helper to create a PandasContext with a DataFrame."""
    pandas_ctx = PandasContext()
    pandas_ctx.register("gold.sales_fact", df)
    return EngineContext(
        context=pandas_ctx,
        df=df,
        engine_type=EngineType.PANDAS,
    )


def create_polars_context(df):
    """Helper to create a PolarsContext with a Polars DataFrame."""
    if not HAS_POLARS:
        pytest.skip("Polars not installed")
    polars_ctx = PolarsContext()
    polars_ctx.register("gold.sales_fact", df)
    return EngineContext(
        context=polars_ctx,
        df=df,
        engine_type=EngineType.POLARS,
    )


class TestDerivedMetricDefinition:
    """Test MetricDefinition with components and formula."""

    def test_derived_metric_requires_components(self):
        """Test that derived metrics require components list."""
        with pytest.raises(ValueError, match="requires 'components'"):
            MetricDefinition(
                name="profit_margin",
                type=MetricType.DERIVED,
                formula="(total_revenue - total_cost) / total_revenue",
            )

    def test_derived_metric_requires_formula(self):
        """Test that derived metrics require formula."""
        with pytest.raises(ValueError, match="requires 'formula'"):
            MetricDefinition(
                name="profit_margin",
                type=MetricType.DERIVED,
                components=["total_revenue", "total_cost"],
            )

    def test_simple_metric_requires_expr(self):
        """Test that simple metrics require expr."""
        with pytest.raises(ValueError, match="requires 'expr'"):
            MetricDefinition(
                name="revenue",
                type=MetricType.SIMPLE,
            )

    def test_valid_derived_metric(self):
        """Test creating a valid derived metric."""
        metric = MetricDefinition(
            name="profit_margin",
            description="Profit margin = (Revenue - Cost) / Revenue",
            type=MetricType.DERIVED,
            components=["total_revenue", "total_cost"],
            formula="(total_revenue - total_cost) / total_revenue",
        )
        assert metric.name == "profit_margin"
        assert metric.type == MetricType.DERIVED
        assert metric.components == ["total_revenue", "total_cost"]
        assert "total_revenue" in metric.formula


class TestDerivedMetricValidation:
    """Test validation of derived metric component references."""

    def test_validate_unknown_component(self):
        """Test validation catches unknown component references."""
        config = SemanticLayerConfig(
            metrics=[
                MetricDefinition(
                    name="total_revenue",
                    expr="SUM(revenue)",
                    source="gold.sales_fact",
                ),
                MetricDefinition(
                    name="profit_margin",
                    type=MetricType.DERIVED,
                    components=["total_revenue", "unknown_metric"],
                    formula="(total_revenue - unknown_metric) / total_revenue",
                ),
            ],
        )
        errors = config.validate_references()
        assert len(errors) == 1
        assert "unknown_metric" in errors[0]

    def test_validate_all_components_exist(self):
        """Test validation passes when all components exist."""
        config = SemanticLayerConfig(
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
                ),
            ],
        )
        errors = config.validate_references()
        assert len(errors) == 0


class TestDerivedMetricSQLGeneration:
    """Test SQL generation for derived metrics."""

    @pytest.fixture
    def config_with_derived(self):
        """Create a config with derived profit_margin metric."""
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
                ),
            ],
            dimensions=[
                DimensionDefinition(name="region", source="gold.sales_fact"),
                DimensionDefinition(name="month", source="gold.sales_fact"),
            ],
        )

    def test_generate_sql_derived_metric(self, config_with_derived):
        """Test SQL generation includes component aggregations and formula."""
        query = SemanticQuery(config_with_derived)
        parsed = query.parse("profit_margin BY region")
        sql, source = query.generate_sql(parsed)

        assert "SUM(revenue)" in sql
        assert "SUM(cost)" in sql
        assert "total_revenue" in sql.lower()
        assert "total_cost" in sql.lower()
        assert "NULLIF" in sql
        assert "gold.sales_fact" in source

    def test_generate_sql_nullif_protection(self, config_with_derived):
        """Test that divisors are wrapped with NULLIF."""
        query = SemanticQuery(config_with_derived)
        parsed = query.parse("profit_margin BY region, month")
        sql, _ = query.generate_sql(parsed)

        assert "NULLIF(" in sql


class TestDerivedMetricExecution:
    """Test execution of derived metrics with correct mathematical results."""

    @pytest.fixture
    def sales_data(self):
        """Create sample sales data for testing."""
        return pd.DataFrame(
            {
                "region": ["East", "East", "West", "West", "East", "West"],
                "month": ["Jan", "Jan", "Jan", "Jan", "Feb", "Feb"],
                "revenue": [100, 200, 150, 250, 300, 400],
                "cost": [60, 120, 100, 150, 180, 200],
            }
        )

    @pytest.fixture
    def profit_margin_config(self):
        """Create config for profit margin calculation."""
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
                ),
            ],
            dimensions=[
                DimensionDefinition(name="region", source="gold.sales_fact"),
                DimensionDefinition(name="month", source="gold.sales_fact"),
            ],
        )

    def test_derived_metric_by_region(self, sales_data, profit_margin_config):
        """Test derived metric calculation at region grain.

        East: revenue=100+200+300=600, cost=60+120+180=360
              profit_margin = (600-360)/600 = 0.4
        West: revenue=150+250+400=800, cost=100+150+200=450
              profit_margin = (800-450)/800 = 0.4375
        """
        context = create_pandas_context(sales_data)
        query = SemanticQuery(profit_margin_config)

        result = query.execute("profit_margin BY region", context)

        assert result.row_count == 2
        df = result.df

        east_row = df[df["region"] == "East"]
        west_row = df[df["region"] == "West"]

        expected_east = (600 - 360) / 600
        expected_west = (800 - 450) / 800

        assert abs(east_row["profit_margin"].iloc[0] - expected_east) < 0.0001
        assert abs(west_row["profit_margin"].iloc[0] - expected_west) < 0.0001

    def test_derived_metric_by_month(self, sales_data, profit_margin_config):
        """Test derived metric at month grain.

        Jan: revenue=100+200+150+250=700, cost=60+120+100+150=430
             profit_margin = (700-430)/700 = 0.3857
        Feb: revenue=300+400=700, cost=180+200=380
             profit_margin = (700-380)/700 = 0.4571
        """
        context = create_pandas_context(sales_data)
        query = SemanticQuery(profit_margin_config)

        result = query.execute("profit_margin BY month", context)

        assert result.row_count == 2
        df = result.df

        jan_row = df[df["month"] == "Jan"]
        feb_row = df[df["month"] == "Feb"]

        expected_jan = (700 - 430) / 700
        expected_feb = (700 - 380) / 700

        assert abs(jan_row["profit_margin"].iloc[0] - expected_jan) < 0.0001
        assert abs(feb_row["profit_margin"].iloc[0] - expected_feb) < 0.0001

    def test_correct_vs_incorrect_calculation(self, profit_margin_config):
        """Demonstrate the difference between correct and averaged calculation.

        This test proves the semantic layer correctly aggregates components
        FIRST, then applies the formula - not averaging pre-calculated ratios.

        Using data designed to show the difference:
        Day 1: revenue=100, cost=20 => margin=80%
        Day 2: revenue=1000, cost=500 => margin=50%

        WRONG: AVG(80%, 50%) = 65%
        CORRECT: (1100-520)/1100 = 52.7%
        """
        test_data = pd.DataFrame(
            {
                "region": ["East", "East"],
                "month": ["Jan", "Jan"],
                "revenue": [100, 1000],
                "cost": [20, 500],
            }
        )

        context = create_pandas_context(test_data)
        query = SemanticQuery(profit_margin_config)

        result = query.execute("total_revenue, total_cost, profit_margin BY region", context)
        df = result.df

        east = df[df["region"] == "East"]
        east_margin = east["profit_margin"].iloc[0]

        correct_margin = (1100 - 520) / 1100

        row1_margin = (100 - 20) / 100
        row2_margin = (1000 - 500) / 1000
        wrong_avg = (row1_margin + row2_margin) / 2

        assert abs(east_margin - correct_margin) < 0.0001

        assert abs(correct_margin - wrong_avg) > 0.1

    def test_overall_aggregation_no_dimensions(self, sales_data, profit_margin_config):
        """Test derived metric with no dimensions (overall total).

        Total: revenue=1400, cost=810
               profit_margin = (1400-810)/1400 = 0.4214
        """
        context = create_pandas_context(sales_data)
        query = SemanticQuery(profit_margin_config)

        result = query.execute("profit_margin", context)

        assert result.row_count == 1

        total_revenue = 100 + 200 + 150 + 250 + 300 + 400
        total_cost = 60 + 120 + 100 + 150 + 180 + 200
        expected = (total_revenue - total_cost) / total_revenue

        assert abs(result.df["profit_margin"].iloc[0] - expected) < 0.0001


class TestDerivedMetricWithSimpleMetrics:
    """Test queries that include both derived and simple metrics."""

    @pytest.fixture
    def mixed_config(self):
        """Create config with both simple and derived metrics."""
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
                ),
            ],
            dimensions=[
                DimensionDefinition(name="region", source="gold.sales_fact"),
            ],
        )

    def test_query_with_mixed_metrics(self, mixed_config):
        """Test query that includes derived and simple metrics together."""
        df = pd.DataFrame(
            {
                "region": ["East", "East", "West"],
                "revenue": [100, 200, 300],
                "cost": [60, 120, 150],
            }
        )

        context = create_pandas_context(df)
        query = SemanticQuery(mixed_config)

        result = query.execute("total_revenue, order_count, profit_margin BY region", context)

        assert "total_revenue" in result.df.columns
        assert "order_count" in result.df.columns
        assert "profit_margin" in result.df.columns

        east = result.df[result.df["region"] == "East"]
        assert east["total_revenue"].iloc[0] == 300
        assert east["order_count"].iloc[0] == 2

        expected_margin = (300 - 180) / 300
        assert abs(east["profit_margin"].iloc[0] - expected_margin) < 0.0001


@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
class TestDerivedMetricPolars:
    """Test derived metrics with Polars engine for engine parity."""

    @pytest.fixture
    def sales_data_polars(self):
        """Create sample sales data as Polars DataFrame."""
        return pl.DataFrame(
            {
                "region": ["East", "East", "West", "West", "East", "West"],
                "month": ["Jan", "Jan", "Jan", "Jan", "Feb", "Feb"],
                "revenue": [100, 200, 150, 250, 300, 400],
                "cost": [60, 120, 100, 150, 180, 200],
            }
        )

    @pytest.fixture
    def profit_margin_config(self):
        """Create config for profit margin calculation."""
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
                ),
            ],
            dimensions=[
                DimensionDefinition(name="region", source="gold.sales_fact"),
                DimensionDefinition(name="month", source="gold.sales_fact"),
            ],
        )

    def test_polars_derived_metric_by_region(self, sales_data_polars, profit_margin_config):
        """Test derived metric with Polars at region grain.

        East: revenue=600, cost=360 -> margin=0.4
        West: revenue=800, cost=450 -> margin=0.4375
        """
        context = create_polars_context(sales_data_polars)
        query = SemanticQuery(profit_margin_config)

        result = query.execute("profit_margin BY region", context)

        assert result.row_count == 2
        df = result.df

        east_row = df.filter(pl.col("region") == "East")
        west_row = df.filter(pl.col("region") == "West")

        expected_east = (600 - 360) / 600
        expected_west = (800 - 450) / 800

        assert abs(east_row["profit_margin"][0] - expected_east) < 0.0001
        assert abs(west_row["profit_margin"][0] - expected_west) < 0.0001

    def test_polars_derived_metric_overall(self, sales_data_polars, profit_margin_config):
        """Test derived metric with Polars for overall total.

        Total: revenue=1400, cost=810 -> margin=0.4214
        """
        context = create_polars_context(sales_data_polars)
        query = SemanticQuery(profit_margin_config)

        result = query.execute("profit_margin", context)

        assert result.row_count == 1

        total_revenue = 100 + 200 + 150 + 250 + 300 + 400
        total_cost = 60 + 120 + 100 + 150 + 180 + 200
        expected = (total_revenue - total_cost) / total_revenue

        assert abs(result.df["profit_margin"][0] - expected) < 0.0001

    def test_polars_correct_vs_incorrect_calculation(self, profit_margin_config):
        """Verify Polars uses correct aggregation (SUM first, then ratio).

        Day 1: revenue=100, cost=20 -> margin=80%
        Day 2: revenue=1000, cost=500 -> margin=50%

        WRONG: AVG(80%, 50%) = 65%
        CORRECT: (1100-520)/1100 = 52.7%
        """
        test_data = pl.DataFrame(
            {
                "region": ["East", "East"],
                "month": ["Jan", "Jan"],
                "revenue": [100, 1000],
                "cost": [20, 500],
            }
        )

        context = create_polars_context(test_data)
        query = SemanticQuery(profit_margin_config)

        result = query.execute("profit_margin BY region", context)
        df = result.df

        east = df.filter(pl.col("region") == "East")
        east_margin = east["profit_margin"][0]

        correct_margin = (1100 - 520) / 1100

        row1_margin = (100 - 20) / 100
        row2_margin = (1000 - 500) / 1000
        wrong_avg = (row1_margin + row2_margin) / 2

        assert abs(east_margin - correct_margin) < 0.0001
        assert abs(correct_margin - wrong_avg) > 0.1

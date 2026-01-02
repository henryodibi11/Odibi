"""Unit tests for Semantic Layer module."""

import pandas as pd
import pytest

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.semantics.metrics import (
    MetricDefinition,
    DimensionDefinition,
    MaterializationConfig,
    SemanticLayerConfig,
    MetricType,
    parse_semantic_config,
)
from odibi.semantics.query import SemanticQuery
from odibi.semantics.materialize import Materializer, IncrementalMaterializer


def create_pandas_context(df, engine=None):
    """Helper to create a PandasContext with a DataFrame."""
    pandas_context = PandasContext()
    ctx = EngineContext(
        context=pandas_context,
        df=df,
        engine_type=EngineType.PANDAS,
        engine=engine,
    )
    return ctx


class TestMetricDefinition:
    """Test MetricDefinition model."""

    def test_valid_metric(self):
        """Test creating a valid metric."""
        metric = MetricDefinition(
            name="revenue",
            description="Total revenue",
            expr="SUM(total_amount)",
            source="fact_orders",
        )
        assert metric.name == "revenue"
        assert metric.expr == "SUM(total_amount)"
        assert metric.source == "fact_orders"
        assert metric.type == MetricType.SIMPLE

    def test_metric_with_filters(self):
        """Test metric with filter conditions."""
        metric = MetricDefinition(
            name="completed_revenue",
            expr="SUM(total_amount)",
            source="fact_orders",
            filters=["status = 'completed'", "amount > 0"],
        )
        assert len(metric.filters) == 2
        assert "status = 'completed'" in metric.filters

    def test_derived_metric(self):
        """Test derived metric type."""
        metric = MetricDefinition(
            name="avg_order_value",
            type=MetricType.DERIVED,
            components=["revenue", "order_count"],
            formula="revenue / order_count",
        )
        assert metric.type == MetricType.DERIVED
        assert metric.source is None
        assert metric.components == ["revenue", "order_count"]
        assert metric.formula == "revenue / order_count"

    def test_metric_name_validation(self):
        """Test that empty name raises error."""
        with pytest.raises(ValueError, match="empty"):
            MetricDefinition(name="", expr="SUM(x)")

    def test_metric_name_normalization(self):
        """Test that name is lowercased."""
        metric = MetricDefinition(name="Revenue_Total", expr="SUM(x)")
        assert metric.name == "revenue_total"

    def test_metric_invalid_name_chars(self):
        """Test that special characters raise error."""
        with pytest.raises(ValueError, match="alphanumeric"):
            MetricDefinition(name="revenue-total", expr="SUM(x)")

    def test_metric_expr_required(self):
        """Test that empty expr raises error."""
        with pytest.raises(ValueError, match="empty"):
            MetricDefinition(name="revenue", expr="")


class TestDimensionDefinition:
    """Test DimensionDefinition model."""

    def test_valid_dimension(self):
        """Test creating a valid dimension."""
        dim = DimensionDefinition(
            name="order_date",
            source="dim_date",
            hierarchy=["year", "quarter", "month", "full_date"],
        )
        assert dim.name == "order_date"
        assert dim.source == "dim_date"
        assert len(dim.hierarchy) == 4

    def test_dimension_get_column(self):
        """Test get_column returns column or name."""
        dim1 = DimensionDefinition(name="region", source="dim_region")
        assert dim1.get_column() == "region"

        dim2 = DimensionDefinition(name="region", source="dim_region", column="region_name")
        assert dim2.get_column() == "region_name"

    def test_dimension_name_normalization(self):
        """Test that name is lowercased."""
        dim = DimensionDefinition(name="Order_Date", source="dim_date")
        assert dim.name == "order_date"


class TestMaterializationConfig:
    """Test MaterializationConfig model."""

    def test_valid_materialization(self):
        """Test creating a valid materialization."""
        mat = MaterializationConfig(
            name="monthly_revenue",
            metrics=["revenue", "order_count"],
            dimensions=["region", "month"],
            output="gold/agg_monthly_revenue",
            schedule="0 2 1 * *",
        )
        assert mat.name == "monthly_revenue"
        assert len(mat.metrics) == 2
        assert len(mat.dimensions) == 2
        assert mat.output == "gold/agg_monthly_revenue"

    def test_materialization_requires_metrics(self):
        """Test that metrics are required."""
        with pytest.raises(ValueError, match="metric"):
            MaterializationConfig(
                name="test",
                metrics=[],
                dimensions=["region"],
                output="test/output",
            )

    def test_materialization_requires_dimensions(self):
        """Test that dimensions are required."""
        with pytest.raises(ValueError, match="dimension"):
            MaterializationConfig(
                name="test",
                metrics=["revenue"],
                dimensions=[],
                output="test/output",
            )


class TestSemanticLayerConfig:
    """Test SemanticLayerConfig model."""

    def test_empty_config(self):
        """Test creating empty config."""
        config = SemanticLayerConfig()
        assert len(config.metrics) == 0
        assert len(config.dimensions) == 0
        assert len(config.materializations) == 0

    def test_get_metric(self):
        """Test getting metric by name."""
        config = SemanticLayerConfig(
            metrics=[
                MetricDefinition(name="revenue", expr="SUM(amount)", source="fact"),
                MetricDefinition(name="orders", expr="COUNT(*)", source="fact"),
            ]
        )
        assert config.get_metric("revenue") is not None
        assert config.get_metric("Revenue") is not None  # Case insensitive
        assert config.get_metric("unknown") is None

    def test_get_dimension(self):
        """Test getting dimension by name."""
        config = SemanticLayerConfig(
            dimensions=[
                DimensionDefinition(name="region", source="dim_region"),
            ]
        )
        assert config.get_dimension("region") is not None
        assert config.get_dimension("unknown") is None

    def test_validate_references(self):
        """Test reference validation."""
        config = SemanticLayerConfig(
            metrics=[
                MetricDefinition(name="revenue", expr="SUM(amount)", source="fact"),
            ],
            dimensions=[
                DimensionDefinition(name="region", source="dim_region"),
            ],
            materializations=[
                MaterializationConfig(
                    name="test",
                    metrics=["revenue", "unknown_metric"],
                    dimensions=["region", "unknown_dim"],
                    output="test/out",
                )
            ],
        )
        errors = config.validate_references()
        assert len(errors) == 2
        assert any("unknown_metric" in e for e in errors)
        assert any("unknown_dim" in e for e in errors)

    def test_parse_semantic_config(self):
        """Test parsing config from dict."""
        config_dict = {
            "metrics": [
                {"name": "revenue", "expr": "SUM(amount)", "source": "fact"},
            ],
            "dimensions": [
                {"name": "region", "source": "dim_region"},
            ],
        }
        config = parse_semantic_config(config_dict)
        assert len(config.metrics) == 1
        assert len(config.dimensions) == 1


class TestSemanticQueryParsing:
    """Test SemanticQuery parsing."""

    @pytest.fixture
    def sample_config(self):
        """Create a sample semantic layer config."""
        return SemanticLayerConfig(
            metrics=[
                MetricDefinition(
                    name="revenue",
                    expr="SUM(total_amount)",
                    source="fact_orders",
                ),
                MetricDefinition(
                    name="order_count",
                    expr="COUNT(*)",
                    source="fact_orders",
                ),
            ],
            dimensions=[
                DimensionDefinition(name="region", source="dim_region"),
                DimensionDefinition(name="month", source="dim_date", column="month"),
            ],
        )

    def test_parse_simple_query(self, sample_config):
        """Test parsing simple query."""
        query = SemanticQuery(sample_config)
        parsed = query.parse("revenue BY region")

        assert parsed.metrics == ["revenue"]
        assert parsed.dimensions == ["region"]
        assert parsed.filters == []

    def test_parse_multiple_metrics(self, sample_config):
        """Test parsing multiple metrics."""
        query = SemanticQuery(sample_config)
        parsed = query.parse("revenue, order_count BY region, month")

        assert parsed.metrics == ["revenue", "order_count"]
        assert parsed.dimensions == ["region", "month"]

    def test_parse_with_filter(self, sample_config):
        """Test parsing query with WHERE clause."""
        query = SemanticQuery(sample_config)
        parsed = query.parse("revenue BY region WHERE year = 2024")

        assert parsed.metrics == ["revenue"]
        assert parsed.dimensions == ["region"]
        assert parsed.filters == ["year = 2024"]

    def test_parse_metrics_only(self, sample_config):
        """Test parsing metrics without dimensions."""
        query = SemanticQuery(sample_config)
        parsed = query.parse("revenue, order_count")

        assert parsed.metrics == ["revenue", "order_count"]
        assert parsed.dimensions == []

    def test_validate_unknown_metric(self, sample_config):
        """Test validation catches unknown metrics."""
        query = SemanticQuery(sample_config)
        parsed = query.parse("unknown_metric BY region")

        errors = query.validate(parsed)
        assert len(errors) == 1
        assert "unknown_metric" in errors[0]

    def test_validate_unknown_dimension(self, sample_config):
        """Test validation catches unknown dimensions."""
        query = SemanticQuery(sample_config)
        parsed = query.parse("revenue BY unknown_dim")

        errors = query.validate(parsed)
        assert len(errors) == 1
        assert "unknown_dim" in errors[0]

    def test_generate_sql(self, sample_config):
        """Test SQL generation."""
        query = SemanticQuery(sample_config)
        parsed = query.parse("revenue, order_count BY region, month")

        sql, source = query.generate_sql(parsed)

        assert "SUM(total_amount) AS revenue" in sql
        assert "COUNT(*) AS order_count" in sql
        assert "GROUP BY" in sql
        assert source == "fact_orders"


class TestSemanticQueryExecution:
    """Test SemanticQuery execution with Pandas."""

    @pytest.fixture
    def sample_config(self):
        """Create a sample semantic layer config."""
        return SemanticLayerConfig(
            metrics=[
                MetricDefinition(
                    name="revenue",
                    expr="SUM(total_amount)",
                    source="fact_orders",
                ),
                MetricDefinition(
                    name="order_count",
                    expr="COUNT(*)",
                    source="fact_orders",
                ),
                MetricDefinition(
                    name="avg_amount",
                    expr="AVG(total_amount)",
                    source="fact_orders",
                ),
            ],
            dimensions=[
                DimensionDefinition(name="region", source="dim_region"),
                DimensionDefinition(name="product", source="dim_product"),
            ],
        )

    @pytest.fixture
    def sample_data(self):
        """Create sample fact data."""
        return pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4, 5],
                "region": ["East", "West", "East", "West", "East"],
                "product": ["A", "A", "B", "B", "A"],
                "total_amount": [100, 200, 150, 250, 300],
                "quantity": [1, 2, 1, 3, 2],
            }
        )

    def test_execute_simple_aggregation(self, sample_config, sample_data):
        """Test executing simple aggregation."""
        query = SemanticQuery(sample_config)

        pandas_ctx = PandasContext()
        pandas_ctx.register("fact_orders", sample_data)

        context = EngineContext(
            context=pandas_ctx,
            df=sample_data,
            engine_type=EngineType.PANDAS,
        )

        result = query.execute("revenue BY region", context)

        assert result.row_count == 2  # East and West
        assert "revenue" in result.df.columns
        assert "region" in result.df.columns

        east = result.df[result.df["region"] == "East"]["revenue"].iloc[0]
        assert east == 550  # 100 + 150 + 300

    def test_execute_multiple_dimensions(self, sample_config, sample_data):
        """Test executing with multiple dimensions."""
        query = SemanticQuery(sample_config)

        pandas_ctx = PandasContext()
        pandas_ctx.register("fact_orders", sample_data)

        context = EngineContext(
            context=pandas_ctx,
            df=sample_data,
            engine_type=EngineType.PANDAS,
        )

        result = query.execute("revenue BY region, product", context)

        assert result.row_count == 4  # East-A, East-B, West-A, West-B
        assert "region" in result.df.columns
        assert "product" in result.df.columns

    def test_execute_multiple_metrics(self, sample_config, sample_data):
        """Test executing with multiple metrics."""
        query = SemanticQuery(sample_config)

        pandas_ctx = PandasContext()
        pandas_ctx.register("fact_orders", sample_data)

        context = EngineContext(
            context=pandas_ctx,
            df=sample_data,
            engine_type=EngineType.PANDAS,
        )

        result = query.execute("revenue, order_count BY region", context)

        assert "revenue" in result.df.columns
        assert "order_count" in result.df.columns

    def test_execute_no_grouping(self, sample_config, sample_data):
        """Test executing without grouping (grand total)."""
        query = SemanticQuery(sample_config)

        pandas_ctx = PandasContext()
        pandas_ctx.register("fact_orders", sample_data)

        context = EngineContext(
            context=pandas_ctx,
            df=sample_data,
            engine_type=EngineType.PANDAS,
        )

        result = query.execute("revenue", context)

        assert result.row_count == 1
        assert result.df["revenue"].iloc[0] == 1000  # Total of all amounts

    def test_execute_with_filter(self, sample_config, sample_data):
        """Test executing with metric filter."""
        config = SemanticLayerConfig(
            metrics=[
                MetricDefinition(
                    name="east_revenue",
                    expr="SUM(total_amount)",
                    source="fact_orders",
                    filters=["region == 'East'"],
                ),
            ],
            dimensions=[
                DimensionDefinition(name="product", source="dim_product"),
            ],
        )
        query = SemanticQuery(config)

        pandas_ctx = PandasContext()
        pandas_ctx.register("fact_orders", sample_data)

        context = EngineContext(
            context=pandas_ctx,
            df=sample_data,
            engine_type=EngineType.PANDAS,
        )

        result = query.execute("east_revenue BY product", context)

        total = result.df["east_revenue"].sum()
        assert total == 550  # Only East region amounts


class TestMaterializer:
    """Test Materializer class."""

    @pytest.fixture
    def sample_config(self):
        """Create a sample config with materialization."""
        return SemanticLayerConfig(
            metrics=[
                MetricDefinition(
                    name="revenue",
                    expr="SUM(total_amount)",
                    source="fact_orders",
                ),
                MetricDefinition(
                    name="order_count",
                    expr="COUNT(*)",
                    source="fact_orders",
                ),
            ],
            dimensions=[
                DimensionDefinition(name="region", source="dim_region"),
                DimensionDefinition(name="month", source="dim_date"),
            ],
            materializations=[
                MaterializationConfig(
                    name="monthly_revenue",
                    metrics=["revenue", "order_count"],
                    dimensions=["region"],
                    output="gold/agg_monthly_revenue",
                    schedule="0 2 1 * *",
                ),
            ],
        )

    @pytest.fixture
    def sample_data(self):
        """Create sample fact data."""
        return pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4],
                "region": ["East", "West", "East", "West"],
                "total_amount": [100, 200, 150, 250],
            }
        )

    def test_execute_materialization(self, sample_config, sample_data):
        """Test executing a materialization."""
        materializer = Materializer(sample_config)

        pandas_ctx = PandasContext()
        pandas_ctx.register("fact_orders", sample_data)

        context = EngineContext(
            context=pandas_ctx,
            df=sample_data,
            engine_type=EngineType.PANDAS,
        )

        result = materializer.execute("monthly_revenue", context)

        assert result.success
        assert result.name == "monthly_revenue"
        assert result.row_count == 2  # East and West

    def test_execute_with_write_callback(self, sample_config, sample_data):
        """Test materialization with write callback."""
        materializer = Materializer(sample_config)
        written_data = {}

        def write_callback(df, output_path):
            written_data["df"] = df
            written_data["path"] = output_path

        pandas_ctx = PandasContext()
        pandas_ctx.register("fact_orders", sample_data)

        context = EngineContext(
            context=pandas_ctx,
            df=sample_data,
            engine_type=EngineType.PANDAS,
        )

        result = materializer.execute("monthly_revenue", context, write_callback)

        assert result.success
        assert "df" in written_data
        assert written_data["path"] == "gold/agg_monthly_revenue"

    def test_unknown_materialization(self, sample_config, sample_data):
        """Test error for unknown materialization."""
        materializer = Materializer(sample_config)

        context = create_pandas_context(sample_data)

        with pytest.raises(ValueError, match="not found"):
            materializer.execute("unknown_mat", context)

    def test_list_materializations(self, sample_config):
        """Test listing materializations."""
        materializer = Materializer(sample_config)
        mats = materializer.list_materializations()

        assert len(mats) == 1
        assert mats[0]["name"] == "monthly_revenue"
        assert mats[0]["schedule"] == "0 2 1 * *"

    def test_get_schedule(self, sample_config):
        """Test getting materialization schedule."""
        materializer = Materializer(sample_config)

        assert materializer.get_schedule("monthly_revenue") == "0 2 1 * *"
        assert materializer.get_schedule("unknown") is None


class TestIncrementalMaterializer:
    """Test IncrementalMaterializer class."""

    @pytest.fixture
    def sample_config(self):
        """Create a sample config."""
        return SemanticLayerConfig(
            metrics=[
                MetricDefinition(
                    name="revenue",
                    expr="SUM(total_amount)",
                    source="fact_orders",
                ),
            ],
            dimensions=[
                DimensionDefinition(name="region", source="dim_region"),
            ],
            materializations=[
                MaterializationConfig(
                    name="regional_revenue",
                    metrics=["revenue"],
                    dimensions=["region"],
                    output="gold/agg_regional",
                ),
            ],
        )

    def test_incremental_replace_strategy(self, sample_config):
        """Test incremental materialization with replace strategy."""
        existing_df = pd.DataFrame(
            {
                "region": ["East", "West"],
                "revenue": [100, 200],
            }
        )

        new_data = pd.DataFrame(
            {
                "order_id": [5, 6],
                "region": ["East", "Central"],
                "total_amount": [50, 75],
                "timestamp": [10, 11],
            }
        )

        materializer = IncrementalMaterializer(sample_config)

        pandas_ctx = PandasContext()
        pandas_ctx.register("fact_orders", new_data)

        context = EngineContext(
            context=pandas_ctx,
            df=new_data,
            engine_type=EngineType.PANDAS,
        )

        result = materializer.execute_incremental(
            name="regional_revenue",
            context=context,
            existing_df=existing_df,
            timestamp_column="timestamp",
            merge_strategy="replace",
        )

        assert result.success
        assert result.row_count == 3  # West (unchanged) + East (replaced) + Central (new)

    def test_incremental_sum_strategy(self, sample_config):
        """Test incremental materialization with sum strategy."""
        existing_df = pd.DataFrame(
            {
                "region": ["East", "West"],
                "revenue": [100, 200],
            }
        )

        new_data = pd.DataFrame(
            {
                "order_id": [5, 6],
                "region": ["East", "West"],
                "total_amount": [50, 75],
                "timestamp": [10, 11],
            }
        )

        materializer = IncrementalMaterializer(sample_config)

        pandas_ctx = PandasContext()
        pandas_ctx.register("fact_orders", new_data)

        context = EngineContext(
            context=pandas_ctx,
            df=new_data,
            engine_type=EngineType.PANDAS,
        )

        result = materializer.execute_incremental(
            name="regional_revenue",
            context=context,
            existing_df=existing_df,
            timestamp_column="timestamp",
            merge_strategy="sum",
        )

        assert result.success
        assert result.row_count == 2  # East and West


class TestSemanticLayerIntegration:
    """Integration tests for the semantic layer."""

    def test_full_workflow(self):
        """Test complete semantic layer workflow."""
        config = SemanticLayerConfig(
            metrics=[
                MetricDefinition(
                    name="revenue",
                    description="Total revenue",
                    expr="SUM(total_amount)",
                    source="fact_orders",
                ),
                MetricDefinition(
                    name="avg_order",
                    description="Average order value",
                    expr="AVG(total_amount)",
                    source="fact_orders",
                ),
            ],
            dimensions=[
                DimensionDefinition(
                    name="region",
                    source="dim_region",
                    hierarchy=["country", "region", "city"],
                ),
                DimensionDefinition(
                    name="product",
                    source="dim_product",
                ),
            ],
            materializations=[
                MaterializationConfig(
                    name="regional_summary",
                    metrics=["revenue", "avg_order"],
                    dimensions=["region"],
                    output="gold/regional_summary",
                ),
            ],
        )

        errors = config.validate_references()
        assert len(errors) == 0

        fact_data = pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4, 5, 6],
                "region": ["East", "West", "East", "West", "North", "East"],
                "product": ["A", "B", "A", "A", "B", "C"],
                "total_amount": [100, 200, 150, 300, 175, 225],
            }
        )

        pandas_ctx = PandasContext()
        pandas_ctx.register("fact_orders", fact_data)

        context = EngineContext(
            context=pandas_ctx,
            df=fact_data,
            engine_type=EngineType.PANDAS,
        )

        query = SemanticQuery(config)
        result = query.execute("revenue, avg_order BY region", context)

        assert result.row_count == 3  # East, West, North
        assert "revenue" in result.df.columns
        assert "avg_order" in result.df.columns

        east_row = result.df[result.df["region"] == "East"]
        assert east_row["revenue"].iloc[0] == 475  # 100 + 150 + 225

        materializer = Materializer(config)
        mat_result = materializer.execute("regional_summary", context)

        assert mat_result.success
        assert mat_result.row_count == 3

"""Tests for semantic layer metrics models."""

import pytest

from odibi.semantics.metrics import (
    DimensionDefinition,
    MaterializationConfig,
    MetricDefinition,
    MetricType,
    SemanticLayerConfig,
    TimeGrain,
    ViewConfig,
    ViewResult,
    parse_semantic_config,
)


class TestMetricType:
    """Test MetricType enum."""

    def test_simple_value(self):
        """Test SIMPLE value."""
        assert MetricType.SIMPLE.value == "simple"

    def test_derived_value(self):
        """Test DERIVED value."""
        assert MetricType.DERIVED.value == "derived"


class TestTimeGrain:
    """Test TimeGrain enum."""

    def test_all_values(self):
        """Test all time grain values."""
        assert TimeGrain.DAY.value == "day"
        assert TimeGrain.WEEK.value == "week"
        assert TimeGrain.MONTH.value == "month"
        assert TimeGrain.QUARTER.value == "quarter"
        assert TimeGrain.YEAR.value == "year"


class TestMetricDefinition:
    """Test MetricDefinition model."""

    def test_simple_metric_valid(self):
        """Test creating a valid simple metric."""
        metric = MetricDefinition(
            name="revenue",
            expr="SUM(amount)",
            source="fact_orders",
        )

        assert metric.name == "revenue"
        assert metric.expr == "SUM(amount)"
        assert metric.type == MetricType.SIMPLE

    def test_name_validation_empty(self):
        """Test that empty name is rejected."""
        with pytest.raises(ValueError, match="empty"):
            MetricDefinition(name="", expr="SUM(x)")

    def test_name_validation_special_chars(self):
        """Test that special characters are rejected."""
        with pytest.raises(ValueError, match="alphanumeric"):
            MetricDefinition(name="revenue-total", expr="SUM(x)")

    def test_name_lowercased(self):
        """Test that name is lowercased."""
        metric = MetricDefinition(name="REVENUE", expr="SUM(x)", source="t")
        assert metric.name == "revenue"

    def test_expr_validation_empty_string(self):
        """Test that empty expr string is rejected for simple metric."""
        with pytest.raises(ValueError):
            MetricDefinition(name="revenue", expr="   ", source="t")

    def test_simple_metric_requires_expr(self):
        """Test that simple metric requires expr."""
        with pytest.raises(ValueError, match="requires 'expr'"):
            MetricDefinition(name="revenue", type=MetricType.SIMPLE)

    def test_derived_metric_requires_components(self):
        """Test that derived metric requires components."""
        with pytest.raises(ValueError, match="requires 'components'"):
            MetricDefinition(
                name="profit_margin",
                type=MetricType.DERIVED,
                formula="a / b",
            )

    def test_derived_metric_requires_formula(self):
        """Test that derived metric requires formula."""
        with pytest.raises(ValueError, match="requires 'formula'"):
            MetricDefinition(
                name="profit_margin",
                type=MetricType.DERIVED,
                components=["a", "b"],
            )

    def test_derived_metric_valid(self):
        """Test creating a valid derived metric."""
        metric = MetricDefinition(
            name="profit_margin",
            type=MetricType.DERIVED,
            components=["revenue", "cost"],
            formula="(revenue - cost) / revenue",
        )

        assert metric.type == MetricType.DERIVED
        assert metric.components == ["revenue", "cost"]

    def test_get_alias_with_label(self):
        """Test get_alias returns label when set."""
        metric = MetricDefinition(
            name="revenue",
            label="Total Revenue",
            expr="SUM(amount)",
            source="t",
        )

        assert metric.get_alias() == "Total Revenue"

    def test_get_alias_without_label(self):
        """Test get_alias returns name when no label."""
        metric = MetricDefinition(name="revenue", expr="SUM(amount)", source="t")
        assert metric.get_alias() == "revenue"


class TestDimensionDefinition:
    """Test DimensionDefinition model."""

    def test_dimension_valid(self):
        """Test creating a valid dimension."""
        dim = DimensionDefinition(name="region")

        assert dim.name == "region"
        assert dim.column is None

    def test_name_validation_empty(self):
        """Test that empty name is rejected."""
        with pytest.raises(ValueError, match="empty"):
            DimensionDefinition(name="")

    def test_name_lowercased(self):
        """Test that name is lowercased."""
        dim = DimensionDefinition(name="REGION")
        assert dim.name == "region"

    def test_dimension_with_column(self):
        """Test dimension with explicit column."""
        dim = DimensionDefinition(name="order_date", column="date")

        assert dim.get_column() == "date"

    def test_get_column_defaults_to_name(self):
        """Test get_column returns name when no column set."""
        dim = DimensionDefinition(name="region")
        assert dim.get_column() == "region"

    def test_dimension_with_hierarchy(self):
        """Test dimension with hierarchy."""
        dim = DimensionDefinition(
            name="order_date",
            hierarchy=["year", "quarter", "month", "day"],
        )

        assert len(dim.hierarchy) == 4

    def test_dimension_with_grain(self):
        """Test dimension with time grain."""
        dim = DimensionDefinition(name="order_date", grain=TimeGrain.MONTH)
        assert dim.grain == TimeGrain.MONTH

    def test_get_alias_with_label(self):
        """Test get_alias returns label when set."""
        dim = DimensionDefinition(name="region", label="Sales Region")
        assert dim.get_alias() == "Sales Region"

    def test_get_alias_without_label(self):
        """Test get_alias returns name when no label."""
        dim = DimensionDefinition(name="region")
        assert dim.get_alias() == "region"


class TestMaterializationConfig:
    """Test MaterializationConfig model."""

    def test_valid_config(self):
        """Test creating a valid materialization config."""
        mat = MaterializationConfig(
            name="monthly_revenue",
            metrics=["revenue", "order_count"],
            dimensions=["region", "month"],
            output="gold/agg_monthly_revenue",
        )

        assert mat.name == "monthly_revenue"
        assert len(mat.metrics) == 2
        assert len(mat.dimensions) == 2

    def test_metrics_required(self):
        """Test that at least one metric is required."""
        with pytest.raises(ValueError, match="metric"):
            MaterializationConfig(
                name="test",
                metrics=[],
                dimensions=["region"],
                output="test",
            )

    def test_dimensions_required(self):
        """Test that at least one dimension is required."""
        with pytest.raises(ValueError, match="dimension"):
            MaterializationConfig(
                name="test",
                metrics=["revenue"],
                dimensions=[],
                output="test",
            )

    def test_with_schedule(self):
        """Test config with schedule."""
        mat = MaterializationConfig(
            name="daily_revenue",
            metrics=["revenue"],
            dimensions=["region"],
            output="gold/daily",
            schedule="0 2 * * *",
        )

        assert mat.schedule == "0 2 * * *"


class TestViewConfig:
    """Test ViewConfig model."""

    def test_valid_config(self):
        """Test creating a valid view config."""
        view = ViewConfig(
            name="vw_revenue_by_region",
            metrics=["revenue"],
            dimensions=["region"],
        )

        assert view.name == "vw_revenue_by_region"
        assert view.db_schema == "semantic"

    def test_name_required_nonempty(self):
        """Test that name cannot be empty."""
        with pytest.raises(ValueError, match="empty"):
            ViewConfig(name="", metrics=["rev"], dimensions=["reg"])

    def test_metrics_required(self):
        """Test that at least one metric is required."""
        with pytest.raises(ValueError, match="metric"):
            ViewConfig(name="test", metrics=[], dimensions=["region"])

    def test_dimensions_required(self):
        """Test that at least one dimension is required."""
        with pytest.raises(ValueError, match="dimension"):
            ViewConfig(name="test", metrics=["revenue"], dimensions=[])

    def test_custom_schema(self):
        """Test custom db_schema."""
        view = ViewConfig(
            name="vw_test",
            metrics=["revenue"],
            dimensions=["region"],
            db_schema="gold",
        )

        assert view.db_schema == "gold"


class TestViewResult:
    """Test ViewResult model."""

    def test_success_result(self):
        """Test creating a success result."""
        result = ViewResult(
            name="vw_test",
            success=True,
            sql="CREATE VIEW...",
        )

        assert result.success is True
        assert result.error is None

    def test_failure_result(self):
        """Test creating a failure result."""
        result = ViewResult(
            name="vw_test",
            success=False,
            sql="CREATE VIEW...",
            error="Syntax error",
        )

        assert result.success is False
        assert result.error == "Syntax error"


class TestSemanticLayerConfig:
    """Test SemanticLayerConfig model."""

    def test_empty_config(self):
        """Test creating empty config."""
        config = SemanticLayerConfig()

        assert config.metrics == []
        assert config.dimensions == []
        assert config.materializations == []
        assert config.views == []

    def test_get_metric(self):
        """Test getting metric by name."""
        config = SemanticLayerConfig(
            metrics=[
                MetricDefinition(name="revenue", expr="SUM(x)", source="t"),
                MetricDefinition(name="orders", expr="COUNT(*)", source="t"),
            ]
        )

        metric = config.get_metric("revenue")
        assert metric.name == "revenue"

        metric = config.get_metric("REVENUE")
        assert metric.name == "revenue"

        assert config.get_metric("unknown") is None

    def test_get_dimension(self):
        """Test getting dimension by name."""
        config = SemanticLayerConfig(
            dimensions=[
                DimensionDefinition(name="region"),
                DimensionDefinition(name="month"),
            ]
        )

        dim = config.get_dimension("region")
        assert dim.name == "region"

        assert config.get_dimension("unknown") is None

    def test_get_materialization(self):
        """Test getting materialization by name."""
        config = SemanticLayerConfig(
            materializations=[
                MaterializationConfig(
                    name="monthly",
                    metrics=["revenue"],
                    dimensions=["region"],
                    output="test",
                )
            ]
        )

        mat = config.get_materialization("monthly")
        assert mat.name == "monthly"

        assert config.get_materialization("unknown") is None

    def test_validate_references_valid(self):
        """Test validation with valid references."""
        config = SemanticLayerConfig(
            metrics=[
                MetricDefinition(name="revenue", expr="SUM(x)", source="t"),
                MetricDefinition(name="cost", expr="SUM(y)", source="t"),
                MetricDefinition(
                    name="profit",
                    type=MetricType.DERIVED,
                    components=["revenue", "cost"],
                    formula="revenue - cost",
                ),
            ],
            dimensions=[DimensionDefinition(name="region")],
            materializations=[
                MaterializationConfig(
                    name="test",
                    metrics=["revenue"],
                    dimensions=["region"],
                    output="out",
                )
            ],
        )

        errors = config.validate_references()
        assert errors == []

    def test_validate_references_unknown_component(self):
        """Test validation catches unknown component."""
        config = SemanticLayerConfig(
            metrics=[
                MetricDefinition(name="revenue", expr="SUM(x)", source="t"),
                MetricDefinition(
                    name="profit",
                    type=MetricType.DERIVED,
                    components=["revenue", "unknown_metric"],
                    formula="revenue - unknown_metric",
                ),
            ]
        )

        errors = config.validate_references()
        assert len(errors) == 1
        assert "unknown_metric" in errors[0]


class TestParseSemanticConfig:
    """Test parse_semantic_config function."""

    def test_parse_full_config(self):
        """Test parsing a full config dictionary."""
        config_dict = {
            "metrics": [{"name": "revenue", "expr": "SUM(amount)", "source": "fact_orders"}],
            "dimensions": [{"name": "region"}],
            "views": [{"name": "vw_test", "metrics": ["revenue"], "dimensions": ["region"]}],
        }

        config = parse_semantic_config(config_dict)

        assert len(config.metrics) == 1
        assert len(config.dimensions) == 1
        assert len(config.views) == 1

    def test_parse_empty_config(self):
        """Test parsing empty config."""
        config = parse_semantic_config({})

        assert config.metrics == []
        assert config.dimensions == []

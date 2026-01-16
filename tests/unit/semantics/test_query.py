"""Tests for SemanticQuery parsing and execution."""

import pandas as pd
import pytest

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.semantics.metrics import (
    DimensionDefinition,
    MetricDefinition,
    MetricType,
    SemanticLayerConfig,
)
from odibi.semantics.query import ParsedQuery, QueryResult, SemanticQuery


def create_pandas_context(df):
    """Helper to create a PandasContext with a DataFrame."""
    pandas_ctx = PandasContext()
    pandas_ctx.register("fact_orders", df)
    return EngineContext(
        context=pandas_ctx,
        df=df,
        engine_type=EngineType.PANDAS,
    )


@pytest.fixture
def sample_config():
    """Create a sample semantic layer config."""
    return SemanticLayerConfig(
        metrics=[
            MetricDefinition(
                name="revenue",
                expr="SUM(amount)",
                source="fact_orders",
            ),
            MetricDefinition(
                name="order_count",
                expr="COUNT(*)",
                source="fact_orders",
            ),
            MetricDefinition(
                name="avg_order_value",
                type=MetricType.DERIVED,
                components=["revenue", "order_count"],
                formula="revenue / order_count",
            ),
        ],
        dimensions=[
            DimensionDefinition(name="region"),
            DimensionDefinition(name="month"),
            DimensionDefinition(name="order_date", column="date"),
        ],
    )


@pytest.fixture
def sample_df():
    """Create a sample DataFrame."""
    return pd.DataFrame(
        {
            "region": ["East", "West", "East", "West", "East", "West"],
            "month": ["Jan", "Jan", "Feb", "Feb", "Jan", "Feb"],
            "date": [
                "2024-01-01",
                "2024-01-15",
                "2024-02-01",
                "2024-02-15",
                "2024-01-20",
                "2024-02-28",
            ],
            "amount": [100, 200, 150, 250, 120, 180],
        }
    )


class TestParsedQuery:
    """Test ParsedQuery dataclass."""

    def test_default_values(self):
        """Test default values for ParsedQuery."""
        parsed = ParsedQuery()
        assert parsed.metrics == []
        assert parsed.dimensions == []
        assert parsed.filters == []
        assert parsed.raw_query == ""

    def test_with_values(self):
        """Test ParsedQuery with values."""
        parsed = ParsedQuery(
            metrics=["revenue", "order_count"],
            dimensions=["region", "month"],
            filters=["status = 'completed'"],
            raw_query="revenue, order_count BY region, month WHERE status = 'completed'",
        )
        assert len(parsed.metrics) == 2
        assert len(parsed.dimensions) == 2
        assert len(parsed.filters) == 1


class TestQueryResult:
    """Test QueryResult dataclass."""

    def test_query_result(self):
        """Test QueryResult creation."""
        df = pd.DataFrame({"col": [1, 2, 3]})
        result = QueryResult(
            df=df,
            metrics=["revenue"],
            dimensions=["region"],
            row_count=3,
            elapsed_ms=100.5,
        )

        assert result.row_count == 3
        assert result.elapsed_ms == 100.5
        assert result.sql_generated is None


class TestSemanticQueryInit:
    """Test SemanticQuery initialization."""

    def test_init_stores_config(self, sample_config):
        """Test that init stores the config."""
        query = SemanticQuery(sample_config)
        assert query.config is sample_config

    def test_init_builds_metric_cache(self, sample_config):
        """Test that init builds metric cache."""
        query = SemanticQuery(sample_config)
        assert "revenue" in query._metric_cache
        assert "order_count" in query._metric_cache
        assert "avg_order_value" in query._metric_cache

    def test_init_builds_dimension_cache(self, sample_config):
        """Test that init builds dimension cache."""
        query = SemanticQuery(sample_config)
        assert "region" in query._dimension_cache
        assert "month" in query._dimension_cache
        assert "order_date" in query._dimension_cache


class TestSemanticQueryParse:
    """Test SemanticQuery parse method."""

    def test_parse_simple_query(self, sample_config):
        """Test parsing a simple query."""
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

    def test_parse_with_where(self, sample_config):
        """Test parsing query with WHERE clause."""
        query = SemanticQuery(sample_config)
        parsed = query.parse("revenue BY region WHERE status = 'completed'")

        assert parsed.metrics == ["revenue"]
        assert parsed.dimensions == ["region"]
        assert len(parsed.filters) == 1
        assert "status = 'completed'" in parsed.filters[0]

    def test_parse_case_insensitive(self, sample_config):
        """Test that parsing is case-insensitive for BY/WHERE."""
        query = SemanticQuery(sample_config)
        parsed = query.parse("REVENUE by REGION where status = 'completed'")

        assert parsed.metrics == ["revenue"]
        assert parsed.dimensions == ["region"]
        assert len(parsed.filters) == 1

    def test_parse_stores_raw_query(self, sample_config):
        """Test that raw query is stored."""
        query = SemanticQuery(sample_config)
        raw = "revenue BY region"
        parsed = query.parse(raw)

        assert parsed.raw_query == raw


class TestSemanticQueryValidate:
    """Test SemanticQuery validate method."""

    def test_validate_valid_query(self, sample_config):
        """Test validation of valid query."""
        query = SemanticQuery(sample_config)
        parsed = query.parse("revenue BY region")

        errors = query.validate(parsed)
        assert errors == []

    def test_validate_unknown_metric(self, sample_config):
        """Test validation catches unknown metric."""
        query = SemanticQuery(sample_config)
        parsed = query.parse("unknown_metric BY region")

        errors = query.validate(parsed)
        assert len(errors) == 1
        assert "unknown_metric" in errors[0].lower()

    def test_validate_unknown_dimension(self, sample_config):
        """Test validation catches unknown dimension."""
        query = SemanticQuery(sample_config)
        parsed = query.parse("revenue BY unknown_dim")

        errors = query.validate(parsed)
        assert len(errors) == 1
        assert "unknown_dim" in errors[0].lower()

    def test_validate_no_metrics(self, sample_config):
        """Test validation catches missing metrics."""
        query = SemanticQuery(sample_config)
        parsed = ParsedQuery(metrics=[], dimensions=["region"])

        errors = query.validate(parsed)
        assert len(errors) == 1
        assert "metric" in errors[0].lower()


class TestSemanticQueryGenerateSQL:
    """Test SemanticQuery generate_sql method."""

    def test_generate_sql_simple(self, sample_config):
        """Test SQL generation for simple query."""
        query = SemanticQuery(sample_config)
        parsed = query.parse("revenue BY region")

        sql, source = query.generate_sql(parsed)

        assert "SELECT" in sql
        assert "SUM(amount)" in sql
        assert "region" in sql
        assert "GROUP BY" in sql
        assert source == "fact_orders"

    def test_generate_sql_multiple_metrics(self, sample_config):
        """Test SQL generation with multiple metrics."""
        query = SemanticQuery(sample_config)
        parsed = query.parse("revenue, order_count BY region")

        sql, source = query.generate_sql(parsed)

        assert "SUM(amount)" in sql
        assert "COUNT(*)" in sql

    def test_generate_sql_no_metrics_raises(self, sample_config):
        """Test that SQL generation requires metrics."""
        query = SemanticQuery(sample_config)
        parsed = ParsedQuery(metrics=[], dimensions=["region"])

        with pytest.raises(ValueError, match="metric"):
            query.generate_sql(parsed)


class TestSemanticQueryExecute:
    """Test SemanticQuery execute method."""

    def test_execute_simple_query(self, sample_config, sample_df):
        """Test executing a simple query."""
        query = SemanticQuery(sample_config)
        context = create_pandas_context(sample_df)

        result = query.execute("revenue BY region", context)

        assert isinstance(result, QueryResult)
        assert result.row_count == 2
        assert "revenue" in result.df.columns

    def test_execute_aggregates_correctly(self, sample_config, sample_df):
        """Test that aggregation is correct."""
        query = SemanticQuery(sample_config)
        context = create_pandas_context(sample_df)

        result = query.execute("revenue BY region", context)

        east_row = result.df[result.df["region"] == "East"]
        west_row = result.df[result.df["region"] == "West"]

        assert east_row["revenue"].iloc[0] == 370
        assert west_row["revenue"].iloc[0] == 630

    def test_execute_count_star(self, sample_config, sample_df):
        """Test executing COUNT(*) metric."""
        query = SemanticQuery(sample_config)
        context = create_pandas_context(sample_df)

        result = query.execute("order_count BY region", context)

        assert result.row_count == 2
        east_row = result.df[result.df["region"] == "East"]
        assert east_row["order_count"].iloc[0] == 3

    def test_execute_multiple_dimensions(self, sample_config, sample_df):
        """Test executing with multiple dimensions."""
        query = SemanticQuery(sample_config)
        context = create_pandas_context(sample_df)

        result = query.execute("revenue BY region, month", context)

        assert result.row_count == 4

    def test_execute_no_dimensions(self, sample_config, sample_df):
        """Test executing without dimensions (total aggregation)."""
        query = SemanticQuery(sample_config)
        context = create_pandas_context(sample_df)

        result = query.execute("revenue", context)

        assert result.row_count == 1
        assert result.df["revenue"].iloc[0] == 1000


class TestSemanticQueryDerivedMetrics:
    """Test SemanticQuery with derived metrics."""

    def test_execute_derived_metric(self, sample_config, sample_df):
        """Test executing a derived metric."""
        query = SemanticQuery(sample_config)
        context = create_pandas_context(sample_df)

        result = query.execute("avg_order_value BY region", context)

        assert "avg_order_value" in result.df.columns
        east_row = result.df[result.df["region"] == "East"]
        expected_aov = 370 / 3
        assert abs(east_row["avg_order_value"].iloc[0] - expected_aov) < 0.01

    def test_execute_mixed_metrics(self, sample_config, sample_df):
        """Test executing simple and derived metrics together."""
        query = SemanticQuery(sample_config)
        context = create_pandas_context(sample_df)

        result = query.execute("revenue, avg_order_value BY region", context)

        assert "revenue" in result.df.columns
        assert "avg_order_value" in result.df.columns


class TestSemanticQueryWithDimensionColumn:
    """Test SemanticQuery with dimension column mapping."""

    def test_dimension_uses_column_name(self, sample_config, sample_df):
        """Test that dimension uses column name when specified."""
        query = SemanticQuery(sample_config)

        dim_def = query._dimension_cache.get("order_date")
        assert dim_def.get_column() == "date"

"""Unit tests for AggregationPattern."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from odibi.config import NodeConfig
from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.patterns.aggregation import AggregationPattern


def create_pandas_context(df, engine=None):
    """Helper to create a PandasContext with a DataFrame."""
    pandas_context = PandasContext()

    def sql_executor(query, ctx):
        import duckdb

        conn = duckdb.connect(":memory:")
        for name in ctx.list_names():
            conn.register(name, ctx.get(name))
        return conn.execute(query).fetchdf()

    ctx = EngineContext(
        context=pandas_context,
        df=df,
        engine_type=EngineType.PANDAS,
        engine=engine,
        sql_executor=sql_executor,
    )
    return ctx


@pytest.fixture
def mock_engine():
    """Create a mock engine."""
    engine = MagicMock()
    engine.connections = {}
    return engine


@pytest.fixture
def mock_config():
    """Create a basic mock NodeConfig."""
    config = MagicMock(spec=NodeConfig)
    config.name = "test_node"
    config.params = {}
    return config


@pytest.fixture
def sample_sales_data():
    """Sample sales data for aggregation testing."""
    return pd.DataFrame(
        {
            "date_sk": [20240101, 20240101, 20240102, 20240102, 20240102],
            "product_sk": [1, 2, 1, 2, 1],
            "region": ["East", "East", "West", "West", "East"],
            "quantity": [10, 5, 8, 12, 6],
            "amount": [100.0, 50.0, 80.0, 120.0, 60.0],
        }
    )


class TestAggregationPatternValidation:
    """Test AggregationPattern validation."""

    def test_validate_requires_grain(self, mock_engine, mock_config):
        """Test that grain is required."""
        mock_config.params = {"measures": [{"name": "total", "expr": "SUM(amount)"}]}

        pattern = AggregationPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="grain"):
            pattern.validate()

    def test_validate_requires_measures(self, mock_engine, mock_config):
        """Test that measures are required."""
        mock_config.params = {"grain": ["date_sk"]}

        pattern = AggregationPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="measures"):
            pattern.validate()

    def test_validate_measure_must_be_dict(self, mock_engine, mock_config):
        """Test that measures must be dicts."""
        mock_config.params = {
            "grain": ["date_sk"],
            "measures": ["SUM(amount)"],
        }

        pattern = AggregationPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="must be a dict"):
            pattern.validate()

    def test_validate_measure_requires_name(self, mock_engine, mock_config):
        """Test that measures must have name."""
        mock_config.params = {
            "grain": ["date_sk"],
            "measures": [{"expr": "SUM(amount)"}],
        }

        pattern = AggregationPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="missing 'name'"):
            pattern.validate()

    def test_validate_measure_requires_expr(self, mock_engine, mock_config):
        """Test that measures must have expr."""
        mock_config.params = {
            "grain": ["date_sk"],
            "measures": [{"name": "total"}],
        }

        pattern = AggregationPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="missing 'expr'"):
            pattern.validate()

    def test_validate_invalid_merge_strategy(self, mock_engine, mock_config):
        """Test that invalid merge_strategy raises error."""
        mock_config.params = {
            "grain": ["date_sk"],
            "measures": [{"name": "total", "expr": "SUM(amount)"}],
            "incremental": {
                "timestamp_column": "date_sk",
                "merge_strategy": "invalid",
            },
        }

        pattern = AggregationPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="merge_strategy"):
            pattern.validate()

    def test_validate_incremental_requires_timestamp(self, mock_engine, mock_config):
        """Test that incremental requires timestamp_column."""
        mock_config.params = {
            "grain": ["date_sk"],
            "measures": [{"name": "total", "expr": "SUM(amount)"}],
            "incremental": {"merge_strategy": "replace"},
        }

        pattern = AggregationPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="timestamp_column"):
            pattern.validate()

    def test_validate_success(self, mock_engine, mock_config):
        """Test validation passes with valid params."""
        mock_config.params = {
            "grain": ["date_sk", "product_sk"],
            "measures": [
                {"name": "total_amount", "expr": "SUM(amount)"},
                {"name": "order_count", "expr": "COUNT(*)"},
            ],
        }

        pattern = AggregationPattern(mock_engine, mock_config)
        pattern.validate()


class TestAggregationPatternBasic:
    """Test basic aggregation functionality."""

    def test_simple_sum_aggregation(self, mock_engine, mock_config, sample_sales_data):
        """Test simple SUM aggregation."""
        mock_config.params = {
            "grain": ["date_sk"],
            "measures": [{"name": "total_amount", "expr": "SUM(amount)"}],
        }

        context = create_pandas_context(sample_sales_data, mock_engine)
        pattern = AggregationPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 2
        assert "date_sk" in result.columns
        assert "total_amount" in result.columns

        day1 = result[result["date_sk"] == 20240101]
        assert day1.iloc[0]["total_amount"] == 150.0

        day2 = result[result["date_sk"] == 20240102]
        assert day2.iloc[0]["total_amount"] == 260.0

    def test_count_aggregation(self, mock_engine, mock_config, sample_sales_data):
        """Test COUNT aggregation."""
        mock_config.params = {
            "grain": ["date_sk"],
            "measures": [{"name": "row_count", "expr": "COUNT(*)"}],
        }

        context = create_pandas_context(sample_sales_data, mock_engine)
        pattern = AggregationPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        day1 = result[result["date_sk"] == 20240101]
        assert day1.iloc[0]["row_count"] == 2

        day2 = result[result["date_sk"] == 20240102]
        assert day2.iloc[0]["row_count"] == 3

    def test_avg_aggregation(self, mock_engine, mock_config, sample_sales_data):
        """Test AVG aggregation."""
        mock_config.params = {
            "grain": ["date_sk"],
            "measures": [{"name": "avg_amount", "expr": "AVG(amount)"}],
        }

        context = create_pandas_context(sample_sales_data, mock_engine)
        pattern = AggregationPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        day1 = result[result["date_sk"] == 20240101]
        assert day1.iloc[0]["avg_amount"] == 75.0

    def test_multiple_measures(self, mock_engine, mock_config, sample_sales_data):
        """Test multiple measures in one aggregation."""
        mock_config.params = {
            "grain": ["date_sk"],
            "measures": [
                {"name": "total_amount", "expr": "SUM(amount)"},
                {"name": "total_qty", "expr": "SUM(quantity)"},
                {"name": "row_count", "expr": "COUNT(*)"},
                {"name": "avg_amount", "expr": "AVG(amount)"},
            ],
        }

        context = create_pandas_context(sample_sales_data, mock_engine)
        pattern = AggregationPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 2
        assert "total_amount" in result.columns
        assert "total_qty" in result.columns
        assert "row_count" in result.columns
        assert "avg_amount" in result.columns

    def test_composite_grain(self, mock_engine, mock_config, sample_sales_data):
        """Test aggregation with composite grain."""
        mock_config.params = {
            "grain": ["date_sk", "product_sk"],
            "measures": [{"name": "total_amount", "expr": "SUM(amount)"}],
        }

        context = create_pandas_context(sample_sales_data, mock_engine)
        pattern = AggregationPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 4

        row = result[(result["date_sk"] == 20240102) & (result["product_sk"] == 1)]
        assert row.iloc[0]["total_amount"] == 140.0


class TestAggregationPatternHaving:
    """Test HAVING clause functionality."""

    def test_having_filters_results(self, mock_engine, mock_config, sample_sales_data):
        """Test that HAVING clause filters aggregation results."""
        mock_config.params = {
            "grain": ["date_sk"],
            "measures": [{"name": "total_amount", "expr": "SUM(amount)"}],
            "having": "SUM(amount) > 200",
        }

        context = create_pandas_context(sample_sales_data, mock_engine)
        pattern = AggregationPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 1
        assert result.iloc[0]["date_sk"] == 20240102


class TestAggregationPatternIncremental:
    """Test incremental aggregation functionality."""

    def test_incremental_replace_overwrites(self, mock_engine, mock_config, tmp_path):
        """Test that replace strategy overwrites matching grain keys."""
        existing_path = tmp_path / "agg_sales.parquet"

        existing_df = pd.DataFrame(
            {
                "date_sk": [20240101, 20240102],
                "total_amount": [100.0, 200.0],
            }
        )
        existing_df.to_parquet(existing_path)

        new_data = pd.DataFrame(
            {
                "date_sk": [20240102, 20240102, 20240103],
                "amount": [50.0, 75.0, 100.0],
            }
        )

        mock_config.params = {
            "grain": ["date_sk"],
            "measures": [{"name": "total_amount", "expr": "SUM(amount)"}],
            "incremental": {
                "timestamp_column": "date_sk",
                "merge_strategy": "replace",
            },
            "target": str(existing_path),
        }

        context = create_pandas_context(new_data, mock_engine)
        pattern = AggregationPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 3

        day1 = result[result["date_sk"] == 20240101]
        assert day1.iloc[0]["total_amount"] == 100.0

        day2 = result[result["date_sk"] == 20240102]
        assert day2.iloc[0]["total_amount"] == 125.0

        day3 = result[result["date_sk"] == 20240103]
        assert day3.iloc[0]["total_amount"] == 100.0

    def test_incremental_sum_adds_values(self, mock_engine, mock_config, tmp_path):
        """Test that sum strategy adds to existing values."""
        existing_path = tmp_path / "agg_sales.parquet"

        existing_df = pd.DataFrame(
            {
                "date_sk": [20240101, 20240102],
                "total_amount": [100.0, 200.0],
            }
        )
        existing_df.to_parquet(existing_path)

        new_data = pd.DataFrame(
            {
                "date_sk": [20240102, 20240103],
                "amount": [50.0, 100.0],
            }
        )

        mock_config.params = {
            "grain": ["date_sk"],
            "measures": [{"name": "total_amount", "expr": "SUM(amount)"}],
            "incremental": {
                "timestamp_column": "date_sk",
                "merge_strategy": "sum",
            },
            "target": str(existing_path),
        }

        context = create_pandas_context(new_data, mock_engine)
        pattern = AggregationPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 3

        day1 = result[result["date_sk"] == 20240101]
        assert day1.iloc[0]["total_amount"] == 100.0

        day2 = result[result["date_sk"] == 20240102]
        assert day2.iloc[0]["total_amount"] == 250.0

        day3 = result[result["date_sk"] == 20240103]
        assert day3.iloc[0]["total_amount"] == 100.0

    def test_incremental_no_existing_target(self, mock_engine, mock_config, tmp_path):
        """Test incremental when target doesn't exist yet."""
        non_existent_path = tmp_path / "new_agg.parquet"

        data = pd.DataFrame(
            {
                "date_sk": [20240101, 20240102],
                "amount": [100.0, 200.0],
            }
        )

        mock_config.params = {
            "grain": ["date_sk"],
            "measures": [{"name": "total_amount", "expr": "SUM(amount)"}],
            "incremental": {
                "timestamp_column": "date_sk",
                "merge_strategy": "replace",
            },
            "target": str(non_existent_path),
        }

        context = create_pandas_context(data, mock_engine)
        pattern = AggregationPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 2


class TestAggregationPatternAuditColumns:
    """Test audit column functionality."""

    def test_adds_load_timestamp(self, mock_engine, mock_config, sample_sales_data):
        """Test that load_timestamp is added when enabled."""
        mock_config.params = {
            "grain": ["date_sk"],
            "measures": [{"name": "total", "expr": "SUM(amount)"}],
            "audit": {"load_timestamp": True},
        }

        context = create_pandas_context(sample_sales_data, mock_engine)
        pattern = AggregationPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert "load_timestamp" in result.columns
        assert pd.notna(result["load_timestamp"].iloc[0])

    def test_adds_source_system(self, mock_engine, mock_config, sample_sales_data):
        """Test that source_system is added when specified."""
        mock_config.params = {
            "grain": ["date_sk"],
            "measures": [{"name": "total", "expr": "SUM(amount)"}],
            "audit": {"source_system": "dw"},
        }

        context = create_pandas_context(sample_sales_data, mock_engine)
        pattern = AggregationPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert "source_system" in result.columns
        assert result["source_system"].iloc[0] == "dw"


class TestAggregationPatternIntegration:
    """Integration tests for full aggregation workflows."""

    def test_full_aggregation_workflow(self, mock_engine, mock_config, sample_sales_data):
        """Test complete aggregation pattern with all features."""
        mock_config.params = {
            "grain": ["date_sk", "region"],
            "measures": [
                {"name": "total_amount", "expr": "SUM(amount)"},
                {"name": "total_qty", "expr": "SUM(quantity)"},
                {"name": "order_count", "expr": "COUNT(*)"},
            ],
            "audit": {
                "load_timestamp": True,
                "source_system": "sales",
            },
        }

        context = create_pandas_context(sample_sales_data, mock_engine)
        pattern = AggregationPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert "date_sk" in result.columns
        assert "region" in result.columns
        assert "total_amount" in result.columns
        assert "total_qty" in result.columns
        assert "order_count" in result.columns
        assert "load_timestamp" in result.columns
        assert "source_system" in result.columns

        east_day1 = result[(result["date_sk"] == 20240101) & (result["region"] == "East")]
        assert east_day1.iloc[0]["total_amount"] == 150.0
        assert east_day1.iloc[0]["total_qty"] == 15
        assert east_day1.iloc[0]["order_count"] == 2

    def test_pattern_registered(self):
        """Test that AggregationPattern is registered in patterns registry."""
        from odibi.patterns import get_pattern_class

        pattern_class = get_pattern_class("aggregation")
        assert pattern_class == AggregationPattern

    def test_min_max_aggregations(self, mock_engine, mock_config, sample_sales_data):
        """Test MIN and MAX aggregations."""
        mock_config.params = {
            "grain": ["date_sk"],
            "measures": [
                {"name": "min_amount", "expr": "MIN(amount)"},
                {"name": "max_amount", "expr": "MAX(amount)"},
            ],
        }

        context = create_pandas_context(sample_sales_data, mock_engine)
        pattern = AggregationPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        day1 = result[result["date_sk"] == 20240101]
        assert day1.iloc[0]["min_amount"] == 50.0
        assert day1.iloc[0]["max_amount"] == 100.0

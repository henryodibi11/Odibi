"""Tests for Materializer and IncrementalMaterializer."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.semantics.materialize import (
    IncrementalMaterializer,
    MaterializationResult,
    Materializer,
)
from odibi.semantics.metrics import (
    DimensionDefinition,
    MaterializationConfig,
    MetricDefinition,
    SemanticLayerConfig,
)


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
        ],
        dimensions=[
            DimensionDefinition(name="region"),
            DimensionDefinition(name="month"),
        ],
        materializations=[
            MaterializationConfig(
                name="monthly_revenue",
                metrics=["revenue", "order_count"],
                dimensions=["region", "month"],
                output="gold/agg_monthly_revenue",
                schedule="0 2 1 * *",
            ),
            MaterializationConfig(
                name="daily_orders",
                metrics=["order_count"],
                dimensions=["region"],
                output="gold/agg_daily_orders",
            ),
        ],
    )


@pytest.fixture
def sample_df():
    """Create a sample DataFrame."""
    return pd.DataFrame(
        {
            "region": ["East", "West", "East", "West"],
            "month": ["Jan", "Jan", "Feb", "Feb"],
            "amount": [100, 200, 150, 250],
        }
    )


class TestMaterializationResult:
    """Test MaterializationResult dataclass."""

    def test_success_result(self):
        """Test creating a success result."""
        result = MaterializationResult(
            name="test_mat",
            output="gold/test",
            row_count=100,
            elapsed_ms=500.0,
            success=True,
        )

        assert result.name == "test_mat"
        assert result.success is True
        assert result.error is None

    def test_failure_result(self):
        """Test creating a failure result."""
        result = MaterializationResult(
            name="test_mat",
            output="gold/test",
            row_count=0,
            elapsed_ms=100.0,
            success=False,
            error="Something went wrong",
        )

        assert result.success is False
        assert result.error == "Something went wrong"


class TestMaterializerInit:
    """Test Materializer initialization."""

    def test_init_stores_config(self, sample_config):
        """Test that init stores the config."""
        materializer = Materializer(sample_config)
        assert materializer.config is sample_config

    def test_init_creates_query(self, sample_config):
        """Test that init creates a SemanticQuery."""
        materializer = Materializer(sample_config)
        assert materializer._query is not None


class TestMaterializerExecute:
    """Test Materializer execute method."""

    @patch.object(Materializer, "_build_query_string")
    def test_execute_success(self, mock_build_query, sample_config, sample_df):
        """Test successful materialization execution."""
        mock_build_query.return_value = "revenue, order_count BY region, month"

        materializer = Materializer(sample_config)

        with patch.object(materializer._query, "execute") as mock_execute:
            mock_result = MagicMock()
            mock_result.df = sample_df
            mock_result.row_count = len(sample_df)
            mock_execute.return_value = mock_result

            context = create_pandas_context(sample_df)
            result = materializer.execute("monthly_revenue", context)

            assert result.success is True
            assert result.name == "monthly_revenue"
            assert result.output == "gold/agg_monthly_revenue"

    def test_execute_unknown_materialization(self, sample_config, sample_df):
        """Test execution with unknown materialization name."""
        materializer = Materializer(sample_config)
        context = create_pandas_context(sample_df)

        with pytest.raises(ValueError, match="not found"):
            materializer.execute("nonexistent", context)

    @patch.object(Materializer, "_build_query_string")
    def test_execute_with_write_callback(self, mock_build_query, sample_config, sample_df):
        """Test execution with write callback."""
        mock_build_query.return_value = "revenue BY region"

        materializer = Materializer(sample_config)
        write_callback = MagicMock()

        with patch.object(materializer._query, "execute") as mock_execute:
            mock_result = MagicMock()
            mock_result.df = sample_df
            mock_result.row_count = len(sample_df)
            mock_execute.return_value = mock_result

            context = create_pandas_context(sample_df)
            materializer.execute("monthly_revenue", context, write_callback)

            write_callback.assert_called_once_with(sample_df, "gold/agg_monthly_revenue")

    @patch.object(Materializer, "_build_query_string")
    def test_execute_handles_error(self, mock_build_query, sample_config, sample_df):
        """Test execution returns failure result on error."""
        mock_build_query.return_value = "revenue BY region"

        materializer = Materializer(sample_config)

        with patch.object(materializer._query, "execute") as mock_execute:
            mock_execute.side_effect = ValueError("Query failed")

            context = create_pandas_context(sample_df)
            result = materializer.execute("monthly_revenue", context)

            assert result.success is False
            assert "Query failed" in result.error


class TestMaterializerExecuteAll:
    """Test Materializer execute_all method."""

    @patch.object(Materializer, "execute")
    def test_execute_all(self, mock_execute, sample_config, sample_df):
        """Test executing all materializations."""
        mock_execute.return_value = MaterializationResult(
            name="test",
            output="test",
            row_count=10,
            elapsed_ms=100.0,
            success=True,
        )

        materializer = Materializer(sample_config)
        context = create_pandas_context(sample_df)

        results = materializer.execute_all(context)

        assert len(results) == 2
        assert mock_execute.call_count == 2


class TestMaterializerBuildQueryString:
    """Test Materializer _build_query_string method."""

    def test_build_query_string(self, sample_config):
        """Test building query string from materialization config."""
        materializer = Materializer(sample_config)

        mat_config = sample_config.get_materialization("monthly_revenue")
        query_string = materializer._build_query_string(mat_config)

        assert "revenue" in query_string
        assert "order_count" in query_string
        assert "BY" in query_string
        assert "region" in query_string
        assert "month" in query_string


class TestMaterializerHelpers:
    """Test Materializer helper methods."""

    def test_get_schedule(self, sample_config):
        """Test getting schedule for materialization."""
        materializer = Materializer(sample_config)

        schedule = materializer.get_schedule("monthly_revenue")
        assert schedule == "0 2 1 * *"

        schedule = materializer.get_schedule("daily_orders")
        assert schedule is None

    def test_get_schedule_unknown(self, sample_config):
        """Test getting schedule for unknown materialization."""
        materializer = Materializer(sample_config)
        schedule = materializer.get_schedule("nonexistent")
        assert schedule is None

    def test_list_materializations(self, sample_config):
        """Test listing all materializations."""
        materializer = Materializer(sample_config)

        mats = materializer.list_materializations()

        assert len(mats) == 2
        assert mats[0]["name"] == "monthly_revenue"
        assert mats[0]["metrics"] == ["revenue", "order_count"]
        assert mats[0]["dimensions"] == ["region", "month"]
        assert mats[0]["output"] == "gold/agg_monthly_revenue"


class TestIncrementalMaterializer:
    """Test IncrementalMaterializer."""

    def test_init(self, sample_config):
        """Test IncrementalMaterializer initialization."""
        inc_materializer = IncrementalMaterializer(sample_config)
        assert inc_materializer.config is sample_config
        assert inc_materializer._base_materializer is not None

    def test_execute_incremental_unknown_materialization(self, sample_config, sample_df):
        """Test incremental execution with unknown materialization."""
        inc_materializer = IncrementalMaterializer(sample_config)
        context = create_pandas_context(sample_df)

        with pytest.raises(ValueError, match="not found"):
            inc_materializer.execute_incremental(
                "nonexistent",
                context,
                sample_df,
                "timestamp",
            )


class TestIncrementalMaterializerMerge:
    """Test IncrementalMaterializer merge strategies."""

    def test_merge_pandas_replace(self, sample_config, sample_df):
        """Test merge with replace strategy."""
        inc_materializer = IncrementalMaterializer(sample_config)

        existing_df = pd.DataFrame(
            {
                "region": ["East", "West"],
                "month": ["Jan", "Jan"],
                "revenue": [100, 200],
            }
        )

        new_df = pd.DataFrame(
            {
                "region": ["East"],
                "month": ["Jan"],
                "revenue": [150],
            }
        )

        merged = inc_materializer._merge_pandas(
            existing_df,
            new_df,
            dimensions=["region", "month"],
            metrics=["revenue"],
            strategy="replace",
        )

        assert len(merged) == 2
        east_jan = merged[(merged["region"] == "East") & (merged["month"] == "Jan")]
        assert east_jan.iloc[0]["revenue"] == 150

    def test_merge_pandas_sum(self, sample_config, sample_df):
        """Test merge with sum strategy."""
        inc_materializer = IncrementalMaterializer(sample_config)

        existing_df = pd.DataFrame(
            {
                "region": ["East", "West"],
                "revenue": [100, 200],
            }
        )

        new_df = pd.DataFrame(
            {
                "region": ["East", "West"],
                "revenue": [50, 100],
            }
        )

        merged = inc_materializer._merge_pandas(
            existing_df,
            new_df,
            dimensions=["region"],
            metrics=["revenue"],
            strategy="sum",
        )

        east_row = merged[merged["region"] == "East"]
        assert east_row.iloc[0]["revenue"] == 150

        west_row = merged[merged["region"] == "West"]
        assert west_row.iloc[0]["revenue"] == 300

    def test_merge_pandas_unknown_strategy(self, sample_config, sample_df):
        """Test merge with unknown strategy raises error."""
        inc_materializer = IncrementalMaterializer(sample_config)

        with pytest.raises(ValueError, match="Unknown merge strategy"):
            inc_materializer._merge_pandas(
                sample_df,
                sample_df,
                dimensions=["region"],
                metrics=["revenue"],
                strategy="invalid",
            )

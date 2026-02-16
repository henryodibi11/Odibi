"""Unit tests for Enhanced FactPattern."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from odibi.config import NodeConfig
from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.patterns.fact import FactPattern


def create_pandas_context(df, engine=None, dimensions=None):
    """Helper to create a PandasContext with a DataFrame and optional dimensions."""
    pandas_context = PandasContext()
    if dimensions:
        for name, dim_df in dimensions.items():
            pandas_context.register(name, dim_df)

    ctx = EngineContext(
        context=pandas_context,
        df=df,
        engine_type=EngineType.PANDAS,
        engine=engine,
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
def sample_fact_data():
    """Sample fact data for testing."""
    return pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4],
            "customer_id": ["C001", "C002", "C003", "C001"],
            "product_id": ["P001", "P002", "P001", "P003"],
            "quantity": [10, 5, 3, 7],
            "price": [100.0, 200.0, 150.0, 50.0],
        }
    )


@pytest.fixture
def sample_customer_dim():
    """Sample customer dimension."""
    return pd.DataFrame(
        {
            "customer_id": ["C001", "C002"],
            "customer_sk": [1, 2],
            "name": ["Alice", "Bob"],
            "is_current": [True, True],
        }
    )


@pytest.fixture
def sample_product_dim():
    """Sample product dimension."""
    return pd.DataFrame(
        {
            "product_id": ["P001", "P002", "P003"],
            "product_sk": [10, 20, 30],
            "name": ["Widget", "Gadget", "Gizmo"],
        }
    )


class TestFactPatternValidation:
    """Test FactPattern validation."""

    def test_validate_deduplicate_requires_keys(self, mock_engine, mock_config):
        """Test that keys are required when deduplicate is true."""
        mock_config.params = {"deduplicate": True}

        pattern = FactPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="keys"):
            pattern.validate()

    def test_validate_invalid_orphan_handling(self, mock_engine, mock_config):
        """Test that invalid orphan_handling raises error."""
        mock_config.params = {"orphan_handling": "ignore"}

        pattern = FactPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="orphan_handling"):
            pattern.validate()

    def test_validate_dimension_missing_source_column(self, mock_engine, mock_config):
        """Test that dimensions must have required keys."""
        mock_config.params = {
            "dimensions": [
                {
                    "dimension_table": "dim_customer",
                    "dimension_key": "customer_id",
                    "surrogate_key": "customer_sk",
                }
            ]
        }

        pattern = FactPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="source_column"):
            pattern.validate()

    def test_validate_success_basic(self, mock_engine, mock_config):
        """Test validation passes with basic params."""
        mock_config.params = {}
        pattern = FactPattern(mock_engine, mock_config)
        pattern.validate()

    def test_validate_success_full_config(self, mock_engine, mock_config):
        """Test validation passes with full config."""
        mock_config.params = {
            "grain": ["order_id"],
            "dimensions": [
                {
                    "source_column": "customer_id",
                    "dimension_table": "dim_customer",
                    "dimension_key": "customer_id",
                    "surrogate_key": "customer_sk",
                    "scd2": True,
                }
            ],
            "orphan_handling": "unknown",
            "deduplicate": True,
            "keys": ["order_id"],
        }
        pattern = FactPattern(mock_engine, mock_config)
        pattern.validate()


class TestFactPatternDeduplication:
    """Test deduplication functionality."""

    def test_deduplicate_removes_duplicates(self, mock_engine, mock_config):
        """Test that deduplication removes duplicate rows."""
        mock_config.params = {
            "deduplicate": True,
            "keys": ["order_id"],
        }

        df = pd.DataFrame(
            {
                "order_id": [1, 1, 2, 3],
                "value": [100, 100, 200, 300],
            }
        )

        context = create_pandas_context(df, mock_engine)
        pattern = FactPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 3

    def test_no_deduplicate_keeps_all(self, mock_engine, mock_config):
        """Test that without deduplication all rows are kept."""
        mock_config.params = {}

        df = pd.DataFrame(
            {
                "order_id": [1, 1, 2, 3],
                "value": [100, 100, 200, 300],
            }
        )

        context = create_pandas_context(df, mock_engine)
        pattern = FactPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 4


class TestFactPatternDimensionLookup:
    """Test dimension lookup functionality."""

    def test_single_dimension_lookup(
        self, mock_engine, mock_config, sample_fact_data, sample_customer_dim
    ):
        """Test looking up SK from a single dimension."""
        mock_config.params = {
            "dimensions": [
                {
                    "source_column": "customer_id",
                    "dimension_table": "dim_customer",
                    "dimension_key": "customer_id",
                    "surrogate_key": "customer_sk",
                }
            ],
            "orphan_handling": "unknown",
        }

        context = create_pandas_context(
            sample_fact_data,
            mock_engine,
            dimensions={"dim_customer": sample_customer_dim},
        )

        pattern = FactPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert "customer_sk" in result.columns

        c001_rows = result[result["customer_id"] == "C001"]
        assert all(c001_rows["customer_sk"] == 1)

        c002_rows = result[result["customer_id"] == "C002"]
        assert all(c002_rows["customer_sk"] == 2)

    def test_multiple_dimension_lookups(
        self,
        mock_engine,
        mock_config,
        sample_fact_data,
        sample_customer_dim,
        sample_product_dim,
    ):
        """Test looking up SKs from multiple dimensions."""
        mock_config.params = {
            "dimensions": [
                {
                    "source_column": "customer_id",
                    "dimension_table": "dim_customer",
                    "dimension_key": "customer_id",
                    "surrogate_key": "customer_sk",
                },
                {
                    "source_column": "product_id",
                    "dimension_table": "dim_product",
                    "dimension_key": "product_id",
                    "surrogate_key": "product_sk",
                },
            ],
            "orphan_handling": "unknown",
        }

        context = create_pandas_context(
            sample_fact_data,
            mock_engine,
            dimensions={
                "dim_customer": sample_customer_dim,
                "dim_product": sample_product_dim,
            },
        )

        pattern = FactPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert "customer_sk" in result.columns
        assert "product_sk" in result.columns

        row = result[result["order_id"] == 2].iloc[0]
        assert row["customer_sk"] == 2
        assert row["product_sk"] == 20

    def test_scd2_filter_current_records(self, mock_engine, mock_config, sample_fact_data):
        """Test that SCD2 dimensions filter for is_current=True."""
        dim_with_history = pd.DataFrame(
            {
                "customer_id": ["C001", "C001", "C002"],
                "customer_sk": [1, 2, 3],
                "is_current": [False, True, True],
            }
        )

        mock_config.params = {
            "dimensions": [
                {
                    "source_column": "customer_id",
                    "dimension_table": "dim_customer",
                    "dimension_key": "customer_id",
                    "surrogate_key": "customer_sk",
                    "scd2": True,
                }
            ],
            "orphan_handling": "unknown",
        }

        context = create_pandas_context(
            sample_fact_data,
            mock_engine,
            dimensions={"dim_customer": dim_with_history},
        )

        pattern = FactPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        c001_rows = result[result["customer_id"] == "C001"]
        assert all(c001_rows["customer_sk"] == 2)


class TestFactPatternOrphanHandling:
    """Test orphan handling functionality."""

    def test_orphan_handling_unknown(
        self, mock_engine, mock_config, sample_fact_data, sample_customer_dim
    ):
        """Test that orphans get SK=0 when orphan_handling is 'unknown'."""
        mock_config.params = {
            "dimensions": [
                {
                    "source_column": "customer_id",
                    "dimension_table": "dim_customer",
                    "dimension_key": "customer_id",
                    "surrogate_key": "customer_sk",
                }
            ],
            "orphan_handling": "unknown",
        }

        context = create_pandas_context(
            sample_fact_data,
            mock_engine,
            dimensions={"dim_customer": sample_customer_dim},
        )

        pattern = FactPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        c003_rows = result[result["customer_id"] == "C003"]
        assert len(c003_rows) == 1
        assert c003_rows.iloc[0]["customer_sk"] == 0

    def test_orphan_handling_reject(
        self, mock_engine, mock_config, sample_fact_data, sample_customer_dim
    ):
        """Test that orphans raise error when orphan_handling is 'reject'."""
        mock_config.params = {
            "dimensions": [
                {
                    "source_column": "customer_id",
                    "dimension_table": "dim_customer",
                    "dimension_key": "customer_id",
                    "surrogate_key": "customer_sk",
                }
            ],
            "orphan_handling": "reject",
        }

        context = create_pandas_context(
            sample_fact_data,
            mock_engine,
            dimensions={"dim_customer": sample_customer_dim},
        )

        pattern = FactPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="orphan records"):
            pattern.execute(context)

    def test_no_orphans_reject_succeeds(self, mock_engine, mock_config, sample_customer_dim):
        """Test that reject mode works when there are no orphans."""
        fact_df = pd.DataFrame(
            {
                "order_id": [1, 2],
                "customer_id": ["C001", "C002"],
            }
        )

        mock_config.params = {
            "dimensions": [
                {
                    "source_column": "customer_id",
                    "dimension_table": "dim_customer",
                    "dimension_key": "customer_id",
                    "surrogate_key": "customer_sk",
                }
            ],
            "orphan_handling": "reject",
        }

        context = create_pandas_context(
            fact_df,
            mock_engine,
            dimensions={"dim_customer": sample_customer_dim},
        )

        pattern = FactPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 2
        assert all(result["customer_sk"] > 0)


class TestFactPatternGrainValidation:
    """Test grain validation functionality."""

    def test_grain_validation_passes(self, mock_engine, mock_config):
        """Test that grain validation passes with unique rows."""
        mock_config.params = {
            "grain": ["order_id"],
        }

        df = pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "value": [100, 200, 300],
            }
        )

        context = create_pandas_context(df, mock_engine)
        pattern = FactPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 3

    def test_grain_validation_fails_with_duplicates(self, mock_engine, mock_config):
        """Test that grain validation fails with duplicate rows."""
        mock_config.params = {
            "grain": ["order_id"],
        }

        df = pd.DataFrame(
            {
                "order_id": [1, 1, 2],
                "value": [100, 100, 200],
            }
        )

        context = create_pandas_context(df, mock_engine)
        pattern = FactPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="Grain validation failed"):
            pattern.execute(context)

    def test_grain_validation_composite_key(self, mock_engine, mock_config):
        """Test grain validation with composite key."""
        mock_config.params = {
            "grain": ["order_id", "line_item"],
        }

        df = pd.DataFrame(
            {
                "order_id": [1, 1, 2],
                "line_item": [1, 2, 1],
                "value": [100, 150, 200],
            }
        )

        context = create_pandas_context(df, mock_engine)
        pattern = FactPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 3


class TestFactPatternMeasures:
    """Test measure calculation functionality."""

    def test_calculated_measure(self, mock_engine, mock_config):
        """Test adding a calculated measure."""
        mock_config.params = {
            "measures": [{"total_amount": "quantity * price"}],
        }

        df = pd.DataFrame(
            {
                "order_id": [1, 2],
                "quantity": [10, 5],
                "price": [100.0, 200.0],
            }
        )

        context = create_pandas_context(df, mock_engine)
        pattern = FactPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert "total_amount" in result.columns
        assert result.iloc[0]["total_amount"] == 1000.0
        assert result.iloc[1]["total_amount"] == 1000.0

    def test_rename_measure(self, mock_engine, mock_config):
        """Test renaming a measure column."""
        mock_config.params = {
            "measures": [{"unit_price": "price"}],
        }

        df = pd.DataFrame(
            {
                "order_id": [1, 2],
                "price": [100.0, 200.0],
            }
        )

        context = create_pandas_context(df, mock_engine)
        pattern = FactPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert "unit_price" in result.columns
        assert "price" not in result.columns

    def test_passthrough_measure(self, mock_engine, mock_config):
        """Test passthrough measures (string only)."""
        mock_config.params = {
            "measures": ["quantity", "price"],
        }

        df = pd.DataFrame(
            {
                "order_id": [1],
                "quantity": [10],
                "price": [100.0],
            }
        )

        context = create_pandas_context(df, mock_engine)
        pattern = FactPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert "quantity" in result.columns
        assert "price" in result.columns


class TestFactPatternAuditColumns:
    """Test audit column functionality."""

    def test_adds_load_timestamp(self, mock_engine, mock_config):
        """Test that load_timestamp is added when enabled."""
        mock_config.params = {
            "audit": {"load_timestamp": True},
        }

        df = pd.DataFrame({"order_id": [1], "value": [100]})

        context = create_pandas_context(df, mock_engine)
        pattern = FactPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert "load_timestamp" in result.columns
        assert pd.notna(result["load_timestamp"].iloc[0])

    def test_adds_source_system(self, mock_engine, mock_config):
        """Test that source_system is added when specified."""
        mock_config.params = {
            "audit": {"source_system": "pos"},
        }

        df = pd.DataFrame({"order_id": [1], "value": [100]})

        context = create_pandas_context(df, mock_engine)
        pattern = FactPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert "source_system" in result.columns
        assert result["source_system"].iloc[0] == "pos"


class TestFactPatternIntegration:
    """Integration tests for full fact pattern workflows."""

    def test_full_fact_workflow(
        self,
        mock_engine,
        mock_config,
        sample_fact_data,
        sample_customer_dim,
        sample_product_dim,
    ):
        """Test complete fact pattern with all features."""
        mock_config.params = {
            "grain": ["order_id"],
            "deduplicate": True,
            "keys": ["order_id"],
            "dimensions": [
                {
                    "source_column": "customer_id",
                    "dimension_table": "dim_customer",
                    "dimension_key": "customer_id",
                    "surrogate_key": "customer_sk",
                },
                {
                    "source_column": "product_id",
                    "dimension_table": "dim_product",
                    "dimension_key": "product_id",
                    "surrogate_key": "product_sk",
                },
            ],
            "orphan_handling": "unknown",
            "measures": [
                "quantity",
                {"total_amount": "quantity * price"},
            ],
            "audit": {
                "load_timestamp": True,
                "source_system": "pos",
            },
        }

        context = create_pandas_context(
            sample_fact_data,
            mock_engine,
            dimensions={
                "dim_customer": sample_customer_dim,
                "dim_product": sample_product_dim,
            },
        )

        pattern = FactPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 4
        assert "customer_sk" in result.columns
        assert "product_sk" in result.columns
        assert "total_amount" in result.columns
        assert "load_timestamp" in result.columns
        assert "source_system" in result.columns

        row1 = result[result["order_id"] == 1].iloc[0]
        assert row1["customer_sk"] == 1
        assert row1["product_sk"] == 10
        assert row1["total_amount"] == 1000.0

        orphan_row = result[result["customer_id"] == "C003"].iloc[0]
        assert orphan_row["customer_sk"] == 0

    def test_pattern_registered(self):
        """Test that FactPattern is registered in patterns registry."""
        from odibi.patterns import get_pattern_class

        pattern_class = get_pattern_class("fact")
        assert pattern_class == FactPattern

    def test_backward_compatibility(self, mock_engine, mock_config):
        """Test that basic usage (pre-enhancement) still works."""
        mock_config.params = {
            "deduplicate": True,
            "keys": ["order_id"],
        }

        df = pd.DataFrame(
            {
                "order_id": [1, 1, 2],
                "value": [100, 100, 200],
            }
        )

        context = create_pandas_context(df, mock_engine)
        pattern = FactPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 2


class TestFactPatternIsExpression:
    """Test _is_expression edge cases."""

    def test_column_name_with_hyphen_not_expression(self, mock_engine, mock_config):
        """Column names with hyphens should NOT be detected as expressions."""
        mock_config.params = {}
        pattern = FactPattern(mock_engine, mock_config)
        assert pattern._is_expression("total-cost") is False

    def test_simple_column_name_not_expression(self, mock_engine, mock_config):
        """Simple column names are not expressions."""
        mock_config.params = {}
        pattern = FactPattern(mock_engine, mock_config)
        assert pattern._is_expression("total_cost") is False

    def test_arithmetic_is_expression(self, mock_engine, mock_config):
        """Arithmetic expressions are detected."""
        mock_config.params = {}
        pattern = FactPattern(mock_engine, mock_config)
        assert pattern._is_expression("quantity * price") is True

    def test_function_call_is_expression(self, mock_engine, mock_config):
        """Function calls with parentheses are detected as expressions."""
        mock_config.params = {}
        pattern = FactPattern(mock_engine, mock_config)
        assert pattern._is_expression("COALESCE(a, b)") is True

    def test_underscore_column_not_expression(self, mock_engine, mock_config):
        """Column names with underscores are not expressions."""
        mock_config.params = {}
        pattern = FactPattern(mock_engine, mock_config)
        assert pattern._is_expression("total_revenue") is False


class TestFactPatternQuarantineParentDirectories:
    """Test that quarantine writer creates parent directories."""

    def test_write_quarantine_pandas_creates_parent_dirs_csv(
        self, mock_engine, mock_config, tmp_path
    ):
        """Test that _write_quarantine_pandas creates parent directories for CSV files."""

        mock_config.params = {}
        pattern = FactPattern(mock_engine, mock_config)

        # Create a DataFrame to write
        df = pd.DataFrame(
            {
                "order_id": [1, 2],
                "customer_id": ["C999", "C998"],
                "_rejection_reason": ["Missing dimension", "Missing dimension"],
            }
        )

        # Use a nested path that doesn't exist
        nested_path = tmp_path / "data" / "quarantine" / "nested" / "orphans.csv"
        assert not nested_path.parent.exists(), "Parent directory should not exist before test"

        # Mock context without connections
        mock_context = MagicMock()
        mock_context.engine = None

        # Call the quarantine writer
        pattern._write_quarantine_pandas(
            context=mock_context,
            df=df,
            connection="",
            path=str(nested_path),
            table=None,
        )

        # Verify directory was created and file exists
        assert nested_path.parent.exists(), "Parent directories should be created"
        assert nested_path.exists(), "Quarantine file should be created"

        # Verify data was written correctly
        result_df = pd.read_csv(nested_path)
        assert len(result_df) == 2
        assert "order_id" in result_df.columns

    def test_write_quarantine_pandas_creates_parent_dirs_parquet(
        self, mock_engine, mock_config, tmp_path
    ):
        """Test that _write_quarantine_pandas creates parent directories for Parquet files."""

        mock_config.params = {}
        pattern = FactPattern(mock_engine, mock_config)

        # Create a DataFrame to write
        df = pd.DataFrame(
            {
                "order_id": [1, 2],
                "customer_id": ["C999", "C998"],
                "_rejection_reason": ["Missing dimension", "Missing dimension"],
            }
        )

        # Use a nested path that doesn't exist
        nested_path = tmp_path / "data" / "quarantine" / "nested" / "orphans.parquet"
        assert not nested_path.parent.exists(), "Parent directory should not exist before test"

        # Mock context without connections
        mock_context = MagicMock()
        mock_context.engine = None

        # Call the quarantine writer
        pattern._write_quarantine_pandas(
            context=mock_context,
            df=df,
            connection="",
            path=str(nested_path),
            table=None,
        )

        # Verify directory was created and file exists
        assert nested_path.parent.exists(), "Parent directories should be created"
        assert nested_path.exists(), "Quarantine file should be created"

        # Verify data was written correctly
        result_df = pd.read_parquet(nested_path)
        assert len(result_df) == 2
        assert "order_id" in result_df.columns

    def test_write_quarantine_pandas_bare_filename_no_crash(
        self, mock_engine, mock_config, tmp_path
    ):
        """Test that _write_quarantine_pandas handles bare filenames without crashing."""
        import os

        # Change to tmp_path so bare filename is written there
        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)

            mock_config.params = {}
            pattern = FactPattern(mock_engine, mock_config)

            # Create a DataFrame to write
            df = pd.DataFrame(
                {
                    "order_id": [1],
                    "_rejection_reason": ["Missing dimension"],
                }
            )

            # Mock context without connections
            mock_context = MagicMock()
            mock_context.engine = None

            # Call with bare filename (no directory path)
            # This should NOT crash even though dirname returns empty string
            pattern._write_quarantine_pandas(
                context=mock_context,
                df=df,
                connection="",
                path="orphans.csv",
                table=None,
            )

            # Verify file was created in current directory
            assert (tmp_path / "orphans.csv").exists()
        finally:
            os.chdir(original_cwd)

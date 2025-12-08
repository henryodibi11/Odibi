"""Unit tests for DimensionPattern."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from odibi.config import NodeConfig
from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.patterns.dimension import DimensionPattern


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
    config.params = {}
    return config


class TestDimensionPatternValidation:
    """Test DimensionPattern validation."""

    def test_validate_requires_natural_key(self, mock_engine, mock_config):
        """Test that natural_key is required."""
        mock_config.params = {"surrogate_key": "customer_sk"}

        pattern = DimensionPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="natural_key"):
            pattern.validate()

    def test_validate_requires_surrogate_key(self, mock_engine, mock_config):
        """Test that surrogate_key is required."""
        mock_config.params = {"natural_key": "customer_id"}

        pattern = DimensionPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="surrogate_key"):
            pattern.validate()

    def test_validate_invalid_scd_type(self, mock_engine, mock_config):
        """Test that invalid scd_type raises error."""
        mock_config.params = {
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd_type": 3,
        }

        pattern = DimensionPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="scd_type"):
            pattern.validate()

    def test_validate_scd2_requires_target(self, mock_engine, mock_config):
        """Test that SCD2 requires target parameter."""
        mock_config.params = {
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd_type": 2,
            "track_columns": ["name"],
        }

        pattern = DimensionPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="target"):
            pattern.validate()

    def test_validate_scd1_requires_track_columns(self, mock_engine, mock_config):
        """Test that SCD1 requires track_columns."""
        mock_config.params = {
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd_type": 1,
        }

        pattern = DimensionPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="track_columns"):
            pattern.validate()

    def test_validate_scd2_requires_track_columns(self, mock_engine, mock_config):
        """Test that SCD2 requires track_columns."""
        mock_config.params = {
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd_type": 2,
            "target": "gold/dim_customer",
        }

        pattern = DimensionPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="track_columns"):
            pattern.validate()

    def test_validate_success_scd0(self, mock_engine, mock_config):
        """Test validation passes for SCD0."""
        mock_config.params = {
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd_type": 0,
        }

        pattern = DimensionPattern(mock_engine, mock_config)
        pattern.validate()

    def test_validate_success_scd1(self, mock_engine, mock_config):
        """Test validation passes for SCD1."""
        mock_config.params = {
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd_type": 1,
            "track_columns": ["name", "email"],
        }

        pattern = DimensionPattern(mock_engine, mock_config)
        pattern.validate()

    def test_validate_success_scd2(self, mock_engine, mock_config):
        """Test validation passes for SCD2."""
        mock_config.params = {
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd_type": 2,
            "track_columns": ["name", "email"],
            "target": "gold/dim_customer",
        }

        pattern = DimensionPattern(mock_engine, mock_config)
        pattern.validate()


class TestDimensionPatternSCD0:
    """Test SCD Type 0 (static) behavior."""

    def test_scd0_first_run_generates_surrogate_keys(self, mock_engine, mock_config, tmp_path):
        """Test first run generates surrogate keys starting from 1."""
        mock_config.params = {
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd_type": 0,
            "audit": {"load_timestamp": False},
        }

        source_df = pd.DataFrame(
            {"customer_id": ["A", "B", "C"], "name": ["Alice", "Bob", "Charlie"]}
        )

        context = create_pandas_context(source_df, mock_engine)

        pattern = DimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert "customer_sk" in result.columns
        assert list(result["customer_sk"]) == [1, 2, 3]
        assert len(result) == 3

    def test_scd0_subsequent_run_only_inserts_new(self, mock_engine, mock_config, tmp_path):
        """Test subsequent run only inserts new records, preserves existing."""
        target_path = tmp_path / "dim_customer.parquet"

        existing_df = pd.DataFrame(
            {
                "customer_id": ["A", "B"],
                "name": ["Alice", "Bob"],
                "customer_sk": [1, 2],
            }
        )
        existing_df.to_parquet(target_path)

        mock_config.params = {
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd_type": 0,
            "target": str(target_path),
            "audit": {"load_timestamp": False},
        }

        source_df = pd.DataFrame(
            {
                "customer_id": ["A", "B", "C"],
                "name": ["Alice Updated", "Bob Updated", "Charlie"],
            }
        )

        context = create_pandas_context(source_df, mock_engine)

        pattern = DimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 3
        assert "customer_sk" in result.columns

        charlie = result[result["customer_id"] == "C"]
        assert len(charlie) == 1
        assert charlie.iloc[0]["customer_sk"] == 3

        alice = result[result["customer_id"] == "A"]
        assert alice.iloc[0]["name"] == "Alice"


class TestDimensionPatternSCD1:
    """Test SCD Type 1 (overwrite) behavior."""

    def test_scd1_first_run_generates_surrogate_keys(self, mock_engine, mock_config):
        """Test first run generates surrogate keys."""
        mock_config.params = {
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd_type": 1,
            "track_columns": ["name"],
            "audit": {"load_timestamp": False},
        }

        source_df = pd.DataFrame({"customer_id": ["A", "B"], "name": ["Alice", "Bob"]})

        context = create_pandas_context(source_df, mock_engine)

        pattern = DimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert "customer_sk" in result.columns
        assert len(result) == 2

    def test_scd1_overwrites_existing_records(self, mock_engine, mock_config, tmp_path):
        """Test SCD1 overwrites changes."""
        target_path = tmp_path / "dim_customer.parquet"

        existing_df = pd.DataFrame(
            {
                "customer_id": ["A", "B"],
                "name": ["Alice", "Bob"],
                "customer_sk": [1, 2],
            }
        )
        existing_df.to_parquet(target_path)

        mock_config.params = {
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd_type": 1,
            "track_columns": ["name"],
            "target": str(target_path),
            "audit": {"load_timestamp": False},
        }

        source_df = pd.DataFrame(
            {
                "customer_id": ["A", "C"],
                "name": ["Alice Updated", "Charlie"],
            }
        )

        context = create_pandas_context(source_df, mock_engine)

        pattern = DimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        alice = result[result["customer_id"] == "A"]
        assert alice.iloc[0]["name"] == "Alice Updated"
        assert alice.iloc[0]["customer_sk"] == 1

        charlie = result[result["customer_id"] == "C"]
        assert charlie.iloc[0]["customer_sk"] == 3

        bob = result[result["customer_id"] == "B"]
        assert bob.iloc[0]["name"] == "Bob"


class TestDimensionPatternAuditColumns:
    """Test audit column functionality."""

    def test_adds_load_timestamp(self, mock_engine, mock_config):
        """Test that load_timestamp is added when enabled."""
        mock_config.params = {
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd_type": 0,
            "audit": {"load_timestamp": True},
        }

        source_df = pd.DataFrame({"customer_id": ["A"], "name": ["Alice"]})

        context = create_pandas_context(source_df, mock_engine)

        pattern = DimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert "load_timestamp" in result.columns
        assert pd.notna(result["load_timestamp"].iloc[0])

    def test_adds_source_system(self, mock_engine, mock_config):
        """Test that source_system is added when specified."""
        mock_config.params = {
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd_type": 0,
            "audit": {"load_timestamp": False, "source_system": "crm"},
        }

        source_df = pd.DataFrame({"customer_id": ["A"], "name": ["Alice"]})

        context = create_pandas_context(source_df, mock_engine)

        pattern = DimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert "source_system" in result.columns
        assert result["source_system"].iloc[0] == "crm"


class TestDimensionPatternUnknownMember:
    """Test unknown member functionality."""

    def test_adds_unknown_member_row(self, mock_engine, mock_config):
        """Test that unknown member row is added with SK=0."""
        mock_config.params = {
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd_type": 0,
            "unknown_member": True,
            "audit": {"load_timestamp": False},
        }

        source_df = pd.DataFrame({"customer_id": ["A", "B"], "name": ["Alice", "Bob"]})

        context = create_pandas_context(source_df, mock_engine)

        pattern = DimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 3

        unknown = result[result["customer_sk"] == 0]
        assert len(unknown) == 1
        assert unknown.iloc[0]["customer_id"] == "-1"
        assert unknown.iloc[0]["name"] == "Unknown"

    def test_unknown_member_not_duplicated(self, mock_engine, mock_config, tmp_path):
        """Test that unknown member is not added if already exists."""
        target_path = tmp_path / "dim_customer.parquet"

        existing_df = pd.DataFrame(
            {
                "customer_id": ["-1", "A"],
                "name": ["Unknown", "Alice"],
                "customer_sk": [0, 1],
            }
        )
        existing_df.to_parquet(target_path)

        mock_config.params = {
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd_type": 0,
            "target": str(target_path),
            "unknown_member": True,
            "audit": {"load_timestamp": False},
        }

        source_df = pd.DataFrame({"customer_id": ["B"], "name": ["Bob"]})

        context = create_pandas_context(source_df, mock_engine)

        pattern = DimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        unknown_count = (result["customer_sk"] == 0).sum()
        assert unknown_count == 1


class TestDimensionPatternIntegration:
    """Integration tests for full dimension pattern workflows."""

    def test_full_dimension_workflow(self, mock_engine, mock_config):
        """Test complete dimension pattern with all features."""
        mock_config.params = {
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd_type": 0,
            "unknown_member": True,
            "audit": {"load_timestamp": True, "source_system": "crm"},
        }

        source_df = pd.DataFrame(
            {
                "customer_id": ["C001", "C002"],
                "name": ["John", "Jane"],
                "email": ["john@mail.com", "jane@mail.com"],
                "city": ["NYC", "LA"],
            }
        )

        context = create_pandas_context(source_df, mock_engine)

        pattern = DimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 3
        assert "customer_sk" in result.columns
        assert "load_timestamp" in result.columns
        assert "source_system" in result.columns

        unknown = result[result["customer_sk"] == 0]
        assert len(unknown) == 1
        assert unknown.iloc[0]["customer_id"] == "-1"

        data_rows = result[result["customer_sk"] > 0]
        assert len(data_rows) == 2
        assert set(data_rows["customer_id"]) == {"C001", "C002"}

    def test_pattern_registered(self):
        """Test that DimensionPattern is registered in patterns registry."""
        from odibi.patterns import get_pattern_class

        pattern_class = get_pattern_class("dimension")
        assert pattern_class == DimensionPattern

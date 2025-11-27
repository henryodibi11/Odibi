from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.config import (
    ColumnMetadata,
    NodeConfig,
    PrivacyConfig,
    PrivacyMethod,
    SchemaMode,
    SchemaPolicyConfig,
)
from odibi.node import NodeExecutor


@pytest.fixture
def mock_context():
    return MagicMock()


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.name = "pandas"
    return engine


@pytest.fixture
def connections():
    return {"src": MagicMock(), "dst": MagicMock()}


def test_schema_policy_application(mock_context, mock_engine, connections):
    """Test that schema policy triggers harmonize_schema in write phase."""
    # Setup Input DF
    df = pd.DataFrame({"id": [1], "new_col": ["x"]})

    # Mock engine methods
    # 1. Table Exists (for first run check) - say it exists
    mock_engine.table_exists.return_value = True

    # 2. Get Schema - return target schema
    target_schema = {"id": "int64", "existing_col": "object"}
    mock_engine.get_table_schema.return_value = target_schema

    # 3. Harmonize - return modified df
    harmonized_df = pd.DataFrame({"id": [1], "existing_col": [None]})
    mock_engine.harmonize_schema.return_value = harmonized_df

    # Config with Schema Policy
    config = NodeConfig(
        name="test_node",
        read={"connection": "src", "format": "csv", "path": "src.csv"},
        write={"connection": "dst", "format": "csv", "path": "dst.csv"},
        schema_policy=SchemaPolicyConfig(mode=SchemaMode.ENFORCE),
    )

    executor = NodeExecutor(mock_context, mock_engine, connections)

    # Execute (Patch read to skip IO)
    with patch.object(executor, "_execute_read_phase", return_value=(df, None)):
        result = executor.execute(config)

    assert result.success

    # Verify get_table_schema called
    mock_engine.get_table_schema.assert_called_with(
        connection=connections["dst"], table=None, path="dst.csv", format="csv"
    )

    # Verify harmonize_schema called
    mock_engine.harmonize_schema.assert_called_once()
    args, _ = mock_engine.harmonize_schema.call_args
    # args: (df, target_schema, policy)
    pd.testing.assert_frame_equal(args[0], df)
    assert args[1] == target_schema
    assert args[2].mode == SchemaMode.ENFORCE

    # Verify write called with harmonized df
    mock_engine.write.assert_called_once()
    _, kwargs = mock_engine.write.call_args
    pd.testing.assert_frame_equal(kwargs["df"], harmonized_df)


def test_schema_policy_skipped_if_no_target(mock_context, mock_engine, connections):
    """Test that schema policy is skipped if target table doesn't exist."""
    df = pd.DataFrame({"id": [1]})

    # Mock table not existing or schema not found
    mock_engine.get_table_schema.return_value = None

    config = NodeConfig(
        name="test_node",
        read={"connection": "src", "format": "csv", "path": "src.csv"},
        write={"connection": "dst", "format": "csv", "path": "dst.csv"},
        schema_policy=SchemaPolicyConfig(mode=SchemaMode.ENFORCE),
    )

    executor = NodeExecutor(mock_context, mock_engine, connections)

    with patch.object(executor, "_execute_read_phase", return_value=(df, None)):
        result = executor.execute(config)

    assert result.success
    mock_engine.harmonize_schema.assert_not_called()

    # Write should use original df
    mock_engine.write.assert_called_once()
    _, kwargs = mock_engine.write.call_args
    pd.testing.assert_frame_equal(kwargs["df"], df)


def test_privacy_anonymization(mock_context, mock_engine, connections):
    """Test that privacy config triggers engine.anonymize."""
    df = pd.DataFrame({"email": ["a@b.com"]})
    anonymized_df = pd.DataFrame({"email": ["hash123"]})

    mock_engine.anonymize.return_value = anonymized_df

    config = NodeConfig(
        name="privacy_node",
        read={"connection": "src", "format": "csv", "path": "src.csv"},
        columns={"email": ColumnMetadata(pii=True)},
        privacy=PrivacyConfig(method=PrivacyMethod.HASH, salt="salty"),
        # Must have write or transform to be valid node? No, checks "at least one of read, transform, write"
    )

    executor = NodeExecutor(mock_context, mock_engine, connections)

    with patch.object(executor, "_execute_read_phase", return_value=(df, None)):
        result = executor.execute(config)

    assert result.success

    # Verify anonymize called
    mock_engine.anonymize.assert_called_once()
    args, _ = mock_engine.anonymize.call_args
    # args: (df, columns, method, salt)
    pd.testing.assert_frame_equal(args[0], df)
    assert args[1] == ["email"]
    assert args[2] == PrivacyMethod.HASH
    assert args[3] == "salty"

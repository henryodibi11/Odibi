from unittest.mock import MagicMock

import pytest

from odibi.config import ColumnMetadata, NodeConfig, PrivacyConfig, PrivacyMethod
from odibi.context import PandasContext
from odibi.node import NodeExecutor


@pytest.fixture
def context():
    return PandasContext()


@pytest.fixture
def executor(context):
    engine = MagicMock()
    engine.name = "pandas"
    # Mock anonymize to return input
    engine.anonymize.side_effect = lambda df, cols, m, s: df

    # Mock get_sample
    engine.get_sample.return_value = []
    engine.profile_nulls.return_value = {}
    engine.get_source_files.return_value = []
    engine.get_schema.return_value = {}
    engine.count_rows.return_value = 0

    return NodeExecutor(context=context, engine=engine, connections={})


def test_pii_inheritance(executor, context):
    # Upstream node has PII
    context.register("upstream", MagicMock(), metadata={"pii_columns": {"email": True}})

    config = NodeConfig(
        name="current",
        depends_on=["upstream"],
        # No local PII definitions
        transformer="deduplicate",  # Minimal valid config
        params={"keys": ["email"]},
    )

    pii = executor._calculate_pii(config)
    assert pii.get("email") is True


def test_pii_local_merge(executor, context):
    # Upstream
    context.register("upstream", MagicMock(), metadata={"pii_columns": {"email": True}})

    # Local adds 'phone'
    config = NodeConfig(
        name="current",
        depends_on=["upstream"],
        columns={"phone": ColumnMetadata(pii=True)},
        transformer="deduplicate",
        params={"keys": ["email"]},
    )

    pii = executor._calculate_pii(config)
    assert pii.get("email") is True
    assert pii.get("phone") is True


def test_declassify(executor, context):
    # Upstream has email (PII)
    context.register("upstream", MagicMock(), metadata={"pii_columns": {"email": True}})

    # Local declassifies email
    config = NodeConfig(
        name="current",
        depends_on=["upstream"],
        privacy=PrivacyConfig(method=PrivacyMethod.HASH, declassify=["email"]),
        transformer="deduplicate",
        params={"keys": ["email"]},
    )

    pii = executor._calculate_pii(config)
    assert "email" not in pii

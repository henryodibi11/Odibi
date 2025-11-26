import pandas as pd
import pytest
from odibi.config import NodeConfig, PrivacyConfig, PrivacyMethod, ColumnMetadata
from odibi.node import Node
from odibi.context import PandasContext
from odibi.engine.pandas_engine import PandasEngine


@pytest.fixture
def simple_context():
    return PandasContext()


@pytest.fixture
def pandas_engine():
    return PandasEngine()


def test_pandas_privacy_hash(simple_context, pandas_engine):
    df = pd.DataFrame({"id": [1, 2], "email": ["a@b.com", "c@d.com"]})
    simple_context.register("input", df)

    config = NodeConfig(
        name="privacy_node",
        depends_on=["input"],
        columns={"email": ColumnMetadata(pii=True)},
        privacy=PrivacyConfig(method=PrivacyMethod.HASH),
        # Dummy read or transform to satisfy validator
        transform={"steps": []},
    )

    node = Node(config, simple_context, pandas_engine, {})
    result = node.execute()

    assert result.success
    res_df = simple_context.get("privacy_node")

    # Check that email is hashed (len 64 for sha256)
    assert len(res_df["email"][0]) == 64
    assert res_df["email"][0] != "a@b.com"
    # Check that id is untouched
    assert res_df["id"][0] == 1


def test_pandas_privacy_mask(simple_context, pandas_engine):
    df = pd.DataFrame({"ccn": ["1234567890123456"]})
    simple_context.register("input", df)

    config = NodeConfig(
        name="mask_node",
        depends_on=["input"],
        columns={"ccn": ColumnMetadata(pii=True)},
        privacy=PrivacyConfig(method=PrivacyMethod.MASK),
        transform={"steps": []},
    )

    node = Node(config, simple_context, pandas_engine, {})
    node.execute()

    res_df = simple_context.get("mask_node")
    masked = res_df["ccn"][0]
    # Regex mask: .(?=.{4}) -> *
    # 123456789012 3456
    # ************ 3456
    assert masked.endswith("3456")
    assert masked.startswith("************")
    assert len(masked) == 16


def test_pandas_privacy_redact(simple_context, pandas_engine):
    df = pd.DataFrame({"secret": ["my_secret"]})
    simple_context.register("input", df)

    config = NodeConfig(
        name="redact_node",
        depends_on=["input"],
        columns={"secret": ColumnMetadata(pii=True)},
        privacy=PrivacyConfig(method=PrivacyMethod.REDACT),
        transform={"steps": []},
    )

    node = Node(config, simple_context, pandas_engine, {})
    node.execute()

    res_df = simple_context.get("redact_node")
    assert res_df["secret"][0] == "[REDACTED]"

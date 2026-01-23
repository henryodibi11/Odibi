from odibi_mcp.contracts.envelope import MCPEnvelope, PolicyApplied
from odibi_mcp.contracts.enums import TruncatedReason
from datetime import datetime


def test_policy_applied_model():
    obj = PolicyApplied(project_scoped=True, path_filtered=False)
    dump = obj.model_dump()
    assert dump["project_scoped"] is True
    assert dump["path_filtered"] is False


def test_mcp_envelope_model():
    env = MCPEnvelope(
        tool_name="test",
        project="demo",
        truncated=True,
        truncated_reason=TruncatedReason.BYTE_LIMIT,
        timestamp=datetime.now(),
    )
    dump = env.model_dump()

    assert dump["tool_name"] == "test"
    assert dump["project"] == "demo"
    assert dump["truncated_reason"] == TruncatedReason.BYTE_LIMIT
    assert isinstance(dump["timestamp"], datetime)

    env = MCPEnvelope(
        tool_name="test",
        project="demo",
        truncated=True,
        truncated_reason=TruncatedReason.BYTE_LIMIT,
        timestamp=datetime.now(),
    )
    dump = env.model_dump()

    assert dump["tool_name"] == "test"
    assert dump["project"] == "demo"
    assert dump["truncated_reason"] == TruncatedReason.BYTE_LIMIT
    assert isinstance(dump["timestamp"], datetime)

from odibi_mcp.server import call_tool
from odibi_mcp.audit.logger import AuditLogger
import asyncio


class DummyLogger:
    def __init__(self):
        self.logged = []

    def info(self, msg, extra=None):
        self.logged.append((msg, extra))


class FakeTextContent:
    def __init__(self, type, text):
        self.type = type
        self.text = text


def test_audit_entry_logged(monkeypatch):
    # Patch AuditLogger to use DummyLogger
    dummy = DummyLogger()
    monkeypatch.setattr("odibi_mcp.server.audit_logger", AuditLogger(dummy))

    # Patch get_knowledge to always return a no-op object for one tool
    class Knowledge:
        def list_transformers(self):
            return {"foo": "bar"}

        def _with_context(self, val):
            return val

    monkeypatch.setattr("odibi_mcp.server.get_knowledge", lambda: Knowledge())
    # Patch TextContent to stub class
    monkeypatch.setattr("odibi_mcp.server.TextContent", FakeTextContent)
    # Prepare arguments
    args = {}
    loop = asyncio.get_event_loop()
    loop.run_until_complete(call_tool("list_transformers", args))
    # Validate dummy logger captured audit
    assert dummy.logged
    (msg, extra) = dummy.logged[0]
    assert msg == "MCP tool call"
    assert extra["tool"] == "list_transformers"
    assert extra["success"] is True
    assert extra["policy"] == {}
    assert "request_id" in extra

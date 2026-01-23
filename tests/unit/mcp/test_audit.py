from odibi_mcp.audit.entry import AuditEntry
from odibi_mcp.audit.logger import AuditLogger
from datetime import datetime


def test_redact_args_redacts_sensitive_keys():
    args = {"token": "abc", "something": "x", "password": "p", "key42": "shouldredact", "arg": "ok"}
    redacted = AuditLogger.redact_args(args)
    assert redacted["token"] == "[REDACTED]"
    assert redacted["password"] == "[REDACTED]"
    assert redacted["key42"] == "[REDACTED]"
    assert redacted["something"] == "x"
    assert redacted["arg"] == "ok"


def test_audit_logger_logs_entry(monkeypatch):
    logs = {}

    class DummyLogger:
        def info(self, msg, extra=None):
            logs["msg"] = msg
            logs["extra"] = extra

    logger = AuditLogger(DummyLogger())
    entry = AuditEntry(
        timestamp=datetime.now(),
        request_id="abc",
        tool_name="foo",
        project="proj",
        environment="prod",
        connection="conn",
        resource_logical="mynode",
        args_summary={"ok": 1},
        duration_ms=1.2,
        success=True,
        error_type=None,
        bytes_read_estimate=None,
        policy_applied={"project_scoped": True},
    )
    logger.log(entry)
    assert logs["msg"] == "MCP tool call"
    assert logs["extra"]["request_id"] == "abc"
    assert logs["extra"]["tool"] == "foo"
    assert logs["extra"]["project"] == "proj"
    assert logs["extra"]["policy"]["project_scoped"] is True

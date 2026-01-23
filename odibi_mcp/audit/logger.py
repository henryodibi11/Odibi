"""
odibi_mcp.audit.logger

AuditLogger for structured logging of MCP tool calls.
Implements redaction and structured message per MCP spec.
"""
import logging
from typing import Dict, Any
from .entry import AuditEntry


class AuditLogger:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def log(self, entry: AuditEntry) -> None:
        self.logger.info(
            "MCP tool call",
            extra={
                "request_id": entry.request_id,
                "tool": entry.tool_name,
                "project": entry.project,
                "env": entry.environment,
                "connection": entry.connection,
                "resource": entry.resource_logical,
                "duration_ms": entry.duration_ms,
                "success": entry.success,
                "error_type": entry.error_type,
                "policy": entry.policy_applied,
            },
        )

    @staticmethod
    def redact_args(args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Redact sensitive arguments before logging.
        Fields such as password/token/secret/key/credential are replaced with [REDACTED].
        """
        redact_keys = {"password", "token", "secret", "key", "credential"}
        return {
            k: "[REDACTED]" if any(r in k.lower() for r in redact_keys) else v
            for k, v in args.items()
        }

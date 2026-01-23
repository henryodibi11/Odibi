"""
odibi_mcp.audit.entry

AuditEntry dataclass for structured MCP audit logging.
Matches all required fields per MCP spec including policy/applied audit.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any


@dataclass
class AuditEntry:
    timestamp: datetime
    request_id: str
    tool_name: str
    project: str
    environment: str
    connection: Optional[str]
    resource_logical: Optional[str]  # Logical name only - never physical
    args_summary: Dict[str, Any]  # Redacted sensitive values
    duration_ms: float
    success: bool
    error_type: Optional[str]
    bytes_read_estimate: Optional[int]
    policy_applied: Dict[str, bool]

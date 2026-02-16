# odibi_mcp/contracts/envelope.py

from pydantic import BaseModel, Field
from typing import Optional


class PolicyApplied(BaseModel):
    """
    Record of policies applied to this response (MCPEnvelope).
    Matches MCP spec: project scoping, connection policy, path filtering, sampling, physical ref gating.
    """

    project_scoped: bool = True
    connection_policy: Optional[str] = None
    path_filtered: bool = False
    sample_capped: bool = False
    physical_ref_allowed: bool = False  # Explicit indicator per spec
    physical_ref_included: bool = False  # Whether it was actually included in result


# MCPEnvelope will be implemented next in this file


# ... existing code ...

from datetime import datetime, timezone
from uuid import uuid4
from odibi_mcp.contracts.enums import TruncatedReason
from odibi import __version__  # Ensure odibi core is in your PYTHONPATH


class MCPEnvelope(BaseModel):
    """
    Standard envelope for all MCP responses.
    Wraps all tool response payloads with policy summary and request metadata.
    Matches MCP spec exactly.
    """

    request_id: str = Field(default_factory=lambda: str(uuid4()))
    tool_name: str
    mcp_schema_version: str = __version__
    project: str
    environment: str = "production"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    duration_ms: Optional[float] = None
    truncated: bool = False
    truncated_reason: Optional[TruncatedReason] = None
    policy_applied: PolicyApplied = Field(default_factory=PolicyApplied)


# ... rest of file ...

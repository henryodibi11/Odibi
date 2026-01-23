# odibi_mcp/utils/errors.py
"""Error handling utilities for MCP."""

from dataclasses import dataclass
from typing import Optional
from enum import Enum


class MCPErrorCode(str, Enum):
    """Standard MCP error codes."""

    PERMISSION_DENIED = "permission_denied"
    NOT_FOUND = "not_found"
    VALIDATION_ERROR = "validation_error"
    INTERNAL_ERROR = "internal_error"
    RATE_LIMITED = "rate_limited"
    TIMEOUT = "timeout"
    INVALID_ARGUMENT = "invalid_argument"


@dataclass
class MCPError:
    """Structured MCP error response."""

    code: MCPErrorCode
    message: str
    details: Optional[dict] = None
    tool_name: Optional[str] = None
    request_id: Optional[str] = None


def create_error_response(
    code: MCPErrorCode,
    message: str,
    tool_name: Optional[str] = None,
    request_id: Optional[str] = None,
    details: Optional[dict] = None,
) -> MCPError:
    """
    Create a standardized error response.

    Args:
        code: Error code from MCPErrorCode
        message: Human-readable error message
        tool_name: Name of the tool that failed
        request_id: Request ID for correlation
        details: Additional error details

    Returns:
        MCPError instance
    """
    return MCPError(
        code=code,
        message=message,
        tool_name=tool_name,
        request_id=request_id,
        details=details,
    )


def handle_permission_error(
    resource: str,
    action: str,
    tool_name: Optional[str] = None,
) -> MCPError:
    """Create a permission denied error."""
    return create_error_response(
        code=MCPErrorCode.PERMISSION_DENIED,
        message=f"Permission denied: cannot {action} on {resource}",
        tool_name=tool_name,
    )


def handle_not_found_error(
    resource_type: str,
    resource_name: str,
    tool_name: Optional[str] = None,
) -> MCPError:
    """Create a not found error."""
    return create_error_response(
        code=MCPErrorCode.NOT_FOUND,
        message=f"{resource_type} '{resource_name}' not found",
        tool_name=tool_name,
    )


def handle_validation_error(
    message: str,
    field: Optional[str] = None,
    tool_name: Optional[str] = None,
) -> MCPError:
    """Create a validation error."""
    details = {"field": field} if field else None
    return create_error_response(
        code=MCPErrorCode.VALIDATION_ERROR,
        message=message,
        tool_name=tool_name,
        details=details,
    )


def wrap_exception(
    exc: Exception,
    tool_name: Optional[str] = None,
    request_id: Optional[str] = None,
) -> MCPError:
    """
    Wrap a Python exception into an MCPError.

    Maps known exception types to appropriate error codes.
    """
    if isinstance(exc, PermissionError):
        code = MCPErrorCode.PERMISSION_DENIED
    elif isinstance(exc, FileNotFoundError):
        code = MCPErrorCode.NOT_FOUND
    elif isinstance(exc, ValueError):
        code = MCPErrorCode.INVALID_ARGUMENT
    elif isinstance(exc, TimeoutError):
        code = MCPErrorCode.TIMEOUT
    else:
        code = MCPErrorCode.INTERNAL_ERROR

    return create_error_response(
        code=code,
        message=str(exc),
        tool_name=tool_name,
        request_id=request_id,
        details={"exception_type": type(exc).__name__},
    )

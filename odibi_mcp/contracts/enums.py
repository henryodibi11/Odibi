# odibi_mcp/contracts/enums.py

from enum import Enum


class TruncatedReason(str, Enum):
    """
    Explicit reasons for response truncation.
    Used in MCPEnvelope.truncated_reason.
    """

    ROW_LIMIT = "row_limit"  # Hit max rows cap
    COLUMN_LIMIT = "column_limit"  # Hit max columns cap
    BYTE_LIMIT = "byte_limit"  # Hit max bytes for schema inference
    CELL_LIMIT = "cell_limit"  # Cell content truncated
    POLICY_MASKING = "policy_masking"  # Content masked by policy
    SAMPLING_ONLY = "sampling_only"  # Only sample returned
    PAGINATION = "pagination"  # More results available via next_token

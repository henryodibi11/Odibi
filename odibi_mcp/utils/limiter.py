# odibi_mcp/utils/limiter.py
"""Sample limiting utilities for MCP responses."""

from dataclasses import dataclass
from typing import List, Optional

from odibi_mcp.contracts.enums import TruncatedReason


@dataclass
class LimitConfig:
    """Configuration for sample limiting."""

    max_rows: int = 100
    max_columns: int = 50
    max_cell_length: int = 1000
    max_bytes: int = 1_000_000  # 1MB


@dataclass
class LimitResult:
    """Result of limiting operation."""

    data: List[dict]
    truncated: bool
    truncated_reason: Optional[TruncatedReason]
    original_row_count: int
    original_column_count: int


def limit_sample(
    rows: List[dict],
    config: Optional[LimitConfig] = None,
) -> LimitResult:
    """
    Apply row/column/cell limits to sample data.

    Args:
        rows: List of row dictionaries
        config: Limit configuration (uses defaults if None)

    Returns:
        LimitResult with truncated data and metadata
    """
    if config is None:
        config = LimitConfig()

    if not rows:
        return LimitResult(
            data=[],
            truncated=False,
            truncated_reason=None,
            original_row_count=0,
            original_column_count=0,
        )

    original_row_count = len(rows)
    original_column_count = len(rows[0]) if rows else 0

    truncated = False
    truncated_reason = None

    # Limit rows
    if len(rows) > config.max_rows:
        rows = rows[: config.max_rows]
        truncated = True
        truncated_reason = TruncatedReason.ROW_LIMIT

    # Limit columns
    if rows and len(rows[0]) > config.max_columns:
        columns_to_keep = list(rows[0].keys())[: config.max_columns]
        rows = [{k: v for k, v in row.items() if k in columns_to_keep} for row in rows]
        truncated = True
        truncated_reason = TruncatedReason.COLUMN_LIMIT

    # Limit cell content
    limited_rows = []
    cell_truncated = False
    for row in rows:
        limited_row = {}
        for k, v in row.items():
            if isinstance(v, str) and len(v) > config.max_cell_length:
                limited_row[k] = v[: config.max_cell_length] + "..."
                cell_truncated = True
            else:
                limited_row[k] = v
        limited_rows.append(limited_row)

    if cell_truncated:
        truncated = True
        truncated_reason = TruncatedReason.CELL_LIMIT

    return LimitResult(
        data=limited_rows,
        truncated=truncated,
        truncated_reason=truncated_reason,
        original_row_count=original_row_count,
        original_column_count=original_column_count,
    )

from pydantic import BaseModel
from typing import Dict, Any, List, Optional
from odibi_mcp.contracts.schema import SchemaChange


class DiffSummary(BaseModel):
    """Schema and row-level diff without exposing raw values by default."""

    schema_diff: SchemaChange
    row_count_diff: int  # run_b - run_a
    null_count_changes: Dict[str, int]  # column -> delta
    distinct_count_changes: Dict[str, int]  # column -> delta
    sample_diff_included: bool = False
    sample_diff: Optional[List[Dict[str, Any]]] = None

# odibi_mcp/contracts/resources.py

from pydantic import BaseModel
from typing import Optional, Literal


class ResourceRef(BaseModel):
    """
    Logical reference to a data resource. Physical ref gated by policy.
    Matches MCP spec.
    """

    kind: Literal[
        "delta_table",
        "delta_path",
        "sql_table",
        "file",
        "directory",
        "node",
        "table",
        "source",
        "sink",
        "transform",
        "pipeline",
        "parquet",
        "csv",
        "json",
        "unknown",
    ]
    logical_name: str  # e.g., sales_bronze.clean_orders
    connection: str  # logical connection name
    physical_ref: Optional[str] = None  # Only included when all gates (3) pass

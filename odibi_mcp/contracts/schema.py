from pydantic import BaseModel, Field
from typing import List, Optional, Any


class ColumnSpec(BaseModel):
    """Typed column specification. Replaces parallel arrays."""

    name: str
    dtype: str
    nullable: bool = True
    description: Optional[str] = None
    semantic_type: Optional[str] = None  # "pii", "metric", "dimension", "key"
    sample_values: Optional[List[Any]] = None  # Max 3 examples


class SchemaResponse(BaseModel):
    """Typed schema response. No parallel arrays."""

    columns: List[ColumnSpec]
    row_count: Optional[int] = None
    partition_columns: List[str] = Field(default_factory=list)


class ColumnChange(BaseModel):
    name: str
    old_type: str
    new_type: str


class SchemaChange(BaseModel):
    run_id: str
    timestamp: Any  # datetime
    added: List[ColumnSpec] = Field(default_factory=list)
    removed: List[str] = Field(default_factory=list)
    type_changed: List[ColumnChange] = Field(default_factory=list)

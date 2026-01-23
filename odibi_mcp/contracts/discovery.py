from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class FileInfo(BaseModel):
    logical_name: str
    size_bytes: int
    modified: datetime
    is_directory: bool = False
    physical_path: Optional[str] = None


class ListFilesResponse(BaseModel):
    connection: str
    path: str
    files: List[FileInfo]
    next_token: Optional[str] = None
    truncated: bool = False
    total_count: Optional[int] = None

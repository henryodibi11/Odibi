from pydantic import BaseModel
from typing import List
from datetime import datetime
from odibi_mcp.contracts.time import TimeWindow


class StatPoint(BaseModel):
    timestamp: datetime
    value: float


class NodeStatsResponse(BaseModel):
    pipeline: str
    node: str
    window: TimeWindow
    run_count: int
    success_count: int
    failure_count: int
    failure_rate: float
    avg_duration_seconds: float
    duration_trend: List[StatPoint]
    row_count_trend: List[StatPoint]

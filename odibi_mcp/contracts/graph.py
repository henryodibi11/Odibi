from pydantic import BaseModel
from typing import Literal, List, Optional


class GraphNode(BaseModel):
    id: str
    type: Literal["source", "transform", "sink"]
    label: str
    layer: Optional[str] = None  # bronze, silver, gold
    status: Optional[str] = None  # success, failed, skipped


class GraphEdge(BaseModel):
    from_node: str
    to_node: str
    edge_type: Literal["data_flow", "dependency"] = "data_flow"


class GraphData(BaseModel):
    nodes: List[GraphNode]
    edges: List[GraphEdge]

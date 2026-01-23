from odibi_mcp.contracts.graph import GraphNode, GraphEdge, GraphData


def test_graph_node():
    node = GraphNode(id="n1", type="source", label="Raw Table", layer="bronze", status="success")
    dump = node.model_dump()
    assert dump["id"] == "n1"
    assert dump["type"] == "source"
    assert dump["label"] == "Raw Table"
    assert dump["layer"] == "bronze"


def test_graph_edge():
    edge = GraphEdge(from_node="n1", to_node="n2")
    dump = edge.model_dump()
    assert dump["from_node"] == "n1"
    assert dump["to_node"] == "n2"
    assert dump["edge_type"] == "data_flow"


def test_graph_data():
    node = GraphNode(id="n1", type="source", label="Raw Table")
    edge = GraphEdge(from_node="n1", to_node="n2")
    graph = GraphData(nodes=[node], edges=[edge])
    dump = graph.model_dump()
    assert dump["nodes"][0]["id"] == "n1"
    assert dump["edges"][0]["from_node"] == "n1"

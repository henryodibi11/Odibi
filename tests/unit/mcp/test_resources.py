from odibi_mcp.contracts.resources import ResourceRef


def test_resource_ref():
    ref = ResourceRef(kind="file", logical_name="sales", connection="main")
    dump = ref.model_dump()
    assert dump["kind"] == "file"
    assert dump["logical_name"] == "sales"
    assert dump["connection"] == "main"

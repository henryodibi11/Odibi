from odibi_mcp.contracts.access import ConnectionPolicy, AccessContext


def test_connection_policy():
    obj = ConnectionPolicy(
        connection="my_conn", allowed_path_prefixes=["/data"], explicit_allow_all=True
    )
    dump = obj.model_dump()
    assert dump["connection"] == "my_conn"
    assert dump["explicit_allow_all"] is True


def test_access_context():
    context = AccessContext(authorized_projects={"demo"})
    dump = context.model_dump()
    assert "demo" in dump["authorized_projects"]

from odibi_mcp.access.context import get_access_context
from odibi_mcp.contracts.access import AccessContext, ConnectionPolicy


def test_access_context_from_config():
    config = {
        "authorized_projects": ["sales", "inventory"],
        "environment": "test",
        "connection_policies": {
            "sql": {"connection": "sql", "explicit_allow_all": True},
        },
        "physical_refs_enabled": True,
    }
    ctx = get_access_context(config)
    assert isinstance(ctx, AccessContext)
    assert ctx.environment == "test"
    assert ctx.physical_refs_enabled is True
    assert "sql" in ctx.connection_policies
    cp = ctx.connection_policies["sql"]
    assert isinstance(cp, ConnectionPolicy)
    assert cp.explicit_allow_all is True
    assert "sales" in ctx.authorized_projects

from odibi_mcp.contracts.access import AccessContext, ConnectionPolicy
from odibi_mcp.access.physical_gate import can_include_physical_ref


def make_ctx(policies=None, physical_refs_enabled=True):
    return AccessContext(
        authorized_projects={"proj"},
        environment="production",
        connection_policies=policies or {},
        physical_refs_enabled=physical_refs_enabled,
    )


def test_ref_gate_all_gates_pass():
    pol = ConnectionPolicy(connection="foo", allow_physical_refs=True)
    ctx = make_ctx({"foo": pol}, True)
    assert can_include_physical_ref(True, "foo", ctx) is True


def test_ref_gate_flag_false():
    pol = ConnectionPolicy(connection="foo", allow_physical_refs=True)
    ctx = make_ctx({"foo": pol}, True)
    assert not can_include_physical_ref(False, "foo", ctx)


def test_ref_gate_policy_false():
    pol = ConnectionPolicy(connection="foo", allow_physical_refs=False)
    ctx = make_ctx({"foo": pol}, True)
    assert not can_include_physical_ref(True, "foo", ctx)


def test_ref_gate_access_ctx_false():
    pol = ConnectionPolicy(connection="foo", allow_physical_refs=True)
    ctx = make_ctx({"foo": pol}, False)
    assert not can_include_physical_ref(True, "foo", ctx)


def test_ref_gate_missing_policy():
    ctx = make_ctx({}, True)
    assert not can_include_physical_ref(True, "foo", ctx)

"""
odibi_mcp.access.physical_gate

PhysicalRefGate utility for MCP: enforce explicit 3-gate physical path exposure policy.
Spec: All 3 conditions must pass to allow a physical ref in the response:
  1. include_physical=True (caller)
  2. ConnectionPolicy.allow_physical_refs is True
  3. AccessContext.physical_refs_enabled is True
"""

from odibi_mcp.contracts.access import AccessContext


def can_include_physical_ref(
    include_physical: bool,
    connection: str,
    access_context: AccessContext,
) -> bool:
    """
    Returns True if all 3 gates pass to expose a physical ref for this connection.
    - include_physical: Caller provided flag (bool)
    - connection: Logical connection name (str)
    - access_context: Unified MCP access policy context
    """
    if not include_physical:
        return False
    if not access_context.physical_refs_enabled:
        return False
    policy = access_context.connection_policies.get(connection)
    if not policy or not policy.allow_physical_refs:
        return False
    return True

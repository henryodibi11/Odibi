from odibi_mcp.contracts.access import AccessContext, ConnectionPolicy
from typing import Dict, Any


def get_access_context(config: Dict[str, Any]) -> AccessContext:
    """
    Instantiate AccessContext from config dict (parsed YAML or env).
    Handles nested ConnectionPolicy
    """
    cp_dict = config.get("connection_policies", {})
    connection_policies = {}
    for name, cp in cp_dict.items():
        connection_policies[name] = ConnectionPolicy(**cp)
    return AccessContext(
        authorized_projects=set(config.get("authorized_projects", [])),
        environment=config.get("environment", "production"),
        connection_policies=connection_policies,
        physical_refs_enabled=config.get("physical_refs_enabled", False),
    )

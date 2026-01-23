"""
odibi_mcp.access.path_validator

Deny-by-default path validation utility for MCP source discovery tools.
Checks if a path is allowed under a ConnectionPolicy (prefixes, explicit allow-all).
"""

from odibi_mcp.contracts.access import ConnectionPolicy


def is_path_allowed(policy: ConnectionPolicy, path: str) -> bool:
    """
    Return True if the given path is allowed under the provided ConnectionPolicy, False otherwise.
    Enforces deny-by-default: must match allowed prefix or explicit_allow_all=True AND not in denied.
    Mirrors logic used in AccessContext/ConnectionPolicy contracts.
    """
    # Denied prefix takes priority
    if any(path.startswith(prefix) for prefix in policy.denied_path_prefixes):
        return False
    # Allow all if policy is explicit
    if policy.explicit_allow_all:
        return True
    # Allow if in allowed prefixes
    if policy.allowed_path_prefixes:
        return any(path.startswith(prefix) for prefix in policy.allowed_path_prefixes)
    # Deny by default if no explicit allows
    return False

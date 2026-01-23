from pydantic import BaseModel, Field, field_validator
from typing import List, Dict, Set


class ConnectionPolicy(BaseModel):
    """
    Per-connection access policy. Deny-by-default for path discovery.
    Matches MCP spec.
    """

    connection: str
    allowed_path_prefixes: List[str] = Field(default_factory=list)
    denied_path_prefixes: List[str] = Field(default_factory=list)
    explicit_allow_all: bool = False
    max_depth: int = 5
    allow_physical_refs: bool = False

    @field_validator("allowed_path_prefixes", mode="after")
    def validate_path_access(cls, v, info):
        explicit_allow_all = info.data.get("explicit_allow_all", False)
        if not v and not explicit_allow_all:
            # Valid state - will deny all path-based discovery
            pass
        return v

    def is_path_allowed(self, path: str) -> bool:
        if any(path.startswith(prefix) for prefix in self.denied_path_prefixes):
            return False
        if self.explicit_allow_all:
            return True
        if not self.allowed_path_prefixes:
            return False
        return any(path.startswith(prefix) for prefix in self.allowed_path_prefixes)


class AccessContext(BaseModel):
    """
    Unified access enforcement context. Injected once, enforced everywhere.
    Matches MCP spec.
    """

    authorized_projects: Set[str]
    environment: str = "production"
    connection_policies: Dict[str, ConnectionPolicy] = Field(default_factory=dict)
    physical_refs_enabled: bool = False

    def check_project(self, project: str) -> None:
        if project not in self.authorized_projects:
            raise PermissionError(f"Access denied: project '{project}' not authorized")

    def check_connection(self, connection: str) -> ConnectionPolicy:
        if connection not in self.connection_policies:
            raise PermissionError(f"Access denied: connection '{connection}' not configured")
        return self.connection_policies[connection]

    def check_path(self, connection: str, path: str) -> None:
        policy = self.check_connection(connection)
        if not policy.is_path_allowed(path):
            raise PermissionError(
                f"Access denied: path '{path}' not allowed for connection '{connection}'"
            )

    def can_include_physical(self, connection: str) -> bool:
        if not self.physical_refs_enabled:
            return False
        policy = self.connection_policies.get(connection)
        return policy is not None and policy.allow_physical_refs

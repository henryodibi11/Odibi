from __future__ import annotations

import hmac
from dataclasses import dataclass
from enum import Enum
from typing import Dict, FrozenSet, List, Optional, Set

from pydantic import BaseModel, Field, field_validator


class ActionEffect(str, Enum):
    """Portable effect classes enforced at the universal dispatcher."""

    PUBLIC_READ = "public_read"
    SENSITIVE_READ = "sensitive_read"
    EXECUTION = "execution"
    FILE_WRITE = "file_write"
    SESSION_MUTATION = "session_mutation"


RESTRICTED_EFFECTS: FrozenSet[ActionEffect] = frozenset(
    effect for effect in ActionEffect if effect is not ActionEffect.PUBLIC_READ
)


@dataclass(frozen=True)
class ApplicationIdentity:
    """An authenticated application principal with explicit effect grants."""

    subject: str
    authorized_effects: FrozenSet[ActionEffect]

    def __post_init__(self) -> None:
        if not isinstance(self.subject, str) or not self.subject.strip():
            raise ValueError("Application identity subject must be a non-empty string")
        if not isinstance(self.authorized_effects, frozenset) or any(
            not isinstance(effect, ActionEffect) for effect in self.authorized_effects
        ):
            raise TypeError("authorized_effects must be a frozenset of ActionEffect values")

    def authorizes(self, effect: ActionEffect) -> bool:
        """Return whether this principal is authorized for an action effect."""
        return effect is ActionEffect.PUBLIC_READ or effect in self.authorized_effects

    @classmethod
    def authenticated_application(cls) -> ApplicationIdentity:
        """Create the principal represented by the configured HTTP Bearer token."""
        return cls(subject="configured-bearer-application", authorized_effects=RESTRICTED_EFFECTS)

    @classmethod
    def trusted_local(cls) -> ApplicationIdentity:
        """Create the explicit principal used by the direct in-process bootstrap."""
        return cls(subject="trusted-local", authorized_effects=RESTRICTED_EFFECTS)


def authenticate_bearer_identity(
    authorization_header: Optional[str], configured_token: Optional[str]
) -> Optional[ApplicationIdentity]:
    """Authenticate one configured application token without exposing its value."""
    if not isinstance(authorization_header, str) or not isinstance(configured_token, str):
        return None
    if not configured_token or any(character.isspace() for character in configured_token):
        return None

    scheme, separator, candidate = authorization_header.partition(" ")
    if (
        separator != " "
        or scheme.lower() != "bearer"
        or not candidate
        or any(character.isspace() for character in candidate)
    ):
        return None
    if not hmac.compare_digest(candidate.encode("utf-8"), configured_token.encode("utf-8")):
        return None
    return ApplicationIdentity.authenticated_application()


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

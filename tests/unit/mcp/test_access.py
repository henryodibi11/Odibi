import secrets

import pytest

from odibi_mcp.contracts.access import (
    AccessContext,
    ActionEffect,
    ApplicationIdentity,
    ConnectionPolicy,
    authenticate_bearer_identity,
)


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


def test_bearer_authentication_requires_exact_configured_credential():
    configured = secrets.token_urlsafe(32)
    different = secrets.token_urlsafe(32)

    for header, token in (
        (None, configured),
        ("", configured),
        ("Bearer", configured),
        (f"Basic {configured}", configured),
        (f"Bearer  {configured}", configured),
        (f"Bearer {configured} extra", configured),
        (f"Bearer {different}", configured),
        (f"Bearer {configured}", None),
        (f"Bearer {configured}", ""),
        (f"Bearer {configured}", "invalid token with spaces"),
    ):
        assert authenticate_bearer_identity(header, token) is None

    identity = authenticate_bearer_identity(f"Bearer {configured}", configured)

    assert isinstance(identity, ApplicationIdentity)
    assert identity.authorizes(ActionEffect.EXECUTION)
    assert identity.authorizes(ActionEffect.FILE_WRITE)


def test_application_identity_requires_explicit_typed_effect_grants():
    with pytest.raises(ValueError):
        ApplicationIdentity(subject="", authorized_effects=frozenset())
    with pytest.raises(TypeError):
        ApplicationIdentity(subject="app", authorized_effects={ActionEffect.EXECUTION})
    with pytest.raises(TypeError):
        ApplicationIdentity(subject="app", authorized_effects=frozenset({"execution"}))

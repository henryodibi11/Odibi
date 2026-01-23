# tests/integration/mcp/test_access_e2e.py
"""End-to-end tests for access enforcement."""

import pytest
import pandas as pd
from odibi_mcp.contracts.access import AccessContext, ConnectionPolicy
from odibi_mcp.access.context import get_access_context
from odibi_mcp.access.path_validator import is_path_allowed
from odibi_mcp.access.physical_gate import can_include_physical_ref
from tests.fixtures.mcp_catalog import MockCatalogManager


def test_access_context_from_config():
    """Test creating AccessContext from config dict."""
    config = {
        "authorized_projects": ["project_a", "project_b"],
        "environment": "production",
        "physical_refs_enabled": True,
        "connection_policies": {
            "adls_main": {
                "connection": "adls_main",
                "allowed_path_prefixes": ["/data/bronze/", "/data/silver/"],
                "allow_physical_refs": True,
            }
        },
    }

    ctx = get_access_context(config)

    assert "project_a" in ctx.authorized_projects
    assert "project_b" in ctx.authorized_projects
    assert ctx.physical_refs_enabled is True
    assert "adls_main" in ctx.connection_policies


def test_project_scoping_filters_data():
    """Test that project scoping filters dataframes."""
    ctx = AccessContext(
        authorized_projects={"allowed_project"},
        environment="test",
    )

    catalog = MockCatalogManager()
    catalog.set_access_context(ctx)

    df = pd.DataFrame(
        {
            "project": ["allowed_project", "denied_project", "allowed_project"],
            "value": [1, 2, 3],
        }
    )

    filtered = catalog._apply_project_scope(df)

    assert len(filtered) == 2
    assert all(filtered["project"] == "allowed_project")


def test_path_validation_deny_by_default():
    """Test that paths are denied by default."""
    policy = ConnectionPolicy(
        connection="test_conn",
        allowed_path_prefixes=[],  # No allowed prefixes
        explicit_allow_all=False,
    )

    assert is_path_allowed(policy, "/any/path") is False
    assert is_path_allowed(policy, "/data/file.csv") is False


def test_path_validation_with_allowlist():
    """Test that allowed paths pass validation."""
    policy = ConnectionPolicy(
        connection="test_conn",
        allowed_path_prefixes=["/data/bronze/", "/data/silver/"],
    )

    assert is_path_allowed(policy, "/data/bronze/file.csv") is True
    assert is_path_allowed(policy, "/data/silver/table/") is True
    assert is_path_allowed(policy, "/data/gold/file.csv") is False


def test_physical_ref_gate():
    """Test physical ref gating with all conditions."""
    policy = ConnectionPolicy(
        connection="test_conn",
        allowed_path_prefixes=["/data/"],
        allow_physical_refs=True,
    )

    ctx = AccessContext(
        authorized_projects={"test"},
        physical_refs_enabled=True,
        connection_policies={"test_conn": policy},
    )

    # All gates pass
    assert (
        can_include_physical_ref(
            include_physical=True,
            connection="test_conn",
            access_context=ctx,
        )
        is True
    )

    # Flag is False
    assert (
        can_include_physical_ref(
            include_physical=False,
            connection="test_conn",
            access_context=ctx,
        )
        is False
    )


def test_access_context_check_project():
    """Test project access check."""
    ctx = AccessContext(
        authorized_projects={"project_a"},
        environment="test",
    )

    # Should pass
    ctx.check_project("project_a")

    # Should raise
    with pytest.raises(PermissionError):
        ctx.check_project("project_b")

import pytest
from odibi_mcp.contracts.access import ConnectionPolicy
from odibi_mcp.access.path_validator import is_path_allowed


@pytest.mark.parametrize(
    "policy,path,expected",
    [
        # Denied prefix blocks all
        (
            ConnectionPolicy(
                connection="c",
                denied_path_prefixes=["foo/"],
                allowed_path_prefixes=["foo/", "bar/"],
                explicit_allow_all=False,
            ),
            "foo/data.csv",
            False,
        ),
        # Allowed prefix passes
        (
            ConnectionPolicy(
                connection="c", allowed_path_prefixes=["data/"], explicit_allow_all=False
            ),
            "data/file.csv",
            True,
        ),
        # Explicit allow all overrides denies
        (
            ConnectionPolicy(
                connection="c", denied_path_prefixes=["abc/"], explicit_allow_all=True
            ),
            "unrelated/file.csv",
            True,
        ),
        # No prefixes (deny by default)
        (ConnectionPolicy(connection="c"), "foo/test.csv", False),
        # Allowed prefix does not match
        (ConnectionPolicy(connection="c", allowed_path_prefixes=["bar/"]), "foo/test.csv", False),
        # Denied prefix doesn't match, allowed prefix matches
        (
            ConnectionPolicy(
                connection="c",
                denied_path_prefixes=["pii/"],
                allowed_path_prefixes=["test/"],
                explicit_allow_all=False,
            ),
            "test/a.csv",
            True,
        ),
        # Denied prefix matches, allowed prefix matches - deny takes priority
        (
            ConnectionPolicy(
                connection="c",
                denied_path_prefixes=["secret/"],
                allowed_path_prefixes=["secret/", "data/"],
            ),
            "secret/foo.csv",
            False,
        ),
    ],
)
def test_is_path_allowed(policy, path, expected):
    assert is_path_allowed(policy, path) == expected

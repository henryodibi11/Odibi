import secrets
from pathlib import Path

import pytest

from odibi_mcp.contracts.access import (
    AccessContext,
    ActionEffect,
    ApplicationIdentity,
    ConnectionPolicy,
    ManagedProjectAccess,
    PreparedRuntimeCall,
    RuntimeAccessDenied,
    authenticate_bearer_identity,
    sanitize_runtime_result,
)


@pytest.fixture
def managed_project(tmp_path):
    root = tmp_path / "managed"
    (root / "data" / "folder").mkdir(parents=True)
    (root / "stories").mkdir()
    (root / "exports").mkdir()
    config = root / "odibi.yaml"
    config.write_text(
        """
project: managed
connections:
  local:
    type: local
    base_path: ./data
  sql:
    type: azure_sql
story:
  connection: local
  path: ../stories
system:
  connection: local
pipelines:
  - pipeline: bounded
    nodes: []
""".lstrip(),
        encoding="utf-8",
    )
    access = ManagedProjectAccess(
        project="managed",
        project_root=root,
        config_path=config,
        export_root=root / "exports",
    )
    return root, config, access


def _prepare(access, action, **kwargs):
    return access.prepare(action, {"project": "managed", **kwargs})


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


@pytest.mark.parametrize("project", [None, "", "other", "../managed", True])
def test_runtime_access_requires_exact_project_before_config_read(
    managed_project, monkeypatch, project
):
    _, _, access = managed_project

    def unexpected_read(*args, **kwargs):
        pytest.fail("invalid project scope must deny before config read")

    monkeypatch.setattr(Path, "open", unexpected_read)

    with pytest.raises(RuntimeAccessDenied, match="PROJECT_SCOPE_REQUIRED"):
        access.prepare(
            "profile_source",
            {"project": project, "connection": "local", "path": "folder/input.csv"},
        )


@pytest.mark.parametrize(
    "path",
    [
        "../outside.csv",
        "/tmp/outside.csv",
        "C:/outside.csv",
        "C:outside.csv",
        "//server/share/outside.csv",
        r"\\server\share\outside.csv",
        r"folder\outside.csv",
        "folder//outside.csv",
        "folder/%2e%2e/outside.csv",
        "folder/file.csv:secret",
        "folder/\x00outside.csv",
        "folder/./outside.csv",
    ],
)
def test_runtime_data_path_ambiguity_denies_before_config_read(managed_project, monkeypatch, path):
    _, _, access = managed_project

    def unexpected_read(*args, **kwargs):
        pytest.fail("invalid path must deny before config read")

    monkeypatch.setattr(Path, "open", unexpected_read)

    with pytest.raises(RuntimeAccessDenied, match="PATH_SCOPE_REQUIRED"):
        _prepare(access, "profile_source", connection="local", path=path)


def test_runtime_data_symlink_escape_denied(managed_project):
    root, _, access = managed_project
    outside = root.parent / "outside"
    outside.mkdir()
    link = root / "data" / "escaped"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"symlink creation is genuinely unavailable: {error!r}")

    with pytest.raises(RuntimeAccessDenied, match="PATH_SCOPE_REQUIRED"):
        _prepare(access, "profile_source", connection="local", path="escaped/input.csv")


def test_runtime_data_canonical_in_root_success_preserves_normalized_inputs(managed_project):
    root, config, access = managed_project

    prepared = _prepare(
        access,
        "profile_folder",
        connection="local",
        folder_path="folder",
        pattern="*.csv",
        max_files=17,
    )

    assert prepared == PreparedRuntimeCall(
        action="profile_folder",
        kwargs={
            "connection": "local",
            "folder_path": "folder",
            "pattern": "*.csv",
            "max_files": 17,
        },
        project_root=root.resolve(),
        config_path=config.resolve(),
    )


@pytest.mark.parametrize(
    "root_kind",
    ["missing", "file"],
)
def test_runtime_access_rejects_missing_or_non_directory_project_root(tmp_path, root_kind):
    root = tmp_path / "root"
    if root_kind == "file":
        root.write_text("not a directory", encoding="utf-8")
    config = tmp_path / "config.yaml"
    config.write_text("project: managed\n", encoding="utf-8")
    access = ManagedProjectAccess("managed", root, config)

    with pytest.raises(RuntimeAccessDenied, match="PROJECT_SCOPE_REQUIRED"):
        _prepare(access, "profile_source", connection="local", path="input.csv")


def test_runtime_access_rejects_same_prefix_config_sibling(tmp_path):
    root = tmp_path / "project"
    sibling = tmp_path / "project-private"
    root.mkdir()
    sibling.mkdir()
    config = sibling / "odibi.yaml"
    config.write_text("project: managed\n", encoding="utf-8")
    access = ManagedProjectAccess("managed", root, config)

    with pytest.raises(RuntimeAccessDenied, match="PROJECT_SCOPE_REQUIRED"):
        _prepare(access, "profile_source", connection="local", path="input.csv")


@pytest.mark.parametrize("config_kind", ["missing", "directory", "symlink_escape"])
def test_runtime_access_rejects_invalid_config_target(tmp_path, config_kind):
    root = tmp_path / "managed"
    root.mkdir()
    config = root / "odibi.yaml"
    if config_kind == "directory":
        config.mkdir()
    elif config_kind == "symlink_escape":
        outside = tmp_path / "outside.yaml"
        outside.write_text("project: managed\n", encoding="utf-8")
        try:
            config.symlink_to(outside)
        except (NotImplementedError, OSError) as error:
            pytest.skip(f"symlink creation is genuinely unavailable: {error!r}")
    access = ManagedProjectAccess("managed", root, config)

    with pytest.raises(RuntimeAccessDenied, match="PROJECT_SCOPE_REQUIRED"):
        _prepare(access, "profile_source", connection="local", path="input.csv")


def test_runtime_access_rejects_local_connection_root_outside_project(tmp_path):
    root = tmp_path / "managed"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    config = root / "odibi.yaml"
    config.write_text(
        f"project: managed\nconnections:\n  local:\n    type: local\n    base_path: {outside}\n",
        encoding="utf-8",
    )
    access = ManagedProjectAccess("managed", root, config)

    with pytest.raises(RuntimeAccessDenied, match="PROJECT_SCOPE_REQUIRED"):
        _prepare(access, "profile_source", connection="local", path="input.csv")


def test_runtime_access_rejects_local_connection_uri_root_mismatch(tmp_path):
    root = tmp_path / "managed"
    root.mkdir()
    config = root / "odibi.yaml"
    config.write_text(
        "project: managed\nconnections:\n  local:\n    type: local\n    base_path: file:///tmp\n",
        encoding="utf-8",
    )
    access = ManagedProjectAccess("managed", root, config)

    with pytest.raises(RuntimeAccessDenied, match="PROJECT_SCOPE_REQUIRED"):
        _prepare(access, "profile_source", connection="local", path="input.csv")


@pytest.mark.parametrize("value", [True, False, 0, -1, 1.5, "10", None])
@pytest.mark.parametrize(
    "action,limit_name,kwargs",
    [
        ("map_environment", "limit", {"connection": "local"}),
        ("profile_source", "max_rows", {"connection": "local", "path": "folder/input.csv"}),
        (
            "profile_folder",
            "max_files",
            {"connection": "local", "folder_path": "folder"},
        ),
        ("node_sample", "limit", {"pipeline": "bounded", "node": "source"}),
        ("node_failed_rows", "limit", {"pipeline": "bounded", "node": "source"}),
        (
            "download_sql",
            "limit",
            {"connection": "sql", "query": "SELECT * FROM Orders", "filename": "out.csv"},
        ),
        (
            "download_table",
            "limit",
            {"connection": "sql", "table": "dbo.Orders", "filename": "out.csv"},
        ),
    ],
)
def test_runtime_privacy_limits_reject_non_positive_exact_integers(
    managed_project, action, limit_name, kwargs, value
):
    _, _, access = managed_project

    with pytest.raises(RuntimeAccessDenied, match="PRIVACY_LIMIT_REQUIRED"):
        _prepare(access, action, **kwargs, **{limit_name: value})


@pytest.mark.parametrize(
    "action,limit_name,maximum,kwargs",
    [
        ("map_environment", "limit", 100, {"connection": "local"}),
        (
            "profile_source",
            "max_rows",
            100,
            {"connection": "local", "path": "folder/input.csv"},
        ),
        (
            "profile_folder",
            "max_files",
            50,
            {"connection": "local", "folder_path": "folder"},
        ),
        ("node_sample", "limit", 100, {"pipeline": "bounded", "node": "source"}),
        (
            "node_failed_rows",
            "limit",
            100,
            {"pipeline": "bounded", "node": "source"},
        ),
        (
            "download_sql",
            "limit",
            1000,
            {"connection": "sql", "query": "SELECT * FROM Orders", "filename": "out.csv"},
        ),
        (
            "download_table",
            "limit",
            1000,
            {"connection": "sql", "table": "dbo.Orders", "filename": "out.csv"},
        ),
    ],
)
def test_runtime_privacy_limit_caps(managed_project, action, limit_name, maximum, kwargs):
    _, _, access = managed_project
    assert _prepare(access, action, **kwargs, **{limit_name: maximum}).kwargs[limit_name] == maximum
    with pytest.raises(RuntimeAccessDenied, match="PRIVACY_LIMIT_REQUIRED"):
        _prepare(access, action, **kwargs, **{limit_name: maximum + 1})


@pytest.mark.parametrize(
    "query",
    [
        "DELETE FROM Orders",
        "SELECT TOP 10 * FROM Orders",
        "SELECT * FROM OPENQUERY(remote, 'DELETE FROM Orders')",
        "SELECT * INTO Backup FROM Orders",
        "SELECT * FROM Orders WHERE private = 1",
        "SELECT * FROM Orders; DROP TABLE Orders",
        "SELECT * FROM Orders -- LIMIT 1",
        "WITH rows AS (SELECT * FROM Orders) SELECT * FROM rows",
    ],
)
def test_download_sql_rejects_unbounded_or_effectful_queries(managed_project, query):
    _, _, access = managed_project

    with pytest.raises(RuntimeAccessDenied, match="INVALID_RUNTIME_ARGUMENT"):
        _prepare(
            access,
            "download_sql",
            connection="sql",
            query=query,
            filename="orders.csv",
        )


@pytest.mark.parametrize(
    "filename",
    [
        "orders.xlsx",
        "orders.exe",
        "../orders.csv",
        "/tmp/orders.csv",
        "C:orders.csv",
        r"folder\orders.csv",
        "orders.csv:secret",
        "orders%2ecsv",
        True,
    ],
)
def test_download_filename_and_format_allowlist(managed_project, filename):
    _, _, access = managed_project

    with pytest.raises(RuntimeAccessDenied):
        _prepare(
            access,
            "download_table",
            connection="sql",
            table="dbo.Orders",
            filename=filename,
        )


def test_download_destination_is_controlled_project_relative_and_collision_safe(managed_project):
    root, config, access = managed_project

    prepared = _prepare(
        access,
        "download_table",
        connection="sql",
        table="dbo.Orders",
        filename="orders.parquet",
    )

    assert prepared.output_path == root / "exports" / "orders.parquet"
    assert prepared.public_output_path == "exports/orders.parquet"
    assert prepared.kwargs == {
        "connection": "sql",
        "table": "dbo.Orders",
        "limit": 1000,
        "output_path": str(root / "exports" / "orders.parquet"),
    }
    assert prepared.config_path == config

    prepared.output_path.write_text("existing", encoding="utf-8")
    with pytest.raises(RuntimeAccessDenied, match="EXPORT_SCOPE_REQUIRED"):
        _prepare(
            access,
            "download_table",
            connection="sql",
            table="dbo.Orders",
            filename="orders.parquet",
        )


def test_download_rejects_export_root_symlink_escape(managed_project):
    root, config, _ = managed_project
    outside = root.parent / "outside-exports"
    outside.mkdir()
    export_link = root / "escaped-exports"
    try:
        export_link.symlink_to(outside, target_is_directory=True)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"symlink creation is genuinely unavailable: {error!r}")
    access = ManagedProjectAccess("managed", root, config, export_link)

    with pytest.raises(RuntimeAccessDenied, match="EXPORT_SCOPE_REQUIRED"):
        _prepare(
            access,
            "download_file",
            connection="local",
            source_path="folder/input.csv",
            filename="input.csv",
        )


@pytest.mark.parametrize("export_kind", ["missing", "file"])
def test_download_rejects_missing_or_non_directory_export_root(managed_project, export_kind):
    root, config, _ = managed_project
    export = root / "invalid-export"
    if export_kind == "file":
        export.write_text("not a directory", encoding="utf-8")
    access = ManagedProjectAccess("managed", root, config, export)

    with pytest.raises(RuntimeAccessDenied, match="EXPORT_SCOPE_REQUIRED"):
        _prepare(
            access,
            "download_table",
            connection="sql",
            table="dbo.Orders",
            filename="orders.csv",
        )


@pytest.mark.parametrize("action", ["lineage_graph", "diagnose"])
def test_remote_physical_reference_actions_deny_before_config_read(
    managed_project, monkeypatch, action
):
    _, _, access = managed_project

    def unexpected_read(*args, **kwargs):
        pytest.fail("remote physical-reference action must deny before config or data read")

    monkeypatch.setattr(Path, "open", unexpected_read)
    with pytest.raises(RuntimeAccessDenied, match="PHYSICAL_REFERENCES_DISABLED"):
        kwargs = {"pipeline": "bounded"} if action == "lineage_graph" else {}
        _prepare(access, action, **kwargs)


def test_map_environment_and_suggestion_are_project_bounded(managed_project):
    _, _, access = managed_project

    mapped = _prepare(
        access,
        "map_environment",
        connection="local",
        path="folder",
        pattern="*.csv",
        limit=23,
    )
    suggested = _prepare(
        access,
        "suggest_pipeline",
        connection="local",
        source_path="folder/input.csv",
        intent="build a dimension",
    )

    assert mapped.kwargs == {
        "connection": "local",
        "path": "folder",
        "pattern": "*.csv",
        "limit": 23,
    }
    assert suggested.kwargs == {
        "connection": "local",
        "source_path": "folder/input.csv",
        "intent": "build a dimension",
    }


def test_map_environment_rejects_inline_connection_before_config_read(managed_project, monkeypatch):
    _, _, access = managed_project

    def unexpected_read(*args, **kwargs):
        pytest.fail("inline connections must deny before config read or outbound access")

    monkeypatch.setattr(Path, "open", unexpected_read)
    with pytest.raises(RuntimeAccessDenied, match="INVALID_RUNTIME_ARGUMENT"):
        _prepare(
            access,
            "map_environment",
            connection={"type": "http", "base_url": "http://private.invalid"},
        )


def test_runtime_result_sanitizes_errors_credentials_and_physical_paths(managed_project):
    root, config, _ = managed_project
    prepared = PreparedRuntimeCall(
        action="story_read",
        kwargs={},
        project_root=root,
        config_path=config,
    )

    result = sanitize_runtime_result(
        {
            "error": "generated failure at /sensitive/host/path",
            "errors": ["secret at C:/private/file"],
            "connection_string": "Server=private;Password=secret",
            "metadata": {"path": "/sensitive/host/path", "source": "abfss://private/data"},
            "detail": "generated at /sensitive/host/path",
        },
        prepared,
    )

    assert result == {
        "error": "Runtime data is unavailable.",
        "errors": ["Runtime data is unavailable."],
        "connection_string": "[redacted]",
        "metadata": {
            "path": "[physical reference withheld]",
            "source": "[physical reference withheld]",
        },
        "detail": "[physical reference withheld]",
        "policy_applied": {"project_scoped": True},
    }

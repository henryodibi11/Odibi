import json
import secrets
from pathlib import Path

import pytest
import yaml

from odibi_mcp.contracts.access import (
    AccessContext,
    ActionEffect,
    ApplicationIdentity,
    ConnectionPolicy,
    LogicalLineageEdge,
    LogicalLineageNode,
    ManagedProjectAccess,
    PreparedRuntimeCall,
    RemoteLogicalLineageProjection,
    RuntimeAccessDenied,
    authenticate_bearer_identity,
    render_remote_logical_lineage_projection,
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


def _configured_node(name, depends_on=None):
    return {
        "name": name,
        "depends_on": depends_on or [],
        "write": {"connection": "local", "format": "parquet", "path": f"out/{name}"},
    }


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

    assert prepared.action == "profile_folder"
    assert prepared.kwargs == {
        "connection": "local",
        "folder_path": "folder",
        "pattern": "*.csv",
        "max_files": 17,
    }
    assert prepared.project_root == root.resolve()
    assert prepared.config_path == config.resolve()
    assert prepared.validated_config_snapshot()["project"] == "managed"
    assert len(prepared.config_fingerprint) == 64


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


def test_remote_diagnose_denies_before_config_read(managed_project, monkeypatch):
    _, _, access = managed_project

    def unexpected_read(*args, **kwargs):
        pytest.fail("remote diagnose must deny before config or data read")

    monkeypatch.setattr(Path, "open", unexpected_read)
    with pytest.raises(RuntimeAccessDenied, match="PHYSICAL_REFERENCES_DISABLED"):
        _prepare(access, "diagnose")


def test_remote_lineage_prepares_and_renders_typed_inline_projection(managed_project):
    _, config_path, access = managed_project
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["pipelines"][0]["nodes"] = [
        _configured_node("source"),
        _configured_node("clean", ["source"]),
    ]
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    prepared = _prepare(access, "lineage_graph", pipeline="bounded")
    result = render_remote_logical_lineage_projection(prepared.logical_lineage)

    assert prepared.kwargs == {"pipeline": "bounded"}
    assert prepared.logical_lineage == RemoteLogicalLineageProjection(
        pipeline="bounded",
        nodes=(LogicalLineageNode("source"), LogicalLineageNode("clean")),
        edges=(LogicalLineageEdge("source", "clean"),),
    )
    assert result == {
        "kind": "logical_lineage_graph",
        "pipeline": "bounded",
        "status": "configured",
        "nodes": [
            {"id": "source", "type": "pipeline_node"},
            {"id": "clean", "type": "pipeline_node"},
        ],
        "edges": [{"source": "source", "target": "clean", "kind": "dependency"}],
        "counts": {
            "nodes_total": 2,
            "nodes_returned": 2,
            "edges_total": 1,
            "edges_returned": 1,
        },
        "truncated": False,
        "truncation": {"nodes": False, "edges": False},
        "policy_applied": {
            "project_scoped": True,
            "logical_only": True,
            "inline_snapshot_only": True,
            "node_limit": 64,
            "edge_limit": 128,
            "identifier_length_limit": 128,
            "response_byte_limit": 65536,
        },
    }
    assert len(json.dumps(result, indent=2).encode("utf-8")) <= 65536


def test_remote_lineage_rejects_imports_without_reading_them(managed_project, monkeypatch):
    root, config_path, access = managed_project
    imported = root / "imported.yaml"
    imported.write_text("sentinel: IMPORT_MUST_NOT_BE_READ", encoding="utf-8")
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["imports"] = ["imported.yaml"]
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    original_open = Path.open
    reads = []

    def config_only_open(path, *args, **kwargs):
        resolved = path.resolve()
        reads.append(resolved)
        if resolved != config_path.resolve():
            pytest.fail("remote logical lineage attempted to read an imported config")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", config_only_open)

    with pytest.raises(RuntimeAccessDenied, match="LOGICAL_PROJECTION_UNAVAILABLE"):
        _prepare(access, "lineage_graph", pipeline="bounded")

    assert reads == [config_path.resolve()]


@pytest.mark.parametrize(
    "nodes",
    [
        "not-a-list",
        ["not-a-mapping"],
        [{"name": "source", "depends_on": "not-a-list"}],
        [{"name": "source", "depends_on": [1]}],
        [{"name": "source", "depends_on": [{"name": "source"}]}],
        [{"name": "source", "depends_on": ["missing"]}],
        [{"name": "duplicate"}, {"name": "duplicate"}],
        [
            {"name": "source"},
            {"name": "target", "depends_on": ["source", "source"]},
        ],
    ],
)
def test_remote_lineage_rejects_malformed_or_ambiguous_graphs(managed_project, nodes):
    _, config_path, access = managed_project
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["pipelines"][0]["nodes"] = nodes
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with pytest.raises(RuntimeAccessDenied, match="LOGICAL_PROJECTION_UNAVAILABLE"):
        _prepare(access, "lineage_graph", pipeline="bounded")


@pytest.mark.parametrize("length", [1, 128])
def test_remote_lineage_identifier_length_boundaries(managed_project, length):
    _, config_path, access = managed_project
    identifier = "n" * length
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["pipelines"][0]["nodes"] = [_configured_node(identifier)]
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    projection = _prepare(access, "lineage_graph", pipeline="bounded").logical_lineage

    assert projection.nodes == (LogicalLineageNode(identifier),)


def test_remote_lineage_rejects_unreviewed_caller_arguments_before_config_read(
    managed_project, monkeypatch
):
    _, _, access = managed_project

    def unexpected_read(*args, **kwargs):
        pytest.fail("unreviewed lineage arguments reached managed config")

    monkeypatch.setattr(Path, "open", unexpected_read)
    with pytest.raises(RuntimeAccessDenied, match="INVALID_RUNTIME_ARGUMENT"):
        _prepare(
            access,
            "lineage_graph",
            pipeline="bounded",
            run_id="caller-runtime-selector",
            cwd="caller-root",
        )


def test_remote_lineage_rejects_identifier_and_source_scan_cap_plus_one(managed_project):
    _, config_path, access = managed_project
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["pipelines"][0]["nodes"] = [_configured_node("n" * 129)]
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with pytest.raises(RuntimeAccessDenied, match="LOGICAL_PROJECTION_UNAVAILABLE"):
        _prepare(access, "lineage_graph", pipeline="bounded")

    config["pipelines"][0]["nodes"] = [_configured_node(f"node_{index}") for index in range(257)]
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    with pytest.raises(RuntimeAccessDenied, match="LOGICAL_PROJECTION_UNAVAILABLE"):
        _prepare(access, "lineage_graph", pipeline="bounded")


def test_remote_lineage_pipeline_and_dependency_source_scan_bounds(managed_project):
    _, config_path, access = managed_project
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["pipelines"] = [{"pipeline": f"other_{index}", "nodes": []} for index in range(255)] + [
        {"pipeline": "bounded", "nodes": []}
    ]
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    assert _prepare(access, "lineage_graph", pipeline="bounded").logical_lineage is not None

    config["pipelines"].append({"pipeline": "one_too_many", "nodes": []})
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    with pytest.raises(RuntimeAccessDenied, match="LOGICAL_PROJECTION_UNAVAILABLE"):
        _prepare(access, "lineage_graph", pipeline="bounded")

    nodes = [_configured_node(f"node_{index}") for index in range(256)]
    for target in range(16):
        nodes[target]["depends_on"] = [f"node_{source}" for source in range(256)]
    config["pipelines"] = [{"pipeline": "bounded", "nodes": nodes}]
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    projection = _prepare(access, "lineage_graph", pipeline="bounded").logical_lineage
    assert projection is not None and len(projection.edges) == 4096

    nodes[16]["depends_on"] = ["node_0"]
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    with pytest.raises(RuntimeAccessDenied, match="LOGICAL_PROJECTION_UNAVAILABLE"):
        _prepare(access, "lineage_graph", pipeline="bounded")


def test_remote_lineage_node_output_min_max_and_cap_plus_one_are_truthful():
    for node_count, expected_returned, expected_truncated in (
        (0, 0, False),
        (1, 1, False),
        (64, 64, False),
        (65, 64, True),
    ):
        projection = RemoteLogicalLineageProjection(
            pipeline="bounded",
            nodes=tuple(LogicalLineageNode(f"node_{index}") for index in range(node_count)),
            edges=(),
        )

        result = render_remote_logical_lineage_projection(projection)

        assert result["counts"] == {
            "nodes_total": node_count,
            "nodes_returned": expected_returned,
            "edges_total": 0,
            "edges_returned": 0,
        }
        assert result["truncation"] == {"nodes": expected_truncated, "edges": False}
        assert result["truncated"] is expected_truncated


def test_remote_lineage_node_truncation_truthfully_marks_omitted_edges():
    projection = RemoteLogicalLineageProjection(
        pipeline="bounded",
        nodes=tuple(LogicalLineageNode(f"node_{index}") for index in range(65)),
        edges=(LogicalLineageEdge("node_0", "node_64"),),
    )

    result = render_remote_logical_lineage_projection(projection)

    assert result["counts"] == {
        "nodes_total": 65,
        "nodes_returned": 64,
        "edges_total": 1,
        "edges_returned": 0,
    }
    assert result["truncation"] == {"nodes": True, "edges": True}
    assert result["truncated"] is True


def test_remote_lineage_edge_output_min_max_and_cap_plus_one_are_truthful():
    nodes = tuple(LogicalLineageNode(f"node_{index}") for index in range(64))
    pairs = [
        (f"node_{source}", f"node_{target}") for target in range(64) for source in range(target)
    ]
    for edge_count, expected_truncated in ((0, False), (1, False), (128, False), (129, True)):
        projection = RemoteLogicalLineageProjection(
            pipeline="bounded",
            nodes=nodes,
            edges=tuple(LogicalLineageEdge(*pair) for pair in pairs[:edge_count]),
        )

        result = render_remote_logical_lineage_projection(projection)

        assert result["counts"]["edges_total"] == edge_count
        assert result["counts"]["edges_returned"] == min(edge_count, 128)
        assert result["truncation"]["edges"] is expected_truncated
        assert result["truncated"] is expected_truncated


def test_remote_lineage_maximum_serialized_response_stays_within_byte_cap():
    identifiers = tuple(f"n{index:03d}" + ("x" * 124) for index in range(64))
    pairs = [
        (identifiers[source], identifiers[target])
        for target in range(64)
        for source in range(target)
    ]
    projection = RemoteLogicalLineageProjection(
        pipeline="p" * 128,
        nodes=tuple(LogicalLineageNode(identifier) for identifier in identifiers),
        edges=tuple(LogicalLineageEdge(*pair) for pair in pairs[:128]),
    )

    result = render_remote_logical_lineage_projection(projection)
    serialized = json.dumps(result, indent=2).encode("utf-8")

    assert result["counts"] == {
        "nodes_total": 64,
        "nodes_returned": 64,
        "edges_total": 128,
        "edges_returned": 128,
    }
    assert result["truncated"] is False
    assert len(serialized) <= result["policy_applied"]["response_byte_limit"] == 65536


@pytest.mark.parametrize(
    "projection",
    [
        {"pipeline": "bounded"},
        RemoteLogicalLineageProjection("bounded", ("not-a-node",), ()),
        RemoteLogicalLineageProjection(
            "bounded", (LogicalLineageNode("source"),), (LogicalLineageEdge("source", "missing"),)
        ),
        RemoteLogicalLineageProjection(
            "bounded",
            (LogicalLineageNode("source"), LogicalLineageNode("target")),
            (LogicalLineageEdge("source", "target"), LogicalLineageEdge("source", "target")),
        ),
    ],
)
def test_remote_lineage_renderer_rejects_malformed_projection_shapes(projection):
    with pytest.raises(RuntimeAccessDenied, match="LOGICAL_PROJECTION_UNAVAILABLE"):
        render_remote_logical_lineage_projection(projection)


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


def test_prepared_runtime_config_fingerprint_rejects_snapshot_mutation(managed_project):
    _, _, access = managed_project
    prepared = _prepare(
        access,
        "profile_source",
        connection="local",
        path="folder/input.csv",
    )

    prepared.config_snapshot["project"] = "rotated-after-validation"

    with pytest.raises(RuntimeAccessDenied, match="PROJECT_SCOPE_REQUIRED"):
        prepared.validated_config_snapshot()


@pytest.mark.parametrize(
    "prefix",
    [
        None,
        "",
        "../other-project",
        "/other-project",
        "C:/other-project",
        "tenant//project",
        r"tenant\project",
        "tenant/./project",
        "tenant/%2e%2e/project",
        "tenant/project:stream",
        "tenant/\x00project",
    ],
)
def test_cloud_story_prefix_must_be_normalized_before_connection_access(managed_project, prefix):
    root, config_path, access = managed_project
    config = {
        "project": "managed",
        "connections": {
            "cloud": {
                "type": "azure_adls",
                "account": "managedaccount",
                "container": "stories",
                "path_prefix": prefix,
            }
        },
        "story": {"connection": "cloud", "path": "stories"},
        "system": {"connection": "cloud"},
        "pipelines": [{"pipeline": "bounded", "nodes": []}],
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    assert root.is_dir()

    with pytest.raises(RuntimeAccessDenied, match="PROJECT_SCOPE_REQUIRED"):
        _prepare(access, "story_read", pipeline="bounded")


def test_cloud_story_prefix_is_preserved_in_validated_snapshot(managed_project):
    _, config_path, access = managed_project
    config = {
        "project": "managed",
        "connections": {
            "cloud": {
                "type": "azure_adls",
                "account": "managedaccount",
                "container": "stories",
                "path_prefix": "tenant/managed",
            }
        },
        "story": {"connection": "cloud", "path": "stories"},
        "system": {"connection": "cloud"},
        "pipelines": [{"pipeline": "bounded", "nodes": []}],
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    prepared = _prepare(access, "story_read", pipeline="bounded")

    assert prepared.validated_config_snapshot()["connections"]["cloud"]["path_prefix"] == (
        "tenant/managed"
    )


def test_map_environment_sanitizer_caps_final_identifier_collections(managed_project):
    _, _, access = managed_project
    prepared = _prepare(access, "map_environment", connection="local", limit=1)

    result = sanitize_runtime_result(
        {
            "structure": [
                {"name": "schema_a", "sample_tables": ["a", "b"]},
                {"name": "schema_b", "sample_tables": ["c"]},
            ],
            "suggested_sources": ["schema_a.a", "schema_b.c"],
            "recommendations": ["inspect schema_a", "inspect schema_b"],
            "ready_for": ["schema_a.a", "schema_b.c"],
        },
        prepared,
    )

    assert len(result["structure"]) == 1
    assert result["structure"][0]["sample_tables"] == ["a"]
    assert result["suggested_sources"] == ["schema_a.a"]
    assert result["recommendations"] == ["inspect schema_a"]
    assert result["ready_for"] == ["schema_a.a"]
    assert result["truncated"] is True
    assert result["truncated_reason"] == "enumeration_limit"
    assert result["policy_applied"] == {
        "project_scoped": True,
        "enumeration_capped": True,
        "enumeration_limit": 1,
    }


def test_runtime_result_sanitizes_errors_credentials_and_physical_paths(managed_project):
    root, config, _ = managed_project
    prepared = PreparedRuntimeCall(
        action="story_read",
        kwargs={},
        project_root=root,
        config_path=config,
        config_snapshot={},
        config_fingerprint="",
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

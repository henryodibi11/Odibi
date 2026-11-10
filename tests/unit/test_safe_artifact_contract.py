"""Synthetic, offline regressions for public diagnostic-output contracts."""

import io
import json
import socket
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from deltalake import DeltaTable

from odibi.catalog import CatalogManager
from odibi.config import AlertConfig, DocsConfig, NodeConfig, PipelineConfig, SystemConfig
from odibi.connections.http import HttpConnection
from odibi.connections.local import LocalConnection
from odibi.context import create_context
from odibi.engine.pandas_engine import PandasEngine
from odibi.node import Node, NodeExecutor, NodeResult
from odibi.pipeline import Pipeline, PipelineResults
from odibi.state import LocalJSONStateBackend, StateManager
from odibi.story.doc_generator import DocGenerator
from odibi.story.generator import StoryGenerator
from odibi.story.metadata import NodeExecutionMetadata, PipelineStoryMetadata
from odibi.story.renderers import HTMLStoryRenderer, JSONStoryRenderer, MarkdownStoryRenderer
from odibi.utils.artifacts import safe_artifact, safe_identity


CANARY = "ODIBI_SYNTHETIC_ARTIFACT_SECRET_721"
BODY = "ODIBI_SYNTHETIC_OPAQUE_BODY_853"


@pytest.fixture(autouse=True)
def offline(monkeypatch, tmp_path):
    from odibi.utils import artifacts

    monkeypatch.setattr(artifacts, "_secrets", set())

    def deny(*args, **kwargs):
        pytest.fail("Contract tests must not make network requests")

    monkeypatch.setattr(socket.socket, "connect", deny)
    monkeypatch.setattr(socket, "create_connection", deny)
    monkeypatch.chdir(tmp_path)


def assert_safe(value):
    encoded = json.dumps(value, default=str)
    assert CANARY not in encoded
    assert BODY not in encoded


def api_config(name="api_node"):
    return NodeConfig(
        name=name,
        sensitive=True,
        read={
            "connection": "api",
            "format": "api",
            "path": f"items?sig={CANARY}",
            "options": {
                "method": "POST",
                "params": {"api_key": CANARY},
                "request_body": {"unclassified_field": BODY},
            },
        },
    )


def metadata():
    node = NodeExecutionMetadata(
        node_name="api_node",
        operation="read",
        status="failed",
        duration=0.1,
        rows_in=11,
        rows_out=7,
        error_message=f"Remote error: Bearer {CANARY}",
        config_snapshot=api_config().model_dump(mode="json"),
        previous_config_snapshot={"read": {"options": {"legacy_opaque": BODY}}},
        executed_sql=[f"SELECT id, '{BODY}' AS credential FROM records"],
    )
    return PipelineStoryMetadata(
        pipeline_name="audit",
        started_at="2026-01-01T00:00:00",
        completed_at="2026-01-01T00:00:01",
        duration=1.0,
        total_nodes=1,
        completed_nodes=0,
        failed_nodes=1,
        skipped_nodes=0,
        nodes=[node],
    )


def test_public_renderers_and_docs_reject_old_unsafe_inputs(tmp_path):
    original = metadata()
    for renderer in (HTMLStoryRenderer(), JSONStoryRenderer(), MarkdownStoryRenderer()):
        content = renderer.render(original)
        assert_safe(content)
        assert "api_node" in content
    captured = {}
    generator = DocGenerator(
        DocsConfig(enabled=True, output_path="memory://audit/docs"),
        "audit",
        write_file=lambda path, content: captured.update({path: content}),
    )
    assert generator.generate(original)
    assert captured
    assert_safe(captured)
    assert (
        original.nodes[0].config_snapshot["read"]["options"]["request_body"]["unclassified_field"]
        == BODY
    )
    assert CANARY in original.nodes[0].error_message


def test_success_and_retry_failure_preserve_requests_but_not_artifacts(monkeypatch, tmp_path):
    received = []

    class Response:
        status = 200
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b'[{"id":17,"amount":42}]'

    def transport(req, **kwargs):
        received.append(req)
        return Response()

    monkeypatch.setattr("odibi.connections.api_fetcher.urlopen", transport)
    conn = HttpConnection(
        "https://synthetic.invalid", headers={"X-API-Key": CANARY}, validate=False
    )
    cfg = api_config()
    before = cfg.model_dump(mode="json")
    pipeline = Pipeline(
        PipelineConfig(pipeline="audit", nodes=[cfg]),
        connections={"api": conn},
        story_config={
            "output_path": str(tmp_path / "stories"),
            "max_sample_rows": 0,
            "docs": {"enabled": True, "output_path": str(tmp_path / "docs")},
        },
    )
    succeeded = pipeline.run()
    assert succeeded.completed == ["api_node"]
    assert CANARY in received[0].full_url
    assert BODY in received[0].data.decode()
    assert CANARY in str(received[0].header_items())
    assert cfg.model_dump(mode="json") == before
    assert_safe(succeeded.node_results["api_node"].metadata)

    def fail(*args, **kwargs):
        raise RuntimeError("Synthetic credential echo " + CANARY)

    monkeypatch.setattr("odibi.connections.api_fetcher.urlopen", fail)
    failed = pipeline.run()
    result = failed.node_results["api_node"]
    assert not result.success
    assert "config_snapshot" not in result.metadata  # normal retry wrapper discards it
    assert result.metadata["retry_history"]
    assert_safe(result.metadata)
    assert_safe(failed.debug_summary())
    result.error = RuntimeError("x" * 190 + CANARY)
    summary = failed.debug_summary()
    assert CANARY[:10] not in summary  # sanitize before truncating the error
    direct = NodeExecutor(create_context("pandas"), PandasEngine(config={}), {"api": conn}).execute(
        cfg
    )
    assert not direct.success
    assert "config_snapshot" in direct.metadata
    assert_safe(direct.metadata)
    artifacts = [p for p in tmp_path.rglob("*") if p.suffix in {".html", ".json", ".md"}]
    assert artifacts
    assert_safe([p.read_text() for p in artifacts])


def test_historical_snapshot_reingestion_is_safe(tmp_path):
    history = tmp_path / "stories" / "audit" / "2000-01-01"
    history.mkdir(parents=True)
    old = metadata().to_dict()
    old["failed_nodes"] = 0
    old["nodes"][0]["status"] = "success"
    (history / "run_00-00-00.json").write_text(json.dumps(old))
    result = NodeResult(
        node_name="api_node",
        success=True,
        duration=1.0,
        metadata={"config_snapshot": {"name": "api_node"}},
    )
    path = StoryGenerator(
        "audit", output_path=str(tmp_path / "stories"), retention_days=36500
    ).generate(
        {"api_node": result},
        ["api_node"],
        [],
        [],
        1.0,
        "2026-01-01T00:00:00",
        "2026-01-01T00:00:01",
    )
    generated = json.loads(Path(path).with_suffix(".json").read_text())
    assert generated["nodes"][0]["previous_config_snapshot"]
    assert_safe(generated)
    assert BODY in (history / "run_00-00-00.json").read_text()  # no historical cleanup


@pytest.fixture
def catalog(tmp_path):
    cat = CatalogManager(
        spark=None,
        config=SystemConfig(connection="local", path="_system", environment="audit"),
        base_path=str(tmp_path / "_system"),
        engine=PandasEngine(config={}),
        connection=LocalConnection(base_path=str(tmp_path)),
    )
    cat.project = "audit"
    cat.bootstrap()
    return cat


def records(cat, table):
    return DeltaTable(cat.tables[table]).to_pyarrow_table().to_pylist()


def test_catalog_public_sinks_and_state_preserve_hashes(catalog, tmp_path):
    cfg = api_config()
    node = Node(cfg, create_context("pandas"), PandasEngine(config={}), {})
    original_hash = node.get_version_hash()
    assert catalog.register_node("audit", cfg)
    registered = records(catalog, "meta_nodes")[0]
    assert registered["version_hash"] == original_hash
    assert_safe(registered)
    catalog.register_pipelines_batch(
        [
            {
                "pipeline_name": "audit",
                "version_hash": original_hash,
                "description": "Bearer " + CANARY,
                "layer": "gold",
                "schedule": "",
                "tags_json": json.dumps(["api_key=" + BODY]),
            }
        ]
    )
    pipelines = records(catalog, "meta_pipelines")
    assert pipelines[0]["version_hash"] == original_hash
    assert_safe(pipelines)
    cfg.read.options["params"]["api_key"] = CANARY + "changed"
    assert node.get_version_hash() != original_hash  # redaction must not flatten functional hashes
    supplied = {
        "pipeline_name": "audit",
        "node_name": "batch",
        "type": "read",
        "version_hash": "exact_hash",
        "config_json": json.dumps(api_config().model_dump(mode="json")),
    }
    catalog.register_nodes_batch([supplied])
    assert CANARY in supplied["config_json"]
    assert_safe(records(catalog, "meta_nodes"))
    metrics = {
        "config_snapshot": api_config().model_dump(mode="json"),
        "retry_history": [{"error": CANARY}],
        "rows": 17,
        "version_hash": original_hash,
    }
    row = {
        "run_id": "audit",
        "pipeline_name": "audit",
        "node_name": "batch",
        "status": "FAILURE",
        "metrics_json": json.dumps(metrics),
    }
    catalog.log_runs_batch([row])
    catalog.log_run("single", "audit", "single", "FAILURE", metrics_json=row["metrics_json"])
    catalog.log_failure(
        "failure", "audit", "audit", "batch", "RuntimeError", "Bearer " + CANARY, "password=" + BODY
    )
    assert_safe(records(catalog, "meta_runs"))
    assert_safe(records(catalog, "meta_failures"))
    backend = LocalJSONStateBackend(str(tmp_path / "state.json"))
    backend.save_pipeline_run("direct", {"nodes": {"n": {"metadata": metrics}}})
    state = StateManager(backend=backend)
    state.save_pipeline_run(
        "audit",
        PipelineResults(
            pipeline_name="audit",
            node_results={
                "n": NodeResult(node_name="n", success=False, duration=0, metadata=metrics)
            },
        ),
    )
    assert_safe((tmp_path / "state.json").read_text())
    assert backend.get_last_run_info("direct", "n")["metadata"]["version_hash"] == original_hash


def test_output_identity_is_not_execution_resolution(catalog):
    path = "https://synthetic.invalid/table?part=west&sig=" + CANARY
    row = {
        "pipeline_name": "audit",
        "node_name": "writer",
        "output_type": "external_table",
        "connection_name": "api",
        "path": path,
        "format": "parquet",
        "table_name": "",
        "last_run": datetime.now(timezone.utc),
        "row_count": 17,
    }
    catalog.register_outputs_batch([row])
    assert catalog.get_node_output("audit", "writer")["path"] == path
    assert_safe(records(catalog, "meta_outputs"))
    assert row["path"] == path
    assert safe_identity("local?part=west&revision=2.csv") == "local?part=west&revision=2.csv"
    azure = "abfss://container@account.dfs.core.windows.net/output"
    assert safe_identity(azure) == azure
    assert "part=west" in safe_identity(path)
    assert_safe(safe_identity(f"https://{CANARY}@synthetic.invalid/resource"))


def test_local_success_provenance_and_failure_records(catalog, tmp_path):
    filename = "input.csv?access_token=" + CANARY
    (tmp_path / filename).write_text("id\n17\n")
    cfg = NodeConfig(
        name="local_node",
        read={"connection": "local", "format": "csv", "path": filename},
        write={
            "connection": "local",
            "format": "csv",
            "path": "out.csv",
            "mode": "overwrite",
            "add_metadata": True,
        },
    )
    local = LocalConnection(base_path=str(tmp_path))
    node = Node(
        cfg,
        create_context("pandas"),
        PandasEngine(config={}),
        {"local": local},
        catalog_manager=catalog,
        pipeline_name="audit",
    )
    result = node.execute()
    assert result.success
    assert pd.read_csv(tmp_path / "out.csv")["id"].tolist() == [17]
    assert_safe((tmp_path / "out.csv").read_text())
    assert_safe(result.metadata)
    assert_safe(records(catalog, "meta_lineage"))
    cfg.read.path = "missing.csv?access_token=" + CANARY
    failed = node.execute()
    assert not failed.success
    assert_safe(failed.metadata)
    assert_safe(records(catalog, "meta_failures"))


@pytest.mark.parametrize("module", ["odibi.utils.logging", "odibi.utils.logging_context"])
@pytest.mark.parametrize("structured", [True, False])
def test_both_loggers_nested_and_fallback_values(module, structured, monkeypatch):
    import importlib

    logger = importlib.import_module(module).StructuredLogger(structured=structured)
    logger.register_secret(CANARY)
    messages = []
    monkeypatch.setattr(logger.logger, "info", messages.append)
    buf = io.StringIO()
    original = {
        "nested": {"innocent": CANARY},
        "sequence": [CANARY, RuntimeError(CANARY)],
        "result": NodeResult(
            node_name="safe_node", success=False, duration=0.1, error=RuntimeError(CANARY)
        ),
        "rows": 17,
    }
    with redirect_stdout(buf):
        logger.info("audit " + CANARY, **original)
    rendered = buf.getvalue() if structured else messages
    assert_safe(rendered)
    assert "17" in str(rendered)
    assert original["nested"]["innocent"] == CANARY


@pytest.mark.parametrize("channel", ["webhook", "slack", "teams"])
def test_alert_public_payload_capture(channel, monkeypatch):
    from odibi.utils.alerting import send_alert

    captured = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def capture(req, **kwargs):
        captured.append(req)
        return Response()

    monkeypatch.setattr("urllib.request.urlopen", capture)
    cfg = AlertConfig(
        type=channel,
        url="https://synthetic.invalid/hook?sig=" + CANARY,
        metadata={"extra": {"client_secret": BODY}},
    )
    assert send_alert(
        cfg,
        "Bearer " + CANARY,
        {
            "pipeline": "audit",
            "status": "FAILURE",
            "node_details": [
                {"node": "node", "success": False, "duration": 0.1, "error": "api_key=" + BODY}
            ],
        },
        throttle=False,
    )
    assert captured[0].full_url == cfg.url
    assert_safe(json.loads(captured[0].data))


def test_sql_diagnostics_are_not_replayable_execution_inputs():
    sql = f"SELECT id, '{BODY}' AS credential FROM records WHERE kind = 'retail'"
    projected = safe_artifact({"sql": sql, "rows": 17, "version_hash": "unchanged"})
    assert_safe(projected)
    assert "SELECT id" in projected["sql"]
    assert projected["rows"] == 17
    assert projected["version_hash"] == "unchanged"
    assert BODY in sql


def test_optional_openlineage_captured_events(monkeypatch):
    import odibi.lineage as lineage

    # Local stand-ins for optional OpenLineage value objects. No client/service.
    for name in (
        "InputDataset",
        "OutputDataset",
        "ParentRunFacet",
        "NominalTimeRunFacet",
        "ProcessingEngineRunFacet",
        "SourceCodeJobFacet",
        "DocumentationJobFacet",
        "ErrorMessageRunFacet",
        "Run",
        "Job",
        "RunEvent",
    ):
        monkeypatch.setattr(lineage, name, lambda **kw: SimpleNamespace(**kw), raising=False)
    monkeypatch.setattr(
        lineage,
        "RunState",
        SimpleNamespace(START="START", COMPLETE="COMPLETE", FAIL="FAIL"),
        raising=False,
    )
    adapter = lineage.OpenLineageAdapter(config=None)
    events = []
    adapter.enabled = True
    adapter.namespace = "audit"
    adapter.pipeline_name = "audit"
    adapter.client = SimpleNamespace(emit=events.append)
    cfg = api_config()
    before = cfg.model_dump(mode="json")
    run_id = adapter.emit_node_start(cfg, "parent")
    adapter.emit_node_complete(
        cfg,
        NodeResult(
            node_name=cfg.name,
            success=False,
            duration=0.1,
            error=RuntimeError("Credential echo " + BODY),
        ),
        run_id,
    )
    pipeline = PipelineConfig(pipeline="audit", description="Bearer " + CANARY, nodes=[cfg])
    adapter.emit_pipeline_start(pipeline)
    adapter.emit_pipeline_complete(pipeline, SimpleNamespace(failed=[cfg.name]))
    assert len(events) == 4
    assert_safe([repr(event) for event in events])
    assert "api_node" in repr(events)
    assert cfg.model_dump(mode="json") == before


def test_semantic_execution_sql_and_remote_story_captures():
    from odibi.semantics.metrics import SemanticLayerConfig
    from odibi.semantics.story import SemanticStoryGenerator

    config = SemanticLayerConfig(
        metrics=[
            {
                "name": "total",
                "expr": f"SUM(CASE WHEN kind = '{BODY}' THEN amount ELSE 0 END)",
                "source": "gold.sales",
            }
        ],
        dimensions=[{"name": "region", "column": "region"}],
        views=[{"name": "vw_total", "metrics": ["total"], "dimensions": ["region"]}],
    )
    original = config.model_dump(mode="json")
    executed = []
    captured = {}
    generator = SemanticStoryGenerator(config, output_path="memory://audit/stories")
    result = generator.execute_with_story(
        executed.append,
        save_sql_to="memory://audit/sql",
        write_file=lambda p, c: captured.update({p: c}),
    )
    assert result.views_created == 1
    assert any(BODY in sql for sql in executed)
    assert_safe(result.to_dict())
    assert generator.save_story(write_file=lambda p, c: captured.update({p: c}))
    assert any(p.endswith(".sql") for p in captured)
    assert any(p.endswith(".json") for p in captured)
    assert any(p.endswith(".html") for p in captured)
    assert_safe(captured)
    assert config.model_dump(mode="json") == original


def test_explicit_and_automatic_manager_registration(tmp_path):
    from odibi.config import ProjectConfig
    from odibi.pipeline import PipelineManager

    cfg = api_config()
    cfg.read.connection = "local"
    project = ProjectConfig(
        project="audit",
        engine="pandas",
        connections={"local": {"type": "local", "base_path": str(tmp_path)}},
        system={"connection": "local", "path": "_system", "environment": "audit"},
        story={"path": "stories", "connection": "local"},
        pipelines=[PipelineConfig(pipeline="audit", nodes=[cfg])],
    )
    manager = PipelineManager(project, {"local": LocalConnection(base_path=str(tmp_path))})
    assert manager.deploy()
    first = records(manager.catalog_manager, "meta_nodes")[0]
    assert_safe(first)
    cfg.read.options["request_body"]["unclassified_field"] = BODY + "changed"
    manager.run(dry_run=True)
    current = records(manager.catalog_manager, "meta_nodes")[0]
    assert current["version_hash"] != first["version_hash"]
    assert_safe(current)


def test_fresh_catalog_requires_current_signed_locator_and_resolves_named_connection(catalog):
    from odibi.references import resolve_input_reference

    now = datetime.now(timezone.utc)
    signed = NodeConfig(
        name="signed",
        write={
            "connection": "api",
            "format": "parquet",
            "path": "https://synthetic.invalid/out?opaque=" + CANARY,
        },
    )
    pipeline = PipelineConfig(pipeline="audit", nodes=[signed])
    catalog.register_outputs_from_config(pipeline)
    assert resolve_input_reference("$audit.signed", catalog)["path"] == signed.write.path
    fresh = CatalogManager(
        spark=None,
        config=catalog.config,
        base_path=catalog.base_path,
        engine=catalog.engine,
        connection=catalog.connection,
    )
    with pytest.raises(ValueError, match="current credentials"):
        fresh.get_node_output("audit", "signed")
    fresh.register_outputs_from_config(pipeline)
    assert resolve_input_reference("$audit.signed", fresh)["path"] == signed.write.path
    catalog.register_outputs_batch(
        [
            {
                "pipeline_name": "audit",
                "node_name": "stable",
                "output_type": "external_table",
                "connection_name": "api",
                "path": "safe/output?part=west",
                "format": "parquet",
                "table_name": "",
                "last_run": now,
                "row_count": 0,
            }
        ]
    )
    clean = CatalogManager(
        spark=None,
        config=catalog.config,
        base_path=catalog.base_path,
        engine=catalog.engine,
        connection=catalog.connection,
    )
    resolved = resolve_input_reference("$audit.stable", clean)
    assert resolved == {"connection": "api", "path": "safe/output?part=west", "format": "parquet"}
    assert_safe(records(catalog, "meta_outputs"))


def test_pipeline_remote_payloads_are_safe(monkeypatch):
    import fsspec

    captured = {}

    class Sink(io.StringIO):
        def __init__(self, path):
            super().__init__()
            self.path = path

        def __exit__(self, *args):
            captured[self.path] = self.getvalue()
            return super().__exit__(*args)

    def local_open(path, mode, **kwargs):
        if mode == "w":
            return Sink(path)
        raise FileNotFoundError(path)

    monkeypatch.setattr(fsspec, "open", local_open)
    generator = StoryGenerator("audit", output_path="memory://audit/stories", retention_days=None)
    result = NodeResult(
        node_name="api_node",
        success=False,
        duration=0.1,
        metadata={"config_snapshot": api_config().model_dump(mode="json")},
        error=RuntimeError("Synthetic opaque credential echo " + BODY),
    )
    generator.generate(
        {"api_node": result},
        [],
        ["api_node"],
        [],
        0.1,
        "2026-01-01T00:00:00",
        "2026-01-01T00:00:01",
    )
    assert any(p.endswith(".json") for p in captured)
    assert any(p.endswith(".html") for p in captured)
    assert_safe(captured)


def test_opaque_request_body_has_no_safe_key_escape():
    value = {
        "read": {
            "options": {
                "method": "POST",
                "timeout": 17,
                "request_body": {"limit": 9341, "version_hash": BODY, "nested": {"method": CANARY}},
            }
        }
    }
    projected = safe_artifact(value, config=True)
    options = projected["read"]["options"]
    assert options["method"] == "POST"
    assert options["timeout"] == 17
    assert options["request_body"]["limit"] == "[REDACTED]"
    assert_safe(projected)
    assert value["read"]["options"]["request_body"]["limit"] == 9341


def test_transform_parameters_do_not_rename_diagnostic_fields_or_schema():
    projected = safe_artifact(
        {
            "config_snapshot": {
                "transform": {"steps": [{"params": {"columns": ["id", "amount"], "mode": "rows"}}]}
            },
            "rows": 17,
            "schema": ["id", "amount"],
        }
    )
    assert projected["rows"] == 17
    assert projected["schema"] == ["id", "amount"]
    assert projected["config_snapshot"]["transform"]["steps"][0]["params"]["mode"] == "[REDACTED]"


def test_asset_schema_and_semantic_catalog_sinks_preserve_runtime_locators(catalog):
    path = "https://synthetic.invalid/output?sig=" + CANARY
    catalog.register_asset("audit", "gold.direct", path, "parquet", "dimension")
    catalog.register_assets_batch(
        [
            {
                "project": "audit",
                "table_name": "gold.batch",
                "path": path,
                "format": "parquet",
                "pattern_type": "fact",
            }
        ]
    )
    assert catalog.resolve_table_path("gold.direct") == path
    assert catalog.resolve_table_path("gold.batch") == path
    assert_safe(records(catalog, "meta_tables"))
    first = catalog.track_schema(path, {"id": "int"}, "audit", "source", "run1")
    second = catalog.track_schema(path, {"id": "int"}, "audit", "source", "run2")
    assert first["changed"] is True
    assert second == {"changed": False, "version": 1}
    assert len(catalog.get_schema_history(path)) == 1
    assert_safe(records(catalog, "meta_schemas"))
    catalog.log_metrics("total", f"SUM(CASE WHEN label = '{BODY}' THEN amount END)", ["id"], path)
    saved = records(catalog, "meta_metrics")
    assert len(saved) == 1
    assert "SUM" in saved[0]["definition_sql"]
    assert_safe(saved)


@pytest.mark.parametrize("engine_name", ["pandas", "polars", "spark"])
def test_engine_provenance_is_diagnostic_not_authentication(engine_name, monkeypatch):
    from odibi.config import WriteMetadataConfig

    config = WriteMetadataConfig(extracted_at=False, source_connection=True, source_table=True)
    if engine_name == "pandas":
        engine = PandasEngine(config={})
        df = pd.DataFrame({"id": [17]})
    elif engine_name == "polars":
        pl = pytest.importorskip("polars")
        from odibi.engine.polars_engine import PolarsEngine

        engine = PolarsEngine(config={})
        df = pl.DataFrame({"id": [17]})
    else:
        import sys

        from odibi.engine.spark_engine import SparkEngine

        # Capture generated literal column payloads locally, without a JVM/service.
        class Frame(dict):
            def withColumn(self, name, value):
                return Frame({**self, name: value})

        functions = SimpleNamespace(lit=lambda value: value)
        monkeypatch.setitem(sys.modules, "pyspark", SimpleNamespace())
        monkeypatch.setitem(sys.modules, "pyspark.sql", SimpleNamespace(functions=functions))
        engine = SparkEngine.__new__(SparkEngine)
        df = Frame(id=[17])
    result = engine.add_write_metadata(
        df,
        config,
        source_connection="warehouse",
        source_path="https://synthetic.invalid/input?sig=" + CANARY,
        source_table="table?api_key=" + BODY,
        is_file_source=True,
    )
    if engine_name == "pandas":
        values = result.to_dict(orient="list")
    elif engine_name == "polars":
        values = result.to_dict(as_series=False)
    else:
        values = dict(result)
    assert_safe(values)
    assert values["id"] == [17]
    assert "warehouse" in str(values["_source_connection"])


@pytest.mark.parametrize(
    "encoded", ["SYNTHETIC%2fOPAQUE", "SYNTHETIC+OPAQUE", "SYNTHETIC%20OPAQUE"]
)
def test_identity_redacts_original_query_encoding_and_sibling_error(encoded):
    uri = "https://synthetic.invalid/input?opaque=" + encoded + "&part=west"
    projected = safe_artifact({"source_path": uri, "error": "Credential echo " + encoded})
    assert encoded not in json.dumps(projected)
    assert "opaque=[REDACTED]&part=west" in projected["source_path"]
    assert projected["error"] == "Credential echo [REDACTED]"
    assert safe_identity("local?opaque=" + encoded) == "local?opaque=" + encoded


def test_engine_options_do_not_register_schema_values_as_credentials():
    from odibi.utils.artifacts import register_config_secrets

    config = {
        "read": {
            "options": {
                "dtype": {"id": "int", "value": "string"},
                "custom_setting": "active",
                "request_body": [BODY],
                "params": {"opaque": CANARY},
            }
        }
    }
    register_config_secrets(config)
    projected = safe_artifact(
        {"schema": ["id", "value", "int", "string", "active"], "error": BODY + CANARY}
    )
    assert projected["schema"] == ["id", "value", "int", "string", "active"]
    assert_safe(projected)
    assert config["read"]["options"]["request_body"] == [BODY]


def test_typed_counters_and_operational_watermark_survive_projection(tmp_path, catalog):
    import numpy as np

    class Engine(PandasEngine):
        def count_rows(self, df):
            return np.int64(len(df))

    HttpConnection(base_url="https://synthetic.invalid", auth={"token": BODY}, validate=False)
    pd.DataFrame({"cursor": [BODY + "-a", BODY + "-b"], "id": [17, 29]}).to_csv(
        tmp_path / "input.csv", index=False
    )
    cfg = NodeConfig(
        name="watermark",
        sensitive=True,
        read={
            "connection": "local",
            "format": "csv",
            "path": "input.csv",
            "incremental": {"mode": "stateful", "column": "cursor"},
        },
    )
    node = Node(
        cfg,
        create_context("pandas"),
        Engine(config={}),
        {"local": LocalConnection(base_path=str(tmp_path))},
        catalog_manager=catalog,
    )
    result = node.execute()
    assert result.success
    assert result.rows_processed == 2
    assert result.metadata["rows"] == 2
    assert isinstance(result.metadata["rows"], int)
    assert node.state_manager.get_hwm("watermark_hwm") == BODY + "-b"
    assert_safe(result.metadata)
    assert_safe(result.model_dump(mode="python"))

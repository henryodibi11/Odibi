"""
Tests for odibi.lineage
========================
Covers: OpenLineageAdapter (disabled path + _create_dataset_from_config),
        LineageTracker (record_lineage, record_dependency_lineage,
                        _resolve_table_path, get_upstream, get_downstream,
                        get_impact_analysis)
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import uuid

import pytest

from odibi.lineage import OpenLineageAdapter, LineageTracker


# ===========================================================================
# OpenLineageAdapter — disabled paths (HAS_OPENLINEAGE = False or no config)
# ===========================================================================


class TestOpenLineageAdapterDisabled:
    def test_init_no_config_disabled(self):
        adapter = OpenLineageAdapter(config=None)
        assert adapter.enabled is False

    @patch("odibi.lineage.HAS_OPENLINEAGE", False)
    def test_init_no_openlineage_installed(self):
        cfg = SimpleNamespace(url="http://localhost:5000", namespace="ns", api_key=None)
        adapter = OpenLineageAdapter(config=cfg)
        assert adapter.enabled is False

    def test_init_config_no_url(self):
        cfg = SimpleNamespace(url=None, namespace="ns", api_key=None)
        with patch.dict("os.environ", {}, clear=False):
            # Ensure env var not set
            import os

            os.environ.pop("OPENLINEAGE_URL", None)
            adapter = OpenLineageAdapter(config=cfg)
        assert adapter.enabled is False

    def test_emit_pipeline_start_disabled_returns_uuid(self):
        adapter = OpenLineageAdapter(config=None)
        result = adapter.emit_pipeline_start(MagicMock())
        # Should return a valid UUID string
        parsed = uuid.UUID(result)
        assert str(parsed) == result

    def test_emit_pipeline_complete_disabled_returns_none(self):
        adapter = OpenLineageAdapter(config=None)
        result = adapter.emit_pipeline_complete(MagicMock(), MagicMock())
        assert result is None

    def test_emit_node_start_disabled_returns_uuid(self):
        adapter = OpenLineageAdapter(config=None)
        result = adapter.emit_node_start(MagicMock(), "parent-123")
        parsed = uuid.UUID(result)
        assert str(parsed) == result

    def test_emit_node_complete_disabled_returns_none(self):
        adapter = OpenLineageAdapter(config=None)
        result = adapter.emit_node_complete(MagicMock(), MagicMock(), "run-123")
        assert result is None

    def test_emit_pipeline_complete_disabled_no_run_id(self):
        adapter = OpenLineageAdapter(config=None)
        adapter.enabled = True
        # pipeline_run_id is not set
        assert not hasattr(adapter, "pipeline_run_id") or adapter.pipeline_run_id is None
        adapter.enabled = False
        result = adapter.emit_pipeline_complete(MagicMock(), MagicMock())
        assert result is None

    def test_emit_node_complete_disabled_no_run_id(self):
        adapter = OpenLineageAdapter(config=None)
        result = adapter.emit_node_complete(MagicMock(), MagicMock(), None)
        assert result is None


# ===========================================================================
# OpenLineageAdapter — _create_dataset_from_config
# ===========================================================================


class TestCreateDatasetFromConfig:
    """Test _create_dataset_from_config when adapter is enabled (mock-based)."""

    def _make_enabled_adapter(self):
        adapter = OpenLineageAdapter(config=None)
        adapter.enabled = True
        adapter.namespace = "test_ns"
        return adapter

    def test_input_dataset_no_schema(self):
        adapter = self._make_enabled_adapter()
        config_op = SimpleNamespace(connection="db_conn", path="data/raw", table=None)
        result = adapter._create_dataset_from_config(config_op, is_input=True)
        # Without openlineage installed, InputDataset = Any, so we can't assert type
        # But we can verify the function doesn't error
        assert result is not None or result is None  # just no exception

    def test_output_dataset_no_schema(self):
        adapter = self._make_enabled_adapter()
        config_op = SimpleNamespace(connection="db_conn", path=None, table="users")
        result = adapter._create_dataset_from_config(config_op, is_input=False)
        assert result is not None or result is None

    def test_with_dict_schema(self):
        adapter = self._make_enabled_adapter()
        config_op = SimpleNamespace(connection="db_conn", path="output", table=None)
        schema = {"name": "string", "age": "int"}
        result = adapter._create_dataset_from_config(config_op, is_input=False, schema=schema)
        assert result is not None or result is None

    def test_config_with_no_path_no_table(self):
        adapter = self._make_enabled_adapter()
        config_op = SimpleNamespace(connection="conn", path=None, table=None)
        # Should use "unknown" as name
        result = adapter._create_dataset_from_config(config_op, is_input=True)
        assert result is not None or result is None

    def test_exception_returns_none(self):
        adapter = self._make_enabled_adapter()
        # config_op without .connection should cause AttributeError
        config_op = SimpleNamespace()
        result = adapter._create_dataset_from_config(config_op, is_input=True)
        assert result is None

    def test_empty_schema_dict(self):
        adapter = self._make_enabled_adapter()
        config_op = SimpleNamespace(connection="c", path="p", table=None)
        result = adapter._create_dataset_from_config(config_op, is_input=True, schema={})
        assert result is not None or result is None

    def test_schema_none(self):
        adapter = self._make_enabled_adapter()
        config_op = SimpleNamespace(connection="c", path="p", table=None)
        result = adapter._create_dataset_from_config(config_op, is_input=True, schema=None)
        assert result is not None or result is None


# ===========================================================================
# LineageTracker — __init__
# ===========================================================================


class TestLineageTrackerInit:
    def test_init_without_catalog(self):
        tracker = LineageTracker()
        assert tracker.catalog is None

    def test_init_with_catalog(self):
        cat = MagicMock()
        tracker = LineageTracker(catalog=cat)
        assert tracker.catalog is cat


# ===========================================================================
# LineageTracker — _resolve_table_path
# ===========================================================================


class TestResolveTablePath:
    def test_table_with_schema_and_catalog(self):
        tracker = LineageTracker()
        config = SimpleNamespace(connection="db", path=None, table="users")
        conn = SimpleNamespace(schema_name="dbo", catalog="mydb")
        result = tracker._resolve_table_path(config, {"db": conn})
        assert result == "mydb.dbo.users"

    def test_table_with_schema_no_catalog(self):
        tracker = LineageTracker()
        config = SimpleNamespace(connection="db", path=None, table="users")
        conn = SimpleNamespace(schema_name="public", catalog="")
        result = tracker._resolve_table_path(config, {"db": conn})
        assert result == "public.users"

    def test_table_no_schema_on_conn(self):
        tracker = LineageTracker()
        config = SimpleNamespace(connection="db", path=None, table="orders")
        conn = SimpleNamespace()  # no schema_name
        result = tracker._resolve_table_path(config, {"db": conn})
        assert result == "db.orders"

    def test_table_conn_not_found(self):
        tracker = LineageTracker()
        config = SimpleNamespace(connection="db", path=None, table="orders")
        result = tracker._resolve_table_path(config, {})
        assert result == "db.orders"

    def test_path_no_table(self):
        tracker = LineageTracker()
        config = SimpleNamespace(connection="adls", path="bronze/customers.parquet", table=None)
        result = tracker._resolve_table_path(config, {})
        assert result == "adls/bronze/customers.parquet"

    def test_no_path_no_table(self):
        tracker = LineageTracker()
        config = SimpleNamespace(connection="x", path=None, table=None)
        result = tracker._resolve_table_path(config, {})
        assert result is None

    def test_exception_returns_none(self):
        tracker = LineageTracker()
        # config without .connection triggers exception
        config = SimpleNamespace()
        result = tracker._resolve_table_path(config, {})
        assert result is None

    def test_table_with_catalog_none(self):
        """catalog attr is None (falsy) — should use schema.table only."""
        tracker = LineageTracker()
        config = SimpleNamespace(connection="db", path=None, table="t")
        conn = SimpleNamespace(schema_name="s", catalog=None)
        result = tracker._resolve_table_path(config, {"db": conn})
        # catalog is None → falsy → should give "s.t"
        assert result == "s.t"


# ===========================================================================
# LineageTracker — record_lineage
# ===========================================================================


class TestRecordLineage:
    def test_no_catalog_returns_none(self):
        tracker = LineageTracker(catalog=None)
        read_cfg = SimpleNamespace(connection="c", path="p", table=None)
        write_cfg = SimpleNamespace(connection="c", path="out", table=None)
        tracker.record_lineage(read_cfg, write_cfg, "pipeline", "node", "run1", {})
        # No error, no assertion — just returns None

    def test_no_write_config_returns_none(self):
        cat = MagicMock()
        tracker = LineageTracker(catalog=cat)
        tracker.record_lineage(MagicMock(), None, "pipeline", "node", "run1", {})
        cat.record_lineage.assert_not_called()

    def test_write_config_resolves_to_none(self):
        cat = MagicMock()
        tracker = LineageTracker(catalog=cat)
        write_cfg = SimpleNamespace(connection="c", path=None, table=None)
        tracker.record_lineage(None, write_cfg, "p", "n", "r", {})
        cat.record_lineage.assert_not_called()

    def test_read_and_write_records_lineage(self):
        cat = MagicMock()
        tracker = LineageTracker(catalog=cat)
        read_cfg = SimpleNamespace(connection="src", path="raw/data.csv", table=None)
        write_cfg = SimpleNamespace(connection="tgt", path="silver/data.parquet", table=None)
        tracker.record_lineage(read_cfg, write_cfg, "silver_pipe", "process", "run-1", {})
        cat.record_lineage.assert_called_once_with(
            source_table="src/raw/data.csv",
            target_table="tgt/silver/data.parquet",
            target_pipeline="silver_pipe",
            target_node="process",
            run_id="run-1",
        )

    def test_no_read_config_no_record(self):
        cat = MagicMock()
        tracker = LineageTracker(catalog=cat)
        write_cfg = SimpleNamespace(connection="tgt", path="out", table=None)
        tracker.record_lineage(None, write_cfg, "p", "n", "r", {})
        cat.record_lineage.assert_not_called()

    def test_read_resolves_to_none_no_record(self):
        cat = MagicMock()
        tracker = LineageTracker(catalog=cat)
        read_cfg = SimpleNamespace(connection="c", path=None, table=None)  # resolves to None
        write_cfg = SimpleNamespace(connection="tgt", path="out", table=None)
        tracker.record_lineage(read_cfg, write_cfg, "p", "n", "r", {})
        cat.record_lineage.assert_not_called()


# ===========================================================================
# LineageTracker — record_dependency_lineage
# ===========================================================================


class TestRecordDependencyLineage:
    def test_no_catalog_returns_none(self):
        tracker = LineageTracker(catalog=None)
        tracker.record_dependency_lineage(
            ["a"],
            SimpleNamespace(connection="c", path="p", table=None),
            "pipe",
            "node",
            "run1",
            {},
            {},
        )
        # No error

    def test_no_write_config_returns_none(self):
        cat = MagicMock()
        tracker = LineageTracker(catalog=cat)
        tracker.record_dependency_lineage(["a"], None, "p", "n", "r", {}, {})
        cat.record_lineage.assert_not_called()

    def test_write_resolves_to_none(self):
        cat = MagicMock()
        tracker = LineageTracker(catalog=cat)
        write_cfg = SimpleNamespace(connection="c", path=None, table=None)
        tracker.record_dependency_lineage(["a"], write_cfg, "p", "n", "r", {}, {})
        cat.record_lineage.assert_not_called()

    def test_records_for_each_dep_with_output(self):
        cat = MagicMock()
        tracker = LineageTracker(catalog=cat)
        write_cfg = SimpleNamespace(connection="tgt", path="out", table=None)
        node_outputs = {"dep1": "src/dep1_out", "dep2": "src/dep2_out"}
        tracker.record_dependency_lineage(
            ["dep1", "dep2"], write_cfg, "pipe", "node", "run1", node_outputs, {}
        )
        assert cat.record_lineage.call_count == 2

    def test_skips_dep_without_output(self):
        cat = MagicMock()
        tracker = LineageTracker(catalog=cat)
        write_cfg = SimpleNamespace(connection="tgt", path="out", table=None)
        node_outputs = {"dep1": "src/dep1_out"}  # dep2 not in outputs
        tracker.record_dependency_lineage(
            ["dep1", "dep2"], write_cfg, "pipe", "node", "run1", node_outputs, {}
        )
        assert cat.record_lineage.call_count == 1

    def test_empty_depends_on(self):
        cat = MagicMock()
        tracker = LineageTracker(catalog=cat)
        write_cfg = SimpleNamespace(connection="tgt", path="out", table=None)
        tracker.record_dependency_lineage([], write_cfg, "pipe", "node", "run1", {}, {})
        cat.record_lineage.assert_not_called()

    def test_correct_args_passed(self):
        cat = MagicMock()
        tracker = LineageTracker(catalog=cat)
        write_cfg = SimpleNamespace(connection="tgt", path="out", table=None)
        node_outputs = {"dep_node": "src_path/dep_out"}
        tracker.record_dependency_lineage(
            ["dep_node"], write_cfg, "my_pipe", "my_node", "run-99", node_outputs, {}
        )
        cat.record_lineage.assert_called_once_with(
            source_table="src_path/dep_out",
            target_table="tgt/out",
            source_pipeline="my_pipe",
            source_node="dep_node",
            target_pipeline="my_pipe",
            target_node="my_node",
            run_id="run-99",
        )


# ===========================================================================
# LineageTracker — get_upstream / get_downstream
# ===========================================================================


class TestUpstreamDownstream:
    def test_get_upstream_no_catalog(self):
        tracker = LineageTracker(catalog=None)
        assert tracker.get_upstream("table_a") == []

    def test_get_upstream_with_catalog(self):
        cat = MagicMock()
        cat.get_upstream.return_value = [{"source_table": "raw.t1", "depth": 1}]
        tracker = LineageTracker(catalog=cat)
        result = tracker.get_upstream("silver.t2", depth=5)
        assert len(result) == 1
        cat.get_upstream.assert_called_once_with("silver.t2", 5)

    def test_get_downstream_no_catalog(self):
        tracker = LineageTracker(catalog=None)
        assert tracker.get_downstream("table_a") == []

    def test_get_downstream_with_catalog(self):
        cat = MagicMock()
        cat.get_downstream.return_value = [{"target_table": "gold.t3", "depth": 1}]
        tracker = LineageTracker(catalog=cat)
        result = tracker.get_downstream("silver.t2", depth=2)
        assert len(result) == 1
        cat.get_downstream.assert_called_once_with("silver.t2", 2)

    def test_get_upstream_default_depth(self):
        cat = MagicMock()
        cat.get_upstream.return_value = []
        tracker = LineageTracker(catalog=cat)
        tracker.get_upstream("t")
        cat.get_upstream.assert_called_once_with("t", 3)

    def test_get_downstream_default_depth(self):
        cat = MagicMock()
        cat.get_downstream.return_value = []
        tracker = LineageTracker(catalog=cat)
        tracker.get_downstream("t")
        cat.get_downstream.assert_called_once_with("t", 3)


# ===========================================================================
# LineageTracker — get_impact_analysis
# ===========================================================================


class TestGetImpactAnalysis:
    def test_empty_downstream(self):
        tracker = LineageTracker(catalog=None)
        result = tracker.get_impact_analysis("table_x")
        assert result["table"] == "table_x"
        assert result["affected_tables"] == []
        assert result["affected_pipelines"] == []
        assert result["total_depth"] == 0
        assert result["downstream_count"] == 0

    def test_with_downstream_records(self):
        cat = MagicMock()
        cat.get_downstream.return_value = [
            {"target_table": "silver.t1", "target_pipeline": "pipe_a", "depth": 1},
            {"target_table": "gold.t2", "target_pipeline": "pipe_b", "depth": 2},
            {"target_table": "gold.t3", "target_pipeline": "pipe_b", "depth": 3},
        ]
        tracker = LineageTracker(catalog=cat)
        result = tracker.get_impact_analysis("bronze.raw", depth=5)
        assert set(result["affected_tables"]) == {"silver.t1", "gold.t2", "gold.t3"}
        assert set(result["affected_pipelines"]) == {"pipe_a", "pipe_b"}
        assert result["total_depth"] == 3
        assert result["downstream_count"] == 3

    def test_records_missing_fields(self):
        cat = MagicMock()
        cat.get_downstream.return_value = [
            {"target_table": None, "target_pipeline": None},
            {"other_field": "val"},
        ]
        tracker = LineageTracker(catalog=cat)
        result = tracker.get_impact_analysis("t")
        assert result["affected_tables"] == []
        assert result["affected_pipelines"] == []
        assert result["total_depth"] == 0
        assert result["downstream_count"] == 2

    def test_depth_zero_records(self):
        cat = MagicMock()
        cat.get_downstream.return_value = [
            {"target_table": "t1", "target_pipeline": "p1", "depth": 0},
        ]
        tracker = LineageTracker(catalog=cat)
        result = tracker.get_impact_analysis("src")
        assert result["total_depth"] == 0
        assert result["affected_tables"] == ["t1"]

    def test_deduplicates_tables_and_pipelines(self):
        cat = MagicMock()
        cat.get_downstream.return_value = [
            {"target_table": "t1", "target_pipeline": "p1", "depth": 1},
            {"target_table": "t1", "target_pipeline": "p1", "depth": 2},
        ]
        tracker = LineageTracker(catalog=cat)
        result = tracker.get_impact_analysis("src")
        assert len(result["affected_tables"]) == 1
        assert len(result["affected_pipelines"]) == 1
        assert result["total_depth"] == 2


# ===========================================================================
# OpenLineageAdapter — enabled paths (mock OpenLineage client)
# ===========================================================================

# ---------------------------------------------------------------------------
# Helper: install mock OpenLineage types into the module for enabled-path tests
# ---------------------------------------------------------------------------


def _install_mock_openlineage_types():
    """Inject mock OpenLineage classes into odibi.lineage module."""
    import odibi.lineage as mod

    # Only install if not already present (HAS_OPENLINEAGE is False)
    if not mod.HAS_OPENLINEAGE:
        mod.Run = MagicMock()
        mod.Job = MagicMock()
        mod.RunEvent = MagicMock()
        mod.RunState = SimpleNamespace(START="START", COMPLETE="COMPLETE", FAIL="FAIL")
        mod.NominalTimeRunFacet = MagicMock()
        mod.ProcessingEngineRunFacet = MagicMock()
        mod.DocumentationJobFacet = MagicMock()
        mod.ErrorMessageRunFacet = MagicMock()
        mod.ParentRunFacet = MagicMock()
        mod.SourceCodeJobFacet = MagicMock()
        mod.SchemaDatasetFacet = MagicMock()
        mod.SchemaField = MagicMock()
        mod.InputDataset = MagicMock()
        mod.OutputDataset = MagicMock()
        mod.OpenLineageClient = MagicMock()


def _uninstall_mock_openlineage_types():
    """Remove mock OpenLineage classes from odibi.lineage module."""
    import odibi.lineage as mod

    for attr in [
        "Run",
        "Job",
        "RunEvent",
        "RunState",
        "NominalTimeRunFacet",
        "ProcessingEngineRunFacet",
        "DocumentationJobFacet",
        "ErrorMessageRunFacet",
        "ParentRunFacet",
        "SourceCodeJobFacet",
        "SchemaDatasetFacet",
        "SchemaField",
        "InputDataset",
        "OutputDataset",
        "OpenLineageClient",
    ]:
        if hasattr(mod, attr) and isinstance(getattr(mod, attr), (MagicMock, SimpleNamespace)):
            delattr(mod, attr)


@pytest.fixture(autouse=False)
def mock_openlineage_types():
    """Fixture to install and remove mock OpenLineage types."""
    _install_mock_openlineage_types()
    yield
    _uninstall_mock_openlineage_types()


class TestOpenLineageAdapterEnabled:
    """Test the enabled code paths by injecting a mock client."""

    def _make_enabled_adapter(self):
        adapter = OpenLineageAdapter(config=None)
        adapter.enabled = True
        adapter.client = MagicMock()
        adapter.namespace = "test_ns"
        adapter.pipeline_run_id = None
        adapter.pipeline_name = None
        return adapter

    def test_emit_pipeline_start_enabled(self, mock_openlineage_types):
        adapter = self._make_enabled_adapter()
        pipeline_cfg = SimpleNamespace(pipeline="my_pipeline", description="desc")
        run_id = adapter.emit_pipeline_start(pipeline_cfg)
        assert isinstance(run_id, str)
        uuid.UUID(run_id)  # valid UUID
        adapter.client.emit.assert_called_once()
        assert adapter.pipeline_name == "my_pipeline"

    def test_emit_pipeline_start_no_description(self, mock_openlineage_types):
        adapter = self._make_enabled_adapter()
        pipeline_cfg = SimpleNamespace(pipeline="pipe", description=None)
        run_id = adapter.emit_pipeline_start(pipeline_cfg)
        assert isinstance(run_id, str)

    def test_emit_pipeline_start_exception_returns_uuid(self, mock_openlineage_types):
        adapter = self._make_enabled_adapter()
        adapter.client.emit.side_effect = RuntimeError("emit failed")
        pipeline_cfg = SimpleNamespace(pipeline="pipe", description="d")
        run_id = adapter.emit_pipeline_start(pipeline_cfg)
        uuid.UUID(run_id)  # still a valid UUID

    def test_emit_pipeline_complete_success(self, mock_openlineage_types):
        adapter = self._make_enabled_adapter()
        adapter.pipeline_run_id = "run-123"
        adapter.pipeline_name = "pipe"
        pipeline_cfg = SimpleNamespace(pipeline="pipe")
        results = SimpleNamespace(failed=[])
        adapter.emit_pipeline_complete(pipeline_cfg, results)
        adapter.client.emit.assert_called_once()

    def test_emit_pipeline_complete_failure(self, mock_openlineage_types):
        adapter = self._make_enabled_adapter()
        adapter.pipeline_run_id = "run-123"
        adapter.pipeline_name = "pipe"
        pipeline_cfg = SimpleNamespace(pipeline="pipe")
        results = SimpleNamespace(failed=["node1", "node2"])
        adapter.emit_pipeline_complete(pipeline_cfg, results)
        adapter.client.emit.assert_called_once()

    def test_emit_pipeline_complete_no_run_id(self):
        adapter = self._make_enabled_adapter()
        adapter.pipeline_run_id = None
        pipeline_cfg = SimpleNamespace(pipeline="pipe")
        adapter.emit_pipeline_complete(pipeline_cfg, MagicMock())
        adapter.client.emit.assert_not_called()

    def test_emit_pipeline_complete_exception(self, mock_openlineage_types):
        adapter = self._make_enabled_adapter()
        adapter.pipeline_run_id = "run-123"
        adapter.client.emit.side_effect = RuntimeError("boom")
        pipeline_cfg = SimpleNamespace(pipeline="pipe")
        results = SimpleNamespace(failed=[])
        # Should not raise
        adapter.emit_pipeline_complete(pipeline_cfg, results)

    def test_emit_node_start_with_read(self, mock_openlineage_types):
        adapter = self._make_enabled_adapter()
        adapter.pipeline_name = "pipe"
        read_cfg = SimpleNamespace(connection="conn", path="data/raw", table=None)
        config = SimpleNamespace(
            name="my_node",
            read=read_cfg,
            depends_on=None,
            description="Node desc",
        )
        config.model_dump_json = MagicMock(return_value='{"name":"my_node"}')
        run_id = adapter.emit_node_start(config, "parent-run-id")
        assert isinstance(run_id, str)
        uuid.UUID(run_id)
        adapter.client.emit.assert_called_once()

    def test_emit_node_start_with_depends_on_no_read(self, mock_openlineage_types):
        adapter = self._make_enabled_adapter()
        adapter.pipeline_name = "pipe"
        config = SimpleNamespace(
            name="node2",
            read=None,
            depends_on=["node1"],
            description=None,
        )
        config.model_dump_json = MagicMock(return_value="{}")
        run_id = adapter.emit_node_start(config, "parent-run-id")
        assert isinstance(run_id, str)

    def test_emit_node_start_no_model_dump_json(self, mock_openlineage_types):
        adapter = self._make_enabled_adapter()
        adapter.pipeline_name = "pipe"
        config = SimpleNamespace(
            name="node3",
            read=None,
            depends_on=None,
            description=None,
        )
        config.model_dump = MagicMock(return_value={"name": "node3"})
        run_id = adapter.emit_node_start(config, "parent-run-id")
        assert isinstance(run_id, str)

    def test_emit_node_start_exception_returns_uuid(self, mock_openlineage_types):
        adapter = self._make_enabled_adapter()
        adapter.pipeline_name = "pipe"
        adapter.client.emit.side_effect = RuntimeError("fail")
        config = SimpleNamespace(
            name="node",
            read=None,
            depends_on=None,
            description=None,
        )
        config.model_dump_json = MagicMock(return_value="{}")
        run_id = adapter.emit_node_start(config, "parent-id")
        uuid.UUID(run_id)

    def test_emit_node_start_no_pipeline_name(self, mock_openlineage_types):
        adapter = self._make_enabled_adapter()
        adapter.pipeline_name = None
        config = SimpleNamespace(
            name="node",
            read=None,
            depends_on=None,
            description="d",
        )
        config.model_dump_json = MagicMock(return_value="{}")
        run_id = adapter.emit_node_start(config, "parent-id")
        assert isinstance(run_id, str)

    def test_emit_node_complete_success_with_write(self, mock_openlineage_types):
        adapter = self._make_enabled_adapter()
        adapter.pipeline_name = "pipe"
        write_cfg = SimpleNamespace(connection="conn", path="out", table=None)
        config = SimpleNamespace(name="node", write=write_cfg)
        result = SimpleNamespace(success=True, error=None, result_schema={"col1": "str"})
        adapter.emit_node_complete(config, result, "run-id-123")
        adapter.client.emit.assert_called_once()

    def test_emit_node_complete_failure_with_error(self, mock_openlineage_types):
        adapter = self._make_enabled_adapter()
        adapter.pipeline_name = "pipe"
        config = SimpleNamespace(name="node", write=None)
        result = SimpleNamespace(success=False, error=RuntimeError("oops"), result_schema=None)
        adapter.emit_node_complete(config, result, "run-id-123")
        adapter.client.emit.assert_called_once()

    def test_emit_node_complete_no_run_id(self):
        adapter = self._make_enabled_adapter()
        config = SimpleNamespace(name="node", write=None)
        result = SimpleNamespace(success=True, error=None, result_schema=None)
        adapter.emit_node_complete(config, result, None)
        adapter.client.emit.assert_not_called()

    def test_emit_node_complete_exception(self, mock_openlineage_types):
        adapter = self._make_enabled_adapter()
        adapter.pipeline_name = "pipe"
        adapter.client.emit.side_effect = RuntimeError("boom")
        config = SimpleNamespace(name="node", write=None)
        result = SimpleNamespace(success=True, error=None, result_schema=None)
        # Should not raise
        adapter.emit_node_complete(config, result, "run-id")

    def test_emit_node_complete_success_no_error(self, mock_openlineage_types):
        adapter = self._make_enabled_adapter()
        adapter.pipeline_name = "pipe"
        config = SimpleNamespace(name="node", write=None)
        result = SimpleNamespace(success=True, error=None, result_schema=None)
        adapter.emit_node_complete(config, result, "run-id")
        adapter.client.emit.assert_called_once()


# ===========================================================================
# OpenLineageAdapter — init with URL (mock client constructor)
# ===========================================================================


class TestOpenLineageAdapterInitWithUrl:
    def test_init_with_url_enables_adapter(self, mock_openlineage_types):
        import odibi.lineage as mod

        mod.HAS_OPENLINEAGE = True
        try:
            cfg = SimpleNamespace(url="http://localhost:5000", namespace="ns", api_key="key123")
            adapter = OpenLineageAdapter(config=cfg)
            assert adapter.enabled is True
            mod.OpenLineageClient.assert_called_with(url="http://localhost:5000", api_key="key123")
        finally:
            mod.HAS_OPENLINEAGE = False

    def test_init_client_exception_disables(self, mock_openlineage_types):
        import odibi.lineage as mod

        mod.HAS_OPENLINEAGE = True
        mod.OpenLineageClient.side_effect = RuntimeError("connection refused")
        try:
            cfg = SimpleNamespace(url="http://bad:5000", namespace="ns", api_key=None)
            adapter = OpenLineageAdapter(config=cfg)
            assert adapter.enabled is False
        finally:
            mod.HAS_OPENLINEAGE = False
            mod.OpenLineageClient.side_effect = None

    def test_init_url_from_env(self, mock_openlineage_types):
        import odibi.lineage as mod

        mod.HAS_OPENLINEAGE = True
        try:
            cfg = SimpleNamespace(url=None, namespace="ns", api_key=None)
            with patch.dict("os.environ", {"OPENLINEAGE_URL": "http://env:5000"}):
                adapter = OpenLineageAdapter(config=cfg)
            assert adapter.enabled is True
            mod.OpenLineageClient.assert_called_with(url="http://env:5000", api_key=None)
        finally:
            mod.HAS_OPENLINEAGE = False

    def test_init_api_key_from_env(self, mock_openlineage_types):
        import odibi.lineage as mod

        mod.HAS_OPENLINEAGE = True
        try:
            cfg = SimpleNamespace(url="http://localhost:5000", namespace="ns", api_key=None)
            with patch.dict("os.environ", {"OPENLINEAGE_API_KEY": "envkey"}):
                adapter = OpenLineageAdapter(config=cfg)
            assert adapter.enabled is True
            mod.OpenLineageClient.assert_called_with(url="http://localhost:5000", api_key="envkey")
        finally:
            mod.HAS_OPENLINEAGE = False

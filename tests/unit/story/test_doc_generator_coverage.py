"""Coverage tests for odibi/story/doc_generator.py — rendering helpers & edge cases."""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch


from odibi.config import DocsConfig
from odibi.story.doc_generator import DocGenerator
from odibi.story.metadata import DeltaWriteInfo, NodeExecutionMetadata, PipelineStoryMetadata


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Cap:
    """Captures write_file calls."""

    def __init__(self):
        self.files: dict = {}

    def __call__(self, path, content):
        self.files[path] = content


def _cfg(enabled=True, output="docs/gen/"):
    return DocsConfig(enabled=enabled, output_path=output)


def _node(name="n1", op="read", status="success", dur=1.0, rows_in=100, rows_out=100, **kw):
    return NodeExecutionMetadata(
        node_name=name,
        operation=op,
        status=status,
        duration=dur,
        rows_in=rows_in,
        rows_out=rows_out,
        **kw,
    )


def _meta(
    pipe="test_pipe", nodes=None, failed=0, project=None, git_info=None, graph_data=None, **kw
):
    nodes = nodes or [_node()]
    total = len(nodes)
    completed = sum(1 for n in nodes if n.status == "success")
    return PipelineStoryMetadata(
        pipeline_name=pipe,
        pipeline_layer="silver",
        run_id="run-1",
        started_at="2026-01-01T00:00:00",
        completed_at="2026-01-01T00:01:00",
        duration=60.0,
        total_nodes=total,
        completed_nodes=completed,
        failed_nodes=failed,
        skipped_nodes=0,
        nodes=nodes,
        project=project,
        git_info=git_info,
        graph_data=graph_data,
        **kw,
    )


def _gen(enabled=True, output="docs/gen/", workspace_root=None):
    cap = _Cap()
    gen = DocGenerator(
        config=_cfg(enabled, output),
        pipeline_name="test_pipe",
        workspace_root=workspace_root or tempfile.mkdtemp(),
        write_file=cap,
    )
    return gen, cap


# ===== Init =====


class TestInit:
    def test_local_path(self):
        gen, _ = _gen()
        assert not gen.is_remote
        assert gen.output_path is not None

    def test_remote_workspace_root(self):
        cap = _Cap()
        gen = DocGenerator(
            config=_cfg(),
            pipeline_name="p",
            workspace_root="abfss://c@a.dfs.core.windows.net/root",
            write_file=cap,
        )
        assert gen.is_remote
        assert gen.output_path is None
        assert "abfss://" in gen.output_path_str

    def test_remote_output_path(self):
        cap = _Cap()
        gen = DocGenerator(
            config=DocsConfig(enabled=True, output_path="abfss://c@a.dfs.core.windows.net/out"),
            pipeline_name="p",
            write_file=cap,
        )
        assert gen.is_remote

    def test_get_state_path_local(self):
        gen, _ = _gen()
        assert gen._get_state_path().endswith(".pipelines.json")

    def test_get_state_path_remote(self):
        cap = _Cap()
        gen = DocGenerator(
            config=_cfg(),
            pipeline_name="p",
            workspace_root="abfss://c@a.dfs.core.windows.net/root",
            write_file=cap,
        )
        assert gen._get_state_path().endswith(".pipelines.json")
        assert "abfss://" in gen._get_state_path()


# ===== Write / Read =====


class TestWriteRead:
    def test_write_file_callback(self):
        gen, cap = _gen()
        gen._write_file("/foo/bar.md", "hello")
        assert cap.files["/foo/bar.md"] == "hello"

    def test_write_file_local(self, tmp_path):
        gen = DocGenerator(
            config=_cfg(),
            pipeline_name="p",
            workspace_root=str(tmp_path),
        )
        p = str(tmp_path / "out.md")
        gen._write_file(p, "content")
        assert Path(p).read_text() == "content"

    def test_read_file_local_exists(self, tmp_path):
        f = tmp_path / "data.json"
        f.write_text('{"x":1}')
        gen = DocGenerator(
            config=_cfg(),
            pipeline_name="p",
            workspace_root=str(tmp_path),
        )
        assert gen._read_file(str(f)) == '{"x":1}'

    def test_read_file_local_missing(self):
        gen, _ = _gen()
        assert gen._read_file("/nonexistent/path.json") is None


# ===== Pipeline State =====


class TestPipelineState:
    def test_load_no_file(self):
        gen, cap = _gen()
        state = gen._load_pipeline_state()
        assert state == {"pipelines": {}, "project": None}

    def test_load_valid_json(self):
        gen, cap = _gen()
        cap.files[gen._get_state_path()] = json.dumps({"pipelines": {"p": {}}, "project": "X"})
        with patch.object(gen, "_read_file", side_effect=lambda p: cap.files.get(p)):
            state = gen._load_pipeline_state()
        assert state["project"] == "X"

    def test_load_corrupt_json(self):
        gen, cap = _gen()
        with patch.object(gen, "_read_file", return_value="NOT JSON"):
            state = gen._load_pipeline_state()
        assert state == {"pipelines": {}, "project": None}

    def test_save_writes_json(self):
        gen, cap = _gen()
        gen._save_pipeline_state({"pipelines": {}, "project": "Y"})
        raw = cap.files[gen._get_state_path()]
        assert json.loads(raw)["project"] == "Y"

    def test_update_pipeline_state(self):
        gen, cap = _gen()
        md = _meta(project="Proj")
        with patch.object(
            gen, "_load_pipeline_state", return_value={"pipelines": {}, "project": None}
        ):
            state = gen._update_pipeline_state(md, "story.html")
        assert state["project"] == "Proj"
        assert "test_pipe" in state["pipelines"]
        entry = state["pipelines"]["test_pipe"]
        assert entry["name"] == "test_pipe"
        assert entry["story_path"] == "story.html"


# ===== Generate =====


class TestGenerate:
    def test_disabled_returns_empty(self):
        gen, cap = _gen(enabled=False)
        result = gen.generate(_meta())
        assert result == {}

    def test_success_run_generates_all(self):
        gen, cap = _gen()
        with patch.object(
            gen, "_load_pipeline_state", return_value={"pipelines": {}, "project": None}
        ):
            result = gen.generate(_meta())
        assert "readme" in result
        assert "technical_details" in result
        assert "run_memo" in result

    def test_failed_run_skips_project_docs(self):
        gen, cap = _gen()
        md = _meta(failed=1, nodes=[_node(status="failed")])
        with patch.object(
            gen, "_load_pipeline_state", return_value={"pipelines": {}, "project": None}
        ):
            result = gen.generate(md)
        assert "readme" not in result
        assert "run_memo" in result


# ===== Readme =====


class TestReadme:
    def _readme(self, md=None, state=None):
        gen, cap = _gen()
        md = md or _meta()
        state = state or {
            "pipelines": {
                "test_pipe": {
                    "name": "test_pipe",
                    "layer": "silver",
                    "last_run_id": "run-1",
                    "last_run_time": "2026-01-01T00:00:00",
                    "last_status": "success",
                    "total_nodes": 1,
                    "completed_nodes": 1,
                    "failed_nodes": 0,
                    "duration": 60.0,
                    "story_path": None,
                    "nodes": [
                        {
                            "name": "n1",
                            "operation": "read",
                            "status": "success",
                            "duration": 1.0,
                            "rows_out": 100,
                        }
                    ],
                }
            },
            "project": "MyProject",
        }
        gen._generate_readme(md, "story.html", state)
        return list(cap.files.values())[0]

    def test_includes_health(self):
        assert "Run Health" in self._readme()

    def test_includes_metrics(self):
        assert "Key Metrics" in self._readme()

    def test_includes_git_info(self):
        md = _meta(git_info={"branch": "main", "commit": "abc12345"})
        content = self._readme(md)
        assert "Version" in content
        assert "main" in content

    def test_includes_project_context(self):
        md = _meta(project="Proj", plant="P1", asset="A1")
        md.business_unit = "BU"
        content = self._readme(md)
        assert "Project Context" in content
        assert "P1" in content

    def test_includes_dag(self):
        gd = {"nodes": [{"id": "n1", "type": "transform"}], "edges": [{"from": "n1", "to": "n2"}]}
        md = _meta(graph_data=gd)
        content = self._readme(md)
        assert "mermaid" in content

    def test_includes_pipeline_table(self):
        content = self._readme()
        assert "Pipelines" in content
        assert "test_pipe" in content

    def test_freshness_info(self):
        node = _node(sample_data=[{"updated_at": "2026-01-01 10:00:00"}])
        md = _meta(nodes=[node])
        content = self._readme(md)
        assert "Data Freshness" in content

    def test_story_link(self):
        gen, cap = _gen()
        state = {
            "pipelines": {
                "test_pipe": {
                    "name": "test_pipe",
                    "layer": "silver",
                    "last_status": "success",
                    "total_nodes": 1,
                    "completed_nodes": 1,
                    "failed_nodes": 0,
                    "duration": 1.0,
                    "story_path": "story.html",
                    "last_run_time": "-",
                    "last_run_id": "r",
                    "nodes": [],
                }
            },
            "project": None,
        }
        gen._generate_readme(_meta(), "story.html", state)
        content = list(cap.files.values())[0]
        assert "story.html" in content


# ===== Technical Details =====


class TestTechnicalDetails:
    def _td(self, md=None, state=None):
        gen, cap = _gen()
        md = md or _meta()
        state = state or {
            "pipelines": {
                "test_pipe": {
                    "name": "test_pipe",
                    "layer": "silver",
                    "last_run_id": "r",
                    "last_run_time": "-",
                    "duration": 1.0,
                    "total_nodes": 1,
                    "completed_nodes": 1,
                    "failed_nodes": 0,
                }
            },
            "project": "P",
        }
        gen._generate_technical_details(md, state)
        return list(cap.files.values())[0]

    def test_header(self):
        assert "Technical Details" in self._td()

    def test_git_info(self):
        md = _meta(
            git_info={
                "commit": "abc12345",
                "branch": "dev",
                "author": "me",
                "message": "fix something long",
            }
        )
        assert "Git Information" in self._td(md)

    def test_quality_issues(self):
        node = _node(
            validation_warnings=["w1"],
            failed_rows_counts={"check": 5},
            null_profile={"col_a": 10.5},
        )
        md = _meta(nodes=[node])
        content = self._td(md)
        assert "Data Quality" in content

    def test_output_schemas(self):
        node = _node(schema_out=["id: int", "name: str"], rows_written=50)
        md = _meta(nodes=[node])
        content = self._td(md)
        assert "Output Schemas" in content

    def test_anomalies(self):
        node = _node(is_anomaly=True, anomaly_reasons=["slow", "row_count"])
        md = _meta(nodes=[node])
        content = self._td(md)
        assert "Anomalies" in content


# ===== Node Card =====


class TestNodeCard:
    def _card(self, node=None, **cfg_kw):
        gen, _ = _gen()
        node = node or _node()
        return gen._render_node_card(node)

    def test_basic(self):
        card = self._card()
        assert "# n1" in card
        assert "Summary" in card

    def test_row_change(self):
        n = _node(rows_change=10, rows_change_pct=10.0)
        assert "Row Change" in self._card(n)

    def test_historical_context(self):
        n = _node(historical_avg_duration=0.5, historical_avg_rows=90)
        card = self._card(n)
        assert "Historical Context" in card

    def test_schema_changes(self):
        n = _node(columns_added=["new_col"], columns_removed=["old_col"], columns_renamed=["a->b"])
        card = self._card(n)
        assert "Schema Changes" in card
        assert "new_col" in card
        assert "old_col" in card

    def test_schema_in_out(self):
        n = _node(schema_in=["a: int"], schema_out=["a: int", "b: str"])
        card = self._card(n)
        assert "Schema In" in card
        assert "Schema Out" in card

    def test_schema_truncation(self):
        cols = [f"col{i}: int" for i in range(25)]
        n = _node(schema_out=cols)
        card = self._card(n)
        assert "more columns" in card

    def test_transformation_stack(self):
        n = _node(transformation_stack=["filter", "join", "agg"])
        assert "Transformations" in self._card(n)

    def test_executed_sql_single(self):
        n = _node(executed_sql=["SELECT * FROM t"])
        card = self._card(n)
        assert "Executed SQL" in card
        assert "SELECT * FROM t" in card

    def test_executed_sql_multiple(self):
        n = _node(executed_sql=["SELECT 1", "SELECT 2"])
        card = self._card(n)
        assert "Query 1" in card
        assert "Query 2" in card

    def test_config_snapshot(self):
        n = _node(config_snapshot={"key": "value"})
        card = self._card(n)
        assert "Configuration" in card

    def test_validation_warnings(self):
        n = _node(validation_warnings=["warn1", "warn2"])
        card = self._card(n)
        assert "Validation Warnings" in card
        assert "warn1" in card

    def test_error_details(self):
        n = _node(
            status="failed",
            error_message="boom",
            error_type="ValueError",
            error_traceback="tb lines",
            error_suggestions=["try this"],
            error_context={"stage": "read"},
        )
        card = self._card(n)
        assert "Error Details" in card
        assert "boom" in card
        assert "ValueError" in card
        assert "try this" in card
        assert "stage" in card

    def test_error_cleaned_traceback(self):
        n = _node(status="failed", error_message="boom", error_traceback_cleaned="clean tb")
        card = self._card(n)
        assert "clean tb" in card

    def test_runbook(self):
        n = _node(runbook_url="https://wiki/runbook")
        assert "Runbook" in self._card(n)

    def test_sample_data(self):
        n = _node(sample_data=[{"id": 1, "name": "a"}])
        assert "Sample Data" in self._card(n)

    def test_null_profile(self):
        n = _node(null_profile={"col_a": 5.5, "col_b": 0.0})
        card = self._card(n)
        assert "Null Profile" in card

    def test_column_statistics(self):
        n = _node(column_statistics={"val": {"min": 0, "max": 100, "mean": 50.5, "stddev": 10.2}})
        assert "Column Statistics" in self._card(n)

    def test_failed_rows_samples(self):
        n = _node(
            failed_rows_samples={"not_null_check": [{"id": 1, "val": None}]},
            failed_rows_counts={"not_null_check": 5},
        )
        card = self._card(n)
        assert "Failed Rows" in card
        assert "not_null_check" in card

    def test_failed_rows_truncated(self):
        n = _node(
            failed_rows_samples={"chk": [{"id": 1}]},
            failed_rows_counts={"chk": 100},
            failed_rows_truncated=True,
            truncated_validations=["chk"],
        )
        card = self._card(n)
        assert "truncated" in card.lower()

    def test_source_files(self):
        n = _node(source_files=[f"file{i}.csv" for i in range(15)])
        card = self._card(n)
        assert "Source Files" in card
        assert "more files" in card

    def test_source_files_short(self):
        n = _node(source_files=["a.csv", "b.csv"])
        card = self._card(n)
        assert "a.csv" in card

    def test_delta_info(self):
        di = DeltaWriteInfo(
            version=5, operation="MERGE", read_version=4, operation_metrics={"numTargetRows": 100}
        )
        n = _node(delta_info=di)
        card = self._card(n)
        assert "Delta Write Info" in card
        assert "MERGE" in card

    def test_retry_history(self):
        n = _node(retry_history=[{"attempt": 1, "error": "timeout"}])
        card = self._card(n)
        assert "Retry History" in card

    def test_execution_steps(self):
        n = _node(execution_steps=["step1", "step2"])
        card = self._card(n)
        assert "Execution Steps" in card

    def test_environment(self):
        n = _node(environment={"python": "3.12", "engine": "pandas"})
        card = self._card(n)
        assert "Environment" in card
        assert "python" in card

    def test_duration_history(self):
        n = _node(duration_history=[{"run_id": "r1", "duration": 1.5}])
        card = self._card(n)
        assert "Duration History" in card

    def test_cross_run_changes(self):
        n = _node(
            changed_from_last_success=True,
            changes_detected=["sql", "schema"],
            previous_rows_out=80,
            previous_duration=0.5,
        )
        card = self._card(n)
        assert "Changes from Last Success" in card


# ===== Node Cards Generation =====


class TestGenerateNodeCards:
    def test_skips_skipped_nodes(self):
        gen, cap = _gen()
        md = _meta(nodes=[_node(status="skipped"), _node(name="n2")])
        cards = gen._generate_node_cards(md)
        assert "n1" not in cards
        assert "n2" in cards

    def test_creates_files(self):
        gen, cap = _gen()
        md = _meta(nodes=[_node(name="my_node")])
        cards = gen._generate_node_cards(md)
        assert "my_node" in cards
        written = list(cap.files.keys())
        assert any("my_node" in p for p in written)


# ===== Run Memo =====


class TestRunMemo:
    def _memo(self, md=None, existing=None):
        gen, cap = _gen()
        md = md or _meta()
        history_path = str(Path(gen.output_path_str) / "RUN_HISTORY.md")
        if existing:
            with patch.object(gen, "_read_file", return_value=existing):
                gen._generate_run_memo(md, history_path, "story.html")
        else:
            with patch.object(gen, "_read_file", return_value=None):
                gen._generate_run_memo(md, history_path, "story.html")
        return cap.files.get(history_path, "")

    def test_basic_content(self):
        content = self._memo()
        assert "Run History" in content
        assert "test_pipe" in content

    def test_includes_story_link(self):
        assert "story.html" in self._memo()

    def test_prepends_to_existing(self):
        old = "# Run History: test_pipe\n\n## old run content\n"
        content = self._memo(existing=old)
        assert content.index("run-1") < content.index("old run content")

    def test_quality_issues(self):
        node = _node(validation_warnings=["w"], failed_rows_counts={"c": 3})
        md = _meta(nodes=[node])
        content = self._memo(md)
        assert "Data Quality" in content

    def test_summary_section(self):
        content = self._memo()
        assert "Summary" in content
        assert "Completed" in content


# ===== Render Helpers =====


class TestNormalizeSchema:
    def test_list(self):
        gen, _ = _gen()
        assert gen._normalize_schema(["a: int"]) == ["a: int"]

    def test_dict(self):
        gen, _ = _gen()
        assert gen._normalize_schema({"a": "int"}) == ["a: int"]

    def test_none(self):
        gen, _ = _gen()
        assert gen._normalize_schema(None) == []

    def test_unknown(self):
        gen, _ = _gen()
        assert gen._normalize_schema(42) == []


class TestSanitizeFilename:
    def test_special_chars(self):
        gen, _ = _gen()
        assert gen._sanitize_filename("My Node/v2.0") == "my_node_v2_0"

    def test_backslash_colon(self):
        gen, _ = _gen()
        assert gen._sanitize_filename("a\\b:c") == "a_b_c"


class TestLayerBadges:
    def test_badge(self):
        gen, _ = _gen()
        assert gen._get_layer_badge("SILVER") == "silver"

    def test_badge_empty(self):
        gen, _ = _gen()
        assert gen._get_layer_badge("") == "-"

    def test_badge_image_bronze(self):
        gen, _ = _gen()
        img = gen._get_layer_badge_image("bronze")
        assert "CD7F32" in img

    def test_badge_image_unknown(self):
        gen, _ = _gen()
        img = gen._get_layer_badge_image("custom")
        assert "808080" in img


class TestResolveStoryUrl:
    def test_returns_as_is(self):
        gen, _ = _gen()
        assert gen._resolve_story_url("foo/bar.html") == "foo/bar.html"

    def test_none(self):
        gen, _ = _gen()
        assert gen._resolve_story_url(None) is None


class TestRenderMermaidDag:
    def test_empty_nodes(self):
        gen, _ = _gen()
        assert gen._render_mermaid_dag({"nodes": [], "edges": []}) == []

    def test_node_types(self):
        gen, _ = _gen()
        gd = {
            "nodes": [
                {"id": "src", "type": "source"},
                {"id": "proc", "type": "transform"},
                {"id": "out", "type": "sink"},
            ],
            "edges": [{"from": "src", "to": "proc"}, {"from": "proc", "to": "out"}],
        }
        lines = gen._render_mermaid_dag(gd)
        assert "flowchart LR" in lines[0]
        assert any("(src)" in line for line in lines)  # stadium
        assert any("/out/" in line for line in lines)  # parallelogram
        assert any("-->" in line for line in lines)


class TestRenderSampleDataTable:
    def test_basic(self):
        gen, _ = _gen()
        data = [{"a": 1, "b": "hello"}, {"a": 2, "b": "world"}]
        lines = gen._render_sample_data_table(data)
        assert any("| a |" in line or "a | b" in line for line in lines)

    def test_truncates_long_values(self):
        gen, _ = _gen()
        data = [{"x": "A" * 50}]
        lines = gen._render_sample_data_table(data)
        assert any("..." in line for line in lines)

    def test_more_rows(self):
        gen, _ = _gen()
        data = [{"a": i} for i in range(10)]
        lines = gen._render_sample_data_table(data, max_rows=3)
        assert any("more rows" in line for line in lines)

    def test_empty(self):
        gen, _ = _gen()
        assert gen._render_sample_data_table([]) == []

    def test_none_values(self):
        gen, _ = _gen()
        data = [{"a": None, "b": 1}]
        lines = gen._render_sample_data_table(data)
        assert len(lines) >= 3  # header + separator + row


class TestRenderNullProfile:
    def test_basic(self):
        gen, _ = _gen()
        profile = {"col_a": 10.5, "col_b": 5.0, "col_c": 0.0}
        lines = gen._render_null_profile(profile)
        assert any("col_a" in line for line in lines)
        # col_c has 0% nulls, should not appear
        assert not any("col_c" in line for line in lines)

    def test_empty(self):
        gen, _ = _gen()
        assert gen._render_null_profile({}) == []

    def test_all_zero(self):
        gen, _ = _gen()
        assert gen._render_null_profile({"a": 0.0, "b": 0}) == []

    def test_truncation(self):
        gen, _ = _gen()
        profile = {f"col{i}": float(i) for i in range(1, 20)}
        lines = gen._render_null_profile(profile)
        assert any("more columns" in line for line in lines)


class TestRenderColumnStatistics:
    def test_basic(self):
        gen, _ = _gen()
        stats = {"val": {"min": 0, "max": 100, "mean": 50.5, "stddev": 10.2}}
        lines = gen._render_column_statistics(stats)
        assert any("val" in line for line in lines)
        assert any("50.50" in line for line in lines)

    def test_empty(self):
        gen, _ = _gen()
        assert gen._render_column_statistics({}) == []

    def test_missing_fields(self):
        gen, _ = _gen()
        stats = {"x": {"min": 1}}
        lines = gen._render_column_statistics(stats)
        assert any("x" in line for line in lines)


class TestRenderFailedRowsSamples:
    def test_basic(self):
        gen, _ = _gen()
        n = _node(
            failed_rows_samples={"chk1": [{"id": 1, "val": None}]},
            failed_rows_counts={"chk1": 10},
        )
        lines = gen._render_failed_rows_samples(n)
        assert any("chk1" in line for line in lines)
        assert any("10" in line for line in lines)

    def test_empty(self):
        gen, _ = _gen()
        n = _node()
        assert gen._render_failed_rows_samples(n) == []

    def test_truncated(self):
        gen, _ = _gen()
        n = _node(
            failed_rows_samples={"chk": [{"id": 1}]},
            failed_rows_counts={"chk": 5},
            failed_rows_truncated=True,
            truncated_validations=["chk"],
        )
        lines = gen._render_failed_rows_samples(n)
        assert any("truncated" in line.lower() for line in lines)

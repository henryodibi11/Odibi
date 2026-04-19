"""Tests for odibi.story.generator — StoryGenerator pure logic paths."""

import json
import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import yaml

from odibi.node import NodeResult
from odibi.story.generator import MultilineString, StoryGenerator, multiline_presenter
from odibi.story.metadata import NodeExecutionMetadata, PipelineStoryMetadata


# ── MultilineString / multiline_presenter ────────────────────────


def test_multiline_string_is_str():
    ms = MultilineString("hello\nworld")
    assert isinstance(ms, str)
    assert ms == "hello\nworld"


def test_multiline_presenter_returns_block_scalar():
    dumper = yaml.Dumper("")
    node = multiline_presenter(dumper, MultilineString("line1\nline2"))
    assert node.style == "|"


# ── __init__ ─────────────────────────────────────────────────────


def test_init_local_path(tmp_path):
    sg = StoryGenerator("test_pipe", output_path=str(tmp_path / "stories"))
    assert sg.is_remote is False
    assert sg.output_path is not None
    assert sg.output_path.exists()


def test_init_remote_path():
    sg = StoryGenerator(
        "test_pipe", output_path="abfss://container@acct.dfs.core.windows.net/stories"
    )
    assert sg.is_remote is True
    assert sg.output_path is None


def test_init_defaults(tmp_path):
    sg = StoryGenerator("pipe1", output_path=str(tmp_path / "s"))
    assert sg.pipeline_name == "pipe1"
    assert sg.max_sample_rows == 10
    assert sg.retention_days == 30
    assert sg.retention_count == 100
    assert sg.catalog_manager is None


# ── _infer_layer_from_path ───────────────────────────────────────


@pytest.mark.parametrize(
    "path,expected",
    [
        ("bronze/raw_data", "bronze"),
        ("silver/customers", "silver"),
        ("gold/reports", "gold"),
        ("raw/files", "raw"),
        ("staging/temp", "staging"),
        ("semantic/views", "semantic"),
        ("other/path", "source"),
        ("BRONZE/UPPER", "bronze"),
    ],
)
def test_infer_layer_from_path(tmp_path, path, expected):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    assert sg._infer_layer_from_path(path) == expected


# ── _compute_anomalies ───────────────────────────────────────────


def _make_node_meta(**kwargs):
    defaults = dict(node_name="n", operation="transform", status="success", duration=1.0)
    defaults.update(kwargs)
    return NodeExecutionMetadata(**defaults)


def test_compute_anomalies_normal(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    node = _make_node_meta(duration=2.0, historical_avg_duration=1.0)
    sg._compute_anomalies(node)
    assert node.is_slow is False
    assert node.is_anomaly is False


def test_compute_anomalies_slow(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    node = _make_node_meta(duration=10.0, historical_avg_duration=2.0)
    sg._compute_anomalies(node)
    assert node.is_slow is True
    assert node.is_anomaly is True
    assert any("Slow" in r for r in node.anomaly_reasons)


def test_compute_anomalies_row_anomaly(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    node = _make_node_meta(rows_out=200, historical_avg_rows=100.0)
    sg._compute_anomalies(node)
    assert node.has_row_anomaly is True
    assert node.is_anomaly is True


def test_compute_anomalies_both(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    node = _make_node_meta(
        duration=10.0,
        historical_avg_duration=2.0,
        rows_out=5,
        historical_avg_rows=100.0,
    )
    sg._compute_anomalies(node)
    assert node.is_slow is True
    assert node.has_row_anomaly is True
    assert len(node.anomaly_reasons) == 2


def test_compute_anomalies_no_history(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    node = _make_node_meta(duration=10.0)
    sg._compute_anomalies(node)
    assert node.is_anomaly is False


def test_compute_anomalies_zero_avg(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    node = _make_node_meta(duration=10.0, historical_avg_duration=0.0)
    sg._compute_anomalies(node)
    assert node.is_slow is False


def test_compute_anomalies_rows_within_threshold(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    node = _make_node_meta(rows_out=110, historical_avg_rows=100.0)
    sg._compute_anomalies(node)
    assert node.has_row_anomaly is False


# ── _convert_result_to_metadata ──────────────────────────────────


def test_convert_success_result(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    result = NodeResult(
        node_name="node_a",
        success=True,
        duration=1.5,
        rows_processed=100,
        rows_read=120,
        rows_written=100,
    )
    meta = sg._convert_result_to_metadata(result, "node_a")
    assert meta.node_name == "node_a"
    assert meta.status == "success"
    assert meta.duration == 1.5
    assert meta.rows_out == 100
    assert meta.rows_in == 120


def test_convert_failed_result(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    err = ValueError("bad data")
    result = NodeResult(
        node_name="node_b",
        success=False,
        duration=0.5,
        error=err,
    )
    meta = sg._convert_result_to_metadata(result, "node_b")
    assert meta.status == "failed"
    assert meta.error_message == "bad data"
    assert meta.error_type == "ValueError"


def test_convert_result_with_metadata_dict(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    result = NodeResult(
        node_name="n",
        success=True,
        duration=1.0,
        metadata={"sql_hash": "abc123", "steps": ["read", "transform"]},
    )
    meta = sg._convert_result_to_metadata(result, "n")
    assert meta.sql_hash == "abc123"
    assert meta.execution_steps == ["read", "transform"]


# ── _get_error_suggestions ───────────────────────────────────────


def test_error_suggestions_no_error(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    result = NodeResult(node_name="n", success=True, duration=1.0)
    assert sg._get_error_suggestions(result, "n", {}) == []


def test_error_suggestions_with_suggestions_attr(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    err = ValueError("bad")
    err.suggestions = ["Try this", "Try that"]
    result = NodeResult(node_name="n", success=False, duration=1.0, error=err)
    suggestions = sg._get_error_suggestions(result, "n", {})
    assert suggestions == ["Try this", "Try that"]


def test_error_suggestions_generic_error(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    result = NodeResult(
        node_name="n",
        success=False,
        duration=1.0,
        error=ValueError("oops"),
    )
    suggestions = sg._get_error_suggestions(result, "n", {})
    assert isinstance(suggestions, list)


# ── _get_error_context ───────────────────────────────────────────


def test_error_context_no_error(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    result = NodeResult(node_name="n", success=True, duration=1.0)
    assert sg._get_error_context(result, {}) is None


def test_error_context_with_schema_diff(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    result = NodeResult(
        node_name="n",
        success=False,
        duration=1.0,
        error=ValueError("x"),
        schema=["a", "b", "c"],
    )
    meta = {"schema_in": ["a", "b"]}
    ctx = sg._get_error_context(result, meta)
    assert ctx is not None
    assert "schema_diff" in ctx
    assert "c" in ctx["schema_diff"]["columns_added"]


def test_error_context_with_row_progress(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    result = NodeResult(
        node_name="n",
        success=False,
        duration=1.0,
        error=ValueError("x"),
        rows_processed=50,
    )
    meta = {"rows_read": 100}
    ctx = sg._get_error_context(result, meta)
    assert ctx["row_progress"]["rows_read"] == 100


def test_error_context_with_steps(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    result = NodeResult(
        node_name="n",
        success=False,
        duration=1.0,
        error=ValueError("x"),
    )
    meta = {"steps": ["read", "transform"]}
    ctx = sg._get_error_context(result, meta)
    assert ctx["execution_trace"]["steps_completed"] == 2


def test_error_context_with_retry_history(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    result = NodeResult(
        node_name="n",
        success=False,
        duration=1.0,
        error=ValueError("x"),
    )
    retry = [{"attempt": 1, "error": "timeout"}]
    ctx = sg._get_error_context(result, {"retry_history": retry})
    assert ctx["retry_history"] == retry


# ── _get_relevant_package_versions ───────────────────────────────


def test_package_versions(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    versions = sg._get_relevant_package_versions()
    assert isinstance(versions, dict)
    assert "pandas" in versions


# ── _clean_config_for_dump ───────────────────────────────────────


def test_clean_config_dict(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    result = sg._clean_config_for_dump({"a": 1, "b": "hello"})
    assert result == {"a": 1, "b": "hello"}


def test_clean_config_list(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    result = sg._clean_config_for_dump([1, "hi", {"x": 2}])
    assert result == [1, "hi", {"x": 2}]


def test_clean_config_multiline_string(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    result = sg._clean_config_for_dump("line1\nline2")
    assert isinstance(result, MultilineString)


def test_clean_config_regular_string(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    result = sg._clean_config_for_dump("no newlines")
    assert not isinstance(result, MultilineString)
    assert result == "no newlines"


def test_clean_config_non_string(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    assert sg._clean_config_for_dump(42) == 42
    assert sg._clean_config_for_dump(None) is None


# ── _get_git_info ────────────────────────────────────────────────


def test_get_git_info(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    info = sg._get_git_info()
    assert "commit" in info
    assert "branch" in info


def test_get_git_info_no_git(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    with patch("subprocess.check_output", side_effect=FileNotFoundError):
        info = sg._get_git_info()
    assert info["commit"] == "unknown"
    assert info["branch"] == "unknown"


# ── get_alert_summary ────────────────────────────────────────────


def test_alert_summary_no_metadata(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    assert sg.get_alert_summary() == {}


def test_alert_summary_with_metadata(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    meta = PipelineStoryMetadata(pipeline_name="p")
    meta.nodes.append(_make_node_meta(rows_out=100))
    sg._last_metadata = meta
    sg._last_story_path = "/some/path.html"
    summary = sg.get_alert_summary()
    assert summary["story_path"] == "/some/path.html"
    assert summary["total_rows_processed"] == 100


# ── _apply_retention ─────────────────────────────────────────────


def test_apply_retention_count(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"), retention_count=2)
    files = []
    for i in range(5):
        f = tmp_path / f"story_{i}.html"
        f.write_text(f"story {i}")
        files.append(f)
    sg._apply_retention(files, [])
    remaining = [f for f in files if f.exists()]
    assert len(remaining) == 2


def test_apply_retention_keeps_within_limit(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"), retention_count=10)
    files = []
    for i in range(3):
        f = tmp_path / f"story_{i}.html"
        f.write_text(f"story {i}")
        files.append(f)
    sg._apply_retention(files, [])
    remaining = [f for f in files if f.exists()]
    assert len(remaining) == 3


# ── _render_index_html ───────────────────────────────────────────


def test_render_index_html(tmp_path):
    sg = StoryGenerator("mypipe", output_path=str(tmp_path / "s"))
    runs = [
        {
            "run_id": "r1",
            "started_at": "2026-01-01",
            "duration": 1.5,
            "total_nodes": 3,
            "completed_nodes": 3,
            "failed_nodes": 0,
            "success_rate": 100.0,
            "html_path": "2026-01-01/run_01.html",
            "status": "success",
        },
        {
            "run_id": "r2",
            "started_at": "2026-01-02",
            "duration": 2.0,
            "total_nodes": 3,
            "completed_nodes": 2,
            "failed_nodes": 1,
            "success_rate": 66.7,
            "html_path": "2026-01-02/run_01.html",
            "status": "failed",
        },
    ]
    html = sg._render_index_html(runs)
    assert "mypipe" in html
    assert "r1" in html
    assert "r2" in html
    assert "✓" in html
    assert "✗" in html


# ── _build_graph_data ────────────────────────────────────────────


def test_build_graph_data_with_graph_data(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    meta = PipelineStoryMetadata(pipeline_name="p")
    graph_data = {
        "nodes": [{"id": "a", "label": "A"}],
        "edges": [{"source": "a", "target": "b"}],
    }
    result = sg._build_graph_data(meta, graph_data, None)
    assert len(result["nodes"]) >= 1
    assert len(result["edges"]) == 1


def test_build_graph_data_from_config(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    meta = PipelineStoryMetadata(pipeline_name="p")
    config = {
        "nodes": [
            {"name": "read_data", "type": "read", "depends_on": []},
            {"name": "transform", "type": "transform", "depends_on": ["read_data"]},
        ]
    }
    result = sg._build_graph_data(meta, None, config)
    node_ids = [n["id"] for n in result["nodes"]]
    assert "read_data" in node_ids
    assert "transform" in node_ids


def test_build_graph_data_fallback(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    node_a = _make_node_meta(node_name="a")
    node_b = _make_node_meta(node_name="b")
    meta = PipelineStoryMetadata(pipeline_name="p")
    meta.nodes = [node_a, node_b]
    result = sg._build_graph_data(meta, None, None)
    node_ids = [n["id"] for n in result["nodes"]]
    assert "a" in node_ids
    assert "b" in node_ids


def test_build_graph_data_cross_pipeline(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    meta = PipelineStoryMetadata(pipeline_name="p")
    config = {
        "nodes": [
            {
                "name": "node1",
                "type": "transform",
                "inputs": {"src": "$other_pipe.output_node"},
            },
        ]
    }
    result = sg._build_graph_data(meta, None, config)
    ext_nodes = [n for n in result["nodes"] if n.get("is_external")]
    assert len(ext_nodes) == 1
    assert ext_nodes[0]["source_pipeline"] == "other_pipe"


# ── _get_duration_history ────────────────────────────────────────


def test_duration_history_remote(tmp_path):
    sg = StoryGenerator("p", output_path="abfss://container@acct/stories")
    assert sg._get_duration_history("node1") == []


def test_duration_history_no_dir(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "stories"))
    assert sg._get_duration_history("node1") == []


def test_duration_history_with_files(tmp_path):
    stories_dir = tmp_path / "stories" / "p" / "2026-01-01"
    stories_dir.mkdir(parents=True)
    data = {"nodes": [{"node_name": "node1", "duration": 2.5}], "run_id": "r1"}
    (stories_dir / "run_01.json").write_text(json.dumps(data))
    sg = StoryGenerator("p", output_path=str(tmp_path / "stories"))
    history = sg._get_duration_history("node1")
    assert len(history) == 1
    assert history[0]["duration"] == 2.5


# ── _find_last_successful_run ────────────────────────────────────


def test_find_last_success_no_output_path():
    sg = StoryGenerator("p", output_path="abfss://c@a/s")
    with patch.object(sg, "_find_last_successful_run_remote", return_value=None):
        assert sg._find_last_successful_run() is None


def test_find_last_success_no_pipeline_dir(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "stories"))
    assert sg._find_last_successful_run() is None


def test_find_last_success_found(tmp_path):
    stories_dir = tmp_path / "stories" / "p" / "2026-01-01"
    stories_dir.mkdir(parents=True)
    data = {"failed_nodes": 0, "run_id": "r1", "nodes": []}
    (stories_dir / "run_01.json").write_text(json.dumps(data))
    sg = StoryGenerator("p", output_path=str(tmp_path / "stories"))
    result = sg._find_last_successful_run()
    assert result is not None
    assert result["run_id"] == "r1"


def test_find_last_success_skips_failed(tmp_path):
    stories_dir = tmp_path / "stories" / "p" / "2026-01-01"
    stories_dir.mkdir(parents=True)
    (stories_dir / "run_01.json").write_text(json.dumps({"failed_nodes": 2, "run_id": "r1"}))
    (stories_dir / "run_02.json").write_text(json.dumps({"failed_nodes": 0, "run_id": "r2"}))
    sg = StoryGenerator("p", output_path=str(tmp_path / "stories"))
    result = sg._find_last_successful_run()
    assert result["run_id"] == "r2"


# ── cleanup ──────────────────────────────────────────────────────


def test_cleanup_remote(tmp_path):
    sg = StoryGenerator("p", output_path="abfss://c@a/s")
    with patch.object(sg, "_cleanup_remote") as mock_cleanup:
        sg.cleanup()
        mock_cleanup.assert_called_once()


def test_cleanup_no_output_path():
    sg = StoryGenerator.__new__(StoryGenerator)
    sg.is_remote = False
    sg.output_path = None
    sg.pipeline_name = "p"
    sg.retention_days = 30
    sg.retention_count = 100
    sg.storage_options = {}
    sg.cleanup()  # should not raise


def test_cleanup_local(tmp_path):
    stories_dir = tmp_path / "stories"
    sg = StoryGenerator("p", output_path=str(stories_dir), retention_count=1)
    pipeline_dir = stories_dir / "p" / "2026-01-01"
    pipeline_dir.mkdir(parents=True)
    (pipeline_dir / "run_01.html").write_text("html1")
    (pipeline_dir / "run_02.html").write_text("html2")
    sg.cleanup()
    remaining = list((stories_dir / "p").rglob("*.html"))
    assert len(remaining) <= 1


# ── _compare_with_last_success ───────────────────────────────────


def test_compare_no_previous_run(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    meta = PipelineStoryMetadata(pipeline_name="p")
    sg._compare_with_last_success(meta)
    assert meta.change_summary is None


def test_compare_detects_row_change(tmp_path):
    stories_dir = tmp_path / "stories" / "p" / "2026-01-01"
    stories_dir.mkdir(parents=True)
    prev = {
        "failed_nodes": 0,
        "run_id": "r_prev",
        "nodes": [{"node_name": "n1", "rows_out": 100, "status": "success"}],
    }
    (stories_dir / "run_01.json").write_text(json.dumps(prev))
    sg = StoryGenerator("p", output_path=str(tmp_path / "stories"))
    meta = PipelineStoryMetadata(pipeline_name="p")
    node = _make_node_meta(node_name="n1", rows_out=200)
    meta.nodes = [node]
    sg._compare_with_last_success(meta)
    assert meta.change_summary is not None
    assert meta.change_summary["rows_changed_count"] == 1


def test_compare_detects_sql_change(tmp_path):
    stories_dir = tmp_path / "stories" / "p" / "2026-01-01"
    stories_dir.mkdir(parents=True)
    prev = {
        "failed_nodes": 0,
        "run_id": "r_prev",
        "nodes": [{"node_name": "n1", "sql_hash": "old_hash", "status": "success"}],
    }
    (stories_dir / "run_01.json").write_text(json.dumps(prev))
    sg = StoryGenerator("p", output_path=str(tmp_path / "stories"))
    meta = PipelineStoryMetadata(pipeline_name="p")
    node = _make_node_meta(node_name="n1")
    node.sql_hash = "new_hash"
    meta.nodes = [node]
    sg._compare_with_last_success(meta)
    assert meta.change_summary["sql_changed_count"] == 1


# ── generate (integration) ───────────────────────────────────────


def test_generate_creates_files(tmp_path):
    sg = StoryGenerator("test_pipe", output_path=str(tmp_path / "stories"))
    result = NodeResult(
        node_name="node_a",
        success=True,
        duration=1.0,
        rows_processed=50,
        metadata={},
    )
    html_path = sg.generate(
        node_results={"node_a": result},
        completed=["node_a"],
        failed=[],
        skipped=[],
        duration=1.0,
        start_time=datetime.now().isoformat(),
        end_time=datetime.now().isoformat(),
    )
    assert html_path.endswith(".html")
    assert os.path.exists(html_path)
    json_path = html_path.replace(".html", ".json")
    assert os.path.exists(json_path)


def test_generate_with_failed_node(tmp_path):
    sg = StoryGenerator("test_pipe", output_path=str(tmp_path / "stories"))
    result = NodeResult(
        node_name="bad_node",
        success=False,
        duration=0.5,
        error=ValueError("broken"),
        metadata={},
    )
    html_path = sg.generate(
        node_results={"bad_node": result},
        completed=[],
        failed=["bad_node"],
        skipped=[],
        duration=0.5,
        start_time=datetime.now().isoformat(),
        end_time=datetime.now().isoformat(),
    )
    assert os.path.exists(html_path)


def test_generate_with_skipped_node(tmp_path):
    sg = StoryGenerator("test_pipe", output_path=str(tmp_path / "stories"))
    html_path = sg.generate(
        node_results={},
        completed=[],
        failed=[],
        skipped=["skip_node"],
        duration=0.1,
        start_time=datetime.now().isoformat(),
        end_time=datetime.now().isoformat(),
    )
    assert os.path.exists(html_path)


def test_generate_with_config(tmp_path):
    sg = StoryGenerator("test_pipe", output_path=str(tmp_path / "stories"))
    result = NodeResult(
        node_name="a",
        success=True,
        duration=1.0,
        metadata={},
    )
    config = {"layer": "silver", "nodes": [{"name": "a"}]}
    html_path = sg.generate(
        node_results={"a": result},
        completed=["a"],
        failed=[],
        skipped=[],
        duration=1.0,
        start_time=datetime.now().isoformat(),
        end_time=datetime.now().isoformat(),
        config=config,
    )
    assert os.path.exists(html_path)


# ── _write_remote ────────────────────────────────────────────────


def test_write_remote_fsspec(tmp_path):
    sg = StoryGenerator("p", output_path="abfss://c@a.dfs.core.windows.net/stories")
    mock_file = MagicMock()
    mock_file.__enter__ = MagicMock(return_value=mock_file)
    mock_file.__exit__ = MagicMock(return_value=False)

    mock_fsspec = MagicMock()
    mock_fsspec.open.return_value = mock_file

    with patch.dict("sys.modules", {"fsspec": mock_fsspec}):
        sg._write_remote("abfss://c@a/stories/test.html", "<html>hello</html>")

    mock_fsspec.open.assert_called_once()
    mock_file.write.assert_called_once_with("<html>hello</html>")


def test_write_remote_dbutils_fallback(tmp_path):
    sg = StoryGenerator("p", output_path="abfss://c@a.dfs.core.windows.net/stories")

    mock_spark_session = MagicMock()
    mock_dbutils_cls = MagicMock()
    mock_dbutils_inst = MagicMock()
    mock_dbutils_cls.return_value = mock_dbutils_inst

    mock_pyspark_dbutils = MagicMock()
    mock_pyspark_dbutils.DBUtils = mock_dbutils_cls

    mock_pyspark_sql = MagicMock()
    mock_pyspark_sql.SparkSession.builder.getOrCreate.return_value = mock_spark_session

    with patch.dict(
        "sys.modules",
        {
            "fsspec": None,  # force ImportError
            "pyspark": MagicMock(),
            "pyspark.dbutils": mock_pyspark_dbutils,
            "pyspark.sql": mock_pyspark_sql,
        },
    ):
        sg._write_remote("abfss://c@a/stories/test.html", "<html>hi</html>")

    mock_dbutils_inst.fs.put.assert_called_once_with(
        "abfss://c@a/stories/test.html", "<html>hi</html>", True
    )


def test_write_remote_both_unavailable():
    sg = StoryGenerator("p", output_path="abfss://c@a.dfs.core.windows.net/stories")

    with patch.dict(
        "sys.modules",
        {
            "fsspec": None,
            "pyspark": None,
            "pyspark.dbutils": None,
            "pyspark.sql": None,
        },
    ):
        with pytest.raises(RuntimeError, match="Could not write story"):
            sg._write_remote("abfss://c@a/stories/test.html", "content")


# ── _cleanup_remote ──────────────────────────────────────────────


def test_cleanup_remote_fsspec_not_available():
    sg = StoryGenerator("p", output_path="abfss://c@a.dfs.core.windows.net/stories")
    with patch.dict("sys.modules", {"fsspec": None, "fsspec.core": None}):
        # Should not raise - just returns
        sg._cleanup_remote()


def test_cleanup_remote_path_not_exists():
    sg = StoryGenerator("p", output_path="abfss://c@a.dfs.core.windows.net/stories")

    mock_fs = MagicMock()
    mock_fs.exists.return_value = False

    mock_fsspec = MagicMock()
    mock_fsspec.core.url_to_fs.return_value = (mock_fs, "container/stories/p")

    with patch.dict("sys.modules", {"fsspec": mock_fsspec, "fsspec.core": mock_fsspec.core}):
        sg._cleanup_remote()

    mock_fs.exists.assert_called_once()


def test_cleanup_remote_empty_file_list():
    sg = StoryGenerator("p", output_path="abfss://c@a.dfs.core.windows.net/stories")

    mock_fs = MagicMock()
    mock_fs.exists.return_value = True
    mock_fs.walk.return_value = []  # no files

    mock_fsspec = MagicMock()
    mock_fsspec.core.url_to_fs.return_value = (mock_fs, "container/stories/p")

    with patch.dict("sys.modules", {"fsspec": mock_fsspec, "fsspec.core": mock_fsspec.core}):
        sg._cleanup_remote()


def test_cleanup_remote_count_retention():
    sg = StoryGenerator(
        "p",
        output_path="abfss://c@a.dfs.core.windows.net/stories",
        retention_count=2,
        retention_days=None,
    )

    mock_fs = MagicMock()
    mock_fs.exists.return_value = True
    mock_fs.walk.return_value = [
        (
            "container/stories/p/2026-01-01",
            [],
            [
                "run_01.html",
                "run_01.json",
                "run_02.html",
                "run_02.json",
                "run_03.html",
                "run_03.json",
            ],
        ),
    ]
    mock_fs.ls.return_value = []

    mock_fsspec = MagicMock()
    mock_fsspec.core.url_to_fs.return_value = (mock_fs, "container/stories/p")

    with patch.dict("sys.modules", {"fsspec": mock_fsspec, "fsspec.core": mock_fsspec.core}):
        sg._cleanup_remote()

    # 3 html files, retention=2 → 1 html deleted; same for json
    assert mock_fs.rm.call_count >= 2


def test_cleanup_remote_time_retention():
    sg = StoryGenerator(
        "p",
        output_path="abfss://c@a.dfs.core.windows.net/stories",
        retention_count=100,
        retention_days=1,
    )

    mock_fs = MagicMock()
    mock_fs.exists.return_value = True
    # Files from an old date
    mock_fs.walk.return_value = [
        (
            "container/stories/p/2020-01-01",
            [],
            [
                "run_01.html",
                "run_01.json",
            ],
        ),
    ]
    mock_fs.ls.return_value = []

    mock_fsspec = MagicMock()
    mock_fsspec.core.url_to_fs.return_value = (mock_fs, "container/stories/p")

    with patch.dict("sys.modules", {"fsspec": mock_fsspec, "fsspec.core": mock_fsspec.core}):
        sg._cleanup_remote()

    # Old date should trigger time-based deletion
    assert mock_fs.rm.call_count >= 1


def test_cleanup_remote_empty_dir_cleanup():
    sg = StoryGenerator(
        "p",
        output_path="abfss://c@a.dfs.core.windows.net/stories",
        retention_count=100,
        retention_days=None,
    )

    mock_fs = MagicMock()
    mock_fs.exists.return_value = True
    mock_fs.walk.return_value = []  # no files
    mock_fs.ls.return_value = ["container/stories/p/2026-01-01"]
    mock_fs.isdir.return_value = True
    mock_fs.ls.side_effect = [
        ["container/stories/p/2026-01-01"],  # first ls for path_prefix
        [],  # second ls for the dir (empty)
    ]

    mock_fsspec = MagicMock()
    mock_fsspec.core.url_to_fs.return_value = (mock_fs, "container/stories/p")

    with patch.dict("sys.modules", {"fsspec": mock_fsspec, "fsspec.core": mock_fsspec.core}):
        sg._cleanup_remote()


def test_cleanup_remote_walk_error():
    sg = StoryGenerator("p", output_path="abfss://c@a.dfs.core.windows.net/stories")

    mock_fs = MagicMock()
    mock_fs.exists.return_value = True
    mock_fs.walk.side_effect = OSError("connection failed")

    mock_fsspec = MagicMock()
    mock_fsspec.core.url_to_fs.return_value = (mock_fs, "container/stories/p")

    with patch.dict("sys.modules", {"fsspec": mock_fsspec, "fsspec.core": mock_fsspec.core}):
        # Should not raise — catches the error
        sg._cleanup_remote()


def test_cleanup_remote_general_exception():
    sg = StoryGenerator("p", output_path="abfss://c@a.dfs.core.windows.net/stories")

    mock_fsspec = MagicMock()
    mock_fsspec.core.url_to_fs.side_effect = Exception("unexpected error")

    with patch.dict("sys.modules", {"fsspec": mock_fsspec, "fsspec.core": mock_fsspec.core}):
        # Should not raise — logs warning
        sg._cleanup_remote()


# ── _generate_pipeline_index ─────────────────────────────────────


def test_generate_pipeline_index_remote():
    sg = StoryGenerator("p", output_path="abfss://c@a.dfs.core.windows.net/stories")
    # Should return early for remote storage
    sg._generate_pipeline_index()


def test_generate_pipeline_index_no_output_path():
    sg = StoryGenerator.__new__(StoryGenerator)
    sg.is_remote = False
    sg.output_path = None
    sg.pipeline_name = "p"
    sg._generate_pipeline_index()


def test_generate_pipeline_index_no_pipeline_dir(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "stories"))
    # Pipeline dir doesn't exist yet (only stories/ exists)
    sg._generate_pipeline_index()
    assert not (tmp_path / "stories" / "p" / "index.html").exists()


def test_generate_pipeline_index_no_json_files(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "stories"))
    pipeline_dir = tmp_path / "stories" / "p" / "2026-01-01"
    pipeline_dir.mkdir(parents=True)
    # Only HTML, no JSON
    (pipeline_dir / "run_01.html").write_text("<html></html>")
    sg._generate_pipeline_index()
    assert not (tmp_path / "stories" / "p" / "index.html").exists()


def test_generate_pipeline_index_valid_json(tmp_path):
    sg = StoryGenerator("test_pipe", output_path=str(tmp_path / "stories"))
    pipeline_dir = tmp_path / "stories" / "test_pipe" / "2026-01-01"
    pipeline_dir.mkdir(parents=True)
    data = {
        "run_id": "r1",
        "started_at": "2026-01-01T00:00:00",
        "duration": 1.5,
        "total_nodes": 3,
        "completed_nodes": 3,
        "failed_nodes": 0,
        "success_rate": 100.0,
    }
    (pipeline_dir / "run_01.json").write_text(json.dumps(data))
    (pipeline_dir / "run_01.html").write_text("<html>story</html>")

    sg._generate_pipeline_index()

    index_path = tmp_path / "stories" / "test_pipe" / "index.html"
    assert index_path.exists()
    content = index_path.read_text()
    assert "test_pipe" in content
    assert "r1" in content
    assert "100.0" in content


def test_generate_pipeline_index_malformed_json(tmp_path):
    sg = StoryGenerator("test_pipe", output_path=str(tmp_path / "stories"))
    pipeline_dir = tmp_path / "stories" / "test_pipe" / "2026-01-01"
    pipeline_dir.mkdir(parents=True)
    # Malformed JSON
    (pipeline_dir / "run_01.json").write_text("{bad json")
    # Valid JSON
    data = {
        "run_id": "r2",
        "started_at": "2026-01-01T01:00:00",
        "duration": 2.0,
        "total_nodes": 2,
        "completed_nodes": 2,
        "failed_nodes": 0,
        "success_rate": 100.0,
    }
    (pipeline_dir / "run_02.json").write_text(json.dumps(data))
    (pipeline_dir / "run_02.html").write_text("<html>story</html>")

    sg._generate_pipeline_index()

    index_path = tmp_path / "stories" / "test_pipe" / "index.html"
    assert index_path.exists()
    content = index_path.read_text()
    assert "r2" in content


def test_generate_pipeline_index_write_failure(tmp_path):
    sg = StoryGenerator("test_pipe", output_path=str(tmp_path / "stories"))
    pipeline_dir = tmp_path / "stories" / "test_pipe" / "2026-01-01"
    pipeline_dir.mkdir(parents=True)
    data = {
        "run_id": "r1",
        "started_at": "2026-01-01T00:00:00",
        "duration": 1.0,
        "total_nodes": 1,
        "completed_nodes": 1,
        "failed_nodes": 0,
        "success_rate": 100.0,
    }
    (pipeline_dir / "run_01.json").write_text(json.dumps(data))
    (pipeline_dir / "run_01.html").write_text("<html></html>")

    with patch(
        "builtins.open",
        side_effect=[
            open(str(pipeline_dir / "run_01.json"), "r", encoding="utf-8"),  # read JSON
            OSError("disk full"),  # write index fails
        ],
    ):
        sg._generate_pipeline_index()  # should not raise


# ── _render_index_html (additional) ──────────────────────────────


def test_render_index_html_pipeline_name(tmp_path):
    sg = StoryGenerator("my_pipeline", output_path=str(tmp_path / "s"))
    runs = [
        {
            "run_id": "r1",
            "started_at": "2026-01-01",
            "duration": 1.0,
            "total_nodes": 2,
            "completed_nodes": 2,
            "failed_nodes": 0,
            "success_rate": 100.0,
            "html_path": "2026-01-01/run_01.html",
            "status": "success",
        }
    ]
    html = sg._render_index_html(runs)
    assert "Pipeline History: my_pipeline" in html


def test_render_index_html_status_classes(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    runs = [
        {
            "run_id": "ok",
            "started_at": "2026-01-01",
            "duration": 1.0,
            "total_nodes": 1,
            "completed_nodes": 1,
            "failed_nodes": 0,
            "success_rate": 100.0,
            "html_path": "2026-01-01/r1.html",
            "status": "success",
        },
        {
            "run_id": "bad",
            "started_at": "2026-01-02",
            "duration": 2.0,
            "total_nodes": 1,
            "completed_nodes": 0,
            "failed_nodes": 1,
            "success_rate": 0.0,
            "html_path": "2026-01-02/r2.html",
            "status": "failed",
        },
    ]
    html = sg._render_index_html(runs)
    assert 'class="success"' in html
    assert 'class="failed"' in html


def test_render_index_html_run_links(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    runs = [
        {
            "run_id": "r1",
            "started_at": "2026-01-01",
            "duration": 1.0,
            "total_nodes": 1,
            "completed_nodes": 1,
            "failed_nodes": 0,
            "success_rate": 100.0,
            "html_path": "2026-01-01/run_01.html",
            "status": "success",
        }
    ]
    html = sg._render_index_html(runs)
    assert 'href="2026-01-01/run_01.html"' in html
    assert ">r1</a>" in html


# ── _find_last_successful_run_remote ─────────────────────────────


def test_find_last_successful_run_remote_no_fsspec():
    sg = StoryGenerator("p", output_path="abfss://c@a.dfs.core.windows.net/stories")
    with patch.dict("sys.modules", {"fsspec": None}):
        result = sg._find_last_successful_run_remote()
    assert result is None


def test_find_last_successful_run_remote_no_json():
    sg = StoryGenerator("p", output_path="abfss://c@a.dfs.core.windows.net/stories")

    mock_fs = MagicMock()
    mock_fs.glob.return_value = []

    mock_fsspec = MagicMock()
    mock_fsspec.filesystem.return_value = mock_fs

    with patch.dict("sys.modules", {"fsspec": mock_fsspec}):
        result = sg._find_last_successful_run_remote()
    assert result is None


def test_find_last_successful_run_remote_finds_success():
    sg = StoryGenerator("p", output_path="abfss://c@a.dfs.core.windows.net/stories")

    run_data = {"failed_nodes": 0, "run_id": "r1", "nodes": []}

    mock_fs = MagicMock()
    mock_fs.glob.return_value = ["c@a.dfs.core.windows.net/stories/p/2026-01-01/run_01.json"]

    mock_fsspec = MagicMock()
    mock_fsspec.filesystem.return_value = mock_fs

    import io

    json_bytes = json.dumps(run_data)
    mock_open_ctx = MagicMock()
    mock_open_ctx.__enter__ = MagicMock(return_value=io.StringIO(json_bytes))
    mock_open_ctx.__exit__ = MagicMock(return_value=False)
    mock_fsspec.open.return_value = mock_open_ctx

    with patch.dict("sys.modules", {"fsspec": mock_fsspec}):
        result = sg._find_last_successful_run_remote()

    assert result is not None
    assert result["run_id"] == "r1"


def test_find_last_successful_run_remote_skips_failed():
    sg = StoryGenerator("p", output_path="abfss://c@a.dfs.core.windows.net/stories")

    failed_data = {"failed_nodes": 2, "run_id": "r_fail"}
    success_data = {"failed_nodes": 0, "run_id": "r_ok"}

    mock_fs = MagicMock()
    mock_fs.glob.return_value = [
        "c@a.dfs/stories/p/2026-01-02/run_01.json",
        "c@a.dfs/stories/p/2026-01-01/run_01.json",
    ]

    mock_fsspec = MagicMock()
    mock_fsspec.filesystem.return_value = mock_fs

    import io

    call_count = {"n": 0}
    data_sequence = [failed_data, success_data]

    def mock_open_fn(*args, **kwargs):
        ctx = MagicMock()
        idx = min(call_count["n"], len(data_sequence) - 1)
        ctx.__enter__ = MagicMock(return_value=io.StringIO(json.dumps(data_sequence[idx])))
        ctx.__exit__ = MagicMock(return_value=False)
        call_count["n"] += 1
        return ctx

    mock_fsspec.open.side_effect = mock_open_fn

    with patch.dict("sys.modules", {"fsspec": mock_fsspec}):
        result = sg._find_last_successful_run_remote()

    assert result is not None
    assert result["run_id"] == "r_ok"


def test_find_last_successful_run_remote_read_error():
    sg = StoryGenerator("p", output_path="abfss://c@a.dfs.core.windows.net/stories")

    success_data = {"failed_nodes": 0, "run_id": "r_ok"}

    mock_fs = MagicMock()
    mock_fs.glob.return_value = [
        "c@a.dfs/stories/p/2026-01-02/run_02.json",
        "c@a.dfs/stories/p/2026-01-01/run_01.json",
    ]

    mock_fsspec = MagicMock()
    mock_fsspec.filesystem.return_value = mock_fs

    import io

    call_count = {"n": 0}

    def mock_open_fn(*args, **kwargs):
        ctx = MagicMock()
        if call_count["n"] == 0:
            ctx.__enter__ = MagicMock(side_effect=OSError("read error"))
        else:
            ctx.__enter__ = MagicMock(return_value=io.StringIO(json.dumps(success_data)))
        ctx.__exit__ = MagicMock(return_value=False)
        call_count["n"] += 1
        return ctx

    mock_fsspec.open.side_effect = mock_open_fn

    with patch.dict("sys.modules", {"fsspec": mock_fsspec}):
        result = sg._find_last_successful_run_remote()

    assert result is not None
    assert result["run_id"] == "r_ok"


def test_find_last_successful_run_remote_fs_error():
    sg = StoryGenerator("p", output_path="abfss://c@a.dfs.core.windows.net/stories")

    mock_fsspec = MagicMock()
    mock_fsspec.filesystem.side_effect = Exception("auth failed")

    with patch.dict("sys.modules", {"fsspec": mock_fsspec}):
        result = sg._find_last_successful_run_remote()
    assert result is None


# ── _clean_config_for_dump (additional) ──────────────────────────


def test_clean_config_nested_dict_with_multiline(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    result = sg._clean_config_for_dump({"sql": "SELECT *\nFROM tbl", "name": "x"})
    assert isinstance(result["sql"], MultilineString)
    assert result["name"] == "x"


def test_clean_config_list_with_multiline(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    result = sg._clean_config_for_dump(["line1\nline2", "single"])
    assert isinstance(result[0], MultilineString)
    assert not isinstance(result[1], MultilineString)


# ── _get_git_info (additional) ───────────────────────────────────


def test_get_git_info_returns_commit_and_branch(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    with patch("subprocess.check_output", side_effect=[b"abc1234\n", b"main\n"]):
        info = sg._get_git_info()
    assert info["commit"] == "abc1234"
    assert info["branch"] == "main"


# ── get_alert_summary (additional) ───────────────────────────────


def test_alert_summary_has_story_path(tmp_path):
    sg = StoryGenerator("p", output_path=str(tmp_path / "s"))
    meta = PipelineStoryMetadata(pipeline_name="p")
    node = _make_node_meta(rows_out=50)
    meta.nodes.append(node)
    sg._last_metadata = meta
    sg._last_story_path = "/output/story.html"
    summary = sg.get_alert_summary()
    assert "story_path" in summary
    assert summary["story_path"] == "/output/story.html"
    assert "total_rows_processed" in summary


# ── generate with remote output ─────────────────────────────────


def test_generate_remote_calls_write_remote(tmp_path):
    sg = StoryGenerator("test_pipe", output_path="abfss://c@a.dfs.core.windows.net/stories")

    result = NodeResult(
        node_name="node_a",
        success=True,
        duration=1.0,
        rows_processed=50,
        metadata={},
    )

    with (
        patch.object(sg, "_write_remote") as mock_write,
        patch.object(sg, "_cleanup_remote"),
        patch.object(sg, "_generate_pipeline_index"),
        patch.object(sg, "_find_last_successful_run", return_value=None),
    ):
        sg.generate(
            node_results={"node_a": result},
            completed=["node_a"],
            failed=[],
            skipped=[],
            duration=1.0,
            start_time=datetime.now().isoformat(),
            end_time=datetime.now().isoformat(),
        )

    # Should have called _write_remote twice (HTML + JSON)
    assert mock_write.call_count == 2
    call_paths = [call.args[0] for call in mock_write.call_args_list]
    assert any(p.endswith(".html") for p in call_paths)
    assert any(p.endswith(".json") for p in call_paths)


def test_generate_remote_write_failure():
    sg = StoryGenerator("test_pipe", output_path="abfss://c@a.dfs.core.windows.net/stories")

    result = NodeResult(
        node_name="node_a",
        success=True,
        duration=1.0,
        rows_processed=50,
        metadata={},
    )

    with (
        patch.object(sg, "_write_remote", side_effect=RuntimeError("write failed")),
        patch.object(sg, "_find_last_successful_run", return_value=None),
    ):
        with pytest.raises(RuntimeError, match="write failed"):
            sg.generate(
                node_results={"node_a": result},
                completed=["node_a"],
                failed=[],
                skipped=[],
                duration=1.0,
                start_time=datetime.now().isoformat(),
                end_time=datetime.now().isoformat(),
            )


# ── _generate_docs ───────────────────────────────────────────────


def test_generate_docs_enabled(tmp_path):
    from odibi.config import DocsConfig

    docs_config = DocsConfig(enabled=True)
    sg = StoryGenerator(
        "test_pipe",
        output_path=str(tmp_path / "stories"),
        docs_config=docs_config,
        workspace_root=str(tmp_path),
    )
    meta = PipelineStoryMetadata(pipeline_name="test_pipe")
    meta.nodes.append(_make_node_meta())

    with patch("odibi.story.doc_generator.DocGenerator") as mock_doc_cls:
        mock_doc_inst = MagicMock()
        mock_doc_inst.generate.return_value = {"readme": "/path/to/readme.md"}
        mock_doc_cls.return_value = mock_doc_inst

        sg._generate_docs(meta, "/path/story.html", "/path/story.json")

    mock_doc_cls.assert_called_once()
    mock_doc_inst.generate.assert_called_once()


def test_generate_docs_raises_logs_warning(tmp_path):
    from odibi.config import DocsConfig

    docs_config = DocsConfig(enabled=True)
    sg = StoryGenerator(
        "test_pipe",
        output_path=str(tmp_path / "stories"),
        docs_config=docs_config,
        workspace_root=str(tmp_path),
    )
    meta = PipelineStoryMetadata(pipeline_name="test_pipe")

    with patch("odibi.story.doc_generator.DocGenerator") as mock_doc_cls:
        mock_doc_cls.return_value.generate.side_effect = Exception("doc gen boom")

        # Should NOT raise — just logs warning
        sg._generate_docs(meta, "/path/story.html", "/path/story.json")

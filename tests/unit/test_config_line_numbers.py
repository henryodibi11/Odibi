"""Tests for line-number-augmented validation errors in load_config_from_file."""

from __future__ import annotations

import pytest

from odibi.config import load_config_from_file
from odibi.utils.config_loader import build_yaml_line_map


# Minimum top-level scaffolding required by ProjectConfig (project, connections,
# story, system, pipelines). Pipelines line is intentionally on line 11.
_HEADER = (
    "project: my_proj\n"  # line 1
    "connections:\n"  # line 2
    "  local_fs:\n"  # line 3
    "    type: local\n"  # line 4
    "    path: /tmp\n"  # line 5
    "story:\n"  # line 6
    "  connection: local_fs\n"  # line 7
    "  path: stories\n"  # line 8
    "system:\n"  # line 9
    "  connection: local_fs\n"  # line 10
    "  path: _system\n"  # line 11
)


def _write(tmp_path, name: str, content: str) -> str:
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return str(p)


def test_build_line_map_basic():
    """build_yaml_line_map records 1-indexed positions for every node."""
    content = (
        "project: my_proj\n"  # line 1
        "connections:\n"  # line 2
        "  local_fs:\n"  # line 3
        "    type: local\n"  # line 4
        "    path: /tmp\n"  # line 5
        "pipelines:\n"  # line 6
        "  - pipeline: bad-name\n"  # line 7
        "    nodes:\n"  # line 8
        "      - name: my_node\n"  # line 9
    )
    line_map = build_yaml_line_map(content)
    assert line_map[("project",)][0] == 1
    assert line_map[("pipelines", 0, "pipeline")][0] == 7
    assert line_map[("pipelines", 0, "nodes", 0, "name")][0] == 9


def test_load_config_bad_pipeline_name_includes_line(tmp_path):
    """Pipeline name with hyphen should fail with line number in message."""
    body = (
        "pipelines:\n"  # line 12
        "  - pipeline: bad-name\n"  # line 13
        "    nodes:\n"  # line 14
        "      - name: ok_node\n"  # line 15
        "        read:\n"
        "          connection: local_fs\n"
        "          path: /data/in.parquet\n"
        "          format: parquet\n"
        "        write:\n"
        "          connection: local_fs\n"
        "          path: /data/out.parquet\n"
        "          format: parquet\n"
    )
    yaml_path = _write(tmp_path, "bad_pipe.yaml", _HEADER + body)
    with pytest.raises(ValueError) as exc_info:
        load_config_from_file(yaml_path)
    msg = str(exc_info.value)
    assert "Configuration validation failed" in msg
    assert "pipelines.0.pipeline" in msg
    assert "bad-name" in msg
    assert ":13:" in msg  # 'pipeline: bad-name' line


def test_load_config_bad_node_name_includes_line(tmp_path):
    """Node name with invalid chars should fail with line number."""
    body = (
        "pipelines:\n"  # line 12
        "  - pipeline: ok_pipe\n"  # line 13
        "    nodes:\n"  # line 14
        "      - name: bad.name\n"  # line 15
        "        read:\n"
        "          connection: local_fs\n"
        "          path: /data/in.parquet\n"
        "          format: parquet\n"
        "        write:\n"
        "          connection: local_fs\n"
        "          path: /data/out.parquet\n"
        "          format: parquet\n"
    )
    yaml_path = _write(tmp_path, "bad_node.yaml", _HEADER + body)
    with pytest.raises(ValueError) as exc_info:
        load_config_from_file(yaml_path)
    msg = str(exc_info.value)
    assert "pipelines.0.nodes.0.name" in msg
    assert "bad.name" in msg
    assert ":15:" in msg  # 'name: bad.name' line


def test_load_config_hallucinated_field_includes_line(tmp_path):
    """Hallucinated 'source:' field should fail with line number on the node."""
    body = (
        "pipelines:\n"
        "  - pipeline: ok_pipe\n"
        "    nodes:\n"
        "      - name: ok_node\n"  # line 15
        "        source:\n"  # line 16 (hallucinated)
        "          connection: local_fs\n"
        "          path: /data/in.parquet\n"
        "          format: parquet\n"
        "        write:\n"
        "          connection: local_fs\n"
        "          path: /data/out.parquet\n"
        "          format: parquet\n"
    )
    yaml_path = _write(tmp_path, "halluc.yaml", _HEADER + body)
    with pytest.raises(ValueError) as exc_info:
        load_config_from_file(yaml_path)
    msg = str(exc_info.value)
    assert "hallucinated" in msg.lower()
    # Should pinpoint somewhere on the node (line 15 or its 'source' child line 16).
    assert ":15:" in msg or ":16:" in msg


def test_load_config_unknown_connection_includes_line(tmp_path):
    """A read.connection that doesn't exist surfaces in the validator message."""
    body = (
        "pipelines:\n"
        "  - pipeline: ok_pipe\n"
        "    nodes:\n"
        "      - name: ok_node\n"
        "        read:\n"
        "          connection: missing_conn\n"
        "          path: /data/in.parquet\n"
        "          format: parquet\n"
        "        write:\n"
        "          connection: local_fs\n"
        "          path: /data/out.parquet\n"
        "          format: parquet\n"
    )
    yaml_path = _write(tmp_path, "bad_conn.yaml", _HEADER + body)
    with pytest.raises(ValueError) as exc_info:
        load_config_from_file(yaml_path)
    msg = str(exc_info.value)
    assert "missing_conn" in msg
    assert "ok_node" in msg


def test_load_config_valid_yaml_returns_config(tmp_path):
    """A fully valid config should parse cleanly without raising."""
    body = (
        "pipelines:\n"
        "  - pipeline: ok_pipe\n"
        "    nodes:\n"
        "      - name: ok_node\n"
        "        read:\n"
        "          connection: local_fs\n"
        "          path: /data/in.parquet\n"
        "          format: parquet\n"
        "        write:\n"
        "          connection: local_fs\n"
        "          path: /data/out.parquet\n"
        "          format: parquet\n"
    )
    yaml_path = _write(tmp_path, "good.yaml", _HEADER + body)
    cfg = load_config_from_file(yaml_path)
    assert cfg.project == "my_proj"
    assert cfg.pipelines[0].pipeline == "ok_pipe"

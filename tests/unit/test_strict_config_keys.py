"""P2: structured config models reject unknown keys with a did-you-mean hint.

Typos used to be silently dropped (Pydantic default extra="ignore"), so an AI
agent could misspell a key and get a green validation that does nothing at
runtime. These tests lock in the hard-fail + suggestion behavior.
"""

import pytest

from odibi.config import NodeConfig, ProjectConfig, ReadConfig, WriteConfig
from odibi.validate.pipeline import format_validation_error, validate_yaml


@pytest.mark.parametrize(
    "model,kwargs,bad_key,suggestion",
    [
        (ReadConfig, {"connection": "c", "format": "csv", "pth": "x"}, "pth", "path"),
        (
            WriteConfig,
            {"connection": "c", "format": "delta", "tabl": "t"},
            "tabl",
            "table",
        ),
        (NodeConfig, {"name": "n", "wrt": {}}, "wrt", "write"),
    ],
)
def test_unknown_key_is_rejected_with_suggestion(model, kwargs, bad_key, suggestion):
    with pytest.raises(Exception) as ei:
        model(**kwargs)
    msg = str(ei.value)
    assert bad_key in msg
    assert f"did you mean '{suggestion}'" in msg, msg


def test_known_keys_still_construct():
    # Sanity: valid keys are unaffected by the strict base.
    ReadConfig(connection="c", format="csv", path="x")
    WriteConfig(connection="c", format="delta", table="t")


def test_validate_yaml_surfaces_did_you_mean():
    yaml_content = """
project: demo
connections:
  local: {type: local, base_path: ./data}
story: {connection: local, path: _stories}
system: {connection: local}
pipelines:
  - pipeline: p
    nodes:
      - name: n
        read: {connection: local, format: csv, pth: d.csv}
        write: {connection: local, format: delta, table: out}
"""
    result = validate_yaml(yaml_content)
    assert result["valid"] is False
    blob = " ".join(e["message"] for e in result["errors"])
    assert "pth" in blob and "did you mean 'path'" in blob, blob


def test_project_level_typo_rejected():
    yaml_content = """
projct: demo
connections:
  local: {type: local, base_path: ./data}
"""
    # 'projct' typo means neither project nor connections-only path is satisfied;
    # the missing 'project' key is reported.
    result = validate_yaml(yaml_content)
    assert result["valid"] is False


def test_format_validation_error_is_actionable_and_url_free():
    # P3: the text `odibi run` shows must be field-level and free of pydantic URLs.
    from pydantic import ValidationError

    try:
        ReadConfig(connection="c", format="csv", pth="x")
    except ValidationError as e:
        text = format_validation_error(e)
    assert "errors.pydantic.dev" not in text
    assert "Configuration is invalid" in text
    assert "pth" in text and "did you mean 'path'" in text
    assert "→" in text  # a fix line is present


def test_pipeline_tags_field_accepted():
    # Pipeline-level tags were silently dropped before; now a real field.
    cfg = ProjectConfig(
        project="p",
        connections={"local": {"type": "local", "base_path": "./d"}},
        story={"connection": "local", "path": "s"},
        system={"connection": "local"},
        pipelines=[
            {
                "pipeline": "pl",
                "tags": ["daily", "critical"],
                "nodes": [
                    {
                        "name": "n",
                        "read": {"connection": "local", "format": "csv", "path": "d.csv"},
                        "write": {"connection": "local", "format": "delta", "table": "o"},
                    }
                ],
            }
        ],
    )
    assert cfg.pipelines[0].tags == ["daily", "critical"]

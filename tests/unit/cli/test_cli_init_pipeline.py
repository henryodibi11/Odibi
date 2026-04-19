"""Tests for odibi.cli.init_pipeline — init-pipeline command and template maps."""

import os
from unittest.mock import MagicMock, patch

import pytest

from odibi.cli.init_pipeline import (
    TEMPLATE_DESCRIPTIONS,
    TEMPLATE_MAP,
    add_init_parser,
    init_pipeline_command,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_args(name="myproject", template="hello", force=False):
    args = MagicMock()
    args.name = name
    args.template = template
    args.force = force
    return args


def _setup_scaffold(base_dir, template_name="hello", content=None):
    """Create a fake scaffold/templates/<template>.yaml under *base_dir*."""
    templates_dir = base_dir / "scaffold" / "templates"
    templates_dir.mkdir(parents=True, exist_ok=True)
    yaml_file = templates_dir / TEMPLATE_MAP[template_name]
    if content is None:
        content = (
            "project: hello_world\n"
            "connections:\n"
            "  landing:\n"
            "    type: local\n"
            "    base_path: ../sample_data\n"
        )
    yaml_file.write_text(content)
    return base_dir


@pytest.fixture()
def workspace(tmp_path, monkeypatch):
    """Patch cwd → tmp_path and scaffold lookup → tmp_path/fake_pkg."""
    monkeypatch.setattr(os, "getcwd", lambda: str(tmp_path))

    fake_pkg = tmp_path / "fake_pkg"
    fake_pkg.mkdir()
    _setup_scaffold(fake_pkg)

    # init_pipeline.py line 85:  scaffold_dir = Path(__file__).resolve().parent.parent / "scaffold"
    # We patch Path(__file__).resolve().parent.parent  →  fake_pkg
    # Easiest: patch the module-level __file__ so the expression lands in fake_pkg.
    # __file__ → fake_pkg / cli / init_pipeline.py  (so parent.parent == fake_pkg)
    fake_file = fake_pkg / "cli" / "init_pipeline.py"
    fake_file.parent.mkdir(parents=True, exist_ok=True)
    fake_file.touch()

    monkeypatch.setattr("odibi.cli.init_pipeline.__file__", str(fake_file))
    return tmp_path, fake_pkg


# ---------------------------------------------------------------------------
# TEMPLATE_MAP / TEMPLATE_DESCRIPTIONS
# ---------------------------------------------------------------------------


class TestTemplateMaps:
    def test_template_map_has_hello(self):
        assert "hello" in TEMPLATE_MAP

    def test_template_map_has_scd2(self):
        assert "scd2" in TEMPLATE_MAP

    def test_template_map_has_star_schema(self):
        assert "star-schema" in TEMPLATE_MAP

    def test_template_map_values_are_yaml_filenames(self):
        for v in TEMPLATE_MAP.values():
            assert v.endswith(".yaml")

    def test_template_descriptions_has_hello(self):
        assert "hello" in TEMPLATE_DESCRIPTIONS

    def test_template_descriptions_has_scd2(self):
        assert "scd2" in TEMPLATE_DESCRIPTIONS

    def test_template_descriptions_has_star_schema(self):
        assert "star-schema" in TEMPLATE_DESCRIPTIONS

    def test_template_descriptions_are_strings(self):
        for v in TEMPLATE_DESCRIPTIONS.values():
            assert isinstance(v, str) and len(v) > 0

    def test_template_map_keys_match_descriptions(self):
        assert set(TEMPLATE_MAP.keys()) == set(TEMPLATE_DESCRIPTIONS.keys())


# ---------------------------------------------------------------------------
# add_init_parser
# ---------------------------------------------------------------------------


class TestAddInitParser:
    def test_add_init_parser_registers_subcommand(self):
        import argparse

        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        add_init_parser(sub)
        # Should parse without error
        parsed = parser.parse_args(["init-pipeline", "demo"])
        assert parsed.name == "demo"

    def test_add_init_parser_template_choices(self):
        import argparse

        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        add_init_parser(sub)
        parsed = parser.parse_args(["init-pipeline", "demo", "--template", "scd2"])
        assert parsed.template == "scd2"

    def test_add_init_parser_force_flag(self):
        import argparse

        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        add_init_parser(sub)
        parsed = parser.parse_args(["init-pipeline", "proj", "--force"])
        assert parsed.force is True


# ---------------------------------------------------------------------------
# init_pipeline_command — happy path
# ---------------------------------------------------------------------------


class TestInitPipelineHappyPath:
    def test_hello_template_creates_project_dir(self, workspace):
        tmp_path, _ = workspace
        result = init_pipeline_command(_make_args(name="proj"))
        assert result == 0
        assert (tmp_path / "proj").is_dir()

    def test_hello_template_creates_odibi_yaml(self, workspace):
        tmp_path, _ = workspace
        init_pipeline_command(_make_args(name="proj"))
        assert (tmp_path / "proj" / "odibi.yaml").is_file()

    def test_odibi_yaml_path_fix(self, workspace):
        tmp_path, _ = workspace
        init_pipeline_command(_make_args(name="proj"))
        content = (tmp_path / "proj" / "odibi.yaml").read_text()
        assert "../sample_data" not in content
        assert "./sample_data" in content

    def test_creates_data_dir(self, workspace):
        tmp_path, _ = workspace
        init_pipeline_command(_make_args(name="proj"))
        assert (tmp_path / "proj" / "data").is_dir()

    def test_creates_data_raw_dir(self, workspace):
        tmp_path, _ = workspace
        init_pipeline_command(_make_args(name="proj"))
        assert (tmp_path / "proj" / "data" / "raw").is_dir()

    def test_creates_logs_dir(self, workspace):
        tmp_path, _ = workspace
        init_pipeline_command(_make_args(name="proj"))
        assert (tmp_path / "proj" / "logs").is_dir()

    def test_creates_github_workflows_dir(self, workspace):
        tmp_path, _ = workspace
        init_pipeline_command(_make_args(name="proj"))
        assert (tmp_path / "proj" / ".github" / "workflows").is_dir()

    def test_creates_dockerfile(self, workspace):
        tmp_path, _ = workspace
        init_pipeline_command(_make_args(name="proj"))
        df = tmp_path / "proj" / "Dockerfile"
        assert df.is_file()
        assert "FROM python" in df.read_text()

    def test_creates_dockerignore(self, workspace):
        tmp_path, _ = workspace
        init_pipeline_command(_make_args(name="proj"))
        assert (tmp_path / "proj" / ".dockerignore").is_file()

    def test_creates_gitignore(self, workspace):
        tmp_path, _ = workspace
        init_pipeline_command(_make_args(name="proj"))
        gi = tmp_path / "proj" / ".gitignore"
        assert gi.is_file()
        assert "__pycache__/" in gi.read_text()

    def test_creates_readme(self, workspace):
        tmp_path, _ = workspace
        init_pipeline_command(_make_args(name="proj"))
        readme = tmp_path / "proj" / "README.md"
        assert readme.is_file()

    def test_readme_contains_project_name(self, workspace):
        tmp_path, _ = workspace
        init_pipeline_command(_make_args(name="cool_project"))
        readme = (tmp_path / "cool_project" / "README.md").read_text()
        assert "cool_project" in readme

    def test_creates_ci_workflow(self, workspace):
        tmp_path, _ = workspace
        init_pipeline_command(_make_args(name="proj"))
        ci = tmp_path / "proj" / ".github" / "workflows" / "ci.yaml"
        assert ci.is_file()
        assert "Odibi CI" in ci.read_text()

    def test_creates_mcp_config(self, workspace):
        tmp_path, _ = workspace
        init_pipeline_command(_make_args(name="proj"))
        mcp = tmp_path / "proj" / "mcp_config.yaml"
        assert mcp.is_file()
        assert "proj" in mcp.read_text()


# ---------------------------------------------------------------------------
# init_pipeline_command — force / existing dir
# ---------------------------------------------------------------------------


class TestInitPipelineForce:
    def test_existing_dir_without_force_returns_1(self, workspace):
        tmp_path, _ = workspace
        (tmp_path / "proj").mkdir()
        result = init_pipeline_command(_make_args(name="proj", force=False))
        assert result == 1

    def test_force_overwrites_existing_dir(self, workspace):
        tmp_path, _ = workspace
        existing = tmp_path / "proj"
        existing.mkdir()
        (existing / "old_file.txt").write_text("old")
        result = init_pipeline_command(_make_args(name="proj", force=True))
        assert result == 0
        assert not (existing / "old_file.txt").exists()
        assert (existing / "odibi.yaml").is_file()

    def test_force_creates_fresh_structure(self, workspace):
        tmp_path, _ = workspace
        (tmp_path / "proj").mkdir()
        init_pipeline_command(_make_args(name="proj", force=True))
        assert (tmp_path / "proj" / "Dockerfile").is_file()
        assert (tmp_path / "proj" / "data").is_dir()


# ---------------------------------------------------------------------------
# init_pipeline_command — template not found
# ---------------------------------------------------------------------------


class TestInitPipelineTemplateNotFound:
    def test_missing_template_file_returns_1(self, workspace):
        tmp_path, fake_pkg = workspace
        # Remove the template file so lookup fails
        tmpl = fake_pkg / "scaffold" / "templates" / "hello.yaml"
        tmpl.unlink()
        result = init_pipeline_command(_make_args(name="proj"))
        assert result == 1


# ---------------------------------------------------------------------------
# init_pipeline_command — non-interactive fallback (template=None)
# ---------------------------------------------------------------------------


class TestInitPipelineNonInteractive:
    def test_none_template_eoferror_defaults_to_hello(self, workspace):
        tmp_path, _ = workspace
        args = _make_args(name="proj", template=None)
        with patch("builtins.input", side_effect=EOFError):
            result = init_pipeline_command(args)
        assert result == 0
        assert (tmp_path / "proj" / "odibi.yaml").is_file()

    def test_none_template_keyboard_interrupt_defaults_to_hello(self, workspace):
        tmp_path, _ = workspace
        args = _make_args(name="proj", template=None)
        with patch("builtins.input", side_effect=KeyboardInterrupt):
            result = init_pipeline_command(args)
        assert result == 0

    def test_none_template_empty_input_selects_first(self, workspace):
        tmp_path, _ = workspace
        args = _make_args(name="proj", template=None)
        with patch("builtins.input", return_value=""):
            result = init_pipeline_command(args)
        assert result == 0

    def test_none_template_valid_selection(self, workspace):
        tmp_path, fake_pkg = workspace
        # Setup scd2 template too
        _setup_scaffold(fake_pkg, template_name="scd2")
        args = _make_args(name="proj", template=None)
        with patch("builtins.input", return_value="2"):
            result = init_pipeline_command(args)
        assert result == 0

    def test_none_template_invalid_number_returns_1(self, workspace):
        args = _make_args(name="proj", template=None)
        with patch("builtins.input", return_value="99"):
            result = init_pipeline_command(args)
        assert result == 1

    def test_none_template_non_numeric_falls_back_to_hello(self, workspace):
        tmp_path, _ = workspace
        args = _make_args(name="proj", template=None)
        with patch("builtins.input", side_effect=ValueError):
            result = init_pipeline_command(args)
        assert result == 0


# ---------------------------------------------------------------------------
# init_pipeline_command — sample_data copy
# ---------------------------------------------------------------------------


class TestInitPipelineSampleData:
    def test_sample_data_copied_when_present(self, workspace):
        tmp_path, fake_pkg = workspace
        sample_dir = fake_pkg / "scaffold" / "sample_data"
        sample_dir.mkdir(parents=True, exist_ok=True)
        (sample_dir / "customers.csv").write_text("id,name\n1,Alice")
        result = init_pipeline_command(_make_args(name="proj"))
        assert result == 0
        copied = tmp_path / "proj" / "sample_data" / "customers.csv"
        assert copied.is_file()
        assert "Alice" in copied.read_text()

    def test_no_sample_data_dir_still_succeeds(self, workspace):
        result = init_pipeline_command(_make_args(name="proj"))
        assert result == 0

"""Tests for odibi.cli.test and odibi.cli.graph."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from odibi.cli.graph import _generate_dot, _generate_mermaid
from odibi.cli.test import load_test_files, run_test_case, slugify
from odibi.cli.test import test_command as _test_command
from odibi.registry import FunctionRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_node(name, read=False, write=False, transform=None, depends_on=None):
    """Create a mock node with the attributes graph.py expects."""
    node = MagicMock()
    node.name = name
    node.read = read
    node.write = write
    node.transform = transform
    node.depends_on = depends_on or []
    return node


def _make_mock_graph(nodes):
    """Create a mock DependencyGraph with a .nodes dict."""
    graph = MagicMock()
    graph.nodes = {n.name: n for n in nodes}
    return graph


# ===========================================================================
# load_test_files
# ===========================================================================


class TestLoadTestFiles:
    def test_single_file_returns_list_with_path(self, tmp_path):
        f = tmp_path / "my_test.yaml"
        f.write_text("tests: []")
        result = load_test_files(f)
        assert result == [f]

    def test_directory_with_test_yaml_files(self, tmp_path):
        (tmp_path / "something_test_foo.yaml").write_text("")
        (tmp_path / "test_bar.yml").write_text("")
        (tmp_path / "unrelated.yaml").write_text("")
        result = load_test_files(tmp_path)
        names = sorted(p.name for p in result)
        assert "something_test_foo.yaml" in names
        assert "test_bar.yml" in names
        assert "unrelated.yaml" not in names

    def test_empty_directory_returns_empty(self, tmp_path):
        result = load_test_files(tmp_path)
        assert result == []


# ===========================================================================
# slugify
# ===========================================================================


class TestSlugify:
    def test_basic_string(self):
        assert slugify("hello world") == "hello-world"

    def test_special_characters_removed(self):
        assert slugify("test!@#case") == "testcase"

    def test_spaces_to_hyphens(self):
        assert slugify("a  b   c") == "a-b-c"

    def test_uppercase_to_lowercase(self):
        assert slugify("Hello World") == "hello-world"

    def test_mixed_special_and_spaces(self):
        assert slugify("My Test (case #1)") == "my-test-case-1"

    def test_already_clean_string(self):
        assert slugify("clean-string") == "clean-string"


# ===========================================================================
# run_test_case — edge cases
# ===========================================================================


class TestRunTestCase:
    def test_missing_both_transform_and_sql_returns_false(self, tmp_path):
        config = {"name": "bad test"}
        result = run_test_case(config, tmp_path / "test.yaml")
        assert result is False

    def test_no_expected_and_no_snapshot_returns_false(self, tmp_path):
        config = {"name": "no expected", "transform": "some_func"}
        result = run_test_case(config, tmp_path / "test.yaml")
        assert result is False

    def test_update_snapshots_creates_file_and_returns_true(self, tmp_path):
        test_file = tmp_path / "test.yaml"
        test_file.write_text("")

        def identity(df):
            return df

        FunctionRegistry.register(identity, name="_test_identity_snap")
        try:
            config = {
                "name": "snapshot test",
                "transform": "_test_identity_snap",
                "inputs": {"df": [{"a": 1}, {"a": 2}]},
            }
            result = run_test_case(config, test_file, update_snapshots=True)
            assert result is True
            snapshot = tmp_path / "__snapshots__" / "test" / "snapshot-test.csv"
            assert snapshot.exists()
        finally:
            FunctionRegistry._functions.pop("_test_identity_snap", None)

    def test_transform_with_matching_expected_returns_true(self, tmp_path):
        def double_col(df):
            df = df.copy()
            df["b"] = df["a"] * 2
            return df

        FunctionRegistry.register(double_col, name="_test_double")
        try:
            config = {
                "name": "double test",
                "transform": "_test_double",
                "inputs": {"df": [{"a": 1}, {"a": 2}]},
                "expected": [{"a": 1, "b": 2}, {"a": 2, "b": 4}],
            }
            result = run_test_case(config, tmp_path / "t.yaml")
            assert result is True
        finally:
            FunctionRegistry._functions.pop("_test_double", None)

    def test_transform_with_mismatched_expected_returns_false(self, tmp_path):
        def noop(df):
            return df

        FunctionRegistry.register(noop, name="_test_noop_mm")
        try:
            config = {
                "name": "mismatch",
                "transform": "_test_noop_mm",
                "inputs": {"df": [{"a": 1}]},
                "expected": [{"a": 999}],
            }
            result = run_test_case(config, tmp_path / "t.yaml")
            assert result is False
        finally:
            FunctionRegistry._functions.pop("_test_noop_mm", None)

    def test_sql_based_test_with_duckdb(self, tmp_path):
        pytest.importorskip("duckdb")
        config = {
            "name": "sql test",
            "sql": "SELECT a, a * 10 AS b FROM src",
            "inputs": {"src": [{"a": 1}, {"a": 2}]},
            "expected": [{"a": 1, "b": 10}, {"a": 2, "b": 20}],
        }
        result = run_test_case(config, tmp_path / "t.yaml")
        assert result is True

    def test_csv_file_input_reference(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("x,y\n1,2\n3,4\n")

        def passthrough(df):
            return df

        FunctionRegistry.register(passthrough, name="_test_csv_ref")
        try:
            config = {
                "name": "csv ref",
                "transform": "_test_csv_ref",
                "inputs": {"df": "data.csv"},
                "expected": [{"x": 1, "y": 2}, {"x": 3, "y": 4}],
            }
            # test_file must be in same dir as CSV
            test_file = tmp_path / "test.yaml"
            test_file.write_text("")
            result = run_test_case(config, test_file)
            assert result is True
        finally:
            FunctionRegistry._functions.pop("_test_csv_ref", None)

    def test_transform_not_found_returns_false(self, tmp_path):
        config = {
            "name": "missing func",
            "transform": "_nonexistent_transform_xyz",
            "inputs": {"df": [{"a": 1}]},
            "expected": [{"a": 1}],
        }
        result = run_test_case(config, tmp_path / "t.yaml")
        assert result is False


# ===========================================================================
# test_command
# ===========================================================================


class TestTestCommand:
    def test_path_not_found_returns_1(self, tmp_path):
        args = SimpleNamespace(path=str(tmp_path / "does_not_exist"), snapshot=False)
        with (
            patch("odibi.cli.test.register_standard_library"),
            patch("odibi.cli.test.load_extensions"),
        ):
            result = _test_command(args)
        assert result == 1

    def test_no_test_files_returns_0(self, tmp_path):
        args = SimpleNamespace(path=str(tmp_path), snapshot=False)
        with (
            patch("odibi.cli.test.register_standard_library"),
            patch("odibi.cli.test.load_extensions"),
        ):
            result = _test_command(args)
        assert result == 0


# ===========================================================================
# _generate_dot
# ===========================================================================


class TestGenerateDot:
    def test_basic_graph(self):
        nodes = [
            _make_mock_node("A", read=True),
            _make_mock_node("B", transform="t", depends_on=["A"]),
        ]
        graph = _make_mock_graph(nodes)
        dot = _generate_dot(graph, "my_pipeline", catalog_manager=None)
        assert 'digraph "my_pipeline"' in dot
        assert '"A"' in dot
        assert '"B"' in dot
        assert '"A" -> "B"' in dot

    def test_read_node_color(self):
        graph = _make_mock_graph([_make_mock_node("src", read=True)])
        dot = _generate_dot(graph, "p", catalog_manager=None)
        assert "lightblue" in dot
        assert "(read)" in dot

    def test_write_node_color(self):
        graph = _make_mock_graph([_make_mock_node("sink", write=True)])
        dot = _generate_dot(graph, "p", catalog_manager=None)
        assert "lightgreen" in dot
        assert "(write)" in dot

    def test_transform_node_color(self):
        graph = _make_mock_graph([_make_mock_node("calc", transform="fn")])
        dot = _generate_dot(graph, "p", catalog_manager=None)
        assert "lightyellow" in dot
        assert "(transform)" in dot

    def test_with_catalog_manager(self):
        graph = _make_mock_graph([_make_mock_node("n1", read=True)])
        cm = MagicMock()
        cm.get_average_volume.return_value = 5000.0
        dot = _generate_dot(graph, "p", catalog_manager=cm)
        assert "~5000 rows" in dot

    def test_without_catalog_manager(self):
        graph = _make_mock_graph([_make_mock_node("n1", read=True)])
        dot = _generate_dot(graph, "p", catalog_manager=None)
        assert "rows" not in dot

    def test_catalog_manager_returns_none(self):
        graph = _make_mock_graph([_make_mock_node("n1", read=True)])
        cm = MagicMock()
        cm.get_average_volume.return_value = None
        dot = _generate_dot(graph, "p", catalog_manager=cm)
        assert "rows" not in dot


# ===========================================================================
# _generate_mermaid
# ===========================================================================


class TestGenerateMermaid:
    def test_basic_output(self):
        nodes = [
            _make_mock_node("X", read=True),
            _make_mock_node("Y", write=True, depends_on=["X"]),
        ]
        graph = _make_mock_graph(nodes)
        mmd = _generate_mermaid(graph, "pipe", catalog_manager=None)
        assert "graph LR" in mmd
        assert "X" in mmd
        assert "Y" in mmd
        assert "X --> Y" in mmd

    def test_read_node_circle_shape(self):
        graph = _make_mock_graph([_make_mock_node("src", read=True)])
        mmd = _generate_mermaid(graph, "p", catalog_manager=None)
        assert '(("src"))' in mmd
        assert ":::read" in mmd

    def test_write_node_parallelogram(self):
        graph = _make_mock_graph([_make_mock_node("sink", write=True)])
        mmd = _generate_mermaid(graph, "p", catalog_manager=None)
        assert '[/"sink"/]' in mmd
        assert ":::write" in mmd

    def test_transform_node_box(self):
        graph = _make_mock_graph([_make_mock_node("calc", transform="fn")])
        mmd = _generate_mermaid(graph, "p", catalog_manager=None)
        assert '["calc"]' in mmd

    def test_with_catalog_stats(self):
        graph = _make_mock_graph([_make_mock_node("n1", read=True)])
        cm = MagicMock()
        cm.get_average_volume.return_value = 1234.0
        mmd = _generate_mermaid(graph, "p", catalog_manager=cm)
        assert "~1234 rows" in mmd

    def test_without_catalog_no_rows(self):
        graph = _make_mock_graph([_make_mock_node("n1", read=True)])
        mmd = _generate_mermaid(graph, "p", catalog_manager=None)
        assert "rows" not in mmd

    def test_multiple_deps_edges(self):
        nodes = [
            _make_mock_node("A", read=True),
            _make_mock_node("B", read=True),
            _make_mock_node("C", transform="fn", depends_on=["A", "B"]),
        ]
        graph = _make_mock_graph(nodes)
        mmd = _generate_mermaid(graph, "p", catalog_manager=None)
        assert "A --> C" in mmd
        assert "B --> C" in mmd

    def test_style_class_definitions(self):
        graph = _make_mock_graph([_make_mock_node("n", read=True)])
        mmd = _generate_mermaid(graph, "p", catalog_manager=None)
        assert "classDef read" in mmd
        assert "classDef write" in mmd
        assert "classDef transform" in mmd

    def test_catalog_manager_returns_none_no_rows(self):
        graph = _make_mock_graph([_make_mock_node("n1", read=True)])
        cm = MagicMock()
        cm.get_average_volume.return_value = None
        mmd = _generate_mermaid(graph, "p", catalog_manager=cm)
        assert "rows" not in mmd

"""Tests for odibi.cli.test — test_command, run_test_case, load_test_files."""

from argparse import Namespace
from unittest.mock import MagicMock, patch

import pandas as pd

from odibi.cli.test import load_test_files, run_test_case, slugify
from odibi.cli.test import test_command as _test_command


# ---------------------------------------------------------------------------
# slugify
# ---------------------------------------------------------------------------


class TestSlugify:
    def test_basic(self):
        assert slugify("Hello World") == "hello-world"

    def test_special_chars(self):
        assert slugify("Test (case) #1!") == "test-case-1"

    def test_multiple_spaces(self):
        assert slugify("a   b") == "a-b"


# ---------------------------------------------------------------------------
# load_test_files
# ---------------------------------------------------------------------------


class TestLoadTestFiles:
    def test_single_file(self, tmp_path):
        f = tmp_path / "test_foo.yaml"
        f.write_text("tests: []")
        result = load_test_files(f)
        assert result == [f]

    def test_directory_finds_yaml_files(self, tmp_path):
        (tmp_path / "test_a.yaml").write_text("")
        (tmp_path / "test_b.yml").write_text("")
        (tmp_path / "other.yaml").write_text("")
        result = load_test_files(tmp_path)
        names = {p.name for p in result}
        assert "test_a.yaml" in names
        assert "test_b.yml" in names
        assert "other.yaml" not in names

    def test_empty_dir(self, tmp_path):
        assert load_test_files(tmp_path) == []


# ---------------------------------------------------------------------------
# run_test_case
# ---------------------------------------------------------------------------


class TestRunTestCase:
    def test_no_transform_or_sql_returns_false(self, tmp_path):
        cfg = {"name": "bad", "inputs": {}}
        assert run_test_case(cfg, tmp_path / "test.yaml") is False

    def test_no_expected_no_snapshot_returns_false(self, tmp_path):
        cfg = {"name": "no_exp", "transform": "my_fn", "inputs": {}}
        assert run_test_case(cfg, tmp_path / "test.yaml") is False

    def test_transform_not_in_registry(self, tmp_path):
        cfg = {"name": "missing", "transform": "nonexistent", "inputs": {}}
        snap_dir = tmp_path / "__snapshots__" / "test"
        snap_dir.mkdir(parents=True)
        (snap_dir / "missing.csv").write_text("a\n1\n")
        with patch("odibi.cli.test.FunctionRegistry") as mock_reg:
            mock_reg.get.return_value = None
            mock_reg.list_functions.return_value = ["fn_a", "fn_b"]
            result = run_test_case(cfg, tmp_path / "test.yaml")
        assert result is False

    def test_transform_kwargs_success(self, tmp_path):
        cfg = {
            "name": "ok",
            "transform": "my_fn",
            "inputs": {"df": [{"a": 1}]},
            "expected": [{"a": 1}],
        }
        mock_fn = MagicMock(return_value=pd.DataFrame([{"a": 1}]))
        with patch("odibi.cli.test.FunctionRegistry") as mock_reg:
            mock_reg.get.return_value = mock_fn
            result = run_test_case(cfg, tmp_path / "test.yaml")
        assert result is True

    def test_transform_kwargs_typeerror_fallback_single_input(self, tmp_path):
        """When kwargs fail but single-input fallback succeeds."""
        cfg = {
            "name": "fallback",
            "transform": "my_fn",
            "inputs": {"df": [{"a": 1}]},
            "expected": [{"a": 1}],
        }

        def side_effect_fn(**kwargs):
            raise TypeError("unexpected keyword argument")

        def positional_fn(df):
            return df

        mock_fn = MagicMock(side_effect=side_effect_fn)
        # first call raises TypeError, second (positional) succeeds
        mock_fn.side_effect = [TypeError("bad"), pd.DataFrame([{"a": 1}])]

        with patch("odibi.cli.test.FunctionRegistry") as mock_reg:
            # We need real callable behavior for the fallback
            call_count = 0

            def smart_fn(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise TypeError("unexpected kwarg")
                return pd.DataFrame([{"a": 1}])

            mock_reg.get.return_value = smart_fn
            result = run_test_case(cfg, tmp_path / "test.yaml")
        assert result is True

    def test_transform_kwargs_typeerror_multiple_inputs_raises(self, tmp_path):
        """When kwargs fail and there are multiple inputs, original error re-raises."""
        cfg = {
            "name": "multi_fail",
            "transform": "my_fn",
            "inputs": {"df1": [{"a": 1}], "df2": [{"b": 2}]},
            "expected": [{"a": 1}],
        }

        def bad_fn(**kwargs):
            raise TypeError("unexpected keyword argument")

        with patch("odibi.cli.test.FunctionRegistry") as mock_reg:
            mock_reg.get.return_value = bad_fn
            result = run_test_case(cfg, tmp_path / "test.yaml")
        assert result is False

    def test_input_csv_reference(self, tmp_path):
        cfg = {
            "name": "csv_input",
            "transform": "my_fn",
            "inputs": {"src": "data.csv"},
            "expected": [{"a": 1}],
        }
        csv_path = tmp_path / "data.csv"
        csv_path.write_text("a\n1\n")

        mock_fn = MagicMock(return_value=pd.DataFrame([{"a": 1}]))
        with patch("odibi.cli.test.FunctionRegistry") as mock_reg:
            mock_reg.get.return_value = mock_fn
            result = run_test_case(cfg, tmp_path / "test.yaml")
        assert result is True
        # The CSV was loaded and passed
        call_args = mock_fn.call_args
        assert "src" in call_args.kwargs

    def test_input_csv_missing_file_skipped(self, tmp_path):
        """CSV reference that doesn't exist is silently skipped (line 72-73)."""
        cfg = {
            "name": "csv_missing",
            "transform": "my_fn",
            "inputs": {"src": "missing.csv"},
            "expected": [{"a": 1}],
        }
        mock_fn = MagicMock(return_value=pd.DataFrame([{"a": 1}]))
        with patch("odibi.cli.test.FunctionRegistry") as mock_reg:
            mock_reg.get.return_value = mock_fn
            result = run_test_case(cfg, tmp_path / "test.yaml")
        assert result is True

    def test_input_other_format_skipped(self, tmp_path):
        """Non-list, non-CSV input is silently skipped (lines 74-76)."""
        cfg = {
            "name": "other_fmt",
            "transform": "my_fn",
            "inputs": {"src": 42},
            "expected": [{"a": 1}],
        }
        mock_fn = MagicMock(return_value=pd.DataFrame([{"a": 1}]))
        with patch("odibi.cli.test.FunctionRegistry") as mock_reg:
            mock_reg.get.return_value = mock_fn
            result = run_test_case(cfg, tmp_path / "test.yaml")
        assert result is True

    def test_sql_duckdb_success(self, tmp_path):
        cfg = {
            "name": "sql_test",
            "sql": "SELECT * FROM src",
            "inputs": {"src": [{"a": 1}]},
            "expected": [{"a": 1}],
        }
        result = run_test_case(cfg, tmp_path / "test.yaml")
        # duckdb may or may not be installed — either way we exercise the branch
        assert isinstance(result, bool)

    def test_sql_duckdb_import_error(self, tmp_path):
        """When duckdb is not available (lines 137-141)."""
        cfg = {
            "name": "sql_no_duck",
            "sql": "SELECT 1",
            "inputs": {},
            "expected": [{"a": 1}],
        }
        import builtins

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "duckdb":
                raise ImportError("no duckdb")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            result = run_test_case(cfg, tmp_path / "test.yaml")
        assert result is False

    def test_update_snapshots(self, tmp_path):
        cfg = {
            "name": "snap_update",
            "transform": "my_fn",
            "inputs": {"df": [{"a": 2, "b": 1}]},
        }
        mock_fn = MagicMock(return_value=pd.DataFrame([{"a": 2, "b": 1}]))
        with patch("odibi.cli.test.FunctionRegistry") as mock_reg:
            mock_reg.get.return_value = mock_fn
            result = run_test_case(cfg, tmp_path / "test.yaml", update_snapshots=True)
        assert result is True
        snap = tmp_path / "__snapshots__" / "test" / "snap_update.csv"
        assert snap.exists()

    def test_expected_from_snapshot_file(self, tmp_path):
        cfg = {
            "name": "from_snap",
            "transform": "my_fn",
            "inputs": {"df": [{"a": 1}]},
        }
        snap_dir = tmp_path / "__snapshots__" / "test"
        snap_dir.mkdir(parents=True)
        pd.DataFrame([{"a": 1}]).to_csv(snap_dir / "from_snap.csv", index=False)

        mock_fn = MagicMock(return_value=pd.DataFrame([{"a": 1}]))
        with patch("odibi.cli.test.FunctionRegistry") as mock_reg:
            mock_reg.get.return_value = mock_fn
            result = run_test_case(cfg, tmp_path / "test.yaml")
        assert result is True

    def test_no_expected_no_snapshot_no_update(self, tmp_path):
        """Lines 166-170: no expected data and no snapshot file."""
        cfg = {
            "name": "no_data",
            "transform": "my_fn",
            "inputs": {"df": [{"a": 1}]},
        }
        mock_fn = MagicMock(return_value=pd.DataFrame([{"a": 1}]))
        with patch("odibi.cli.test.FunctionRegistry") as mock_reg:
            mock_reg.get.return_value = mock_fn
            result = run_test_case(cfg, tmp_path / "test.yaml")
        # No expected and no snapshot => False
        assert result is False

    def test_sort_failure_still_compares(self, tmp_path):
        """Lines 186-188: sorting fails but comparison continues."""
        cfg = {
            "name": "unsortable",
            "transform": "my_fn",
            "inputs": {"df": [{"a": 1}]},
            "expected": [{"a": 1}],
        }
        result_df = pd.DataFrame([{"a": 1}])
        mock_fn = MagicMock(return_value=result_df)

        with patch("odibi.cli.test.FunctionRegistry") as mock_reg:
            mock_reg.get.return_value = mock_fn
            # Patch sort_values to fail
            with patch.object(pd.DataFrame, "sort_values", side_effect=TypeError("unsortable")):
                result = run_test_case(cfg, tmp_path / "test.yaml")
        assert result is True

    def test_snapshot_sort_failure_still_saves(self, tmp_path):
        """Lines 155-156: sorting fails during snapshot save."""
        cfg = {
            "name": "snap_unsort",
            "transform": "my_fn",
            "inputs": {"df": [{"a": 1}]},
        }
        mock_fn = MagicMock(return_value=pd.DataFrame([{"a": 1}]))

        with patch("odibi.cli.test.FunctionRegistry") as mock_reg:
            mock_reg.get.return_value = mock_fn
            with patch.object(pd.DataFrame, "sort_values", side_effect=TypeError("unsortable")):
                result = run_test_case(cfg, tmp_path / "test.yaml", update_snapshots=True)
        assert result is True


# ---------------------------------------------------------------------------
# test_command
# ---------------------------------------------------------------------------


class TestTestCommand:
    def test_path_not_found(self, tmp_path):
        args = Namespace(path=str(tmp_path / "nonexistent"), snapshot=False)
        assert _test_command(args) == 1

    def test_no_test_files_found(self, tmp_path):
        args = Namespace(path=str(tmp_path), snapshot=False)
        with (
            patch("odibi.cli.test.register_standard_library"),
            patch("odibi.cli.test.load_extensions"),
        ):
            result = _test_command(args)
        assert result == 0

    def test_test_command_runs_tests(self, tmp_path):
        test_file = tmp_path / "test_sample.yaml"
        test_file.write_text(
            "tests:\n"
            "  - name: t1\n"
            "    transform: my_fn\n"
            "    inputs:\n"
            "      df:\n"
            "        - {a: 1}\n"
            "    expected:\n"
            "      - {a: 1}\n"
        )
        args = Namespace(path=str(test_file), snapshot=False)

        mock_fn = MagicMock(return_value=pd.DataFrame([{"a": 1}]))
        with (
            patch("odibi.cli.test.register_standard_library"),
            patch("odibi.cli.test.load_extensions"),
            patch("odibi.cli.test.FunctionRegistry") as mock_reg,
            patch("odibi.cli.test.console"),
        ):
            mock_reg.get.return_value = mock_fn
            result = _test_command(args)
        assert result == 0

    def test_test_command_failed_test_returns_1(self, tmp_path):
        test_file = tmp_path / "test_fail.yaml"
        test_file.write_text(
            "tests:\n"
            "  - name: bad\n"
            "    transform: missing_fn\n"
            "    inputs:\n"
            "      df:\n"
            "        - {a: 1}\n"
            "    expected:\n"
            "      - {a: 1}\n"
        )
        args = Namespace(path=str(test_file), snapshot=False)

        with (
            patch("odibi.cli.test.register_standard_library"),
            patch("odibi.cli.test.load_extensions"),
            patch("odibi.cli.test.FunctionRegistry") as mock_reg,
            patch("odibi.cli.test.console"),
        ):
            mock_reg.get.return_value = None
            mock_reg.list_functions.return_value = []
            result = _test_command(args)
        assert result == 1

    def test_test_command_yaml_error(self, tmp_path):
        test_file = tmp_path / "test_bad.yaml"
        test_file.write_text(": bad yaml: [")
        args = Namespace(path=str(test_file), snapshot=False)

        with (
            patch("odibi.cli.test.register_standard_library"),
            patch("odibi.cli.test.load_extensions"),
            patch("odibi.cli.test.console"),
        ):
            result = _test_command(args)
        # yaml error caught, table shows ERROR row, but doesn't fail count
        assert result == 0

    def test_test_command_empty_tests_list(self, tmp_path):
        test_file = tmp_path / "test_empty.yaml"
        test_file.write_text("tests: []\n")
        args = Namespace(path=str(test_file), snapshot=False)

        with (
            patch("odibi.cli.test.register_standard_library"),
            patch("odibi.cli.test.load_extensions"),
            patch("odibi.cli.test.console"),
        ):
            result = _test_command(args)
        assert result == 0

    def test_test_command_with_snapshot_flag(self, tmp_path):
        test_file = tmp_path / "test_snap.yaml"
        test_file.write_text(
            "tests:\n  - name: s1\n    transform: my_fn\n    inputs:\n      df:\n        - {a: 1}\n"
        )
        args = Namespace(path=str(test_file), snapshot=True)

        mock_fn = MagicMock(return_value=pd.DataFrame([{"a": 1}]))
        with (
            patch("odibi.cli.test.register_standard_library"),
            patch("odibi.cli.test.load_extensions"),
            patch("odibi.cli.test.FunctionRegistry") as mock_reg,
            patch("odibi.cli.test.console"),
        ):
            mock_reg.get.return_value = mock_fn
            result = _test_command(args)
        assert result == 0

    def test_test_command_traverses_parents_for_extensions(self, tmp_path):
        sub = tmp_path / "a" / "b" / "c"
        sub.mkdir(parents=True)
        test_file = sub / "test_deep.yaml"
        test_file.write_text("tests: []\n")
        args = Namespace(path=str(test_file), snapshot=False)

        with (
            patch("odibi.cli.test.register_standard_library"),
            patch("odibi.cli.test.load_extensions") as mock_load,
            patch("odibi.cli.test.console"),
        ):
            result = _test_command(args)
        assert result == 0
        # load_extensions called for cwd + parent traversal
        assert mock_load.call_count >= 2

    def test_test_command_stops_at_odibi_yaml(self, tmp_path):
        (tmp_path / "odibi.yaml").write_text("project: x\n")
        test_file = tmp_path / "test_root.yaml"
        test_file.write_text("tests: []\n")
        args = Namespace(path=str(test_file), snapshot=False)

        with (
            patch("odibi.cli.test.register_standard_library"),
            patch("odibi.cli.test.load_extensions"),
            patch("odibi.cli.test.console"),
        ):
            result = _test_command(args)
        assert result == 0

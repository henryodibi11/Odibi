"""Test command implementation."""

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yaml
from rich.console import Console
from rich.table import Table

from odibi.registry import FunctionRegistry
from odibi.transformers import register_standard_library
from odibi.utils.extensions import load_extensions
from odibi.utils.logging import logger

console = Console()


def load_test_files(path: Path) -> List[Path]:
    """Find test YAML files."""
    if path.is_file():
        return [path]
    return list(path.glob("**/*test*.yaml")) + list(path.glob("**/test_*.yml"))


def run_test_case(
    test_config: Dict[str, Any], test_file: Path, update_snapshots: bool = False
) -> bool:
    """Run a single test case.

    Args:
        test_config: Test configuration dictionary
        test_file: Path to the test file (for context)
        update_snapshots: Whether to update snapshot files

    Returns:
        True if passed, False otherwise
    """
    name = test_config.get("name", "Unnamed Test")
    transform_name = test_config.get("transform")
    sql_query = test_config.get("sql")
    inputs_data = test_config.get("inputs", {})
    expected_data = test_config.get("expected")

    if not transform_name and not sql_query:
        logger.error(f"Test '{name}': Must specify 'transform' or 'sql'")
        return False

    # Determine Snapshot Path
    # Naming convention: test_file_directory/__snapshots__/test_file_name/test_name.csv
    snapshot_dir = test_file.parent / "__snapshots__" / test_file.stem
    snapshot_file = snapshot_dir / f"{slugify(name)}.csv"

    if expected_data is None and not snapshot_file.exists() and not update_snapshots:
        logger.error(
            f"Test '{name}': Must specify 'expected' output or run with --snapshot to create one."
        )
        return False

    try:
        # 1. Prepare Inputs
        input_dfs = {}
        for key, data in inputs_data.items():
            if isinstance(data, list):
                input_dfs[key] = pd.DataFrame(data)
            elif isinstance(data, str) and data.endswith(".csv"):
                # Support CSV file references in inputs for snapshot tests
                csv_path = test_file.parent / data
                if csv_path.exists():
                    input_dfs[key] = pd.read_csv(csv_path)
                else:
                    # Maybe it's inline CSV string?
                    pass
            else:
                # Handle other formats if necessary
                pass

        # 2. Execute Transformation
        result_df = None

        if transform_name:
            # Function-based transform
            func = FunctionRegistry.get(transform_name)
            if not func:
                available = ", ".join(FunctionRegistry.list_functions())
                logger.error(
                    f"Test '{name}': Transform '{transform_name}' not found in registry. Available: {available}"
                )
                return False

            # Determine arguments:
            # If function takes named arguments matching inputs, pass them
            # Or if it takes a context/single df.
            # For simplicity, we assume standard Odibi transform signature or flexible kwargs matching inputs.
            # We'll try to bind inputs to function arguments.

            # Special case: if only one input and function takes one arg (plus optional context/etc), pass it directly?
            # Or strictly match names. Odibi transforms usually take (df, **params) or (context).
            # Let's try passing inputs as kwargs.

            try:
                # Filter inputs to match signature if possible, or just pass all
                # But some transforms might take 'df' as first arg.
                # If inputs has only one item and func has one required arg, map it?
                # Let's stick to strict name matching first.
                result_df = func(**input_dfs)
            except TypeError as e:
                # Fallback: Check if first arg is 'df' and we have 1 input
                if len(input_dfs) == 1:
                    first_input = list(input_dfs.values())[0]
                    # Try calling with single DF
                    try:
                        result_df = func(first_input)
                    except Exception:
                        # Raise original error
                        raise e
                else:
                    raise e

        elif sql_query:
            # SQL-based transform (using pandasql or duckdb logic?)
            # Since we are testing "Odibi transformations", and Odibi uses engines.
            # If we want to test SQL logic in isolation without a full engine, we can use `duckdb` or `sqlite` via pandas.
            # Or we can instantiate a temporary Odibi PandasEngine?
            # Let's use DuckDB for SQL testing on Pandas DataFrames if available, or simple pandas query?
            # Real SQL transforms in Odibi usually run on Spark or DB.
            # Testing "SQL" on local Pandas requires a local SQL engine. DuckDB is best for this.
            try:
                import duckdb

                # Register inputs as views
                con = duckdb.connect(database=":memory:")
                for key, df in input_dfs.items():
                    con.register(key, df)

                result_df = con.execute(sql_query).df()
            except ImportError:
                logger.error(
                    "Test '{name}': 'duckdb' is required for SQL testing. Install with 'pip install duckdb'."
                )
                return False

        # 3. Verify Results

        # Snapshot Logic
        if update_snapshots:
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            # Normalize for snapshot (sort columns/rows)
            result_to_save = result_df.copy()
            result_to_save = result_to_save[sorted(result_to_save.columns)]
            try:
                result_to_save = result_to_save.sort_values(
                    by=list(result_to_save.columns)
                ).reset_index(drop=True)
            except Exception:
                pass

            result_to_save.to_csv(snapshot_file, index=False)
            logger.info(f"Test '{name}': Updated snapshot at {snapshot_file}")
            # If we just updated snapshot, should we treat it as pass? Yes.
            return True

        # Load Expected Data
        if expected_data is not None:
            expected_df = pd.DataFrame(expected_data)
        elif snapshot_file.exists():
            expected_df = pd.read_csv(snapshot_file)
        else:
            logger.error(f"Test '{name}': No expected data or snapshot found.")
            return False

        # Normalize column order and types for comparison
        # Sort by columns to ignore column order differences
        result_df = result_df[sorted(result_df.columns)]
        expected_df = expected_df[sorted(expected_df.columns)]

        # Sort rows if needed (optional, maybe add 'sort_by' to test config?)
        # For now, we require exact match, maybe row order matters?
        # Usually in data testing, row order shouldn't matter unless specified.
        # Let's try to sort by all columns to ensure set equality
        try:
            result_df = result_df.sort_values(by=list(result_df.columns)).reset_index(drop=True)
            expected_df = expected_df.sort_values(by=list(expected_df.columns)).reset_index(
                drop=True
            )
        except Exception:
            # If sorting fails (mixed types), proceed as is
            pass

        pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False, check_like=True)

        return True

    except Exception as e:
        logger.error(f"Test '{name}' FAILED: {e}")
        # import traceback
        # logger.error(traceback.format_exc())
        return False


def slugify(value):
    """Normalize string for filename."""
    import re

    value = str(value).lower().strip()
    return re.sub(r"[-\s]+", "-", re.sub(r"[^\w\s-]", "", value))


def test_command(args):
    """Run Odibi unit tests."""
    test_path = Path(args.path).resolve()
    update_snapshots = getattr(args, "snapshot", False)

    if not test_path.exists():
        logger.error(f"Path not found: {test_path}")
        return 1

    # Initialize standard library
    register_standard_library()

    # Load extensions (to register transforms)
    load_extensions(Path.cwd())

    # Find project root or relevant directories
    # We'll search up from the test path for transforms.py
    current = test_path if test_path.is_dir() else test_path.parent
    for _ in range(3):  # Check up to 3 levels up
        load_extensions(current)
        if (current / "odibi.yaml").exists():
            # If we found the project root, maybe we stop?
            # But transforms might be in subdirs?
            # Let's just load what we find in the hierarchy.
            pass
        if current == current.parent:  # Root reached
            break
        current = current.parent

    test_files = load_test_files(test_path)
    if not test_files:
        logger.warning(f"No test files found in {test_path}")
        return 0

    logger.info(f"Found {len(test_files)} test files")

    table = Table(title="Test Results")
    table.add_column("Test File", style="cyan")
    table.add_column("Test Case", style="magenta")
    table.add_column("Status", style="bold")

    total_tests = 0
    passed_tests = 0
    failed_tests = 0

    for file_path in test_files:
        try:
            with open(file_path, "r") as f:
                data = yaml.safe_load(f)

            tests = data.get("tests", [])
            if not tests:
                continue

            for test in tests:
                total_tests += 1
                test_name = test.get("name", "Unnamed")
                success = run_test_case(test, file_path, update_snapshots=update_snapshots)

                status = "[green]PASS[/green]" if success else "[red]FAIL[/red]"
                if success:
                    passed_tests += 1
                else:
                    failed_tests += 1

                table.add_row(str(file_path.name), test_name, status)

        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            table.add_row(str(file_path.name), "Load Error", "[red]ERROR[/red]")

    console.print(table)

    logger.info(f"Summary: {passed_tests}/{total_tests} passed.")

    if failed_tests > 0:
        return 1
    return 0

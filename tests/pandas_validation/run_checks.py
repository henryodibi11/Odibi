import sys
from pathlib import Path

import pandas as pd

# Add root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from odibi.config import OnMissingColumns, OnNewColumns, SchemaMode, SchemaPolicyConfig
from odibi.engine.pandas_engine import LazyDataset, PandasEngine


def run_test(name, func):
    print(f"Running {name}...", end=" ")
    try:
        func()
        print("PASS")
        return True
    except Exception as e:
        print(f"FAIL: {e}")
        # traceback.print_exc()
        return False


def setup_engine():
    return PandasEngine()


def setup_lazy_dataset():
    # We need a real file for materialization to work if it tries to read
    # But we can mock the read call if we are careful, or just use a temp file.
    # For this script, let's create a temp file in current dir
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    path = "temp_test.csv"
    df.to_csv(path, index=False)
    return LazyDataset(path=path, format="csv", options={}), path


def cleanup(path):
    import os

    if os.path.exists(path):
        os.remove(path)


def check_anonymize_lazy():
    engine = setup_engine()
    lazy, path = setup_lazy_dataset()
    try:
        res = engine.anonymize(lazy, columns=["col2"], method="redact")
        if not isinstance(res, pd.DataFrame):
            raise ValueError("Result is not a DataFrame")
        if res["col2"].iloc[0] != "[REDACTED]":
            raise ValueError("Redaction failed")
    finally:
        cleanup(path)


def check_harmonize_schema_lazy():
    engine = setup_engine()
    lazy, path = setup_lazy_dataset()
    policy = SchemaPolicyConfig(
        mode=SchemaMode.ENFORCE,
        on_missing_columns=OnMissingColumns.FILL_NULL,
        on_new_columns=OnNewColumns.IGNORE,
    )
    target_schema = {"col1": "int64"}
    try:
        res = engine.harmonize_schema(lazy, target_schema, policy)
        if not isinstance(res, pd.DataFrame):
            raise ValueError("Result is not a DataFrame")
    finally:
        cleanup(path)


def check_execute_operation_lazy():
    engine = setup_engine()
    lazy, path = setup_lazy_dataset()
    try:
        res = engine.execute_operation("drop", {"columns": ["col2"]}, lazy)
        if not isinstance(res, pd.DataFrame):
            raise ValueError("Result is not a DataFrame")
        if "col2" in res.columns:
            raise ValueError("Column not dropped")
    finally:
        cleanup(path)


def check_validate_schema_lazy():
    engine = setup_engine()
    lazy, path = setup_lazy_dataset()
    schema_rules = {"types": {"col1": "int64"}}
    try:
        res = engine.validate_schema(lazy, schema_rules)
        if not isinstance(res, list):
            raise ValueError("Result is not a list")
    finally:
        cleanup(path)


def check_profile_nulls_lazy():
    engine = setup_engine()
    lazy, path = setup_lazy_dataset()
    try:
        res = engine.profile_nulls(lazy)
        if not isinstance(res, dict):
            raise ValueError("Result is not a dict")
    finally:
        cleanup(path)


def check_duplicate_methods():
    # Static check on the file content or runtime behavior
    # Runtime: we can check if the methods work as expected
    engine = setup_engine()
    df = pd.DataFrame({"a": [1, None]})
    res = engine.count_nulls(df, ["a"])
    if res["a"] != 1:
        raise ValueError("count_nulls failed")


if __name__ == "__main__":
    tests = [
        ("check_anonymize_lazy", check_anonymize_lazy),
        ("check_harmonize_schema_lazy", check_harmonize_schema_lazy),
        ("check_execute_operation_lazy", check_execute_operation_lazy),
        ("check_validate_schema_lazy", check_validate_schema_lazy),
        ("check_profile_nulls_lazy", check_profile_nulls_lazy),
        ("check_duplicate_methods", check_duplicate_methods),
    ]

    failed = False
    for name, func in tests:
        if not run_test(name, func):
            failed = True

    sys.exit(1 if failed else 0)

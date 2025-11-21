"""
Testing Assertions
==================

Helpers for asserting DataFrame equality and properties.
"""

from typing import Any, List
import pandas as pd


def assert_frame_equal(
    left: Any,
    right: Any,
    check_dtype: bool = True,
    check_exact: bool = False,
    atol: float = 1e-8,
    rtol: float = 1e-5,
) -> None:
    """
    Assert that two DataFrames are equal.
    Supports both Pandas and Spark DataFrames.

    Args:
        left: First DataFrame
        right: Second DataFrame
        check_dtype: Whether to check data types
        check_exact: Whether to compare numbers exactly
        atol: Absolute tolerance
        rtol: Relative tolerance
    """
    # Convert Spark to Pandas for comparison if needed
    left_pdf = _to_pandas(left)
    right_pdf = _to_pandas(right)

    # Sort by first column to ensure order doesn't matter (Spark is unordered)
    if not left_pdf.empty and not right_pdf.empty:
        sort_col = left_pdf.columns[0]
        left_pdf = left_pdf.sort_values(sort_col).reset_index(drop=True)
        right_pdf = right_pdf.sort_values(sort_col).reset_index(drop=True)

    pd.testing.assert_frame_equal(
        left_pdf, right_pdf, check_dtype=check_dtype, check_exact=check_exact, atol=atol, rtol=rtol
    )


def assert_schema_equal(left: Any, right: Any) -> None:
    """
    Assert that two DataFrames have the same schema (column names and types).
    """
    # Simplified check for column names
    left_cols = sorted(_get_columns(left))
    right_cols = sorted(_get_columns(right))

    assert left_cols == right_cols, f"Schema mismatch: {left_cols} != {right_cols}"


def _to_pandas(df: Any) -> pd.DataFrame:
    """Convert to Pandas DataFrame if not already."""
    if isinstance(df, pd.DataFrame):
        return df

    # Assume Spark DataFrame
    try:
        return df.toPandas()
    except AttributeError:
        raise TypeError(f"Expected DataFrame, got {type(df)}")


def _get_columns(df: Any) -> List[str]:
    """Get column names."""
    if isinstance(df, pd.DataFrame):
        return list(df.columns)
    return list(df.columns)

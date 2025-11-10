"""
Pivot Operation
===============

Converts long-format data into wide-format structure.
"""

from odibi.transformations import transformation
from odibi.transformations.templates import purpose_detail_result


@transformation("pivot", category="reshaping", tags=["restructure", "wide"])
def pivot(df, group_by, pivot_column, value_column, agg_func="first"):
    """
    Convert long-format data into wide-format structure.

    Args:
        df: Input DataFrame
        group_by: Columns to group by (list or string)
        pivot_column: Column whose values become new columns
        value_column: Column whose values fill the new columns
        agg_func: Aggregation function (default: "first")

    Returns:
        Wide-format DataFrame

    Example:
        Input (long format):
        | ID   | Category | Value |
        |------|----------|-------|
        | A    | Sales    | 100   |
        | A    | Revenue  | 200   |

        Output (wide format):
        | ID   | Sales | Revenue |
        |------|-------|---------|
        | A    | 100   | 200     |
    """
    # Ensure group_by is a list
    if isinstance(group_by, str):
        group_by = [group_by]

    return df.pivot_table(
        index=group_by, columns=pivot_column, values=value_column, aggfunc=agg_func
    ).reset_index()


@pivot.explain
def explain(group_by, pivot_column, value_column, agg_func="first", **context):
    """Generate context-aware explanation for pivot operation."""
    # Ensure group_by is a list for display
    if isinstance(group_by, str):
        group_by = [group_by]

    return purpose_detail_result(
        purpose="Convert long-format data into wide-format structure",
        details=[
            f"Groups by: {', '.join(group_by)}",
            f"Pivots column: `{pivot_column}` into separate columns",
            f"Aggregates: `{value_column}` using `{agg_func}()`",
            "Produces one row per unique combination of group columns",
        ],
        result=f"Wide-format dataset with one column per `{pivot_column}` value, ready for analysis",
    )

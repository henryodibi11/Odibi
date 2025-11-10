"""
Unpivot Operation
=================

Converts wide-format data into long-format structure.
"""

from odibi.transformations import transformation
from odibi.transformations.templates import purpose_detail_result


@transformation("unpivot", category="reshaping", tags=["restructure", "long", "melt"])
def unpivot(df, id_vars, value_vars=None, var_name="variable", value_name="value"):
    """
    Convert wide-format data into long-format structure.

    Args:
        df: Input DataFrame
        id_vars: Columns to use as identifier variables
        value_vars: Columns to unpivot (default: all except id_vars)
        var_name: Name for variable column (default: "variable")
        value_name: Name for value column (default: "value")

    Returns:
        Long-format DataFrame

    Example:
        Input (wide format):
        | ID   | Sales | Revenue |
        |------|-------|---------|
        | A    | 100   | 200     |

        Output (long format):
        | ID   | variable | value |
        |------|----------|-------|
        | A    | Sales    | 100   |
        | A    | Revenue  | 200   |
    """
    # Ensure id_vars is a list
    if isinstance(id_vars, str):
        id_vars = [id_vars]

    return df.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name=var_name,
        value_name=value_name,
    )


@unpivot.explain
def explain(id_vars, value_vars=None, var_name="variable", value_name="value", **context):
    """Generate context-aware explanation for unpivot operation."""
    # Ensure id_vars is a list for display
    if isinstance(id_vars, str):
        id_vars = [id_vars]

    details = [
        f"Identifier columns (preserved): {', '.join(id_vars)}",
        f"Variable name column: `{var_name}`",
        f"Value column: `{value_name}`",
    ]

    if value_vars:
        if isinstance(value_vars, str):
            value_vars = [value_vars]
        details.append(f"Columns to unpivot: {', '.join(value_vars)}")
    else:
        details.append("Unpivots all columns except identifiers")

    return purpose_detail_result(
        purpose="Convert wide-format data into long-format structure",
        details=details,
        result="Long-format dataset with one row per measurement, ready for analysis",
    )

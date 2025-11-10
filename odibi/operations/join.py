"""
Join Operation
==============

Joins two DataFrames together.
"""

from odibi.transformations import transformation
from odibi.transformations.templates import purpose_detail_result


@transformation("join", category="combining", tags=["merge", "combine"])
def join(df, right_df, on=None, left_on=None, right_on=None, how="inner"):
    """
    Join two DataFrames together.

    Args:
        df: Left DataFrame
        right_df: Right DataFrame to join
        on: Column(s) to join on (if same name in both DataFrames)
        left_on: Column(s) in left DataFrame
        right_on: Column(s) in right DataFrame
        how: Type of join ("inner", "left", "right", "outer")

    Returns:
        Joined DataFrame

    Example:
        Left DataFrame:
        | ID   | Value |
        |------|-------|
        | A    | 100   |

        Right DataFrame:
        | ID   | Target |
        |------|--------|
        | A    | 120    |

        Result:
        | ID   | Value | Target |
        |------|-------|--------|
        | A    | 100   | 120    |
    """
    return df.merge(right_df, on=on, left_on=left_on, right_on=right_on, how=how)


@join.explain
def explain(right_df=None, on=None, left_on=None, right_on=None, how="inner", **context):
    """Generate context-aware explanation for join operation."""
    # Determine join keys
    if on is not None:
        if isinstance(on, str):
            key_desc = f"on column: `{on}`"
        else:
            key_desc = f"on columns: {', '.join(f'`{k}`' for k in on)}"
    elif left_on is not None and right_on is not None:
        if isinstance(left_on, str):
            key_desc = f"on `{left_on}` = `{right_on}`"
        else:
            left_keys = ", ".join(f"`{k}`" for k in left_on)
            right_keys = ", ".join(f"`{k}`" for k in right_on)
            key_desc = f"on {left_keys} = {right_keys}"
    else:
        key_desc = "on index"

    # Describe join type
    join_types = {
        "inner": "Keep only matching records from both datasets",
        "left": "Keep all records from left dataset, match from right",
        "right": "Keep all records from right dataset, match from left",
        "outer": "Keep all records from both datasets",
    }
    join_desc = join_types.get(how, f"{how} join")

    return purpose_detail_result(
        purpose=f"Combine datasets using {how} join",
        details=[
            f"Join type: `{how}` - {join_desc}",
            f"Join condition: {key_desc}",
            "Merges columns from both datasets into single table",
        ],
        result="Combined dataset with enriched information from both sources",
    )

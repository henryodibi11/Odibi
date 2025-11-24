from typing import List, Literal, Union, Optional, Dict
from pydantic import BaseModel, Field
from odibi.context import EngineContext
from odibi.enums import EngineType

# -------------------------------------------------------------------------
# 1. Join
# -------------------------------------------------------------------------


class JoinParams(BaseModel):
    right_dataset: str = Field(..., description="Name of the node/dataset to join with")
    on: Union[str, List[str]] = Field(..., description="Column(s) to join on")
    how: Literal["inner", "left", "right", "full", "cross"] = Field("left", description="Join type")
    prefix: Optional[str] = Field(
        None, description="Prefix for columns from right dataset to avoid collisions"
    )


def join(context: EngineContext, params: JoinParams) -> EngineContext:
    """
    Joins the current dataset with another dataset from the context.
    """
    # Get Right DF
    right_df = context.get(params.right_dataset)
    if right_df is None:
        raise ValueError(
            f"Dataset '{params.right_dataset}' not found in context. Ensure it is in 'depends_on'."
        )

    # Register Right DF as temp view
    right_view_name = f"join_right_{params.right_dataset}"
    context.register_temp_view(right_view_name, right_df)

    # Construct Join Condition
    if isinstance(params.on, str):
        join_cols = [params.on]
    else:
        join_cols = params.on

    join_condition = " AND ".join([f"df.{col} = {right_view_name}.{col}" for col in join_cols])

    # Handle Column Selection (to apply prefix if needed)
    # Strategy: We explicitly construct the projection to handle collisions safely
    # and avoid ambiguous column references.

    # 1. Get Columns
    left_cols = context.columns
    right_cols = list(right_df.columns) if hasattr(right_df, "columns") else []

    # 2. Use Native Pandas optimization if possible
    if context.engine_type == EngineType.PANDAS:
        # Pandas defaults to ('_x', '_y'). We want ('', '_{prefix or right_dataset}')
        suffix = f"_{params.prefix}" if params.prefix else f"_{params.right_dataset}"
        res = context.df.merge(right_df, on=params.on, how=params.how, suffixes=("", suffix))
        return context.with_df(res)

    # 3. For SQL/Spark, build explicit projection
    projection = []

    # Add Left Columns (with Coalesce for keys in Outer Join)
    for col in left_cols:
        if col in join_cols and params.how in ["right", "full", "outer"]:
            # Coalesce to ensure we get non-null key from either side
            projection.append(f"COALESCE(df.{col}, {right_view_name}.{col}) AS {col}")
        else:
            projection.append(f"df.{col}")

    # Add Right Columns (skip keys, handle collisions)
    for col in right_cols:
        if col in join_cols:
            continue

        if col in left_cols:
            # Collision! Apply prefix or default to right_dataset name
            prefix = params.prefix if params.prefix else params.right_dataset
            projection.append(f"{right_view_name}.{col} AS {prefix}_{col}")
        else:
            projection.append(f"{right_view_name}.{col}")

    select_clause = ", ".join(projection)
    sql_query = f"""
        SELECT {select_clause}
        FROM df
        {params.how.upper()} JOIN {right_view_name}
        ON {join_condition}
    """
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 2. Union
# -------------------------------------------------------------------------


class UnionParams(BaseModel):
    datasets: List[str] = Field(..., description="List of node names to union with current")
    by_name: bool = Field(True, description="Match columns by name (UNION ALL BY NAME)")


def union(context: EngineContext, params: UnionParams) -> EngineContext:
    """
    Unions current dataset with others.
    """
    union_sqls = []

    # Add current
    union_sqls.append("SELECT * FROM df")

    # Add others
    for ds_name in params.datasets:
        other_df = context.get(ds_name)
        if other_df is None:
            raise ValueError(f"Dataset '{ds_name}' not found.")

        view_name = f"union_{ds_name}"
        context.register_temp_view(view_name, other_df)
        union_sqls.append(f"SELECT * FROM {view_name}")

    # Construct Query
    # DuckDB supports "UNION ALL BY NAME", Spark does too in recent versions.
    operator = "UNION ALL BY NAME" if params.by_name else "UNION ALL"

    # Fallback for engines without BY NAME if needed (omitted for brevity, assuming modern engines)
    # Spark < 3.1 might need logic.

    sql_query = f" {operator} ".join(union_sqls)
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 3. Pivot
# -------------------------------------------------------------------------


class PivotParams(BaseModel):
    group_by: List[str]
    pivot_col: str
    agg_col: str
    agg_func: Literal["sum", "count", "avg", "max", "min", "first"] = "sum"
    values: Optional[List[str]] = Field(
        None, description="Specific values to pivot (for Spark optimization)"
    )


def pivot(context: EngineContext, params: PivotParams) -> EngineContext:
    """
    Pivots row values into columns.
    """
    if context.engine_type == EngineType.SPARK:
        df = context.df.groupBy(*params.group_by)

        if params.values:
            pivot_op = df.pivot(params.pivot_col, params.values)
        else:
            pivot_op = df.pivot(params.pivot_col)

        # Construct agg expression dynamically based on string
        import pyspark.sql.functions as F

        agg_expr = getattr(F, params.agg_func)(params.agg_col)

        res = pivot_op.agg(agg_expr)
        return context.with_df(res)

    elif context.engine_type == EngineType.PANDAS:
        import pandas as pd

        # pivot_table is robust
        res = pd.pivot_table(
            context.df,
            index=params.group_by,
            columns=params.pivot_col,
            values=params.agg_col,
            aggfunc=params.agg_func,
        ).reset_index()
        return context.with_df(res)

    else:
        raise ValueError(f"Unsupported engine: {context.engine_type}")


# -------------------------------------------------------------------------
# 4. Unpivot (Stack)
# -------------------------------------------------------------------------


class UnpivotParams(BaseModel):
    id_cols: List[str]
    value_vars: List[str]
    var_name: str = "variable"
    value_name: str = "value"


def unpivot(context: EngineContext, params: UnpivotParams) -> EngineContext:
    """
    Unpivots columns into rows (Melt/Stack).
    """
    if context.engine_type == EngineType.PANDAS:
        res = context.df.melt(
            id_vars=params.id_cols,
            value_vars=params.value_vars,
            var_name=params.var_name,
            value_name=params.value_name,
        )
        return context.with_df(res)

    elif context.engine_type == EngineType.SPARK:
        # Spark Stack Syntax: stack(n, col1, val1, col2, val2, ...)
        import pyspark.sql.functions as F

        # Construct stack expression string
        # "stack(2, 'A', A, 'B', B) as (variable, value)"
        num_vars = len(params.value_vars)
        stack_args = []
        for col in params.value_vars:
            stack_args.append(f"'{col}'")  # The label
            stack_args.append(col)  # The value

        stack_expr = f"stack({num_vars}, {', '.join(stack_args)}) as ({params.var_name}, {params.value_name})"

        res = context.df.select(*params.id_cols, F.expr(stack_expr))
        return context.with_df(res)

    else:
        raise ValueError(f"Unsupported engine: {context.engine_type}")


# -------------------------------------------------------------------------
# 5. Aggregate
# -------------------------------------------------------------------------


class AggregateParams(BaseModel):
    group_by: List[str] = Field(..., description="Columns to group by")
    aggregations: Dict[str, str] = Field(
        ..., description="Map of column to aggregation function (sum, avg, min, max, count)"
    )


def aggregate(context: EngineContext, params: AggregateParams) -> EngineContext:
    """
    Performs grouping and aggregation via SQL.
    """
    group_cols = ", ".join(params.group_by)
    agg_exprs = []

    for col, func in params.aggregations.items():
        # Construct agg: SUM(col) AS col
        agg_exprs.append(f"{func.upper()}({col}) AS {col}")

    # Select grouped cols + aggregated cols
    # Note: params.group_by are already columns, so we list them
    select_items = params.group_by + agg_exprs
    select_clause = ", ".join(select_items)

    sql_query = f"SELECT {select_clause} FROM df GROUP BY {group_cols}"
    return context.sql(sql_query)

import time
from enum import Enum
from typing import Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, field_validator

from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.utils.logging_context import get_logging_context

# -------------------------------------------------------------------------
# 1. Join
# -------------------------------------------------------------------------


class JoinParams(BaseModel):
    """
    Configuration for joining datasets.

    Scenario 1: Simple Left Join
    ```yaml
    join:
      right_dataset: "customers"
      on: "customer_id"
      how: "left"
    ```

    Scenario 2: Join with Prefix (avoid collisions)
    ```yaml
    join:
      right_dataset: "orders"
      on: ["user_id"]
      how: "inner"
      prefix: "ord"  # Result cols: ord_date, ord_amount...
    ```
    """

    right_dataset: str = Field(..., description="Name of the node/dataset to join with")
    on: Union[str, List[str]] = Field(..., description="Column(s) to join on")
    how: Literal["inner", "left", "right", "full", "cross", "anti", "semi"] = Field(
        "left", description="Join type"
    )
    prefix: Optional[str] = Field(
        None, description="Prefix for columns from right dataset to avoid collisions"
    )

    @field_validator("on")
    @classmethod
    def coerce_on_to_list(cls, v):
        if isinstance(v, str):
            return [v]
        if not v:
            raise ValueError(
                f"Join 'on' parameter must contain at least one join key column. "
                f"Got: {v!r}. Provide column name(s) that exist in both datasets."
            )
        return v


def join(context: EngineContext, params: JoinParams) -> EngineContext:
    """
    Joins the current dataset with another dataset from the context.
    """
    ctx = get_logging_context()
    start_time = time.time()

    ctx.debug(
        "Join starting",
        right_dataset=params.right_dataset,
        join_type=params.how,
        keys=params.on,
    )

    # Get row count before transformation
    rows_before = None
    try:
        rows_before = context.df.shape[0] if hasattr(context.df, "shape") else None
        if rows_before is None and hasattr(context.df, "count"):
            rows_before = context.df.count()
    except Exception as e:
        ctx.debug(f"Could not get row count: {type(e).__name__}")

    # Get Right DF
    right_df = context.get(params.right_dataset)
    if right_df is None:
        ctx.error(
            "Join failed: right dataset not found",
            right_dataset=params.right_dataset,
            available_datasets=(
                list(context.context._data.keys()) if hasattr(context, "context") else None
            ),
        )
        raise ValueError(
            f"Join failed: dataset '{params.right_dataset}' not found in context. "
            f"Available datasets: {list(context.context._data.keys()) if hasattr(context, 'context') and hasattr(context.context, '_data') else 'unknown'}. "
            f"Ensure '{params.right_dataset}' is listed in 'depends_on' for this node."
        )

    # Get right df row count
    right_rows = None
    try:
        right_rows = right_df.shape[0] if hasattr(right_df, "shape") else None
        if right_rows is None and hasattr(right_df, "count"):
            right_rows = right_df.count()
    except Exception as e:
        ctx.debug(f"Could not get row count: {type(e).__name__}")

    ctx.debug(
        "Join datasets loaded",
        left_rows=rows_before,
        right_rows=right_rows,
        right_dataset=params.right_dataset,
    )

    # Register Right DF as temp view
    right_view_name = f"join_right_{params.right_dataset}"
    context.register_temp_view(right_view_name, right_df)

    # Construct Join Condition
    # params.on is guaranteed to be List[str] by validator
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

        # Map 'full' to 'outer' for Pandas compatibility
        pandas_how = "outer" if params.how == "full" else params.how

        # Handle anti and semi joins for pandas
        if params.how == "anti":
            # Anti join: rows in left that don't match right
            merged = context.df.merge(right_df[params.on], on=params.on, how="left", indicator=True)
            res = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
        elif params.how == "semi":
            # Semi join: rows in left that match right (no columns from right)
            merged = context.df.merge(right_df[params.on], on=params.on, how="inner")
            res = merged.drop_duplicates(subset=params.on)
        else:
            res = context.df.merge(right_df, on=params.on, how=pandas_how, suffixes=("", suffix))

        rows_after = res.shape[0] if hasattr(res, "shape") else None
        elapsed_ms = (time.time() - start_time) * 1000

        ctx.debug(
            "Join completed",
            join_type=params.how,
            rows_before=rows_before,
            rows_after=rows_after,
            row_delta=rows_after - rows_before if rows_before and rows_after else None,
            right_rows=right_rows,
            elapsed_ms=round(elapsed_ms, 2),
        )

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

    # Map join types to SQL syntax
    join_type_sql = params.how.upper()
    if params.how == "anti":
        join_type_sql = "LEFT ANTI"
    elif params.how == "semi":
        join_type_sql = "LEFT SEMI"

    sql_query = f"""
        SELECT {select_clause}
        FROM df
        {join_type_sql} JOIN {right_view_name}
        ON {join_condition}
    """
    result = context.sql(sql_query)

    # Log completion
    rows_after = None
    try:
        rows_after = result.df.shape[0] if hasattr(result.df, "shape") else None
        if rows_after is None and hasattr(result.df, "count"):
            rows_after = result.df.count()
    except Exception as e:
        ctx.debug(f"Could not get row count: {type(e).__name__}")

    elapsed_ms = (time.time() - start_time) * 1000
    ctx.debug(
        "Join completed",
        join_type=params.how,
        rows_before=rows_before,
        rows_after=rows_after,
        row_delta=rows_after - rows_before if rows_before and rows_after else None,
        right_rows=right_rows,
        elapsed_ms=round(elapsed_ms, 2),
    )

    return result


# -------------------------------------------------------------------------
# 2. Union
# -------------------------------------------------------------------------


class UnionParams(BaseModel):
    """
    Configuration for unioning datasets.

    Example (By Name - Default):
    ```yaml
    union:
      datasets: ["sales_2023", "sales_2024"]
      by_name: true
    ```

    Example (By Position):
    ```yaml
    union:
      datasets: ["legacy_data"]
      by_name: false
    ```
    """

    datasets: List[str] = Field(..., description="List of node names to union with current")
    by_name: bool = Field(True, description="Match columns by name (UNION ALL BY NAME)")


def union(context: EngineContext, params: UnionParams) -> EngineContext:
    """
    Unions current dataset with others.
    """
    ctx = get_logging_context()
    start_time = time.time()

    ctx.debug(
        "Union starting",
        datasets=params.datasets,
        by_name=params.by_name,
    )

    # Get row count of current df
    rows_before = None
    try:
        rows_before = context.df.shape[0] if hasattr(context.df, "shape") else None
        if rows_before is None and hasattr(context.df, "count"):
            rows_before = context.df.count()
    except Exception as e:
        ctx.debug(f"Could not get row count: {type(e).__name__}")

    union_sqls = []
    dataset_row_counts = {"current": rows_before}

    # Add current
    union_sqls.append("SELECT * FROM df")

    # Add others
    for ds_name in params.datasets:
        other_df = context.get(ds_name)
        if other_df is None:
            ctx.error(
                "Union failed: dataset not found",
                missing_dataset=ds_name,
                requested_datasets=params.datasets,
            )
            raise ValueError(
                f"Union failed: dataset '{ds_name}' not found in context. "
                f"Requested datasets: {params.datasets}. "
                f"Available datasets: {list(context.context._data.keys()) if hasattr(context, 'context') and hasattr(context.context, '_data') else 'unknown'}. "
                f"Ensure all datasets are listed in 'depends_on'."
            )

        # Get row count of other df
        try:
            other_rows = other_df.shape[0] if hasattr(other_df, "shape") else None
            if other_rows is None and hasattr(other_df, "count"):
                other_rows = other_df.count()
            dataset_row_counts[ds_name] = other_rows
        except Exception as e:
            ctx.debug(f"Could not get row count: {type(e).__name__}")

        view_name = f"union_{ds_name}"
        context.register_temp_view(view_name, other_df)
        union_sqls.append(f"SELECT * FROM {view_name}")

    ctx.debug(
        "Union datasets loaded",
        dataset_row_counts=dataset_row_counts,
    )

    # Construct Query
    # DuckDB supports "UNION ALL BY NAME", Spark does too in recent versions.
    operator = "UNION ALL BY NAME" if params.by_name else "UNION ALL"

    # Fallback for engines without BY NAME if needed (omitted for brevity, assuming modern engines)
    # Spark < 3.1 might need logic.

    sql_query = f" {operator} ".join(union_sqls)
    result = context.sql(sql_query)

    # Log completion
    rows_after = None
    try:
        rows_after = result.df.shape[0] if hasattr(result.df, "shape") else None
        if rows_after is None and hasattr(result.df, "count"):
            rows_after = result.df.count()
    except Exception as e:
        ctx.debug(f"Could not get row count: {type(e).__name__}")

    elapsed_ms = (time.time() - start_time) * 1000
    ctx.debug(
        "Union completed",
        datasets_count=len(params.datasets) + 1,
        rows_after=rows_after,
        elapsed_ms=round(elapsed_ms, 2),
    )

    return result


# -------------------------------------------------------------------------
# 3. Pivot
# -------------------------------------------------------------------------


class PivotParams(BaseModel):
    """
    Configuration for pivoting data.

    Example:
    ```yaml
    pivot:
      group_by: ["product_id", "region"]
      pivot_col: "month"
      agg_col: "sales"
      agg_func: "sum"
    ```

    Example (Optimized for Spark):
    ```yaml
    pivot:
      group_by: ["id"]
      pivot_col: "category"
      values: ["A", "B", "C"]  # Explicit values avoid extra pass
      agg_col: "amount"
    ```
    """

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
    ctx = get_logging_context()
    start_time = time.time()

    ctx.debug(
        "Pivot starting",
        group_by=params.group_by,
        pivot_col=params.pivot_col,
        agg_col=params.agg_col,
        agg_func=params.agg_func,
        values=params.values,
    )

    # Get row count before transformation
    rows_before = None
    try:
        rows_before = context.df.shape[0] if hasattr(context.df, "shape") else None
        if rows_before is None and hasattr(context.df, "count"):
            rows_before = context.df.count()
    except Exception as e:
        ctx.debug(f"Could not get row count: {type(e).__name__}")

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

        rows_after = res.count() if hasattr(res, "count") else None
        elapsed_ms = (time.time() - start_time) * 1000
        ctx.debug(
            "Pivot completed",
            rows_before=rows_before,
            rows_after=rows_after,
            elapsed_ms=round(elapsed_ms, 2),
        )

        return context.with_df(res)

    elif context.engine_type == EngineType.PANDAS:
        import pandas as pd

        # Map 'avg' to 'mean' for pandas compatibility
        agg_func_mapped = "mean" if params.agg_func == "avg" else params.agg_func

        # pivot_table is robust
        res = pd.pivot_table(
            context.df,
            index=params.group_by,
            columns=params.pivot_col,
            values=params.agg_col,
            aggfunc=agg_func_mapped,
        ).reset_index()

        rows_after = res.shape[0] if hasattr(res, "shape") else None
        elapsed_ms = (time.time() - start_time) * 1000
        ctx.debug(
            "Pivot completed",
            rows_before=rows_before,
            rows_after=rows_after,
            columns_after=len(res.columns) if hasattr(res, "columns") else None,
            elapsed_ms=round(elapsed_ms, 2),
        )

        return context.with_df(res)

    else:
        ctx.error(
            "Pivot failed: unsupported engine",
            engine_type=str(context.engine_type),
        )
        raise ValueError(
            f"Pivot transformer does not support engine type '{context.engine_type}'. "
            f"Supported engines: SPARK, PANDAS. "
            f"Check your engine configuration."
        )


# -------------------------------------------------------------------------
# 4. Unpivot (Stack)
# -------------------------------------------------------------------------


class UnpivotParams(BaseModel):
    """
    Configuration for unpivoting (melting) data.

    Example:
    ```yaml
    unpivot:
      id_cols: ["product_id"]
      value_vars: ["jan_sales", "feb_sales", "mar_sales"]
      var_name: "month"
      value_name: "sales"
    ```
    """

    id_cols: List[str]
    value_vars: List[str]
    var_name: str = "variable"
    value_name: str = "value"


def unpivot(context: EngineContext, params: UnpivotParams) -> EngineContext:
    """
    Unpivots columns into rows (Melt/Stack).
    """
    ctx = get_logging_context()
    start_time = time.time()

    ctx.debug(
        "Unpivot starting",
        id_cols=params.id_cols,
        value_vars=params.value_vars,
        var_name=params.var_name,
        value_name=params.value_name,
    )

    # Get row count before transformation
    rows_before = None
    try:
        rows_before = context.df.shape[0] if hasattr(context.df, "shape") else None
        if rows_before is None and hasattr(context.df, "count"):
            rows_before = context.df.count()
    except Exception as e:
        ctx.debug(f"Could not get row count: {type(e).__name__}")

    if context.engine_type == EngineType.PANDAS:
        res = context.df.melt(
            id_vars=params.id_cols,
            value_vars=params.value_vars,
            var_name=params.var_name,
            value_name=params.value_name,
        )

        rows_after = res.shape[0] if hasattr(res, "shape") else None
        elapsed_ms = (time.time() - start_time) * 1000
        ctx.debug(
            "Unpivot completed",
            rows_before=rows_before,
            rows_after=rows_after,
            value_vars_count=len(params.value_vars),
            elapsed_ms=round(elapsed_ms, 2),
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

        stack_expr = (
            f"stack({num_vars}, {', '.join(stack_args)}) "
            f"as ({params.var_name}, {params.value_name})"
        )

        res = context.df.select(*params.id_cols, F.expr(stack_expr))

        rows_after = res.count() if hasattr(res, "count") else None
        elapsed_ms = (time.time() - start_time) * 1000
        ctx.debug(
            "Unpivot completed",
            rows_before=rows_before,
            rows_after=rows_after,
            value_vars_count=len(params.value_vars),
            elapsed_ms=round(elapsed_ms, 2),
        )

        return context.with_df(res)

    else:
        ctx.error(
            "Unpivot failed: unsupported engine",
            engine_type=str(context.engine_type),
        )
        raise ValueError(
            f"Unpivot transformer does not support engine type '{context.engine_type}'. "
            f"Supported engines: SPARK, PANDAS. "
            f"Check your engine configuration."
        )


# -------------------------------------------------------------------------
# 5. Aggregate
# -------------------------------------------------------------------------


class AggFunc(str, Enum):
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    FIRST = "first"


class AggregateParams(BaseModel):
    """
    Configuration for aggregation.

    Example:
    ```yaml
    aggregate:
      group_by: ["department", "region"]
      aggregations:
        salary: "sum"
        employee_id: "count"
        age: "avg"
    ```
    """

    group_by: List[str] = Field(..., description="Columns to group by")
    aggregations: Dict[str, AggFunc] = Field(
        ..., description="Map of column to aggregation function (sum, avg, min, max, count)"
    )


def aggregate(context: EngineContext, params: AggregateParams) -> EngineContext:
    """
    Performs grouping and aggregation via SQL.
    """
    ctx = get_logging_context()
    start_time = time.time()

    ctx.debug(
        "Aggregate starting",
        group_by=params.group_by,
        aggregations={col: func.value for col, func in params.aggregations.items()},
    )

    # Get row count before transformation
    rows_before = None
    try:
        rows_before = context.df.shape[0] if hasattr(context.df, "shape") else None
        if rows_before is None and hasattr(context.df, "count"):
            rows_before = context.df.count()
    except Exception as e:
        ctx.debug(f"Could not get row count: {type(e).__name__}")

    group_cols = ", ".join(params.group_by)
    agg_exprs = []

    for col, func in params.aggregations.items():
        # Construct agg: SUM(col) AS col
        agg_exprs.append(f"{func.value.upper()}({col}) AS {col}")

    # Select grouped cols + aggregated cols
    # Note: params.group_by are already columns, so we list them
    select_items = params.group_by + agg_exprs
    select_clause = ", ".join(select_items)

    sql_query = f"SELECT {select_clause} FROM df GROUP BY {group_cols}"
    result = context.sql(sql_query)

    # Log completion
    rows_after = None
    try:
        rows_after = result.df.shape[0] if hasattr(result.df, "shape") else None
        if rows_after is None and hasattr(result.df, "count"):
            rows_after = result.df.count()
    except Exception as e:
        ctx.debug(f"Could not get row count: {type(e).__name__}")

    elapsed_ms = (time.time() - start_time) * 1000
    ctx.debug(
        "Aggregate completed",
        group_by=params.group_by,
        rows_before=rows_before,
        rows_after=rows_after,
        aggregation_count=len(params.aggregations),
        elapsed_ms=round(elapsed_ms, 2),
    )

    return result

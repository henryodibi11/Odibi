from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field
from odibi.context import EngineContext
from odibi.enums import EngineType

# -------------------------------------------------------------------------
# 1. Deduplicate (Window)
# -------------------------------------------------------------------------


class DeduplicateParams(BaseModel):
    keys: List[str]
    order_by: Optional[str] = Field(
        None, description="SQL Order by clause (e.g. 'updated_at DESC')"
    )


def deduplicate(context: EngineContext, params: DeduplicateParams) -> EngineContext:
    """
    Deduplicates data using Window functions.
    """
    partition_clause = ", ".join(params.keys)
    order_clause = params.order_by if params.order_by else "(SELECT NULL)"

    # Dialect handling for EXCEPT/EXCLUDE
    except_clause = "EXCEPT"
    if context.engine_type == EngineType.PANDAS:
        # DuckDB uses EXCLUDE
        except_clause = "EXCLUDE"

    sql_query = f"""
        SELECT * {except_clause}(_rn) FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY {partition_clause} ORDER BY {order_clause}) as _rn
            FROM df
        ) WHERE _rn = 1
    """
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 2. Explode List
# -------------------------------------------------------------------------


class ExplodeParams(BaseModel):
    column: str
    outer: bool = Field(False, description="If True, keep rows with empty lists (explode_outer)")


def explode_list_column(context: EngineContext, params: ExplodeParams) -> EngineContext:
    if context.engine_type == EngineType.SPARK:
        import pyspark.sql.functions as F

        func = F.explode_outer if params.outer else F.explode
        df = context.df.withColumn(params.column, func(F.col(params.column)))
        return context.with_df(df)

    elif context.engine_type == EngineType.PANDAS:
        df = context.df.explode(params.column)
        if not params.outer:
            # Inner explode behavior: Drop rows where explode resulted in NaN (empty list source)
            df = df.dropna(subset=[params.column])
        return context.with_df(df)

    else:
        raise ValueError(f"Unsupported engine: {context.engine_type}")


# -------------------------------------------------------------------------
# 3. Dict Mapping
# -------------------------------------------------------------------------


class DictMappingParams(BaseModel):
    column: str
    mapping: Dict[Any, Any]
    default: Optional[Any] = None
    output_column: Optional[str] = None


def dict_based_mapping(context: EngineContext, params: DictMappingParams) -> EngineContext:
    target_col = params.output_column or params.column

    if context.engine_type == EngineType.SPARK:
        import pyspark.sql.functions as F
        from itertools import chain

        # Create map expression
        mapping_expr = F.create_map([F.lit(x) for x in chain(*params.mapping.items())])

        df = context.df.withColumn(target_col, mapping_expr[F.col(params.column)])
        if params.default is not None:
            df = df.withColumn(target_col, F.coalesce(F.col(target_col), F.lit(params.default)))
        return context.with_df(df)

    elif context.engine_type == EngineType.PANDAS:
        df = context.df.copy()
        # Pandas map is fast
        df[target_col] = df[params.column].map(params.mapping)
        if params.default is not None:
            df[target_col] = df[target_col].fillna(params.default)
        return context.with_df(df)

    else:
        raise ValueError(f"Unsupported engine: {context.engine_type}")


# -------------------------------------------------------------------------
# 4. Regex Replace
# -------------------------------------------------------------------------


class RegexReplaceParams(BaseModel):
    column: str
    pattern: str
    replacement: str


def regex_replace(context: EngineContext, params: RegexReplaceParams) -> EngineContext:
    """
    SQL-based Regex replacement.
    """
    # Spark and DuckDB both support REGEXP_REPLACE(col, pattern, replacement)
    sql_query = f"SELECT *, REGEXP_REPLACE({params.column}, '{params.pattern}', '{params.replacement}') AS {params.column} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 5. Unpack Struct (Flatten)
# -------------------------------------------------------------------------


class UnpackStructParams(BaseModel):
    column: str


def unpack_struct(context: EngineContext, params: UnpackStructParams) -> EngineContext:
    """
    Flattens a struct/dict column into top-level columns.
    """
    if context.engine_type == EngineType.SPARK:
        # Spark: "select col.* from df"
        sql_query = f"SELECT *, {params.column}.* FROM df"
        # Usually we want to drop the original struct?
        # For safety, we keep original but append fields.
        # Actually "SELECT *" includes the struct.
        # Let's assume users drop it later or we just select expanded.
        return context.sql(sql_query)

    elif context.engine_type == EngineType.PANDAS:
        import pandas as pd

        # Pandas: json_normalize or Apply(pd.Series)
        # Optimization: df[col].tolist() is much faster than apply(pd.Series)
        # assuming the column contains dictionaries/structs.
        try:
            expanded = pd.DataFrame(context.df[params.column].tolist(), index=context.df.index)
        except Exception:
            # Fallback if tolist() fails (e.g. mixed types)
            expanded = context.df[params.column].apply(pd.Series)

        # Rename to avoid collisions? Default behavior is to use keys.
        # Join back
        res = pd.concat([context.df, expanded], axis=1)
        return context.with_df(res)

    else:
        raise ValueError(f"Unsupported engine: {context.engine_type}")


# -------------------------------------------------------------------------
# 6. Hash Columns
# -------------------------------------------------------------------------


class HashParams(BaseModel):
    columns: List[str]
    algorithm: Literal["sha256", "md5"] = "sha256"


def hash_columns(context: EngineContext, params: HashParams) -> EngineContext:
    """
    Hashes columns for PII/Anonymization.
    """
    # Removed unused 'expressions' variable

    # Since SQL syntax differs, use Dual Engine
    if context.engine_type == EngineType.SPARK:
        import pyspark.sql.functions as F

        df = context.df
        for col in params.columns:
            if params.algorithm == "sha256":
                df = df.withColumn(col, F.sha2(F.col(col), 256))
            elif params.algorithm == "md5":
                df = df.withColumn(col, F.md5(F.col(col)))
        return context.with_df(df)

    elif context.engine_type == EngineType.PANDAS:
        df = context.df.copy()

        # Optimization: Try PyArrow compute for vectorized hashing if available
        # For now, the below logic is a placeholder for future vectorized hashing.
        # The import is unused in the current implementation fallback, triggering linter errors.
        # We will stick to the stable hashlib fallback for now.
        pass

        import hashlib

        def hash_val(val, alg):
            if val is None:
                return None
            encoded = str(val).encode("utf-8")
            if alg == "sha256":
                return hashlib.sha256(encoded).hexdigest()
            return hashlib.md5(encoded).hexdigest()

        # Vectorize? difficult with standard lib hashlib.
        # Apply is acceptable for this security feature vs complexity of numpy deps
        for col in params.columns:
            # Optimization: Ensure string type once
            s_col = df[col].astype(str)
            df[col] = s_col.apply(lambda x: hash_val(x, params.algorithm))

        return context.with_df(df)

    else:
        raise ValueError(f"Unsupported engine: {context.engine_type}")


# -------------------------------------------------------------------------
# 7. Validate and Flag
# -------------------------------------------------------------------------


class ValidateAndFlagParams(BaseModel):
    # key: rule name, value: sql condition (must be true for valid)
    rules: Dict[str, str] = Field(
        ..., description="Map of rule name to SQL condition (must be TRUE)"
    )
    flag_col: str = Field("_issues", description="Name of the column to store failed rules")


def validate_and_flag(context: EngineContext, params: ValidateAndFlagParams) -> EngineContext:
    """
    Validates rules and appends a column with a list/string of failed rule names.
    """
    # Strategy:
    # Use CONCAT_WS (Spark) or list_value/string concatenation (DuckDB)
    # to build a list of failed rules.
    # For each rule, IF NOT condition THEN 'rule_name' ELSE NULL

    rule_exprs = []

    for name, condition in params.rules.items():
        # If condition fails (NOT condition), return name, else NULL
        expr = f"CASE WHEN NOT ({condition}) THEN '{name}' ELSE NULL END"
        rule_exprs.append(expr)

    if not rule_exprs:
        return context.sql(f"SELECT *, NULL AS {params.flag_col} FROM df")

    # Both Spark and DuckDB support concat_ws which skips NULLs
    concatted = f"concat_ws(', ', {', '.join(rule_exprs)})"

    # If result is empty string, replace with NULL to indicate clean?
    # Or keep empty string. Let's keep empty string if that's what concat_ws returns for all nulls.
    # Actually, for validation, NULL usually means "no issues".
    # concat_ws returns "" if all inputs are null (in Spark) or empty string.
    # We can wrap in NULLIF(..., '')

    final_expr = f"NULLIF({concatted}, '')"

    sql_query = f"SELECT *, {final_expr} AS {params.flag_col} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 8. Window Calculation
# -------------------------------------------------------------------------


class WindowCalculationParams(BaseModel):
    target_col: str
    function: str = Field(..., description="Window function e.g. 'sum(amount)', 'rank()'")
    partition_by: List[str] = Field(default_factory=list)
    order_by: Optional[str] = None


def window_calculation(context: EngineContext, params: WindowCalculationParams) -> EngineContext:
    """
    Generic wrapper for Window functions.
    """
    partition_clause = ""
    if params.partition_by:
        partition_clause = f"PARTITION BY {', '.join(params.partition_by)}"

    order_clause = ""
    if params.order_by:
        order_clause = f"ORDER BY {params.order_by}"

    over_clause = f"OVER ({partition_clause} {order_clause})".strip()

    expr = f"{params.function} {over_clause}"

    sql_query = f"SELECT *, {expr} AS {params.target_col} FROM df"
    return context.sql(sql_query)

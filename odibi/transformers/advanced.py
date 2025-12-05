import time
from enum import Enum
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator

from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.utils.logging_context import get_logging_context

# -------------------------------------------------------------------------
# 1. Deduplicate (Window)
# -------------------------------------------------------------------------


class DeduplicateParams(BaseModel):
    """
    Configuration for deduplication.

    Scenario: Keep latest record
    ```yaml
    deduplicate:
      keys: ["id"]
      order_by: "updated_at DESC"
    ```
    """

    keys: List[str] = Field(
        ..., description="List of columns to partition by (columns that define uniqueness)"
    )
    order_by: Optional[str] = Field(
        None,
        description="SQL Order by clause (e.g. 'updated_at DESC') to determine which record to keep (first one is kept)",
    )


def deduplicate(context: EngineContext, params: DeduplicateParams) -> EngineContext:
    """
    Deduplicates data using Window functions.
    """
    ctx = get_logging_context()
    start_time = time.time()

    ctx.debug(
        "Deduplicate starting",
        keys=params.keys,
        order_by=params.order_by,
    )

    # Get row count before transformation (optional, for logging only)
    rows_before = None
    try:
        rows_before = context.df.shape[0] if hasattr(context.df, "shape") else None
        if rows_before is None and hasattr(context.df, "count"):
            rows_before = context.df.count()
    except Exception as e:
        ctx.debug(f"Could not get row count before transform: {type(e).__name__}")

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
    result = context.sql(sql_query)

    # Get row count after transformation (optional, for logging only)
    rows_after = None
    try:
        rows_after = result.df.shape[0] if hasattr(result.df, "shape") else None
        if rows_after is None and hasattr(result.df, "count"):
            rows_after = result.df.count()
    except Exception as e:
        ctx.debug(f"Could not get row count after transform: {type(e).__name__}")

    elapsed_ms = (time.time() - start_time) * 1000
    duplicates_removed = rows_before - rows_after if rows_before and rows_after else None
    ctx.debug(
        "Deduplicate completed",
        keys=params.keys,
        rows_before=rows_before,
        rows_after=rows_after,
        duplicates_removed=duplicates_removed,
        elapsed_ms=round(elapsed_ms, 2),
    )

    return result


# -------------------------------------------------------------------------
# 2. Explode List
# -------------------------------------------------------------------------


class ExplodeParams(BaseModel):
    """
    Configuration for exploding lists.

    Scenario: Flatten list of items per order
    ```yaml
    explode_list_column:
      column: "items"
      outer: true  # Keep orders with empty items list
    ```
    """

    column: str = Field(..., description="Column containing the list/array to explode")
    outer: bool = Field(
        False,
        description="If True, keep rows with empty lists (explode_outer behavior). If False, drops them.",
    )


def explode_list_column(context: EngineContext, params: ExplodeParams) -> EngineContext:
    ctx = get_logging_context()
    start_time = time.time()

    ctx.debug(
        "Explode starting",
        column=params.column,
        outer=params.outer,
    )

    rows_before = None
    try:
        rows_before = context.df.shape[0] if hasattr(context.df, "shape") else None
        if rows_before is None and hasattr(context.df, "count"):
            rows_before = context.df.count()
    except Exception:
        pass

    if context.engine_type == EngineType.SPARK:
        import pyspark.sql.functions as F

        func = F.explode_outer if params.outer else F.explode
        df = context.df.withColumn(params.column, func(F.col(params.column)))

        rows_after = df.count() if hasattr(df, "count") else None
        elapsed_ms = (time.time() - start_time) * 1000
        ctx.debug(
            "Explode completed",
            column=params.column,
            rows_before=rows_before,
            rows_after=rows_after,
            elapsed_ms=round(elapsed_ms, 2),
        )
        return context.with_df(df)

    elif context.engine_type == EngineType.PANDAS:
        df = context.df.explode(params.column)
        if not params.outer:
            df = df.dropna(subset=[params.column])

        rows_after = df.shape[0] if hasattr(df, "shape") else None
        elapsed_ms = (time.time() - start_time) * 1000
        ctx.debug(
            "Explode completed",
            column=params.column,
            rows_before=rows_before,
            rows_after=rows_after,
            elapsed_ms=round(elapsed_ms, 2),
        )
        return context.with_df(df)

    else:
        ctx.error("Explode failed: unsupported engine", engine_type=str(context.engine_type))
        raise ValueError(f"Unsupported engine: {context.engine_type}")


# -------------------------------------------------------------------------
# 3. Dict Mapping
# -------------------------------------------------------------------------

JsonScalar = Union[str, int, float, bool, None]


class DictMappingParams(BaseModel):
    """
    Configuration for dictionary mapping.

    Scenario: Map status codes to labels
    ```yaml
    dict_based_mapping:
      column: "status_code"
      mapping:
        "1": "Active"
        "0": "Inactive"
      default: "Unknown"
      output_column: "status_desc"
    ```
    """

    column: str = Field(..., description="Column to map values from")
    mapping: Dict[str, JsonScalar] = Field(
        ..., description="Dictionary of source value -> target value"
    )
    default: Optional[JsonScalar] = Field(
        None, description="Default value if source value is not found in mapping"
    )
    output_column: Optional[str] = Field(
        None, description="Name of output column. If not provided, overwrites source column."
    )


def dict_based_mapping(context: EngineContext, params: DictMappingParams) -> EngineContext:
    target_col = params.output_column or params.column

    if context.engine_type == EngineType.SPARK:
        from itertools import chain

        import pyspark.sql.functions as F

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
    """
    Configuration for regex replacement.

    Example:
    ```yaml
    regex_replace:
      column: "phone"
      pattern: "[^0-9]"
      replacement: ""
    ```
    """

    column: str = Field(..., description="Column to apply regex replacement on")
    pattern: str = Field(..., description="Regex pattern to match")
    replacement: str = Field(..., description="String to replace matches with")


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
    """
    Configuration for unpacking structs.

    Example:
    ```yaml
    unpack_struct:
      column: "user_info"
    ```
    """

    column: str = Field(
        ..., description="Struct/Dictionary column to unpack/flatten into individual columns"
    )


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
        except Exception as e:
            import logging

            logger = logging.getLogger(__name__)
            logger.debug(f"Optimized struct unpack failed (falling back to slow apply): {e}")
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


class HashAlgorithm(str, Enum):
    SHA256 = "sha256"
    MD5 = "md5"


class HashParams(BaseModel):
    """
    Configuration for column hashing.

    Example:
    ```yaml
    hash_columns:
      columns: ["email", "ssn"]
      algorithm: "sha256"
    ```
    """

    columns: List[str] = Field(..., description="List of columns to hash")
    algorithm: HashAlgorithm = Field(
        HashAlgorithm.SHA256, description="Hashing algorithm. Options: 'sha256', 'md5'"
    )


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
            if params.algorithm == HashAlgorithm.SHA256:
                df = df.withColumn(col, F.sha2(F.col(col), 256))
            elif params.algorithm == HashAlgorithm.MD5:
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
            if alg == HashAlgorithm.SHA256:
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
# 7. Generate Surrogate Key
# -------------------------------------------------------------------------


class SurrogateKeyParams(BaseModel):
    """
    Configuration for surrogate key generation.

    Example:
    ```yaml
    generate_surrogate_key:
      columns: ["region", "product_id"]
      separator: "-"
      output_col: "unique_id"
    ```
    """

    columns: List[str] = Field(..., description="Columns to combine for the key")
    separator: str = Field("-", description="Separator between values")
    output_col: str = Field("surrogate_key", description="Name of the output column")


def generate_surrogate_key(context: EngineContext, params: SurrogateKeyParams) -> EngineContext:
    """
    Generates a deterministic surrogate key (MD5) from a combination of columns.
    Handles NULLs by treating them as empty strings to ensure consistency.
    """
    # Logic: MD5( CONCAT_WS( separator, COALESCE(col1, ''), COALESCE(col2, '') ... ) )

    from odibi.enums import EngineType

    # 1. Build the concatenation expression
    # We must cast to string and coalesce nulls

    def safe_col(col):
        # Spark/DuckDB cast syntax slightly different but standard SQL CAST(x AS STRING) usually works
        # Spark: cast(col as string)
        # DuckDB: cast(col as varchar) or string
        return f"COALESCE(CAST({col} AS STRING), '')"

    if context.engine_type == EngineType.SPARK:
        # Spark CONCAT_WS skips nulls, but we coerced them to empty string above anyway for safety.
        # Actually, if we want strict "dbt style" surrogate keys, we often treat NULL as a specific token.
        # But empty string is standard for "simple" SKs.

        cols_expr = ", ".join([safe_col(c) for c in params.columns])
        concat_expr = f"concat_ws('{params.separator}', {cols_expr})"
        final_expr = f"md5({concat_expr})"

    else:
        # DuckDB / Pandas
        # DuckDB also supports concat_ws and md5.
        # Note: DuckDB CAST AS STRING is valid.

        cols_expr = ", ".join([safe_col(c) for c in params.columns])
        concat_expr = f"concat_ws('{params.separator}', {cols_expr})"
        final_expr = f"md5({concat_expr})"

    sql_query = f"SELECT *, {final_expr} AS {params.output_col} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 8. Parse JSON
# -------------------------------------------------------------------------


class ParseJsonParams(BaseModel):
    """
    Configuration for JSON parsing.

    Example:
    ```yaml
    parse_json:
      column: "raw_json"
      json_schema: "id INT, name STRING"
      output_col: "parsed_struct"
    ```
    """

    column: str = Field(..., description="String column containing JSON")
    json_schema: str = Field(
        ..., description="DDL schema string (e.g. 'a INT, b STRING') or Spark StructType DDL"
    )
    output_col: Optional[str] = None


def parse_json(context: EngineContext, params: ParseJsonParams) -> EngineContext:
    """
    Parses a JSON string column into a Struct/Map column.
    """
    from odibi.enums import EngineType

    target = params.output_col or f"{params.column}_parsed"

    if context.engine_type == EngineType.SPARK:
        # Spark: from_json(col, schema)
        expr = f"from_json({params.column}, '{params.json_schema}')"

    else:
        # DuckDB / Pandas
        # DuckDB: json_transform(col, 'schema') is experimental.
        # Standard: from_json(col, 'schema') works in recent DuckDB versions.
        # But reliable way is usually casting or json extraction if we know the structure?
        # Actually, DuckDB allows: cast(json_parse(col) as STRUCT(a INT, b VARCHAR...))

        # We need to convert the generic DDL schema string to DuckDB STRUCT syntax?
        # That is complex.
        # SIMPLIFICATION: For DuckDB, we might rely on automatic inference if we use `json_parse`?
        # Or just `json_parse(col)` which returns a JSON type (which is distinct).
        # Then user can unpack it.

        # Let's try `json_parse(col)`.
        # Note: If user provided specific schema to enforce types, that's harder in DuckDB SQL string without parsing the DDL.
        # Spark's schema string "a INT, b STRING" is not valid DuckDB STRUCT(a INT, b VARCHAR).

        # For V1 of this function, we will focus on Spark (where it's critical).
        # For DuckDB, we will use `CAST(col AS JSON)` which is the standard way to parse JSON string to JSON type.
        # `json_parse` is an alias in some versions but CAST is more stable.

        expr = f"CAST({params.column} AS JSON)"

    sql_query = f"SELECT *, {expr} AS {target} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 9. Validate and Flag
# -------------------------------------------------------------------------


class ValidateAndFlagParams(BaseModel):
    """
    Configuration for validation flagging.

    Example:
    ```yaml
    validate_and_flag:
      flag_col: "data_issues"
      rules:
        age_check: "age >= 0"
        email_format: "email LIKE '%@%'"
    ```
    """

    # key: rule name, value: sql condition (must be true for valid)
    rules: Dict[str, str] = Field(
        ..., description="Map of rule name to SQL condition (must be TRUE)"
    )
    flag_col: str = Field("_issues", description="Name of the column to store failed rules")

    @field_validator("rules")
    @classmethod
    def require_non_empty_rules(cls, v):
        if not v:
            raise ValueError("ValidateAndFlag: 'rules' must not be empty")
        return v


def validate_and_flag(context: EngineContext, params: ValidateAndFlagParams) -> EngineContext:
    """
    Validates rules and appends a column with a list/string of failed rule names.
    """
    ctx = get_logging_context()
    start_time = time.time()

    ctx.debug(
        "ValidateAndFlag starting",
        rules=list(params.rules.keys()),
        flag_col=params.flag_col,
    )

    rule_exprs = []

    for name, condition in params.rules.items():
        expr = f"CASE WHEN NOT ({condition}) THEN '{name}' ELSE NULL END"
        rule_exprs.append(expr)

    if not rule_exprs:
        return context.sql(f"SELECT *, NULL AS {params.flag_col} FROM df")

    concatted = f"concat_ws(', ', {', '.join(rule_exprs)})"
    final_expr = f"NULLIF({concatted}, '')"

    sql_query = f"SELECT *, {final_expr} AS {params.flag_col} FROM df"
    result = context.sql(sql_query)

    elapsed_ms = (time.time() - start_time) * 1000
    ctx.debug(
        "ValidateAndFlag completed",
        rules_count=len(params.rules),
        elapsed_ms=round(elapsed_ms, 2),
    )

    return result


# -------------------------------------------------------------------------
# 10. Window Calculation
# -------------------------------------------------------------------------


class WindowCalculationParams(BaseModel):
    """
    Configuration for window functions.

    Example:
    ```yaml
    window_calculation:
      target_col: "cumulative_sales"
      function: "sum(sales)"
      partition_by: ["region"]
      order_by: "date ASC"
    ```
    """

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


# -------------------------------------------------------------------------
# 11. Normalize JSON
# -------------------------------------------------------------------------


class NormalizeJsonParams(BaseModel):
    """
    Configuration for JSON normalization.

    Example:
    ```yaml
    normalize_json:
      column: "json_data"
      sep: "_"
    ```
    """

    column: str = Field(..., description="Column containing nested JSON/Struct")
    sep: str = Field("_", description="Separator for nested fields (e.g., 'parent_child')")


def normalize_json(context: EngineContext, params: NormalizeJsonParams) -> EngineContext:
    """
    Flattens a nested JSON/Struct column.
    """
    if context.engine_type == EngineType.SPARK:
        # Spark: Top-level flatten using "col.*"
        sql_query = f"SELECT *, {params.column}.* FROM df"
        return context.sql(sql_query)

    elif context.engine_type == EngineType.PANDAS:
        import json

        import pandas as pd

        df = context.df.copy()

        # Ensure we have dicts
        s = df[params.column]
        if len(s) > 0:
            first_val = s.iloc[0]
            if isinstance(first_val, str):
                # Try to parse if string
                try:
                    s = s.apply(json.loads)
                except Exception as e:
                    import logging

                    logger = logging.getLogger(__name__)
                    logger.warning(f"Failed to parse JSON strings in column '{params.column}': {e}")
                    # We proceed, but json_normalize will likely fail if data is not dicts.

        # json_normalize
        # Handle empty case
        if s.empty:
            return context.with_df(df)

        normalized = pd.json_normalize(s, sep=params.sep)
        # Align index
        normalized.index = df.index

        # Join back (avoid collision if possible, or use suffixes)
        # We use rsuffix just in case
        df = df.join(normalized, rsuffix="_json")
        return context.with_df(df)

    else:
        raise ValueError(f"Unsupported engine: {context.engine_type}")


# -------------------------------------------------------------------------
# 12. Sessionize
# -------------------------------------------------------------------------


class SessionizeParams(BaseModel):
    """
    Configuration for sessionization.

    Example:
    ```yaml
    sessionize:
      timestamp_col: "event_time"
      user_col: "user_id"
      threshold_seconds: 1800
    ```
    """

    timestamp_col: str = Field(
        ..., description="Timestamp column to calculate session duration from"
    )
    user_col: str = Field(..., description="User identifier to partition sessions by")
    threshold_seconds: int = Field(
        1800,
        description="Inactivity threshold in seconds (default: 30 minutes). If gap > threshold, new session starts.",
    )
    session_col: str = Field(
        "session_id", description="Output column name for the generated session ID"
    )


def sessionize(context: EngineContext, params: SessionizeParams) -> EngineContext:
    """
    Assigns session IDs based on inactivity threshold.
    """
    if context.engine_type == EngineType.SPARK:
        # Spark SQL
        # 1. Lag timestamp to get prev_timestamp
        # 2. Calculate diff: ts - prev_ts
        # 3. Flag new session: if diff > threshold OR prev_ts is null -> 1 else 0
        # 4. Sum(flags) over (partition by user order by ts) -> session_id

        threshold = params.threshold_seconds

        # We use nested queries for clarity and safety against multiple aggregations
        sql = f"""
        WITH lagged AS (
            SELECT *,
                   LAG({params.timestamp_col}) OVER (PARTITION BY {params.user_col} ORDER BY {params.timestamp_col}) as _prev_ts
            FROM df
        ),
        flagged AS (
            SELECT *,
                   CASE
                     WHEN _prev_ts IS NULL THEN 1
                     WHEN (unix_timestamp({params.timestamp_col}) - unix_timestamp(_prev_ts)) > {threshold} THEN 1
                     ELSE 0
                   END as _is_new_session
            FROM lagged
        )
        SELECT *,
               concat({params.user_col}, '-', sum(_is_new_session) OVER (PARTITION BY {params.user_col} ORDER BY {params.timestamp_col})) as {params.session_col}
        FROM flagged
        """
        # Note: This returns intermediate columns (_prev_ts, _is_new_session) as well.
        # Ideally we select * EXCEPT ... but Spark < 3.1 doesn't support EXCEPT in SELECT list easily without listing all cols.
        # We leave them for now, or user can drop them.
        return context.sql(sql)

    elif context.engine_type == EngineType.PANDAS:
        import pandas as pd

        df = context.df.copy()

        # Ensure datetime
        if not pd.api.types.is_datetime64_any_dtype(df[params.timestamp_col]):
            df[params.timestamp_col] = pd.to_datetime(df[params.timestamp_col])

        # Sort
        df = df.sort_values([params.user_col, params.timestamp_col])

        user = df[params.user_col]

        # Calculate time diff (in seconds)
        # We groupby user to ensure shift doesn't cross user boundaries for diff
        # But diff() doesn't support groupby well directly on Series without apply?
        # Actually `groupby().diff()` works.
        time_diff = df.groupby(params.user_col)[params.timestamp_col].diff().dt.total_seconds()

        # Flag new session
        # New if: time_diff > threshold OR time_diff is NaT (start of group)
        is_new = (time_diff > params.threshold_seconds) | (time_diff.isna())

        # Cumulative sum per user
        session_ids = is_new.groupby(user).cumsum()

        df[params.session_col] = user.astype(str) + "-" + session_ids.astype(int).astype(str)

        return context.with_df(df)

    else:
        raise ValueError(f"Unsupported engine: {context.engine_type}")


# -------------------------------------------------------------------------
# 13. Geocode (Stub)
# -------------------------------------------------------------------------


class GeocodeParams(BaseModel):
    """
    Configuration for geocoding.

    Example:
    ```yaml
    geocode:
      address_col: "full_address"
      output_col: "lat_long"
    ```
    """

    address_col: str = Field(..., description="Column containing the address to geocode")
    output_col: str = Field("lat_long", description="Name of the output column for coordinates")


def geocode(context: EngineContext, params: GeocodeParams) -> EngineContext:
    """
    Geocoding Stub.
    """
    import logging

    logger = logging.getLogger(__name__)
    logger.warning("Geocode transformer is a stub. No actual geocoding performed.")

    # Pass-through
    return context.with_df(context.df)

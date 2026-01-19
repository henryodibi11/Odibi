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
    """
    Explodes a list/array column into multiple rows.

    For each element in the specified list column, creates a new row.
    If 'outer' is True, keeps rows with empty lists (like explode_outer).
    Supports Spark and Pandas engines.
    """
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
        raise ValueError(
            f"Explode transformer does not support engine type '{context.engine_type}'. "
            f"Supported engines: SPARK, PANDAS. "
            f"Check your engine configuration."
        )


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
    """
    Maps values in a column using a provided dictionary.

    For each value in the specified column, replaces it with the mapped value.
    If 'default' is provided, uses it for values not found in the mapping.
    Supports Spark and Pandas engines.
    """
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
            df[target_col] = df[target_col].fillna(params.default).infer_objects(copy=False)
        return context.with_df(df)

    else:
        raise ValueError(
            f"Dict-based mapping does not support engine type '{context.engine_type}'. "
            f"Supported engines: SPARK, PANDAS. "
            f"Check your engine configuration."
        )


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
    Applies a regex replacement to a column.

    Uses SQL-based REGEXP_REPLACE to replace all matches of the pattern in the specified column
    with the given replacement string. Works on both Spark and DuckDB/Pandas engines.
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
        raise ValueError(
            f"Unpack struct does not support engine type '{context.engine_type}'. "
            f"Supported engines: SPARK, PANDAS. "
            f"Check your engine configuration."
        )


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

    def safe_col(col, quote_char):
        # Spark/DuckDB cast syntax slightly different but standard SQL CAST(x AS STRING) usually works
        # Spark: cast(col as string) with backticks for quoting
        # DuckDB: cast(col as varchar) with double quotes for quoting
        return f"COALESCE(CAST({quote_char}{col}{quote_char} AS STRING), '')"

    if context.engine_type == EngineType.SPARK:
        # Spark CONCAT_WS skips nulls, but we coerced them to empty string above anyway for safety.
        # Actually, if we want strict "dbt style" surrogate keys, we often treat NULL as a specific token.
        # But empty string is standard for "simple" SKs.
        quote_char = "`"
        cols_expr = ", ".join([safe_col(c, quote_char) for c in params.columns])
        concat_expr = f"concat_ws('{params.separator}', {cols_expr})"
        final_expr = f"md5({concat_expr})"
        output_col = f"`{params.output_col}`"

    else:
        # DuckDB / Pandas
        # DuckDB also supports concat_ws and md5.
        # Note: DuckDB CAST AS STRING is valid.
        quote_char = '"'
        cols_expr = ", ".join([safe_col(c, quote_char) for c in params.columns])
        concat_expr = f"concat_ws('{params.separator}', {cols_expr})"
        final_expr = f"md5({concat_expr})"
        output_col = f'"{params.output_col}"'

    sql_query = f"SELECT *, {final_expr} AS {output_col} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 7b. Generate Numeric Key (BIGINT surrogate key)
# -------------------------------------------------------------------------


class NumericKeyParams(BaseModel):
    """
    Configuration for numeric surrogate key generation.

    Generates a deterministic BIGINT key from a hash of specified columns.
    Useful when unioning data from multiple sources where some have IDs
    and others don't.

    Example:
    ```yaml
    - function: generate_numeric_key
      params:
        columns: [DateID, store_id, reason_id, duration_min, notes]
        output_col: ID
        coalesce_with: ID  # Keep existing ID if not null
    ```

    The generated key is:
    - Deterministic: same input data = same ID every time
    - BIGINT: large numeric space to avoid collisions
    - Stable: safe for gold layer / incremental loads
    """

    columns: List[str] = Field(..., description="Columns to combine for the key")
    separator: str = Field("|", description="Separator between values")
    output_col: str = Field("numeric_key", description="Name of the output column")
    coalesce_with: Optional[str] = Field(
        None,
        description="Existing column to coalesce with (keep existing value if not null)",
    )


def generate_numeric_key(context: EngineContext, params: NumericKeyParams) -> EngineContext:
    """
    Generates a deterministic BIGINT surrogate key from a hash of columns.

    This is useful when:
    - Unioning data from multiple sources
    - Some sources have IDs, some don't
    - You need stable numeric IDs for gold layer

    The key is generated by:
    1. Concatenating columns with separator
    2. Computing MD5 hash
    3. Converting first 15 hex chars to BIGINT

    If coalesce_with is specified, keeps the existing value when not null.
    If output_col == coalesce_with, the original column is replaced.
    """
    from odibi.enums import EngineType

    def safe_col(col, quote_char):
        # Normalize: TRIM whitespace, then treat empty string and NULL as equivalent
        return f"COALESCE(NULLIF(TRIM(CAST({quote_char}{col}{quote_char} AS STRING)), ''), '')"

    # Check if we need to replace the original column
    # Replace if: coalesce_with == output_col, OR output_col already exists in dataframe
    col_names = list(context.df.columns)
    output_exists = params.output_col in col_names
    replace_column = (
        params.coalesce_with and params.output_col == params.coalesce_with
    ) or output_exists

    if context.engine_type == EngineType.SPARK:
        quote_char = "`"
        cols_expr = ", ".join([safe_col(c, quote_char) for c in params.columns])
        concat_expr = f"concat_ws('{params.separator}', {cols_expr})"
        hash_expr = f"CAST(CONV(SUBSTRING(md5({concat_expr}), 1, 15), 16, 10) AS BIGINT)"

        if params.coalesce_with:
            final_expr = f"COALESCE(CAST(`{params.coalesce_with}` AS BIGINT), {hash_expr})"
        else:
            final_expr = hash_expr

        output_col = f"`{params.output_col}`"

    else:
        # DuckDB/Pandas - use ABS(HASH()) instead of CONV (DuckDB doesn't have CONV)
        quote_char = '"'
        cols_expr = ", ".join([safe_col(c, quote_char) for c in params.columns])
        concat_expr = f"concat_ws('{params.separator}', {cols_expr})"
        # hash() in DuckDB returns BIGINT directly
        hash_expr = f"ABS(hash({concat_expr}))"

        if params.coalesce_with:
            final_expr = f'COALESCE(CAST("{params.coalesce_with}" AS BIGINT), {hash_expr})'
        else:
            final_expr = hash_expr

        output_col = f'"{params.output_col}"'

    if replace_column:
        # Replace the original column by selecting all columns except the original,
        # then adding the new computed column
        col_to_exclude = params.coalesce_with if params.coalesce_with else params.output_col
        if context.engine_type == EngineType.SPARK:
            all_cols = [f"`{c}`" for c in col_names if c != col_to_exclude]
        else:
            # Pandas/DuckDB
            all_cols = [f'"{c}"' for c in col_names if c != col_to_exclude]
        cols_select = ", ".join(all_cols)
        sql_query = f"SELECT {cols_select}, {final_expr} AS {output_col} FROM df"
    else:
        sql_query = f"SELECT *, {final_expr} AS {output_col} FROM df"

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


# -------------------------------------------------------------------------
# 14. Split Events by Period
# -------------------------------------------------------------------------


class ShiftDefinition(BaseModel):
    """Definition of a single shift."""

    name: str = Field(..., description="Name of the shift (e.g., 'Day', 'Night')")
    start: str = Field(..., description="Start time in HH:MM format (e.g., '06:00')")
    end: str = Field(..., description="End time in HH:MM format (e.g., '14:00')")


class SplitEventsByPeriodParams(BaseModel):
    """
    Configuration for splitting events that span multiple time periods.

    Splits events that span multiple days, hours, or shifts into individual
    segments per period. Useful for OEE/downtime analysis, billing, and
    time-based aggregations.

    Example - Split by day:
    ```yaml
    split_events_by_period:
      start_col: "Shutdown_Start_Time"
      end_col: "Shutdown_End_Time"
      period: "day"
      duration_col: "Shutdown_Duration_Min"
    ```

    Example - Split by shift:
    ```yaml
    split_events_by_period:
      start_col: "event_start"
      end_col: "event_end"
      period: "shift"
      duration_col: "duration_minutes"
      shifts:
        - name: "Day"
          start: "06:00"
          end: "14:00"
        - name: "Swing"
          start: "14:00"
          end: "22:00"
        - name: "Night"
          start: "22:00"
          end: "06:00"
    ```
    """

    start_col: str = Field(..., description="Column containing the event start timestamp")
    end_col: str = Field(..., description="Column containing the event end timestamp")
    period: str = Field(
        "day",
        description="Period type to split by: 'day', 'hour', or 'shift'",
    )
    duration_col: Optional[str] = Field(
        None,
        description="Output column name for duration in minutes. If not set, no duration column is added.",
    )
    shifts: Optional[List[ShiftDefinition]] = Field(
        None,
        description="List of shift definitions (required when period='shift')",
    )
    shift_col: Optional[str] = Field(
        "shift_name",
        description="Output column name for shift name (only used when period='shift')",
    )

    def model_post_init(self, __context):
        if self.period == "shift" and not self.shifts:
            raise ValueError("shifts must be provided when period='shift'")
        if self.period not in ("day", "hour", "shift"):
            raise ValueError(f"Invalid period: {self.period}. Must be 'day', 'hour', or 'shift'")


def split_events_by_period(
    context: EngineContext, params: SplitEventsByPeriodParams
) -> EngineContext:
    """
    Splits events that span multiple time periods into individual segments.

    For events spanning multiple days/hours/shifts, this creates separate rows
    for each period with adjusted start/end times and recalculated durations.
    """
    if params.period == "day":
        return _split_by_day(context, params)
    elif params.period == "hour":
        return _split_by_hour(context, params)
    elif params.period == "shift":
        return _split_by_shift(context, params)
    else:
        raise ValueError(f"Unsupported period: {params.period}")


def _split_by_day(context: EngineContext, params: SplitEventsByPeriodParams) -> EngineContext:
    """Split events by day boundaries."""
    start_col = params.start_col
    end_col = params.end_col

    if context.engine_type == EngineType.SPARK:
        duration_col_exists = params.duration_col and params.duration_col.lower() in [
            c.lower() for c in context.df.columns
        ]

        ts_start = f"to_timestamp({start_col})"
        ts_end = f"to_timestamp({end_col})"

        duration_expr = ""
        if params.duration_col:
            duration_expr = f", (unix_timestamp(adj_{end_col}) - unix_timestamp(adj_{start_col})) / 60.0 AS {params.duration_col}"

        single_day_except = "_event_days"
        if duration_col_exists:
            single_day_except = f"_event_days, {params.duration_col}"

        single_day_sql = f"""
        WITH events_with_days AS (
            SELECT *,
                {ts_start} AS _ts_start,
                {ts_end} AS _ts_end,
                datediff(to_date({ts_end}), to_date({ts_start})) + 1 AS _event_days
            FROM df
        )
        SELECT * EXCEPT({single_day_except}, _ts_start, _ts_end){f", (unix_timestamp(_ts_end) - unix_timestamp(_ts_start)) / 60.0 AS {params.duration_col}" if params.duration_col else ""}
        FROM events_with_days
        WHERE _event_days = 1
        """

        multi_day_except_adjusted = "_exploded_day, _event_days, _ts_start, _ts_end"
        if duration_col_exists:
            multi_day_except_adjusted = (
                f"_exploded_day, _event_days, _ts_start, _ts_end, {params.duration_col}"
            )

        multi_day_sql = f"""
        WITH events_with_days AS (
            SELECT *,
                {ts_start} AS _ts_start,
                {ts_end} AS _ts_end,
                datediff(to_date({ts_end}), to_date({ts_start})) + 1 AS _event_days
            FROM df
        ),
        multi_day AS (
            SELECT *,
                explode(sequence(to_date(_ts_start), to_date(_ts_end), interval 1 day)) AS _exploded_day
            FROM events_with_days
            WHERE _event_days > 1
        ),
        multi_day_adjusted AS (
            SELECT * EXCEPT({multi_day_except_adjusted}),
                CASE
                    WHEN to_date(_exploded_day) = to_date(_ts_start) THEN _ts_start
                    ELSE to_timestamp(concat(cast(_exploded_day as string), ' 00:00:00'))
                END AS adj_{start_col},
                CASE
                    WHEN to_date(_exploded_day) = to_date(_ts_end) THEN _ts_end
                    ELSE to_timestamp(concat(cast(date_add(_exploded_day, 1) as string), ' 00:00:00'))
                END AS adj_{end_col}
            FROM multi_day
        )
        SELECT * EXCEPT({start_col}, {end_col}, adj_{start_col}, adj_{end_col}),
            adj_{start_col} AS {start_col},
            adj_{end_col} AS {end_col}
            {duration_expr}
        FROM multi_day_adjusted
        """

        single_day_df = context.sql(single_day_sql).df
        multi_day_df = context.sql(multi_day_sql).df
        result_df = single_day_df.unionByName(multi_day_df, allowMissingColumns=True)
        return context.with_df(result_df)

    elif context.engine_type == EngineType.PANDAS:
        import pandas as pd

        df = context.df.copy()

        df[start_col] = pd.to_datetime(df[start_col])
        df[end_col] = pd.to_datetime(df[end_col])

        df["_event_days"] = (df[end_col].dt.normalize() - df[start_col].dt.normalize()).dt.days + 1

        single_day = df[df["_event_days"] == 1].copy()
        multi_day = df[df["_event_days"] > 1].copy()

        if params.duration_col and not single_day.empty:
            single_day[params.duration_col] = (
                single_day[end_col] - single_day[start_col]
            ).dt.total_seconds() / 60.0

        if not multi_day.empty:
            rows = []
            for _, row in multi_day.iterrows():
                start = row[start_col]
                end = row[end_col]
                current_day = start.normalize()

                while current_day <= end.normalize():
                    new_row = row.copy()

                    if current_day == start.normalize():
                        new_start = start
                    else:
                        new_start = current_day

                    next_day = current_day + pd.Timedelta(days=1)
                    if next_day > end:
                        new_end = end
                    else:
                        new_end = next_day

                    new_row[start_col] = new_start
                    new_row[end_col] = new_end

                    if params.duration_col:
                        new_row[params.duration_col] = (new_end - new_start).total_seconds() / 60.0

                    rows.append(new_row)
                    current_day = next_day

            multi_day_exploded = pd.DataFrame(rows)
            result = pd.concat([single_day, multi_day_exploded], ignore_index=True)
        else:
            result = single_day

        result = result.drop(columns=["_event_days"], errors="ignore")
        return context.with_df(result)

    else:
        raise ValueError(
            f"Split events by day does not support engine type '{context.engine_type}'. "
            f"Supported engines: SPARK, PANDAS. "
            f"Check your engine configuration."
        )


def _split_by_hour(context: EngineContext, params: SplitEventsByPeriodParams) -> EngineContext:
    """Split events by hour boundaries."""
    start_col = params.start_col
    end_col = params.end_col

    if context.engine_type == EngineType.SPARK:
        ts_start = f"to_timestamp({start_col})"
        ts_end = f"to_timestamp({end_col})"

        duration_expr = ""
        if params.duration_col:
            duration_expr = f", (unix_timestamp({end_col}) - unix_timestamp({start_col})) / 60.0 AS {params.duration_col}"

        sql = f"""
        WITH events_with_hours AS (
            SELECT *,
                {ts_start} AS _ts_start,
                {ts_end} AS _ts_end,
                CAST((unix_timestamp({ts_end}) - unix_timestamp({ts_start})) / 3600 AS INT) + 1 AS _event_hours
            FROM df
        ),
        multi_hour AS (
            SELECT *,
                explode(sequence(
                    date_trunc('hour', _ts_start),
                    date_trunc('hour', _ts_end),
                    interval 1 hour
                )) AS _exploded_hour
            FROM events_with_hours
            WHERE _event_hours > 1
        ),
        multi_hour_adjusted AS (
            SELECT * EXCEPT(_exploded_hour, _event_hours, _ts_start, _ts_end),
                CASE
                    WHEN date_trunc('hour', _ts_start) = _exploded_hour THEN _ts_start
                    ELSE _exploded_hour
                END AS {start_col},
                CASE
                    WHEN date_trunc('hour', _ts_end) = _exploded_hour THEN _ts_end
                    ELSE _exploded_hour + interval 1 hour
                END AS {end_col}
            FROM multi_hour
        ),
        single_hour AS (
            SELECT * EXCEPT(_event_hours, _ts_start, _ts_end){duration_expr}
            FROM events_with_hours
            WHERE _event_hours = 1
        )
        SELECT *{duration_expr} FROM multi_hour_adjusted
        UNION ALL
        SELECT * FROM single_hour
        """
        return context.sql(sql)

    elif context.engine_type == EngineType.PANDAS:
        import pandas as pd

        df = context.df.copy()

        df[start_col] = pd.to_datetime(df[start_col])
        df[end_col] = pd.to_datetime(df[end_col])

        df["_event_hours"] = ((df[end_col] - df[start_col]).dt.total_seconds() / 3600).astype(
            int
        ) + 1

        single_hour = df[df["_event_hours"] == 1].copy()
        multi_hour = df[df["_event_hours"] > 1].copy()

        if params.duration_col and not single_hour.empty:
            single_hour[params.duration_col] = (
                single_hour[end_col] - single_hour[start_col]
            ).dt.total_seconds() / 60.0

        if not multi_hour.empty:
            rows = []
            for _, row in multi_hour.iterrows():
                start = row[start_col]
                end = row[end_col]
                current_hour = start.floor("h")

                while current_hour <= end.floor("h"):
                    new_row = row.copy()

                    if current_hour == start.floor("h"):
                        new_start = start
                    else:
                        new_start = current_hour

                    next_hour = current_hour + pd.Timedelta(hours=1)
                    if next_hour > end:
                        new_end = end
                    else:
                        new_end = next_hour

                    new_row[start_col] = new_start
                    new_row[end_col] = new_end

                    if params.duration_col:
                        new_row[params.duration_col] = (new_end - new_start).total_seconds() / 60.0

                    rows.append(new_row)
                    current_hour = next_hour

            multi_hour_exploded = pd.DataFrame(rows)
            result = pd.concat([single_hour, multi_hour_exploded], ignore_index=True)
        else:
            result = single_hour

        result = result.drop(columns=["_event_hours"], errors="ignore")
        return context.with_df(result)

    else:
        raise ValueError(
            f"Split events by hour does not support engine type '{context.engine_type}'. "
            f"Supported engines: SPARK, PANDAS. "
            f"Check your engine configuration."
        )


def _split_by_shift(context: EngineContext, params: SplitEventsByPeriodParams) -> EngineContext:
    """Split events by shift boundaries."""
    start_col = params.start_col
    end_col = params.end_col
    shift_col = params.shift_col or "shift_name"
    shifts = params.shifts

    if context.engine_type == EngineType.SPARK:
        ts_start = f"to_timestamp({start_col})"
        ts_end = f"to_timestamp({end_col})"

        duration_expr = ""
        if params.duration_col:
            duration_expr = f", (unix_timestamp({end_col}) - unix_timestamp({start_col})) / 60.0 AS {params.duration_col}"

        sql = f"""
        WITH base AS (
            SELECT *,
                {ts_start} AS _ts_start,
                {ts_end} AS _ts_end,
                datediff(to_date({ts_end}), to_date({ts_start})) + 1 AS _span_days
            FROM df
        ),
        day_exploded AS (
            SELECT *,
                explode(sequence(to_date(_ts_start), to_date(_ts_end), interval 1 day)) AS _day
            FROM base
        ),
        with_shift_times AS (
            SELECT *,
                {
            ", ".join(
                [
                    f"to_timestamp(concat(cast(_day as string), ' {s.start}:00')) AS _shift_{i}_start, "
                    + f"to_timestamp(concat(cast(date_add(_day, {1 if int(s.end.split(':')[0]) < int(s.start.split(':')[0]) else 0}) as string), ' {s.end}:00')) AS _shift_{i}_end"
                    for i, s in enumerate(shifts)
                ]
            )
        }
            FROM day_exploded
        ),
        shift_segments AS (
            {
            " UNION ALL ".join(
                [
                    f"SELECT * EXCEPT({', '.join([f'_shift_{j}_start, _shift_{j}_end' for j in range(len(shifts))])}, _ts_start, _ts_end), "
                    f"GREATEST(_ts_start, _shift_{i}_start) AS {start_col}, "
                    f"LEAST(_ts_end, _shift_{i}_end) AS {end_col}, "
                    f"'{s.name}' AS {shift_col} "
                    f"FROM with_shift_times "
                    f"WHERE _ts_start < _shift_{i}_end AND _ts_end > _shift_{i}_start"
                    for i, s in enumerate(shifts)
                ]
            )
        }
        )
        SELECT * EXCEPT(_span_days, _day){duration_expr}
        FROM shift_segments
        WHERE {start_col} < {end_col}
        """
        return context.sql(sql)

    elif context.engine_type == EngineType.PANDAS:
        import pandas as pd
        from datetime import timedelta

        df = context.df.copy()
        df[start_col] = pd.to_datetime(df[start_col])
        df[end_col] = pd.to_datetime(df[end_col])

        def parse_time(t_str):
            h, m = map(int, t_str.split(":"))
            return timedelta(hours=h, minutes=m)

        rows = []
        for _, row in df.iterrows():
            event_start = row[start_col]
            event_end = row[end_col]

            current_day = event_start.normalize()
            while current_day <= event_end.normalize():
                for shift in shifts:
                    shift_start_delta = parse_time(shift.start)
                    shift_end_delta = parse_time(shift.end)

                    if shift_end_delta <= shift_start_delta:
                        shift_start_dt = current_day + shift_start_delta
                        shift_end_dt = current_day + timedelta(days=1) + shift_end_delta
                    else:
                        shift_start_dt = current_day + shift_start_delta
                        shift_end_dt = current_day + shift_end_delta

                    seg_start = max(event_start, shift_start_dt)
                    seg_end = min(event_end, shift_end_dt)

                    if seg_start < seg_end:
                        new_row = row.copy()
                        new_row[start_col] = seg_start
                        new_row[end_col] = seg_end
                        new_row[shift_col] = shift.name

                        if params.duration_col:
                            new_row[params.duration_col] = (
                                seg_end - seg_start
                            ).total_seconds() / 60.0

                        rows.append(new_row)

                current_day += timedelta(days=1)

        if rows:
            result = pd.DataFrame(rows)
        else:
            result = df.copy()
            result[shift_col] = None
            if params.duration_col:
                result[params.duration_col] = 0.0

        return context.with_df(result)

    else:
        raise ValueError(
            f"Split events by shift does not support engine type '{context.engine_type}'. "
            f"Supported engines: SPARK, PANDAS. "
            f"Check your engine configuration."
        )

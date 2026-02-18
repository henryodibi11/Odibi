import time
from enum import Enum
from typing import Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field

from odibi.context import EngineContext
from odibi.utils.logging_context import get_logging_context

# -------------------------------------------------------------------------
# 1. Filter Rows
# -------------------------------------------------------------------------


class FilterRowsParams(BaseModel):
    """
    Configuration for filtering rows.

    Example:
    ```yaml
    filter_rows:
      condition: "age > 18 AND status = 'active'"
    ```

    Example (Null Check):
    ```yaml
    filter_rows:
      condition: "email IS NOT NULL AND email != ''"
    ```
    """

    condition: str = Field(
        ..., description="SQL WHERE clause (e.g., 'age > 18 AND status = \"active\"')"
    )


def filter_rows(context: EngineContext, params: FilterRowsParams) -> EngineContext:
    """
    Filters rows using a standard SQL WHERE clause.

    Design:
    - SQL-First: Pushes filtering to the engine's optimizer.
    - Zero-Copy: No data movement to Python.
    """
    ctx = get_logging_context()
    start_time = time.time()

    ctx.debug(
        "FilterRows starting",
        condition=params.condition,
    )

    rows_before = None
    try:
        rows_before = context.df.shape[0] if hasattr(context.df, "shape") else None
        if rows_before is None and hasattr(context.df, "count"):
            rows_before = context.df.count()
    except Exception as e:
        ctx.debug(f"Could not get row count before transform: {type(e).__name__}")

    sql_query = f"SELECT * FROM df WHERE {params.condition}"
    result = context.sql(sql_query)

    rows_after = None
    try:
        rows_after = result.df.shape[0] if hasattr(result.df, "shape") else None
        if rows_after is None and hasattr(result.df, "count"):
            rows_after = result.df.count()
    except Exception as e:
        ctx.debug(f"Could not get row count after transform: {type(e).__name__}")

    elapsed_ms = (time.time() - start_time) * 1000
    rows_filtered = rows_before - rows_after if rows_before and rows_after else None
    ctx.debug(
        "FilterRows completed",
        rows_before=rows_before,
        rows_after=rows_after,
        rows_filtered=rows_filtered,
        elapsed_ms=round(elapsed_ms, 2),
    )

    return result


# -------------------------------------------------------------------------
# 2. Derive Columns
# -------------------------------------------------------------------------


class DeriveColumnsParams(BaseModel):
    """
    Configuration for derived columns.

    Example:
    ```yaml
    derive_columns:
      derivations:
        total_price: "quantity * unit_price"
        full_name: "concat(first_name, ' ', last_name)"
    ```

    Note: Engine will fail if expressions reference non-existent columns.
    """

    # key: new_column_name, value: sql_expression
    derivations: Dict[str, str] = Field(..., description="Map of column name to SQL expression")


def derive_columns(context: EngineContext, params: DeriveColumnsParams) -> EngineContext:
    """
    Appends new columns based on SQL expressions.

    Design:
    - Uses projection to add fields.
    - Keeps all existing columns via `*`.
    """
    ctx = get_logging_context()
    start_time = time.time()

    ctx.debug(
        "DeriveColumns starting",
        derivations=list(params.derivations.keys()),
    )

    columns_before = len(context.columns) if context.columns else 0

    expressions = [f"{expr} AS {col}" for col, expr in params.derivations.items()]
    select_clause = ", ".join(expressions)

    sql_query = f"SELECT *, {select_clause} FROM df"
    result = context.sql(sql_query)

    columns_after = len(result.columns) if result.columns else 0
    elapsed_ms = (time.time() - start_time) * 1000
    ctx.debug(
        "DeriveColumns completed",
        columns_added=list(params.derivations.keys()),
        columns_before=columns_before,
        columns_after=columns_after,
        elapsed_ms=round(elapsed_ms, 2),
    )

    return result


# -------------------------------------------------------------------------
# 3. Cast Columns
# -------------------------------------------------------------------------


class SimpleType(str, Enum):
    """Simple type aliases for common SQL types.

    Provides user-friendly type names that are normalized to standard SQL types.
    Supports both short forms (int, str, float, bool) and SQL standard forms
    (integer, string, double, boolean).
    """

    INT = "int"
    INTEGER = "integer"
    STR = "str"
    STRING = "string"
    FLOAT = "float"
    DOUBLE = "double"
    BOOL = "bool"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"


class CastColumnsParams(BaseModel):
    """
    Configuration for column type casting.

    Example:
    ```yaml
    cast_columns:
      casts:
        age: "int"
        salary: "DOUBLE"
        created_at: "TIMESTAMP"
        tags: "ARRAY<STRING>"  # Raw SQL types allowed
    ```
    """

    # key: column_name, value: target_type
    casts: Dict[str, Union[SimpleType, str]] = Field(
        ..., description="Map of column to target SQL type"
    )


def cast_columns(context: EngineContext, params: CastColumnsParams) -> EngineContext:
    """
    Casts specific columns to new types while keeping others intact.

    Normalizes common type aliases (int->INTEGER, str->STRING, float->DOUBLE)
    and passes through complex SQL types as-is (e.g., ARRAY<STRING>).

    Args:
        context: Current execution context with dataframe
        params: Cast configuration with column-to-type mapping

    Returns:
        New context with columns cast to specified types

    Example:
        Cast columns using simple type aliases and complex SQL types:
        ```yaml
        cast_columns:
          casts:
            age: "int"
            salary: "double"
            tags: "ARRAY<STRING>"
        ```
    """
    current_cols = context.columns
    projection = []

    # Standardized type map for "Simple over Clever"
    type_map = {
        "int": "INTEGER",
        "integer": "INTEGER",
        "str": "STRING",
        "string": "STRING",
        "float": "DOUBLE",
        "double": "DOUBLE",
        "bool": "BOOLEAN",
        "boolean": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
    }

    for col in current_cols:
        if col in params.casts:
            raw_type = params.casts[col]
            # Handle Enum or str
            if isinstance(raw_type, Enum):
                raw_type_str = raw_type.value
            else:
                raw_type_str = str(raw_type)

            target_type = type_map.get(raw_type_str.lower(), raw_type_str)
            projection.append(f"CAST({col} AS {target_type}) AS {col}")
        else:
            projection.append(col)

    sql_query = f"SELECT {', '.join(projection)} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 4. Clean Text
# -------------------------------------------------------------------------


class CleanTextParams(BaseModel):
    """
    Configuration for text cleaning.

    Example:
    ```yaml
    clean_text:
      columns: ["email", "username"]
      trim: true
      case: "lower"
    ```
    """

    columns: List[str] = Field(..., description="List of columns to clean")
    trim: bool = Field(True, description="Apply TRIM()")
    case: Literal["lower", "upper", "preserve"] = Field("preserve", description="Case conversion")


def clean_text(context: EngineContext, params: CleanTextParams) -> EngineContext:
    """
    Applies string cleaning operations (Trim/Case) via SQL.
    """
    current_cols = context.columns
    projection = []

    for col in current_cols:
        if col in params.columns:
            expr = col
            if params.trim:
                expr = f"TRIM({expr})"
            if params.case == "lower":
                expr = f"LOWER({expr})"
            elif params.case == "upper":
                expr = f"UPPER({expr})"
            projection.append(f"{expr} AS {col}")
        else:
            projection.append(col)

    sql_query = f"SELECT {', '.join(projection)} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 5. Extract Date Parts
# -------------------------------------------------------------------------


class ExtractDateParams(BaseModel):
    """
    Configuration for extracting date parts.

    Example:
    ```yaml
    extract_date_parts:
      source_col: "created_at"
      prefix: "created"
      parts: ["year", "month"]
    ```
    """

    source_col: str
    prefix: Optional[str] = None
    parts: List[Literal["year", "month", "day", "hour"]] = ["year", "month", "day"]


def extract_date_parts(context: EngineContext, params: ExtractDateParams) -> EngineContext:
    """
    Extracts date parts using ANSI SQL extract/functions.
    """
    prefix = params.prefix or params.source_col
    expressions = []

    for part in params.parts:
        # Standard SQL compatible syntax
        # Note: Using YEAR(col) syntax which is supported by Spark and DuckDB
        if part == "year":
            expressions.append(f"YEAR({params.source_col}) AS {prefix}_year")
        elif part == "month":
            expressions.append(f"MONTH({params.source_col}) AS {prefix}_month")
        elif part == "day":
            expressions.append(f"DAY({params.source_col}) AS {prefix}_day")
        elif part == "hour":
            expressions.append(f"HOUR({params.source_col}) AS {prefix}_hour")

    select_clause = ", ".join(expressions)
    sql_query = f"SELECT *, {select_clause} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 6. Normalize Schema
# -------------------------------------------------------------------------


class NormalizeSchemaParams(BaseModel):
    """
    Configuration for schema normalization.

    Example:
    ```yaml
    normalize_schema:
      rename:
        old_col: "new_col"
      drop: ["unused_col"]
      select_order: ["id", "new_col", "created_at"]
    ```
    """

    rename: Optional[Dict[str, str]] = Field(
        default_factory=dict, description="old_name -> new_name"
    )
    drop: Optional[List[str]] = Field(
        default_factory=list, description="Columns to remove; ignored if not present"
    )
    select_order: Optional[List[str]] = Field(
        None, description="Final column order; any missing columns appended after"
    )


def normalize_schema(context: EngineContext, params: NormalizeSchemaParams) -> EngineContext:
    """
    Structural transformation to rename, drop, and reorder columns.

    Note: This is one of the few that might behave better with native API in some cases,
    but SQL projection handles it perfectly and is consistent.
    """
    current_cols = context.columns

    # 1. Determine columns to keep (exclude dropped)
    cols_to_keep = [c for c in current_cols if c not in (params.drop or [])]

    # 2. Prepare projection with renames
    projection = []

    # Helper to get SQL expr for a column
    def get_col_expr(col_name: str) -> str:
        if params.rename and col_name in params.rename:
            return f"{col_name} AS {params.rename[col_name]}"
        return col_name

    def get_final_name(col_name: str) -> str:
        if params.rename and col_name in params.rename:
            return params.rename[col_name]
        return col_name

    # 3. Reordering logic
    if params.select_order:
        # Use the user's strict order
        for target_col in params.select_order:
            # Find which source column maps to this target
            # This inverse lookup is a bit complex if we renamed
            # Simplification: We assume select_order uses the FINAL names

            found = False
            # Check if it's a renamed column
            if params.rename:
                for old, new in params.rename.items():
                    if new == target_col:
                        projection.append(f"{old} AS {new}")
                        found = True
                        break

            if not found:
                # Must be an original column
                projection.append(target_col)
    else:
        # Use existing order of kept columns
        for col in cols_to_keep:
            projection.append(get_col_expr(col))

    sql_query = f"SELECT {', '.join(projection)} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 7. Sort
# -------------------------------------------------------------------------


class SortParams(BaseModel):
    """
    Configuration for sorting.

    Example:
    ```yaml
    sort:
      by: ["created_at", "id"]
      ascending: false
    ```
    """

    by: Union[str, List[str]] = Field(..., description="Column(s) to sort by")
    ascending: bool = Field(True, description="Sort order")


def sort(context: EngineContext, params: SortParams) -> EngineContext:
    """
    Sort the dataset by one or more columns.

    Parameters
    ----------
    context : EngineContext
        The engine context containing the DataFrame to sort.
    params : SortParams
        Parameters specifying columns to sort by and sort order.

    Returns
    -------
    EngineContext
        The updated engine context with the sorted DataFrame.
    """
    cols = [params.by] if isinstance(params.by, str) else params.by
    direction = "ASC" if params.ascending else "DESC"
    # Apply direction to all columns for simplicity
    order_clause = ", ".join([f"{col} {direction}" for col in cols])

    return context.sql(f"SELECT * FROM df ORDER BY {order_clause}")


# -------------------------------------------------------------------------
# 8. Limit / Sample
# -------------------------------------------------------------------------


class LimitParams(BaseModel):
    """
    Configuration for result limiting.

    Example:
    ```yaml
    limit:
      n: 100
      offset: 0
    ```
    """

    n: int = Field(..., description="Number of rows to return")
    offset: int = Field(0, description="Number of rows to skip")


def limit(context: EngineContext, params: LimitParams) -> EngineContext:
    """
    Limit the number of rows returned from the dataset.

    Parameters
    ----------
    context : EngineContext
        The engine context containing the DataFrame to limit.
    params : LimitParams
        Parameters specifying the number of rows to return and the offset.

    Returns
    -------
    EngineContext
        The updated engine context with the limited DataFrame.
    """
    return context.sql(f"SELECT * FROM df LIMIT {params.n} OFFSET {params.offset}")


class SampleParams(BaseModel):
    """
    Configuration for random sampling.

    Example:
    ```yaml
    sample:
      fraction: 0.1
      seed: 42
    ```
    """

    fraction: float = Field(..., description="Fraction of rows to return (0.0 to 1.0)")
    seed: Optional[int] = None


def sample(context: EngineContext, params: SampleParams) -> EngineContext:
    """
    Return a random sample of rows from the dataset.

    Parameters
    ----------
    context : EngineContext
        The engine context containing the DataFrame to sample from.
    params : SampleParams
        Parameters specifying the fraction of rows to return and the random seed.

    Returns
    -------
    EngineContext
        The updated engine context with the sampled DataFrame.
    """
    # Generic SQL sampling: WHERE rand() < fraction
    # Spark uses rand(), DuckDB (Pandas) uses random()

    func = "rand()"
    from odibi.enums import EngineType

    if context.engine_type == EngineType.PANDAS:
        func = "random()"

    sql_query = f"SELECT * FROM df WHERE {func} < {params.fraction}"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 9. Distinct
# -------------------------------------------------------------------------


class DistinctParams(BaseModel):
    """
    Configuration for distinct rows.

    Example:
    ```yaml
    distinct:
      columns: ["category", "status"]
    ```
    """

    columns: Optional[List[str]] = Field(
        None, description="Columns to project (if None, keeps all columns unique)"
    )


def distinct(context: EngineContext, params: DistinctParams) -> EngineContext:
    """
    Return unique rows from the dataset using SQL DISTINCT.

    Parameters
    ----------
    context : EngineContext
        The engine context containing the DataFrame to deduplicate.
    params : DistinctParams
        Parameters specifying which columns to consider for uniqueness. If None, all columns are used.

    Returns
    -------
    EngineContext
        The updated engine context with duplicate rows removed.
    """
    if params.columns:
        cols = ", ".join(params.columns)
        return context.sql(f"SELECT DISTINCT {cols} FROM df")
    else:
        return context.sql("SELECT DISTINCT * FROM df")


# -------------------------------------------------------------------------
# 10. Fill Nulls
# -------------------------------------------------------------------------


class FillNullsParams(BaseModel):
    """
    Configuration for filling null values.

    Example:
    ```yaml
    fill_nulls:
      values:
        count: 0
        description: "N/A"
    ```
    """

    # key: column, value: fill value (str, int, float, bool)
    values: Dict[str, Union[str, int, float, bool]] = Field(
        ..., description="Map of column to fill value"
    )


def fill_nulls(context: EngineContext, params: FillNullsParams) -> EngineContext:
    """
    Replaces null values with specified defaults using COALESCE.
    """
    current_cols = context.columns
    projection = []

    for col in current_cols:
        if col in params.values:
            fill_val = params.values[col]
            # Quote string values
            if isinstance(fill_val, str):
                fill_val = f"'{fill_val}'"
            # Boolean to SQL
            elif isinstance(fill_val, bool):
                fill_val = "TRUE" if fill_val else "FALSE"

            projection.append(f"COALESCE({col}, {fill_val}) AS {col}")
        else:
            projection.append(col)

    sql_query = f"SELECT {', '.join(projection)} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 11. Split Part
# -------------------------------------------------------------------------


class SplitPartParams(BaseModel):
    """
    Configuration for splitting strings.

    Example:
    ```yaml
    split_part:
      col: "email"
      delimiter: "@"
      index: 2  # Extracts domain
    ```
    """

    col: str = Field(..., description="Column to split")
    delimiter: str = Field(..., description="Delimiter to split by")
    index: int = Field(..., description="1-based index of the token to extract")


def split_part(context: EngineContext, params: SplitPartParams) -> EngineContext:
    """
    Extracts the Nth part of a string after splitting by a delimiter.
    """
    import re

    from odibi.enums import EngineType

    if context.engine_type == EngineType.SPARK:
        # Spark: element_at(split(col, delimiter), index)
        # Note: Spark's split function uses Regex. We escape the delimiter to treat it as a literal.
        safe_delimiter = re.escape(params.delimiter).replace("\\", "\\\\")
        expr = f"element_at(split({params.col}, '{safe_delimiter}'), {params.index})"
    else:
        # DuckDB / Postgres / Standard: split_part(col, delimiter, index)
        expr = f"split_part({params.col}, '{params.delimiter}', {params.index})"

    sql_query = f"SELECT *, {expr} AS {params.col}_part_{params.index} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 12. Date Add
# -------------------------------------------------------------------------


class DateAddParams(BaseModel):
    """
    Configuration for date addition.

    Example:
    ```yaml
    date_add:
      col: "created_at"
      value: 1
      unit: "day"
    ```
    """

    col: str
    value: int
    unit: Literal["day", "month", "year", "hour", "minute", "second"]


def date_add(context: EngineContext, params: DateAddParams) -> EngineContext:
    """
    Adds an interval to a date/timestamp column.
    """
    # Standard SQL: col + INTERVAL 'value' unit
    # DuckDB supports this. Spark supports this.

    expr = f"{params.col} + INTERVAL {params.value} {params.unit}"
    target_col = f"{params.col}_future"

    sql_query = f"SELECT *, {expr} AS {target_col} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 13. Date Trunc
# -------------------------------------------------------------------------


class DateTruncParams(BaseModel):
    """
    Configuration for date truncation.

    Example:
    ```yaml
    date_trunc:
      col: "created_at"
      unit: "month"
    ```
    """

    col: str
    unit: Literal["year", "month", "day", "hour", "minute", "second"]


def date_trunc(context: EngineContext, params: DateTruncParams) -> EngineContext:
    """
    Truncates a date/timestamp to the specified precision.
    """
    # Standard SQL: date_trunc('unit', col)
    # DuckDB: date_trunc('unit', col)
    # Spark: date_trunc('unit', col)

    expr = f"date_trunc('{params.unit}', {params.col})"
    target_col = f"{params.col}_trunc"

    sql_query = f"SELECT *, {expr} AS {target_col} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 14. Date Diff
# -------------------------------------------------------------------------


class DateDiffParams(BaseModel):
    """
    Configuration for date difference.

    Example:
    ```yaml
    date_diff:
      start_col: "created_at"
      end_col: "updated_at"
      unit: "day"
    ```
    """

    start_col: str
    end_col: str
    unit: Literal["day", "hour", "minute", "second"] = "day"


def date_diff(context: EngineContext, params: DateDiffParams) -> EngineContext:
    """
    Calculates difference between two dates/timestamps.
    Returns the elapsed time in the specified unit (as float for sub-day units).
    """
    from odibi.enums import EngineType

    if context.engine_type == EngineType.SPARK:
        if params.unit == "day":
            # Spark datediff returns days (integer)
            expr = f"datediff({params.end_col}, {params.start_col})"
        else:
            # For hours/minutes, convert difference in seconds
            diff_sec = f"(unix_timestamp({params.end_col}) - unix_timestamp({params.start_col}))"
            if params.unit == "hour":
                expr = f"({diff_sec} / 3600.0)"
            elif params.unit == "minute":
                expr = f"({diff_sec} / 60.0)"
            else:
                expr = diff_sec
    else:
        # DuckDB
        if params.unit == "day":
            expr = f"date_diff('day', {params.start_col}, {params.end_col})"
        else:
            # For elapsed time semantics (consistent with Spark math), use seconds diff / factor
            diff_sec = f"date_diff('second', {params.start_col}, {params.end_col})"
            if params.unit == "hour":
                expr = f"({diff_sec} / 3600.0)"
            elif params.unit == "minute":
                expr = f"({diff_sec} / 60.0)"
            else:
                expr = diff_sec

    target_col = f"diff_{params.unit}"
    sql_query = f"SELECT *, {expr} AS {target_col} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 15. Case When
# -------------------------------------------------------------------------


class CaseWhenCase(BaseModel):
    condition: str
    value: str


class CaseWhenParams(BaseModel):
    """
    Configuration for conditional logic.

    Example:
    ```yaml
    case_when:
      output_col: "age_group"
      default: "'Adult'"
      cases:
        - condition: "age < 18"
          value: "'Minor'"
        - condition: "age > 65"
          value: "'Senior'"
    ```
    """

    # List of (condition, value) tuples
    cases: List[CaseWhenCase] = Field(..., description="List of conditional branches")
    default: str = Field("NULL", description="Default value if no condition met")
    output_col: str = Field(..., description="Name of the resulting column")


def case_when(context: EngineContext, params: CaseWhenParams) -> EngineContext:
    """
    Implements structured CASE WHEN logic.
    """
    when_clauses = []
    for case in params.cases:
        condition = case.condition
        value = case.value
        if condition and value:
            when_clauses.append(f"WHEN {condition} THEN {value}")

    full_case = f"CASE {' '.join(when_clauses)} ELSE {params.default} END"

    sql_query = f"SELECT *, {full_case} AS {params.output_col} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 16. Convert Timezone
# -------------------------------------------------------------------------


class ConvertTimezoneParams(BaseModel):
    """
    Configuration for timezone conversion.

    Example:
    ```yaml
    convert_timezone:
      col: "utc_time"
      source_tz: "UTC"
      target_tz: "America/New_York"
    ```
    """

    col: str = Field(..., description="Timestamp column to convert")
    source_tz: str = Field("UTC", description="Source timezone (e.g., 'UTC', 'America/New_York')")
    target_tz: str = Field(..., description="Target timezone (e.g., 'America/Los_Angeles')")
    output_col: Optional[str] = Field(
        None, description="Name of the result column (default: {col}_{target_tz})"
    )


def convert_timezone(context: EngineContext, params: ConvertTimezoneParams) -> EngineContext:
    """
    Converts a timestamp from one timezone to another.
    Assumes the input column is a naive timestamp representing time in source_tz,
    or a timestamp with timezone.
    """
    from odibi.enums import EngineType

    target = params.output_col or f"{params.col}_converted"

    if context.engine_type == EngineType.SPARK:
        # Spark: from_utc_timestamp(to_utc_timestamp(col, source_tz), target_tz)
        # logic:
        # 1. Interpret 'col' as being in 'source_tz', convert to UTC instant -> to_utc_timestamp(col, source)
        # 2. Render that instant in 'target_tz' -> from_utc_timestamp(instant, target)

        expr = f"from_utc_timestamp(to_utc_timestamp({params.col}, '{params.source_tz}'), '{params.target_tz}')"

    else:
        # DuckDB / Postgres
        # Logic:
        # 1. Interpret 'col' as timestamp in source_tz -> col AT TIME ZONE source_tz (Creates TIMESTAMPTZ)
        # 2. Convert that TIMESTAMPTZ to local time in target_tz -> AT TIME ZONE target_tz (Creates TIMESTAMP)

        # Note: We assume the input is NOT already a TIMESTAMPTZ. If it is, the first cast might be redundant but usually safe.
        # We cast to TIMESTAMP first to ensure we start with "Naive" interpretation.

        expr = f"({params.col}::TIMESTAMP AT TIME ZONE '{params.source_tz}') AT TIME ZONE '{params.target_tz}'"

    sql_query = f"SELECT *, {expr} AS {target} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 17. Concat Columns
# -------------------------------------------------------------------------


class ConcatColumnsParams(BaseModel):
    """
    Configuration for string concatenation.

    Example:
    ```yaml
    concat_columns:
      columns: ["first_name", "last_name"]
      separator: " "
      output_col: "full_name"
    ```
    """

    columns: List[str] = Field(..., description="Columns to concatenate")
    separator: str = Field("", description="Separator string")
    output_col: str = Field(..., description="Resulting column name")


def concat_columns(context: EngineContext, params: ConcatColumnsParams) -> EngineContext:
    """
    Concatenates multiple columns into one string.
    NULLs are skipped (treated as empty string) using CONCAT_WS behavior.
    """
    # Logic: CONCAT_WS(separator, col1, col2...)
    # Both Spark and DuckDB support CONCAT_WS with skip-null behavior.

    cols_str = ", ".join(params.columns)

    # Note: Spark CONCAT_WS requires separator as first arg.
    # DuckDB CONCAT_WS requires separator as first arg.

    expr = f"concat_ws('{params.separator}', {cols_str})"

    sql_query = f"SELECT *, {expr} AS {params.output_col} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 18. Select Columns
# -------------------------------------------------------------------------


class SelectColumnsParams(BaseModel):
    """
    Configuration for selecting specific columns (whitelist).

    Example:
    ```yaml
    select_columns:
      columns: ["id", "name", "created_at"]
    ```
    """

    columns: List[str] = Field(..., description="List of column names to keep")


def select_columns(context: EngineContext, params: SelectColumnsParams) -> EngineContext:
    """
    Keeps only the specified columns, dropping all others.
    """
    cols_str = ", ".join(params.columns)
    sql_query = f"SELECT {cols_str} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 19. Drop Columns
# -------------------------------------------------------------------------


class DropColumnsParams(BaseModel):
    """
    Configuration for dropping specific columns (blacklist).

    Example:
    ```yaml
    drop_columns:
      columns: ["_internal_id", "_temp_flag", "_processing_date"]
    ```
    """

    columns: List[str] = Field(..., description="List of column names to drop")


def drop_columns(context: EngineContext, params: DropColumnsParams) -> EngineContext:
    """
    Removes the specified columns from the DataFrame.
    """
    # Use EXCEPT syntax (Spark) or EXCLUDE (DuckDB)
    from odibi.enums import EngineType

    drop_cols = ", ".join(params.columns)

    if context.engine_type == EngineType.PANDAS:
        # DuckDB uses EXCLUDE
        sql_query = f"SELECT * EXCLUDE ({drop_cols}) FROM df"
    else:
        # Spark uses EXCEPT
        sql_query = f"SELECT * EXCEPT ({drop_cols}) FROM df"

    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 20. Rename Columns
# -------------------------------------------------------------------------


class RenameColumnsParams(BaseModel):
    """
    Configuration for bulk column renaming.

    Example:
    ```yaml
    rename_columns:
      mapping:
        customer_id: cust_id
        order_date: date
        total_amount: amount
    ```
    """

    mapping: Dict[str, str] = Field(..., description="Map of old column name to new column name")


def rename_columns(context: EngineContext, params: RenameColumnsParams) -> EngineContext:
    """
    Renames columns according to the provided mapping.
    Columns not in the mapping are kept unchanged.
    """
    # Build SELECT with aliases for renamed columns
    current_cols = context.columns
    select_parts = []

    for col in current_cols:
        if col in params.mapping:
            select_parts.append(f"{col} AS {params.mapping[col]}")
        else:
            select_parts.append(col)

    cols_str = ", ".join(select_parts)
    sql_query = f"SELECT {cols_str} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 21. Add Prefix
# -------------------------------------------------------------------------


class AddPrefixParams(BaseModel):
    """
    Configuration for adding a prefix to column names.

    Example - All columns:
    ```yaml
    add_prefix:
      prefix: "src_"
    ```

    Example - Specific columns:
    ```yaml
    add_prefix:
      prefix: "raw_"
      columns: ["id", "name", "value"]
    ```
    """

    prefix: str = Field(..., description="Prefix to add to column names")
    columns: Optional[List[str]] = Field(
        None, description="Columns to prefix (default: all columns)"
    )
    exclude: Optional[List[str]] = Field(None, description="Columns to exclude from prefixing")


def add_prefix(context: EngineContext, params: AddPrefixParams) -> EngineContext:
    """
    Adds a prefix to column names.
    """
    current_cols = context.columns
    target_cols = params.columns or current_cols
    exclude_cols = set(params.exclude or [])

    select_parts = []
    for col in current_cols:
        if col in target_cols and col not in exclude_cols:
            select_parts.append(f"{col} AS {params.prefix}{col}")
        else:
            select_parts.append(col)

    cols_str = ", ".join(select_parts)
    sql_query = f"SELECT {cols_str} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 22. Add Suffix
# -------------------------------------------------------------------------


class AddSuffixParams(BaseModel):
    """
    Configuration for adding a suffix to column names.

    Example - All columns:
    ```yaml
    add_suffix:
      suffix: "_raw"
    ```

    Example - Specific columns:
    ```yaml
    add_suffix:
      suffix: "_v2"
      columns: ["id", "name", "value"]
    ```
    """

    suffix: str = Field(..., description="Suffix to add to column names")
    columns: Optional[List[str]] = Field(
        None, description="Columns to suffix (default: all columns)"
    )
    exclude: Optional[List[str]] = Field(None, description="Columns to exclude from suffixing")


def add_suffix(context: EngineContext, params: AddSuffixParams) -> EngineContext:
    """
    Adds a suffix to column names.
    """
    current_cols = context.columns
    target_cols = params.columns or current_cols
    exclude_cols = set(params.exclude or [])

    select_parts = []
    for col in current_cols:
        if col in target_cols and col not in exclude_cols:
            select_parts.append(f"{col} AS {col}{params.suffix}")
        else:
            select_parts.append(col)

    cols_str = ", ".join(select_parts)
    sql_query = f"SELECT {cols_str} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 23. Normalize Column Names
# -------------------------------------------------------------------------


class NormalizeColumnNamesParams(BaseModel):
    """
    Configuration for normalizing column names.

    Example:
    ```yaml
    normalize_column_names:
      style: "snake_case"
      lowercase: true
    ```
    """

    style: Literal["snake_case", "none"] = Field(
        "snake_case",
        description="Naming style: 'snake_case' converts spaces/special chars to underscores",
    )
    lowercase: bool = Field(True, description="Convert names to lowercase")
    remove_special: bool = Field(True, description="Remove special characters except underscores")


def normalize_column_names(
    context: EngineContext, params: NormalizeColumnNamesParams
) -> EngineContext:
    """
    Normalizes column names to a consistent style.

    Useful for cleaning up messy source data with spaces, mixed case, or special
    characters. Converts names like "First Name" or "firstName" to "first_name".

    Args:
        context: Current execution context with dataframe
        params: Normalization configuration (style, lowercase, special char handling)

    Returns:
        New context with standardized column names

    Example:
        Convert all columns to lowercase snake_case:
        ```yaml
        normalize_column_names:
          style: "snake_case"
          lowercase: true
          remove_special: true
        ```
    """
    import re

    current_cols = context.columns
    select_parts = []

    for col in current_cols:
        new_name = col

        # Apply lowercase
        if params.lowercase:
            new_name = new_name.lower()

        # Apply snake_case (replace spaces and special chars with underscores)
        if params.style == "snake_case":
            # Replace spaces, dashes, dots with underscores
            new_name = re.sub(r"[\s\-\.]+", "_", new_name)
            # Insert underscore before uppercase letters (camelCase to snake_case)
            new_name = re.sub(r"([a-z])([A-Z])", r"\1_\2", new_name).lower()

        # Remove special characters
        if params.remove_special:
            new_name = re.sub(r"[^a-zA-Z0-9_]", "", new_name)
            # Remove consecutive underscores
            new_name = re.sub(r"_+", "_", new_name)
            # Remove leading/trailing underscores
            new_name = new_name.strip("_")

        if new_name != col:
            select_parts.append(f'"{col}" AS {new_name}')
        else:
            select_parts.append(f'"{col}"')

    cols_str = ", ".join(select_parts)
    sql_query = f"SELECT {cols_str} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 24. Coalesce Columns
# -------------------------------------------------------------------------


class CoalesceColumnsParams(BaseModel):
    """
    Configuration for coalescing columns (first non-null value).

    Example - Phone number fallback:
    ```yaml
    coalesce_columns:
      columns: ["mobile_phone", "work_phone", "home_phone"]
      output_col: "primary_phone"
    ```

    Example - Timestamp fallback:
    ```yaml
    coalesce_columns:
      columns: ["updated_at", "modified_at", "created_at"]
      output_col: "last_change_at"
    ```
    """

    columns: List[str] = Field(..., description="List of columns to coalesce (in priority order)")
    output_col: str = Field(..., description="Name of the output column")
    drop_source: bool = Field(False, description="Drop the source columns after coalescing")


def coalesce_columns(context: EngineContext, params: CoalesceColumnsParams) -> EngineContext:
    """
    Returns the first non-null value from a list of columns.

    Useful for fallback/priority scenarios where you want to use the first available
    value from multiple columns (e.g., primary phone, backup phone, home phone).

    Args:
        context: Current execution context with dataframe
        params: Coalesce configuration (columns list, output name, drop source option)

    Returns:
        New context with coalesced column added

    Example:
        Create a primary phone column from multiple fallback options:
        ```yaml
        coalesce_columns:
          columns: ["mobile_phone", "work_phone", "home_phone"]
          output_col: "primary_phone"
          drop_source: true
        ```
    """
    from odibi.enums import EngineType

    cols_str = ", ".join(params.columns)
    expr = f"COALESCE({cols_str}) AS {params.output_col}"

    if params.drop_source:
        drop_cols = ", ".join(params.columns)
        if context.engine_type == EngineType.PANDAS:
            sql_query = f"SELECT * EXCLUDE ({drop_cols}), {expr} FROM df"
        else:
            sql_query = f"SELECT * EXCEPT ({drop_cols}), {expr} FROM df"
    else:
        sql_query = f"SELECT *, {expr} FROM df"

    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 25. Replace Values
# -------------------------------------------------------------------------


class ReplaceValuesParams(BaseModel):
    """
    Configuration for bulk value replacement.

    Example - Standardize nulls:
    ```yaml
    replace_values:
      columns: ["status", "category"]
      mapping:
        "N/A": null
        "": null
        "Unknown": null
    ```

    Example - Code replacement:
    ```yaml
    replace_values:
      columns: ["country_code"]
      mapping:
        "US": "USA"
        "UK": "GBR"
    ```
    """

    columns: List[str] = Field(..., description="Columns to apply replacements to")
    mapping: Dict[str, Optional[str]] = Field(
        ..., description="Map of old value to new value (use null for NULL)"
    )


def replace_values(context: EngineContext, params: ReplaceValuesParams) -> EngineContext:
    """
    Replaces values in specified columns according to the mapping.

    Supports replacing specific values with new values or NULL. Useful for data
    standardization (e.g., "N/A" -> NULL, "Unknown" -> NULL).

    Args:
        context: Current execution context with dataframe
        params: Replacement configuration (columns and value mapping)

    Returns:
        New context with replaced values

    Example:
        Standardize missing value indicators:
        ```yaml
        replace_values:
          columns: ["status", "category"]
          mapping:
            "N/A": null
            "Unknown": null
            "PENDING": "pending"
        ```
    """
    current_cols = context.columns
    select_parts = []

    for col in current_cols:
        if col in params.columns:
            # Build nested CASE WHEN for replacements
            case_parts = []
            for old_val, new_val in params.mapping.items():
                if old_val == "":
                    case_parts.append(f"WHEN {col} = '' THEN {_sql_value(new_val)}")
                else:
                    case_parts.append(f"WHEN {col} = '{old_val}' THEN {_sql_value(new_val)}")

            if case_parts:
                case_expr = f"CASE {' '.join(case_parts)} ELSE {col} END AS {col}"
                select_parts.append(case_expr)
            else:
                select_parts.append(col)
        else:
            select_parts.append(col)

    cols_str = ", ".join(select_parts)
    sql_query = f"SELECT {cols_str} FROM df"
    return context.sql(sql_query)


def _sql_value(val: Optional[str]) -> str:
    """
    Convert a Python value to a SQL literal for use in SQL expressions.

    Parameters
    ----------
    val : Optional[str]
        The Python value to convert. If None, returns the SQL NULL literal.
        Otherwise, returns the value as a single-quoted SQL string.

    Returns
    -------
    str
        The SQL literal representation of the input value.
    """
    if val is None:
        return "NULL"
    return f"'{str(val).replace(chr(39), chr(39) + chr(39))}'"


# -------------------------------------------------------------------------
# 26. Trim Whitespace
# -------------------------------------------------------------------------


class TrimWhitespaceParams(BaseModel):
    """
    Configuration for trimming whitespace from string columns.

    Example - All string columns:
    ```yaml
    trim_whitespace: {}
    ```

    Example - Specific columns:
    ```yaml
    trim_whitespace:
      columns: ["name", "address", "city"]
    ```
    """

    columns: Optional[List[str]] = Field(
        None,
        description="Columns to trim (default: all string columns detected at runtime)",
    )


def trim_whitespace(context: EngineContext, params: TrimWhitespaceParams) -> EngineContext:
    """
    Trims leading and trailing whitespace from string columns.

    If no columns are specified, applies to all columns (SQL TRIM handles
    non-strings gracefully). Useful for cleaning up data from sources with
    inconsistent spacing.

    Args:
        context: Current execution context with dataframe
        params: Trim configuration (optional column list)

    Returns:
        New context with trimmed string columns

    Example:
        Trim specific columns:
        ```yaml
        trim_whitespace:
          columns: ["name", "address", "city"]
        ```

        Trim all string columns:
        ```yaml
        trim_whitespace: {}
        ```
    """
    current_cols = context.columns
    target_cols = params.columns

    # If no columns specified, we trim all columns (SQL TRIM handles non-strings gracefully in most cases)
    if target_cols is None:
        target_cols = current_cols

    select_parts = []
    for col in current_cols:
        if col in target_cols:
            select_parts.append(f"TRIM({col}) AS {col}")
        else:
            select_parts.append(col)

    cols_str = ", ".join(select_parts)
    sql_query = f"SELECT {cols_str} FROM df"
    return context.sql(sql_query)

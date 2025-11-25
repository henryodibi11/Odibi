from typing import Dict, List, Literal, Optional, Union
from enum import Enum

from pydantic import BaseModel, Field

from odibi.context import EngineContext

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
    sql_query = f"SELECT * FROM df WHERE {params.condition}"
    return context.sql(sql_query)


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
    expressions = [f"{expr} AS {col}" for col, expr in params.derivations.items()]
    select_clause = ", ".join(expressions)

    sql_query = f"SELECT *, {select_clause} FROM df"
    return context.sql(sql_query)


# -------------------------------------------------------------------------
# 3. Cast Columns
# -------------------------------------------------------------------------


class SimpleType(str, Enum):
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
    Sorts the dataset.
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
    Limits result size.
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
    Samples data using random filtering.
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
    Returns unique rows (SELECT DISTINCT).
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

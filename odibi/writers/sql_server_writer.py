"""SQL Server MERGE and overwrite writer for incremental sync operations.

Phase 1: Spark → SQL Server MERGE via staging table.
Phase 2: Enhanced overwrite strategies and validations.
Phase 3: Pandas engine support.
Phase 4: Polars engine support, auto schema/table creation, schema evolution, batch processing.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from odibi.config import (
    SqlServerAuditColsConfig,
    SqlServerMergeOptions,
    SqlServerMergeValidationConfig,
    SqlServerOverwriteOptions,
    SqlServerOverwriteStrategy,
    SqlServerSchemaEvolutionMode,
)
from odibi.utils.logging_context import get_logging_context


# Type mapping for schema inference
POLARS_TO_SQL_TYPE_MAP: Dict[str, str] = {
    "Int8": "TINYINT",
    "Int16": "SMALLINT",
    "Int32": "INT",
    "Int64": "BIGINT",
    "UInt8": "TINYINT",
    "UInt16": "SMALLINT",
    "UInt32": "INT",
    "UInt64": "BIGINT",
    "Float32": "REAL",
    "Float64": "FLOAT",
    "Boolean": "BIT",
    "Utf8": "NVARCHAR(MAX)",
    "String": "NVARCHAR(MAX)",
    "Date": "DATE",
    "Datetime": "DATETIME2",
    "Time": "TIME",
    "Duration": "BIGINT",
    "Binary": "VARBINARY(MAX)",
    "Null": "NVARCHAR(1)",
}

PANDAS_TO_SQL_TYPE_MAP: Dict[str, str] = {
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INT",
    "int64": "BIGINT",
    "uint8": "TINYINT",
    "uint16": "SMALLINT",
    "uint32": "INT",
    "uint64": "BIGINT",
    "float16": "REAL",
    "float32": "REAL",
    "float64": "FLOAT",
    "bool": "BIT",
    "boolean": "BIT",
    "object": "NVARCHAR(MAX)",
    "string": "NVARCHAR(MAX)",
    "datetime64[ns]": "DATETIME2",
    "datetime64[us]": "DATETIME2",
    "timedelta64[ns]": "BIGINT",
    "category": "NVARCHAR(MAX)",
}


@dataclass
class MergeResult:
    """Result of a SQL Server MERGE operation."""

    inserted: int = 0
    updated: int = 0
    deleted: int = 0

    @property
    def total_affected(self) -> int:
        return self.inserted + self.updated + self.deleted


@dataclass
class OverwriteResult:
    """Result of a SQL Server overwrite operation."""

    rows_written: int = 0
    strategy: str = "truncate_insert"


@dataclass
class ValidationResult:
    """Result of data validation checks."""

    is_valid: bool = True
    null_key_count: int = 0
    duplicate_key_count: int = 0
    errors: List[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class SqlServerMergeWriter:
    """
    Executes SQL Server MERGE and overwrite operations.

    Supports:
    - MERGE via staging table pattern
    - Enhanced overwrite with multiple strategies
    - Data validations (null keys, duplicate keys)
    - Both Spark and Pandas DataFrames
    """

    def __init__(self, connection: Any):
        """
        Initialize the writer with a SQL Server connection.

        Args:
            connection: Connection object with execute_sql and get_spark_options methods
        """
        self.connection = connection
        self.ctx = get_logging_context()

    def get_staging_table_name(self, target_table: str, staging_schema: str) -> str:
        """
        Generate staging table name from target table.

        Args:
            target_table: Target table name (e.g., 'oee.oee_fact')
            staging_schema: Schema for staging table

        Returns:
            Staging table name (e.g., '[staging].[oee_fact_staging]')
        """
        if "." in target_table:
            _, table_name = target_table.split(".", 1)
        else:
            table_name = target_table

        table_name = table_name.strip("[]")
        return f"[{staging_schema}].[{table_name}_staging]"

    def escape_column(self, col: str) -> str:
        """Escape column name for SQL Server."""
        col = col.strip("[]")
        return f"[{col}]"

    def parse_table_name(self, table: str) -> Tuple[str, str]:
        """
        Parse table name into schema and table parts.

        Args:
            table: Table name (e.g., 'oee.oee_fact' or 'oee_fact')

        Returns:
            Tuple of (schema, table_name)
        """
        if "." in table:
            schema, table_name = table.split(".", 1)
        else:
            schema = "dbo"
            table_name = table

        schema = schema.strip("[]")
        table_name = table_name.strip("[]")
        return schema, table_name

    def get_escaped_table_name(self, table: str) -> str:
        """Get fully escaped table name."""
        schema, table_name = self.parse_table_name(table)
        return f"[{schema}].[{table_name}]"

    def check_table_exists(self, table: str) -> bool:
        """
        Check if a table exists in SQL Server.

        Args:
            table: Table name (e.g., 'oee.oee_fact')

        Returns:
            True if table exists
        """
        schema, table_name = self.parse_table_name(table)
        sql = f"""
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
        """
        result = self.connection.execute_sql(sql)
        # Result is now a list of rows (fetchall already called in AzureSQL.execute)
        row = result[0] if result else None
        return row is not None

    def read_target_hashes(
        self,
        target_table: str,
        merge_keys: List[str],
        hash_column: str,
    ) -> List[Dict[str, Any]]:
        """
        Read merge keys and hash column from target table for incremental comparison.

        Args:
            target_table: Target table name
            merge_keys: Key columns
            hash_column: Hash column name

        Returns:
            List of dicts with keys and hash values, or empty list if hash column missing
        """
        escaped_table = self.get_escaped_table_name(target_table)

        # Check if hash column exists in target table before querying
        existing_columns = self.get_table_columns(target_table)
        if existing_columns and hash_column not in existing_columns:
            self.ctx.info(
                "Hash column not found in target table, skipping incremental comparison",
                hash_column=hash_column,
                target_table=target_table,
            )
            return []

        key_cols = ", ".join([self.escape_column(k) for k in merge_keys])
        hash_col = self.escape_column(hash_column)

        sql = f"SELECT {key_cols}, {hash_col} FROM {escaped_table}"
        self.ctx.debug("Reading target hashes for incremental merge", table=target_table)

        result = self.connection.execute_sql(sql)
        if not result:
            return []

        # Convert SQLAlchemy Row objects to dicts for Spark compatibility
        # Row objects have _mapping attribute or can be accessed via _asdict()
        dicts = []
        for row in result:
            if hasattr(row, "_asdict"):
                dicts.append(row._asdict())
            elif hasattr(row, "_mapping"):
                dicts.append(dict(row._mapping))
            else:
                # Fallback: assume row is dict-like or tuple with known columns
                columns = merge_keys + [hash_column]
                dicts.append(dict(zip(columns, row)))
        return dicts

    def get_hash_column_name(
        self,
        df_columns: List[str],
        options_hash_column: Optional[str],
    ) -> Optional[str]:
        """
        Determine which hash column to use for incremental merge.

        Args:
            df_columns: List of DataFrame column names
            options_hash_column: Explicitly configured hash column

        Returns:
            Hash column name or None if not available
        """
        if options_hash_column:
            if options_hash_column in df_columns:
                return options_hash_column
            else:
                self.ctx.warning(
                    f"Configured hash_column '{options_hash_column}' not found in DataFrame"
                )
                return None

        # Auto-detect common hash column names
        for candidate in ["_hash_diff", "_hash", "hash_diff", "row_hash"]:
            if candidate in df_columns:
                self.ctx.debug(f"Auto-detected hash column: {candidate}")
                return candidate

        return None

    def compute_hash_spark(
        self, df: Any, columns: List[str], hash_col_name: str = "_computed_hash"
    ):
        """
        Compute hash column for Spark DataFrame.

        Args:
            df: Spark DataFrame
            columns: Columns to include in hash
            hash_col_name: Name for the computed hash column

        Returns:
            DataFrame with hash column added
        """
        from pyspark.sql import functions as F

        # Concatenate columns and compute MD5 hash
        concat_expr = F.concat_ws(
            "||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in columns]
        )
        return df.withColumn(hash_col_name, F.md5(concat_expr))

    def compute_hash_pandas(
        self, df: Any, columns: List[str], hash_col_name: str = "_computed_hash"
    ):
        """
        Compute hash column for Pandas DataFrame.

        Args:
            df: Pandas DataFrame
            columns: Columns to include in hash
            hash_col_name: Name for the computed hash column

        Returns:
            DataFrame with hash column added
        """
        import hashlib

        def row_hash(row):
            concat = "||".join(str(row[c]) if row[c] is not None else "NULL" for c in columns)
            return hashlib.md5(concat.encode()).hexdigest()

        df = df.copy()
        df[hash_col_name] = df.apply(row_hash, axis=1)
        return df

    def compute_hash_polars(
        self, df: Any, columns: List[str], hash_col_name: str = "_computed_hash"
    ):
        """
        Compute hash column for Polars DataFrame.

        Args:
            df: Polars DataFrame
            columns: Columns to include in hash
            hash_col_name: Name for the computed hash column

        Returns:
            DataFrame with hash column added
        """
        import polars as pl

        # Concatenate columns and compute hash
        concat_expr = pl.concat_str(
            [pl.col(c).cast(pl.Utf8).fill_null("NULL") for c in columns],
            separator="||",
        )
        return df.with_columns(concat_expr.hash().cast(pl.Utf8).alias(hash_col_name))

    def filter_changed_rows_spark(
        self,
        source_df: Any,
        target_hashes: List[Dict[str, Any]],
        merge_keys: List[str],
        hash_column: str,
    ):
        """
        Filter Spark DataFrame to only rows that are new or changed.

        Args:
            source_df: Source Spark DataFrame
            target_hashes: List of dicts with target keys and hashes
            merge_keys: Key columns
            hash_column: Hash column name

        Returns:
            Filtered DataFrame with only new/changed rows
        """
        from pyspark.sql import functions as F

        if not target_hashes:
            # No existing data, all rows are new
            return source_df

        # Get SparkSession from DataFrame
        spark = source_df.sparkSession

        # Create DataFrame from target hashes
        target_df = spark.createDataFrame(target_hashes)

        # Rename hash column in target to avoid collision
        target_hash_col = f"_target_{hash_column}"
        target_df = target_df.withColumnRenamed(hash_column, target_hash_col)

        # Left join source with target on merge keys
        join_condition = [source_df[k] == target_df[k] for k in merge_keys]
        joined = source_df.join(target_df, join_condition, "left")

        # Filter to rows where:
        # 1. No match in target (new rows) - target hash is null
        # 2. Hash differs (changed rows)
        changed = joined.filter(
            F.col(target_hash_col).isNull() | (F.col(hash_column) != F.col(target_hash_col))
        )

        # Drop the target columns
        for k in merge_keys:
            changed = changed.drop(target_df[k])
        changed = changed.drop(target_hash_col)

        return changed

    def filter_changed_rows_pandas(
        self,
        source_df: Any,
        target_hashes: List[Dict[str, Any]],
        merge_keys: List[str],
        hash_column: str,
    ):
        """
        Filter Pandas DataFrame to only rows that are new or changed.

        Args:
            source_df: Source Pandas DataFrame
            target_hashes: List of dicts with target keys and hashes
            merge_keys: Key columns
            hash_column: Hash column name

        Returns:
            Filtered DataFrame with only new/changed rows
        """
        import pandas as pd

        if not target_hashes:
            return source_df

        target_df = pd.DataFrame(target_hashes)
        target_hash_col = f"_target_{hash_column}"
        target_df = target_df.rename(columns={hash_column: target_hash_col})

        # Merge to find matching rows
        merged = source_df.merge(target_df, on=merge_keys, how="left")

        # Filter to new or changed rows
        is_new = merged[target_hash_col].isna()
        is_changed = merged[hash_column] != merged[target_hash_col]
        changed = merged[is_new | is_changed].copy()

        # Drop the target hash column
        changed = changed.drop(columns=[target_hash_col])

        return changed

    def filter_changed_rows_polars(
        self,
        source_df: Any,
        target_hashes: List[Dict[str, Any]],
        merge_keys: List[str],
        hash_column: str,
    ):
        """
        Filter Polars DataFrame to only rows that are new or changed.

        Args:
            source_df: Source Polars DataFrame
            target_hashes: List of dicts with target keys and hashes
            merge_keys: Key columns
            hash_column: Hash column name

        Returns:
            Filtered DataFrame with only new/changed rows
        """
        import polars as pl

        if not target_hashes:
            return source_df

        target_df = pl.DataFrame(target_hashes)
        target_hash_col = f"_target_{hash_column}"
        target_df = target_df.rename({hash_column: target_hash_col})

        # Join to find matching rows
        joined = source_df.join(target_df, on=merge_keys, how="left")

        # Filter to new or changed rows
        changed = joined.filter(
            pl.col(target_hash_col).is_null() | (pl.col(hash_column) != pl.col(target_hash_col))
        )

        # Drop the target hash column
        changed = changed.drop(target_hash_col)

        return changed

    def validate_keys_spark(
        self,
        df: Any,
        merge_keys: List[str],
        config: Optional[SqlServerMergeValidationConfig] = None,
    ) -> ValidationResult:
        """
        Validate merge keys in a Spark DataFrame.

        Args:
            df: Spark DataFrame
            merge_keys: Key columns to validate
            config: Validation configuration

        Returns:
            ValidationResult with validation status
        """
        config = config or SqlServerMergeValidationConfig()
        result = ValidationResult()

        if config.check_null_keys:
            from pyspark.sql import functions as F

            null_condition = F.lit(False)
            for key in merge_keys:
                null_condition = null_condition | F.col(key).isNull()

            null_count = df.filter(null_condition).count()
            if null_count > 0:
                result.null_key_count = null_count
                result.errors.append(
                    f"Found {null_count} rows with NULL values in merge keys: {merge_keys}"
                )
                result.is_valid = False

        if config.check_duplicate_keys:
            total_count = df.count()
            distinct_count = df.select(*merge_keys).distinct().count()
            duplicate_count = total_count - distinct_count

            if duplicate_count > 0:
                result.duplicate_key_count = duplicate_count
                result.errors.append(
                    f"Found {duplicate_count} duplicate key combinations in merge keys: {merge_keys}"
                )
                result.is_valid = False

        return result

    def validate_keys_pandas(
        self,
        df: Any,
        merge_keys: List[str],
        config: Optional[SqlServerMergeValidationConfig] = None,
    ) -> ValidationResult:
        """
        Validate merge keys in a Pandas DataFrame.

        Args:
            df: Pandas DataFrame
            merge_keys: Key columns to validate
            config: Validation configuration

        Returns:
            ValidationResult with validation status
        """
        config = config or SqlServerMergeValidationConfig()
        result = ValidationResult()

        if config.check_null_keys:
            null_mask = df[merge_keys].isnull().any(axis=1)
            null_count = null_mask.sum()

            if null_count > 0:
                result.null_key_count = int(null_count)
                result.errors.append(
                    f"Found {null_count} rows with NULL values in merge keys: {merge_keys}"
                )
                result.is_valid = False

        if config.check_duplicate_keys:
            duplicates = df.duplicated(subset=merge_keys, keep=False)
            duplicate_count = (
                duplicates.sum() - df.duplicated(subset=merge_keys, keep="first").sum()
            )

            if duplicate_count > 0:
                result.duplicate_key_count = int(duplicate_count)
                result.errors.append(
                    f"Found {duplicate_count} duplicate key combinations in merge keys: {merge_keys}"
                )
                result.is_valid = False

        return result

    def validate_keys_polars(
        self,
        df: Any,
        merge_keys: List[str],
        config: Optional[SqlServerMergeValidationConfig] = None,
    ) -> ValidationResult:
        """
        Validate merge keys in a Polars DataFrame/LazyFrame.

        Args:
            df: Polars DataFrame or LazyFrame
            merge_keys: Key columns to validate
            config: Validation configuration

        Returns:
            ValidationResult with validation status
        """
        try:
            import polars as pl
        except ImportError:
            raise ImportError("Polars not installed. Run 'pip install polars'.")

        config = config or SqlServerMergeValidationConfig()
        result = ValidationResult()

        is_lazy = isinstance(df, pl.LazyFrame)
        if is_lazy:
            df_materialized = df.collect()
        else:
            df_materialized = df

        if config.check_null_keys:
            null_condition = pl.lit(False)
            for key in merge_keys:
                null_condition = null_condition | pl.col(key).is_null()

            null_count = df_materialized.filter(null_condition).height

            if null_count > 0:
                result.null_key_count = null_count
                result.errors.append(
                    f"Found {null_count} rows with NULL values in merge keys: {merge_keys}"
                )
                result.is_valid = False

        if config.check_duplicate_keys:
            total_count = df_materialized.height
            distinct_count = df_materialized.select(merge_keys).unique().height
            duplicate_count = total_count - distinct_count

            if duplicate_count > 0:
                result.duplicate_key_count = duplicate_count
                result.errors.append(
                    f"Found {duplicate_count} duplicate key combinations in merge keys: {merge_keys}"
                )
                result.is_valid = False

        return result

    def check_schema_exists(self, schema: str) -> bool:
        """Check if a schema exists in SQL Server."""
        sql = f"SELECT 1 FROM sys.schemas WHERE name = '{schema}'"
        result = self.connection.execute_sql(sql)
        # Result is now a list of rows (fetchall already called in AzureSQL.execute)
        row = result[0] if result else None
        return row is not None

    def create_schema(self, schema: str) -> None:
        """Create a schema if it doesn't exist."""
        if not self.check_schema_exists(schema):
            sql = f"CREATE SCHEMA [{schema}]"
            self.ctx.info("Creating schema", schema=schema)
            self.connection.execute_sql(sql)

    def get_table_columns(self, table: str) -> Dict[str, str]:
        """
        Get column names and types for a table.

        Returns:
            Dictionary mapping column names to SQL types
        """
        schema, table_name = self.parse_table_name(table)
        sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        result = self.connection.execute_sql(sql)
        columns = {}
        for row in result:
            if isinstance(row, dict):
                columns[row["COLUMN_NAME"]] = row["DATA_TYPE"]
            else:
                columns[row[0]] = row[1]
        return columns

    def infer_sql_type_pandas(self, dtype: Any) -> str:
        """Infer SQL Server type from Pandas dtype."""
        dtype_str = str(dtype).lower()
        for pattern, sql_type in PANDAS_TO_SQL_TYPE_MAP.items():
            if pattern in dtype_str:
                return sql_type
        return "NVARCHAR(MAX)"

    def infer_sql_type_polars(self, dtype: Any) -> str:
        """Infer SQL Server type from Polars dtype."""
        dtype_str = str(dtype)
        for pattern, sql_type in POLARS_TO_SQL_TYPE_MAP.items():
            if pattern in dtype_str:
                return sql_type
        return "NVARCHAR(MAX)"

    def create_table_from_pandas(
        self,
        df: Any,
        table: str,
        audit_cols: Optional[SqlServerAuditColsConfig] = None,
    ) -> None:
        """
        Create a SQL Server table from Pandas DataFrame schema.

        Args:
            df: Pandas DataFrame
            table: Target table name
            audit_cols: Optional audit column config to add created_ts/updated_ts columns
        """
        schema, table_name = self.parse_table_name(table)
        columns = []
        existing_cols = set()
        for col_name, dtype in df.dtypes.items():
            sql_type = self.infer_sql_type_pandas(dtype)
            escaped_col = self.escape_column(col_name)
            columns.append(f"{escaped_col} {sql_type} NULL")
            existing_cols.add(col_name)

        if audit_cols:
            if audit_cols.created_col and audit_cols.created_col not in existing_cols:
                escaped_col = self.escape_column(audit_cols.created_col)
                columns.append(f"{escaped_col} DATETIME2 NULL")
                self.ctx.debug(f"Adding audit column: {audit_cols.created_col}")
            if audit_cols.updated_col and audit_cols.updated_col not in existing_cols:
                escaped_col = self.escape_column(audit_cols.updated_col)
                columns.append(f"{escaped_col} DATETIME2 NULL")
                self.ctx.debug(f"Adding audit column: {audit_cols.updated_col}")

        columns_sql = ",\n    ".join(columns)
        sql = f"CREATE TABLE [{schema}].[{table_name}] (\n    {columns_sql}\n)"
        self.ctx.info("Creating table from DataFrame", table=table)
        self.connection.execute_sql(sql)

    def create_table_from_polars(
        self,
        df: Any,
        table: str,
        audit_cols: Optional[SqlServerAuditColsConfig] = None,
    ) -> None:
        """
        Create a SQL Server table from Polars DataFrame schema.

        Args:
            df: Polars DataFrame or LazyFrame
            table: Target table name
            audit_cols: Optional audit column config to add created_ts/updated_ts columns
        """
        try:
            import polars as pl
        except ImportError:
            raise ImportError("Polars not installed. Run 'pip install polars'.")

        schema_name, table_name = self.parse_table_name(table)

        if isinstance(df, pl.LazyFrame):
            df_schema = df.collect_schema()
        else:
            df_schema = df.schema

        columns = []
        existing_cols = set()
        for col_name, dtype in df_schema.items():
            sql_type = self.infer_sql_type_polars(dtype)
            escaped_col = self.escape_column(col_name)
            columns.append(f"{escaped_col} {sql_type} NULL")
            existing_cols.add(col_name)

        if audit_cols:
            if audit_cols.created_col and audit_cols.created_col not in existing_cols:
                escaped_col = self.escape_column(audit_cols.created_col)
                columns.append(f"{escaped_col} DATETIME2 NULL")
                self.ctx.debug(f"Adding audit column: {audit_cols.created_col}")
            if audit_cols.updated_col and audit_cols.updated_col not in existing_cols:
                escaped_col = self.escape_column(audit_cols.updated_col)
                columns.append(f"{escaped_col} DATETIME2 NULL")
                self.ctx.debug(f"Adding audit column: {audit_cols.updated_col}")

        columns_sql = ",\n    ".join(columns)
        sql = f"CREATE TABLE [{schema_name}].[{table_name}] (\n    {columns_sql}\n)"
        self.ctx.info("Creating table from Polars DataFrame", table=table)
        self.connection.execute_sql(sql)

    def add_columns(self, table: str, new_columns: Dict[str, str]) -> None:
        """Add new columns to an existing table."""
        if not new_columns:
            return

        escaped_table = self.get_escaped_table_name(table)
        for col_name, sql_type in new_columns.items():
            escaped_col = self.escape_column(col_name)
            sql = f"ALTER TABLE {escaped_table} ADD {escaped_col} {sql_type} NULL"
            self.ctx.info("Adding column to table", table=table, column=col_name)
            self.connection.execute_sql(sql)

    def create_primary_key(self, table: str, columns: List[str]) -> None:
        """
        Create a clustered primary key on the specified columns.

        First makes columns NOT NULL (required for PK), then adds the constraint.

        Args:
            table: Table name (e.g., 'oee.oee_fact')
            columns: List of column names for the primary key
        """
        escaped_table = self.get_escaped_table_name(table)
        schema, table_name = self.parse_table_name(table)
        pk_name = f"PK_{table_name}"

        # Get column types so we can ALTER to NOT NULL
        existing_cols = self.get_table_columns(table)

        # First, make PK columns NOT NULL (required for primary key)
        for col in columns:
            escaped_col = self.escape_column(col)
            col_type = existing_cols.get(col, "NVARCHAR(MAX)")
            alter_sql = (
                f"ALTER TABLE {escaped_table} ALTER COLUMN {escaped_col} {col_type} NOT NULL"
            )
            self.ctx.debug(f"Setting column NOT NULL: {col}")
            self.connection.execute_sql(alter_sql)

        # Now create the primary key
        escaped_cols = ", ".join([self.escape_column(c) for c in columns])
        sql = f"""
        ALTER TABLE {escaped_table}
        ADD CONSTRAINT [{pk_name}] PRIMARY KEY CLUSTERED ({escaped_cols})
        """
        self.ctx.info(
            "Creating primary key",
            table=table,
            constraint=pk_name,
            columns=columns,
        )
        self.connection.execute_sql(sql)

    def create_index(self, table: str, columns: List[str], index_name: str = None) -> None:
        """
        Create a nonclustered index on the specified columns.

        Args:
            table: Table name (e.g., 'oee.oee_fact')
            columns: List of column names for the index
            index_name: Optional custom index name (auto-generated if not provided)
        """
        escaped_table = self.get_escaped_table_name(table)
        schema, table_name = self.parse_table_name(table)

        if index_name is None:
            col_suffix = "_".join(columns[:3])  # Use first 3 columns in name
            index_name = f"IX_{table_name}_{col_suffix}"

        escaped_cols = ", ".join([self.escape_column(c) for c in columns])

        sql = f"""
        CREATE NONCLUSTERED INDEX [{index_name}]
        ON {escaped_table} ({escaped_cols})
        """
        self.ctx.info(
            "Creating index",
            table=table,
            index=index_name,
            columns=columns,
        )
        self.connection.execute_sql(sql)

    def handle_schema_evolution_pandas(
        self, df: Any, table: str, evolution_config: Any
    ) -> List[str]:
        """
        Handle schema evolution for Pandas DataFrame.

        Returns list of columns to write (may be subset if mode=ignore).
        """
        if evolution_config is None:
            return list(df.columns)

        mode = evolution_config.mode
        existing_cols = self.get_table_columns(table)
        df_cols = set(df.columns)
        table_cols = set(existing_cols.keys())

        new_cols = df_cols - table_cols

        if mode == SqlServerSchemaEvolutionMode.STRICT:
            if new_cols:
                raise ValueError(
                    f"Schema evolution mode is 'strict' but DataFrame has new columns "
                    f"not in target table: {new_cols}"
                )
            return list(df.columns)

        elif mode == SqlServerSchemaEvolutionMode.EVOLVE:
            if new_cols and evolution_config.add_columns:
                new_cols_with_types = {}
                for col in new_cols:
                    new_cols_with_types[col] = self.infer_sql_type_pandas(df[col].dtype)
                self.add_columns(table, new_cols_with_types)
            return list(df.columns)

        elif mode == SqlServerSchemaEvolutionMode.IGNORE:
            return [c for c in df.columns if c in table_cols]

        return list(df.columns)

    def handle_schema_evolution_polars(
        self, df: Any, table: str, evolution_config: Any
    ) -> List[str]:
        """
        Handle schema evolution for Polars DataFrame.

        Returns list of columns to write (may be subset if mode=ignore).
        """
        try:
            import polars as pl
        except ImportError:
            raise ImportError("Polars not installed. Run 'pip install polars'.")

        if evolution_config is None:
            if isinstance(df, pl.LazyFrame):
                return list(df.collect_schema().names())
            return df.columns

        mode = evolution_config.mode
        existing_cols = self.get_table_columns(table)

        if isinstance(df, pl.LazyFrame):
            df_schema = df.collect_schema()
            df_cols = set(df_schema.names())
        else:
            df_schema = df.schema
            df_cols = set(df.columns)

        table_cols = set(existing_cols.keys())
        new_cols = df_cols - table_cols

        if mode == SqlServerSchemaEvolutionMode.STRICT:
            if new_cols:
                raise ValueError(
                    f"Schema evolution mode is 'strict' but DataFrame has new columns "
                    f"not in target table: {new_cols}"
                )
            return list(df_cols)

        elif mode == SqlServerSchemaEvolutionMode.EVOLVE:
            if new_cols and evolution_config.add_columns:
                new_cols_with_types = {}
                for col in new_cols:
                    new_cols_with_types[col] = self.infer_sql_type_polars(df_schema[col])
                self.add_columns(table, new_cols_with_types)
            return list(df_cols)

        elif mode == SqlServerSchemaEvolutionMode.IGNORE:
            return [c for c in df_cols if c in table_cols]

        return list(df_cols)

    def truncate_staging(self, staging_table: str) -> None:
        """
        Truncate staging table if it exists.

        Args:
            staging_table: Full staging table name (e.g., '[staging].[oee_fact_staging]')
        """
        sql = f"""
        IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL
            TRUNCATE TABLE {staging_table}
        """
        self.ctx.debug("Truncating staging table", staging_table=staging_table)
        self.connection.execute_sql(sql)

    def truncate_table(self, table: str) -> None:
        """Truncate a table."""
        escaped = self.get_escaped_table_name(table)
        sql = f"TRUNCATE TABLE {escaped}"
        self.ctx.debug("Truncating table", table=table)
        self.connection.execute_sql(sql)

    def delete_from_table(self, table: str) -> int:
        """Delete all rows from a table and return count."""
        escaped = self.get_escaped_table_name(table)
        sql = f"DELETE FROM {escaped}; SELECT @@ROWCOUNT AS deleted_count;"
        self.ctx.debug("Deleting from table", table=table)
        result = self.connection.execute_sql(sql)
        # Result is now a list of rows (fetchall already called in AzureSQL.execute)
        row = result[0] if result else None
        if row:
            return row.get("deleted_count", 0) if isinstance(row, dict) else row[0]
        return 0

    def drop_table(self, table: str) -> None:
        """Drop a table if it exists."""
        escaped = self.get_escaped_table_name(table)
        sql = f"DROP TABLE IF EXISTS {escaped}"
        self.ctx.debug("Dropping table", table=table)
        self.connection.execute_sql(sql)

    def build_merge_sql(
        self,
        target_table: str,
        staging_table: str,
        merge_keys: List[str],
        columns: List[str],
        options: Optional[SqlServerMergeOptions] = None,
    ) -> str:
        """
        Build T-SQL MERGE statement.

        Args:
            target_table: Target table name
            staging_table: Staging table name
            merge_keys: Key columns for ON clause
            columns: All columns in the DataFrame
            options: Merge options (conditions, audit cols, etc.)

        Returns:
            T-SQL MERGE statement
        """
        options = options or SqlServerMergeOptions()

        exclude_cols = set(options.exclude_columns)
        audit_created = options.audit_cols.created_col if options.audit_cols else None
        audit_updated = options.audit_cols.updated_col if options.audit_cols else None

        merge_cols = [c for c in columns if c not in exclude_cols]

        update_cols = [c for c in merge_cols if c not in merge_keys and c != audit_created]
        insert_cols = [c for c in merge_cols]

        on_clause = " AND ".join(
            [f"target.{self.escape_column(k)} = source.{self.escape_column(k)}" for k in merge_keys]
        )

        update_set_parts = []
        for col in update_cols:
            if col == audit_updated:
                update_set_parts.append(f"{self.escape_column(col)} = GETUTCDATE()")
            else:
                update_set_parts.append(
                    f"{self.escape_column(col)} = source.{self.escape_column(col)}"
                )
        update_set = ",\n            ".join(update_set_parts)

        insert_col_list = ", ".join([self.escape_column(c) for c in insert_cols])
        insert_value_parts = []
        for col in insert_cols:
            if col == audit_created or col == audit_updated:
                insert_value_parts.append("GETUTCDATE()")
            else:
                insert_value_parts.append(f"source.{self.escape_column(col)}")
        insert_values = ", ".join(insert_value_parts)

        target_escaped = self.get_escaped_table_name(target_table)

        sql_parts = [
            "DECLARE @MergeActions TABLE (action NVARCHAR(10));",
            "",
            f"MERGE {target_escaped} AS target",
            f"USING {staging_table} AS source",
            f"ON {on_clause}",
        ]

        if options.update_condition:
            sql_parts.append(f"WHEN MATCHED AND {options.update_condition} THEN")
        else:
            sql_parts.append("WHEN MATCHED THEN")

        sql_parts.append("    UPDATE SET")
        sql_parts.append(f"        {update_set}")

        if options.delete_condition:
            sql_parts.append(f"WHEN MATCHED AND {options.delete_condition} THEN")
            sql_parts.append("    DELETE")

        if options.insert_condition:
            sql_parts.append(f"WHEN NOT MATCHED BY TARGET AND {options.insert_condition} THEN")
        else:
            sql_parts.append("WHEN NOT MATCHED BY TARGET THEN")

        sql_parts.append(f"    INSERT ({insert_col_list})")
        sql_parts.append(f"    VALUES ({insert_values})")

        sql_parts.append("OUTPUT $action INTO @MergeActions;")
        sql_parts.append("")
        sql_parts.append("SELECT")
        sql_parts.append("    SUM(CASE WHEN action = 'INSERT' THEN 1 ELSE 0 END) AS inserted,")
        sql_parts.append("    SUM(CASE WHEN action = 'UPDATE' THEN 1 ELSE 0 END) AS updated,")
        sql_parts.append("    SUM(CASE WHEN action = 'DELETE' THEN 1 ELSE 0 END) AS deleted")
        sql_parts.append("FROM @MergeActions;")

        return "\n".join(sql_parts)

    def execute_merge(
        self,
        target_table: str,
        staging_table: str,
        merge_keys: List[str],
        columns: List[str],
        options: Optional[SqlServerMergeOptions] = None,
    ) -> MergeResult:
        """
        Execute MERGE operation and return counts.

        Args:
            target_table: Target table name
            staging_table: Staging table name
            merge_keys: Key columns for ON clause
            columns: All columns in the DataFrame
            options: Merge options

        Returns:
            MergeResult with insert/update/delete counts
        """
        sql = self.build_merge_sql(
            target_table=target_table,
            staging_table=staging_table,
            merge_keys=merge_keys,
            columns=columns,
            options=options,
        )

        self.ctx.debug(
            "Executing MERGE",
            target_table=target_table,
            staging_table=staging_table,
            merge_keys=merge_keys,
        )

        try:
            result = self.connection.execute_sql(sql)

            # Result is now a list of rows (fetchall already called in AzureSQL.execute)
            row = result[0] if result else None
            if row:
                if isinstance(row, dict):
                    merge_result = MergeResult(
                        inserted=row.get("inserted", 0) or 0,
                        updated=row.get("updated", 0) or 0,
                        deleted=row.get("deleted", 0) or 0,
                    )
                else:
                    merge_result = MergeResult(
                        inserted=row[0] or 0,
                        updated=row[1] or 0,
                        deleted=row[2] or 0,
                    )
            else:
                merge_result = MergeResult()

            self.ctx.info(
                "MERGE completed",
                target_table=target_table,
                inserted=merge_result.inserted,
                updated=merge_result.updated,
                deleted=merge_result.deleted,
                total_affected=merge_result.total_affected,
            )

            return merge_result

        except Exception as e:
            self.ctx.error(
                "MERGE failed",
                target_table=target_table,
                error_type=type(e).__name__,
                error_message=str(e),
            )
            raise

    def merge(
        self,
        df: Any,
        spark_engine: Any,
        target_table: str,
        merge_keys: List[str],
        options: Optional[SqlServerMergeOptions] = None,
        jdbc_options: Optional[Dict[str, Any]] = None,
    ) -> MergeResult:
        """
        Execute full merge operation: validation + staging write + MERGE.

        Args:
            df: Spark DataFrame to merge
            spark_engine: SparkEngine instance for writing to staging
            target_table: Target table name (e.g., 'oee.oee_fact')
            merge_keys: Key columns for ON clause
            options: Merge options
            jdbc_options: JDBC connection options

        Returns:
            MergeResult with counts
        """
        options = options or SqlServerMergeOptions()
        jdbc_options = jdbc_options or {}

        # Auto-create schema if needed
        if options.auto_create_schema:
            schema, _ = self.parse_table_name(target_table)
            if not self.check_schema_exists(schema):
                self.create_schema(schema)

        # Check if table exists, auto-create if configured
        if not self.check_table_exists(target_table):
            if options.auto_create_table:
                self.ctx.info(
                    "Auto-creating target table from Spark DataFrame",
                    target_table=target_table,
                )

                # Create table using JDBC write with overwrite mode (initial load)
                staging_jdbc_options = {**jdbc_options, "dbtable": target_table}
                df.write.format("jdbc").options(**staging_jdbc_options).mode("overwrite").save()

                row_count = df.count()

                # Add audit columns if configured (JDBC doesn't create them automatically)
                if options.audit_cols:
                    audit_cols_to_add = {}
                    existing_cols = self.get_table_columns(target_table)
                    if (
                        options.audit_cols.created_col
                        and options.audit_cols.created_col not in existing_cols
                    ):
                        audit_cols_to_add[options.audit_cols.created_col] = "DATETIME2"
                    if (
                        options.audit_cols.updated_col
                        and options.audit_cols.updated_col not in existing_cols
                    ):
                        audit_cols_to_add[options.audit_cols.updated_col] = "DATETIME2"
                    if audit_cols_to_add:
                        self.add_columns(target_table, audit_cols_to_add)

                    # Populate audit columns for all rows on first load
                    escaped_table = self.get_escaped_table_name(target_table)
                    update_parts = []
                    if options.audit_cols.created_col:
                        escaped_col = self.escape_column(options.audit_cols.created_col)
                        update_parts.append(f"{escaped_col} = GETUTCDATE()")
                    if options.audit_cols.updated_col:
                        escaped_col = self.escape_column(options.audit_cols.updated_col)
                        update_parts.append(f"{escaped_col} = GETUTCDATE()")
                    if update_parts:
                        update_sql = f"UPDATE {escaped_table} SET {', '.join(update_parts)}"
                        self.ctx.debug("Populating audit columns on initial load")
                        self.connection.execute_sql(update_sql)

                # Create primary key or index on merge keys if configured
                if options.primary_key_on_merge_keys:
                    self.create_primary_key(target_table, merge_keys)
                elif options.index_on_merge_keys:
                    self.create_index(target_table, merge_keys)

                self.ctx.info(
                    "Target table created and initial data loaded",
                    target_table=target_table,
                    rows=row_count,
                )
                # Return as if merge completed (all inserts)
                return MergeResult(inserted=row_count, updated=0, deleted=0)
            else:
                raise ValueError(
                    f"Target table '{target_table}' does not exist. "
                    "SQL Server MERGE mode requires the target table to exist. "
                    "Set auto_create_table=true or use mode='overwrite' for initial load."
                )

        if options.validations:
            validation_result = self.validate_keys_spark(df, merge_keys, options.validations)
            if not validation_result.is_valid:
                error_msg = "; ".join(validation_result.errors)
                if options.validations.fail_on_validation_error:
                    raise ValueError(f"Merge key validation failed: {error_msg}")
                else:
                    self.ctx.warning(f"Merge key validation warnings: {error_msg}")

        staging_table = self.get_staging_table_name(target_table, options.staging_schema)

        # Auto-create staging schema if needed
        if options.auto_create_schema:
            if not self.check_schema_exists(options.staging_schema):
                self.create_schema(options.staging_schema)

        self.ctx.info(
            "Starting SQL Server MERGE",
            target_table=target_table,
            staging_table=staging_table,
            merge_keys=merge_keys,
            incremental=options.incremental,
        )

        self.truncate_staging(staging_table)

        columns = list(df.columns)
        df_to_write = df

        if options.audit_cols:
            if options.audit_cols.created_col and options.audit_cols.created_col not in columns:
                columns.append(options.audit_cols.created_col)
            if options.audit_cols.updated_col and options.audit_cols.updated_col not in columns:
                columns.append(options.audit_cols.updated_col)

        # Incremental merge: filter to only changed rows before writing to staging
        if options.incremental:
            hash_column = self.get_hash_column_name(df.columns, options.hash_column)

            if hash_column is None and options.change_detection_columns:
                # Compute hash from specified columns
                hash_column = "_computed_hash"
                df_to_write = self.compute_hash_spark(
                    df, options.change_detection_columns, hash_column
                )
                columns.append(hash_column)
            elif hash_column is None:
                # Compute hash from all non-key columns
                non_key_cols = [c for c in df.columns if c not in merge_keys]
                if non_key_cols:
                    hash_column = "_computed_hash"
                    df_to_write = self.compute_hash_spark(df, non_key_cols, hash_column)
                    columns.append(hash_column)

            if hash_column:
                # Read target hashes and filter source
                target_hashes = self.read_target_hashes(target_table, merge_keys, hash_column)
                original_count = df_to_write.count()
                df_to_write = self.filter_changed_rows_spark(
                    df_to_write, target_hashes, merge_keys, hash_column
                )
                filtered_count = df_to_write.count()
                self.ctx.info(
                    "Incremental filter applied",
                    original_rows=original_count,
                    changed_rows=filtered_count,
                    skipped_rows=original_count - filtered_count,
                )

                if filtered_count == 0:
                    self.ctx.info("No changed rows detected, skipping merge")
                    return MergeResult(inserted=0, updated=0, deleted=0)

        staging_jdbc_options = {**jdbc_options, "dbtable": staging_table}
        df_to_write.write.format("jdbc").options(**staging_jdbc_options).mode("overwrite").save()

        self.ctx.debug("Staging write completed", staging_table=staging_table)

        result = self.execute_merge(
            target_table=target_table,
            staging_table=staging_table,
            merge_keys=merge_keys,
            columns=columns,
            options=options,
        )

        return result

    def merge_pandas(
        self,
        df: Any,
        target_table: str,
        merge_keys: List[str],
        options: Optional[SqlServerMergeOptions] = None,
    ) -> MergeResult:
        """
        Execute full merge operation for Pandas DataFrame.

        Args:
            df: Pandas DataFrame to merge
            target_table: Target table name (e.g., 'oee.oee_fact')
            merge_keys: Key columns for ON clause
            options: Merge options

        Returns:
            MergeResult with counts
        """
        options = options or SqlServerMergeOptions()

        schema, _ = self.parse_table_name(target_table)
        if options.auto_create_schema:
            self.create_schema(schema)

        table_exists = self.check_table_exists(target_table)
        if not table_exists:
            if options.auto_create_table:
                self.create_table_from_pandas(df, target_table, audit_cols=options.audit_cols)
                if options.primary_key_on_merge_keys:
                    self.create_primary_key(target_table, merge_keys)
                elif options.index_on_merge_keys:
                    self.create_index(target_table, merge_keys)
            else:
                raise ValueError(
                    f"Target table '{target_table}' does not exist. "
                    "SQL Server MERGE mode requires the target table to exist. "
                    "Set auto_create_table=true or use mode='overwrite' for initial load."
                )

        if options.validations:
            validation_result = self.validate_keys_pandas(df, merge_keys, options.validations)
            if not validation_result.is_valid:
                error_msg = "; ".join(validation_result.errors)
                if options.validations.fail_on_validation_error:
                    raise ValueError(f"Merge key validation failed: {error_msg}")
                else:
                    self.ctx.warning(f"Merge key validation warnings: {error_msg}")

        staging_table = self.get_staging_table_name(target_table, options.staging_schema)

        self.ctx.info(
            "Starting SQL Server MERGE (Pandas)",
            target_table=target_table,
            staging_table=staging_table,
            merge_keys=merge_keys,
            incremental=options.incremental,
        )

        columns = list(df.columns)
        df_to_write = df

        if options.audit_cols:
            if options.audit_cols.created_col and options.audit_cols.created_col not in columns:
                columns.append(options.audit_cols.created_col)
            if options.audit_cols.updated_col and options.audit_cols.updated_col not in columns:
                columns.append(options.audit_cols.updated_col)

        # Incremental merge: filter to only changed rows before writing to staging
        if options.incremental and table_exists:
            hash_column = self.get_hash_column_name(list(df.columns), options.hash_column)

            if hash_column is None and options.change_detection_columns:
                hash_column = "_computed_hash"
                df_to_write = self.compute_hash_pandas(
                    df, options.change_detection_columns, hash_column
                )
                columns.append(hash_column)
            elif hash_column is None:
                non_key_cols = [c for c in df.columns if c not in merge_keys]
                if non_key_cols:
                    hash_column = "_computed_hash"
                    df_to_write = self.compute_hash_pandas(df, list(non_key_cols), hash_column)
                    columns.append(hash_column)

            if hash_column:
                target_hashes = self.read_target_hashes(target_table, merge_keys, hash_column)
                original_count = len(df_to_write)
                df_to_write = self.filter_changed_rows_pandas(
                    df_to_write, target_hashes, merge_keys, hash_column
                )
                filtered_count = len(df_to_write)
                self.ctx.info(
                    "Incremental filter applied (Pandas)",
                    original_rows=original_count,
                    changed_rows=filtered_count,
                    skipped_rows=original_count - filtered_count,
                )

                if filtered_count == 0:
                    self.ctx.info("No changed rows detected, skipping merge")
                    return MergeResult(inserted=0, updated=0, deleted=0)

        schema, table_name = staging_table.strip("[]").split("].[")
        schema = schema.strip("[")
        table_name = table_name.strip("]")

        self.connection.write_table(
            df=df_to_write,
            table_name=table_name,
            schema=schema,
            if_exists="replace",
        )

        self.ctx.debug("Staging write completed (Pandas)", staging_table=staging_table)

        result = self.execute_merge(
            target_table=target_table,
            staging_table=staging_table,
            merge_keys=merge_keys,
            columns=columns,
            options=options,
        )

        return result

    def overwrite_spark(
        self,
        df: Any,
        target_table: str,
        options: Optional[SqlServerOverwriteOptions] = None,
        jdbc_options: Optional[Dict[str, Any]] = None,
    ) -> OverwriteResult:
        """
        Execute enhanced overwrite operation for Spark DataFrame.

        Args:
            df: Spark DataFrame to write
            target_table: Target table name
            options: Overwrite options
            jdbc_options: JDBC connection options

        Returns:
            OverwriteResult with row count
        """
        options = options or SqlServerOverwriteOptions()
        jdbc_options = jdbc_options or {}
        strategy = options.strategy

        self.ctx.info(
            "Starting SQL Server overwrite",
            target_table=target_table,
            strategy=strategy.value,
        )

        table_exists = self.check_table_exists(target_table)

        if strategy == SqlServerOverwriteStrategy.DROP_CREATE:
            if table_exists:
                self.drop_table(target_table)

            jdbc_options_with_table = {**jdbc_options, "dbtable": target_table}
            df.write.format("jdbc").options(**jdbc_options_with_table).mode("overwrite").save()

        elif strategy == SqlServerOverwriteStrategy.TRUNCATE_INSERT:
            if table_exists:
                self.truncate_table(target_table)
                jdbc_options_with_table = {**jdbc_options, "dbtable": target_table}
                df.write.format("jdbc").options(**jdbc_options_with_table).mode("append").save()
            else:
                jdbc_options_with_table = {**jdbc_options, "dbtable": target_table}
                df.write.format("jdbc").options(**jdbc_options_with_table).mode("overwrite").save()

        elif strategy == SqlServerOverwriteStrategy.DELETE_INSERT:
            if table_exists:
                self.delete_from_table(target_table)
                jdbc_options_with_table = {**jdbc_options, "dbtable": target_table}
                df.write.format("jdbc").options(**jdbc_options_with_table).mode("append").save()
            else:
                jdbc_options_with_table = {**jdbc_options, "dbtable": target_table}
                df.write.format("jdbc").options(**jdbc_options_with_table).mode("overwrite").save()

        row_count = df.count()

        self.ctx.info(
            "Overwrite completed",
            target_table=target_table,
            strategy=strategy.value,
            rows_written=row_count,
        )

        return OverwriteResult(rows_written=row_count, strategy=strategy.value)

    def overwrite_pandas(
        self,
        df: Any,
        target_table: str,
        options: Optional[SqlServerOverwriteOptions] = None,
    ) -> OverwriteResult:
        """
        Execute enhanced overwrite operation for Pandas DataFrame.

        Args:
            df: Pandas DataFrame to write
            target_table: Target table name
            options: Overwrite options

        Returns:
            OverwriteResult with row count
        """
        options = options or SqlServerOverwriteOptions()
        strategy = options.strategy

        self.ctx.info(
            "Starting SQL Server overwrite (Pandas)",
            target_table=target_table,
            strategy=strategy.value,
        )

        table_exists = self.check_table_exists(target_table)
        schema, table_name = self.parse_table_name(target_table)

        if strategy == SqlServerOverwriteStrategy.DROP_CREATE:
            if table_exists:
                self.drop_table(target_table)
            self.connection.write_table(
                df=df,
                table_name=table_name,
                schema=schema,
                if_exists="replace",
            )

        elif strategy == SqlServerOverwriteStrategy.TRUNCATE_INSERT:
            if table_exists:
                self.truncate_table(target_table)
                self.connection.write_table(
                    df=df,
                    table_name=table_name,
                    schema=schema,
                    if_exists="append",
                )
            else:
                self.connection.write_table(
                    df=df,
                    table_name=table_name,
                    schema=schema,
                    if_exists="replace",
                )

        elif strategy == SqlServerOverwriteStrategy.DELETE_INSERT:
            if table_exists:
                self.delete_from_table(target_table)
                self.connection.write_table(
                    df=df,
                    table_name=table_name,
                    schema=schema,
                    if_exists="append",
                )
            else:
                self.connection.write_table(
                    df=df,
                    table_name=table_name,
                    schema=schema,
                    if_exists="replace",
                )

        row_count = len(df)

        self.ctx.info(
            "Overwrite completed (Pandas)",
            target_table=target_table,
            strategy=strategy.value,
            rows_written=row_count,
        )

        return OverwriteResult(rows_written=row_count, strategy=strategy.value)

    def merge_polars(
        self,
        df: Any,
        target_table: str,
        merge_keys: List[str],
        options: Optional[SqlServerMergeOptions] = None,
    ) -> MergeResult:
        """
        Execute full merge operation for Polars DataFrame (Phase 4).

        Args:
            df: Polars DataFrame or LazyFrame to merge
            target_table: Target table name (e.g., 'oee.oee_fact')
            merge_keys: Key columns for ON clause
            options: Merge options

        Returns:
            MergeResult with counts
        """
        try:
            import polars as pl
        except ImportError:
            raise ImportError("Polars not installed. Run 'pip install polars'.")

        options = options or SqlServerMergeOptions()

        if isinstance(df, pl.LazyFrame):
            df = df.collect()

        schema, _ = self.parse_table_name(target_table)
        if options.auto_create_schema:
            self.create_schema(schema)

        table_exists = self.check_table_exists(target_table)
        if not table_exists:
            if options.auto_create_table:
                self.create_table_from_polars(df, target_table, audit_cols=options.audit_cols)
            else:
                raise ValueError(
                    f"Target table '{target_table}' does not exist. "
                    "SQL Server MERGE mode requires the target table to exist. "
                    "Set auto_create_table=true or use mode='overwrite' for initial load."
                )

        if options.schema_evolution and table_exists:
            columns = self.handle_schema_evolution_polars(
                df, target_table, options.schema_evolution
            )
        else:
            columns = list(df.columns)

        if options.audit_cols:
            if options.audit_cols.created_col and options.audit_cols.created_col not in columns:
                columns.append(options.audit_cols.created_col)
            if options.audit_cols.updated_col and options.audit_cols.updated_col not in columns:
                columns.append(options.audit_cols.updated_col)

        if options.validations:
            validation_result = self.validate_keys_polars(df, merge_keys, options.validations)
            if not validation_result.is_valid:
                error_msg = "; ".join(validation_result.errors)
                if options.validations.fail_on_validation_error:
                    raise ValueError(f"Merge key validation failed: {error_msg}")
                else:
                    self.ctx.warning(f"Merge key validation warnings: {error_msg}")

        staging_table = self.get_staging_table_name(target_table, options.staging_schema)
        staging_schema, staging_table_name = staging_table.strip("[]").split("].[")
        staging_schema = staging_schema.strip("[")
        staging_table_name = staging_table_name.strip("]")

        if options.auto_create_schema:
            self.create_schema(staging_schema)

        self.ctx.info(
            "Starting SQL Server MERGE (Polars)",
            target_table=target_table,
            staging_table=staging_table,
            merge_keys=merge_keys,
            incremental=options.incremental,
        )

        df_to_write = df

        # Incremental merge: filter to only changed rows before writing to staging
        if options.incremental and table_exists:
            hash_column = self.get_hash_column_name(df.columns, options.hash_column)

            if hash_column is None and options.change_detection_columns:
                hash_column = "_computed_hash"
                df_to_write = self.compute_hash_polars(
                    df, options.change_detection_columns, hash_column
                )
                columns.append(hash_column)
            elif hash_column is None:
                non_key_cols = [c for c in df.columns if c not in merge_keys]
                if non_key_cols:
                    hash_column = "_computed_hash"
                    df_to_write = self.compute_hash_polars(df, non_key_cols, hash_column)
                    columns.append(hash_column)

            if hash_column:
                target_hashes = self.read_target_hashes(target_table, merge_keys, hash_column)
                original_count = len(df_to_write)
                df_to_write = self.filter_changed_rows_polars(
                    df_to_write, target_hashes, merge_keys, hash_column
                )
                filtered_count = len(df_to_write)
                self.ctx.info(
                    "Incremental filter applied (Polars)",
                    original_rows=original_count,
                    changed_rows=filtered_count,
                    skipped_rows=original_count - filtered_count,
                )

                if filtered_count == 0:
                    self.ctx.info("No changed rows detected, skipping merge")
                    return MergeResult(inserted=0, updated=0, deleted=0)

        df_pandas = df_to_write.to_pandas()

        batch_size = options.batch_size
        if batch_size and len(df_pandas) > batch_size:
            for i in range(0, len(df_pandas), batch_size):
                chunk = df_pandas.iloc[i : i + batch_size]
                if_exists = "replace" if i == 0 else "append"
                self.connection.write_table(
                    df=chunk,
                    table_name=staging_table_name,
                    schema=staging_schema,
                    if_exists=if_exists,
                )
                self.ctx.debug(f"Wrote batch {i // batch_size + 1}", rows=len(chunk))
        else:
            self.connection.write_table(
                df=df_pandas,
                table_name=staging_table_name,
                schema=staging_schema,
                if_exists="replace",
            )

        self.ctx.debug("Staging write completed (Polars)", staging_table=staging_table)

        result = self.execute_merge(
            target_table=target_table,
            staging_table=staging_table,
            merge_keys=merge_keys,
            columns=columns,
            options=options,
        )

        return result

    def overwrite_polars(
        self,
        df: Any,
        target_table: str,
        options: Optional[SqlServerOverwriteOptions] = None,
    ) -> OverwriteResult:
        """
        Execute enhanced overwrite operation for Polars DataFrame (Phase 4).

        Args:
            df: Polars DataFrame or LazyFrame to write
            target_table: Target table name
            options: Overwrite options

        Returns:
            OverwriteResult with row count
        """
        try:
            import polars as pl
        except ImportError:
            raise ImportError("Polars not installed. Run 'pip install polars'.")

        options = options or SqlServerOverwriteOptions()
        strategy = options.strategy

        if isinstance(df, pl.LazyFrame):
            df = df.collect()

        schema, table_name = self.parse_table_name(target_table)
        if options.auto_create_schema:
            self.create_schema(schema)

        self.ctx.info(
            "Starting SQL Server overwrite (Polars)",
            target_table=target_table,
            strategy=strategy.value,
        )

        table_exists = self.check_table_exists(target_table)

        if options.auto_create_table and not table_exists:
            self.create_table_from_polars(df, target_table)
            table_exists = True

        if options.schema_evolution and table_exists:
            columns_to_write = self.handle_schema_evolution_polars(
                df, target_table, options.schema_evolution
            )
            df_to_write = df.select(columns_to_write)
        else:
            df_to_write = df

        df_pandas = df_to_write.to_pandas()

        batch_size = options.batch_size
        if strategy == SqlServerOverwriteStrategy.DROP_CREATE:
            if table_exists:
                self.drop_table(target_table)
            if batch_size and len(df_pandas) > batch_size:
                for i in range(0, len(df_pandas), batch_size):
                    chunk = df_pandas.iloc[i : i + batch_size]
                    if_exists = "replace" if i == 0 else "append"
                    self.connection.write_table(
                        df=chunk,
                        table_name=table_name,
                        schema=schema,
                        if_exists=if_exists,
                    )
            else:
                self.connection.write_table(
                    df=df_pandas,
                    table_name=table_name,
                    schema=schema,
                    if_exists="replace",
                )

        elif strategy == SqlServerOverwriteStrategy.TRUNCATE_INSERT:
            if table_exists:
                self.truncate_table(target_table)
                if batch_size and len(df_pandas) > batch_size:
                    for i in range(0, len(df_pandas), batch_size):
                        chunk = df_pandas.iloc[i : i + batch_size]
                        self.connection.write_table(
                            df=chunk,
                            table_name=table_name,
                            schema=schema,
                            if_exists="append",
                        )
                else:
                    self.connection.write_table(
                        df=df_pandas,
                        table_name=table_name,
                        schema=schema,
                        if_exists="append",
                    )
            else:
                self.connection.write_table(
                    df=df_pandas,
                    table_name=table_name,
                    schema=schema,
                    if_exists="replace",
                )

        elif strategy == SqlServerOverwriteStrategy.DELETE_INSERT:
            if table_exists:
                self.delete_from_table(target_table)
                if batch_size and len(df_pandas) > batch_size:
                    for i in range(0, len(df_pandas), batch_size):
                        chunk = df_pandas.iloc[i : i + batch_size]
                        self.connection.write_table(
                            df=chunk,
                            table_name=table_name,
                            schema=schema,
                            if_exists="append",
                        )
                else:
                    self.connection.write_table(
                        df=df_pandas,
                        table_name=table_name,
                        schema=schema,
                        if_exists="append",
                    )
            else:
                self.connection.write_table(
                    df=df_pandas,
                    table_name=table_name,
                    schema=schema,
                    if_exists="replace",
                )

        row_count = len(df)

        self.ctx.info(
            "Overwrite completed (Polars)",
            target_table=target_table,
            strategy=strategy.value,
            rows_written=row_count,
        )

        return OverwriteResult(rows_written=row_count, strategy=strategy.value)

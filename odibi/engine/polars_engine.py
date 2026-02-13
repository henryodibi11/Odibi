"""Polars engine implementation."""

import hashlib
import os
from typing import Any, Dict, List, Optional

try:
    import polars as pl
except ImportError:
    pl = None

try:
    import pyarrow as pa
except ImportError:
    pa = None

from odibi.context import Context
from odibi.engine.base import Engine
from odibi.enums import EngineType


class PolarsEngine(Engine):
    """Polars-based execution engine (High Performance)."""

    name = "polars"
    engine_type = EngineType.POLARS

    def __init__(
        self,
        connections: Optional[Dict[str, Any]] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize Polars engine.

        Args:
            connections: Dictionary of connection objects
            config: Engine configuration (optional)
        """
        if pl is None:
            raise ImportError("Polars not installed. Run 'pip install polars'.")

        self.connections = connections or {}
        self.config = config or {}

    def materialize(self, df: Any) -> Any:
        """Materialize lazy dataset into memory (DataFrame).

        Args:
            df: LazyFrame or DataFrame

        Returns:
            Materialized DataFrame (pl.DataFrame)
        """
        if isinstance(df, pl.LazyFrame):
            return df.collect()
        return df

    def read(
        self,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        streaming: bool = False,
        schema: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Any:
        """Read data using Polars (Lazy by default).

        Args:
            connection: Connection object providing base path and storage options.
            format: Data format (csv, parquet, delta, json, sql, sql_server, azure_sql).
            table: Table name (mutually exclusive with path).
            path: File path (mutually exclusive with table).
            streaming: Whether to enable streaming mode (uses scan methods when possible).
            schema: Optional schema specification for SQL queries.
            options: Additional read options to pass to Polars readers.
            **kwargs: Additional keyword arguments.

        Returns:
            pl.LazyFrame or pl.DataFrame
        """
        options = options or {}

        # SQL Server / Azure SQL Support
        if format in ["sql", "sql_server", "azure_sql"]:
            if not hasattr(connection, "read_table") and not hasattr(connection, "read_sql_query"):
                raise ValueError(
                    f"Cannot read SQL table: connection type '{type(connection).__name__}' "
                    "does not support SQL operations. Use a SQL-compatible connection."
                )
            table_name = table or path
            if not table_name:
                raise ValueError("SQL read requires 'table' or 'path' to specify the table name.")
            if "." in table_name:
                schema_name, tbl = table_name.split(".", 1)
            else:
                schema_name, tbl = "dbo", table_name

            # Check for incremental SQL filter in options
            sql_filter = options.get("filter")

            if sql_filter and hasattr(connection, "read_sql_query"):
                # Build query with WHERE clause for SQL pushdown
                if schema_name:
                    base_query = f"SELECT * FROM [{schema_name}].[{tbl}]"
                else:
                    base_query = f"SELECT * FROM [{tbl}]"
                full_query = f"{base_query} WHERE {sql_filter}"
                pdf = connection.read_sql_query(full_query)
            else:
                # read_table returns Pandas DataFrame
                pdf = connection.read_table(table_name=tbl, schema=schema_name)

            return pl.from_pandas(pdf).lazy()

        # Get full path
        if path:
            if connection:
                full_path = connection.get_path(path)
            else:
                full_path = path
        elif table:
            if connection:
                full_path = connection.get_path(table)
            else:
                raise ValueError(
                    f"Cannot read table '{table}': connection is required when using 'table' parameter. "
                    "Provide a valid connection object or use 'path' for file-based reads."
                )
        else:
            raise ValueError(
                "Read operation failed: neither 'path' nor 'table' was provided. "
                "Specify a file path or table name in your configuration."
            )

        # Handle glob patterns/lists
        # Polars scan methods often support glob strings directly.

        try:
            if format == "csv":
                # scan_csv supports glob patterns
                return pl.scan_csv(full_path, **options)

            elif format == "parquet":
                return pl.scan_parquet(full_path, **options)

            elif format == "json":
                # scan_ndjson for newline delimited json, read_json for standard
                # Assuming ndjson/jsonl for big data usually
                if options.get("json_lines", True):  # Default to ndjson scan
                    return pl.scan_ndjson(full_path, **options)
                else:
                    # Standard JSON doesn't support lazy scan well in all versions, fallback to read
                    return pl.read_json(full_path, **options).lazy()

            elif format == "delta":
                # scan_delta requires 'deltalake' extra usually or feature
                storage_options = options.get("storage_options", None)
                version = options.get("versionAsOf", None)

                # scan_delta is available in recent polars
                # It might accept storage_options in recent versions
                delta_opts = {}
                if storage_options:
                    delta_opts["storage_options"] = storage_options
                if version is not None:
                    delta_opts["version"] = version

                return pl.scan_delta(full_path, **delta_opts)

            elif format == "excel":
                # Excel format: delegate to Pandas (best Excel support), convert to Polars
                from odibi.engine.pandas_engine import PandasEngine
                from odibi.context import get_logging_context

                ctx = get_logging_context().with_context(engine="polars")
                ctx.debug("Reading Excel via Pandas engine (best Excel support)")

                # Get storage_options from connection for Azure/cloud authentication
                excel_storage_options = None
                if hasattr(connection, "pandas_storage_options"):
                    excel_storage_options = connection.pandas_storage_options()

                # Use Pandas engine for Excel reading
                pandas_engine = PandasEngine()
                pdf = pandas_engine._read_excel_with_patterns(
                    full_path,
                    sheet_pattern=options.pop("sheet_pattern", None),
                    sheet_pattern_case_sensitive=options.pop("sheet_pattern_case_sensitive", False),
                    add_source_file=options.pop("add_source_file", False),
                    is_glob="*" in str(full_path) or "?" in str(full_path),
                    ctx=ctx,
                    storage_options=excel_storage_options,
                    **options,
                )

                # Convert Pandas DataFrame to Polars LazyFrame
                ctx.info(f"Excel read completed (via Pandas): {path}", row_count=len(pdf))
                return pl.from_pandas(pdf).lazy()

            elif format == "api":
                # API format: delegate to Pandas (uses ApiFetcher), convert to Polars
                from odibi.connections.api_fetcher import create_api_fetcher
                from odibi.connections.http import HttpConnection
                from odibi.context import get_logging_context

                ctx = get_logging_context().with_context(engine="polars")
                ctx.debug("Reading API via ApiFetcher", endpoint=str(path))

                if not isinstance(connection, HttpConnection):
                    raise ValueError(
                        f"Cannot read API data: connection type '{type(connection).__name__}' "
                        "is not an HttpConnection. Use an HTTP connection for API format."
                    )

                # Extract API-specific options
                api_options = options.copy()
                params = api_options.pop("params", {})
                max_records = api_options.pop("max_records", None)
                method = api_options.pop("method", "GET")
                request_body = api_options.pop("request_body", None)

                # Create fetcher with connection's base_url and headers
                fetcher = create_api_fetcher(
                    base_url=connection.base_url,
                    headers=connection.headers,
                    options=api_options,
                )

                # Fetch data as Pandas DataFrame
                pdf = fetcher.fetch_dataframe(
                    endpoint=str(full_path),
                    params=params,
                    max_records=max_records,
                    method=method,
                    request_body=request_body,
                )

                ctx.info(f"API read completed: {path}", row_count=len(pdf))
                return pl.from_pandas(pdf).lazy()

            else:
                raise ValueError(
                    f"Unsupported format for Polars engine: '{format}'. "
                    "Supported formats: csv, parquet, json, delta, excel, api, sql, sql_server, azure_sql."
                )

        except Exception as e:
            raise ValueError(
                f"Failed to read {format} from '{full_path}': {e}. "
                "Check that the file exists, the format is correct, and you have read permissions."
            )

    def write(
        self,
        df: Any,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        mode: str = "overwrite",
        options: Optional[Dict[str, Any]] = None,
        streaming_config: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Write data using Polars.

        Args:
            df: DataFrame or LazyFrame to write.
            connection: Connection object providing base path and storage options.
            format: Output format (csv, parquet, delta, json, sql, sql_server, azure_sql).
            table: Table name (mutually exclusive with path).
            path: File path (mutually exclusive with table).
            mode: Write mode (overwrite, append, upsert, append_once).
            options: Additional write options to pass to Polars writers.
            streaming_config: Streaming configuration (not used in Polars engine).

        Returns:
            Optional dict with write statistics for Delta writes, None otherwise.
        """
        options = options or {}

        if format in ["sql", "sql_server", "azure_sql"]:
            return self._write_sql(df, connection, table, mode, options)

        if path:
            if connection:
                full_path = connection.get_path(path)
            else:
                full_path = path
        elif table:
            if connection:
                full_path = connection.get_path(table)
            else:
                raise ValueError(
                    f"Cannot write to table '{table}': connection is required when using 'table' parameter. "
                    "Provide a valid connection object or use 'path' for file-based writes."
                )
        else:
            raise ValueError(
                "Write operation failed: neither 'path' nor 'table' was provided. "
                "Specify a file path or table name in your configuration."
            )

        is_lazy = isinstance(df, pl.LazyFrame)

        parent_dir = os.path.dirname(full_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        if format == "parquet":
            if is_lazy:
                df.sink_parquet(full_path, **options)
            else:
                df.write_parquet(full_path, **options)

        elif format == "csv":
            if is_lazy:
                df.sink_csv(full_path, **options)
            else:
                df.write_csv(full_path, **options)

        elif format == "json":
            if is_lazy:
                df.sink_ndjson(full_path, **options)
            else:
                df.write_ndjson(full_path, **options)

        elif format == "delta":
            if is_lazy:
                df = df.collect()

            storage_options = options.get("storage_options", None)
            delta_write_options = options.copy()
            if "storage_options" in delta_write_options:
                del delta_write_options["storage_options"]

            df.write_delta(
                full_path, mode=mode, storage_options=storage_options, **delta_write_options
            )

        else:
            raise ValueError(
                f"Unsupported write format for Polars engine: '{format}'. "
                "Supported formats: csv, parquet, json, delta."
            )

        return None

    def _write_sql(
        self,
        df: Any,
        connection: Any,
        table: Optional[str],
        mode: str,
        options: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Handle SQL writing including merge and enhanced overwrite for Polars (Phase 4)."""
        from odibi.utils.logging_context import get_logging_context

        ctx = get_logging_context().with_context(engine="polars")

        if not hasattr(connection, "write_table"):
            raise ValueError(
                f"Connection type '{type(connection).__name__}' does not support SQL operations"
            )

        if not table:
            raise ValueError(
                "SQL write operation failed: 'table' parameter is required but was not provided. "
                "Specify the target table name in your configuration."
            )

        if mode == "merge":
            merge_keys = options.get("merge_keys")
            merge_options = options.get("merge_options")

            if not merge_keys:
                raise ValueError(
                    "MERGE mode requires 'merge_keys' in options. "
                    "Specify the key columns for the MERGE ON clause."
                )

            from odibi.writers.sql_server_writer import SqlServerMergeWriter

            writer = SqlServerMergeWriter(connection)
            ctx.debug(
                "Executing SQL Server MERGE (Polars)",
                target=table,
                merge_keys=merge_keys,
            )

            result = writer.merge_polars(
                df=df,
                target_table=table,
                merge_keys=merge_keys,
                options=merge_options,
            )

            ctx.info(
                "SQL Server MERGE completed (Polars)",
                target=table,
                inserted=result.inserted,
                updated=result.updated,
                deleted=result.deleted,
            )

            return {
                "mode": "merge",
                "inserted": result.inserted,
                "updated": result.updated,
                "deleted": result.deleted,
                "total_affected": result.total_affected,
            }

        if mode == "overwrite" and options.get("overwrite_options"):
            from odibi.writers.sql_server_writer import SqlServerMergeWriter

            overwrite_options = options.get("overwrite_options")
            writer = SqlServerMergeWriter(connection)

            ctx.debug(
                "Executing SQL Server enhanced overwrite (Polars)",
                target=table,
                strategy=(
                    overwrite_options.strategy.value
                    if hasattr(overwrite_options, "strategy")
                    else "truncate_insert"
                ),
            )

            result = writer.overwrite_polars(
                df=df,
                target_table=table,
                options=overwrite_options,
            )

            ctx.info(
                "SQL Server enhanced overwrite completed (Polars)",
                target=table,
                strategy=result.strategy,
                rows_written=result.rows_written,
            )

            return {
                "mode": "overwrite",
                "strategy": result.strategy,
                "rows_written": result.rows_written,
            }

        if isinstance(df, pl.LazyFrame):
            df = df.collect()

        if "." in table:
            schema, table_name = table.split(".", 1)
        else:
            schema, table_name = "dbo", table

        if_exists = "replace"
        if mode == "append":
            if_exists = "append"
        elif mode == "fail":
            if_exists = "fail"

        df_pandas = df.to_pandas()
        chunksize = options.get("chunksize", 1000)

        connection.write_table(
            df=df_pandas,
            table_name=table_name,
            schema=schema,
            if_exists=if_exists,
            chunksize=chunksize,
        )
        return None

    def execute_sql(self, sql: str, context: Context) -> Any:
        """Execute SQL query using Polars SQLContext.

        Args:
            sql: SQL query string
            context: Execution context with registered DataFrames

        Returns:
            pl.LazyFrame
        """
        ctx = pl.SQLContext()

        # Register datasets from context
        # We iterate over all registered names in the context
        try:
            names = context.list_names()
            for name in names:
                df = context.get(name)
                # Register LazyFrame or DataFrame
                # Polars SQLContext supports registering LazyFrame, DataFrame, and some others
                # We might need to convert if it's not a Polars object, but we assume Polars engine uses Polars objects
                ctx.register(name, df)
        except Exception:
            # If context doesn't support listing or getting, we proceed with empty context
            # (e.g. if context is not fully compatible or empty)
            pass

        return ctx.execute(sql, eager=False)

    def execute_operation(self, operation: str, params: Dict[str, Any], df: Any) -> Any:
        """Execute built-in operation.

        Args:
            operation: Name of the operation to execute (e.g., 'pivot', 'unpivot', 'explode').
            params: Dictionary of parameters specific to the operation.
            df: DataFrame or LazyFrame to operate on.

        Returns:
            Transformed DataFrame or LazyFrame.
        """
        # Ensure LazyFrame for consistency if possible, but operations work on both usually.
        # If DataFrame, some operations might need different methods.

        if operation == "pivot":
            # Pivot requires materialization usually in other engines, but Polars LazyFrame has 'collect' or similar constraints?
            # Polars lazy pivot is not fully supported in older versions without collect, but check recent.
            # Pivot changes shape drastically.
            # params: pivot_column, value_column, group_by, agg_func

            # If lazy, we might need to collect for pivot if lazy pivot isn't supported or experimental.
            # But let's try to keep it lazy if possible.
            # As of recent Polars, pivot is available on DataFrame, experimental on LazyFrame?
            # Actually, 'unstack' or 'pivot' on LazyFrame is limited.
            # Safe bet: materialize if needed, or use lazy pivot if available.

            # Let's collect if input is lazy, because pivot usually implies strict schema change hard to predict.
            if isinstance(df, pl.LazyFrame):
                df = df.collect()

            return df.pivot(
                index=params.get("group_by"),
                on=params["pivot_column"],
                values=params["value_column"],
                aggregate_function=params.get("agg_func", "first"),
            )  # Returns DataFrame

        elif operation == "drop_duplicates":
            subset = params.get("subset")
            if isinstance(df, pl.LazyFrame):
                return df.unique(subset=subset)
            return df.unique(subset=subset)

        elif operation == "fillna":
            value = params.get("value")
            # Polars uses fill_null
            if isinstance(value, dict):
                # Fill specific columns
                # value = {'col1': 0, 'col2': 'unknown'}
                # We need to chain with_columns
                exprs = []
                for col, val in value.items():
                    exprs.append(pl.col(col).fill_null(val))
                return df.with_columns(exprs)
            else:
                # Fill all columns? Polars fill_null requires specifying columns or using all()
                return df.fill_null(value)

        elif operation == "drop":
            columns = params.get("columns") or params.get("labels")
            return df.drop(columns)

        elif operation == "rename":
            columns = params.get("columns") or params.get("mapper")
            return df.rename(columns)

        elif operation == "sort":
            by = params.get("by")
            descending = not params.get("ascending", True)
            if isinstance(df, pl.LazyFrame):
                return df.sort(by, descending=descending)
            return df.sort(by, descending=descending)

        elif operation == "sample":
            # Sample n or frac
            n = params.get("n")
            frac = params.get("frac")
            seed = params.get("random_state")

            # Lazy sample supported
            if n is not None:
                # Note: Polars Lazy sample might be approximate or require 'collect' depending on version/backend?
                # But usually supported.
                if isinstance(df, pl.LazyFrame):
                    # LazyFrame.sample takes n (int) or fraction.
                    # But polars 0.19+ changed sample signature?
                    # It's generally `sample(n=..., fraction=..., seed=...)`
                    return (
                        df.collect().sample(n=n, seed=seed).lazy()
                    )  # Collecting for exact sample n on lazy might be needed if not supported?
                    # Actually, fetch(n) is head. Sample is random.
                    # Let's materialize for safety with sample as it's often for checks.
                    pass
                return df.sample(n=n, seed=seed)
            elif frac is not None:
                if isinstance(df, pl.LazyFrame):
                    # Lazy sampling by fraction is supported
                    pass  # fall through
                return df.sample(fraction=frac, seed=seed)

        elif operation == "filter":
            # Legacy or simple filter
            pass

        else:
            # Fallback: check if operation is a registered transformer
            from odibi.context import EngineContext, PandasContext
            from odibi.registry import FunctionRegistry

            if FunctionRegistry.has_function(operation):
                func = FunctionRegistry.get_function(operation)
                param_model = FunctionRegistry.get_param_model(operation)

                # Create EngineContext from current df (use PandasContext as placeholder)
                engine_ctx = EngineContext(
                    context=PandasContext(),
                    df=df,
                    engine=self,
                    engine_type=self.engine_type,
                )

                # Validate and instantiate params
                if param_model:
                    validated_params = param_model(**params)
                    result_ctx = func(engine_ctx, validated_params)
                else:
                    result_ctx = func(engine_ctx, **params)

                return result_ctx.df

        return df

    def get_schema(self, df: Any) -> Any:
        """Get DataFrame schema.

        Args:
            df: DataFrame or LazyFrame to get schema from.

        Returns:
            Dictionary mapping column names to data type strings.
        """
        # Polars schema is a dict {name: DataType}
        # We can return a dict of strings for compatibility
        schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema
        return {name: str(dtype) for name, dtype in schema.items()}

    def get_shape(self, df: Any) -> tuple:
        """Get DataFrame shape.

        Args:
            df: DataFrame or LazyFrame to get shape from.

        Returns:
            Tuple of (rows, columns) as integers.
        """
        if isinstance(df, pl.LazyFrame):
            # Expensive to count rows in LazyFrame without scan
            # But usually shape implies (rows, cols)
            # columns is cheap. rows requires partial scan or metadata.
            # Fetching 1 row might give columns.
            # For exact row count, we need collect(count)
            cols = len(df.collect_schema().names())
            rows = df.select(pl.len()).collect().item()
            return (rows, cols)
        return df.shape

    def count_rows(self, df: Any) -> int:
        """Count rows in DataFrame.

        Args:
            df: DataFrame or LazyFrame to count rows from.

        Returns:
            Number of rows as integer.
        """
        if isinstance(df, pl.LazyFrame):
            return df.select(pl.len()).collect().item()
        return len(df)

    def count_nulls(self, df: Any, columns: List[str]) -> Dict[str, int]:
        """Count nulls in specified columns.

        Args:
            df: DataFrame or LazyFrame to analyze.
            columns: List of column names to count nulls for.

        Returns:
            Dictionary mapping column names to null counts.
        """
        if isinstance(df, pl.LazyFrame):
            # efficient null count
            return df.select([pl.col(c).null_count() for c in columns]).collect().to_dicts()[0]

        return df.select([pl.col(c).null_count() for c in columns]).to_dicts()[0]

    def validate_schema(self, df: Any, schema_rules: Dict[str, Any]) -> List[str]:
        """Validate DataFrame schema.

        Args:
            df: DataFrame or LazyFrame to validate.
            schema_rules: Dictionary containing schema validation rules
                (e.g., required_columns, expected_types, disallowed_columns).

        Returns:
            List of validation failure messages (empty if all validations pass).
        """
        failures = []

        # Schema is dict-like in Polars
        current_schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema
        current_cols = current_schema.keys()

        if "required_columns" in schema_rules:
            required = schema_rules["required_columns"]
            missing = set(required) - set(current_cols)
            if missing:
                failures.append(f"Missing required columns: {', '.join(missing)}")

        if "types" in schema_rules:
            for col, expected_type in schema_rules["types"].items():
                if col not in current_cols:
                    failures.append(f"Column '{col}' not found for type validation")
                    continue

                actual_type = str(current_schema[col])
                # Basic type check - simplistic string matching
                if expected_type.lower() not in actual_type.lower():
                    failures.append(
                        f"Column '{col}' has type '{actual_type}', expected '{expected_type}'"
                    )

        return failures

    def validate_data(self, df: Any, validation_config: Any) -> List[str]:
        """Validate data against rules.

        Args:
            df: DataFrame or LazyFrame
            validation_config: ValidationConfig object

        Returns:
            List of validation failure messages
        """
        failures = []

        if isinstance(df, pl.LazyFrame):
            schema = df.collect_schema()
            columns = schema.names()
        else:
            columns = df.columns

        if getattr(validation_config, "not_empty", False):
            count = self.count_rows(df)
            if count == 0:
                failures.append("DataFrame is empty")

        if getattr(validation_config, "no_nulls", None):
            cols = validation_config.no_nulls
            null_counts = self.count_nulls(df, cols)
            for col, count in null_counts.items():
                if count > 0:
                    failures.append(f"Column '{col}' has {count} null values")

        if getattr(validation_config, "schema_validation", None):
            schema_failures = self.validate_schema(df, validation_config.schema_validation)
            failures.extend(schema_failures)

        if getattr(validation_config, "ranges", None):
            for col, bounds in validation_config.ranges.items():
                if col in columns:
                    min_val = bounds.get("min")
                    max_val = bounds.get("max")

                    if min_val is not None:
                        if isinstance(df, pl.LazyFrame):
                            min_violations = (
                                df.filter(pl.col(col) < min_val).select(pl.len()).collect().item()
                            )
                        else:
                            min_violations = len(df.filter(pl.col(col) < min_val))
                        if min_violations > 0:
                            failures.append(f"Column '{col}' has values < {min_val}")

                    if max_val is not None:
                        if isinstance(df, pl.LazyFrame):
                            max_violations = (
                                df.filter(pl.col(col) > max_val).select(pl.len()).collect().item()
                            )
                        else:
                            max_violations = len(df.filter(pl.col(col) > max_val))
                        if max_violations > 0:
                            failures.append(f"Column '{col}' has values > {max_val}")
                else:
                    failures.append(f"Column '{col}' not found for range validation")

        if getattr(validation_config, "allowed_values", None):
            for col, allowed in validation_config.allowed_values.items():
                if col in columns:
                    if isinstance(df, pl.LazyFrame):
                        invalid_count = (
                            df.filter(~pl.col(col).is_in(allowed)).select(pl.len()).collect().item()
                        )
                    else:
                        invalid_count = len(df.filter(~pl.col(col).is_in(allowed)))
                    if invalid_count > 0:
                        failures.append(f"Column '{col}' has invalid values")
                else:
                    failures.append(f"Column '{col}' not found for allowed values validation")

        return failures

    def get_sample(self, df: Any, n: int = 10) -> List[Dict[str, Any]]:
        """Get sample rows as list of dictionaries.

        Args:
            df: DataFrame or LazyFrame to sample from.
            n: Number of rows to sample (default: 10).

        Returns:
            List of dictionaries, each representing a row.
        """
        if isinstance(df, pl.LazyFrame):
            return df.limit(n).collect().to_dicts()
        return df.head(n).to_dicts()

    def profile_nulls(self, df: Any) -> Dict[str, float]:
        """Calculate null percentage for each column.

        Args:
            df: DataFrame or LazyFrame to profile.

        Returns:
            Dictionary mapping column names to null percentages (0.0 to 1.0).
        """
        if isinstance(df, pl.LazyFrame):
            # null_count() / count()
            # We can do this in one expression
            total_count = df.select(pl.len()).collect().item()
            if total_count == 0:
                return {col: 0.0 for col in df.collect_schema().names()}

            cols = df.collect_schema().names()
            null_counts = df.select([pl.col(c).null_count().alias(c) for c in cols]).collect()
            return {col: null_counts[col][0] / total_count for col in cols}

        total_count = len(df)
        if total_count == 0:
            return {col: 0.0 for col in df.columns}

        null_counts = df.null_count()
        return {col: null_counts[col][0] / total_count for col in df.columns}

    def table_exists(
        self, connection: Any, table: Optional[str] = None, path: Optional[str] = None
    ) -> bool:
        """Check if table or location exists.

        Args:
            connection: Connection object to resolve paths.
            table: Table name to check (mutually exclusive with path).
            path: File path to check (mutually exclusive with table).

        Returns:
            True if the table or path exists, False otherwise.
        """
        if path:
            full_path = connection.get_path(path)
            return os.path.exists(full_path)
        return False

    def harmonize_schema(self, df: Any, target_schema: Dict[str, str], policy: Any) -> Any:
        """Harmonize DataFrame schema.

        Args:
            df: DataFrame or LazyFrame to harmonize.
            target_schema: Dictionary mapping column names to target data types.
            policy: SchemaPolicyConfig object defining harmonization behavior
                (mode, on_missing_columns, on_new_columns).

        Returns:
            DataFrame or LazyFrame with harmonized schema.
        """
        # policy: SchemaPolicyConfig
        from odibi.config import OnMissingColumns, OnNewColumns, SchemaMode

        # Helper to get current columns/schema
        if isinstance(df, pl.LazyFrame):
            current_schema = df.collect_schema()
        else:
            current_schema = df.schema

        current_cols = current_schema.names()
        target_cols = list(target_schema.keys())

        missing = set(target_cols) - set(current_cols)
        new_cols = set(current_cols) - set(target_cols)

        # 1. Validation
        if missing and getattr(policy, "on_missing_columns", None) == OnMissingColumns.FAIL:
            raise ValueError(
                f"Schema Policy Violation: DataFrame is missing required columns {missing}. "
                f"Available columns: {current_cols}. Add missing columns or set on_missing_columns policy."
            )

        if new_cols and getattr(policy, "on_new_columns", None) == OnNewColumns.FAIL:
            raise ValueError(
                f"Schema Policy Violation: DataFrame contains unexpected columns {new_cols}. "
                f"Expected columns: {target_cols}. Remove extra columns or set on_new_columns policy."
            )

        # 2. Transformations
        exprs = []

        # Handle Missing (Add nulls)
        # Evolve means we keep new columns, Enforce means we select only target
        mode = getattr(policy, "mode", SchemaMode.ENFORCE)

        if (
            mode == SchemaMode.EVOLVE
            and getattr(policy, "on_new_columns", None) == OnNewColumns.ADD_NULLABLE
        ):
            # Add missing (if missing cols exist, we fill them with nulls)
            # on_missing_columns controls what to do with missing target cols.
            # If mode is EVOLVE, we typically keep everything?
            # But harmonize_schema is about matching a TARGET schema.
            # If target has cols that df doesn't:
            # If on_missing_columns == FILL_NULL -> Add them as null.
            pass

        # We should respect on_missing_columns regardless of mode?
        if missing and getattr(policy, "on_missing_columns", None) == OnMissingColumns.FILL_NULL:
            for col in missing:
                exprs.append(pl.lit(None).alias(col))

        if exprs:
            df = df.with_columns(exprs)

        # Now Select
        if mode == SchemaMode.ENFORCE:
            # Select only target columns.
            # Missing columns were added above if configured.
            # New columns (not in target) are dropped implicitly by selecting target_cols.
            # But wait, we added exprs to df (lazy).

            final_cols = []
            for col in target_cols:
                final_cols.append(pl.col(col))

            df = df.select(final_cols)

        elif mode == SchemaMode.EVOLVE:
            # We keep new columns.
            # If target has columns that were missing in df, we added them above (if FILL_NULL).
            # If df has columns not in target (new_cols), we keep them.
            pass

        return df

    def anonymize(
        self, df: Any, columns: List[str], method: str, salt: Optional[str] = None
    ) -> Any:
        """Anonymize specified columns.

        Args:
            df: DataFrame or LazyFrame to anonymize.
            columns: List of column names to anonymize.
            method: Anonymization method ('mask', 'hash', 'redact').
            salt: Optional salt string for hash-based anonymization.

        Returns:
            DataFrame or LazyFrame with anonymized columns.
        """
        if method == "mask":
            # Mask all but last 4 characters: '******1234'
            # Regex look-around not supported in some envs.
            # Manual approach:
            # If len > 4: repeat('*', len-4) + suffix(4)
            # Else: keep original (or mask all? Pandas engine masked all but last 4, which implies keeping small strings?)
            # Pandas: .str.replace(r".(?=.{4})", "*") -> replaces chars that are followed by 4 chars.
            # If str is "123", no char is followed by 4 chars -> "123".
            # If str is "12345", '1' is followed by '2345' (4 chars) -> "*2345".

            return df.with_columns(
                [
                    pl.when(pl.col(c).cast(pl.Utf8).str.len_chars() > 4)
                    .then(
                        pl.concat_str(
                            [
                                pl.lit("*").repeat_by(pl.col(c).str.len_chars() - 4).list.join(""),
                                pl.col(c).str.slice(-4),
                            ]
                        )
                    )
                    .otherwise(pl.col(c).cast(pl.Utf8))
                    .alias(c)
                    for c in columns
                ]
            )

        elif method == "hash":
            # Polars hash() is non-cryptographic usually (xxHash).
            # For cryptographic hash (sha256), we might need map_elements (slow) or plugin.
            # Requirement is just 'hash', often consistent for analytics.
            # Gap Analysis mentions "salt".
            # PandasEngine used sha256 with salt.
            # Polars `hash` is fast 64-bit hash.
            # If we need SHA256, we must use map_elements (python UDF) or custom.
            # For "High Performance", map_elements is bad.
            # However, without native plugin, we have no choice for SHA256.
            # Let's implement SHA256 via map_elements for compatibility,
            # OR use Polars internal hash if user accepts non-crypto.
            # But "salt" implies security/crypto usage.

            def _hash_val(val):
                if val is None:
                    return None
                to_hash = str(val)
                if salt:
                    to_hash += salt
                return hashlib.sha256(to_hash.encode("utf-8")).hexdigest()

            # Apply to each column. Warning: Slow path.
            # But Polars UDFs are still faster than Pandas apply often due to no GIL? No, Python UDF has GIL.
            return df.with_columns(
                [pl.col(c).map_elements(_hash_val, return_dtype=pl.Utf8).alias(c) for c in columns]
            )

        elif method == "redact":
            return df.with_columns([pl.lit("[REDACTED]").alias(c) for c in columns])

        return df

    def get_table_schema(
        self,
        connection: Any,
        table: Optional[str] = None,
        path: Optional[str] = None,
        format: Optional[str] = None,
    ) -> Optional[Dict[str, str]]:
        """Get schema of an existing table/file.

        Args:
            connection: Connection object
            table: Table name
            path: File path
            format: Data format (optional, helps with file-based sources)

        Returns:
            Schema dict or None if table doesn't exist or schema fetch fails.
        """
        from odibi.utils.logging_context import get_logging_context

        ctx = get_logging_context().with_context(engine="polars")

        try:
            if table and format in ["sql", "sql_server", "azure_sql"]:
                query = f"SELECT TOP 0 * FROM {table}"
                df = connection.read_sql(query)
                return {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}

            if path:
                full_path = connection.get_path(path) if connection else path
                if not os.path.exists(full_path):
                    return None

                if format == "delta":
                    try:
                        from deltalake import DeltaTable

                        dt = DeltaTable(full_path)
                        arrow_schema = dt.schema().to_pyarrow()
                        return {field.name: str(field.type) for field in arrow_schema}
                    except ImportError:
                        ctx.warning(
                            "deltalake library not installed for schema introspection",
                            path=full_path,
                        )
                        return None

                elif format == "parquet":
                    try:
                        import pyarrow.parquet as pq
                        import glob as glob_mod

                        target_path = full_path
                        if os.path.isdir(full_path):
                            files = glob_mod.glob(os.path.join(full_path, "*.parquet"))
                            if not files:
                                return None
                            target_path = files[0]

                        schema = pq.read_schema(target_path)
                        return {field.name: str(field.type) for field in schema}
                    except ImportError:
                        lf = pl.scan_parquet(full_path)
                        schema = lf.collect_schema()
                        return {name: str(dtype) for name, dtype in schema.items()}

                elif format == "csv":
                    lf = pl.scan_csv(full_path)
                    schema = lf.collect_schema()
                    return {name: str(dtype) for name, dtype in schema.items()}

        except (FileNotFoundError, PermissionError):
            return None
        except Exception as e:
            ctx.warning(f"Failed to infer schema for {table or path}: {e}")
            return None

        return None

    def maintain_table(
        self,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        config: Optional[Any] = None,
    ) -> None:
        """Run table maintenance operations (optimize, vacuum) for Delta tables.

        Args:
            connection: Connection object
            format: Table format
            table: Table name
            path: Table path
            config: AutoOptimizeConfig object

        Returns:
            None
        """
        from odibi.utils.logging_context import get_logging_context

        ctx = get_logging_context().with_context(engine="polars")

        if format != "delta" or not config or not getattr(config, "enabled", False):
            return

        if not path and not table:
            return

        full_path = connection.get_path(path if path else table) if connection else (path or table)

        ctx.info("Starting table maintenance", path=str(full_path))

        try:
            from deltalake import DeltaTable
        except ImportError:
            ctx.warning(
                "Auto-optimize skipped: 'deltalake' library not installed",
                path=str(full_path),
            )
            return

        try:
            import time

            start = time.time()

            storage_opts = {}
            if hasattr(connection, "pandas_storage_options"):
                storage_opts = connection.pandas_storage_options()

            dt = DeltaTable(full_path, storage_options=storage_opts)

            ctx.info("Running Delta OPTIMIZE (compaction)", path=str(full_path))
            dt.optimize.compact()

            retention = getattr(config, "vacuum_retention_hours", None)
            if retention is not None and retention > 0:
                ctx.info(
                    "Running Delta VACUUM",
                    path=str(full_path),
                    retention_hours=retention,
                )
                dt.vacuum(
                    retention_hours=retention,
                    enforce_retention_duration=True,
                    dry_run=False,
                )

            elapsed = (time.time() - start) * 1000
            ctx.info(
                "Table maintenance completed",
                path=str(full_path),
                elapsed_ms=round(elapsed, 2),
            )

        except Exception as e:
            ctx.warning(
                "Auto-optimize failed",
                path=str(full_path),
                error=str(e),
            )

    def get_source_files(self, df: Any) -> List[str]:
        """Get list of source files that generated this DataFrame.

        For Polars, this checks if source file info was stored
        in the DataFrame's metadata during read.

        Args:
            df: DataFrame or LazyFrame

        Returns:
            List of file paths (or empty list if not applicable/supported)
        """
        if isinstance(df, pl.LazyFrame):
            return []

        if hasattr(df, "attrs"):
            return df.attrs.get("odibi_source_files", [])

        return []

    def vacuum_delta(
        self,
        connection: Any,
        path: str,
        retention_hours: int = 168,
        dry_run: bool = False,
        enforce_retention_duration: bool = True,
    ) -> Dict[str, Any]:
        """VACUUM a Delta table to remove old files.

        Args:
            connection: Connection object
            path: Delta table path
            retention_hours: Retention period (default 168 = 7 days)
            dry_run: If True, only show files to be deleted
            enforce_retention_duration: If False, allows retention < 168 hours (testing only)

        Returns:
            Dictionary with files_deleted count
        """
        from odibi.utils.logging_context import get_logging_context
        import time

        ctx = get_logging_context().with_context(engine="polars")
        start = time.time()

        ctx.debug(
            "Starting Delta VACUUM",
            path=path,
            retention_hours=retention_hours,
            dry_run=dry_run,
        )

        try:
            from deltalake import DeltaTable
        except ImportError:
            ctx.error("Delta Lake library not installed", path=path)
            raise ImportError(
                "Delta Lake support requires 'pip install odibi[polars]' "
                "or 'pip install deltalake'. See README.md for installation instructions."
            )

        full_path = connection.get_path(path) if connection else path

        storage_opts = {}
        if hasattr(connection, "pandas_storage_options"):
            storage_opts = connection.pandas_storage_options()

        dt = DeltaTable(full_path, storage_options=storage_opts)
        deleted_files = dt.vacuum(
            retention_hours=retention_hours,
            dry_run=dry_run,
            enforce_retention_duration=enforce_retention_duration,
        )

        elapsed = (time.time() - start) * 1000
        ctx.info(
            "Delta VACUUM completed",
            path=str(full_path),
            files_deleted=len(deleted_files),
            dry_run=dry_run,
            elapsed_ms=round(elapsed, 2),
        )

        return {"files_deleted": len(deleted_files)}

    def get_delta_history(
        self, connection: Any, path: str, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get Delta table history.

        Args:
            connection: Connection object
            path: Delta table path
            limit: Maximum number of versions to return

        Returns:
            List of version metadata dictionaries
        """
        from odibi.utils.logging_context import get_logging_context
        import time

        ctx = get_logging_context().with_context(engine="polars")
        start = time.time()

        ctx.debug("Getting Delta table history", path=path, limit=limit)

        try:
            from deltalake import DeltaTable
        except ImportError:
            ctx.error("Delta Lake library not installed", path=path)
            raise ImportError(
                "Delta Lake support requires 'pip install odibi[polars]' "
                "or 'pip install deltalake'. See README.md for installation instructions."
            )

        full_path = connection.get_path(path) if connection else path

        storage_opts = {}
        if hasattr(connection, "pandas_storage_options"):
            storage_opts = connection.pandas_storage_options()

        dt = DeltaTable(full_path, storage_options=storage_opts)
        history = dt.history(limit=limit)

        elapsed = (time.time() - start) * 1000
        ctx.info(
            "Delta history retrieved",
            path=str(full_path),
            versions_returned=len(history) if history else 0,
            elapsed_ms=round(elapsed, 2),
        )

        return history

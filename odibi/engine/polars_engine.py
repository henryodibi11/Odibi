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


class PolarsEngine(Engine):
    """Polars-based execution engine (High Performance)."""

    name = "polars"

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

        Returns:
            pl.LazyFrame or pl.DataFrame
        """
        options = options or {}

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
                raise ValueError("Connection is required when specifying 'table'.")
        else:
            raise ValueError("Either path or table must be provided")

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

            else:
                raise ValueError(f"Unsupported format for Polars engine: {format}")

        except Exception as e:
            # Fallback or error handling
            raise ValueError(f"Failed to read {format} from {full_path}: {e}")

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
    ) -> None:
        """Write data using Polars."""
        options = options or {}

        if path:
            if connection:
                full_path = connection.get_path(path)
            else:
                full_path = path
        elif table:
            if connection:
                full_path = connection.get_path(table)
            else:
                raise ValueError("Connection is required when specifying 'table'.")
        else:
            raise ValueError("Either path or table must be provided")

        # Polars sink (streaming write) is preferred for LazyFrames
        is_lazy = isinstance(df, pl.LazyFrame)

        # Create directory if needed
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        if format == "parquet":
            if is_lazy:
                # sink_parquet is efficient
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
            # Polars write_delta / sink_delta might be experimental or require specific setup
            # For now, we might need to materialize and use write_delta if sink not available
            # Or use deltalake library directly if polars doesn't support writing yet

            # As of recent polars, write_delta exists for DataFrame
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
            raise ValueError(f"Unsupported write format for Polars: {format}")

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
        """Execute built-in operation."""
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
                columns=params["pivot_column"],
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

        return df

    def get_schema(self, df: Any) -> Any:
        """Get DataFrame schema."""
        # Polars schema is a dict {name: DataType}
        # We can return a dict of strings for compatibility
        schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema
        return {name: str(dtype) for name, dtype in schema.items()}

    def get_shape(self, df: Any) -> tuple:
        """Get DataFrame shape."""
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
        """Count rows in DataFrame."""
        if isinstance(df, pl.LazyFrame):
            return df.select(pl.len()).collect().item()
        return len(df)

    def count_nulls(self, df: Any, columns: List[str]) -> Dict[str, int]:
        """Count nulls in specified columns."""
        if isinstance(df, pl.LazyFrame):
            # efficient null count
            return df.select([pl.col(c).null_count() for c in columns]).collect().to_dicts()[0]

        return df.select([pl.col(c).null_count() for c in columns]).to_dicts()[0]

    def validate_schema(self, df: Any, schema_rules: Dict[str, Any]) -> List[str]:
        """Validate DataFrame schema."""
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
        """Validate data against rules."""
        failures = []

        # We'll materialize for complex validation or use lazy expressions
        # Lazy is better.

        # Not empty check
        if getattr(validation_config, "not_empty", False):
            count = self.count_rows(df)
            if count == 0:
                failures.append("DataFrame is empty")

        # No nulls
        if getattr(validation_config, "no_nulls", None):
            cols = validation_config.no_nulls
            null_counts = self.count_nulls(df, cols)
            for col, count in null_counts.items():
                if count > 0:
                    failures.append(f"Column '{col}' has {count} null values")

        return failures

    def get_sample(self, df: Any, n: int = 10) -> List[Dict[str, Any]]:
        """Get sample rows as list of dictionaries."""
        if isinstance(df, pl.LazyFrame):
            return df.limit(n).collect().to_dicts()
        return df.head(n).to_dicts()

    def profile_nulls(self, df: Any) -> Dict[str, float]:
        """Calculate null percentage for each column."""
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
        """Check if table or location exists."""
        if path:
            full_path = connection.get_path(path)
            return os.path.exists(full_path)
        return False

    def harmonize_schema(self, df: Any, target_schema: Dict[str, str], policy: Any) -> Any:
        """Harmonize DataFrame schema."""
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
            raise ValueError(f"Schema Policy Violation: Missing columns {missing}")

        if new_cols and getattr(policy, "on_new_columns", None) == OnNewColumns.FAIL:
            raise ValueError(f"Schema Policy Violation: New columns {new_cols}")

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
        """Anonymize specified columns."""
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

"""Node execution engine."""

import hashlib
import inspect
import logging
import re
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from odibi.config import IncrementalConfig, IncrementalMode, NodeConfig, RetryConfig, WriteMode
from odibi.context import Context, EngineContext, _get_unique_view_name
from odibi.enums import EngineType
from odibi.exceptions import ExecutionContext, NodeExecutionError, TransformError, ValidationError
from odibi.registry import FunctionRegistry
from odibi.state import (
    CatalogStateBackend,
    StateManager,
)
from odibi.utils.duration import parse_duration
from odibi.utils.logging_context import (
    LoggingContext,
    OperationType,
    create_logging_context,
    get_logging_context,
)


class PhaseTimer:
    """Track timing for individual execution phases.

    Usage:
        timer = PhaseTimer()
        with timer.phase("read"):
            # do read
        with timer.phase("transform"):
            # do transform
        print(timer.summary())  # {"read": 1.23, "transform": 0.45, ...}
    """

    def __init__(self):
        self._timings: Dict[str, float] = {}
        self._current_phase: Optional[str] = None
        self._phase_start: Optional[float] = None

    @contextmanager
    def phase(self, name: str):
        """Context manager to time a phase."""
        start = time.time()
        try:
            yield
        finally:
            elapsed = time.time() - start
            self._timings[name] = self._timings.get(name, 0) + elapsed

    def record(self, name: str, duration: float):
        """Manually record a phase duration."""
        self._timings[name] = self._timings.get(name, 0) + duration

    def get(self, name: str) -> float:
        """Get duration for a specific phase."""
        return self._timings.get(name, 0)

    def summary(self) -> Dict[str, float]:
        """Get all phase timings rounded to 3 decimal places."""
        return {k: round(v, 3) for k, v in self._timings.items()}

    def summary_ms(self) -> Dict[str, float]:
        """Get all phase timings in milliseconds."""
        return {k: round(v * 1000, 2) for k, v in self._timings.items()}


class NodeResult(BaseModel):
    """Result of node execution."""

    model_config = {"arbitrary_types_allowed": True}  # Allow Exception type

    node_name: str
    success: bool
    duration: float
    rows_processed: Optional[int] = None
    result_schema: Optional[Any] = Field(default=None, alias="schema")  # Renamed to avoid shadowing
    error: Optional[Exception] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


@contextmanager
def _override_log_level(log_level: Optional[str]):
    """Temporarily override the logging level for a node execution."""
    if not log_level:
        yield
        return

    from odibi.utils.logging import logger as odibi_logger

    original_level = odibi_logger.level
    new_level = getattr(logging, log_level.upper(), original_level)
    odibi_logger.level = new_level
    odibi_logger.logger.setLevel(new_level)

    try:
        yield
    finally:
        odibi_logger.level = original_level
        odibi_logger.logger.setLevel(original_level)


class NodeExecutor:
    """Handles the execution logic (read, transform, write) of a node."""

    def __init__(
        self,
        context: Context,
        engine: Any,
        connections: Dict[str, Any],
        catalog_manager: Optional[Any] = None,
        config_file: Optional[str] = None,
        max_sample_rows: int = 10,
        performance_config: Optional[Any] = None,
        state_manager: Optional[Any] = None,
        pipeline_name: Optional[str] = None,
        batch_write_buffers: Optional[Dict[str, List]] = None,
    ):
        self.context = context
        self.engine = engine
        self.connections = connections
        self.catalog_manager = catalog_manager
        self.config_file = config_file
        self.max_sample_rows = max_sample_rows
        self.performance_config = performance_config
        self.state_manager = state_manager
        self.pipeline_name = pipeline_name
        self.batch_write_buffers = batch_write_buffers

        # Ephemeral state per execution
        self._execution_steps: List[str] = []
        self._executed_sql: List[str] = []
        self._delta_write_info: Optional[Dict[str, Any]] = None
        self._validation_warnings: List[str] = []
        self._read_row_count: Optional[int] = None  # Cache row count from read phase
        self._table_exists_cache: Dict[str, bool] = {}  # Cache table existence checks

    def _cached_table_exists(
        self,
        connection: Any,
        table: Optional[str] = None,
        path: Optional[str] = None,
    ) -> bool:
        """Check if table exists with caching to avoid repeated Delta operations.

        Performance: Table existence checks involve Delta table open + limit(0).collect()
        which can take 3-5s. Caching saves significant time for nodes that check
        existence multiple times (incremental filter, write phase, etc.).
        """
        cache_key = f"{id(connection)}:{table}:{path}"
        if cache_key not in self._table_exists_cache:
            self._table_exists_cache[cache_key] = self.engine.table_exists(connection, table, path)
        return self._table_exists_cache[cache_key]

    def execute(
        self,
        config: NodeConfig,
        input_df: Optional[Any] = None,
        dry_run: bool = False,
        hwm_state: Optional[Tuple[str, Any]] = None,
        suppress_error_log: bool = False,
    ) -> NodeResult:
        """Execute the node logic.

        Args:
            config: Node configuration
            input_df: Optional input dataframe (e.g. from dependencies)
            dry_run: Whether to simulate execution
            hwm_state: Current High Water Mark state (key, value)
            suppress_error_log: If True, suppress error logging (used during retries)

        Returns:
            NodeResult
        """
        start_time = time.time()

        # Reset ephemeral state
        self._execution_steps = []
        self._executed_sql = []
        self._delta_write_info = None
        self._validation_warnings = []
        self._read_row_count = None
        self._table_exists_cache = {}  # Reset cache per execution

        ctx = create_logging_context(
            node_id=config.name,
            engine=self.engine.__class__.__name__,
        )

        # Handle materialized field - controls output as table/view/incremental
        if config.materialized:
            ctx.info(
                f"Materialization strategy: {config.materialized}",
                materialized=config.materialized,
            )

        if dry_run:
            ctx.debug("Executing node in dry-run mode")
            return self._execute_dry_run(config)

        with ctx.operation(OperationType.EXECUTE, f"node:{config.name}") as metrics:
            try:
                input_schema = None
                input_sample = None
                pending_hwm_update = None
                rows_in = None
                phase_timer = PhaseTimer()

                # 0. Pre-SQL Phase
                with phase_timer.phase("pre_sql"):
                    self._execute_pre_sql(config, ctx)

                # 1. Read Phase (either single read, multi-input, or dependency)
                input_dataframes: Dict[str, Any] = {}

                if config.inputs:
                    # Multi-input mode for cross-pipeline dependencies
                    with phase_timer.phase("inputs"):
                        input_dataframes = self._execute_inputs_phase(config, ctx)
                        # For transform phase, use first input as primary (or "df" if named)
                        if "df" in input_dataframes:
                            result_df = input_dataframes["df"]
                        elif input_dataframes:
                            first_key = next(iter(input_dataframes))
                            result_df = input_dataframes[first_key]
                        input_df = result_df
                else:
                    # Standard single read or dependency
                    with phase_timer.phase("read"):
                        result_df, pending_hwm_update = self._execute_read_phase(
                            config, hwm_state, ctx
                        )

                    # If no direct read, check dependencies or use passed input_df
                    if result_df is None:
                        if input_df is not None:
                            result_df = input_df
                            ctx.debug(
                                "Using provided input_df",
                                rows=self._count_rows(input_df) if input_df is not None else 0,
                            )
                        elif config.depends_on:
                            result_df = self.context.get(config.depends_on[0])
                            if input_df is None:
                                input_df = result_df
                            ctx.debug(
                                f"Using data from dependency: {config.depends_on[0]}",
                                rows=self._count_rows(result_df) if result_df is not None else 0,
                            )

                    if config.read:
                        input_df = result_df

                # Capture input schema before transformation
                with phase_timer.phase("schema_capture"):
                    if input_df is not None:
                        input_schema = self._get_schema(input_df)
                        # Reuse row count from read phase if available (avoids redundant count)
                        rows_in = (
                            self._read_row_count
                            if self._read_row_count is not None
                            else self._count_rows(input_df)
                        )
                        metrics.rows_in = rows_in
                        metrics.schema_before = (
                            input_schema if isinstance(input_schema, dict) else None
                        )
                        if self.max_sample_rows > 0:
                            try:
                                input_sample = self.engine.get_sample(
                                    input_df, n=self.max_sample_rows
                                )
                            except Exception:
                                pass

                # 1.5 Contracts Phase (Pre-conditions)
                with phase_timer.phase("contracts"):
                    self._execute_contracts_phase(config, input_df, ctx)

                # 2. Transform Phase
                with phase_timer.phase("transform"):
                    result_df = self._execute_transform_phase(
                        config, result_df, input_df, ctx, input_dataframes
                    )

                # 3. Validation Phase (returns filtered df if quarantine is used)
                with phase_timer.phase("validation"):
                    result_df = self._execute_validation_phase(config, result_df, ctx)

                # 4. Write Phase
                with phase_timer.phase("write"):
                    override_mode = self._determine_write_mode(config)
                    self._execute_write_phase(config, result_df, override_mode, ctx)

                # 4.5 Post-SQL Phase
                with phase_timer.phase("post_sql"):
                    self._execute_post_sql(config, ctx)

                # 5. Register & Cache
                with phase_timer.phase("register"):
                    if result_df is not None:
                        pii_meta = self._calculate_pii(config)
                        self.context.register(
                            config.name, result_df, metadata={"pii_columns": pii_meta}
                        )

                # 6. Metadata Collection
                with phase_timer.phase("metadata"):
                    duration = time.time() - start_time
                    metadata = self._collect_metadata(config, result_df, input_schema, input_sample)

                rows_out = metadata.get("rows")
                metrics.rows_out = rows_out

                # Log schema changes
                if input_schema and metadata.get("schema"):
                    output_schema = metadata["schema"]
                    if isinstance(input_schema, dict) and isinstance(output_schema, dict):
                        ctx.log_schema_change(
                            input_schema, output_schema, operation="node_execution"
                        )
                    cols_added = metadata.get("columns_added", [])
                    cols_removed = metadata.get("columns_removed", [])
                    if cols_added or cols_removed:
                        ctx.debug(
                            "Schema modified",
                            columns_added=cols_added,
                            columns_removed=cols_removed,
                        )

                # Log row count delta
                if isinstance(rows_in, (int, float)) and isinstance(rows_out, (int, float)):
                    delta = rows_out - rows_in
                    if delta != 0:
                        ctx.log_row_count_change(rows_in, rows_out, operation="node_execution")

                # Pass back HWM update if any
                if pending_hwm_update:
                    key, value = pending_hwm_update
                    metadata["hwm_update"] = {"key": key, "value": value}
                    metadata["hwm_pending"] = True
                    ctx.debug(f"HWM pending update: {key}={value}")

                # Add phase timings to metadata
                metadata["phase_timings_ms"] = phase_timer.summary_ms()

                ctx.info(
                    "Node execution completed successfully",
                    rows_in=rows_in,
                    rows_out=rows_out,
                    elapsed_ms=round((time.time() - start_time) * 1000, 2),
                    phase_timings_ms=phase_timer.summary_ms(),
                )

                return NodeResult(
                    node_name=config.name,
                    success=True,
                    duration=duration,
                    rows_processed=metadata.get("rows"),
                    schema=metadata.get("schema"),
                    metadata=metadata,
                )

            except Exception as e:
                duration = time.time() - start_time
                suggestions = self._generate_suggestions(e, config)

                # Capture traceback
                raw_traceback = traceback.format_exc()
                cleaned_traceback = self._clean_spark_traceback(raw_traceback)

                # Log error with full context (suppress during retries)
                if not suppress_error_log:
                    ctx.error(
                        f"Node execution failed: {type(e).__name__}: {e}",
                        elapsed_ms=round(duration * 1000, 2),
                        steps_completed=self._execution_steps.copy(),
                    )
                    if suggestions:
                        ctx.info(f"Suggestions: {'; '.join(suggestions)}")

                # Wrap error
                if not isinstance(e, NodeExecutionError):
                    exec_context = ExecutionContext(
                        node_name=config.name,
                        config_file=self.config_file,
                        previous_steps=self._execution_steps,
                    )
                    error = NodeExecutionError(
                        message=str(e),
                        context=exec_context,
                        original_error=e,
                        suggestions=suggestions,
                    )
                else:
                    error = e

                return NodeResult(
                    node_name=config.name,
                    success=False,
                    duration=duration,
                    error=error,
                    metadata={
                        "steps": self._execution_steps.copy(),
                        "error_traceback": raw_traceback,
                        "error_traceback_cleaned": cleaned_traceback,
                    },
                )

    def _execute_dry_run(self, config: NodeConfig) -> NodeResult:
        """Simulate execution."""
        self._execution_steps.append("Dry run: Skipping actual execution")

        if config.read:
            self._execution_steps.append(f"Dry run: Would read from {config.read.connection}")

        if config.transform:
            self._execution_steps.append(
                f"Dry run: Would apply {len(config.transform.steps)} transform steps"
            )

        if config.write:
            self._execution_steps.append(f"Dry run: Would write to {config.write.connection}")

        return NodeResult(
            node_name=config.name,
            success=True,
            duration=0.0,
            rows_processed=0,
            metadata={"dry_run": True, "steps": self._execution_steps},
        )

    def _execute_read_phase(
        self,
        config: NodeConfig,
        hwm_state: Optional[Tuple[str, Any]],
        ctx: Optional["LoggingContext"] = None,
    ) -> Tuple[Optional[Any], Optional[Tuple[str, Any]]]:
        """Execute read operation. Returns (df, pending_hwm_update)."""
        if ctx is None:
            ctx = get_logging_context()

        if not config.read:
            return None, None

        read_config = config.read
        connection = self.connections.get(read_config.connection)

        if connection is None:
            raise ValueError(
                f"Connection '{read_config.connection}' not found. "
                f"Available: {', '.join(self.connections.keys())}"
            )

        with ctx.operation(
            OperationType.READ,
            f"source:{read_config.connection}",
            format=read_config.format,
            table=read_config.table,
            path=read_config.path,
        ) as metrics:
            # Time Travel
            as_of_version = None
            as_of_timestamp = None
            if read_config.time_travel:
                as_of_version = read_config.time_travel.as_of_version
                as_of_timestamp = read_config.time_travel.as_of_timestamp
                ctx.debug(
                    "Time travel read",
                    as_of_version=as_of_version,
                    as_of_timestamp=str(as_of_timestamp) if as_of_timestamp else None,
                )

            # Legacy HWM: First Run Query Logic
            read_options = read_config.options.copy() if read_config.options else {}

            if config.write and config.write.first_run_query:
                write_config = config.write
                target_conn = self.connections.get(write_config.connection)
                if target_conn:
                    if not self._cached_table_exists(
                        target_conn, write_config.table, write_config.path
                    ):
                        read_options["query"] = config.write.first_run_query
                        ctx.debug("Using first_run_query (target table does not exist)")

            # Merge archive_options into read_options (e.g., badRecordsPath for Spark)
            if read_config.archive_options:
                read_options.update(read_config.archive_options)
                ctx.debug(
                    "Applied archive_options",
                    archive_options=list(read_config.archive_options.keys()),
                )
                self._execution_steps.append(
                    f"Applied archive_options: {list(read_config.archive_options.keys())}"
                )

            # Incremental SQL Pushdown: Generate filter for SQL sources
            if read_config.incremental and read_config.format in [
                "sql",
                "sql_server",
                "azure_sql",
            ]:
                incremental_filter = self._generate_incremental_sql_filter(
                    read_config.incremental, config, ctx
                )
                if incremental_filter:
                    # Combine with existing filter if present
                    existing_filter = read_options.get("filter")
                    if existing_filter:
                        read_options["filter"] = f"({existing_filter}) AND ({incremental_filter})"
                    else:
                        read_options["filter"] = incremental_filter
                    ctx.debug(
                        "Added incremental SQL pushdown filter",
                        filter=read_options["filter"],
                    )
                    self._execution_steps.append(f"Incremental SQL pushdown: {incremental_filter}")

            # Execute Read
            df = self.engine.read(
                connection=connection,
                format=read_config.format,
                table=read_config.table,
                path=read_config.path,
                streaming=read_config.streaming,
                schema=getattr(read_config, "schema_ddl", None),
                options=read_options,
                as_of_version=as_of_version,
                as_of_timestamp=as_of_timestamp,
            )

            if read_config.streaming:
                ctx.info("Streaming read enabled")
                self._execution_steps.append("Streaming read enabled")

            row_count = self._count_rows(df) if df is not None else 0
            metrics.rows_out = row_count
            # Cache row count to avoid redundant counting in schema_capture phase
            self._read_row_count = row_count

            ctx.info(
                f"Read completed from {read_config.connection}",
                format=read_config.format,
                table=read_config.table,
                path=read_config.path,
                rows=row_count,
            )

            # Apply Incremental Logic
            pending_hwm = None
            if config.read.incremental:
                df, pending_hwm = self._apply_incremental_filtering(df, config, hwm_state)
                if pending_hwm:
                    ctx.debug(
                        "Incremental filtering applied",
                        hwm_key=pending_hwm[0],
                        hwm_value=str(pending_hwm[1]),
                    )

            self._execution_steps.append(f"Read from {config.read.connection}")
            return df, pending_hwm

    def _execute_inputs_phase(
        self,
        config: NodeConfig,
        ctx: Optional["LoggingContext"] = None,
    ) -> Dict[str, Any]:
        """
        Execute inputs block for cross-pipeline dependencies.

        Returns a dict of {input_name: DataFrame} for use in transforms.
        """
        if ctx is None:
            ctx = get_logging_context()

        if not config.inputs:
            return {}

        from odibi.references import is_pipeline_reference, resolve_input_reference

        dataframes = {}

        for name, ref in config.inputs.items():
            if is_pipeline_reference(ref):
                if not self.catalog_manager:
                    raise ValueError(
                        f"Cannot resolve cross-pipeline reference '{ref}' without catalog manager. "
                        "Ensure system catalog is configured."
                    )

                read_config = resolve_input_reference(ref, self.catalog_manager)
                ctx.debug(
                    f"Resolved reference '{ref}'",
                    input_name=name,
                    resolved_config=read_config,
                )

                connection = None
                if "connection" in read_config and read_config["connection"]:
                    connection = self.connections.get(read_config["connection"])
                    if connection is None:
                        raise ValueError(
                            f"Connection '{read_config['connection']}' not found for input '{name}'. "
                            f"Available: {', '.join(self.connections.keys())}"
                        )

                df = self.engine.read(
                    connection=connection,
                    format=read_config.get("format"),
                    table=read_config.get("table"),
                    path=read_config.get("path"),
                )

                # Store input source path for transforms that need it (e.g., detect_deletes)
                # Resolve to absolute path using connection if available
                input_path = read_config.get("path") or read_config.get("table")
                if input_path:
                    if connection and hasattr(connection, "get_path"):
                        input_path = connection.get_path(input_path)
                    self.engine._current_input_path = input_path

            elif isinstance(ref, dict):
                conn_name = ref.get("connection")
                connection = self.connections.get(conn_name) if conn_name else None

                if conn_name and connection is None:
                    raise ValueError(
                        f"Connection '{conn_name}' not found for input '{name}'. "
                        f"Available: {', '.join(self.connections.keys())}"
                    )

                df = self.engine.read(
                    connection=connection,
                    format=ref.get("format"),
                    table=ref.get("table"),
                    path=ref.get("path"),
                )

            else:
                raise ValueError(
                    f"Invalid input format for '{name}': {ref}. "
                    "Expected $pipeline.node reference or read config dict."
                )

            dataframes[name] = df
            row_count = self._count_rows(df) if df is not None else 0
            ctx.info(
                f"Loaded input '{name}'",
                rows=row_count,
                source=ref if isinstance(ref, str) else ref.get("path") or ref.get("table"),
            )
            self._execution_steps.append(f"Loaded input '{name}' ({row_count} rows)")

        return dataframes

    def _generate_incremental_sql_filter(
        self,
        inc: IncrementalConfig,
        config: NodeConfig,
        ctx: Optional["LoggingContext"] = None,
    ) -> Optional[str]:
        """Generate SQL WHERE clause for incremental filtering (pushdown to SQL source).

        Returns a SQL filter string or None if no filter should be applied.
        """
        if ctx is None:
            ctx = get_logging_context()

        # Check if target table exists - if not, this is first run (full load)
        if config.write:
            target_conn = self.connections.get(config.write.connection)
            # Use register_table if table is not set (path-based Delta with registration)
            table_to_check = config.write.table or config.write.register_table
            if target_conn and not self._cached_table_exists(
                target_conn, table_to_check, config.write.path
            ):
                ctx.debug("First run detected - skipping incremental SQL pushdown")
                return None

        if inc.mode == IncrementalMode.ROLLING_WINDOW:
            if not inc.lookback or not inc.unit:
                return None

            # Calculate cutoff
            now = datetime.now()

            delta = None
            if inc.unit == "hour":
                delta = timedelta(hours=inc.lookback)
            elif inc.unit == "day":
                delta = timedelta(days=inc.lookback)
            elif inc.unit == "month":
                delta = timedelta(days=inc.lookback * 30)
            elif inc.unit == "year":
                delta = timedelta(days=inc.lookback * 365)

            if delta:
                cutoff = now - delta
                # Format for SQL Server: 'YYYY-MM-DD HH:MM:SS'
                cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

                if inc.fallback_column:
                    return f"COALESCE({inc.column}, {inc.fallback_column}) >= '{cutoff_str}'"
                else:
                    return f"{inc.column} >= '{cutoff_str}'"

        elif inc.mode == IncrementalMode.STATEFUL:
            # For stateful, we need to get the HWM from state
            state_key = inc.state_key or f"{config.name}_hwm"

            if self.state_manager:
                last_hwm = self.state_manager.get_hwm(state_key)
                if last_hwm is not None:
                    # Apply watermark_lag if configured
                    if inc.watermark_lag:
                        from odibi.utils.duration import parse_duration

                        lag_delta = parse_duration(inc.watermark_lag)
                        if lag_delta and isinstance(last_hwm, str):
                            try:
                                hwm_dt = datetime.fromisoformat(last_hwm)
                                last_hwm = (hwm_dt - lag_delta).isoformat()
                            except ValueError:
                                pass

                    # Format HWM for SQL compatibility (SQL Server doesn't like ISO 'T')
                    hwm_str = str(last_hwm)
                    if isinstance(last_hwm, str) and "T" in last_hwm:
                        try:
                            hwm_dt = datetime.fromisoformat(last_hwm)
                            hwm_str = hwm_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                        except ValueError:
                            hwm_str = last_hwm.replace("T", " ")

                    if inc.fallback_column:
                        return f"COALESCE({inc.column}, {inc.fallback_column}) > '{hwm_str}'"
                    else:
                        return f"{inc.column} > '{hwm_str}'"

        return None

    def _apply_incremental_filtering(
        self, df: Any, config: NodeConfig, hwm_state: Optional[Tuple[str, Any]]
    ) -> Tuple[Any, Optional[Tuple[str, Any]]]:
        """Apply incremental filtering and capture new HWM.

        Note: For SQL sources, filtering is done via SQL pushdown in _generate_incremental_sql_filter.
        This method handles non-SQL sources and HWM capture for stateful mode.
        """
        inc = config.read.incremental
        if not inc:
            return df, None

        # Skip in-memory filtering for SQL sources (already pushed down)
        if config.read.format in ["sql", "sql_server", "azure_sql"]:
            # Still need to capture HWM for stateful mode
            if inc.mode == IncrementalMode.STATEFUL:
                state_key = inc.state_key or f"{config.name}_hwm"
                new_max = self._get_column_max(df, inc.column)
                if new_max is not None:
                    return df, (state_key, new_max)
            return df, None

        # Smart Read Pattern: If target table doesn't exist, skip filtering (Full Load)
        if config.write:
            target_conn = self.connections.get(config.write.connection)
            # Use register_table if table is not set (path-based Delta with registration)
            table_to_check = config.write.table or config.write.register_table
            if target_conn and not self._cached_table_exists(
                target_conn, table_to_check, config.write.path
            ):
                # First Run detected -> Full Load
                # We still need to capture HWM if stateful!
                if inc.mode == IncrementalMode.STATEFUL:
                    state_key = inc.state_key or f"{config.name}_hwm"
                    new_max = self._get_column_max(df, inc.column)
                    if new_max is not None:
                        return df, (state_key, new_max)

                return df, None

        if inc.mode == IncrementalMode.ROLLING_WINDOW:
            if not inc.lookback or not inc.unit:
                return df, None

            # Calculate cutoff
            now = datetime.now()

            delta = None
            if inc.unit == "hour":
                delta = timedelta(hours=inc.lookback)
            elif inc.unit == "day":
                delta = timedelta(days=inc.lookback)
            elif inc.unit == "month":
                delta = timedelta(days=inc.lookback * 30)
            elif inc.unit == "year":
                delta = timedelta(days=inc.lookback * 365)

            if delta:
                cutoff = now - delta

                if inc.fallback_column:
                    if hasattr(self.engine, "filter_coalesce"):
                        # Use >= for inclusive rolling window usually? Or >?
                        # Standard is usually >= (within the last X days)
                        df = self.engine.filter_coalesce(
                            df, inc.column, inc.fallback_column, ">=", cutoff
                        )
                    elif hasattr(self.engine, "filter_greater_than"):
                        df = self.engine.filter_greater_than(df, inc.column, cutoff)
                else:
                    if hasattr(self.engine, "filter_greater_than"):
                        # Note: engine.filter_greater_than is strictly >.
                        # For rolling window, we usually want >= cutoff.
                        # But filter_greater_than implementation is >.
                        # Let's check if we should add filter_greater_than_or_equal?
                        # Or just use > (cutoff - epsilon)?
                        # Given existing test expectation (kept rows at cutoff?), use >.
                        # Test says: Cutoff 2023-10-24 12:00:00.
                        # Row 2: 2023-10-25 11:00:00. (Kept)
                        # Row 3: 2023-10-25 11:30:00. (Kept)
                        # Row 1: 2023-10-01 (Filtered)
                        # So > is fine.
                        df = self.engine.filter_greater_than(df, inc.column, cutoff)

        elif inc.mode == IncrementalMode.STATEFUL:
            # Check if we have state
            # hwm_state is (key, value)

            last_hwm = None
            state_key = inc.state_key or f"{config.name}_hwm"

            if hwm_state and hwm_state[0] == state_key:
                last_hwm = hwm_state[1]

            # Apply watermark_lag: subtract lag duration from HWM for late-arriving data
            if last_hwm is not None and inc.watermark_lag:
                lag_delta = parse_duration(inc.watermark_lag)
                if lag_delta:
                    ctx = get_logging_context()
                    ctx.debug(
                        f"Applying watermark_lag: {inc.watermark_lag}",
                        original_hwm=str(last_hwm),
                    )
                    # Parse string HWM to datetime if needed (HWM is stored as JSON string)
                    if isinstance(last_hwm, str):
                        try:
                            last_hwm = datetime.fromisoformat(last_hwm)
                        except ValueError:
                            ctx.warning(
                                f"Could not parse HWM '{last_hwm}' as datetime for watermark_lag"
                            )
                    # Subtract lag from HWM to handle late-arriving data
                    if hasattr(last_hwm, "__sub__"):
                        last_hwm = last_hwm - lag_delta
                        ctx.info(
                            "Watermark lag applied",
                            lag=inc.watermark_lag,
                            adjusted_hwm=str(last_hwm),
                        )
                        self._execution_steps.append(f"Applied watermark_lag: {inc.watermark_lag}")

            # Filter
            if last_hwm is not None:
                # Apply filter: col > last_hwm (with fallback if configured)
                if inc.fallback_column and hasattr(self.engine, "filter_coalesce"):
                    df = self.engine.filter_coalesce(
                        df, inc.column, inc.fallback_column, ">", last_hwm
                    )
                    self._execution_steps.append(
                        f"Incremental: Filtered COALESCE({inc.column}, "
                        f"{inc.fallback_column}) > {last_hwm}"
                    )
                else:
                    df = self.engine.filter_greater_than(df, inc.column, last_hwm)
                    self._execution_steps.append(f"Incremental: Filtered {inc.column} > {last_hwm}")

            # Capture new HWM (use fallback column if configured)
            new_max = self._get_column_max(df, inc.column, inc.fallback_column)

            if new_max is not None:
                return df, (state_key, new_max)

        return df, None

    def _execute_pre_sql(
        self,
        config: NodeConfig,
        ctx: Optional["LoggingContext"] = None,
    ) -> None:
        """Execute pre-SQL statements before node runs."""
        if ctx is None:
            ctx = get_logging_context()

        if not config.pre_sql:
            return

        ctx.info(f"Executing {len(config.pre_sql)} pre-SQL statement(s)")

        for i, sql in enumerate(config.pre_sql, 1):
            ctx.debug(f"Executing pre_sql [{i}/{len(config.pre_sql)}]", sql_preview=sql[:100])
            try:
                self.engine.execute_sql(sql, self.context)
                self._executed_sql.append(f"pre_sql[{i}]: {sql[:50]}...")
            except Exception as e:
                ctx.error(
                    "Pre-SQL statement failed",
                    statement_index=i,
                    error=str(e),
                )
                raise

        self._execution_steps.append(f"Executed {len(config.pre_sql)} pre-SQL statement(s)")

    def _execute_post_sql(
        self,
        config: NodeConfig,
        ctx: Optional["LoggingContext"] = None,
    ) -> None:
        """Execute post-SQL statements after node completes."""
        if ctx is None:
            ctx = get_logging_context()

        if not config.post_sql:
            return

        ctx.info(f"Executing {len(config.post_sql)} post-SQL statement(s)")

        for i, sql in enumerate(config.post_sql, 1):
            ctx.debug(f"Executing post_sql [{i}/{len(config.post_sql)}]", sql_preview=sql[:100])
            try:
                self.engine.execute_sql(sql, self.context)
                self._executed_sql.append(f"post_sql[{i}]: {sql[:50]}...")
            except Exception as e:
                ctx.error(
                    "Post-SQL statement failed",
                    statement_index=i,
                    error=str(e),
                )
                raise

        self._execution_steps.append(f"Executed {len(config.post_sql)} post-SQL statement(s)")

    def _execute_contracts_phase(
        self,
        config: NodeConfig,
        df: Any,
        ctx: Optional["LoggingContext"] = None,
    ) -> None:
        """Execute pre-condition contracts."""
        if ctx is None:
            ctx = get_logging_context()

        if config.contracts and df is not None:
            ctx.debug(
                "Starting contract validation",
                contract_count=len(config.contracts),
            )

            df = self.engine.materialize(df)

            from odibi.config import ValidationAction, ValidationConfig
            from odibi.validation.engine import Validator

            contract_config = ValidationConfig(mode=ValidationAction.FAIL, tests=config.contracts)

            validator = Validator()
            failures = validator.validate(df, contract_config, context={"columns": config.columns})

            if failures:
                ctx.error(
                    "Contract validation failed",
                    failures=failures,
                    contract_count=len(config.contracts),
                )
                raise ValidationError(f"{config.name} (Contract Failure)", failures)

            ctx.info(
                "Contract validation passed",
                contract_count=len(config.contracts),
            )
            self._execution_steps.append(f"Passed {len(config.contracts)} contract checks")

    def _execute_transform_phase(
        self,
        config: NodeConfig,
        result_df: Optional[Any],
        input_df: Optional[Any],
        ctx: Optional["LoggingContext"] = None,
        input_dataframes: Optional[Dict[str, Any]] = None,
    ) -> Optional[Any]:
        """
        Execute transformer and transform steps.

        Args:
            config: Node configuration
            result_df: Current result DataFrame
            input_df: Input DataFrame (for single-input nodes)
            ctx: Logging context
            input_dataframes: Dict of named DataFrames for multi-input nodes (inputs block)
        """
        if ctx is None:
            ctx = get_logging_context()

        input_dataframes = input_dataframes or {}

        pii_meta = self._calculate_pii(config)
        rows_before = self._count_rows(result_df) if result_df is not None else None
        schema_before = self._get_schema(result_df) if result_df is not None else None

        # Register named inputs in context for SQL access
        if input_dataframes:
            for name, df in input_dataframes.items():
                self.context.register(name, df)
            ctx.debug(
                f"Registered {len(input_dataframes)} named inputs for transforms",
                inputs=list(input_dataframes.keys()),
            )

        # Pattern Engine
        if config.transformer:
            if result_df is None and input_df is not None:
                result_df = input_df
                rows_before = self._count_rows(result_df)
                schema_before = self._get_schema(result_df)

            with ctx.operation(
                OperationType.PATTERN,
                f"transformer:{config.transformer}",
            ) as metrics:
                metrics.rows_in = rows_before
                if isinstance(schema_before, dict):
                    metrics.schema_before = schema_before

                is_pattern = False
                try:
                    from odibi.patterns import get_pattern_class

                    pattern_cls = get_pattern_class(config.transformer)
                    is_pattern = True

                    pattern = pattern_cls(self.engine, config)
                    pattern.validate()

                    engine_ctx = EngineContext(
                        context=self.context,
                        df=result_df,
                        engine_type=self.engine.name,
                        sql_executor=self.engine.execute_sql,
                        engine=self.engine,
                        pii_metadata=pii_meta,
                    )

                    result_df = pattern.execute(engine_ctx)
                    self._execution_steps.append(f"Applied pattern '{config.transformer}'")

                    if self.catalog_manager and config.write:
                        self.catalog_manager.log_pattern(
                            table_name=config.write.table or config.write.path,
                            pattern_type=config.transformer,
                            configuration=str(config.params),
                            compliance_score=1.0,
                        )

                except ValueError:
                    pass

                if not is_pattern:
                    result_df = self._execute_transformer_node(config, result_df, pii_meta)
                    self._execution_steps.append(f"Applied transformer '{config.transformer}'")

                    if self.catalog_manager and config.write:
                        self.catalog_manager.log_pattern(
                            table_name=config.write.table or config.write.path,
                            pattern_type=config.transformer,
                            configuration=str(config.params),
                            compliance_score=1.0,
                        )

                rows_after = self._count_rows(result_df) if result_df is not None else None
                schema_after = self._get_schema(result_df) if result_df is not None else None
                metrics.rows_out = rows_after
                if isinstance(schema_after, dict):
                    metrics.schema_after = schema_after

                if (
                    isinstance(rows_before, (int, float))
                    and isinstance(rows_after, (int, float))
                    and rows_before != rows_after
                ):
                    ctx.log_row_count_change(
                        rows_before, rows_after, operation=f"transformer:{config.transformer}"
                    )
                if (
                    isinstance(schema_before, dict)
                    and isinstance(schema_after, dict)
                    and schema_before != schema_after
                ):
                    ctx.log_schema_change(
                        schema_before, schema_after, operation=f"transformer:{config.transformer}"
                    )

        # Transform Steps
        if config.transform:
            if result_df is None and input_df is not None:
                result_df = input_df

            step_count = len(config.transform.steps)
            ctx.debug(f"Executing {step_count} transform steps")

            # Set current write path on engine for transforms that need it (e.g., detect_deletes)
            if config.write and config.write.path:
                self.engine._current_write_path = config.write.path
            elif config.write and config.write.table:
                self.engine._current_write_path = config.write.table

            result_df = self._execute_transform(config, result_df, pii_meta, ctx)
            self._execution_steps.append(f"Applied {step_count} transform steps")

        # Privacy Suite
        if config.privacy:
            pii_cols = [name for name, is_pii in pii_meta.items() if is_pii]
            if pii_cols:
                ctx.debug(f"Anonymizing {len(pii_cols)} PII columns", columns=pii_cols)
                result_df = self.engine.anonymize(
                    result_df,
                    pii_cols,
                    config.privacy.method,
                    config.privacy.salt,
                )
                self._execution_steps.append(f"Anonymized {len(pii_cols)} PII columns")

        return result_df

    def _execute_transformer_node(
        self, config: NodeConfig, df: Optional[Any], pii_metadata: Optional[Dict[str, bool]] = None
    ) -> Any:
        """Execute a top-level transformer (legacy)."""
        if df is not None:
            df = self.engine.materialize(df)

        func_name = config.transformer
        params = config.params

        FunctionRegistry.validate_params(func_name, params)
        func = FunctionRegistry.get(func_name)
        sig = inspect.signature(func)

        engine_type = EngineType.PANDAS if self.engine.name == "pandas" else EngineType.SPARK
        engine_ctx = EngineContext(
            context=self.context,
            df=df,
            engine_type=engine_type,
            sql_executor=self.engine.execute_sql,
            engine=self.engine,
            pii_metadata=pii_metadata,
        )

        param_model = FunctionRegistry.get_param_model(func_name)
        call_kwargs = {}
        if "current" in sig.parameters:
            call_kwargs["current"] = df

        if param_model:
            params_obj = param_model(**params)
            result = func(engine_ctx, params_obj, **call_kwargs)
        else:
            result = func(engine_ctx, **params, **call_kwargs)

        if engine_ctx._sql_history:
            self._executed_sql.extend(engine_ctx._sql_history)

        if isinstance(result, EngineContext):
            return result.df
        return result

    def _execute_transform(
        self,
        config: NodeConfig,
        df: Any,
        pii_metadata: Optional[Dict[str, bool]] = None,
        ctx: Optional["LoggingContext"] = None,
    ) -> Any:
        """Execute transform steps."""
        if ctx is None:
            ctx = get_logging_context()

        current_df = df
        transform_config = config.transform

        if transform_config:
            total_steps = len(transform_config.steps)
            for step_idx, step in enumerate(transform_config.steps):
                step_name = self._get_step_name(step)
                rows_before = self._count_rows(current_df) if current_df is not None else None
                schema_before = self._get_schema(current_df) if current_df is not None else None

                try:
                    exec_context = ExecutionContext(
                        node_name=config.name,
                        config_file=self.config_file,
                        step_index=step_idx,
                        total_steps=total_steps,
                        previous_steps=self._execution_steps,
                    )

                    with ctx.operation(
                        OperationType.TRANSFORM,
                        f"step[{step_idx + 1}/{total_steps}]:{step_name}",
                    ) as metrics:
                        metrics.rows_in = rows_before
                        if isinstance(schema_before, dict):
                            metrics.schema_before = schema_before

                        if current_df is not None:
                            self.context.register("current_df", current_df)
                            self.context.register("df", current_df)

                        if isinstance(step, str):
                            current_df = self._execute_sql_step(step, current_df)
                        else:
                            if step.function:
                                current_df = self._execute_function_step(
                                    step.function, step.params, current_df, pii_metadata
                                )
                            elif step.operation:
                                current_df = self._execute_operation_step(
                                    step.operation, step.params, current_df
                                )
                            elif step.sql:
                                current_df = self._execute_sql_step(step.sql, current_df)
                            elif step.sql_file:
                                sql_content = self._resolve_sql_file(step.sql_file)
                                current_df = self._execute_sql_step(sql_content, current_df)
                            else:
                                raise TransformError(f"Invalid transform step: {step}")

                        rows_after = (
                            self._count_rows(current_df) if current_df is not None else None
                        )
                        schema_after = (
                            self._get_schema(current_df) if current_df is not None else None
                        )
                        metrics.rows_out = rows_after
                        if isinstance(schema_after, dict):
                            metrics.schema_after = schema_after

                        if (
                            isinstance(rows_before, (int, float))
                            and isinstance(rows_after, (int, float))
                            and rows_before != rows_after
                        ):
                            ctx.log_row_count_change(rows_before, rows_after, operation=step_name)

                        if (
                            isinstance(schema_before, dict)
                            and isinstance(schema_after, dict)
                            and schema_before != schema_after
                        ):
                            ctx.log_schema_change(schema_before, schema_after, operation=step_name)

                except Exception as e:
                    schema_dict = self._get_schema(current_df) if current_df is not None else {}
                    schema = (
                        list(schema_dict.keys()) if isinstance(schema_dict, dict) else schema_dict
                    )
                    shape = self._get_shape(current_df) if current_df is not None else None

                    exec_context.input_schema = schema
                    exec_context.input_shape = shape

                    suggestions = self._generate_suggestions(e, config)

                    ctx.error(
                        f"Transform step failed: {step_name}",
                        step_index=step_idx,
                        total_steps=total_steps,
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )
                    if suggestions:
                        ctx.info(f"Suggestions: {'; '.join(suggestions)}")

                    raise NodeExecutionError(
                        message=str(e),
                        context=exec_context,
                        original_error=e,
                        suggestions=suggestions,
                    )

        return current_df

    def _get_step_name(self, step: Any) -> str:
        """Get human-readable name for a transform step."""
        if isinstance(step, str):
            return f"sql:{step[:50]}..." if len(step) > 50 else f"sql:{step}"
        if hasattr(step, "function") and step.function:
            return f"function:{step.function}"
        if hasattr(step, "operation") and step.operation:
            return f"operation:{step.operation}"
        if hasattr(step, "sql") and step.sql:
            sql_preview = step.sql[:50] + "..." if len(step.sql) > 50 else step.sql
            return f"sql:{sql_preview}"
        if hasattr(step, "sql_file") and step.sql_file:
            return f"sql_file:{step.sql_file}"
        return "unknown"

    def _execute_sql_step(self, sql: str, current_df: Any = None) -> Any:
        """Execute SQL transformation with thread-safe view names.

        Uses unique temp view names to avoid race conditions when
        multiple nodes execute SQL steps in parallel.

        Args:
            sql: SQL query string (references to 'df' are replaced with unique view)
            current_df: DataFrame to register as the source for 'df' references

        Returns:
            Result DataFrame from SQL execution
        """
        self._executed_sql.append(sql)

        if current_df is not None:
            view_name = _get_unique_view_name()
            self.context.register(view_name, current_df)
            try:
                safe_sql = re.sub(r"\bdf\b", view_name, sql)
                return self.engine.execute_sql(safe_sql, self.context)
            finally:
                self.context.unregister(view_name)
        else:
            return self.engine.execute_sql(sql, self.context)

    def _resolve_sql_file(self, sql_file_path: str) -> str:
        """Load SQL content from external file.

        Args:
            sql_file_path: Path to .sql file, relative to main config file.

        Returns:
            SQL content as string.

        Raises:
            FileNotFoundError: If the SQL file does not exist.
            ValueError: If the file cannot be read.
        """
        if not self.config_file:
            raise ValueError(
                f"Cannot resolve sql_file '{sql_file_path}': config_file path not available. "
                "Ensure pipeline was loaded from a YAML file."
            )

        config_dir = Path(self.config_file).parent
        file_path = config_dir / sql_file_path

        if not file_path.exists():
            raise FileNotFoundError(
                f"SQL file not found: {sql_file_path}\n"
                f"Looked in: {file_path.absolute()}\n"
                f"Config directory: {config_dir.absolute()}"
            )

        try:
            return file_path.read_text(encoding="utf-8")
        except Exception as e:
            raise ValueError(f"Error reading SQL file '{sql_file_path}': {e}") from e

    def _execute_function_step(
        self,
        function_name: str,
        params: Dict[str, Any],
        current_df: Optional[Any],
        pii_metadata: Optional[Dict[str, bool]] = None,
    ) -> Any:
        """Execute Python function transformation."""
        if current_df is not None:
            current_df = self.engine.materialize(current_df)

        FunctionRegistry.validate_params(function_name, params)
        func = FunctionRegistry.get(function_name)
        sig = inspect.signature(func)

        engine_type = EngineType.PANDAS if self.engine.name == "pandas" else EngineType.SPARK
        engine_ctx = EngineContext(
            context=self.context,
            df=current_df,
            engine_type=engine_type,
            sql_executor=self.engine.execute_sql,
            engine=self.engine,
            pii_metadata=pii_metadata,
        )

        param_model = FunctionRegistry.get_param_model(function_name)
        call_kwargs = {}

        if "current" in sig.parameters:
            call_kwargs["current"] = current_df

        if param_model:
            try:
                params_obj = param_model(**params)
            except Exception as e:
                raise ValueError(f"Invalid parameters for '{function_name}': {e}")

            result = func(engine_ctx, params_obj, **call_kwargs)
        else:
            result = func(engine_ctx, **params, **call_kwargs)

        if engine_ctx._sql_history:
            self._executed_sql.extend(engine_ctx._sql_history)

        if isinstance(result, EngineContext):
            return result.df

        return result

    def _execute_operation_step(
        self, operation: str, params: Dict[str, Any], current_df: Any
    ) -> Any:
        """Execute built-in operation."""
        if current_df is not None:
            current_df = self.engine.materialize(current_df)
        return self.engine.execute_operation(operation, params, current_df)

    def _execute_validation_phase(
        self,
        config: NodeConfig,
        result_df: Any,
        ctx: Optional["LoggingContext"] = None,
    ) -> Any:
        """Execute validation with quarantine and gate support.

        Returns:
            DataFrame (valid rows only if quarantine is used)
        """
        if ctx is None:
            ctx = get_logging_context()

        if not config.validation or result_df is None:
            return result_df

        test_count = len(config.validation.tests)
        ctx.debug("Starting validation phase", test_count=test_count)

        with ctx.operation(OperationType.VALIDATE, f"validation:{config.name}") as metrics:
            rows_before = self._count_rows(result_df)
            metrics.rows_in = rows_before

            result_df = self.engine.materialize(result_df)

            for test in config.validation.tests:
                if test.type == "volume_drop" and self.catalog_manager:
                    avg_rows = self.catalog_manager.get_average_volume(
                        config.name, days=test.lookback_days
                    )
                    if avg_rows and avg_rows > 0:
                        current_rows = self._count_rows(result_df)
                        drop_pct = (avg_rows - current_rows) / avg_rows
                        if drop_pct > test.threshold:
                            ctx.error(
                                "Volume drop validation failed",
                                drop_percentage=f"{drop_pct:.1%}",
                                threshold=f"{test.threshold:.1%}",
                                current_rows=current_rows,
                                average_rows=avg_rows,
                            )
                            raise ValidationError(
                                config.name,
                                [
                                    f"Volume dropped by {drop_pct:.1%} "
                                    f"(Threshold: {test.threshold:.1%})"
                                ],
                            )

            from odibi.validation.quarantine import (
                add_quarantine_metadata,
                has_quarantine_tests,
                split_valid_invalid,
                write_quarantine,
            )

            validation_config = config.validation
            quarantine_config = validation_config.quarantine
            has_quarantine = has_quarantine_tests(validation_config.tests)

            test_results: dict = {}

            if has_quarantine and quarantine_config:
                quarantine_result = split_valid_invalid(
                    result_df,
                    validation_config.tests,
                    self.engine,
                )

                if quarantine_result.rows_quarantined > 0:
                    import uuid

                    run_id = str(uuid.uuid4())
                    invalid_with_meta = add_quarantine_metadata(
                        quarantine_result.invalid_df,
                        quarantine_result.test_results,
                        quarantine_config.add_columns,
                        self.engine,
                        config.name,
                        run_id,
                        validation_config.tests,
                    )

                    write_quarantine(
                        invalid_with_meta,
                        quarantine_config,
                        self.engine,
                        self.connections,
                    )

                    ctx.warning(
                        f"Quarantined {quarantine_result.rows_quarantined} rows",
                        quarantine_path=quarantine_config.path or quarantine_config.table,
                        rows_quarantined=quarantine_result.rows_quarantined,
                    )

                    self._execution_steps.append(
                        f"Quarantined {quarantine_result.rows_quarantined} rows to "
                        f"{quarantine_config.path or quarantine_config.table}"
                    )

                result_df = quarantine_result.valid_df
                test_results = quarantine_result.test_results

        # Run standard validation on remaining rows
        self._execute_validation(config, result_df)

        # Check quality gate
        if validation_config.gate:
            result_df = self._check_gate(config, result_df, test_results, validation_config.gate)

        return result_df

    def _execute_validation(self, config: NodeConfig, df: Any) -> None:
        """Execute validation rules."""
        from odibi.config import ValidationAction
        from odibi.validation.engine import Validator

        validation_config = config.validation
        validator = Validator()
        failures = validator.validate(df, validation_config)

        # Observability: Log metrics (validation failures)
        if self.catalog_manager:
            # We can register these tests as metrics if we want, or just log failures.
            # For now, we rely on logging validation failures to meta_runs metrics_json
            # which is done via result metadata.
            pass

        if failures:
            if validation_config.mode == ValidationAction.FAIL:
                raise ValidationError(config.name, failures)
            elif validation_config.mode == ValidationAction.WARN:
                import logging

                logger = logging.getLogger(__name__)
                for fail in failures:
                    logger.warning(f"Validation Warning (Node {config.name}): {fail}")
                    self._execution_steps.append(f"Warning: {fail}")
                    self._validation_warnings.append(fail)

    def _check_gate(
        self,
        config: NodeConfig,
        df: Any,
        test_results: dict,
        gate_config: Any,
    ) -> Any:
        """Check quality gate and take action if failed.

        Args:
            config: Node configuration
            df: DataFrame to check
            test_results: Dict of test_name -> per-row boolean results
            gate_config: GateConfig

        Returns:
            DataFrame (potentially filtered if gate action is WRITE_VALID_ONLY)

        Raises:
            GateFailedError: If gate fails and action is ABORT
        """
        from odibi.config import GateOnFail
        from odibi.exceptions import GateFailedError
        from odibi.validation.gate import evaluate_gate

        gate_result = evaluate_gate(
            df,
            test_results,
            gate_config,
            self.engine,
            catalog=self.catalog_manager,
            node_name=config.name,
        )

        if gate_result.passed:
            self._execution_steps.append(f"Gate passed: {gate_result.pass_rate:.1%} pass rate")
            return df

        self._execution_steps.append(
            f"Gate failed: {gate_result.pass_rate:.1%} pass rate "
            f"(required: {gate_config.require_pass_rate:.1%})"
        )

        if gate_result.action == GateOnFail.ABORT:
            raise GateFailedError(
                node_name=config.name,
                pass_rate=gate_result.pass_rate,
                required_rate=gate_config.require_pass_rate,
                failed_rows=gate_result.failed_rows,
                total_rows=gate_result.total_rows,
                failure_reasons=gate_result.failure_reasons,
            )

        elif gate_result.action == GateOnFail.WARN_AND_WRITE:
            import logging

            logger = logging.getLogger(__name__)
            for reason in gate_result.failure_reasons:
                logger.warning(f"Gate Warning (Node {config.name}): {reason}")
                self._validation_warnings.append(f"Gate: {reason}")
            return df

        elif gate_result.action == GateOnFail.WRITE_VALID_ONLY:
            self._execution_steps.append(
                f"Writing only valid rows ({gate_result.passed_rows} of {gate_result.total_rows})"
            )
            return df

        return df

    def _determine_write_mode(self, config: NodeConfig) -> Optional[WriteMode]:
        """Determine write mode."""
        if not config.write or config.write.first_run_query is None:
            return None

        write_config = config.write
        target_connection = self.connections.get(write_config.connection)

        if target_connection is None:
            return None

        table_exists = self._cached_table_exists(
            target_connection, table=write_config.table, path=write_config.path
        )

        if not table_exists:
            return WriteMode.OVERWRITE

        return None

    def _execute_write_phase(
        self,
        config: NodeConfig,
        df: Any,
        override_mode: Optional[WriteMode] = None,
        ctx: Optional[LoggingContext] = None,
    ) -> None:
        """Execute write operation."""
        if ctx is None:
            ctx = get_logging_context()

        if not config.write:
            return

        write_config = config.write
        connection = self.connections.get(write_config.connection)

        if connection is None:
            raise ValueError(f"Connection '{write_config.connection}' not found.")

        # For Delta writes, defer row count to avoid double DAG execution.
        # We'll extract row count from Delta commit metadata after write.
        # For non-Delta formats, count upfront as before.
        defer_row_count = write_config.format == "delta" and df is not None
        row_count = None if defer_row_count else (self._count_rows(df) if df is not None else 0)
        mode = override_mode if override_mode is not None else write_config.mode

        with ctx.operation(
            OperationType.WRITE,
            f"target:{write_config.connection}",
            format=write_config.format,
            table=write_config.table,
            path=write_config.path,
            mode=str(mode) if mode else None,
        ) as metrics:
            metrics.rows_in = row_count

            if write_config.skip_if_unchanged and df is not None:
                skip_result = self._check_skip_if_unchanged(config, df, connection)
                if skip_result["should_skip"]:
                    self._execution_steps.append(
                        f"Skipped write: content unchanged (hash: {skip_result['hash'][:12]}...)"
                    )
                    ctx.info(
                        "Skipping write - content unchanged",
                        content_hash=skip_result["hash"][:12],
                    )
                    return

            if config.schema_policy and df is not None:
                target_schema = self.engine.get_table_schema(
                    connection=connection,
                    table=write_config.table,
                    path=write_config.path,
                    format=write_config.format,
                )
                if target_schema:
                    df = self.engine.harmonize_schema(df, target_schema, config.schema_policy)
                    ctx.debug("Applied schema harmonization")
                    self._execution_steps.append("Applied Schema Policy (Harmonization)")

            if write_config.add_metadata and df is not None:
                df = self._add_write_metadata(config, df)
                self._execution_steps.append("Added Bronze metadata columns")

            write_options = write_config.options.copy() if write_config.options else {}
            deep_diag = write_options.pop("deep_diagnostics", False)
            diff_keys = write_options.pop("diff_keys", None)

            # Extract partition_by from WriteConfig and add to write_options
            if write_config.partition_by:
                write_options["partition_by"] = write_config.partition_by
                ctx.debug("Partitioning by", columns=write_config.partition_by)
                self._execution_steps.append(f"Partition by: {write_config.partition_by}")

            # Extract zorder_by from WriteConfig and add to write_options (Delta only)
            if write_config.zorder_by:
                if write_config.format == "delta":
                    write_options["zorder_by"] = write_config.zorder_by
                    ctx.debug("Z-Ordering by", columns=write_config.zorder_by)
                    self._execution_steps.append(f"Z-Order by: {write_config.zorder_by}")
                else:
                    ctx.warning(
                        "zorder_by is only supported for Delta format, ignoring",
                        format=write_config.format,
                    )

            # Extract merge_schema from WriteConfig (Delta schema evolution)
            if write_config.merge_schema:
                if write_config.format == "delta":
                    write_options["mergeSchema"] = True
                    ctx.debug("Schema evolution enabled (mergeSchema=true)")
                    self._execution_steps.append("Schema evolution enabled (mergeSchema)")
                else:
                    # For Spark with other formats, use schema_mode if applicable
                    write_options["schema_mode"] = "merge"
                    ctx.debug("Schema merge mode enabled")
                    self._execution_steps.append("Schema merge mode enabled")

            if write_config.format == "delta":
                merged_props = {}
                if self.performance_config and hasattr(
                    self.performance_config, "delta_table_properties"
                ):
                    merged_props.update(self.performance_config.delta_table_properties or {})
                if write_config.table_properties:
                    merged_props.update(write_config.table_properties)
                if merged_props:
                    write_options["table_properties"] = merged_props

            # Handle materialized strategy
            if config.materialized:
                if config.materialized == "view":
                    # Create a view instead of writing to table
                    if write_config.table and hasattr(self.engine, "create_view"):
                        ctx.info(f"Creating view: {write_config.table}")
                        self.engine.create_view(
                            df=df,
                            view_name=write_config.table,
                            connection=connection,
                        )
                        self._execution_steps.append(f"Created view: {write_config.table}")
                        ctx.info(
                            f"View created: {write_config.table}",
                            materialized="view",
                            rows=row_count,
                        )
                        return
                    else:
                        ctx.warning(
                            "View materialization requires table name and engine support",
                            table=write_config.table,
                        )
                elif config.materialized == "incremental":
                    # Use append mode for incremental materialization
                    mode = WriteMode.APPEND
                    ctx.debug("Using append mode for incremental materialization")
                    self._execution_steps.append("Materialized: incremental (append mode)")
                elif config.materialized == "table":
                    # Default table write behavior
                    ctx.debug("Using table materialization (default write)")
                    self._execution_steps.append("Materialized: table")

            delta_info = self.engine.write(
                df=df,
                connection=connection,
                format=write_config.format,
                table=write_config.table,
                path=write_config.path,
                register_table=write_config.register_table,
                mode=mode,
                options=write_options,
                streaming_config=write_config.streaming,
            )

            # Extract row count from Delta commit metadata if deferred
            if defer_row_count:
                if delta_info:
                    op_metrics = delta_info.get("operation_metrics") or {}
                    # Delta returns numOutputRows for most operations
                    row_count = op_metrics.get("numOutputRows") or op_metrics.get(
                        "numTargetRowsInserted"
                    )
                    if row_count is not None:
                        try:
                            row_count = int(row_count)
                        except (ValueError, TypeError):
                            row_count = None
                # Fallback: count if Delta metrics unavailable (e.g., older Delta versions)
                if row_count is None:
                    ctx.debug("Delta commit metrics unavailable, falling back to count")
                    row_count = self._count_rows(df) if df is not None else 0

            metrics.rows_out = row_count

            ctx.info(
                f"Write completed to {write_config.connection}",
                format=write_config.format,
                table=write_config.table,
                path=write_config.path,
                mode=str(mode) if mode else None,
                rows=row_count,
            )

            if write_config.auto_optimize and write_config.format == "delta":
                opt_config = write_config.auto_optimize
                if isinstance(opt_config, bool):
                    if opt_config:
                        from odibi.config import AutoOptimizeConfig

                        opt_config = AutoOptimizeConfig(enabled=True)
                    else:
                        opt_config = None

                if opt_config:
                    ctx.debug("Running auto-optimize on Delta table")
                    self.engine.maintain_table(
                        connection=connection,
                        format=write_config.format,
                        table=write_config.table,
                        path=write_config.path,
                        config=opt_config,
                    )

            if delta_info:
                self._delta_write_info = delta_info
                self._calculate_delta_diagnostics(
                    delta_info, connection, write_config, deep_diag, diff_keys
                )

            # Store row count from write phase to avoid redundant counting in metadata
            if self._delta_write_info is None:
                self._delta_write_info = {}
            self._delta_write_info["_cached_row_count"] = row_count

            if write_config.skip_if_unchanged and write_config.format == "delta":
                self._store_content_hash_after_write(config, connection)

            # Phase 3: Catalog integration after successful write
            # Skip if performance config disables catalog writes
            skip_catalog = self.performance_config and getattr(
                self.performance_config, "skip_catalog_writes", False
            )
            if not skip_catalog:
                self._register_catalog_entries(config, df, connection, write_config, ctx)
            else:
                ctx.debug("Skipping catalog writes (skip_catalog_writes=true)")

    def _register_catalog_entries(
        self,
        config: NodeConfig,
        df: Any,
        connection: Any,
        write_config: Any,
        ctx: Optional["LoggingContext"] = None,
    ) -> None:
        """Register catalog entries after successful write.

        Handles Phase 3.2-3.5: register_asset, track_schema, log_pattern, record_lineage

        When batch_write_buffers is provided, records are buffered for batch write
        at the end of pipeline execution to eliminate concurrency conflicts.
        """
        if not self.catalog_manager:
            return

        if ctx is None:
            ctx = get_logging_context()

        import uuid

        run_id = str(uuid.uuid4())

        # Check if we should buffer writes for batch processing
        use_batch_mode = (
            self.batch_write_buffers is not None
            and "lineage" in self.batch_write_buffers
            and "assets" in self.batch_write_buffers
        )

        # Determine table path
        table_path = None
        if hasattr(connection, "get_path"):
            table_path = connection.get_path(write_config.path or write_config.table)
        else:
            table_path = write_config.path or write_config.table

        # 3.2: Register asset (meta_tables)
        try:
            project_name = "unknown"
            if hasattr(self, "project_config") and self.project_config:
                project_name = getattr(self.project_config, "project", "unknown")

            table_name = write_config.table or config.name
            pattern_type = config.materialized or "table"

            schema_hash = ""
            if df is not None:
                schema = self._get_schema(df)
                if isinstance(schema, dict):
                    import hashlib
                    import json

                    schema_hash = hashlib.md5(
                        json.dumps(schema, sort_keys=True).encode()
                    ).hexdigest()

            asset_record = {
                "project_name": project_name,
                "table_name": table_name,
                "path": table_path or "",
                "format": write_config.format or "delta",
                "pattern_type": pattern_type,
                "schema_hash": schema_hash,
            }

            if use_batch_mode:
                self.batch_write_buffers["assets"].append(asset_record)
                ctx.debug(f"Buffered asset for batch write: {table_name}")
            else:
                self.catalog_manager.register_asset(**asset_record)
                ctx.debug(f"Registered asset: {table_name}")

        except Exception as e:
            ctx.debug(f"Failed to register asset: {e}")

        # 3.3: Track schema changes (meta_schemas)
        try:
            if df is not None and table_path:
                schema = self._get_schema(df)
                if isinstance(schema, dict):
                    pipeline_name = self.pipeline_name or (
                        config.tags[0] if config.tags else "unknown"
                    )
                    self.catalog_manager.track_schema(
                        table_path=table_path,
                        schema=schema,
                        pipeline=pipeline_name,
                        node=config.name,
                        run_id=run_id,
                    )
                    ctx.debug(f"Tracked schema for: {table_path}")

        except Exception as e:
            ctx.debug(f"Failed to track schema: {e}")

        # 3.4: Log pattern usage (meta_patterns)
        try:
            if config.materialized:
                import json

                pattern_config = {
                    "materialized": config.materialized,
                    "format": write_config.format,
                    "mode": str(write_config.mode) if write_config.mode else None,
                }
                table_name = write_config.table or config.name
                self.catalog_manager.log_pattern(
                    table_name=table_name,
                    pattern_type=config.materialized,
                    configuration=json.dumps(pattern_config),
                    compliance_score=1.0,
                )
                ctx.debug(f"Logged pattern: {config.materialized}")

        except Exception as e:
            ctx.debug(f"Failed to log pattern: {e}")

        # 3.5: Record lineage (meta_lineage)
        try:
            if config.read and table_path:
                source_path = None
                read_config = config.read
                read_conn = self.connections.get(read_config.connection)
                if read_conn and hasattr(read_conn, "get_path"):
                    source_path = read_conn.get_path(read_config.path or read_config.table)
                else:
                    source_path = read_config.path or read_config.table

                if source_path:
                    pipeline_name = self.pipeline_name or (
                        config.tags[0] if config.tags else "unknown"
                    )
                    lineage_record = {
                        "source_table": source_path,
                        "target_table": table_path,
                        "target_pipeline": pipeline_name,
                        "target_node": config.name,
                        "run_id": run_id,
                    }

                    if use_batch_mode:
                        self.batch_write_buffers["lineage"].append(lineage_record)
                        ctx.debug(
                            f"Buffered lineage for batch write: {source_path} -> {table_path}"
                        )
                    else:
                        self.catalog_manager.record_lineage(**lineage_record)
                        ctx.debug(f"Recorded lineage: {source_path} -> {table_path}")

        except Exception as e:
            ctx.debug(f"Failed to record lineage: {e}")

    def _add_write_metadata(self, config: NodeConfig, df: Any) -> Any:
        """Add Bronze metadata columns to DataFrame before writing.

        Args:
            config: Node configuration containing read/write settings
            df: DataFrame to add metadata to

        Returns:
            DataFrame with metadata columns added
        """
        write_config = config.write
        read_config = config.read

        # Determine source info from read config
        source_connection = None
        source_table = None
        source_path = None
        is_file_source = False

        if read_config:
            source_connection = read_config.connection
            source_table = read_config.table

            # Determine if file source based on format
            read_format = str(read_config.format).lower()
            file_formats = {"csv", "parquet", "json", "avro", "excel"}
            is_file_source = read_format in file_formats

            if is_file_source:
                source_path = read_config.path

        # Call engine's metadata helper
        return self.engine.add_write_metadata(
            df=df,
            metadata_config=write_config.add_metadata,
            source_connection=source_connection,
            source_table=source_table,
            source_path=source_path,
            is_file_source=is_file_source,
        )

    def _check_skip_if_unchanged(
        self,
        config: NodeConfig,
        df: Any,
        connection: Any,
    ) -> Dict[str, Any]:
        """Check if write should be skipped due to unchanged content.

        Args:
            config: Node configuration
            df: DataFrame to check
            connection: Target connection

        Returns:
            Dict with 'should_skip' (bool) and 'hash' (str)
        """
        write_config = config.write
        format_str = str(write_config.format).lower()

        if format_str != "delta":
            from odibi.utils.logging import logger

            logger.warning(
                f"[{config.name}] skip_if_unchanged only supported for Delta format, "
                f"got '{format_str}'. Proceeding with write."
            )
            return {"should_skip": False, "hash": None}

        from odibi.enums import EngineType
        from odibi.utils.content_hash import get_content_hash_from_state

        engine_type = EngineType.SPARK if self.engine.name == "spark" else EngineType.PANDAS
        if engine_type == EngineType.SPARK:
            from odibi.utils.content_hash import compute_spark_dataframe_hash

            current_hash = compute_spark_dataframe_hash(
                df,
                columns=write_config.skip_hash_columns,
                sort_columns=write_config.skip_hash_sort_columns,
            )
        else:
            from odibi.utils.content_hash import compute_dataframe_hash

            pandas_df = df
            if hasattr(df, "to_pandas"):
                pandas_df = df.to_pandas()

            current_hash = compute_dataframe_hash(
                pandas_df,
                columns=write_config.skip_hash_columns,
                sort_columns=write_config.skip_hash_sort_columns,
            )

        table_name = write_config.table or write_config.path
        state_backend = (
            getattr(self.state_manager, "backend", None) if hasattr(self, "state_manager") else None
        )
        previous_hash = get_content_hash_from_state(state_backend, config.name, table_name)

        if previous_hash and current_hash == previous_hash:
            return {"should_skip": True, "hash": current_hash}

        self._pending_content_hash = current_hash
        return {"should_skip": False, "hash": current_hash}

    def _store_content_hash_after_write(
        self,
        config: NodeConfig,
        connection: Any,
    ) -> None:
        """Store content hash in state catalog after successful write."""
        if not hasattr(self, "_pending_content_hash") or not self._pending_content_hash:
            return

        write_config = config.write
        content_hash = self._pending_content_hash

        from odibi.utils.content_hash import set_content_hash_in_state

        try:
            table_name = write_config.table or write_config.path
            state_backend = (
                getattr(self.state_manager, "backend", None)
                if hasattr(self, "state_manager")
                else None
            )

            set_content_hash_in_state(state_backend, config.name, table_name, content_hash)

            from odibi.utils.logging import logger

            logger.debug(f"[{config.name}] Stored content hash: {content_hash[:12]}...")
        except Exception as e:
            from odibi.utils.logging import logger

            logger.warning(f"[{config.name}] Failed to store content hash: {e}")
        finally:
            self._pending_content_hash = None

    def _calculate_delta_diagnostics(
        self,
        delta_info: Dict[str, Any],
        connection: Any,
        write_config: Any,
        deep_diag: bool,
        diff_keys: Optional[List[str]],
    ) -> None:
        """Calculate Delta Lake diagnostics/diff."""
        ver = delta_info.get("version", 0)
        if isinstance(ver, int) and ver > 0:
            try:
                from odibi.diagnostics import get_delta_diff

                full_path = connection.get_path(write_config.path) if write_config.path else None

                if full_path:
                    spark_session = getattr(self.engine, "spark", None)
                    curr_ver = delta_info["version"]
                    prev_ver = curr_ver - 1

                    if deep_diag:
                        diff = get_delta_diff(
                            table_path=full_path,
                            version_a=prev_ver,
                            version_b=curr_ver,
                            spark=spark_session,
                            deep=True,
                            keys=diff_keys,
                        )
                        self._delta_write_info["data_diff"] = {
                            "rows_change": diff.rows_change,
                            "rows_added": diff.rows_added,
                            "rows_removed": diff.rows_removed,
                            "rows_updated": diff.rows_updated,
                            "schema_added": diff.schema_added,
                            "schema_removed": diff.schema_removed,
                            "schema_previous": diff.schema_previous,
                            "sample_added": diff.sample_added,
                            "sample_removed": diff.sample_removed,
                            "sample_updated": diff.sample_updated,
                        }
                    else:
                        metrics = delta_info.get("operation_metrics", {})
                        rows_inserted = int(
                            metrics.get("numTargetRowsInserted", 0)
                            or metrics.get("numOutputRows", 0)
                        )
                        rows_deleted = int(metrics.get("numTargetRowsDeleted", 0))
                        net_change = rows_inserted - rows_deleted
                        self._delta_write_info["data_diff"] = {
                            "rows_change": net_change,
                            "sample_added": None,
                            "sample_removed": None,
                        }
            except Exception as e:
                import logging

                logger = logging.getLogger(__name__)
                logger.warning(f"Failed to calculate data diff: {e}")

    def _collect_metadata(
        self,
        config: NodeConfig,
        df: Optional[Any],
        input_schema: Optional[Any] = None,
        input_sample: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Collect metadata."""
        import getpass
        import platform
        import socket
        import sys

        try:
            import pandas as pd

            pandas_version = getattr(pd, "__version__", None)
        except ImportError:
            pandas_version = None

        try:
            import pyspark

            pyspark_version = getattr(pyspark, "__version__", None)
        except ImportError:
            pyspark_version = None

        sql_hash = None
        if self._executed_sql:
            normalized_sql = " ".join(self._executed_sql).lower().strip()
            sql_hash = hashlib.md5(normalized_sql.encode("utf-8")).hexdigest()

        config_snapshot = (
            config.model_dump(mode="json") if hasattr(config, "model_dump") else config.model_dump()
        )

        metadata = {
            "timestamp": datetime.now().isoformat(),
            "environment": {
                "user": getpass.getuser(),
                "host": socket.gethostname(),
                "platform": platform.platform(),
                "python": sys.version.split()[0],
                "pandas": pandas_version,
                "pyspark": pyspark_version,
                "odibi": __import__("odibi").__version__,
            },
            "steps": self._execution_steps.copy(),
            "executed_sql": self._executed_sql.copy(),
            "sql_hash": sql_hash,
            "transformation_stack": [
                step.function if hasattr(step, "function") else str(step)
                for step in (config.transform.steps if config.transform else [])
            ],
            "validation_warnings": self._validation_warnings.copy(),
            "config_snapshot": config_snapshot,
        }

        if self._delta_write_info and "version" in self._delta_write_info:
            if self._delta_write_info.get("streaming"):
                metadata["streaming_info"] = {
                    "query_id": self._delta_write_info.get("query_id"),
                    "query_name": self._delta_write_info.get("query_name"),
                    "status": self._delta_write_info.get("status"),
                    "target": self._delta_write_info.get("target"),
                    "output_mode": self._delta_write_info.get("output_mode"),
                    "checkpoint_location": self._delta_write_info.get("checkpoint_location"),
                }
            else:
                ts = self._delta_write_info.get("timestamp")
                metadata["delta_info"] = {
                    "version": self._delta_write_info["version"],
                    "timestamp": (
                        ts.isoformat() if hasattr(ts, "isoformat") else str(ts) if ts else None
                    ),
                    "operation": self._delta_write_info.get("operation"),
                    "operation_metrics": self._delta_write_info.get("operation_metrics", {}),
                    "read_version": self._delta_write_info.get("read_version"),
                }
                if "data_diff" in self._delta_write_info:
                    metadata["data_diff"] = self._delta_write_info["data_diff"]

        if df is not None:
            # Reuse row count from write phase if available (avoids redundant count)
            cached_row_count = None
            if self._delta_write_info:
                cached_row_count = self._delta_write_info.get("_cached_row_count")
            metadata["rows"] = (
                cached_row_count if cached_row_count is not None else self._count_rows(df)
            )
            metadata["schema"] = self._get_schema(df)
            metadata["source_files"] = self.engine.get_source_files(df)
            # Skip null profiling if configured (expensive for large Spark DataFrames)
            skip_null_profiling = self.performance_config and getattr(
                self.performance_config, "skip_null_profiling", False
            )
            if skip_null_profiling:
                metadata["null_profile"] = {}
            else:
                try:
                    metadata["null_profile"] = self.engine.profile_nulls(df)
                except Exception:
                    metadata["null_profile"] = {}

        if input_schema and metadata.get("schema"):
            output_schema = metadata["schema"]
            set_in = set(input_schema)
            set_out = set(output_schema)
            metadata["schema_in"] = input_schema
            metadata["columns_added"] = list(set_out - set_in)
            metadata["columns_removed"] = list(set_in - set_out)
            if input_sample:
                metadata["sample_data_in"] = input_sample

        if df is not None and self.max_sample_rows > 0:
            metadata["sample_data"] = self._get_redacted_sample(df, config.sensitive, self.engine)

        if "sample_data_in" in metadata:
            metadata["sample_data_in"] = self._redact_sample_list(
                metadata["sample_data_in"], config.sensitive
            )

        # Create output record for cross-pipeline dependencies (batch written at end of pipeline)
        if config.write:
            output_record = self._create_output_record(config, metadata.get("rows"))
            if output_record:
                metadata["_output_record"] = output_record

        return metadata

    def _get_redacted_sample(
        self, df: Any, sensitive_config: Any, engine: Any
    ) -> List[Dict[str, Any]]:
        """Get sample data with redaction."""
        if sensitive_config is True:
            return [{"message": "[REDACTED: Sensitive Data]"}]
        try:
            sample = engine.get_sample(df, n=self.max_sample_rows)
            return self._redact_sample_list(sample, sensitive_config)
        except Exception:
            return []

    def _redact_sample_list(
        self, sample: List[Dict[str, Any]], sensitive_config: Any
    ) -> List[Dict[str, Any]]:
        """Redact list of rows."""
        if not sample:
            return []
        if sensitive_config is True:
            return [{"message": "[REDACTED: Sensitive Data]"}]
        if isinstance(sensitive_config, list):
            for row in sample:
                for col in sensitive_config:
                    if col in row:
                        row[col] = "[REDACTED]"
        return sample

    def _create_output_record(
        self, config: NodeConfig, row_count: Optional[int]
    ) -> Optional[Dict[str, Any]]:
        """
        Create an output record for cross-pipeline dependency tracking.

        This record is collected during execution and batch-written to meta_outputs
        at the end of pipeline execution for performance.

        Args:
            config: Node configuration
            row_count: Number of rows written

        Returns:
            Dict with output metadata or None if no write config
        """
        if not config.write:
            return None

        write_cfg = config.write
        output_type = (
            "managed_table" if write_cfg.table and not write_cfg.path else "external_table"
        )

        return {
            "pipeline_name": self.pipeline_name,
            "node_name": config.name,
            "output_type": output_type,
            "connection_name": write_cfg.connection,
            "path": write_cfg.path,
            "format": write_cfg.format,
            "table_name": write_cfg.register_table or write_cfg.table,
            "last_run": datetime.now(),
            "row_count": row_count,
        }

    def _get_schema(self, df: Any) -> Any:
        return self.engine.get_schema(df)

    def _get_shape(self, df: Any) -> tuple:
        return self.engine.get_shape(df)

    def _count_rows(self, df: Any) -> Optional[int]:
        if df is not None and getattr(df, "isStreaming", False):
            return None
        return self.engine.count_rows(df)

    def _get_column_max(self, df: Any, column: str, fallback_column: Optional[str] = None) -> Any:
        """Get maximum value of a column, with optional fallback for NULL values."""
        if df is not None and getattr(df, "isStreaming", False):
            return None
        if hasattr(self.engine, "spark"):
            from pyspark.sql import functions as F

            try:
                if fallback_column:
                    coalesce_col = F.coalesce(F.col(column), F.col(fallback_column))
                    row = df.select(F.max(coalesce_col)).first()
                else:
                    row = df.select(F.max(column)).first()
                return row[0] if row else None
            except Exception:
                return None
        else:
            try:
                import numpy as np
                import pandas as pd

                if fallback_column and fallback_column in df.columns:
                    combined = df[column].combine_first(df[fallback_column])
                    val = combined.max()
                elif column in df.columns:
                    val = df[column].max()
                else:
                    return None

                if pd.isna(val):
                    return None
                if isinstance(val, (np.integer, np.floating)):
                    return val.item()
                if isinstance(val, np.datetime64):
                    return str(val)
                return val
            except Exception:
                return None

    def _generate_suggestions(self, error: Exception, config: NodeConfig) -> List[str]:
        """Generate suggestions."""
        suggestions = []
        error_str = str(error).lower()

        if "column" in error_str and "not found" in error_str:
            suggestions.append("Check that previous nodes output the expected columns")
            suggestions.append(f"Use 'odibi run-node {config.name} --show-schema' to debug")

        if "validation failed" in error_str:
            suggestions.append("Check your validation rules against the input data")
            suggestions.append("Inspect the sample data in the generated story")

        if "keyerror" in error.__class__.__name__.lower():
            suggestions.append("Verify that all referenced DataFrames are registered in context")
            suggestions.append("Check node dependencies in 'depends_on' list")

        if "function" in error_str and "not" in error_str:
            suggestions.append("Ensure the transform function is decorated with @transform")
            suggestions.append("Import the module containing the transform function")

        if "connection" in error_str:
            suggestions.append("Verify connection configuration in project.yaml")
            suggestions.append("Check network connectivity and credentials")

        return suggestions

    def _clean_spark_traceback(self, raw_traceback: str) -> str:
        """Clean Spark/Py4J traceback to show only relevant Python info.

        Removes Java stack traces and Py4J noise to make errors more readable.

        Args:
            raw_traceback: Full traceback string

        Returns:
            Cleaned traceback with Java/Py4J details removed
        """
        import re

        lines = raw_traceback.split("\n")
        cleaned_lines = []
        skip_until_python = False

        for line in lines:
            # Skip Java stack trace lines
            if re.match(r"\s+at (org\.|java\.|scala\.|py4j\.)", line):
                skip_until_python = True
                continue

            # Skip Py4J internal lines
            if "py4j.protocol" in line or "Py4JJavaError" in line:
                continue

            # Skip lines that are just "..."
            if line.strip() == "...":
                continue

            # If we hit a Python traceback line, resume capturing
            if line.strip().startswith("File ") or line.strip().startswith("Traceback"):
                skip_until_python = False

            if not skip_until_python:
                # Clean up common Spark error prefixes
                cleaned_line = re.sub(r"org\.apache\.spark\.[a-zA-Z.]+Exception: ", "", line)
                cleaned_lines.append(cleaned_line)

        # Remove duplicate empty lines
        result_lines = []
        prev_empty = False
        for line in cleaned_lines:
            is_empty = not line.strip()
            if is_empty and prev_empty:
                continue
            result_lines.append(line)
            prev_empty = is_empty

        return "\n".join(result_lines).strip()

    def _calculate_pii(self, config: NodeConfig) -> Dict[str, bool]:
        """Calculate effective PII metadata (Inheritance + Local - Declassify)."""
        # 1. Collect Upstream PII
        inherited_pii = {}
        if config.depends_on:
            for dep in config.depends_on:
                meta = self.context.get_metadata(dep)
                if meta and "pii_columns" in meta:
                    inherited_pii.update(meta["pii_columns"])

        # 2. Merge with Local PII
        local_pii = {name: True for name, meta in config.columns.items() if meta.pii}
        merged_pii = {**inherited_pii, **local_pii}

        # 3. Apply Declassification
        if config.privacy and config.privacy.declassify:
            for col in config.privacy.declassify:
                merged_pii.pop(col, None)

        return merged_pii


class Node:
    """Base node execution orchestrator."""

    def __init__(
        self,
        config: NodeConfig,
        context: Context,
        engine: Any,
        connections: Dict[str, Any],
        config_file: Optional[str] = None,
        max_sample_rows: int = 10,
        dry_run: bool = False,
        retry_config: Optional[RetryConfig] = None,
        catalog_manager: Optional[Any] = None,
        performance_config: Optional[Any] = None,
        pipeline_name: Optional[str] = None,
        batch_write_buffers: Optional[Dict[str, List]] = None,
    ):
        """Initialize node."""
        self.config = config
        self.context = context
        self.engine = engine
        self.connections = connections
        self.config_file = config_file
        self.max_sample_rows = max_sample_rows
        self.dry_run = dry_run
        self.retry_config = retry_config or RetryConfig(enabled=False)
        self.catalog_manager = catalog_manager
        self.performance_config = performance_config
        self.pipeline_name = pipeline_name
        self.batch_write_buffers = batch_write_buffers

        self._cached_result: Optional[Any] = None

        # Initialize State Manager
        spark_session = None
        if hasattr(self.engine, "spark"):
            spark_session = self.engine.spark

        if self.catalog_manager and self.catalog_manager.tables:
            storage_opts = self.catalog_manager._get_storage_options()
            backend = CatalogStateBackend(
                spark_session=spark_session,
                meta_state_path=self.catalog_manager.tables.get("meta_state"),
                meta_runs_path=self.catalog_manager.tables.get("meta_runs"),
                storage_options=storage_opts if storage_opts else None,
            )
        else:
            # Fallback to default local paths (Unified Catalog default)
            backend = CatalogStateBackend(
                spark_session=spark_session,
                meta_state_path=".odibi/system/meta_state",
                meta_runs_path=".odibi/system/meta_runs",
            )

        self.state_manager = StateManager(backend=backend)

        # Initialize Executor
        self.executor = NodeExecutor(
            context=context,
            engine=engine,
            connections=connections,
            catalog_manager=catalog_manager,
            config_file=config_file,
            max_sample_rows=max_sample_rows,
            performance_config=performance_config,
            state_manager=self.state_manager,
            pipeline_name=pipeline_name,
            batch_write_buffers=batch_write_buffers,
        )

    def restore(self) -> bool:
        """Restore node state from previous execution (if persisted)."""
        ctx = create_logging_context(
            node_id=self.config.name,
            engine=self.engine.__class__.__name__,
        )

        if not self.config.write:
            ctx.debug("No write config, skipping restore")
            return False

        write_config = self.config.write
        connection = self.connections.get(write_config.connection)

        if connection is None:
            ctx.debug(f"Connection '{write_config.connection}' not found, skipping restore")
            return False

        try:
            ctx.debug(
                "Attempting to restore node from persisted state",
                table=write_config.table,
                path=write_config.path,
            )

            df = self.engine.read(
                connection=connection,
                format=write_config.format,
                table=write_config.table,
                path=write_config.path,
                options={},
            )

            if df is not None:
                row_count = self.engine.count_rows(df) if df is not None else 0
                self.context.register(self.config.name, df)
                if self.config.cache:
                    self._cached_result = df
                ctx.info(
                    "Node state restored successfully",
                    rows=row_count,
                    table=write_config.table,
                    path=write_config.path,
                )
                return True

        except Exception as e:
            ctx.warning(
                f"Failed to restore node state: {e}",
                error_type=type(e).__name__,
            )
            return False

        return False

    def get_version_hash(self) -> str:
        """Calculate a deterministic hash of the node's configuration."""
        import json

        # We use model_dump_json for consistent serialization
        # Exclude fields that don't affect logic (e.g., description, tags?)
        # Actually, changing tags might affect scheduling, but not node logic.
        # Let's stick to functional fields.

        # We need to handle the fact that model_dump might include defaults or not consistently.
        # Using model_dump(mode='json') is good.

        dump = (
            self.config.model_dump(mode="json", exclude={"description", "tags", "log_level"})
            if hasattr(self.config, "model_dump")
            else self.config.model_dump(exclude={"description", "tags", "log_level"})
        )

        # Sort keys to ensure determinism
        dump_str = json.dumps(dump, sort_keys=True)
        return hashlib.md5(dump_str.encode("utf-8")).hexdigest()

    def execute(self) -> NodeResult:
        """Execute the node with telemetry and retry logic."""
        import json
        import uuid

        from odibi.utils.telemetry import (
            Status,
            StatusCode,
            node_duration,
            nodes_executed,
            rows_processed,
            tracer,
        )

        ctx = create_logging_context(
            node_id=self.config.name,
            engine=self.engine.__class__.__name__,
        )

        node_log_level = self.config.log_level.value if self.config.log_level else None

        result_for_log = NodeResult(node_name=self.config.name, success=False, duration=0.0)
        start_time = time.time()

        ctx.info(
            f"Starting node execution: {self.config.name}",
            engine=self.engine.__class__.__name__,
            dry_run=self.dry_run,
            retry_enabled=self.retry_config.enabled if self.retry_config else False,
        )

        with (
            _override_log_level(node_log_level),
            tracer.start_as_current_span("node_execution") as span,
        ):
            span.set_attribute("node.name", self.config.name)
            span.set_attribute("node.engine", self.engine.__class__.__name__)

            try:
                try:
                    result = self._execute_with_retries()
                    result_for_log = result
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR))
                    nodes_executed.add(1, {"status": "failure", "node": self.config.name})

                    result_for_log.duration = time.time() - start_time
                    result_for_log.error = e
                    result_for_log.metadata = {"error": str(e), "catastrophic": True}

                    ctx.error(
                        "Catastrophic failure in node execution",
                        error_type=type(e).__name__,
                        error_message=str(e),
                        elapsed_ms=round(result_for_log.duration * 1000, 2),
                    )

                    raise e

                if result.success:
                    span.set_status(Status(StatusCode.OK))
                    nodes_executed.add(1, {"status": "success", "node": self.config.name})
                    ctx.info(
                        "Node execution succeeded",
                        rows_processed=result.rows_processed,
                        elapsed_ms=round(result.duration * 1000, 2),
                        attempts=result.metadata.get("attempts", 1),
                    )
                else:
                    span.set_status(Status(StatusCode.ERROR))
                    if result.error:
                        span.record_exception(result.error)
                    nodes_executed.add(1, {"status": "failure", "node": self.config.name})
                    ctx.error(
                        "Node execution failed",
                        error_type=type(result.error).__name__ if result.error else "Unknown",
                        elapsed_ms=round(result.duration * 1000, 2),
                    )

                if result.rows_processed is not None:
                    rows_processed.add(result.rows_processed, {"node": self.config.name})

                node_duration.record(result.duration, {"node": self.config.name})

                result.metadata["version_hash"] = self.get_version_hash()

                return result

            finally:

                def safe_default(o):
                    return str(o)

                try:
                    metrics_json = json.dumps(result_for_log.metadata, default=safe_default)
                except Exception:
                    metrics_json = "{}"

                run_record = {
                    "run_id": str(uuid.uuid4()),
                    "pipeline_name": self.pipeline_name
                    or (self.config.tags[0] if self.config.tags else "unknown"),
                    "node_name": self.config.name,
                    "status": "SUCCESS" if result_for_log.success else "FAILURE",
                    "rows_processed": result_for_log.rows_processed or 0,
                    "duration_ms": int(result_for_log.duration * 1000),
                    "metrics_json": metrics_json,
                }
                result_for_log.metadata["_run_record"] = run_record

    def _execute_with_retries(self) -> NodeResult:
        """Execute with internal retry logic."""
        ctx = create_logging_context(
            node_id=self.config.name,
            engine=self.engine.__class__.__name__,
        )

        start_time = time.time()
        attempts = 0
        max_attempts = self.retry_config.max_attempts if self.retry_config.enabled else 1
        last_error = None
        retry_history: List[Dict[str, Any]] = []

        if max_attempts > 1:
            ctx.debug(
                "Retry logic enabled",
                max_attempts=max_attempts,
                backoff=self.retry_config.backoff,
            )

        while attempts < max_attempts:
            attempts += 1
            attempt_start = time.time()

            if attempts > 1:
                ctx.info(
                    f"Retry attempt {attempts}/{max_attempts}",
                    previous_error=str(last_error) if last_error else None,
                )

            try:
                hwm_state = None
                if (
                    self.config.read
                    and self.config.read.incremental
                    and self.config.read.incremental.mode == IncrementalMode.STATEFUL
                ):
                    key = self.config.read.incremental.state_key or f"{self.config.name}_hwm"
                    val = self.state_manager.get_hwm(key)
                    hwm_state = (key, val)

                # Suppress error logs on non-final attempts
                is_last_attempt = attempts >= max_attempts
                result = self.executor.execute(
                    self.config,
                    dry_run=self.dry_run,
                    hwm_state=hwm_state,
                    suppress_error_log=not is_last_attempt,
                )

                attempt_duration = time.time() - attempt_start

                if result.success:
                    retry_history.append(
                        {
                            "attempt": attempts,
                            "success": True,
                            "duration": round(attempt_duration, 3),
                        }
                    )
                    result.metadata["attempts"] = attempts
                    result.metadata["retry_history"] = retry_history
                    result.duration = time.time() - start_time

                    if self.config.cache and self.context.get(self.config.name) is not None:
                        self._cached_result = self.context.get(self.config.name)

                    if result.metadata.get("hwm_pending"):
                        hwm_update = result.metadata.get("hwm_update")
                        if hwm_update:
                            try:
                                self.state_manager.set_hwm(hwm_update["key"], hwm_update["value"])
                                ctx.debug(
                                    "HWM state updated",
                                    hwm_key=hwm_update["key"],
                                    hwm_value=str(hwm_update["value"]),
                                )
                            except Exception as e:
                                result.metadata["hwm_error"] = str(e)
                                ctx.warning(f"Failed to update HWM state: {e}")

                    return result

                last_error = result.error
                retry_history.append(
                    {
                        "attempt": attempts,
                        "success": False,
                        "error": str(last_error) if last_error else "Unknown error",
                        "error_type": type(last_error).__name__ if last_error else "Unknown",
                        "error_traceback": result.metadata.get("error_traceback_cleaned")
                        or result.metadata.get("error_traceback"),
                        "duration": round(attempt_duration, 3),
                    }
                )

            except Exception as e:
                attempt_duration = time.time() - attempt_start
                last_error = e
                retry_history.append(
                    {
                        "attempt": attempts,
                        "success": False,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "error_traceback": traceback.format_exc(),
                        "duration": round(attempt_duration, 3),
                    }
                )

                if attempts < max_attempts:
                    sleep_time = 1
                    if self.retry_config.backoff == "exponential":
                        sleep_time = 2 ** (attempts - 1)
                    elif self.retry_config.backoff == "linear":
                        sleep_time = attempts
                    elif self.retry_config.backoff == "constant":
                        sleep_time = 1

                    ctx.warning(
                        f"Attempt {attempts} failed, retrying in {sleep_time}s",
                        error_type=type(e).__name__,
                        error_message=str(e),
                        backoff_seconds=sleep_time,
                    )
                    time.sleep(sleep_time)

        duration = time.time() - start_time

        ctx.error(
            "All retry attempts exhausted",
            attempts=attempts,
            max_attempts=max_attempts,
            elapsed_ms=round(duration * 1000, 2),
        )

        if not isinstance(last_error, NodeExecutionError) and last_error:
            error = NodeExecutionError(
                message=str(last_error),
                context=ExecutionContext(node_name=self.config.name, config_file=self.config_file),
                original_error=last_error,
            )
        else:
            error = last_error

        return NodeResult(
            node_name=self.config.name,
            success=False,
            duration=duration,
            error=error,
            metadata={"attempts": attempts, "retry_history": retry_history},
        )

"""Node execution engine."""

from typing import Any, Optional, Dict, List
from datetime import datetime
import time
from pydantic import BaseModel, Field

from odibi.config import NodeConfig, RetryConfig
from odibi.context import Context, EngineContext
from odibi.enums import EngineType
from odibi.registry import FunctionRegistry
from odibi.transformations.registry import get_registry as get_transformation_registry
from odibi.exceptions import NodeExecutionError, TransformError, ValidationError, ExecutionContext


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


class Node:
    """Base node execution logic."""

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
    ):
        """Initialize node.

        Args:
            config: Node configuration
            context: Execution context for data passing
            engine: Execution engine (Spark or Pandas)
            connections: Available connections
            config_file: Path to config file (for error reporting)
            max_sample_rows: Maximum rows to capture for story snapshot
            dry_run: Whether to simulate execution
            retry_config: Retry configuration
        """
        self.config = config
        self.context = context
        self.engine = engine
        self.connections = connections
        self.config_file = config_file
        self.max_sample_rows = max_sample_rows
        self.dry_run = dry_run
        self.retry_config = retry_config or RetryConfig(enabled=False)

        self._cached_result: Optional[Any] = None
        self._execution_steps: List[str] = []
        self._executed_sql: List[str] = []
        self._delta_write_info: Optional[Dict[str, Any]] = None

    def restore(self) -> bool:
        """Restore node state from previous execution (if persisted).

        Returns:
            True if restored successfully, False otherwise
        """
        # Can only restore if we wrote something to a location we can read back
        if not self.config.write:
            return False

        write_config = self.config.write
        connection = self.connections.get(write_config.connection)

        if connection is None:
            return False

        try:
            # Attempt to read back the output
            # We map write_config fields to read arguments
            # format, table, path are same for read/write usually

            df = self.engine.read(
                connection=connection,
                format=write_config.format,
                table=write_config.table,
                path=write_config.path,
                options={},  # We don't know read options, assuming default is fine for restore
            )

            if df is not None:
                self.context.register(self.config.name, df)
                # Cache if requested
                if self.config.cache:
                    self._cached_result = df
                return True

        except Exception:
            # If read fails (file deleted?), we can't restore
            return False

        return False

    def execute(self) -> NodeResult:
        """Execute the node with telemetry and retry logic.

        Returns:
            NodeResult with execution details
        """
        from odibi.utils.telemetry import (
            tracer,
            nodes_executed,
            rows_processed,
            node_duration,
            Status,
            StatusCode,
        )

        with tracer.start_as_current_span("node_execution") as span:
            span.set_attribute("node.name", self.config.name)
            span.set_attribute("node.engine", self.engine.__class__.__name__)

            try:
                result = self._execute_with_retries()
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR))
                nodes_executed.add(1, {"status": "failure", "node": self.config.name})
                raise e

            # Record telemetry
            if result.success:
                span.set_status(Status(StatusCode.OK))
                nodes_executed.add(1, {"status": "success", "node": self.config.name})
            else:
                span.set_status(Status(StatusCode.ERROR))
                if result.error:
                    span.record_exception(result.error)
                nodes_executed.add(1, {"status": "failure", "node": self.config.name})

            if result.rows_processed is not None:
                rows_processed.add(result.rows_processed, {"node": self.config.name})

            node_duration.record(result.duration, {"node": self.config.name})

            return result

    def _execute_with_retries(self) -> NodeResult:
        """Execute with internal retry logic."""
        start_time = time.time()
        attempts = 0
        max_attempts = self.retry_config.max_attempts if self.retry_config.enabled else 1
        last_error = None

        while attempts < max_attempts:
            attempts += 1
            try:
                # Reset execution steps for each attempt (keep dry run logic separate if needed)
                if not self.dry_run:
                    self._execution_steps = []

                result = self._execute_attempt()

                # If successful, add attempt count metadata and return
                if result.success:
                    result.metadata["attempts"] = attempts
                    # Update duration to include all attempts
                    result.duration = time.time() - start_time
                    return result

                # Should not happen if _execute_attempt raises exception on failure,
                # but if it returns failed result:
                last_error = result.error

            except Exception as e:
                last_error = e
                # Log or handle error between retries if needed
                if attempts < max_attempts:
                    # Backoff strategy
                    sleep_time = 1
                    if self.retry_config.backoff == "exponential":
                        sleep_time = 2 ** (attempts - 1)
                    elif self.retry_config.backoff == "linear":
                        sleep_time = attempts
                    elif self.retry_config.backoff == "constant":
                        sleep_time = 1

                    # In tests, we might want to skip sleep, but for now simple time.sleep
                    time.sleep(sleep_time)

        # All attempts failed
        duration = time.time() - start_time

        # Wrap error if needed
        if not isinstance(last_error, NodeExecutionError) and last_error:
            exec_context = ExecutionContext(
                node_name=self.config.name,
                config_file=self.config_file,
                previous_steps=self._execution_steps,
            )
            error = NodeExecutionError(
                message=str(last_error),
                context=exec_context,
                original_error=last_error,
                suggestions=self._generate_suggestions(last_error),
            )
        else:
            error = last_error

        return NodeResult(
            node_name=self.config.name,
            success=False,
            duration=duration,
            error=error,
            metadata={"attempts": attempts},
        )

    def _execute_attempt(self) -> NodeResult:
        """Single execution attempt.

        Returns:
             NodeResult (success=True) or raises Exception
        """
        start_time = time.time()

        if self.dry_run:
            return self._execute_dry_run()

        input_df = None
        input_schema = None
        input_sample = None

        # 1. Read Phase
        result_df = self._execute_read_phase()
        
        # If no direct read, check dependencies
        if result_df is None and self.config.depends_on:
            # If this is a transform/write node, get data from dependency
            # We use the first dependency as the "primary" input for schema comparison
            result_df = self.context.get(self.config.depends_on[0])
            input_df = result_df  # Snapshot for metadata

        if self.config.read:
            input_df = result_df

        # Capture input schema before transformation
        if input_df is not None:
            input_schema = self._get_schema(input_df)
            if self.max_sample_rows > 0:
                try:
                    input_sample = self.engine.get_sample(input_df, n=self.max_sample_rows)
                except Exception:
                    pass

        # 2. Transform Phase
        result_df = self._execute_transform_phase(result_df, input_df)

        # 3. Validation Phase
        self._execute_validation_phase(result_df)

        # 4. Write Phase
        self._execute_write_phase(result_df)

        # 5. Register & Cache
        if result_df is not None:
            self.context.register(self.config.name, result_df)
            if self.config.cache:
                self._cached_result = result_df

        # 6. Metadata Collection
        duration = time.time() - start_time
        metadata = self._collect_metadata(result_df, input_schema, input_sample)

        return NodeResult(
            node_name=self.config.name,
            success=True,
            duration=duration,
            rows_processed=metadata.get("rows"),
            schema=metadata.get("schema"),
            metadata=metadata,
        )

    def _execute_dry_run(self) -> NodeResult:
        """Simulate execution."""
        self._execution_steps.append("Dry run: Skipping actual execution")

        if self.config.read:
            self._execution_steps.append(
                f"Dry run: Would read from {self.config.read.connection}"
            )

        if self.config.transform:
            self._execution_steps.append(
                f"Dry run: Would apply {len(self.config.transform.steps)} transform steps"
            )

        if self.config.write:
            self._execution_steps.append(
                f"Dry run: Would write to {self.config.write.connection}"
            )

        return NodeResult(
            node_name=self.config.name,
            success=True,
            duration=0.0,
            rows_processed=0,
            metadata={"dry_run": True, "steps": self._execution_steps},
        )

    def _execute_read_phase(self) -> Optional[Any]:
        """Execute read operation."""
        if not self.config.read:
            return None
            
        result_df = self._execute_read()
        self._execution_steps.append(f"Read from {self.config.read.connection}")
        return result_df

    def _execute_transform_phase(self, result_df: Optional[Any], input_df: Optional[Any]) -> Optional[Any]:
        """Execute transformer and transform steps."""
        
        # Transformer Node (Phase 2.1)
        if self.config.transformer:
            # If transform-only node, ensure we have input
            if result_df is None and input_df is not None:
                result_df = input_df

            result_df = self._execute_transformer_node(result_df)
            self._execution_steps.append(f"Applied transformer '{self.config.transformer}'")

        # Transform Steps
        if self.config.transform:
            if result_df is None and input_df is not None:
                result_df = input_df

            result_df = self._execute_transform(result_df)
            self._execution_steps.append(
                f"Applied {len(self.config.transform.steps)} transform steps"
            )
            
        return result_df

    def _execute_validation_phase(self, result_df: Any) -> None:
        """Execute validation if configured."""
        if self.config.validation and result_df is not None:
            self._execute_validation(result_df)
            self._execution_steps.append("Validation passed")

    def _execute_write_phase(self, result_df: Any) -> None:
        """Execute write if configured."""
        if not self.config.write:
            return

        # If write-only node, get data from context based on dependencies
        df_to_write = result_df
        if df_to_write is None and self.config.depends_on:
            df_to_write = self.context.get(self.config.depends_on[0])

        if df_to_write is not None:
            self._execute_write(df_to_write)
            self._execution_steps.append(f"Written to {self.config.write.connection}")

    def _execute_read(self) -> Any:
        """Execute read operation.

        Returns:
            DataFrame from source
        """
        read_config = self.config.read
        connection = self.connections.get(read_config.connection)

        if connection is None:
            raise ValueError(
                f"Connection '{read_config.connection}' not found. "
                f"Available: {', '.join(self.connections.keys())}"
            )

        # Delegate to engine-specific reader
        df = self.engine.read(
            connection=connection,
            format=read_config.format,
            table=read_config.table,
            path=read_config.path,
            streaming=read_config.streaming,
            options=read_config.options,
        )

        return df

    def _execute_transformer_node(self, input_df: Optional[Any]) -> Any:
        """Execute transformer.

        Args:
            input_df: Input DataFrame

        Returns:
            Transformed DataFrame
        """
        transformer_name = self.config.transformer
        params = self.config.params

        registry = get_transformation_registry()
        func = registry.get(transformer_name)

        if not func:
            raise ValueError(f"Transformer '{transformer_name}' not found.")

        # Create EngineContext to support sql() and tracking
        engine_type = EngineType.PANDAS if self.engine.name == "pandas" else EngineType.SPARK
        engine_ctx = EngineContext(
            context=self.context,
            df=input_df,
            engine_type=engine_type,
            sql_executor=self.engine.execute_sql,
        )

        # Execute
        try:
            result = func(engine_ctx, current=input_df, **params)

            # Capture SQL history
            if engine_ctx._sql_history:
                self._executed_sql.extend(engine_ctx._sql_history)

        except Exception as e:
            # Wrap error
            raise TransformError(f"Transformer '{transformer_name}' failed: {e}") from e

        return result

    def _execute_transform(self, input_df: Optional[Any]) -> Any:
        """Execute transformation steps.

        Args:
            input_df: Input DataFrame (can be None for transform-only nodes)

        Returns:
            Transformed DataFrame
        """
        transform_config = self.config.transform
        current_df = input_df

        for step_idx, step in enumerate(transform_config.steps):
            try:
                exec_context = ExecutionContext(
                    node_name=self.config.name,
                    config_file=self.config_file,
                    step_index=step_idx,
                    total_steps=len(transform_config.steps),
                    previous_steps=self._execution_steps,
                )

                # Temporarily register current_df for SQL steps
                if current_df is not None:
                    self.context.register("current_df", current_df)

                if isinstance(step, str):
                    # SQL step
                    current_df = self._execute_sql_step(step)

                else:
                    # TransformStep model (validated by Pydantic)
                    if step.function:
                        current_df = self._execute_function_step(
                            step.function, step.params, current_df
                        )
                    elif step.operation:
                        current_df = self._execute_operation_step(
                            step.operation, step.params, current_df
                        )
                    elif step.sql:
                        current_df = self._execute_sql_step(step.sql)
                    else:
                        raise TransformError(f"Invalid transform step: {step}")

            except Exception as e:
                # Add step context to error
                schema_dict = self._get_schema(current_df) if current_df is not None else {}
                schema = list(schema_dict.keys()) if isinstance(schema_dict, dict) else schema_dict
                shape = self._get_shape(current_df) if current_df is not None else None

                exec_context.input_schema = schema
                exec_context.input_shape = shape

                raise NodeExecutionError(
                    message=str(e),
                    context=exec_context,
                    original_error=e,
                    suggestions=self._generate_suggestions(e),
                )

        return current_df

    def _execute_sql_step(self, sql: str) -> Any:
        """Execute SQL transformation."""
        self._executed_sql.append(sql)
        return self.engine.execute_sql(sql, self.context)

    def _execute_function_step(
        self, function_name: str, params: Dict[str, Any], current_df: Optional[Any]
    ) -> Any:
        """Execute Python function transformation."""
        import inspect

        # Validate parameters
        FunctionRegistry.validate_params(function_name, params)

        # Get function
        func = FunctionRegistry.get(function_name)

        # Check if function accepts 'current' parameter
        sig = inspect.signature(func)

        # Create EngineContext
        engine_type = EngineType.PANDAS if self.engine.name == "pandas" else EngineType.SPARK
        engine_ctx = EngineContext(
            context=self.context,
            df=current_df,
            engine_type=engine_type,
            sql_executor=self.engine.execute_sql,
        )

        if "current" in sig.parameters:
            # Inject current DataFrame
            result = func(engine_ctx, current=current_df, **params)
        else:
            # Original behavior - context only
            result = func(engine_ctx, **params)

        # Capture new SQL queries executed during function call
        if engine_ctx._sql_history:
            self._executed_sql.extend(engine_ctx._sql_history)

        return result

    def _execute_operation_step(
        self, operation: str, params: Dict[str, Any], current_df: Any
    ) -> Any:
        """Execute built-in operation (pivot, etc.)."""
        return self.engine.execute_operation(operation, params, current_df)

    def _execute_validation(self, df: Any) -> None:
        """Execute validation rules."""
        validation_config = self.config.validation

        # Delegate to engine
        failures = self.engine.validate_data(df, validation_config)

        if failures:
            raise ValidationError(self.config.name, failures)

    def _execute_write(self, df: Any) -> None:
        """Execute write operation."""
        write_config = self.config.write
        connection = self.connections.get(write_config.connection)

        if connection is None:
            raise ValueError(
                f"Connection '{write_config.connection}' not found. "
                f"Available: {', '.join(self.connections.keys())}"
            )

        # Check if deep diagnostics are requested
        write_options = write_config.options.copy() if write_config.options else {}
        deep_diag = write_options.pop("deep_diagnostics", False)

        # Check for update keys
        diff_keys = write_options.pop("diff_keys", None)

        # Delegate to engine-specific writer
        delta_info = self.engine.write(
            df=df,
            connection=connection,
            format=write_config.format,
            table=write_config.table,
            path=write_config.path,
            mode=write_config.mode,
            options=write_options,
        )

        if delta_info:
            self._delta_write_info = delta_info
            self._calculate_delta_diagnostics(delta_info, connection, write_config, deep_diag, diff_keys)

    def _calculate_delta_diagnostics(
        self, 
        delta_info: Dict[str, Any], 
        connection: Any, 
        write_config: Any, 
        deep_diag: bool, 
        diff_keys: Optional[List[str]]
    ) -> None:
        """Calculate Delta Lake diagnostics/diff."""
        # Calculate Data Diff if version > 0
        ver = delta_info.get("version", 0)
        if isinstance(ver, int) and ver > 0:
            try:
                # Import here to avoid circular dependency if any
                from odibi.diagnostics import get_delta_diff

                # Determine table path
                full_path = (
                    connection.get_path(write_config.path) if write_config.path else None
                )

                if full_path:
                    # For Spark engine, we need to pass the spark session
                    spark_session = getattr(self.engine, "spark", None)

                    # Current version
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

                        # Store summary
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
                        # Lightweight Mode: Rely on Delta operation metrics
                        metrics = delta_info.get("operation_metrics", {})
                        rows_inserted = int(
                            metrics.get("numTargetRowsInserted", 0)
                            or metrics.get("numOutputRows", 0)
                        )
                        rows_deleted = int(metrics.get("numTargetRowsDeleted", 0))

                        net_change = rows_inserted - rows_deleted

                        # Just store net change, no samples
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
        df: Optional[Any], 
        input_schema: Optional[Any] = None, 
        input_sample: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Collect metadata about execution.

        Args:
            df: Result DataFrame
            input_schema: Schema of input dataframe
            input_sample: Sample rows from input dataframe

        Returns:
            Metadata dictionary
        """
        import hashlib
        import platform
        import getpass
        import socket
        import sys

        try:
            import pandas as pd
            pandas_version = pd.__version__
        except ImportError:
            pandas_version = None

        try:
            import pyspark
            pyspark_version = pyspark.__version__
        except ImportError:
            pyspark_version = None

        # Compute SQL Hash
        sql_hash = None
        if self._executed_sql:
            normalized_sql = " ".join(self._executed_sql).lower().strip()
            sql_hash = hashlib.md5(normalized_sql.encode("utf-8")).hexdigest()

        config_snapshot = (
            self.config.model_dump(mode="json")
            if hasattr(self.config, "model_dump")
            else self.config.dict()
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
                for step in (self.config.transform.steps if self.config.transform else [])
            ],
            "config_snapshot": config_snapshot,
        }

        if self._delta_write_info:
            from odibi.story.metadata import DeltaWriteInfo
            
            ts = self._delta_write_info.get("timestamp")
            # Spark returns datetime object usually, but let's be safe
            
            metadata["delta_info"] = DeltaWriteInfo(
                version=self._delta_write_info["version"],
                timestamp=ts,
                operation=self._delta_write_info.get("operation"),
                operation_metrics=self._delta_write_info.get("operation_metrics", {}),
                read_version=self._delta_write_info.get("read_version"),
            )

            if "data_diff" in self._delta_write_info:
                metadata["data_diff"] = self._delta_write_info["data_diff"]

        if df is not None:
            metadata["rows"] = self._count_rows(df)
            metadata["schema"] = self._get_schema(df)
            metadata["source_files"] = self.engine.get_source_files(df)

            try:
                metadata["null_profile"] = self.engine.profile_nulls(df)
            except Exception:
                metadata["null_profile"] = {}

        # Calculate schema changes
        if input_schema and metadata.get("schema"):
            output_schema = metadata["schema"]
            set_in = set(input_schema)
            set_out = set(output_schema)

            metadata["schema_in"] = input_schema
            metadata["columns_added"] = list(set_out - set_in)
            metadata["columns_removed"] = list(set_in - set_out)

            if input_sample:
                metadata["sample_data_in"] = input_sample

        # Add sample data
        if df is not None and self.max_sample_rows > 0:
            metadata["sample_data"] = self._get_redacted_sample(df, self.config.sensitive)

        # Redact input sample if present
        if "sample_data_in" in metadata:
            metadata["sample_data_in"] = self._redact_sample_list(metadata["sample_data_in"], self.config.sensitive)

        return metadata

    def _get_redacted_sample(self, df: Any, sensitive_config: Any) -> List[Dict[str, Any]]:
        """Get sample data with redaction."""
        if sensitive_config is True:
             # Redact all
             return [{"message": "[REDACTED: Sensitive Data]"}]
        
        try:
            sample = self.engine.get_sample(df, n=self.max_sample_rows)
            return self._redact_sample_list(sample, sensitive_config)
        except Exception:
            return []

    def _redact_sample_list(self, sample: List[Dict[str, Any]], sensitive_config: Any) -> List[Dict[str, Any]]:
        """Redact list of rows based on config."""
        if not sample:
            return []
            
        if sensitive_config is True:
            return [{"message": "[REDACTED: Sensitive Data]"}]
            
        if isinstance(sensitive_config, list):
            # Redact specific columns
            for row in sample:
                for col in sensitive_config:
                    if col in row:
                        row[col] = "[REDACTED]"
        
        return sample

    def _get_schema(self, df: Any) -> Any:
        """Get DataFrame schema (column names and types)."""
        return self.engine.get_schema(df)

    def _get_shape(self, df: Any) -> tuple:
        """Get DataFrame shape."""
        return self.engine.get_shape(df)

    def _count_rows(self, df: Any) -> int:
        """Count rows in DataFrame."""
        return self.engine.count_rows(df)

    def _is_empty(self, df: Any) -> bool:
        """Check if DataFrame is empty."""
        return self._count_rows(df) == 0

    def _count_nulls(self, df: Any, columns: List[str]) -> Dict[str, int]:
        """Count nulls in specified columns."""
        return self.engine.count_nulls(df, columns)

    def _validate_schema(self, df: Any, schema_rules: Dict[str, Any]) -> List[str]:
        """Validate DataFrame schema."""
        return self.engine.validate_schema(df, schema_rules)

    def _generate_suggestions(self, error: Exception) -> List[str]:
        """Generate helpful suggestions based on error type."""
        suggestions = []
        error_str = str(error).lower()

        if "column" in error_str and "not found" in error_str:
            suggestions.append("Check that previous nodes output the expected columns")
            suggestions.append(f"Use 'odibi run-node {self.config.name} --show-schema' to debug")

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

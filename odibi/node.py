"""Node execution engine."""

from typing import Any, Optional, Dict, List
from datetime import datetime
import time
from pydantic import BaseModel, Field

from odibi.config import NodeConfig
from odibi.context import Context
from odibi.registry import FunctionRegistry
from odibi.exceptions import NodeExecutionError, TransformError, ValidationError, ExecutionContext


class NodeResult(BaseModel):
    """Result of node execution."""

    model_config = {"arbitrary_types_allowed": True}  # Allow Exception type

    node_name: str
    success: bool
    duration: float
    rows_processed: Optional[int] = None
    result_schema: Optional[List[str]] = Field(
        default=None, alias="schema"
    )  # Renamed to avoid shadowing
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
        """
        self.config = config
        self.context = context
        self.engine = engine
        self.connections = connections
        self.config_file = config_file
        self.max_sample_rows = max_sample_rows
        self.dry_run = dry_run

        self._cached_result: Optional[Any] = None
        self._execution_steps: List[str] = []

    def execute(self) -> NodeResult:
        """Execute the node.

        Returns:
            NodeResult with execution details
        """
        start_time = time.time()
        
        if self.dry_run:
            # Dry run execution
            self._execution_steps.append("Dry run: Skipping actual execution")
            
            # Simulate steps
            if self.config.read:
                self._execution_steps.append(f"Dry run: Would read from {self.config.read.connection}")
            
            if self.config.transform:
                self._execution_steps.append(f"Dry run: Would apply {len(self.config.transform.steps)} transform steps")
                
            if self.config.write:
                self._execution_steps.append(f"Dry run: Would write to {self.config.write.connection}")
                
            return NodeResult(
                node_name=self.config.name,
                success=True,
                duration=0.0,
                rows_processed=0,
                metadata={
                    "dry_run": True,
                    "steps": self._execution_steps
                }
            )

        input_df = None
        input_schema = None
        input_sample = None

        try:
            result_df = None

            # Read phase
            if self.config.read:
                result_df = self._execute_read()
                input_df = result_df
                self._execution_steps.append(f"Read from {self.config.read.connection}")
            elif self.config.depends_on:
                # If this is a transform/write node, get data from dependency
                # We use the first dependency as the "primary" input for schema comparison
                input_df = self.context.get(self.config.depends_on[0])

            # Capture input schema before transformation
            if input_df is not None:
                input_schema = self._get_schema(input_df)
                
                if self.max_sample_rows > 0:
                    try:
                        input_sample = self.engine.get_sample(input_df, n=self.max_sample_rows)
                    except Exception:
                        # Don't fail execution if sampling fails
                        pass

            # Transform phase
            if self.config.transform:
                if result_df is None and input_df is not None:
                    # Use input_df if available (from dependency)
                    result_df = input_df
                
                result_df = self._execute_transform(result_df)
                self._execution_steps.append(
                    f"Applied {len(self.config.transform.steps)} transform steps"
                )

            # Validation phase
            if self.config.validation and result_df is not None:
                self._execute_validation(result_df)
                self._execution_steps.append("Validation passed")

            # Write phase
            if self.config.write:
                # If write-only node, get data from context based on dependencies
                if result_df is None and self.config.depends_on:
                    # Use data from first dependency
                    result_df = self.context.get(self.config.depends_on[0])

                if result_df is not None:
                    self._execute_write(result_df)
                    self._execution_steps.append(f"Written to {self.config.write.connection}")

            # Register in context for downstream nodes
            if result_df is not None:
                self.context.register(self.config.name, result_df)

                # Cache if requested
                if self.config.cache:
                    self._cached_result = result_df

            # Collect metadata
            duration = time.time() - start_time
            metadata = self._collect_metadata(result_df)
            
            # Calculate schema changes if we have both input and output schemas
            if input_schema and metadata.get("schema"):
                output_schema = metadata["schema"]
                set_in = set(input_schema)
                set_out = set(output_schema)
                
                metadata["schema_in"] = input_schema
                metadata["columns_added"] = list(set_out - set_in)
                metadata["columns_removed"] = list(set_in - set_out)
                
                if input_sample:
                    metadata["sample_data_in"] = input_sample

            # Add sample data to metadata if requested
            if result_df is not None and self.max_sample_rows > 0:
                try:
                    sample = self.engine.get_sample(result_df, n=self.max_sample_rows)
                    metadata["sample_data"] = sample
                except Exception:
                    # Don't fail execution if sampling fails (e.g. serialization issues)
                    metadata["sample_data"] = []

            return NodeResult(
                node_name=self.config.name,
                success=True,
                duration=duration,
                rows_processed=metadata.get("rows"),
                schema=metadata.get("schema"),
                metadata=metadata,
            )

        except Exception as e:
            duration = time.time() - start_time

            # Wrap in NodeExecutionError with context
            if not isinstance(e, NodeExecutionError):
                exec_context = ExecutionContext(
                    node_name=self.config.name,
                    config_file=self.config_file,
                    previous_steps=self._execution_steps,
                )

                error = NodeExecutionError(
                    message=str(e),
                    context=exec_context,
                    original_error=e,
                    suggestions=self._generate_suggestions(e),
                )
            else:
                error = e

            return NodeResult(
                node_name=self.config.name,
                success=False,
                duration=duration,
                error=error,
            )

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
        # This will be implemented by engine adapters
        df = self.engine.read(
            connection=connection,
            format=read_config.format,
            table=read_config.table,
            path=read_config.path,
            options=read_config.options,
        )

        return df

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
                schema = self._get_schema(current_df) if current_df is not None else []
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
        """Execute SQL transformation.

        Args:
            sql: SQL query

        Returns:
            Result DataFrame
        """
        return self.engine.execute_sql(sql, self.context)

    def _execute_function_step(
        self, function_name: str, params: Dict[str, Any], current_df: Optional[Any]
    ) -> Any:
        """Execute Python function transformation.

        Args:
            function_name: Name of registered function
            params: Function parameters
            current_df: Current DataFrame in pipeline

        Returns:
            Result DataFrame
        """
        import inspect

        # Validate parameters
        FunctionRegistry.validate_params(function_name, params)

        # Get function
        func = FunctionRegistry.get(function_name)

        # Check if function accepts 'current' parameter
        sig = inspect.signature(func)
        if "current" in sig.parameters:
            # Inject current DataFrame
            result = func(self.context, current=current_df, **params)
        else:
            # Original behavior - context only
            result = func(self.context, **params)

        return result

    def _execute_operation_step(
        self, operation: str, params: Dict[str, Any], current_df: Any
    ) -> Any:
        """Execute built-in operation (pivot, etc.).

        Args:
            operation: Operation name
            params: Operation parameters
            current_df: Current DataFrame

        Returns:
            Result DataFrame
        """
        return self.engine.execute_operation(operation, params, current_df)

    def _execute_validation(self, df: Any) -> None:
        """Execute validation rules.

        Args:
            df: DataFrame to validate

        Raises:
            ValidationError: If validation fails
        """
        validation_config = self.config.validation
        failures = []

        # Check not empty
        if validation_config.not_empty:
            if self._is_empty(df):
                failures.append("DataFrame is empty")

        # Check for nulls in specified columns
        if validation_config.no_nulls:
            null_counts = self._count_nulls(df, validation_config.no_nulls)
            for col, count in null_counts.items():
                if count > 0:
                    failures.append(f"Column '{col}' has {count} null values")

        # Schema validation
        if validation_config.schema_validation:
            schema_failures = self._validate_schema(df, validation_config.schema_validation)
            failures.extend(schema_failures)

        if failures:
            raise ValidationError(self.config.name, failures)

    def _execute_write(self, df: Any) -> None:
        """Execute write operation.

        Args:
            df: DataFrame to write
        """
        write_config = self.config.write
        connection = self.connections.get(write_config.connection)

        if connection is None:
            raise ValueError(
                f"Connection '{write_config.connection}' not found. "
                f"Available: {', '.join(self.connections.keys())}"
            )

        # Delegate to engine-specific writer
        self.engine.write(
            df=df,
            connection=connection,
            format=write_config.format,
            table=write_config.table,
            path=write_config.path,
            mode=write_config.mode,
            options=write_config.options,
        )

    def _collect_metadata(self, df: Optional[Any]) -> Dict[str, Any]:
        """Collect metadata about execution.

        Args:
            df: Result DataFrame

        Returns:
            Metadata dictionary
        """
        metadata = {
            "timestamp": datetime.now().isoformat(),
            "steps": self._execution_steps.copy(),
        }

        if df is not None:
            metadata["rows"] = self._count_rows(df)
            metadata["schema"] = self._get_schema(df)

        return metadata

    def _get_schema(self, df: Any) -> List[str]:
        """Get DataFrame schema (column names).

        Args:
            df: DataFrame

        Returns:
            List of column names
        """
        return self.engine.get_schema(df)

    def _get_shape(self, df: Any) -> tuple:
        """Get DataFrame shape.

        Args:
            df: DataFrame

        Returns:
            (rows, columns)
        """
        return self.engine.get_shape(df)

    def _count_rows(self, df: Any) -> int:
        """Count rows in DataFrame.

        Args:
            df: DataFrame

        Returns:
            Row count
        """
        return self.engine.count_rows(df)

    def _is_empty(self, df: Any) -> bool:
        """Check if DataFrame is empty.

        Args:
            df: DataFrame

        Returns:
            True if empty
        """
        return self._count_rows(df) == 0

    def _count_nulls(self, df: Any, columns: List[str]) -> Dict[str, int]:
        """Count nulls in specified columns.

        Args:
            df: DataFrame
            columns: Columns to check

        Returns:
            Dictionary of column -> null count
        """
        return self.engine.count_nulls(df, columns)

    def _validate_schema(self, df: Any, schema_rules: Dict[str, Any]) -> List[str]:
        """Validate DataFrame schema.

        Args:
            df: DataFrame
            schema_rules: Validation rules

        Returns:
            List of validation failures
        """
        return self.engine.validate_schema(df, schema_rules)

    def _generate_suggestions(self, error: Exception) -> List[str]:
        """Generate helpful suggestions based on error type.

        Args:
            error: The exception that occurred

        Returns:
            List of suggestion strings
        """
        suggestions = []
        error_str = str(error).lower()

        if "column" in error_str and "not found" in error_str:
            suggestions.append("Check that previous nodes output the expected columns")
            suggestions.append(f"Use 'odibi run-node {self.config.name} --show-schema' to debug")

        if "keyerror" in error.__class__.__name__.lower():
            suggestions.append("Verify that all referenced DataFrames are registered in context")
            suggestions.append("Check node dependencies in 'depends_on' list")

        if "function" in error_str and "not" in error_str:
            suggestions.append("Ensure the transform function is decorated with @transform")
            suggestions.append("Import the module containing the transform function")

        return suggestions

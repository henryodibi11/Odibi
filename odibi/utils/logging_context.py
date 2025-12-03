"""Enhanced logging context for structured observability.

This module provides a context-based logging system that captures:
- Pipeline and node context
- Operation timing
- Row counts and schema changes
- Engine-specific metrics

Design: Composition over inheritance - LoggingContext wraps the base logger.
"""

import codecs
import json
import logging
import sys
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

try:
    from rich.console import Console
    from rich.logging import RichHandler

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class OperationType(str, Enum):
    """Types of operations for logging categorization."""

    READ = "read"
    WRITE = "write"
    TRANSFORM = "transform"
    VALIDATE = "validate"
    RESOLVE = "resolve"
    CONNECT = "connect"
    GRAPH = "graph"
    CONFIG = "config"
    EXECUTE = "execute"
    PATTERN = "pattern"


@dataclass
class OperationMetrics:
    """Metrics captured during an operation."""

    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    rows_in: Optional[int] = None
    rows_out: Optional[int] = None
    schema_before: Optional[Dict[str, str]] = None
    schema_after: Optional[Dict[str, str]] = None
    partition_count: Optional[int] = None
    memory_bytes: Optional[int] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    @property
    def elapsed_ms(self) -> Optional[float]:
        """Get elapsed time in milliseconds."""
        if self.end_time is None:
            return None
        return (self.end_time - self.start_time) * 1000

    @property
    def row_delta(self) -> Optional[int]:
        """Get row count change."""
        if self.rows_in is not None and self.rows_out is not None:
            return self.rows_out - self.rows_in
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging."""
        result = {}
        if self.elapsed_ms is not None:
            result["elapsed_ms"] = round(self.elapsed_ms, 2)
        if self.rows_in is not None:
            result["rows_in"] = self.rows_in
        if self.rows_out is not None:
            result["rows_out"] = self.rows_out
        if self.row_delta is not None:
            result["row_delta"] = self.row_delta
        if self.schema_before:
            result["columns_before"] = len(self.schema_before)
        if self.schema_after:
            result["columns_after"] = len(self.schema_after)
        if self.partition_count is not None:
            result["partitions"] = self.partition_count
        if self.memory_bytes is not None:
            result["memory_mb"] = round(self.memory_bytes / (1024 * 1024), 2)
        result.update(self.extra)
        return result


class StructuredLogger:
    """Logger that supports both human-readable and JSON output with secret redaction."""

    def __init__(self, structured: bool = False, level: str = "INFO"):
        self.structured = structured
        self.level = getattr(logging, level.upper(), logging.INFO)
        self._secrets: set = set()

        if (
            sys.platform == "win32"
            and sys.stdout
            and sys.stdout.encoding
            and sys.stdout.encoding.lower() != "utf-8"
        ):
            try:
                sys.stdout.reconfigure(encoding="utf-8")
            except AttributeError:
                sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

        if not self.structured and RICH_AVAILABLE:
            logging.basicConfig(
                level=self.level,
                format="%(message)s",
                datefmt="[%X]",
                handlers=[
                    RichHandler(
                        rich_tracebacks=True,
                        markup=True,
                        show_path=False,
                        console=(
                            Console(force_terminal=True, legacy_windows=False)
                            if sys.platform == "win32"
                            else None
                        ),
                    )
                ],
            )
        else:
            logging.basicConfig(level=self.level, format="%(message)s", stream=sys.stdout)

        self.logger = logging.getLogger("odibi")
        self.logger.setLevel(self.level)

        third_party_level = max(self.level, logging.WARNING)
        for logger_name in [
            "py4j",
            "azure",
            "azure.core.pipeline.policies.http_logging_policy",
            "adlfs",
            "urllib3",
            "fsspec",
        ]:
            logging.getLogger(logger_name).setLevel(third_party_level)

    def register_secret(self, secret: str) -> None:
        """Register a secret string to be redacted from logs."""
        if secret and isinstance(secret, str) and len(secret.strip()) > 0:
            self._secrets.add(secret)

    def _redact(self, text: str) -> str:
        """Redact registered secrets from text."""
        if not text or not self._secrets:
            return text

        for secret in self._secrets:
            if secret in text:
                text = text.replace(secret, "[REDACTED]")
        return text

    def info(self, message: str, **kwargs) -> None:
        self._log("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs) -> None:
        self._log("WARNING", message, **kwargs)

    def error(self, message: str, **kwargs) -> None:
        self._log("ERROR", message, **kwargs)

    def debug(self, message: str, **kwargs) -> None:
        self._log("DEBUG", message, **kwargs)

    def _log(self, level: str, message: str, **kwargs) -> None:
        level_val = getattr(logging, level, logging.INFO)
        if level_val < self.level:
            return

        message = self._redact(str(message))

        redacted_kwargs = {}
        for k, v in kwargs.items():
            if isinstance(v, str):
                redacted_kwargs[k] = self._redact(v)
            else:
                redacted_kwargs[k] = v

        if self.structured:
            log_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": level,
                "message": message,
                **redacted_kwargs,
            }
            print(json.dumps(log_entry))
        else:
            context_str = ""
            if redacted_kwargs:
                context_items = [f"{k}={v}" for k, v in redacted_kwargs.items()]
                context_str = f" ({', '.join(context_items)})"

            formatted_msg = f"{message}{context_str}"

            if level == "INFO":
                self.logger.info(formatted_msg)
            elif level == "WARNING":
                self.logger.warning(f"[WARN] {formatted_msg}")
            elif level == "ERROR":
                self.logger.error(f"[ERROR] {formatted_msg}")
            elif level == "DEBUG":
                self.logger.debug(f"[DEBUG] {formatted_msg}")


class LoggingContext:
    """Context-aware logging wrapper for pipeline operations.

    Provides structured logging with automatic context injection and timing.
    Uses composition pattern - wraps a StructuredLogger instance.

    Example:
        >>> with LoggingContext(pipeline_id="etl_daily", node_id="load_users") as ctx:
        ...     ctx.log_operation_start(OperationType.READ, file="users.csv")
        ...     # ... perform read ...
        ...     ctx.log_operation_end(rows=1000)
    """

    def __init__(
        self,
        logger: Optional[StructuredLogger] = None,
        pipeline_id: Optional[str] = None,
        node_id: Optional[str] = None,
        engine: Optional[str] = None,
    ):
        """Initialize logging context.

        Args:
            logger: StructuredLogger instance (uses global if None)
            pipeline_id: Pipeline identifier for correlation
            node_id: Current node identifier
            engine: Engine type (pandas/spark/polars)
        """
        self._logger = logger
        self.pipeline_id = pipeline_id
        self.node_id = node_id
        self.engine = engine
        self._operation_stack: List[tuple] = []
        self._current_metrics: Optional[OperationMetrics] = None

    @property
    def logger(self) -> StructuredLogger:
        """Get the underlying logger."""
        if self._logger is None:
            from odibi.utils.logging import logger as global_logger

            return global_logger
        return self._logger

    def _base_context(self) -> Dict[str, Any]:
        """Build base context dict for all log entries."""
        ctx = {"timestamp": datetime.utcnow().isoformat()}
        if self.pipeline_id:
            ctx["pipeline_id"] = self.pipeline_id
        if self.node_id:
            ctx["node_id"] = self.node_id
        if self.engine:
            ctx["engine"] = self.engine
        return ctx

    def with_context(
        self,
        pipeline_id: Optional[str] = None,
        node_id: Optional[str] = None,
        engine: Optional[str] = None,
    ) -> "LoggingContext":
        """Create a new LoggingContext with updated context."""
        return LoggingContext(
            logger=self._logger,
            pipeline_id=pipeline_id or self.pipeline_id,
            node_id=node_id or self.node_id,
            engine=engine or self.engine,
        )

    def __enter__(self) -> "LoggingContext":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            self.log_exception(exc_val, operation="context_exit")

    def info(self, message: str, **kwargs) -> None:
        """Log info message with context."""
        self.logger.info(message, **{**self._base_context(), **kwargs})

    def warning(self, message: str, **kwargs) -> None:
        """Log warning message with context."""
        self.logger.warning(message, **{**self._base_context(), **kwargs})

    def error(self, message: str, **kwargs) -> None:
        """Log error message with context."""
        self.logger.error(message, **{**self._base_context(), **kwargs})

    def debug(self, message: str, **kwargs) -> None:
        """Log debug message with context."""
        self.logger.debug(message, **{**self._base_context(), **kwargs})

    @contextmanager
    def operation(
        self,
        op_type: OperationType,
        description: str = "",
        **initial_context,
    ):
        """Context manager for timed operations with automatic logging.

        Args:
            op_type: Type of operation
            description: Human-readable description
            **initial_context: Additional context to include

        Yields:
            OperationMetrics object to populate with results

        Example:
            >>> with ctx.operation(OperationType.TRANSFORM, "apply_filter") as metrics:
            ...     metrics.rows_in = len(df)
            ...     result = df.filter(...)
            ...     metrics.rows_out = len(result)
        """
        metrics = OperationMetrics()
        self._current_metrics = metrics
        self._operation_stack.append((op_type, description, time.time()))

        self.debug(
            f"Starting {op_type.value}: {description}",
            operation=op_type.value,
            **initial_context,
        )

        try:
            yield metrics
            metrics.end_time = time.time()

            log_data = {
                "operation": op_type.value,
                **metrics.to_dict(),
                **initial_context,
            }

            self.info(f"Completed {op_type.value}: {description}", **log_data)

        except Exception as e:
            metrics.end_time = time.time()
            self.log_exception(
                e,
                operation=op_type.value,
                description=description,
                elapsed_ms=metrics.elapsed_ms,
            )
            raise
        finally:
            self._operation_stack.pop()
            self._current_metrics = None

    def log_operation_start(
        self,
        op_type: OperationType,
        description: str = "",
        **kwargs,
    ) -> OperationMetrics:
        """Log operation start and return metrics tracker.

        Args:
            op_type: Type of operation
            description: Operation description
            **kwargs: Additional context

        Returns:
            OperationMetrics to track operation details
        """
        metrics = OperationMetrics()
        self._current_metrics = metrics
        self._operation_stack.append((op_type, description, time.time()))

        self.debug(
            f"Starting {op_type.value}: {description}",
            operation=op_type.value,
            **kwargs,
        )

        return metrics

    def log_operation_end(
        self,
        metrics: Optional[OperationMetrics] = None,
        success: bool = True,
        **kwargs,
    ) -> None:
        """Log operation completion.

        Args:
            metrics: Metrics from log_operation_start (uses current if None)
            success: Whether operation succeeded
            **kwargs: Additional context
        """
        if metrics is None:
            metrics = self._current_metrics

        if metrics is not None:
            metrics.end_time = time.time()

        if self._operation_stack:
            op_type, description, _ = self._operation_stack.pop()
        else:
            op_type, description = OperationType.EXECUTE, "unknown"

        log_data = {"operation": op_type.value, "success": success, **kwargs}

        if metrics is not None:
            log_data.update(metrics.to_dict())

        if success:
            self.info(f"Completed {op_type.value}: {description}", **log_data)
        else:
            self.warning(f"Failed {op_type.value}: {description}", **log_data)

        self._current_metrics = None

    def log_exception(
        self,
        exception: Exception,
        operation: Optional[str] = None,
        include_traceback: bool = False,
        **kwargs,
    ) -> None:
        """Log exception with context.

        Args:
            exception: The exception to log
            operation: Operation that failed
            include_traceback: Whether to include full traceback
            **kwargs: Additional context
        """
        error_data = {
            "error_type": type(exception).__name__,
            "error_message": str(exception),
            **kwargs,
        }

        if operation:
            error_data["operation"] = operation

        if include_traceback:
            error_data["traceback"] = traceback.format_exc()

        self.error(f"Exception: {type(exception).__name__}: {exception}", **error_data)

    def log_schema_change(
        self,
        before: Dict[str, str],
        after: Dict[str, str],
        operation: str = "transform",
    ) -> None:
        """Log schema changes between transformations.

        Args:
            before: Schema before transformation
            after: Schema after transformation
            operation: Name of the transformation
        """
        cols_before = set(before.keys())
        cols_after = set(after.keys())

        added = cols_after - cols_before
        removed = cols_before - cols_after

        type_changes = {}
        for col in cols_before & cols_after:
            if before[col] != after[col]:
                type_changes[col] = f"{before[col]} -> {after[col]}"

        self.debug(
            f"Schema change in {operation}",
            operation=operation,
            columns_before=len(cols_before),
            columns_after=len(cols_after),
            columns_added=list(added) if added else None,
            columns_removed=list(removed) if removed else None,
            type_changes=type_changes if type_changes else None,
        )

    def log_row_count_change(
        self,
        before: int,
        after: int,
        operation: str = "transform",
    ) -> None:
        """Log row count changes.

        Args:
            before: Row count before
            after: Row count after
            operation: Name of the transformation
        """
        delta = after - before
        pct_change = ((after - before) / before * 100) if before > 0 else 0

        msg = (
            f"Row count change in {operation}: {before} -> {after} ({delta:+d}, {pct_change:+.1f}%)"
        )
        self.debug(msg, operation=operation, rows_before=before, rows_after=after)

    def log_spark_metrics(
        self,
        partition_count: Optional[int] = None,
        shuffle_partitions: Optional[int] = None,
        broadcast_size_mb: Optional[float] = None,
        cached: bool = False,
        **kwargs,
    ) -> None:
        """Log Spark-specific metrics.

        Args:
            partition_count: Number of partitions
            shuffle_partitions: Shuffle partition count
            broadcast_size_mb: Broadcast variable size
            cached: Whether data is cached
            **kwargs: Additional metrics
        """
        metrics = {}
        if partition_count is not None:
            metrics["partitions"] = partition_count
        if shuffle_partitions is not None:
            metrics["shuffle_partitions"] = shuffle_partitions
        if broadcast_size_mb is not None:
            metrics["broadcast_size_mb"] = broadcast_size_mb
        if cached:
            metrics["cached"] = cached
        metrics.update(kwargs)

        if metrics:
            self.debug("Spark metrics", **metrics)

    def log_pandas_metrics(
        self,
        memory_mb: Optional[float] = None,
        dtypes: Optional[Dict[str, str]] = None,
        chunked: bool = False,
        chunk_size: Optional[int] = None,
        **kwargs,
    ) -> None:
        """Log Pandas-specific metrics.

        Args:
            memory_mb: Memory footprint in MB
            dtypes: Column dtypes
            chunked: Whether using chunked processing
            chunk_size: Chunk size if chunked
            **kwargs: Additional metrics
        """
        metrics = {}
        if memory_mb is not None:
            metrics["memory_mb"] = round(memory_mb, 2)
            if memory_mb > 1000:
                self.warning(
                    f"High memory usage: {memory_mb:.2f} MB",
                    memory_mb=round(memory_mb, 2),
                )
        if dtypes:
            metrics["dtype_count"] = len(dtypes)
        if chunked:
            metrics["chunked"] = True
            if chunk_size:
                metrics["chunk_size"] = chunk_size
        metrics.update(kwargs)

        if metrics:
            self.debug("Pandas metrics", **metrics)

    def log_validation_result(
        self,
        passed: bool,
        rule_name: str,
        failures: Optional[List[str]] = None,
        **kwargs,
    ) -> None:
        """Log validation result.

        Args:
            passed: Whether validation passed
            rule_name: Name of validation rule
            failures: List of failure messages
            **kwargs: Additional context
        """
        if passed:
            self.debug(f"Validation passed: {rule_name}", rule=rule_name, passed=True, **kwargs)
        else:
            self.warning(
                f"Validation failed: {rule_name}",
                rule=rule_name,
                passed=False,
                failures=failures,
                **kwargs,
            )

    def log_connection(
        self,
        connection_type: str,
        connection_name: str,
        action: str = "connect",
        **kwargs,
    ) -> None:
        """Log connection activity.

        Args:
            connection_type: Type of connection (azure_blob, sql_server, etc.)
            connection_name: Name of the connection
            action: Action being performed
            **kwargs: Additional context (excluding secrets)
        """
        self.debug(
            f"Connection {action}: {connection_name}",
            connection_type=connection_type,
            connection_name=connection_name,
            action=action,
            **kwargs,
        )

    def log_file_io(
        self,
        path: str,
        format: str,
        mode: str,
        rows: Optional[int] = None,
        size_mb: Optional[float] = None,
        partitions: Optional[List[str]] = None,
        **kwargs,
    ) -> None:
        """Log file I/O operations.

        Args:
            path: File path
            format: File format (csv, parquet, delta, etc.)
            mode: I/O mode (read, write, append, overwrite)
            rows: Row count
            size_mb: File size in MB
            partitions: Partition columns
            **kwargs: Additional context
        """
        log_data = {
            "path": path,
            "format": format,
            "mode": mode,
        }
        if rows is not None:
            log_data["rows"] = rows
        if size_mb is not None:
            log_data["size_mb"] = round(size_mb, 2)
        if partitions:
            log_data["partitions"] = partitions
        log_data.update(kwargs)

        self.info(f"File I/O: {mode} {format} at {path}", **log_data)

    def log_graph_operation(
        self,
        operation: str,
        node_count: Optional[int] = None,
        edge_count: Optional[int] = None,
        layer_count: Optional[int] = None,
        **kwargs,
    ) -> None:
        """Log dependency graph operations.

        Args:
            operation: Graph operation (load, resolve, validate, etc.)
            node_count: Number of nodes
            edge_count: Number of edges/dependencies
            layer_count: Number of execution layers
            **kwargs: Additional context
        """
        log_data = {"operation": operation}
        if node_count is not None:
            log_data["nodes"] = node_count
        if edge_count is not None:
            log_data["edges"] = edge_count
        if layer_count is not None:
            log_data["layers"] = layer_count
        log_data.update(kwargs)

        self.debug(f"Graph {operation}", **log_data)


_global_context: Optional[LoggingContext] = None


def get_logging_context() -> LoggingContext:
    """Get the global logging context."""
    global _global_context
    if _global_context is None:
        from odibi.utils.logging import logger

        _global_context = LoggingContext(logger=logger)
    return _global_context


def set_logging_context(context: LoggingContext) -> None:
    """Set the global logging context."""
    global _global_context
    _global_context = context


def create_logging_context(
    pipeline_id: Optional[str] = None,
    node_id: Optional[str] = None,
    engine: Optional[str] = None,
) -> LoggingContext:
    """Create a new logging context with the specified parameters.

    Args:
        pipeline_id: Pipeline identifier
        node_id: Node identifier
        engine: Engine type

    Returns:
        New LoggingContext instance
    """
    from odibi.utils.logging import logger

    return LoggingContext(
        logger=logger,
        pipeline_id=pipeline_id,
        node_id=node_id,
        engine=engine,
    )

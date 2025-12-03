"""Utilities for ODIBI setup and configuration.

Includes:
- Configuration loading with env var substitution
- Structured logging and context-aware logging
- Key Vault and connection helpers
- Rich console output utilities
- Pipeline progress visualization
"""

from .config_loader import load_yaml_with_env
from .console import (
    error,
    get_console,
    info,
    is_rich_available,
    print_panel,
    print_rule,
    print_table,
    success,
    warning,
)
from .logging import StructuredLogger, configure_logging, logger
from .logging_context import (
    LoggingContext,
    OperationMetrics,
    OperationType,
    create_logging_context,
    get_logging_context,
    set_logging_context,
)
from .progress import NodeStatus, PipelineProgress
from .setup_helpers import (
    configure_connections_parallel,
    fetch_keyvault_secrets_parallel,
    validate_databricks_environment,
)

__all__ = [
    "fetch_keyvault_secrets_parallel",
    "configure_connections_parallel",
    "validate_databricks_environment",
    "load_yaml_with_env",
    "StructuredLogger",
    "configure_logging",
    "logger",
    "LoggingContext",
    "OperationMetrics",
    "OperationType",
    "create_logging_context",
    "get_logging_context",
    "set_logging_context",
    # Console utilities
    "is_rich_available",
    "get_console",
    "success",
    "error",
    "warning",
    "info",
    "print_table",
    "print_panel",
    "print_rule",
    # Progress utilities
    "NodeStatus",
    "PipelineProgress",
]

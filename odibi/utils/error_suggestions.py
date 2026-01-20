"""Centralized error suggestion engine for ODIBI.

This module provides pattern-based error analysis and actionable suggestions
for all error types across the framework. Any component can use get_suggestions()
to get context-aware help for users.

Usage:
    from odibi.utils.error_suggestions import get_suggestions, ErrorContext

    ctx = ErrorContext(
        engine="pandas",
        connection_name="wwi_oltp",
        connection_type="azure_sql",
        node_name="bronze_customers",
        available_columns=["CustomerID", "CustomerName"],
    )
    suggestions = get_suggestions(error, ctx)
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set
import re


@dataclass
class ErrorContext:
    """Context for generating relevant suggestions."""

    engine: Optional[str] = None
    connection_name: Optional[str] = None
    connection_type: Optional[str] = None
    node_name: Optional[str] = None
    pipeline_name: Optional[str] = None
    transformer_name: Optional[str] = None
    available_columns: Optional[List[str]] = None
    expected_columns: Optional[List[str]] = None
    table_name: Optional[str] = None
    path: Optional[str] = None
    format: Optional[str] = None
    auth_mode: Optional[str] = None
    step_index: Optional[int] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ErrorPattern:
    """A pattern that matches errors and provides suggestions."""

    name: str
    patterns: List[str]
    suggestions: List[str]
    condition: Optional[Callable[[str, ErrorContext], bool]] = None
    dynamic_suggestions: Optional[Callable[[str, ErrorContext], List[str]]] = None

    def matches(self, error_msg: str, ctx: ErrorContext) -> bool:
        """Check if this pattern matches the error."""
        error_lower = error_msg.lower()
        pattern_match = any(p.lower() in error_lower for p in self.patterns)
        if not pattern_match:
            return False
        if self.condition and not self.condition(error_msg, ctx):
            return False
        return True

    def get_suggestions(self, error_msg: str, ctx: ErrorContext) -> List[str]:
        """Get suggestions for this error."""
        result = list(self.suggestions)
        if self.dynamic_suggestions:
            result.extend(self.dynamic_suggestions(error_msg, ctx))
        return result


# =============================================================================
# ODBC / Driver Errors
# =============================================================================

ODBC_PATTERNS = [
    ErrorPattern(
        name="odbc_driver_missing",
        patterns=["odbc driver", "driver not found", "im002", "data source name not found"],
        suggestions=[
            "Install ODBC Driver 18 for SQL Server",
            "Windows: Download from Microsoft (search 'ODBC Driver 18 SQL Server')",
            "Linux: sudo apt-get install msodbcsql18 (or msodbcsql17)",
            "macOS: brew install microsoft/mssql-release/msodbcsql18",
            "After install, restart your terminal/IDE",
        ],
    ),
    ErrorPattern(
        name="odbc_connection_timeout",
        patterns=["timeout expired", "connection timeout", "login timeout"],
        suggestions=[
            "Check network connectivity to the SQL server",
            "Verify the server name is correct (include .database.windows.net for Azure)",
            "Increase timeout in connection config if on slow network",
            "Check if VPN is required to access the server",
        ],
    ),
]

# =============================================================================
# Azure SQL Errors
# =============================================================================

AZURE_SQL_PATTERNS = [
    ErrorPattern(
        name="azure_sql_login_failed",
        patterns=["login failed", "authentication failed"],
        suggestions=[
            "Verify username and password are correct",
            "Check that SQL authentication is enabled on the server",
            "For AAD auth, ensure the identity has database access",
        ],
        dynamic_suggestions=lambda msg, ctx: [
            f"Current auth_mode: {ctx.auth_mode}" if ctx.auth_mode else None,
            "If using environment variables, verify they are set correctly",
        ],
    ),
    ErrorPattern(
        name="azure_sql_firewall",
        patterns=["firewall", "tcp provider", "connection refused", "network-related"],
        suggestions=[
            "Add your IP to Azure SQL Server firewall rules",
            "Go to Azure Portal > SQL Server > Networking > Add client IP",
            "If running in Azure (Databricks, etc.), enable 'Allow Azure services'",
            "Check if private endpoint is required",
        ],
    ),
    ErrorPattern(
        name="azure_sql_invalid_object",
        patterns=["invalid object name", "table doesn't exist", "object not found"],
        suggestions=[
            "Verify the table/schema name is correct (case-sensitive)",
            "Check you're connected to the correct database",
            "Use fully qualified name: [schema].[table]",
        ],
        dynamic_suggestions=lambda msg, ctx: [
            f"Attempted table: {ctx.table_name}" if ctx.table_name else None,
        ],
    ),
    ErrorPattern(
        name="azure_sql_permission",
        patterns=["permission denied", "access denied", "execute permission"],
        suggestions=[
            "Grant SELECT permission on the table to your user",
            "Check database role membership",
            "For service principals, ensure proper role assignment",
        ],
    ),
]

# =============================================================================
# Delta Lake Errors
# =============================================================================

DELTA_PATTERNS = [
    ErrorPattern(
        name="delta_not_found",
        patterns=["delta table not found", "is not a delta table", "path does not exist"],
        suggestions=[
            "Verify the path exists and contains a _delta_log folder",
            "For first run, use mode='overwrite' to create the table",
            "Check connection path_prefix is correct",
        ],
        dynamic_suggestions=lambda msg, ctx: [
            f"Target path: {ctx.path}" if ctx.path else None,
        ],
    ),
    ErrorPattern(
        name="delta_schema_mismatch",
        patterns=["schema mismatch", "cannot write", "field.*not found in schema"],
        suggestions=[
            "Enable schema evolution: merge_schema=True or overwrite_schema=True",
            "Check that column names match exactly (case-sensitive)",
            "Compare source schema with target Delta table schema",
        ],
    ),
    ErrorPattern(
        name="delta_concurrent_write",
        patterns=["concurrent", "conflicting", "transaction conflict"],
        suggestions=[
            "Enable retries for concurrent writes",
            "Use merge operations instead of overwrites for concurrent access",
            "Check if another job is writing to the same table",
        ],
    ),
    ErrorPattern(
        name="delta_version",
        patterns=["protocol version", "reader version", "writer version"],
        suggestions=[
            "Upgrade delta-spark or deltalake library",
            "Check Delta table was created with compatible version",
            "May need to upgrade the Delta table: UPGRADE TABLE",
        ],
    ),
]

# =============================================================================
# ADLS / Azure Storage Errors
# =============================================================================

ADLS_PATTERNS = [
    ErrorPattern(
        name="adls_auth_failed",
        patterns=["authenticationfailed", "authorizationfailed", "403 forbidden", "access denied"],
        suggestions=[
            "Verify storage account key or SAS token is correct",
            "Check container-level permissions",
            "For service principal, verify RBAC role: Storage Blob Data Contributor",
        ],
        dynamic_suggestions=lambda msg, ctx: [
            f"Current auth_mode: {ctx.auth_mode}" if ctx.auth_mode else None,
        ],
    ),
    ErrorPattern(
        name="adls_not_found",
        patterns=["containernotfound", "blobnotfound", "pathnotfound", "404"],
        suggestions=[
            "Verify container name exists in the storage account",
            "Check path_prefix is correct (no leading slash)",
            "Ensure the blob/file exists at the specified path",
        ],
    ),
    ErrorPattern(
        name="adls_account_not_found",
        patterns=["accountnotfound", "name or service not known", "storage account"],
        suggestions=[
            "Verify storage account name is correct",
            "Check the account exists and is accessible",
            "Account URL should be: accountname.dfs.core.windows.net",
        ],
    ),
]

# =============================================================================
# Helper Functions for Dynamic Suggestions (defined early for forward refs)
# =============================================================================


def _column_mismatch_suggestions(error_msg: str, ctx: ErrorContext) -> List[str]:
    """Generate suggestions for column mismatch errors."""
    suggestions = []

    if ctx.available_columns:
        col_list = ", ".join(ctx.available_columns[:10])
        if len(ctx.available_columns) > 10:
            col_list += f"... (+{len(ctx.available_columns) - 10} more)"
        suggestions.append(f"Available columns: {col_list}")

    if ctx.expected_columns:
        missing = set(ctx.expected_columns) - set(ctx.available_columns or [])
        if missing:
            suggestions.append(f"Missing columns: {', '.join(missing)}")

    # Try to extract the problematic column from error message
    match = re.search(r"['\"]([^'\"]+)['\"]", error_msg)
    if match and ctx.available_columns:
        bad_col = match.group(1)
        # Find similar columns (basic fuzzy match)
        similar = [
            c
            for c in ctx.available_columns
            if bad_col.lower() in c.lower() or c.lower() in bad_col.lower()
        ]
        if similar:
            suggestions.append(f"Did you mean: {', '.join(similar[:3])}?")

    return [s for s in suggestions if s]


def _extract_module_suggestion(error_msg: str) -> List[str]:
    """Extract module name and suggest install command."""
    match = re.search(r"No module named ['\"]?([a-zA-Z0-9_]+)", error_msg)
    if match:
        module = match.group(1)
        return [f"Try: pip install {module}"]
    return []


# =============================================================================
# Spark Errors
# =============================================================================

SPARK_PATTERNS = [
    ErrorPattern(
        name="spark_analysis_exception",
        patterns=["analysisexception", "cannot resolve", "column.*cannot be resolved"],
        suggestions=[
            "Check column names match exactly (Spark is case-sensitive by default)",
            "Verify the DataFrame is registered in the correct context",
            "Use df.printSchema() to see available columns",
        ],
        dynamic_suggestions=_column_mismatch_suggestions,
    ),
    ErrorPattern(
        name="spark_class_not_found",
        patterns=["classnotfound", "noclassdef", "delta", "jar"],
        suggestions=[
            "Ensure delta-spark package is installed: pip install delta-spark",
            "For Databricks, Delta is built-in - check cluster config",
            "Verify Spark session has Delta extensions configured",
        ],
    ),
    ErrorPattern(
        name="spark_memory",
        patterns=["outofmemory", "java heap space", "gc overhead"],
        suggestions=[
            "Increase driver/executor memory in Spark config",
            "Repartition large DataFrames: df.repartition(n)",
            "Use persist() strategically to avoid recomputation",
            "Process data in smaller batches",
        ],
    ),
    ErrorPattern(
        name="spark_parse_exception",
        patterns=["parseexception", "syntax error", "mismatched input"],
        suggestions=[
            "Check SQL syntax is valid",
            "Escape special characters in column names with backticks: `column-name`",
            "Verify string literals use single quotes",
        ],
    ),
]

# =============================================================================
# Pandas Errors
# =============================================================================

PANDAS_PATTERNS = [
    ErrorPattern(
        name="pandas_keyerror",
        patterns=["keyerror"],
        suggestions=[
            "Column name not found in DataFrame",
            "Check for typos in column name (case-sensitive)",
            "Use df.columns.tolist() to see available columns",
        ],
        dynamic_suggestions=_column_mismatch_suggestions,
    ),
    ErrorPattern(
        name="pandas_memory",
        patterns=["memoryerror", "unable to allocate"],
        suggestions=[
            "Data too large for Pandas - consider using Spark or Polars",
            "Read data in chunks: pd.read_sql(..., chunksize=10000)",
            "Select only needed columns instead of SELECT *",
            "Use appropriate dtypes (e.g., category for low-cardinality strings)",
        ],
    ),
    ErrorPattern(
        name="pandas_dtype_mismatch",
        patterns=["cannot convert", "dtype", "invalid literal", "could not convert"],
        suggestions=[
            "Data type mismatch during conversion",
            "Use errors='coerce' to handle invalid values",
            "Check source data for unexpected values (nulls, strings in numeric columns)",
        ],
    ),
]

# =============================================================================
# Polars Errors
# =============================================================================

POLARS_PATTERNS = [
    ErrorPattern(
        name="polars_column_not_found",
        patterns=["columnnotfounderror", "column.*not found"],
        suggestions=[
            "Column name not found - Polars is case-sensitive",
            "Use df.columns to see available columns",
            "Check column wasn't renamed in a previous step",
        ],
        dynamic_suggestions=_column_mismatch_suggestions,
    ),
    ErrorPattern(
        name="polars_schema",
        patterns=["schemaerror", "schema mismatch"],
        suggestions=[
            "Schema doesn't match expected structure",
            "Use strict=False for lenient type casting",
            "Check column types match between operations",
        ],
    ),
]

# =============================================================================
# Schema / Column Errors
# =============================================================================

SCHEMA_PATTERNS = [
    ErrorPattern(
        name="column_not_found",
        patterns=["column", "not found", "does not exist", "missing column"],
        suggestions=[
            "Verify column name spelling (case-sensitive in most engines)",
            "Check if column was renamed or dropped in a previous step",
            "Use 'odibi run --node NODE_NAME --show-schema' to debug",
        ],
        dynamic_suggestions=_column_mismatch_suggestions,
    ),
    ErrorPattern(
        name="duplicate_columns",
        patterns=["duplicate", "column names", "ambiguous"],
        suggestions=[
            "Rename columns before join to avoid duplicates",
            "Use suffixes parameter in join operations",
            "Check that derived columns don't conflict with existing names",
        ],
    ),
]

# =============================================================================
# Validation Errors
# =============================================================================

VALIDATION_PATTERNS = [
    ErrorPattern(
        name="validation_failed",
        patterns=["validation failed", "quality gate", "pass rate"],
        suggestions=[
            "Check validation rules against actual data",
            "Inspect quarantine table for failed rows",
            "Review the story output for sample failures",
            "Consider adjusting threshold if data quality is expected to be lower initially",
        ],
    ),
    ErrorPattern(
        name="fk_validation_failed",
        patterns=["foreign key", "orphan", "referential integrity"],
        suggestions=[
            "Parent table may be missing referenced values",
            "Check load order - parent tables should load before children",
            "Use on_violation='warn' to log without failing",
            "Inspect orphan records in the validation output",
        ],
    ),
    ErrorPattern(
        name="null_constraint",
        patterns=["null", "not null", "required field", "missing required"],
        suggestions=[
            "Source data contains unexpected nulls",
            "Add null handling in transform: coalesce or fill_na",
            "Check if source extraction is incomplete",
        ],
    ),
]

# =============================================================================
# Transformer Errors
# =============================================================================

TRANSFORMER_PATTERNS = [
    ErrorPattern(
        name="transformer_not_found",
        patterns=["function", "not registered", "unknown transformer", "not found"],
        suggestions=[
            "Check transformer name spelling",
            "Run 'odibi list transformers' to see available transformers",
            "Custom transformers need @transform decorator and import",
        ],
        dynamic_suggestions=lambda msg, ctx: [
            f"Looking for: {ctx.transformer_name}" if ctx.transformer_name else None,
            "Run 'odibi explain <transformer_name>' for usage",
        ],
    ),
    ErrorPattern(
        name="transformer_params",
        patterns=["missing required", "parameter", "argument", "unexpected keyword"],
        suggestions=[
            "Check transformer parameters against documentation",
            "Run 'odibi explain <transformer_name>' for required params",
            "Verify YAML indentation for nested parameters",
        ],
    ),
    ErrorPattern(
        name="scd2_error",
        patterns=["scd2", "slowly changing", "effective_from", "effective_to"],
        suggestions=[
            "Verify key_columns and tracked_columns are correct",
            "Check that target table exists or use initial load first",
            "Ensure date columns are proper datetime types",
        ],
    ),
    ErrorPattern(
        name="merge_error",
        patterns=["merge", "upsert", "match_keys"],
        suggestions=[
            "Verify match_keys exist in both source and target",
            "Check that target path/table exists",
            "For Delta, ensure table was created with proper keys",
        ],
    ),
]

# =============================================================================
# Connection Errors
# =============================================================================

CONNECTION_PATTERNS = [
    ErrorPattern(
        name="connection_not_found",
        patterns=["connection", "not found", "not defined", "undefined connection"],
        suggestions=[
            "Check connection name in YAML matches definition",
            "Verify connections: section exists in config",
            "Connection names are case-sensitive",
        ],
        dynamic_suggestions=lambda msg, ctx: [
            f"Looking for connection: {ctx.connection_name}" if ctx.connection_name else None,
        ],
    ),
    ErrorPattern(
        name="env_var_missing",
        patterns=["${", "environment variable", "env var", "not set"],
        suggestions=[
            "Set the environment variable or use .env file",
            "Verify .env file is in the same directory as config",
            "Check variable name spelling (case-sensitive)",
            "Use: export VAR_NAME=value (Linux/Mac) or set VAR_NAME=value (Windows)",
        ],
    ),
]

# =============================================================================
# YAML / Config Errors
# =============================================================================

CONFIG_PATTERNS = [
    ErrorPattern(
        name="yaml_parse_error",
        patterns=["yaml", "parse error", "scanner error", "mapping values"],
        suggestions=[
            "Check YAML indentation (use spaces, not tabs)",
            "Verify all strings with special chars are quoted",
            "Use a YAML validator to check syntax",
            "Common issue: missing space after colon",
        ],
    ),
    ErrorPattern(
        name="config_validation",
        patterns=["validation error", "pydantic", "field required", "extra fields"],
        suggestions=[
            "Check config against YAML schema documentation",
            "Run 'odibi explain <pattern_name>' for valid fields",
            "Remove any unrecognized fields",
        ],
    ),
]

# =============================================================================
# Import / Dependency Errors
# =============================================================================

DEPENDENCY_PATTERNS = [
    ErrorPattern(
        name="import_error",
        patterns=["modulenotfounderror", "no module named", "importerror"],
        suggestions=[
            "Install missing package: pip install <package>",
            "For optional dependencies, install with extras: pip install odibi[spark]",
            "Check virtual environment is activated",
        ],
        dynamic_suggestions=lambda msg, ctx: _extract_module_suggestion(msg),
    ),
    ErrorPattern(
        name="polars_not_installed",
        patterns=["polars not installed", "polars is not"],
        suggestions=[
            "Install Polars: pip install polars",
            "Or install with odibi: pip install odibi[polars]",
        ],
    ),
    ErrorPattern(
        name="spark_not_installed",
        patterns=["spark not installed", "pyspark", "spark is not"],
        suggestions=[
            "Install PySpark: pip install pyspark",
            "Or install with odibi: pip install odibi[spark]",
            "For Delta support: pip install delta-spark",
        ],
    ),
    ErrorPattern(
        name="deltalake_not_installed",
        patterns=["deltalake", "delta-rs", "deltalake library"],
        suggestions=[
            "Install deltalake: pip install deltalake",
            "Required for Pandas/Polars Delta operations",
            "Spark uses delta-spark instead",
        ],
    ),
]


# =============================================================================
# Pattern Registry
# =============================================================================

ALL_PATTERNS: List[ErrorPattern] = [
    *ODBC_PATTERNS,
    *AZURE_SQL_PATTERNS,
    *DELTA_PATTERNS,
    *ADLS_PATTERNS,
    *SPARK_PATTERNS,
    *PANDAS_PATTERNS,
    *POLARS_PATTERNS,
    *SCHEMA_PATTERNS,
    *VALIDATION_PATTERNS,
    *TRANSFORMER_PATTERNS,
    *CONNECTION_PATTERNS,
    *CONFIG_PATTERNS,
    *DEPENDENCY_PATTERNS,
]


def get_suggestions(
    error: Exception,
    ctx: Optional[ErrorContext] = None,
    include_debug_command: bool = True,
) -> List[str]:
    """Get actionable suggestions for an error.

    Args:
        error: The exception that occurred
        ctx: Optional context about where the error occurred
        include_debug_command: Whether to include odibi CLI debug hints

    Returns:
        List of actionable suggestions

    Example:
        >>> from odibi.utils.error_suggestions import get_suggestions, ErrorContext
        >>> ctx = ErrorContext(engine="pandas", node_name="my_node")
        >>> suggestions = get_suggestions(my_error, ctx)
        >>> for s in suggestions:
        ...     print(f"  - {s}")
    """
    ctx = ctx or ErrorContext()
    error_msg = str(error)
    error_type = type(error).__name__

    # Combine error message and type for matching
    full_error = f"{error_type}: {error_msg}"

    suggestions: List[str] = []
    matched_patterns: Set[str] = set()

    for pattern in ALL_PATTERNS:
        if pattern.matches(full_error, ctx):
            if pattern.name not in matched_patterns:
                suggestions.extend(pattern.get_suggestions(full_error, ctx))
                matched_patterns.add(pattern.name)

    # Deduplicate while preserving order
    seen = set()
    unique_suggestions = []
    for s in suggestions:
        if s and s not in seen:
            seen.add(s)
            unique_suggestions.append(s)

    # Add debug command if node context available
    if include_debug_command and ctx.node_name:
        unique_suggestions.append(f"Debug: odibi run CONFIG.yaml --node {ctx.node_name} --dry-run")

    return unique_suggestions


def get_suggestions_for_connection(
    error: Exception,
    connection_name: str,
    connection_type: str,
    auth_mode: Optional[str] = None,
) -> List[str]:
    """Convenience function for connection errors.

    Args:
        error: The exception that occurred
        connection_name: Name of the connection that failed
        connection_type: Type of connection (azure_sql, azure_adls, etc.)
        auth_mode: Authentication mode if applicable

    Returns:
        List of actionable suggestions
    """
    ctx = ErrorContext(
        connection_name=connection_name,
        connection_type=connection_type,
        auth_mode=auth_mode,
    )
    return get_suggestions(error, ctx, include_debug_command=False)


def get_suggestions_for_transform(
    error: Exception,
    node_name: str,
    transformer_name: str,
    engine: str,
    available_columns: Optional[List[str]] = None,
    step_index: Optional[int] = None,
) -> List[str]:
    """Convenience function for transformer errors.

    Args:
        error: The exception that occurred
        node_name: Name of the node being executed
        transformer_name: Name of the transformer that failed
        engine: Engine type (pandas, spark, polars)
        available_columns: Columns available in the input DataFrame
        step_index: Index of the transform step that failed

    Returns:
        List of actionable suggestions
    """
    ctx = ErrorContext(
        node_name=node_name,
        transformer_name=transformer_name,
        engine=engine,
        available_columns=available_columns,
        step_index=step_index,
    )
    return get_suggestions(error, ctx)


def format_error_with_suggestions(
    error: Exception,
    ctx: Optional[ErrorContext] = None,
    max_suggestions: int = 5,
) -> str:
    """Format an error message with suggestions.

    Args:
        error: The exception that occurred
        ctx: Optional context
        max_suggestions: Maximum suggestions to include

    Returns:
        Formatted error string with suggestions
    """
    suggestions = get_suggestions(error, ctx)[:max_suggestions]

    parts = [f"Error: {error}"]

    if suggestions:
        parts.append("\n\nSuggestions:")
        for i, s in enumerate(suggestions, 1):
            parts.append(f"\n  {i}. {s}")

    return "".join(parts)

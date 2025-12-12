"""Code execution tools for running Python and SQL in Databricks.

Enables the agent to execute code snippets, run SQL queries,
and interact with Spark DataFrames.
"""

import io
import traceback
from contextlib import redirect_stdout, redirect_stderr
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class ExecutionResult:
    """Result of code execution."""

    success: bool
    output: str
    error: Optional[str] = None
    result_value: Optional[Any] = None
    execution_time: float = 0.0


def is_databricks() -> bool:
    """Check if running in Databricks."""
    import os
    return (
        "DATABRICKS_RUNTIME_VERSION" in os.environ
        or "SPARK_HOME" in os.environ
        or _check_spark_available()
    )


def _check_spark_available() -> bool:
    """Check if Spark is available."""
    try:
        from pyspark.sql import SparkSession
        return SparkSession.getActiveSession() is not None
    except ImportError:
        return False


def get_spark_session():
    """Get the active Spark session."""
    try:
        from pyspark.sql import SparkSession
        return SparkSession.getActiveSession()
    except ImportError:
        return None


def execute_python(
    code: str,
    timeout: int = 30,
    capture_display: bool = True,
) -> ExecutionResult:
    """Execute Python code and return the result.

    Args:
        code: Python code to execute.
        timeout: Maximum execution time in seconds.
        capture_display: Whether to capture print statements.

    Returns:
        ExecutionResult with output and any errors.
    """
    import time

    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()

    namespace = {
        "__builtins__": __builtins__,
        "print": print,
    }

    if is_databricks():
        spark = get_spark_session()
        if spark:
            namespace["spark"] = spark
            namespace["sc"] = spark.sparkContext

        try:
            import pandas as pd
            namespace["pd"] = pd
        except ImportError:
            pass

        try:
            import numpy as np
            namespace["np"] = np
        except ImportError:
            pass

    start_time = time.time()

    try:
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            exec(code, namespace)

            if "\n" not in code and not code.strip().startswith(("import", "from", "def", "class", "if", "for", "while", "with", "try")):
                try:
                    eval_result = eval(code.split("\n")[-1], namespace)
                    if eval_result is not None:
                        print(repr(eval_result))
                except Exception:
                    pass

        execution_time = time.time() - start_time
        output = stdout_capture.getvalue()
        errors = stderr_capture.getvalue()

        if errors:
            output += f"\n[stderr]: {errors}"

        return ExecutionResult(
            success=True,
            output=output if output else "(No output)",
            execution_time=execution_time,
        )

    except Exception:
        execution_time = time.time() - start_time
        error_msg = traceback.format_exc()

        return ExecutionResult(
            success=False,
            output=stdout_capture.getvalue(),
            error=error_msg,
            execution_time=execution_time,
        )


def execute_sql(
    query: str,
    limit: int = 100,
    show_schema: bool = False,
) -> ExecutionResult:
    """Execute a SQL query using Spark.

    Args:
        query: SQL query to execute.
        limit: Maximum rows to return.
        show_schema: Whether to include schema info.

    Returns:
        ExecutionResult with query results.
    """
    import time

    spark = get_spark_session()
    if not spark:
        return ExecutionResult(
            success=False,
            output="",
            error="Spark session not available. Are you running in Databricks?",
        )

    start_time = time.time()

    try:
        df = spark.sql(query)

        output_parts = []

        if show_schema:
            schema_str = df._jdf.schema().treeString()
            output_parts.append(f"**Schema:**\n```\n{schema_str}\n```\n")

        pdf = df.limit(limit).toPandas()
        execution_time = time.time() - start_time

        row_count = df.count() if limit < 1000 else f">{limit}"
        output_parts.append(f"**Results:** ({row_count} rows, showing {len(pdf)})\n")
        output_parts.append(pdf.to_markdown(index=False))

        return ExecutionResult(
            success=True,
            output="\n".join(output_parts),
            result_value=pdf,
            execution_time=execution_time,
        )

    except Exception as e:
        execution_time = time.time() - start_time
        return ExecutionResult(
            success=False,
            output="",
            error=str(e),
            execution_time=execution_time,
        )


def list_tables(
    database: Optional[str] = None,
    pattern: Optional[str] = None,
) -> ExecutionResult:
    """List available tables in the catalog.

    Args:
        database: Database/schema to list tables from.
        pattern: Optional pattern to filter table names.

    Returns:
        ExecutionResult with table listing.
    """
    spark = get_spark_session()
    if not spark:
        return ExecutionResult(
            success=False,
            output="",
            error="Spark session not available.",
        )

    try:
        if database:
            tables = spark.catalog.listTables(database)
        else:
            tables = spark.catalog.listTables()

        table_info = []
        for t in tables:
            if pattern and pattern.lower() not in t.name.lower():
                continue
            table_info.append({
                "name": t.name,
                "database": t.database,
                "type": t.tableType,
                "isTemporary": t.isTemporary,
            })

        if not table_info:
            return ExecutionResult(
                success=True,
                output="No tables found.",
            )

        import pandas as pd
        pdf = pd.DataFrame(table_info)
        return ExecutionResult(
            success=True,
            output=pdf.to_markdown(index=False),
            result_value=table_info,
        )

    except Exception as e:
        return ExecutionResult(
            success=False,
            output="",
            error=str(e),
        )


def describe_table(table_name: str) -> ExecutionResult:
    """Get schema and sample data from a table.

    Args:
        table_name: Full table name (database.table or just table).

    Returns:
        ExecutionResult with table description.
    """
    spark = get_spark_session()
    if not spark:
        return ExecutionResult(
            success=False,
            output="",
            error="Spark session not available.",
        )

    try:
        df = spark.table(table_name)

        output_parts = []

        output_parts.append(f"## Table: `{table_name}`\n")

        schema_str = df._jdf.schema().treeString()
        output_parts.append(f"### Schema\n```\n{schema_str}\n```\n")

        row_count = df.count()
        output_parts.append(f"### Row Count: {row_count:,}\n")

        sample = df.limit(5).toPandas()
        output_parts.append(f"### Sample Data\n{sample.to_markdown(index=False)}")

        return ExecutionResult(
            success=True,
            output="\n".join(output_parts),
        )

    except Exception as e:
        return ExecutionResult(
            success=False,
            output="",
            error=str(e),
        )


def format_execution_result(result: ExecutionResult) -> str:
    """Format an execution result for display.

    Args:
        result: ExecutionResult to format.

    Returns:
        Markdown-formatted result.
    """
    if not result.success:
        return f"❌ **Execution Failed**\n\n```\n{result.error}\n```"

    time_str = f" ({result.execution_time:.2f}s)" if result.execution_time > 0.1 else ""

    output = result.output
    if len(output) > 5000:
        output = output[:5000] + "\n\n... (truncated)"

    return f"✅ **Executed**{time_str}\n\n{output}"

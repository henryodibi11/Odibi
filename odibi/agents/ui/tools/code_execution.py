"""Code execution tools for running Python and SQL in Databricks.

Enables the agent to execute code snippets, run SQL queries,
and interact with Spark DataFrames.
"""

import io
import traceback
from contextlib import redirect_stdout, redirect_stderr
from dataclasses import dataclass, field
from typing import Any, Optional

from .output_formatters import (
    auto_format,
    format_dataframe,
    format_plot_as_base64,
    format_spark_schema,
)


@dataclass
class ExecutionResult:
    """Result of code execution."""

    success: bool
    output: str
    error: Optional[str] = None
    result_value: Optional[Any] = None
    execution_time: float = 0.0
    captured_figures: list = field(default_factory=list)  # For matplotlib figures


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
    captured_figures = []
    result_value = None

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

    # Add matplotlib with figure capturing
    try:
        import matplotlib

        matplotlib.use("Agg")  # Non-interactive backend
        import matplotlib.pyplot as plt

        namespace["plt"] = plt
        namespace["matplotlib"] = matplotlib
    except ImportError:
        plt = None

    start_time = time.time()

    try:
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            exec(code, namespace)

            # Try to get the last expression's value for auto-formatting
            lines = code.strip().split("\n")
            last_line = lines[-1].strip() if lines else ""
            if last_line and not last_line.startswith(
                ("import", "from", "def", "class", "if", "for", "while", "with", "try", "#")
            ):
                try:
                    eval_result = eval(last_line, namespace)
                    if eval_result is not None:
                        result_value = eval_result
                except Exception:
                    pass

        # Capture any matplotlib figures
        if plt is not None:
            figs = [plt.figure(i) for i in plt.get_fignums()]
            for fig in figs:
                captured_figures.append(fig)

        execution_time = time.time() - start_time
        output = stdout_capture.getvalue()
        errors = stderr_capture.getvalue()

        if errors:
            output += f"\n[stderr]: {errors}"

        return ExecutionResult(
            success=True,
            output=output if output else "(No output)",
            result_value=result_value,
            execution_time=execution_time,
            captured_figures=captured_figures,
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

        # Show query (truncated)
        query_display = query.strip()
        if len(query_display) > 200:
            query_display = query_display[:200] + "..."
        output_parts.append(f"```sql\n{query_display}\n```")

        if show_schema:
            schema_str = df._jdf.schema().treeString()
            formatted_schema = format_spark_schema(schema_str)
            output_parts.append(formatted_schema)

        pdf = df.limit(limit).toPandas()
        execution_time = time.time() - start_time

        # Format results as clean table
        try:
            total_rows = df.count() if limit < 1000 else None
        except Exception:
            total_rows = None

        if total_rows is not None:
            title = f"Results ({total_rows:,} total rows, showing {len(pdf)})"
        else:
            title = f"Results (showing {len(pdf)} rows)"

        formatted_df = format_dataframe(pdf, title=title, show_dtypes=show_schema)
        output_parts.append(formatted_df)

        return ExecutionResult(
            success=True,
            output="\n\n".join(output_parts),
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
            table_info.append(
                {
                    "name": t.name,
                    "database": t.database,
                    "type": t.tableType,
                    "isTemporary": t.isTemporary,
                }
            )

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

        output_parts.append(f"## 📋 Table: `{table_name}`")

        # Format schema as clean table
        schema_str = df._jdf.schema().treeString()
        formatted_schema = format_spark_schema(schema_str)
        output_parts.append(formatted_schema)

        # Row count
        try:
            row_count = df.count()
            output_parts.append(f"**Row Count:** {row_count:,}")
        except Exception:
            output_parts.append("**Row Count:** *(unable to count)*")

        # Sample data as formatted table
        sample = df.limit(5).toPandas()
        formatted_sample = format_dataframe(sample, title="Sample Data", max_rows=5)
        output_parts.append(formatted_sample)

        return ExecutionResult(
            success=True,
            output="\n\n".join(output_parts),
        )

    except Exception as e:
        return ExecutionResult(
            success=False,
            output="",
            error=str(e),
        )


def format_execution_result(result: ExecutionResult) -> str:
    """Format an execution result for display with smart formatting.

    Automatically formats DataFrames as tables, dicts as JSON, plots as images.

    Args:
        result: ExecutionResult to format.

    Returns:
        Markdown-formatted result.
    """
    if not result.success:
        return f"❌ **Execution Failed**\n\n```\n{result.error}\n```"

    time_str = f" ({result.execution_time:.2f}s)" if result.execution_time > 0.1 else ""
    parts = [f"✅ **Executed**{time_str}"]

    # Format the result value if present (DataFrames, dicts, etc.)
    if result.result_value is not None:
        formatted_value = auto_format(result.result_value)
        parts.append(formatted_value)
    elif result.output and result.output != "(No output)":
        # Format text output with collapsing for large outputs
        output = result.output.strip()
        lines = output.split("\n")

        if len(lines) > 50:
            # Collapse large output
            parts.append(
                f"<details>\n"
                f"<summary>📄 Output ({len(lines)} lines)</summary>\n\n"
                f"```\n{output}\n```\n\n"
                f"</details>"
            )
        elif len(output) > 3000:
            # Truncate very long single-line output
            parts.append(f"```\n{output[:3000]}\n```\n\n*... truncated ({len(output)} chars)*")
        else:
            parts.append(f"```\n{output}\n```")

    # Embed any captured figures
    for fig in result.captured_figures:
        try:
            img_md = format_plot_as_base64(fig)
            parts.append(img_md)
        except Exception:
            parts.append("*(Chart could not be rendered)*")

    if len(parts) == 1:
        parts.append("*(No output)*")

    return "\n\n".join(parts)

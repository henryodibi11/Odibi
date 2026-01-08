"""Output formatters for human-readable display in the agent UI.

Provides clean formatting for:
- DataFrames (markdown tables with truncation)
- SQL query results
- JSON/dicts (pretty print with collapsible sections)
- File contents (syntax highlighting)
- Schema info (clean tables)
- Plots/charts (base64 embedded images)
"""

from __future__ import annotations

import base64
import io
import json
from pathlib import Path
from typing import Any, Optional

# Language mappings for syntax highlighting
EXTENSION_TO_LANGUAGE = {
    ".py": "python",
    ".js": "javascript",
    ".ts": "typescript",
    ".jsx": "javascript",
    ".tsx": "typescript",
    ".json": "json",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".md": "markdown",
    ".sql": "sql",
    ".sh": "bash",
    ".bash": "bash",
    ".zsh": "zsh",
    ".ps1": "powershell",
    ".html": "html",
    ".css": "css",
    ".scss": "scss",
    ".less": "less",
    ".xml": "xml",
    ".toml": "toml",
    ".ini": "ini",
    ".cfg": "ini",
    ".rs": "rust",
    ".go": "go",
    ".java": "java",
    ".scala": "scala",
    ".rb": "ruby",
    ".php": "php",
    ".c": "c",
    ".cpp": "cpp",
    ".h": "c",
    ".hpp": "cpp",
    ".cs": "csharp",
    ".r": "r",
    ".R": "r",
    ".swift": "swift",
    ".kt": "kotlin",
    ".vue": "vue",
    ".svelte": "svelte",
    ".dockerfile": "dockerfile",
    ".tf": "hcl",
    ".hcl": "hcl",
    ".lua": "lua",
    ".pl": "perl",
    ".ex": "elixir",
    ".exs": "elixir",
    ".erl": "erlang",
    ".clj": "clojure",
    ".hs": "haskell",
    ".graphql": "graphql",
    ".gql": "graphql",
}

# Max rows to display by default for DataFrames
DEFAULT_MAX_ROWS = 25
DEFAULT_MAX_COLS = 15
DEFAULT_MAX_COL_WIDTH = 50
DEFAULT_JSON_COLLAPSE_THRESHOLD = 500  # characters


def detect_language(path: str) -> str:
    """Detect language for syntax highlighting from file path.

    Args:
        path: File path or filename.

    Returns:
        Language identifier for code blocks.
    """
    ext = Path(path).suffix.lower()
    return EXTENSION_TO_LANGUAGE.get(ext, "text")


def format_dataframe(
    df: Any,
    max_rows: int = DEFAULT_MAX_ROWS,
    max_cols: int = DEFAULT_MAX_COLS,
    max_col_width: int = DEFAULT_MAX_COL_WIDTH,
    show_dtypes: bool = False,
    title: Optional[str] = None,
) -> str:
    """Format a pandas or polars DataFrame as a clean markdown table.

    Args:
        df: DataFrame (pandas or polars).
        max_rows: Maximum rows to display.
        max_cols: Maximum columns to display.
        max_col_width: Maximum width per column.
        show_dtypes: Whether to show column types.
        title: Optional title for the table.

    Returns:
        Markdown-formatted table string.
    """
    try:
        import pandas as pd
    except ImportError:
        return f"```\n{str(df)}\n```"

    # Convert polars to pandas if needed
    if hasattr(df, "to_pandas"):
        df = df.to_pandas()

    if not isinstance(df, pd.DataFrame):
        return f"```\n{str(df)}\n```"

    total_rows, total_cols = df.shape
    truncated_rows = total_rows > max_rows
    truncated_cols = total_cols > max_cols

    # Slice the DataFrame
    display_df = df.head(max_rows)
    if truncated_cols:
        display_df = display_df.iloc[:, :max_cols]

    # Truncate long values
    def truncate_value(val: Any) -> str:
        s = str(val) if val is not None else ""
        if len(s) > max_col_width:
            return s[: max_col_width - 3] + "..."
        return s

    display_df = display_df.map(truncate_value)

    # Build output
    parts = []

    if title:
        parts.append(f"**{title}**\n")

    # Shape info
    shape_info = f"📊 **{total_rows:,} rows × {total_cols} columns**"
    if truncated_rows or truncated_cols:
        shape_info += f" (showing {min(max_rows, total_rows)} × {min(max_cols, total_cols)})"
    parts.append(shape_info)

    # Column types if requested
    if show_dtypes:
        types_info = " | ".join(
            f"`{col}`: {dtype}" for col, dtype in list(df.dtypes.items())[:max_cols]
        )
        parts.append(f"*Types:* {types_info}")

    # The table
    try:
        table = display_df.to_markdown(index=False)
        parts.append(table)
    except Exception:
        # Fallback to string representation
        parts.append(f"```\n{display_df.to_string()}\n```")

    # Truncation notice
    if truncated_rows:
        remaining = total_rows - max_rows
        parts.append(f"\n*... {remaining:,} more rows*")

    if truncated_cols:
        remaining_cols = total_cols - max_cols
        hidden_cols = list(df.columns[max_cols : max_cols + 5])
        parts.append(f"*... {remaining_cols} more columns: {', '.join(hidden_cols)}...*")

    return "\n\n".join(parts)


def format_sql_result(
    df: Any,
    query: str = "",
    execution_time: float = 0.0,
    max_rows: int = DEFAULT_MAX_ROWS,
    show_schema: bool = False,
) -> str:
    """Format SQL query results with query info and clean table.

    Args:
        df: Result DataFrame.
        query: The SQL query that was executed.
        execution_time: Query execution time in seconds.
        max_rows: Maximum rows to display.
        show_schema: Whether to show column types.

    Returns:
        Formatted markdown string.
    """
    parts = []

    # Query header
    if query:
        query_display = query.strip()
        if len(query_display) > 200:
            query_display = query_display[:200] + "..."
        parts.append(f"```sql\n{query_display}\n```")

    # Execution info
    time_str = f" in {execution_time:.2f}s" if execution_time > 0.01 else ""
    parts.append(f"✅ **Query executed**{time_str}")

    # Results table
    parts.append(format_dataframe(df, max_rows=max_rows, show_dtypes=show_schema))

    return "\n\n".join(parts)


def format_schema(
    columns: list[dict],
    table_name: str = "",
    sample_values: Optional[dict] = None,
) -> str:
    """Format schema information as a clean table.

    Args:
        columns: List of column dicts with 'name', 'type', 'nullable' keys.
        table_name: Optional table name.
        sample_values: Optional dict mapping column names to sample values.

    Returns:
        Markdown-formatted schema table.
    """
    parts = []

    if table_name:
        parts.append(f"## 📋 Schema: `{table_name}`")

    # Build table header
    has_samples = sample_values is not None
    if has_samples:
        header = "| Column | Type | Nullable | Sample |\n|--------|------|----------|--------|"
    else:
        header = "| Column | Type | Nullable |\n|--------|------|----------|"
    parts.append(header)

    # Build rows
    for col in columns:
        name = col.get("name", "")
        dtype = col.get("type", "")
        nullable = "✓" if col.get("nullable", True) else "✗"

        if has_samples:
            sample = str(sample_values.get(name, ""))[:30]
            if len(str(sample_values.get(name, ""))) > 30:
                sample += "..."
            parts.append(f"| `{name}` | `{dtype}` | {nullable} | {sample} |")
        else:
            parts.append(f"| `{name}` | `{dtype}` | {nullable} |")

    return "\n".join(parts)


def format_spark_schema(schema_tree: str, table_name: str = "") -> str:
    """Format Spark schema tree string as clean markdown.

    Args:
        schema_tree: Schema tree string from df._jdf.schema().treeString().
        table_name: Optional table name.

    Returns:
        Formatted markdown.
    """
    parts = []

    if table_name:
        parts.append(f"## 📋 Schema: `{table_name}`")

    # Parse the tree string into a table
    lines = schema_tree.strip().split("\n")
    if len(lines) > 1:
        parts.append("| Column | Type |")
        parts.append("|--------|------|")

        for line in lines[1:]:  # Skip the "root" line
            # Parse " |-- name: type (nullable = true)"
            line = line.strip()
            if line.startswith("|--"):
                line = line[3:].strip()
            elif line.startswith("|"):
                line = line[1:].strip()

            if ":" in line:
                name, rest = line.split(":", 1)
                dtype = rest.split("(")[0].strip()
                parts.append(f"| `{name.strip()}` | `{dtype}` |")
    else:
        # Fallback to code block
        parts.append(f"```\n{schema_tree}\n```")

    return "\n".join(parts)


def format_json(
    data: Any,
    title: str = "",
    collapse_threshold: int = DEFAULT_JSON_COLLAPSE_THRESHOLD,
    indent: int = 2,
) -> str:
    """Format JSON/dict with pretty printing and optional collapsing.

    Args:
        data: Dict, list, or JSON-serializable data.
        title: Optional title.
        collapse_threshold: Collapse if output exceeds this many characters.
        indent: JSON indentation level.

    Returns:
        Markdown with collapsible JSON if large.
    """
    try:
        formatted = json.dumps(data, indent=indent, default=str, ensure_ascii=False)
    except (TypeError, ValueError):
        formatted = str(data)

    parts = []

    if title:
        parts.append(f"**{title}**\n")

    if len(formatted) > collapse_threshold:
        # Count items for summary
        if isinstance(data, dict):
            summary = f"{len(data)} keys"
        elif isinstance(data, list):
            summary = f"{len(data)} items"
        else:
            summary = f"{len(formatted)} chars"

        parts.append(
            f"<details>\n"
            f"<summary>📦 JSON ({summary})</summary>\n\n"
            f"```json\n{formatted}\n```\n\n"
            f"</details>"
        )
    else:
        parts.append(f"```json\n{formatted}\n```")

    return "\n".join(parts)


def format_dict(data: dict, title: str = "") -> str:
    """Format a dictionary as a readable key-value list.

    Args:
        data: Dictionary to format.
        title: Optional title.

    Returns:
        Markdown formatted key-value pairs.
    """
    if not data:
        return "*Empty*"

    parts = []
    if title:
        parts.append(f"**{title}**\n")

    for key, value in data.items():
        if isinstance(value, (dict, list)) and len(str(value)) > 100:
            value_str = format_json(value, collapse_threshold=100)
        else:
            value_str = f"`{value}`" if isinstance(value, (str, int, float)) else str(value)
        parts.append(f"- **{key}:** {value_str}")

    return "\n".join(parts)


def format_code(
    code: str,
    language: str = "python",
    title: str = "",
    line_numbers: bool = False,
    max_lines: int = 100,
) -> str:
    """Format code with syntax highlighting and optional truncation.

    Args:
        code: Code string.
        language: Language for syntax highlighting.
        title: Optional title.
        line_numbers: Whether to add line numbers.
        max_lines: Maximum lines before truncation.

    Returns:
        Markdown code block.
    """
    parts = []

    if title:
        parts.append(f"**{title}**\n")

    lines = code.split("\n")
    total_lines = len(lines)
    truncated = total_lines > max_lines

    if truncated:
        lines = lines[:max_lines]

    if line_numbers:
        width = len(str(len(lines)))
        lines = [f"{i + 1:>{width}} | {line}" for i, line in enumerate(lines)]

    code_content = "\n".join(lines)
    parts.append(f"```{language}\n{code_content}\n```")

    if truncated:
        remaining = total_lines - max_lines
        parts.append(f"*... {remaining} more lines*")

    return "\n".join(parts)


def format_file_content(
    content: str,
    path: str,
    total_lines: int = 0,
    start_line: int = 1,
    end_line: Optional[int] = None,
) -> str:
    """Format file content with syntax highlighting and metadata.

    Args:
        content: File content.
        path: File path (for language detection).
        total_lines: Total lines in file.
        start_line: Starting line number shown.
        end_line: Ending line number shown.

    Returns:
        Formatted markdown with file info.
    """
    language = detect_language(path)
    filename = Path(path).name

    lines = content.split("\n")
    shown_lines = len(lines)
    actual_end = end_line or (start_line + shown_lines - 1)

    # Build header
    header_parts = [f"📄 **{filename}**"]

    if total_lines > 0:
        if shown_lines < total_lines:
            header_parts.append(f"Lines {start_line}-{actual_end} of {total_lines}")
        else:
            header_parts.append(f"{total_lines} lines")

    header = " | ".join(header_parts)

    # Add line numbers to content
    numbered_lines = []
    for i, line in enumerate(lines):
        line_num = start_line + i
        numbered_lines.append(f"{line_num:4} | {line}")

    numbered_content = "\n".join(numbered_lines)

    return f"{header}\n\n```{language}\n{numbered_content}\n```"


def format_plot_as_base64(fig: Any, format: str = "png", dpi: int = 100) -> str:
    """Convert a matplotlib or plotly figure to embedded base64 image.

    Args:
        fig: Matplotlib Figure or Plotly Figure.
        format: Image format (png, svg, jpg).
        dpi: Resolution for matplotlib.

    Returns:
        Markdown image tag with base64 data.
    """
    try:
        # Try matplotlib first
        import matplotlib.pyplot as plt

        if hasattr(fig, "savefig"):
            buffer = io.BytesIO()
            fig.savefig(buffer, format=format, dpi=dpi, bbox_inches="tight")
            plt.close(fig)
            buffer.seek(0)
            img_data = base64.b64encode(buffer.read()).decode("utf-8")
            return f"![Chart](data:image/{format};base64,{img_data})"
    except ImportError:
        pass

    try:
        # Try plotly
        if hasattr(fig, "to_image"):
            img_bytes = fig.to_image(format=format)
            img_data = base64.b64encode(img_bytes).decode("utf-8")
            return f"![Chart](data:image/{format};base64,{img_data})"
    except (ImportError, Exception):
        pass

    return "*(Chart could not be rendered)*"


def format_list(
    items: list,
    title: str = "",
    numbered: bool = False,
    max_items: int = 50,
) -> str:
    """Format a list as markdown bullet or numbered list.

    Args:
        items: List of items.
        title: Optional title.
        numbered: Use numbered list instead of bullets.
        max_items: Maximum items to show.

    Returns:
        Markdown list.
    """
    if not items:
        return "*Empty list*"

    parts = []
    if title:
        parts.append(f"**{title}** ({len(items)} items)\n")

    truncated = len(items) > max_items
    display_items = items[:max_items]

    for i, item in enumerate(display_items):
        if numbered:
            parts.append(f"{i + 1}. {item}")
        else:
            parts.append(f"- {item}")

    if truncated:
        remaining = len(items) - max_items
        parts.append(f"\n*... and {remaining} more items*")

    return "\n".join(parts)


def format_execution_output(
    output: str,
    success: bool = True,
    execution_time: float = 0.0,
    title: str = "Output",
    error: Optional[str] = None,
    max_lines: int = 50,
) -> str:
    """Format code execution output with status and optional truncation.

    Args:
        output: Output text.
        success: Whether execution succeeded.
        execution_time: Execution time in seconds.
        title: Output title.
        error: Error message if failed.
        max_lines: Maximum lines before collapsing.

    Returns:
        Formatted markdown.
    """
    parts = []

    # Status header
    if success:
        status = "✅"
        time_str = f" ({execution_time:.2f}s)" if execution_time > 0.1 else ""
        parts.append(f"**{status} {title}**{time_str}")
    else:
        parts.append(f"**❌ {title} Failed**")
        if error:
            parts.append(f"\n```\n{error}\n```")
        return "\n".join(parts)

    if not output or output == "(No output)":
        parts.append("*(No output)*")
        return "\n".join(parts)

    lines = output.strip().split("\n")
    total_lines = len(lines)

    if total_lines > max_lines:
        # Collapse large output
        parts.append(
            f"<details>\n"
            f"<summary>📄 Output ({total_lines} lines)</summary>\n\n"
            f"```\n{output}\n```\n\n"
            f"</details>"
        )
    else:
        parts.append(f"```\n{output}\n```")

    return "\n".join(parts)


def auto_format(value: Any, title: str = "") -> str:
    """Automatically detect type and format appropriately.

    Args:
        value: Any value to format.
        title: Optional title.

    Returns:
        Formatted markdown string.
    """
    # Try pandas DataFrame
    try:
        import pandas as pd

        if isinstance(value, pd.DataFrame):
            return format_dataframe(value, title=title)
    except ImportError:
        pass

    # Try polars DataFrame
    try:
        import polars as pl

        if isinstance(value, pl.DataFrame):
            return format_dataframe(value, title=title)
    except ImportError:
        pass

    # Matplotlib figure
    try:
        from matplotlib.figure import Figure

        if isinstance(value, Figure):
            return format_plot_as_base64(value)
    except ImportError:
        pass

    # Plotly figure
    try:
        import plotly.graph_objects as go

        if isinstance(value, go.Figure):
            return format_plot_as_base64(value)
    except ImportError:
        pass

    # Dict
    if isinstance(value, dict):
        if len(str(value)) > 200:
            return format_json(value, title=title)
        return format_dict(value, title=title)

    # List
    if isinstance(value, list):
        if all(isinstance(item, dict) for item in value):
            return format_json(value, title=title)
        return format_list(value, title=title)

    # String that looks like JSON
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, (dict, list)):
                return format_json(parsed, title=title)
        except (json.JSONDecodeError, TypeError):
            pass

        # Multi-line string as code block
        if "\n" in value and len(value) > 100:
            return f"```\n{value}\n```"

    # Fallback
    return str(value)

"""Rich console utilities for polished terminal/notebook output.

This module provides Rich-based console output with graceful fallback
when Rich is not installed. Works in both CLI and Jupyter/Databricks notebooks.
"""

from typing import Any, Dict, List, Optional, Union

_RICH_AVAILABLE: Optional[bool] = None


def is_rich_available() -> bool:
    """Check if Rich library is available.

    Returns:
        True if Rich is installed and importable, False otherwise.
    """
    global _RICH_AVAILABLE
    if _RICH_AVAILABLE is None:
        try:
            import importlib.util

            _RICH_AVAILABLE = importlib.util.find_spec("rich") is not None
        except (ImportError, ModuleNotFoundError):
            _RICH_AVAILABLE = False
    return _RICH_AVAILABLE


def get_console() -> Optional[Any]:
    """Get a Rich Console instance.

    Returns:
        Rich Console instance if available, None otherwise.
    """
    if not is_rich_available():
        return None

    from rich.console import Console

    return Console()


def _is_notebook_environment() -> bool:
    """Detect if running in Jupyter/Databricks notebook."""
    try:
        from IPython import get_ipython

        shell = get_ipython()
        if shell is None:
            return False
        shell_class = shell.__class__.__name__
        return shell_class in ("ZMQInteractiveShell", "DatabricksShell", "Shell")
    except (ImportError, NameError):
        return False


def success(message: str, prefix: str = "✓") -> None:
    """Print a success message in green.

    Args:
        message: The message to display.
        prefix: Prefix symbol (default: ✓).
    """
    if is_rich_available():
        console = get_console()
        console.print(f"[green]{prefix}[/green] {message}")
    else:
        print(f"{prefix} {message}")


def error(message: str, prefix: str = "✗") -> None:
    """Print an error message in red.

    Args:
        message: The message to display.
        prefix: Prefix symbol (default: ✗).
    """
    if is_rich_available():
        console = get_console()
        console.print(f"[red]{prefix}[/red] {message}")
    else:
        print(f"{prefix} {message}")


def warning(message: str, prefix: str = "⚠") -> None:
    """Print a warning message in yellow.

    Args:
        message: The message to display.
        prefix: Prefix symbol (default: ⚠).
    """
    if is_rich_available():
        console = get_console()
        console.print(f"[yellow]{prefix}[/yellow] {message}")
    else:
        print(f"{prefix} {message}")


def info(message: str, prefix: str = "ℹ") -> None:
    """Print an info message in blue.

    Args:
        message: The message to display.
        prefix: Prefix symbol (default: ℹ).
    """
    if is_rich_available():
        console = get_console()
        console.print(f"[blue]{prefix}[/blue] {message}")
    else:
        print(f"{prefix} {message}")


def print_table(
    data: Union[List[Dict[str, Any]], Any],
    title: Optional[str] = None,
    columns: Optional[List[str]] = None,
) -> None:
    """Render data as a Rich table or plain text fallback.

    Args:
        data: List of dicts or a DataFrame to display.
        title: Optional table title.
        columns: Optional list of column names to include.
    """
    if not data:
        print("(empty)")
        return

    rows: List[Dict[str, Any]] = []
    if hasattr(data, "to_dict"):
        rows = data.to_dict("records")
    elif isinstance(data, list):
        rows = data
    else:
        print(str(data))
        return

    if not rows:
        print("(empty)")
        return

    col_names = columns or list(rows[0].keys())

    if is_rich_available():
        from rich.table import Table

        console = get_console()
        table = Table(title=title, show_header=True, header_style="bold cyan")

        for col in col_names:
            table.add_column(col)

        for row in rows:
            table.add_row(*[str(row.get(col, "")) for col in col_names])

        console.print(table)
    else:
        if title:
            print(f"\n{title}")
            print("-" * len(title))

        max_widths = {col: len(col) for col in col_names}
        for row in rows:
            for col in col_names:
                max_widths[col] = max(max_widths[col], len(str(row.get(col, ""))))

        header = " | ".join(col.ljust(max_widths[col]) for col in col_names)
        print(header)
        print("-" * len(header))

        for row in rows:
            line = " | ".join(str(row.get(col, "")).ljust(max_widths[col]) for col in col_names)
            print(line)


def print_panel(
    content: str,
    title: Optional[str] = None,
    border_style: str = "blue",
    padding: tuple = (0, 1),
) -> None:
    """Display content in a boxed panel.

    Args:
        content: The content to display.
        title: Optional panel title.
        border_style: Border color/style (default: blue).
        padding: Tuple of (vertical, horizontal) padding.
    """
    if is_rich_available():
        from rich.panel import Panel

        console = get_console()
        panel = Panel(content, title=title, border_style=border_style, padding=padding)
        console.print(panel)
    else:
        width = max(len(line) for line in content.split("\n")) + 4
        if title:
            width = max(width, len(title) + 6)

        border = "─" * width
        print(f"┌{border}┐")
        if title:
            centered_title = f" {title} ".center(width)
            print(f"│{centered_title}│")
            print(f"├{border}┤")

        for line in content.split("\n"):
            padded = f"  {line}  ".ljust(width)
            print(f"│{padded}│")

        print(f"└{border}┘")


def print_rule(title: Optional[str] = None, style: str = "blue") -> None:
    """Print a horizontal rule/divider.

    Args:
        title: Optional centered title in the rule.
        style: Line color/style.
    """
    if is_rich_available():
        from rich.rule import Rule

        console = get_console()
        console.print(Rule(title, style=style))
    else:
        if title:
            print(f"\n{'─' * 10} {title} {'─' * 10}\n")
        else:
            print("─" * 40)

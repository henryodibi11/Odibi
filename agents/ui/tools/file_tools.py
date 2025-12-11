"""File system tools for agents.

Read, write, and list files from the codebase.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class FileResult:
    """Result of a file operation."""

    success: bool
    content: str
    path: str
    error: Optional[str] = None
    line_count: int = 0


def read_file(
    path: str,
    start_line: int = 1,
    end_line: Optional[int] = None,
    max_lines: int = 500,
) -> FileResult:
    """Read a file from the filesystem.

    Args:
        path: Absolute or relative path to the file.
        start_line: Starting line number (1-indexed).
        end_line: Ending line number (inclusive).
        max_lines: Maximum lines to return.

    Returns:
        FileResult with content or error.
    """
    try:
        file_path = Path(path)
        if not file_path.exists():
            return FileResult(
                success=False,
                content="",
                path=str(path),
                error=f"File not found: {path}",
            )

        if not file_path.is_file():
            return FileResult(
                success=False,
                content="",
                path=str(path),
                error=f"Not a file: {path}",
            )

        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()

        total_lines = len(lines)
        start_idx = max(0, start_line - 1)
        end_idx = min(total_lines, end_line or (start_idx + max_lines))

        selected_lines = lines[start_idx:end_idx]
        numbered_content = "".join(
            f"{i + start_idx + 1}: {line}" for i, line in enumerate(selected_lines)
        )

        return FileResult(
            success=True,
            content=numbered_content,
            path=str(file_path.absolute()),
            line_count=total_lines,
        )

    except Exception as e:
        return FileResult(
            success=False,
            content="",
            path=str(path),
            error=str(e),
        )


def write_file(
    path: str,
    content: str,
    create_dirs: bool = True,
) -> FileResult:
    """Write content to a file.

    Args:
        path: Path to write to.
        content: Content to write.
        create_dirs: Create parent directories if needed.

    Returns:
        FileResult indicating success/failure.
    """
    try:
        file_path = Path(path)

        if create_dirs:
            file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

        return FileResult(
            success=True,
            content=f"Successfully wrote {len(content)} bytes to {path}",
            path=str(file_path.absolute()),
            line_count=content.count("\n") + 1,
        )

    except Exception as e:
        return FileResult(
            success=False,
            content="",
            path=str(path),
            error=str(e),
        )


def list_directory(
    path: str,
    pattern: str = "*",
    recursive: bool = False,
    max_items: int = 100,
) -> FileResult:
    """List contents of a directory.

    Args:
        path: Directory path.
        pattern: Glob pattern to filter.
        recursive: Whether to recurse into subdirectories.
        max_items: Maximum items to return.

    Returns:
        FileResult with directory listing.
    """
    try:
        dir_path = Path(path)
        if not dir_path.exists():
            return FileResult(
                success=False,
                content="",
                path=str(path),
                error=f"Directory not found: {path}",
            )

        if not dir_path.is_dir():
            return FileResult(
                success=False,
                content="",
                path=str(path),
                error=f"Not a directory: {path}",
            )

        if recursive:
            items = list(dir_path.rglob(pattern))[:max_items]
        else:
            items = list(dir_path.glob(pattern))[:max_items]

        lines = []
        for item in sorted(items):
            rel_path = item.relative_to(dir_path)
            suffix = "/" if item.is_dir() else ""
            lines.append(f"{rel_path}{suffix}")

        return FileResult(
            success=True,
            content="\n".join(lines),
            path=str(dir_path.absolute()),
            line_count=len(lines),
        )

    except Exception as e:
        return FileResult(
            success=False,
            content="",
            path=str(path),
            error=str(e),
        )


def format_file_for_display(result: FileResult, language: str = "python") -> str:
    """Format a file result for display in chat.

    Args:
        result: The file result to format.
        language: Language for syntax highlighting.

    Returns:
        Markdown-formatted file content.
    """
    if not result.success:
        return f"**Error:** {result.error}"

    extension = Path(result.path).suffix.lstrip(".")
    lang = extension if extension else language

    return f"""**File:** `{result.path}`
**Lines:** {result.line_count}

```{lang}
{result.content}
```"""

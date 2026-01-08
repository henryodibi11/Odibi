"""File system tools for agents.

Read, write, and list files from the codebase.
Supports both local filesystem and Databricks (DBFS/Workspace).
"""

from __future__ import annotations

import os
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


def is_databricks() -> bool:
    """Check if running in a Databricks environment."""
    return (
        "DATABRICKS_RUNTIME_VERSION" in os.environ
        or "SPARK_HOME" in os.environ
        or os.path.exists("/databricks")
    )


def normalize_databricks_path(path: str) -> str:
    """Normalize a path for Databricks file operations.

    Converts various path formats to the correct format for file I/O:
    - dbfs:/path -> /dbfs/path (for local file API)
    - /dbfs/path -> /dbfs/path (already correct)
    - Workspace paths stay as-is

    Args:
        path: The input path.

    Returns:
        Normalized path for file operations.
    """
    path = str(path)

    if path.startswith("dbfs:/"):
        return "/dbfs" + path[5:]

    return path


def get_dbfs_display_path(path: str) -> str:
    """Get the display-friendly path for Databricks.

    Args:
        path: The normalized path.

    Returns:
        User-friendly display path.
    """
    if path.startswith("/dbfs/"):
        return "dbfs:" + path[5:]
    return path


def suggest_databricks_path(path: str) -> str:
    """Suggest a valid Databricks path if the given path might not work.

    Args:
        path: The attempted path.

    Returns:
        Suggestion message or empty string.
    """
    if not is_databricks():
        return ""

    if path.startswith("/dbfs/") or path.startswith("dbfs:/"):
        return ""

    if path.startswith("/Workspace/"):
        return ""

    suggestions = [
        f"  - DBFS: /dbfs/FileStore/{Path(path).name}",
        f"  - Workspace: /Workspace/Users/your-email/{Path(path).name}",
    ]

    return (
        "\n\n**Databricks path suggestions:**\n"
        + "\n".join(suggestions)
        + "\n\nNote: Local paths in Databricks are ephemeral."
    )


def read_file(
    path: str,
    start_line: int = 1,
    end_line: Optional[int] = None,
    max_lines: Optional[int] = None,
    read_all: bool = True,
) -> FileResult:
    """Read a file from the filesystem.

    Supports Databricks DBFS and Workspace paths.

    Args:
        path: Absolute or relative path to the file.
              For Databricks: use /dbfs/... or dbfs:/... for DBFS,
              /Workspace/... for workspace files.
        start_line: Starting line number (1-indexed).
        end_line: Ending line number (inclusive).
        max_lines: Maximum lines to return (only used if read_all=False).
        read_all: If True (default), read entire file. If False, use max_lines limit.

    Returns:
        FileResult with content or error.
    """
    try:
        normalized_path = normalize_databricks_path(path)
        file_path = Path(normalized_path)

        if not file_path.exists():
            suggestion = suggest_databricks_path(path)
            return FileResult(
                success=False,
                content="",
                path=str(path),
                error=f"File not found: {path}{suggestion}",
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

        # Determine end index based on read_all flag
        if end_line is not None:
            end_idx = min(total_lines, end_line)
        elif read_all:
            end_idx = total_lines
        else:
            effective_max = max_lines or 500
            end_idx = min(total_lines, start_idx + effective_max)

        selected_lines = lines[start_idx:end_idx]
        lines_read = len(selected_lines)

        # Add header with verification info
        header = f"📄 {path} | {lines_read} of {total_lines} lines"
        if lines_read == total_lines:
            header += " | ✅ COMPLETE"
        else:
            header += f" | ⚠️ PARTIAL (lines {start_idx + 1}-{end_idx})"
        header += "\n" + "─" * 60 + "\n"

        numbered_content = header + "".join(
            f"{i + start_idx + 1}: {line}" for i, line in enumerate(selected_lines)
        )

        # Add truncation warning if file was cut off
        if end_idx < total_lines and not read_all:
            remaining = total_lines - end_idx
            numbered_content += (
                f"\n\n⚠️ TRUNCATED: Lines {start_idx + 1}-{end_idx} of {total_lines}. "
                f"{remaining} more lines. Use read_all=True or specify end_line."
            )

        display_path = get_dbfs_display_path(str(file_path.absolute()))

        return FileResult(
            success=True,
            content=numbered_content,
            path=display_path,
            line_count=total_lines,
        )

    except Exception as e:
        return FileResult(
            success=False,
            content="",
            path=str(path),
            error=str(e),
        )


@dataclass
class WriteResult(FileResult):
    """Extended result for write operations with diff support."""

    diff: str = ""
    old_content: str = ""
    is_new_file: bool = False


def write_file(
    path: str,
    content: str,
    create_dirs: bool = True,
    generate_diff: bool = True,
) -> WriteResult:
    """Write content to a file with diff preview.

    Supports Databricks DBFS and Workspace paths.

    Args:
        path: Path to write to.
              For Databricks: use /dbfs/... or dbfs:/... for DBFS,
              /Workspace/... for workspace files.
        content: Content to write.
        create_dirs: Create parent directories if needed.
        generate_diff: Whether to generate a diff of changes.

    Returns:
        WriteResult indicating success/failure with optional diff.
    """
    try:
        normalized_path = normalize_databricks_path(path)
        file_path = Path(normalized_path)

        if is_databricks():
            is_valid_dbfs = normalized_path.startswith("/dbfs/")
            is_valid_workspace = normalized_path.startswith("/Workspace/")

            if not is_valid_dbfs and not is_valid_workspace:
                suggestion = suggest_databricks_path(path)
                return WriteResult(
                    success=False,
                    content="",
                    path=str(path),
                    error=(
                        f"Invalid Databricks path: {path}\n"
                        f"Local paths are ephemeral in Databricks.{suggestion}"
                    ),
                )

        if create_dirs:
            file_path.parent.mkdir(parents=True, exist_ok=True)

        old_content = ""
        is_new_file = not file_path.exists()
        diff = ""

        if file_path.exists():
            try:
                old_content = file_path.read_text(encoding="utf-8")
                FileHistory.save_state(str(file_path), old_content)

                if generate_diff and old_content != content:
                    diff = _generate_diff(old_content, content, file_path.name)
            except Exception:
                pass

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

        # Verify write by reading back
        try:
            written_content = file_path.read_text(encoding="utf-8")
            if written_content != content:
                return WriteResult(
                    success=False,
                    content="",
                    path=str(path),
                    error=(
                        f"Write verification failed: wrote {len(content)} bytes "
                        f"but file contains {len(written_content)} bytes"
                    ),
                )
            verified_lines = written_content.count("\n") + 1
        except Exception as e:
            return WriteResult(
                success=False,
                content="",
                path=str(path),
                error=f"Write verification failed: {e}",
            )

        display_path = get_dbfs_display_path(str(file_path.absolute()))

        status = "Created" if is_new_file else "Updated"
        message = (
            f"✅ {status} {display_path} ({len(content)} bytes, {verified_lines} lines) - VERIFIED"
        )

        return WriteResult(
            success=True,
            content=message,
            path=display_path,
            line_count=verified_lines,
            diff=diff,
            old_content=old_content,
            is_new_file=is_new_file,
        )

    except PermissionError:
        suggestion = suggest_databricks_path(path)
        return WriteResult(
            success=False,
            content="",
            path=str(path),
            error=f"Permission denied: {path}{suggestion}",
        )

    except Exception as e:
        suggestion = suggest_databricks_path(path) if is_databricks() else ""
        return WriteResult(
            success=False,
            content="",
            path=str(path),
            error=f"{str(e)}{suggestion}",
        )


def _generate_diff(old_content: str, new_content: str, filename: str) -> str:
    """Generate a unified diff between old and new content.

    Args:
        old_content: Original file content.
        new_content: New file content.
        filename: Name of the file for diff header.

    Returns:
        Unified diff string.
    """
    import difflib

    old_lines = old_content.splitlines(keepends=True)
    new_lines = new_content.splitlines(keepends=True)

    diff = difflib.unified_diff(
        old_lines,
        new_lines,
        fromfile=f"a/{filename}",
        tofile=f"b/{filename}",
        lineterm="",
    )

    return "".join(diff)


def format_write_result(result: WriteResult, show_diff: bool = True) -> str:
    """Format a write result for display with optional diff.

    Args:
        result: The write result to format.
        show_diff: Whether to include the diff preview.

    Returns:
        Markdown-formatted result.
    """
    if not result.success:
        return f"❌ **Error:** {result.error}"

    output = f"✅ {result.content}"

    if show_diff and result.diff:
        output += f"\n\n<details>\n<summary>📝 View changes</summary>\n\n```diff\n{result.diff}\n```\n\n</details>"
    elif result.is_new_file:
        output += " (new file)"

    return output


def list_directory(
    path: str,
    pattern: str = "*",
    recursive: bool = False,
    max_items: int = 100,
) -> FileResult:
    """List contents of a directory.

    Supports Databricks DBFS and Workspace paths.

    Args:
        path: Directory path.
              For Databricks: use /dbfs/... or dbfs:/... for DBFS,
              /Workspace/... for workspace files.
        pattern: Glob pattern to filter.
        recursive: Whether to recurse into subdirectories.
        max_items: Maximum items to return.

    Returns:
        FileResult with directory listing.
    """
    try:
        normalized_path = normalize_databricks_path(path)
        dir_path = Path(normalized_path)

        if not dir_path.exists():
            suggestion = suggest_databricks_path(path)
            return FileResult(
                success=False,
                content="",
                path=str(path),
                error=f"Directory not found: {path}{suggestion}",
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

        display_path = get_dbfs_display_path(str(dir_path.absolute()))

        return FileResult(
            success=True,
            content="\n".join(lines),
            path=display_path,
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


class FileHistory:
    """Tracks file edit history for undo functionality."""

    _history: dict[str, list[str]] = {}
    _max_history = 10

    @classmethod
    def save_state(cls, path: str, content: str) -> None:
        """Save a file state before editing."""
        normalized = str(Path(path).resolve())
        if normalized not in cls._history:
            cls._history[normalized] = []
        cls._history[normalized].append(content)
        cls._history[normalized] = cls._history[normalized][-cls._max_history :]

    @classmethod
    def get_previous(cls, path: str) -> str | None:
        """Get the previous state of a file."""
        normalized = str(Path(path).resolve())
        history = cls._history.get(normalized, [])
        if len(history) >= 1:
            return history[-1]
        return None

    @classmethod
    def pop_previous(cls, path: str) -> str | None:
        """Pop and return the previous state of a file."""
        normalized = str(Path(path).resolve())
        history = cls._history.get(normalized, [])
        if history:
            return history.pop()
        return None

    @classmethod
    def clear(cls, path: str | None = None) -> None:
        """Clear history for a file or all files."""
        if path:
            normalized = str(Path(path).resolve())
            cls._history.pop(normalized, None)
        else:
            cls._history.clear()


def edit_file(
    path: str,
    old_str: str,
    new_str: str,
    replace_all: bool = False,
) -> WriteResult:
    """Make surgical edits to a file by replacing specific text.

    This is more efficient than write_file for small changes to large files.
    The old_str must be found exactly (including whitespace) in the file.

    Args:
        path: Path to the file to edit.
        old_str: Exact text to find and replace. Must match exactly.
        new_str: Text to replace old_str with.
        replace_all: If True, replace all occurrences. If False, old_str must be unique.

    Returns:
        WriteResult indicating success/failure with diff.
    """
    try:
        normalized_path = normalize_databricks_path(path)
        file_path = Path(normalized_path)

        if not file_path.exists():
            return WriteResult(
                success=False,
                content="",
                path=str(path),
                error=f"File not found: {path}",
            )

        # Read current content
        old_content = file_path.read_text(encoding="utf-8")

        # Check if old_str exists
        if old_str not in old_content:
            # Try to find similar content for helpful error
            lines_with_similar = []
            old_lines = old_str.split("\n")
            if old_lines:
                first_line = old_lines[0].strip()
                for i, line in enumerate(old_content.split("\n"), 1):
                    if first_line and first_line in line:
                        lines_with_similar.append(f"  Line {i}: {line[:80]}...")

            hint = ""
            if lines_with_similar:
                hint = "\n\nSimilar content found at:\n" + "\n".join(lines_with_similar[:3])

            return WriteResult(
                success=False,
                content="",
                path=str(path),
                error=f"old_str not found in file. Make sure it matches exactly including whitespace.{hint}",
            )

        # Check uniqueness if not replace_all
        count = old_content.count(old_str)
        if not replace_all and count > 1:
            return WriteResult(
                success=False,
                content="",
                path=str(path),
                error=f"old_str appears {count} times in file. Add more context to make it unique, or use replace_all=true.",
            )

        # Save history for undo
        FileHistory.save_state(str(file_path), old_content)

        # Perform replacement
        if replace_all:
            new_content = old_content.replace(old_str, new_str)
            replacements = count
        else:
            new_content = old_content.replace(old_str, new_str, 1)
            replacements = 1

        # Generate diff
        diff = _generate_diff(old_content, new_content, file_path.name)

        # Write new content
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(new_content)

        # Verify write
        written_content = file_path.read_text(encoding="utf-8")
        if written_content != new_content:
            # Restore old content
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(old_content)
            return WriteResult(
                success=False,
                content="",
                path=str(path),
                error="Write verification failed, restored original content",
            )

        display_path = get_dbfs_display_path(str(file_path.absolute()))
        verified_lines = new_content.count("\n") + 1

        message = f"✅ Edited {display_path} ({replacements} replacement{'s' if replacements > 1 else ''}, {verified_lines} lines)"

        return WriteResult(
            success=True,
            content=message,
            path=display_path,
            line_count=verified_lines,
            diff=diff,
            old_content=old_content,
            is_new_file=False,
        )

    except Exception as e:
        return WriteResult(
            success=False,
            content="",
            path=str(path),
            error=str(e),
        )


def undo_edit(path: str) -> FileResult:
    """Undo the last edit to a file.

    Args:
        path: Path to the file to restore.

    Returns:
        FileResult indicating success/failure.
    """
    try:
        normalized_path = normalize_databricks_path(path)
        file_path = Path(normalized_path)

        if not file_path.exists():
            return FileResult(
                success=False,
                content="",
                path=str(path),
                error=f"File not found: {path}",
            )

        previous_content = FileHistory.pop_previous(str(file_path))
        if previous_content is None:
            return FileResult(
                success=False,
                content="",
                path=str(path),
                error="No previous version available to restore",
            )

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(previous_content)

        display_path = get_dbfs_display_path(str(file_path.absolute()))

        return FileResult(
            success=True,
            content=f"Restored previous version of {display_path}",
            path=display_path,
            line_count=previous_content.count("\n") + 1,
        )

    except Exception as e:
        return FileResult(
            success=False,
            content="",
            path=str(path),
            error=str(e),
        )


def get_databricks_paths_help() -> str:
    """Get help text for Databricks path usage.

    Returns:
        Markdown help text.
    """
    return """
## Databricks File Paths

When running in Databricks, use these path formats:

### DBFS (Distributed File System)
- **Read/Write:** `/dbfs/FileStore/your-folder/file.txt`
- **Display:** `dbfs:/FileStore/your-folder/file.txt`

### Workspace Files
- **Path:** `/Workspace/Users/your-email@company.com/file.txt`

### Unity Catalog Volumes
- **Path:** `/Volumes/catalog/schema/volume/file.txt`

### Examples
```python
# Write to DBFS
write_file("/dbfs/FileStore/output/results.csv", data)

# Read from Workspace
read_file("/Workspace/Users/me@company.com/config.yaml")

# List DBFS directory
list_directory("/dbfs/FileStore/")
```

**Note:** Local paths like `/tmp/` or relative paths are ephemeral
and will not persist across cluster restarts.
"""

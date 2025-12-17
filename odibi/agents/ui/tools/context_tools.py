"""Context and file linking tools.

Utilities for creating file links and managing attached file context.
"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import quote


@dataclass
class AttachedFile:
    """A file attached to the conversation context."""

    path: str
    content: Optional[str] = None
    language: str = "text"
    line_start: int = 1
    line_end: Optional[int] = None


@dataclass
class ContextState:
    """Tracks attached files and context for the current session."""

    attached_files: list[AttachedFile] = field(default_factory=list)
    active_file: Optional[str] = None
    cursor_line: Optional[int] = None

    def attach(
        self,
        path: str,
        content: Optional[str] = None,
        line_start: int = 1,
        line_end: Optional[int] = None,
    ) -> AttachedFile:
        """Attach a file to context."""
        ext = Path(path).suffix.lstrip(".")
        lang_map = {
            "py": "python",
            "js": "javascript",
            "ts": "typescript",
            "yaml": "yaml",
            "yml": "yaml",
            "json": "json",
            "md": "markdown",
            "sql": "sql",
            "sh": "bash",
        }
        language = lang_map.get(ext, "text")

        attached = AttachedFile(
            path=path,
            content=content,
            language=language,
            line_start=line_start,
            line_end=line_end,
        )
        self.attached_files.append(attached)
        return attached

    def clear(self) -> None:
        """Clear all attached files."""
        self.attached_files = []
        self.active_file = None
        self.cursor_line = None

    def get_context_prompt(self) -> str:
        """Generate context prompt for LLM."""
        if not self.attached_files and not self.active_file:
            return ""

        parts = ["## Attached Context\n"]

        if self.active_file:
            parts.append(f"**Active file:** `{self.active_file}`")
            if self.cursor_line:
                parts.append(f" (cursor at line {self.cursor_line})")
            parts.append("\n")

        for f in self.attached_files:
            parts.append(f"\n### {Path(f.path).name}")
            parts.append(f"\n**Path:** `{f.path}`")
            if f.content:
                parts.append(f"\n```{f.language}\n{f.content}\n```")

        return "".join(parts)


_context_state: Optional[ContextState] = None


def get_context_state() -> ContextState:
    """Get the global context state."""
    global _context_state
    if _context_state is None:
        _context_state = ContextState()
    return _context_state


def create_file_link(
    path: str,
    line: Optional[int] = None,
    line_end: Optional[int] = None,
    label: Optional[str] = None,
) -> str:
    """Create a clickable file:// link.

    Args:
        path: Absolute path to the file.
        line: Optional line number to link to.
        line_end: Optional end line for a range.
        label: Optional display label (defaults to filename).

    Returns:
        Markdown link to the file.
    """
    path_obj = Path(path)

    if not path_obj.is_absolute():
        path_obj = path_obj.resolve()

    encoded_path = quote(str(path_obj), safe="/:\\")

    url = f"file:///{encoded_path.lstrip('/')}"

    if line:
        if line_end and line_end != line:
            url += f"#L{line}-L{line_end}"
        else:
            url += f"#L{line}"

    if label:
        display = label
    else:
        display = path_obj.name
        if line:
            display += f":{line}"
            if line_end and line_end != line:
                display += f"-{line_end}"

    return f"[{display}]({url})"


def linkify_paths(text: str, base_path: Optional[str] = None) -> str:
    """Convert file paths in text to clickable links.

    Args:
        text: Text that may contain file paths.
        base_path: Base path to resolve relative paths against.

    Returns:
        Text with paths converted to markdown links.
    """
    path_pattern = re.compile(
        r"`([A-Za-z]:[/\\][^`\n]+|/[^`\n]+\.[a-zA-Z0-9]+)`"
        r"|"
        r"\b([A-Za-z]:[/\\][^\s\n]+\.[a-zA-Z0-9]+)"
        r"|"
        r"\b(/[^\s\n]+\.[a-zA-Z0-9]+)\b"
    )

    def replace_path(match: re.Match) -> str:
        path = match.group(1) or match.group(2) or match.group(3)
        if not path:
            return match.group(0)

        path_obj = Path(path)
        if not path_obj.is_absolute() and base_path:
            path_obj = Path(base_path) / path_obj

        if path_obj.suffix:
            line_match = re.search(r":(\d+)(?:-(\d+))?$", str(path))
            line = None
            line_end = None
            if line_match:
                line = int(line_match.group(1))
                if line_match.group(2):
                    line_end = int(line_match.group(2))
                path = re.sub(r":\d+(?:-\d+)?$", "", str(path))

            return create_file_link(str(path_obj), line=line, line_end=line_end)

        return match.group(0)

    return path_pattern.sub(replace_path, text)


def format_file_mention(
    path: str,
    content: Optional[str] = None,
    line_start: int = 1,
    max_lines: int = 50,
) -> str:
    """Format a file mention for display in chat.

    Args:
        path: Path to the file.
        content: Optional file content.
        line_start: Starting line number.
        max_lines: Maximum lines to show.

    Returns:
        Markdown formatted file mention.
    """
    path_obj = Path(path)
    link = create_file_link(path, line=line_start if line_start > 1 else None)

    ext = path_obj.suffix.lstrip(".")
    lang_map = {
        "py": "python",
        "js": "javascript",
        "ts": "typescript",
        "yaml": "yaml",
        "yml": "yaml",
        "json": "json",
    }
    lang = lang_map.get(ext, ext or "text")

    parts = [f"📄 {link}"]

    if content:
        lines = content.splitlines()[:max_lines]
        preview = "\n".join(lines)
        if len(content.splitlines()) > max_lines:
            preview += f"\n... ({len(content.splitlines()) - max_lines} more lines)"
        parts.append(f"\n```{lang}\n{preview}\n```")

    return "\n".join(parts)


def attach_file(path: str, read_content: bool = True) -> str:
    """Attach a file to the conversation context.

    Args:
        path: Path to the file.
        read_content: Whether to read the file content.

    Returns:
        Confirmation message.
    """
    context = get_context_state()

    content = None
    if read_content:
        try:
            content = Path(path).read_text(encoding="utf-8")
        except Exception:
            pass

    context.attach(path, content)
    return f"Attached: {Path(path).name}"


def set_active_file(path: str, cursor_line: Optional[int] = None) -> None:
    """Set the currently active/focused file.

    Args:
        path: Path to the active file.
        cursor_line: Current cursor line.
    """
    context = get_context_state()
    context.active_file = path
    context.cursor_line = cursor_line

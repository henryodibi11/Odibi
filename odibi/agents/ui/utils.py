"""UI utilities for formatting, diffs, links, and display helpers."""

import difflib
import os
import re
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from .constants import estimate_cost


def get_odibi_root(config: Optional[object] = None, fallback: str = "d:/odibi") -> str:
    """Get the Odibi root directory from config, environment, or fallback.

    Resolution order:
    1. config.odibi_root (if config has this attribute)
    2. ODIBI_ROOT environment variable
    3. Relative path detection (walk up from current file to find .odibi/)
    4. Fallback value

    Args:
        config: Optional config object with odibi_root attribute.
        fallback: Fallback path if nothing else works.

    Returns:
        Path to the Odibi root directory.
    """
    if config is not None and hasattr(config, "odibi_root") and config.odibi_root:
        return str(config.odibi_root)

    env_root = os.environ.get("ODIBI_ROOT")
    if env_root:
        return env_root

    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / ".odibi").is_dir():
            return str(parent)
        if (parent / "agents").is_dir() and (parent / ".git").exists():
            return str(parent)

    return fallback


def create_file_link(path: str, line: Optional[int] = None, end_line: Optional[int] = None) -> str:
    """Create a clickable file link in markdown.

    Args:
        path: File path.
        line: Optional line number.
        end_line: Optional end line for range.

    Returns:
        Markdown link to the file.
    """
    path_obj = Path(path)
    display_name = path_obj.name

    encoded_path = quote(str(path), safe="/:\\")

    if path.startswith("/"):
        file_url = f"file://{encoded_path}"
    else:
        file_url = f"file:///{encoded_path}"

    if line:
        if end_line:
            file_url += f"#L{line}-L{end_line}"
        else:
            file_url += f"#L{line}"
        display_name += f":{line}"

    return f"[{display_name}]({file_url})"


def create_vscode_link(path: str, line: Optional[int] = None) -> str:
    """Create a VS Code clickable link.

    Args:
        path: File path.
        line: Optional line number.

    Returns:
        VS Code protocol link.
    """
    encoded_path = quote(str(path), safe="/:\\")

    if line:
        return f"vscode://file/{encoded_path}:{line}"
    return f"vscode://file/{encoded_path}"


def linkify_file_paths(text: str, base_path: Optional[str] = None) -> str:
    """Convert file paths in text to clickable links.

    Args:
        text: Text containing file paths.
        base_path: Optional base path for relative paths.

    Returns:
        Text with file paths converted to links.
    """
    extensions = r"py|ts|js|yaml|yml|json|md|txt|toml|cfg|ini"
    path_pattern = (
        rf"(?:^|\s)([a-zA-Z]:[/\\][^\s\n]+\.(?:{extensions})" rf"|/[^\s\n]+\.(?:{extensions}))"
    )

    def replace_path(match):
        prefix = match.group(0)[0] if match.group(0)[0].isspace() else ""
        path = match.group(1)

        line = None
        line_match = re.search(r":(\d+)(?::(\d+))?$", path)
        if line_match:
            line = int(line_match.group(1))
            path = path[: line_match.start()]

        link = create_file_link(path, line)
        return f"{prefix}{link}"

    return re.sub(path_pattern, replace_path, text)


def create_diff(old_content: str, new_content: str, filename: str = "file") -> str:
    """Create a unified diff between two contents.

    Args:
        old_content: Original content.
        new_content: New content.
        filename: Filename for diff header.

    Returns:
        Unified diff as string.
    """
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


def format_diff_html(diff: str) -> str:
    """Format a diff as HTML with syntax highlighting.

    Args:
        diff: Unified diff string.

    Returns:
        HTML formatted diff.
    """
    lines = []
    lines.append('<div class="diff-preview">')

    for line in diff.split("\n"):
        if line.startswith("+++") or line.startswith("---"):
            lines.append(f'<div class="diff-line header">{_escape_html(line)}</div>')
        elif line.startswith("@@"):
            lines.append(f'<div class="diff-line range">{_escape_html(line)}</div>')
        elif line.startswith("+"):
            lines.append(f'<div class="diff-line added">{_escape_html(line)}</div>')
        elif line.startswith("-"):
            lines.append(f'<div class="diff-line removed">{_escape_html(line)}</div>')
        else:
            lines.append(f'<div class="diff-line context">{_escape_html(line)}</div>')

    lines.append("</div>")
    return "\n".join(lines)


def format_diff_markdown(diff: str) -> str:
    """Format a diff as Markdown.

    Args:
        diff: Unified diff string.

    Returns:
        Markdown formatted diff.
    """
    return f"```diff\n{diff}\n```"


def _escape_html(text: str) -> str:
    """Escape HTML special characters."""
    return (
        text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )


def format_token_usage(
    model: str,
    input_tokens: int,
    output_tokens: int,
    show_cost: bool = True,
) -> str:
    """Format token usage for display.

    Args:
        model: Model name.
        input_tokens: Number of input tokens.
        output_tokens: Number of output tokens.
        show_cost: Whether to show estimated cost.

    Returns:
        Formatted token usage string.
    """
    total = input_tokens + output_tokens
    result = f"📊 {input_tokens:,} in / {output_tokens:,} out ({total:,} total)"

    if show_cost:
        cost = estimate_cost(model, input_tokens, output_tokens)
        result += f" | 💰 ${cost:.4f}"

    return result


def format_collapsible(title: str, content: str, collapsed: bool = True) -> str:
    """Create a collapsible section in Markdown (using HTML details).

    Args:
        title: Section title.
        content: Section content.
        collapsed: Whether to start collapsed.

    Returns:
        HTML details element.
    """
    open_attr = "" if collapsed else " open"
    return f"""<details{open_attr}>
<summary>{title}</summary>

{content}

</details>"""


def format_tool_result_collapsible(tool_name: str, emoji: str, result: str) -> str:
    """Format a tool result as a collapsible section.

    Args:
        tool_name: Name of the tool.
        emoji: Emoji for the tool.
        result: Tool result.

    Returns:
        Collapsible formatted result.
    """
    preview = result[:100].replace("\n", " ")
    if len(result) > 100:
        preview += "..."

    title = f"{emoji} **{tool_name}** - {preview}"
    return format_collapsible(title, result, collapsed=True)


def truncate_for_display(text: str, max_length: int = 500, suffix: str = "...") -> str:
    """Truncate text for display.

    Args:
        text: Text to truncate.
        max_length: Maximum length.
        suffix: Suffix to append if truncated.

    Returns:
        Truncated text.
    """
    if len(text) <= max_length:
        return text
    return text[: max_length - len(suffix)] + suffix


class ConversationBranch:
    """Manages conversation branching for trying different approaches."""

    def __init__(self):
        self.branches: dict[str, list[dict]] = {"main": []}
        self.current_branch = "main"
        self._branch_counter = 0

    def get_history(self, branch: Optional[str] = None) -> list[dict]:
        """Get conversation history for a branch."""
        branch = branch or self.current_branch
        return self.branches.get(branch, [])

    def add_message(self, message: dict, branch: Optional[str] = None):
        """Add a message to a branch."""
        branch = branch or self.current_branch
        if branch not in self.branches:
            self.branches[branch] = []
        self.branches[branch].append(message)

    def create_branch(self, from_message_index: int = -1) -> str:
        """Create a new branch from a specific point.

        Args:
            from_message_index: Index to branch from (-1 for current position).

        Returns:
            Name of the new branch.
        """
        self._branch_counter += 1
        new_branch = f"branch_{self._branch_counter}"

        source = self.branches[self.current_branch]
        if from_message_index == -1:
            from_message_index = len(source)

        self.branches[new_branch] = source[:from_message_index].copy()
        return new_branch

    def switch_branch(self, branch: str):
        """Switch to a different branch."""
        if branch in self.branches:
            self.current_branch = branch

    def list_branches(self) -> list[str]:
        """List all branches."""
        return list(self.branches.keys())

    def delete_branch(self, branch: str):
        """Delete a branch."""
        if branch != "main" and branch in self.branches:
            del self.branches[branch]
            if self.current_branch == branch:
                self.current_branch = "main"


def format_elapsed_time(seconds: float) -> str:
    """Format elapsed time for display.

    Args:
        seconds: Elapsed time in seconds.

    Returns:
        Formatted time string.
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"

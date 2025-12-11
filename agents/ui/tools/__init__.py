"""Tool implementations for agent actions.

Provides file operations, code search, shell commands, web search,
diagrams, and context management for the AI agents.
"""

from .context_tools import attach_file, create_file_link, linkify_paths, set_active_file
from .diagram_tools import format_diagram, render_mermaid
from .file_tools import list_directory, read_file, undo_edit, write_file
from .search_tools import glob_files, grep_search, semantic_search
from .shell_tools import run_command, run_diagnostics, run_odibi_pipeline, run_typecheck
from .web_tools import read_web_page, web_search
from .git_tools import git_diff, git_log, git_status, is_git_repo

__all__ = [
    # File operations
    "read_file",
    "write_file",
    "list_directory",
    "undo_edit",
    # Search
    "grep_search",
    "glob_files",
    "semantic_search",
    # Shell
    "run_command",
    "run_odibi_pipeline",
    "run_diagnostics",
    "run_typecheck",
    # Web
    "web_search",
    "read_web_page",
    # Diagrams
    "render_mermaid",
    "format_diagram",
    # Context
    "create_file_link",
    "linkify_paths",
    "attach_file",
    "set_active_file",
    # Git
    "git_status",
    "git_diff",
    "git_log",
    "is_git_repo",
]

"""MCP Server for smart, safe file operations.

Provides file editing, searching, and shell execution tools similar to Amp.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import urllib.request
from pathlib import Path

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

# Edit history for undo functionality: path -> previous content
_edit_history: dict[str, str] = {}

server = Server("odibi-fileops")


def _get_workspace() -> Path:
    """Get workspace root from env or cwd."""
    # Check for workspace env vars (set by Continue config or user)
    for var in ("ODIBI_WORKSPACE", "PYTHONPATH", "WORKSPACE"):
        if val := os.environ.get(var):
            # PYTHONPATH might have multiple paths, take first
            first_path = val.split(os.pathsep)[0]
            p = Path(first_path)
            if p.is_dir():
                return p
    # Fallback: derive from this file's location (odibi_mcp/fileops/server.py -> odibi root)
    server_file = Path(__file__).resolve()
    odibi_root = server_file.parent.parent.parent
    if odibi_root.is_dir():
        return odibi_root
    return Path.cwd()


def _resolve_path(path: str) -> Path:
    """Resolve path: expand ~, env vars, make absolute relative to workspace."""
    expanded = os.path.expanduser(os.path.expandvars(path))
    p = Path(expanded)
    if p.is_absolute():
        return p.resolve()
    # Relative paths resolve from workspace, not cwd
    return (_get_workspace() / p).resolve()


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available file operation tools."""
    return [
        Tool(
            name="read_file",
            description="""READ A FILE - Use this instead of cat/Get-Content/type.

WHEN TO USE: Always use this to read file contents. Returns numbered lines.
DO NOT USE: cat, Get-Content, type, or any shell command to read files.

Example: read_file("src/main.py", 1, 100) → reads lines 1-100""",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute or relative path to file"},
                    "start_line": {
                        "type": "integer",
                        "description": "Start line (1-indexed, default 1)",
                        "default": 1,
                    },
                    "end_line": {
                        "type": "integer",
                        "description": "End line (default 500)",
                        "default": 500,
                    },
                },
                "required": ["path"],
            },
        ),
        Tool(
            name="edit_file",
            description="""EDIT A FILE - Surgical find-and-replace. Use this instead of overwriting files.

WHEN TO USE: Any time you need to modify an existing file.
DO NOT USE: create_file with overwrite=True, or any method that replaces entire file content.

REQUIREMENTS:
- old_str must match EXACTLY (including whitespace/indentation)
- old_str must appear exactly ONCE in the file
- If edit fails, use read_file to get exact text, then retry

Example: edit_file("config.py", "debug = False", "debug = True")""",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path to file to edit"},
                    "old_str": {
                        "type": "string",
                        "description": "EXACT text to find (must match perfectly, including whitespace)",
                    },
                    "new_str": {"type": "string", "description": "Text to replace old_str with"},
                },
                "required": ["path", "old_str", "new_str"],
            },
        ),
        Tool(
            name="create_file",
            description="""CREATE A NEW FILE - Use only for new files, not for editing.

WHEN TO USE: Creating a brand new file that doesn't exist.
DO NOT USE: For modifying existing files - use edit_file instead.

SAFETY: Fails if file exists (unless overwrite=True, but avoid this for edits).

Example: create_file("new_module.py", "def hello():\\n    print('hi')")""",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path for new file"},
                    "content": {"type": "string", "description": "File content"},
                    "overwrite": {
                        "type": "boolean",
                        "description": "Overwrite if exists (default False, avoid for edits)",
                        "default": False,
                    },
                },
                "required": ["path", "content"],
            },
        ),
        # DISABLED: undo_edit
        Tool(
            name="glob_files",
            description="""FIND FILES BY PATTERN - Use instead of Get-ChildItem/find/ls.

WHEN TO USE: Finding files by name pattern.
PATTERNS:
- "**/*.py" → all Python files
- "src/**/*.ts" → TypeScript in src/
- "**/test_*.py" → all test files
- "**/*config*" → files with 'config' in name

Example: glob_files("**/*.yaml", 20) → find up to 20 YAML files""",
            inputSchema={
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Glob pattern (e.g., '**/*.py', 'src/**/*.ts')",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results (default 100)",
                        "default": 100,
                    },
                },
                "required": ["pattern"],
            },
        ),
        Tool(
            name="grep",
            description="""SEARCH FILE CONTENTS - Find text/patterns in files. Use instead of Select-String/grep.

WHEN TO USE: Finding where something is defined, used, or mentioned.
EXAMPLES:
- Find function: grep("def my_function", "src/")
- Find class: grep("class MyClass", ".")
- Find import: grep("import pandas", "**/*.py")

Returns: file:line: matched_text""",
            inputSchema={
                "type": "object",
                "properties": {
                    "pattern": {"type": "string", "description": "Regex pattern to search for"},
                    "path": {
                        "type": "string",
                        "description": "File or directory to search in",
                    },
                    "case_sensitive": {
                        "type": "boolean",
                        "description": "Case sensitive search (default True)",
                        "default": True,
                    },
                },
                "required": ["pattern", "path"],
            },
        ),
        Tool(
            name="run_command",
            description="""EXECUTE SHELL COMMAND - Run any shell command and get output.

WHEN TO USE: Running Python, tests, builds, or any CLI command.
EXAMPLES:
- run_command("python -m pytest tests/")
- run_command("python -m odibi run pipeline.yaml")
- run_command("python -m odibi run pipeline.yaml", timeout=600)  # 10 min for long pipelines
- run_command("pip install package")

Returns stdout and stderr. Default timeout: 300 seconds (5 min). Use timeout parameter for longer operations.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "command": {"type": "string", "description": "Shell command to execute"},
                    "cwd": {
                        "type": "string",
                        "description": "Working directory (optional, defaults to workspace)",
                    },
                    "timeout": {
                        "type": "integer",
                        "description": "Timeout in seconds (default: 300, max: 3600). Use higher values for long-running commands like API pipelines.",
                        "default": 300,
                    },
                },
                "required": ["command"],
            },
        ),
        # DISABLED: find_path (use glob_files instead)
        Tool(
            name="list_dir",
            description="""LIST DIRECTORY - Show contents of a directory. Use instead of ls/dir.

WHEN TO USE: Exploring directory structure, seeing what files exist.

Example: list_dir("src/", True) → lists with file sizes and dates""",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Directory path to list"},
                    "details": {
                        "type": "boolean",
                        "description": "Include size/modified time (default False)",
                        "default": False,
                    },
                },
                "required": ["path"],
            },
        ),
        # DISABLED: create_dir, delete_path, move_path, copy_path, path_exists, resolve_path
        # DISABLED: fetch_url (use playwright instead)
        # DISABLED: git_status, git_diff, git_log (use run_command if needed)
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls."""
    try:
        if name == "read_file":
            return _read_file(
                arguments["path"],
                arguments.get("start_line", 1),
                arguments.get("end_line", 500),
            )
        elif name == "edit_file":
            return _edit_file(
                arguments["path"],
                arguments["old_str"],
                arguments["new_str"],
            )
        elif name == "create_file":
            return _create_file(
                arguments["path"],
                arguments["content"],
                arguments.get("overwrite", False),
            )
        elif name == "undo_edit":
            return _undo_edit(arguments["path"])
        elif name == "glob_files":
            return _glob_files(arguments["pattern"], arguments.get("limit", 100))
        elif name == "grep":
            return _grep(
                arguments["pattern"],
                arguments["path"],
                arguments.get("case_sensitive", True),
            )
        elif name == "run_command":
            return _run_command(arguments["command"], arguments.get("cwd"), arguments.get("timeout", 300))
        elif name == "find_path":
            return _find_path(arguments["name"])
        elif name == "list_dir":
            return _list_dir(arguments["path"], arguments.get("details", False))
        elif name == "create_dir":
            return _create_dir(arguments["path"])
        elif name == "delete_path":
            return _delete_path(arguments["path"])
        elif name == "move_path":
            return _move_path(arguments["src"], arguments["dst"])
        elif name == "copy_path":
            return _copy_path(arguments["src"], arguments["dst"])
        elif name == "path_exists":
            return _path_exists(arguments["path"])
        elif name == "resolve_path":
            return _resolve_path_tool(arguments["path"])
        elif name == "fetch_url":
            return _fetch_url(arguments["url"])
        elif name == "git_status":
            return _git_status(arguments.get("path"))
        elif name == "git_diff":
            return _git_diff(arguments.get("path"))
        elif name == "git_log":
            return _git_log(arguments.get("n", 10), arguments.get("path"))
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error: {e}")]


def _read_file(path: str, start_line: int, end_line: int) -> list[TextContent]:
    """Read file with line numbers."""
    resolved = _resolve_path(path)
    if not resolved.exists():
        return [TextContent(type="text", text=f"File not found: {resolved}")]
    if not resolved.is_file():
        return [TextContent(type="text", text=f"Not a file: {resolved}")]

    try:
        content = resolved.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        content = resolved.read_text(encoding="latin-1")

    lines = content.splitlines()
    start_idx = max(0, start_line - 1)
    end_idx = min(len(lines), end_line)

    numbered = []
    for i, line in enumerate(lines[start_idx:end_idx], start=start_line):
        numbered.append(f"{i}: {line}")

    result = "\n".join(numbered)
    if end_idx < len(lines):
        result += f"\n\n[... {len(lines) - end_idx} more lines]"

    return [TextContent(type="text", text=result)]


def _edit_file(path: str, old_str: str, new_str: str) -> list[TextContent]:
    """Surgical edit with exact match requirement."""
    resolved = _resolve_path(path)
    if not resolved.exists():
        return [TextContent(type="text", text=f"File not found: {resolved}")]
    if not resolved.is_file():
        return [TextContent(type="text", text=f"Not a file: {resolved}")]

    try:
        content = resolved.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        content = resolved.read_text(encoding="latin-1")

    count = content.count(old_str)
    if count == 0:
        return [
            TextContent(
                type="text",
                text=f"FAILED: old_str not found in file.\n\nSearched for:\n{old_str[:200]}{'...' if len(old_str) > 200 else ''}",
            )
        ]
    if count > 1:
        return [
            TextContent(
                type="text",
                text=f"FAILED: old_str found {count} times. Must be unique. Add more context to make it unique.",
            )
        ]

    # Store for undo
    _edit_history[str(resolved)] = content

    new_content = content.replace(old_str, new_str, 1)
    resolved.write_text(new_content, encoding="utf-8")

    return [TextContent(type="text", text=f"OK: Edited {resolved}")]


def _create_file(path: str, content: str, overwrite: bool) -> list[TextContent]:
    """Create file, optionally overwriting."""
    resolved = _resolve_path(path)

    if resolved.exists() and not overwrite:
        return [
            TextContent(
                type="text",
                text=f"FAILED: File exists: {resolved}\nUse overwrite=True to replace.",
            )
        ]

    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(content, encoding="utf-8")

    return [TextContent(type="text", text=f"OK: Created {resolved}")]


def _undo_edit(path: str) -> list[TextContent]:
    """Revert last edit."""
    resolved = _resolve_path(path)
    key = str(resolved)

    if key not in _edit_history:
        return [TextContent(type="text", text=f"No edit history for: {resolved}")]

    previous = _edit_history.pop(key)
    resolved.write_text(previous, encoding="utf-8")

    return [TextContent(type="text", text=f"OK: Reverted {resolved}")]


def _glob_files(pattern: str, limit: int) -> list[TextContent]:
    """Find files by glob pattern."""
    cwd = Path.cwd()

    # Handle patterns with leading path
    if "/" in pattern or "\\" in pattern:
        base = Path(pattern.split("*")[0].rstrip("/\\")) if "*" in pattern else cwd
        if not base.exists():
            base = cwd
    else:
        base = cwd

    matches = list(base.glob(pattern))[:limit]
    result = "\n".join(str(m) for m in matches)

    if not matches:
        result = f"No matches for pattern: {pattern}"
    elif len(matches) == limit:
        result += f"\n\n[Limited to {limit} results]"

    return [TextContent(type="text", text=result)]


def _grep(pattern: str, path: str, case_sensitive: bool) -> list[TextContent]:
    """Search for pattern in files."""
    resolved = _resolve_path(path)
    if not resolved.exists():
        return [TextContent(type="text", text=f"Path not found: {resolved}")]

    flags = 0 if case_sensitive else re.IGNORECASE
    try:
        regex = re.compile(pattern, flags)
    except re.error as e:
        return [TextContent(type="text", text=f"Invalid regex: {e}")]

    results = []
    max_results = 100
    max_per_file = 10

    def search_file(file_path: Path) -> list[str]:
        matches = []
        try:
            content = file_path.read_text(encoding="utf-8", errors="ignore")
            for i, line in enumerate(content.splitlines(), 1):
                if regex.search(line):
                    truncated = line[:200] + "..." if len(line) > 200 else line
                    matches.append(f"{file_path}:{i}: {truncated}")
                    if len(matches) >= max_per_file:
                        break
        except Exception:
            pass
        return matches

    if resolved.is_file():
        results = search_file(resolved)
    else:
        for file_path in resolved.rglob("*"):
            if file_path.is_file():
                results.extend(search_file(file_path))
                if len(results) >= max_results:
                    break

    if not results:
        return [TextContent(type="text", text=f"No matches for: {pattern}")]

    output = "\n".join(results[:max_results])
    if len(results) >= max_results:
        output += f"\n\n[Limited to {max_results} results]"

    return [TextContent(type="text", text=output)]


def _run_command(command: str, cwd: str | None, timeout: int = 300) -> list[TextContent]:
    """Execute shell command with real-time output capture."""
    work_dir = _resolve_path(cwd) if cwd else None
    
    # Cap timeout at 1 hour max, minimum 10s
    timeout = max(10, min(timeout, 3600))

    try:
        import time
        start = time.time()
        
        # Use Popen for better output handling
        process = subprocess.Popen(
            command,
            shell=True,
            cwd=work_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        
        # Wait with timeout
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            return [TextContent(type="text", text=f"""⏱️ COMMAND TIMEOUT after {timeout}s

The command was killed after {timeout} seconds. This doesn't mean it failed - 
long-running commands like odibi pipelines may have completed in the background.

WHAT TO DO:
1. Check if output files were created (use list_dir or glob_files)
2. Check odibi stories: story_read(pipeline_name)
3. Run with higher timeout: run_command(cmd, timeout=600)
4. For very long operations, run in terminal instead

Partial output:
{stdout}
{stderr}""")]
        
        elapsed = time.time() - start
        
        output = ""
        if stdout:
            output += stdout
        if stderr:
            if output:
                output += "\n--- STDERR ---\n"
            output += stderr
        if process.returncode != 0:
            output += f"\n\n❌ [Exit code: {process.returncode}]"
        else:
            output += f"\n\n✅ [Completed successfully in {elapsed:.1f}s]"
        
        return [TextContent(type="text", text=output or "(no output)")]
    except Exception as e:
        return [TextContent(type="text", text=f"""❌ COMMAND FAILED

Error: {e}

WHAT TO DO:
1. Check the command syntax
2. Verify file paths exist
3. Try running in terminal to see full error""")]


def _find_path(name: str) -> list[TextContent]:
    """Smart path finder."""
    cwd = Path.cwd()

    # Try exact path
    exact = _resolve_path(name)
    if exact.exists():
        return [TextContent(type="text", text=str(exact))]

    # Try in cwd
    in_cwd = cwd / name
    if in_cwd.exists():
        return [TextContent(type="text", text=str(in_cwd.resolve()))]

    # Try glob in cwd
    matches = list(cwd.glob(f"**/{name}"))[:10]
    if matches:
        result = "\n".join(str(m) for m in matches)
        return [TextContent(type="text", text=result)]

    # Try glob pattern
    if "*" in name:
        matches = list(cwd.glob(name))[:10]
        if matches:
            result = "\n".join(str(m) for m in matches)
            return [TextContent(type="text", text=result)]

    return [TextContent(type="text", text=f"Not found: {name}")]


def _list_dir(path: str, details: bool) -> list[TextContent]:
    """List directory contents."""
    resolved = _resolve_path(path)
    if not resolved.exists():
        return [TextContent(type="text", text=f"Directory not found: {resolved}")]
    if not resolved.is_dir():
        return [TextContent(type="text", text=f"Not a directory: {resolved}")]

    entries = []
    for item in sorted(resolved.iterdir()):
        name = item.name + ("/" if item.is_dir() else "")
        if details:
            try:
                stat = item.stat()
                size = stat.st_size
                from datetime import datetime

                mtime = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M")
                entries.append(f"{name:40} {size:>10} {mtime}")
            except Exception:
                entries.append(name)
        else:
            entries.append(name)

    return [TextContent(type="text", text="\n".join(entries) or "(empty)")]


def _create_dir(path: str) -> list[TextContent]:
    """Create directory with parents."""
    resolved = _resolve_path(path)
    resolved.mkdir(parents=True, exist_ok=True)
    return [TextContent(type="text", text=f"OK: Created {resolved}")]


def _delete_path(path: str) -> list[TextContent]:
    """Delete file or empty directory."""
    resolved = _resolve_path(path)
    if not resolved.exists():
        return [TextContent(type="text", text=f"Path not found: {resolved}")]

    if resolved.is_file():
        resolved.unlink()
        return [TextContent(type="text", text=f"OK: Deleted file {resolved}")]
    elif resolved.is_dir():
        try:
            resolved.rmdir()
            return [TextContent(type="text", text=f"OK: Deleted directory {resolved}")]
        except OSError:
            return [
                TextContent(
                    type="text",
                    text=f"FAILED: Directory not empty: {resolved}\nUse run_command with 'rm -rf' if you really want to delete non-empty directories.",
                )
            ]
    return [TextContent(type="text", text=f"Unknown path type: {resolved}")]


def _move_path(src: str, dst: str) -> list[TextContent]:
    """Move/rename path."""
    src_resolved = _resolve_path(src)
    dst_resolved = _resolve_path(dst)

    if not src_resolved.exists():
        return [TextContent(type="text", text=f"Source not found: {src_resolved}")]

    dst_resolved.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src_resolved), str(dst_resolved))

    return [TextContent(type="text", text=f"OK: Moved {src_resolved} -> {dst_resolved}")]


def _copy_path(src: str, dst: str) -> list[TextContent]:
    """Copy file or folder."""
    src_resolved = _resolve_path(src)
    dst_resolved = _resolve_path(dst)

    if not src_resolved.exists():
        return [TextContent(type="text", text=f"Source not found: {src_resolved}")]

    dst_resolved.parent.mkdir(parents=True, exist_ok=True)

    if src_resolved.is_file():
        shutil.copy2(str(src_resolved), str(dst_resolved))
    else:
        shutil.copytree(str(src_resolved), str(dst_resolved))

    return [TextContent(type="text", text=f"OK: Copied {src_resolved} -> {dst_resolved}")]


def _path_exists(path: str) -> list[TextContent]:
    """Check if path exists."""
    resolved = _resolve_path(path)
    exists = resolved.exists()
    path_type = "directory" if resolved.is_dir() else "file" if resolved.is_file() else "unknown"

    if exists:
        return [TextContent(type="text", text=f"Yes: {resolved} ({path_type})")]
    return [TextContent(type="text", text=f"No: {resolved}")]


def _resolve_path_tool(path: str) -> list[TextContent]:
    """Resolve path."""
    resolved = _resolve_path(path)
    return [TextContent(type="text", text=str(resolved))]


def _fetch_url(url: str) -> list[TextContent]:
    """Fetch URL content."""
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "odibi-fileops/1.0"},
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            content = response.read().decode("utf-8", errors="replace")
            return [TextContent(type="text", text=content[:50000])]
    except Exception as e:
        return [TextContent(type="text", text=f"Fetch failed: {e}")]


def _git_command(args: list[str], cwd: Path | None) -> str:
    """Run git command and return output."""
    try:
        result = subprocess.run(
            ["git"] + args,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.stdout + result.stderr
    except Exception as e:
        return f"Git command failed: {e}"


def _git_status(path: str | None) -> list[TextContent]:
    """Get git status."""
    cwd = _resolve_path(path) if path else None
    output = _git_command(["status", "--short"], cwd)
    return [TextContent(type="text", text=output or "(working tree clean)")]


def _git_diff(path: str | None) -> list[TextContent]:
    """Get git diff."""
    cwd = _resolve_path(path) if path else None
    output = _git_command(["diff"], cwd)
    return [TextContent(type="text", text=output or "(no changes)")]


def _git_log(n: int, path: str | None) -> list[TextContent]:
    """Get git log."""
    cwd = _resolve_path(path) if path else None
    output = _git_command(["log", f"-{n}", "--oneline"], cwd)
    return [TextContent(type="text", text=output or "(no commits)")]


async def main():
    """Run the MCP server."""
    import sys
    # Immediate startup signal for debugging
    print("odibi-fileops: starting...", file=sys.stderr, flush=True)
    print(f"odibi-fileops: workspace={_get_workspace()}", file=sys.stderr, flush=True)
    print(f"odibi-fileops: ODIBI_WORKSPACE={os.environ.get('ODIBI_WORKSPACE', 'NOT SET')}", file=sys.stderr, flush=True)
    print(f"odibi-fileops: cwd={os.getcwd()}", file=sys.stderr, flush=True)
    async with stdio_server() as (read_stream, write_stream):
        print("odibi-fileops: stdio ready", file=sys.stderr, flush=True)
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())

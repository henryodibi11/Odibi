"""Git integration tools.

View diffs, status, and manage git operations.
"""

import subprocess
from dataclasses import dataclass
from typing import Optional


@dataclass
class GitResult:
    """Result of a git operation."""

    success: bool
    output: str
    error: Optional[str] = None
    command: str = ""


def _run_git(
    args: list[str],
    working_dir: str,
    timeout: int = 30,
) -> GitResult:
    """Run a git command.

    Args:
        args: Git command arguments (without 'git').
        working_dir: Repository directory.
        timeout: Command timeout.

    Returns:
        GitResult with output.
    """
    cmd = ["git"] + args

    try:
        result = subprocess.run(
            cmd,
            cwd=working_dir,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        return GitResult(
            success=result.returncode == 0,
            output=result.stdout,
            error=result.stderr if result.returncode != 0 else None,
            command=" ".join(cmd),
        )

    except subprocess.TimeoutExpired:
        return GitResult(
            success=False,
            output="",
            error=f"Git command timed out after {timeout}s",
            command=" ".join(cmd),
        )
    except FileNotFoundError:
        return GitResult(
            success=False,
            output="",
            error="Git is not installed or not in PATH",
            command=" ".join(cmd),
        )
    except Exception as e:
        return GitResult(
            success=False,
            output="",
            error=str(e),
            command=" ".join(cmd),
        )


def git_status(working_dir: str, short: bool = True) -> GitResult:
    """Get git repository status.

    Args:
        working_dir: Repository directory.
        short: Use short format.

    Returns:
        GitResult with status output.
    """
    args = ["status"]
    if short:
        args.append("--short")
    return _run_git(args, working_dir)


def git_diff(
    working_dir: str,
    path: Optional[str] = None,
    staged: bool = False,
    context_lines: int = 3,
) -> GitResult:
    """Get git diff.

    Args:
        working_dir: Repository directory.
        path: Optional specific file to diff.
        staged: Show staged changes only.
        context_lines: Lines of context around changes.

    Returns:
        GitResult with diff output.
    """
    args = ["diff", f"-U{context_lines}"]
    if staged:
        args.append("--staged")
    if path:
        args.extend(["--", path])
    return _run_git(args, working_dir)


def git_log(
    working_dir: str,
    max_count: int = 10,
    oneline: bool = True,
    path: Optional[str] = None,
) -> GitResult:
    """Get git log.

    Args:
        working_dir: Repository directory.
        max_count: Maximum commits to show.
        oneline: Use one-line format.
        path: Optional file to show history for.

    Returns:
        GitResult with log output.
    """
    args = ["log", f"-{max_count}"]
    if oneline:
        args.append("--oneline")
    if path:
        args.extend(["--", path])
    return _run_git(args, working_dir)


def git_branch(working_dir: str) -> GitResult:
    """Get current branch and list branches.

    Args:
        working_dir: Repository directory.

    Returns:
        GitResult with branch info.
    """
    return _run_git(["branch", "-v"], working_dir)


def git_show(
    working_dir: str,
    ref: str = "HEAD",
    path: Optional[str] = None,
) -> GitResult:
    """Show a commit or file at a ref.

    Args:
        working_dir: Repository directory.
        ref: Commit ref (default HEAD).
        path: Optional file path.

    Returns:
        GitResult with show output.
    """
    args = ["show", ref]
    if path:
        args.extend(["--", path])
    return _run_git(args, working_dir)


def git_blame(
    working_dir: str,
    path: str,
    start_line: Optional[int] = None,
    end_line: Optional[int] = None,
) -> GitResult:
    """Get git blame for a file.

    Args:
        working_dir: Repository directory.
        path: File path.
        start_line: Optional start line.
        end_line: Optional end line.

    Returns:
        GitResult with blame output.
    """
    args = ["blame"]
    if start_line and end_line:
        args.extend(["-L", f"{start_line},{end_line}"])
    args.append(path)
    return _run_git(args, working_dir)


def git_stash_list(working_dir: str) -> GitResult:
    """List git stashes.

    Args:
        working_dir: Repository directory.

    Returns:
        GitResult with stash list.
    """
    return _run_git(["stash", "list"], working_dir)


def is_git_repo(path: str) -> bool:
    """Check if a path is inside a git repository.

    Args:
        path: Path to check.

    Returns:
        True if inside a git repo.
    """
    result = _run_git(["rev-parse", "--git-dir"], path)
    return result.success


def get_repo_root(path: str) -> Optional[str]:
    """Get the root directory of the git repository.

    Args:
        path: Path inside the repo.

    Returns:
        Repository root path or None.
    """
    result = _run_git(["rev-parse", "--show-toplevel"], path)
    if result.success:
        return result.output.strip()
    return None


def format_git_result(result: GitResult) -> str:
    """Format git result for display.

    Args:
        result: GitResult to format.

    Returns:
        Markdown formatted output.
    """
    if not result.success:
        return f"❌ **Git Error:** {result.error}\n\n`{result.command}`"

    if not result.output.strip():
        return f"✅ `{result.command}` - No output (clean)"

    return f"""✅ `{result.command}`

```diff
{result.output}
```"""


def format_git_status(result: GitResult) -> str:
    """Format git status for display.

    Args:
        result: GitResult from git_status.

    Returns:
        Markdown formatted status.
    """
    if not result.success:
        return f"❌ **Git Error:** {result.error}"

    if not result.output.strip():
        return "✅ **Working tree clean** - No changes"

    lines = result.output.strip().split("\n")
    staged = []
    unstaged = []
    untracked = []

    for line in lines:
        if line.startswith("??"):
            untracked.append(line[3:])
        elif line[0] != " ":
            staged.append(line)
        else:
            unstaged.append(line)

    parts = ["📊 **Git Status:**\n"]

    if staged:
        parts.append("**Staged:**")
        for f in staged:
            parts.append(f"- `{f}`")
        parts.append("")

    if unstaged:
        parts.append("**Modified:**")
        for f in unstaged:
            parts.append(f"- `{f}`")
        parts.append("")

    if untracked:
        parts.append("**Untracked:**")
        for f in untracked[:10]:
            parts.append(f"- `{f}`")
        if len(untracked) > 10:
            parts.append(f"- ... and {len(untracked) - 10} more")

    return "\n".join(parts)


def format_diff(result: GitResult, max_lines: int = 100) -> str:
    """Format git diff for display.

    Args:
        result: GitResult from git_diff.
        max_lines: Maximum lines to show.

    Returns:
        Markdown formatted diff.
    """
    if not result.success:
        return f"❌ **Git Error:** {result.error}"

    if not result.output.strip():
        return "✅ **No changes**"

    lines = result.output.split("\n")
    if len(lines) > max_lines:
        output = "\n".join(lines[:max_lines])
        output += f"\n\n... ({len(lines) - max_lines} more lines)"
    else:
        output = result.output

    return f"""📝 **Diff:**

```diff
{output}
```"""

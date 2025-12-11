"""Code search tools for agents.

Grep-style searching and glob pattern matching.
"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class SearchMatch:
    """A single search match."""

    file_path: str
    line_number: int
    line_content: str
    match_start: int = 0
    match_end: int = 0


@dataclass
class SearchResult:
    """Result of a search operation."""

    success: bool
    matches: list[SearchMatch] = field(default_factory=list)
    total_matches: int = 0
    files_searched: int = 0
    error: Optional[str] = None


def grep_search(
    pattern: str,
    path: str,
    file_pattern: str = "*.py",
    case_sensitive: bool = False,
    is_regex: bool = False,
    max_results: int = 50,
    max_results_per_file: int = 10,
    context_lines: int = 0,
) -> SearchResult:
    """Search for a pattern in files.

    Args:
        pattern: Text or regex pattern to search for.
        path: Directory or file to search in.
        file_pattern: Glob pattern for files to search.
        case_sensitive: Whether search is case-sensitive.
        is_regex: Whether pattern is a regex.
        max_results: Maximum total results.
        max_results_per_file: Maximum results per file.
        context_lines: Number of context lines around match.

    Returns:
        SearchResult with all matches.
    """
    try:
        search_path = Path(path)
        if not search_path.exists():
            return SearchResult(
                success=False,
                error=f"Path not found: {path}",
            )

        if is_regex:
            flags = 0 if case_sensitive else re.IGNORECASE
            try:
                regex = re.compile(pattern, flags)
            except re.error as e:
                return SearchResult(
                    success=False,
                    error=f"Invalid regex: {e}",
                )
        else:
            if not case_sensitive:
                pattern_lower = pattern.lower()

        matches = []
        files_searched = 0

        if search_path.is_file():
            files = [search_path]
        else:
            files = list(search_path.rglob(file_pattern))

        for file_path in files:
            if not file_path.is_file():
                continue

            files_searched += 1
            file_matches = 0

            try:
                with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                    lines = f.readlines()

                for line_num, line in enumerate(lines, 1):
                    if file_matches >= max_results_per_file:
                        break
                    if len(matches) >= max_results:
                        break

                    if is_regex:
                        match = regex.search(line)
                        if match:
                            matches.append(
                                SearchMatch(
                                    file_path=str(file_path),
                                    line_number=line_num,
                                    line_content=line.rstrip(),
                                    match_start=match.start(),
                                    match_end=match.end(),
                                )
                            )
                            file_matches += 1
                    else:
                        search_line = line if case_sensitive else line.lower()
                        search_pattern = pattern if case_sensitive else pattern_lower
                        pos = search_line.find(search_pattern)
                        if pos >= 0:
                            matches.append(
                                SearchMatch(
                                    file_path=str(file_path),
                                    line_number=line_num,
                                    line_content=line.rstrip(),
                                    match_start=pos,
                                    match_end=pos + len(pattern),
                                )
                            )
                            file_matches += 1

            except Exception:
                continue

            if len(matches) >= max_results:
                break

        return SearchResult(
            success=True,
            matches=matches,
            total_matches=len(matches),
            files_searched=files_searched,
        )

    except Exception as e:
        return SearchResult(
            success=False,
            error=str(e),
        )


def glob_files(
    pattern: str,
    path: str,
    max_results: int = 100,
) -> SearchResult:
    """Find files matching a glob pattern.

    Args:
        pattern: Glob pattern (e.g., "**/*.py", "*test*.py").
        path: Base directory to search from.
        max_results: Maximum files to return.

    Returns:
        SearchResult with matching file paths.
    """
    try:
        base_path = Path(path)
        if not base_path.exists():
            return SearchResult(
                success=False,
                error=f"Path not found: {path}",
            )

        if not base_path.is_dir():
            return SearchResult(
                success=False,
                error=f"Not a directory: {path}",
            )

        files = list(base_path.glob(pattern))[:max_results]

        matches = [
            SearchMatch(
                file_path=str(f),
                line_number=0,
                line_content=str(f.relative_to(base_path)),
            )
            for f in sorted(files)
            if f.is_file()
        ]

        return SearchResult(
            success=True,
            matches=matches,
            total_matches=len(matches),
            files_searched=1,
        )

    except Exception as e:
        return SearchResult(
            success=False,
            error=str(e),
        )


def format_search_results(result: SearchResult) -> str:
    """Format search results for display in chat.

    Args:
        result: The search result to format.

    Returns:
        Markdown-formatted search results.
    """
    if not result.success:
        return f"**Search Error:** {result.error}"

    if not result.matches:
        return f"**No matches found** (searched {result.files_searched} files)"

    lines = [f"**Found {result.total_matches} matches** in {result.files_searched} files:\n"]

    current_file = ""
    for match in result.matches:
        if match.file_path != current_file:
            current_file = match.file_path
            lines.append(f"\n**`{current_file}`**")

        if match.line_number > 0:
            content = match.line_content[:150]
            if len(match.line_content) > 150:
                content += "..."
            lines.append(f"- L{match.line_number}: `{content}`")
        else:
            lines.append(f"- {match.line_content}")

    return "\n".join(lines)

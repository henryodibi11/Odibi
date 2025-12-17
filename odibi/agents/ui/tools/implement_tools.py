"""Implementation tools for autonomous code changes with verification.

Provides compound tools that implement features and automatically verify with tests.
"""

from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

from .shell_tools import run_pytest, run_ruff, CommandResult


@dataclass
class ImplementationResult:
    """Result of an implementation attempt."""

    success: bool
    description: str
    files_changed: list[str] = field(default_factory=list)
    test_results: Optional[CommandResult] = None
    lint_results: Optional[CommandResult] = None
    iterations: int = 0
    error: Optional[str] = None


def implement_feature(
    description: str,
    target_files: list[str],
    test_pattern: Optional[str] = None,
    max_iterations: int = 3,
    project_root: str = "D:/odibi",
) -> ImplementationResult:
    """
    Implement a feature with automatic test verification.

    This is a compound operation that:
    1. Runs initial tests to establish baseline
    2. Allows agent to make changes (via write_file)
    3. Runs tests after changes
    4. Reports success/failure

    Note: The actual code writing is done by the agent via write_file tool.
    This tool orchestrates the test-verify cycle.

    Args:
        description: What to implement
        target_files: Files that will be modified
        test_pattern: Pytest pattern (e.g., '-k profiler' or 'tests/unit/test_x.py')
        max_iterations: Max attempts to fix failing tests
        project_root: Root directory for running tests

    Returns:
        ImplementationResult with test/lint outcomes
    """
    result = ImplementationResult(
        success=False,
        description=description,
        files_changed=target_files,
    )

    # Step 1: Run lint check
    lint_result = run_ruff(
        path=project_root,
        fix=False,
    )
    result.lint_results = lint_result

    # Step 2: Run tests if pattern provided
    if test_pattern:
        test_result = run_pytest(
            test_path=test_pattern,
            working_dir=project_root,
            verbose=True,
            extra_args="-x",  # Stop on first failure
        )
        result.test_results = test_result
        result.success = test_result.success
    else:
        # No tests specified, just check lint passed
        result.success = lint_result.success

    result.iterations = 1
    return result


def format_implementation_result(result: ImplementationResult) -> str:
    """Format implementation result for display."""
    lines = [
        f"## {'✅' if result.success else '❌'} Implementation: {result.description}",
        "",
        f"**Files:** {', '.join(result.files_changed)}",
        f"**Iterations:** {result.iterations}",
        "",
    ]

    if result.lint_results:
        lint_icon = "✅" if result.lint_results.success else "⚠️"
        lines.append(f"### {lint_icon} Lint Check")
        if not result.lint_results.success and result.lint_results.stdout:
            lines.extend(
                [
                    "```",
                    result.lint_results.stdout[:1000],
                    "```",
                ]
            )
        elif result.lint_results.success:
            lines.append("_No lint issues_")
        lines.append("")

    if result.test_results:
        test_icon = "✅" if result.test_results.success else "❌"
        lines.append(f"### {test_icon} Test Results")
        output = result.test_results.stdout or result.test_results.stderr
        if output:
            # Truncate long output
            if len(output) > 2000:
                output = output[:2000] + "\n... (truncated)"
            lines.extend(
                [
                    "```",
                    output,
                    "```",
                ]
            )
        lines.append("")

    if result.error:
        lines.extend(
            [
                "### ❌ Error",
                f"```\n{result.error}\n```",
            ]
        )

    return "\n".join(lines)


def verify_changes(
    test_pattern: str,
    project_root: str = "D:/odibi",
) -> CommandResult:
    """
    Verify code changes by running tests.

    Use this after making changes to verify they work.

    Args:
        test_pattern: Pytest pattern to run
        project_root: Project root directory

    Returns:
        CommandResult with test output
    """
    return run_pytest(
        test_path=test_pattern,
        working_dir=project_root,
        verbose=True,
        extra_args="-x",
    )


def find_tests(source_file: str, project_root: str = "D:/odibi") -> dict:
    """
    Find test files related to a source file.

    Looks for:
    1. tests/unit/test_{module}.py
    2. tests/test_{module}.py
    3. {module}_test.py in same directory
    4. Any test file containing the module name

    Args:
        source_file: Path to source file
        project_root: Project root directory

    Returns:
        Dict with found test files and suggested test commands
    """

    source_path = Path(source_file)
    module_name = source_path.stem  # e.g., "engine" from "engine.py"
    parent_dir = source_path.parent.name  # e.g., "validation"

    found_tests = []
    search_patterns = [
        f"tests/unit/test_{module_name}.py",
        f"tests/unit/test_{parent_dir}_{module_name}.py",
        f"tests/test_{module_name}.py",
        f"tests/**/test_{module_name}.py",
        f"tests/**/*{module_name}*.py",
    ]

    root = Path(project_root)
    for pattern in search_patterns:
        matches = list(root.glob(pattern))
        for match in matches:
            rel_path = str(match.relative_to(root))
            if rel_path not in found_tests and "test" in rel_path.lower():
                found_tests.append(rel_path)

    # Generate suggested pytest commands
    if found_tests:
        test_cmd = f"pytest {found_tests[0]} -v"
        all_tests_cmd = f"pytest {' '.join(found_tests)} -v"
    else:
        test_cmd = f"pytest -k {module_name} -v"
        all_tests_cmd = test_cmd

    return {
        "source_file": source_file,
        "module_name": module_name,
        "found_tests": found_tests,
        "test_count": len(found_tests),
        "suggested_command": test_cmd,
        "all_tests_command": all_tests_cmd,
    }


def format_find_tests_result(result: dict) -> str:
    """Format find_tests result for display."""
    lines = [
        f"## 🧪 Tests for `{result['module_name']}`",
        "",
    ]

    if result["found_tests"]:
        lines.append(f"**Found {result['test_count']} test file(s):**")
        for tf in result["found_tests"]:
            lines.append(f"- `{tf}`")
        lines.append("")
        lines.append(f"**Run tests:** `{result['suggested_command']}`")
    else:
        lines.append("_No existing test files found._")
        lines.append("")
        lines.append(f"**Suggested search:** `{result['suggested_command']}`")
        lines.append("")
        lines.append("Consider creating: `tests/unit/test_{}.py`".format(result["module_name"]))

    return "\n".join(lines)


def code_review(
    project_root: str = "D:/odibi",
    path: Optional[str] = None,
    staged_only: bool = False,
) -> dict:
    """
    Review uncommitted changes.

    Args:
        project_root: Project root directory
        path: Specific path to review
        staged_only: Only review staged changes

    Returns:
        Dict with diff, file list, and review summary
    """
    from .git_tools import git_diff, git_status

    status = git_status(working_dir=project_root)
    diff_result = git_diff(
        working_dir=project_root,
        path=path,
        staged=staged_only,
    )

    # Count changes
    lines_added = diff_result.stdout.count("\n+") if diff_result.stdout else 0
    lines_removed = diff_result.stdout.count("\n-") if diff_result.stdout else 0

    return {
        "success": diff_result.success,
        "diff": diff_result.stdout or "",
        "status": status,
        "lines_added": lines_added,
        "lines_removed": lines_removed,
        "path": path or "entire repo",
        "staged_only": staged_only,
    }


def format_code_review_result(result: dict) -> str:
    """Format code review result for display."""
    lines = [
        "## 📝 Code Review",
        "",
        f"**Scope:** {result['path']}",
        f"**Lines added:** +{result['lines_added']}",
        f"**Lines removed:** -{result['lines_removed']}",
        "",
    ]

    if result["diff"]:
        diff = result["diff"]
        if len(diff) > 3000:
            diff = diff[:3000] + "\n... (truncated)"
        lines.extend(
            [
                "### Diff",
                "```diff",
                diff,
                "```",
            ]
        )
    else:
        lines.append("_No changes to review._")

    return "\n".join(lines)

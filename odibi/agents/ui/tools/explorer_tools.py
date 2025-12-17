"""Explorer tools for sandboxed experimentation.

Allows the agent to run experiments in isolated sandbox environments,
capturing diffs and results without affecting the trusted codebase.
"""

from pathlib import Path
from typing import Any


def run_explorer_experiment(
    source_path: str,
    experiment_name: str,
    commands: list[str],
    description: str = "",
    hypothesis: str = "",
    timeout: int = 120,
    sandbox_root: str = "D:/explorer_sandboxes",
    trusted_repo: str = "D:/odibi",
) -> dict[str, Any]:
    """Run an experiment in an isolated sandbox.

    Args:
        source_path: Path to directory to clone for experimentation
        experiment_name: Short name for the experiment
        commands: List of shell commands to execute
        description: What the experiment attempts
        hypothesis: Expected outcome
        timeout: Command timeout in seconds
        sandbox_root: Where sandboxes are created
        trusted_repo: The trusted repository root

    Returns:
        Dict with experiment results including stdout, stderr, diff, and status
    """
    try:
        from odibi.agents.explorer.clone_manager import RepoCloneManager
        from odibi.agents.explorer.experiment_runner import ExperimentRunner
    except ImportError as e:
        return {
            "success": False,
            "error": f"Explorer module not available: {e}",
            "suggestion": "Ensure agents.explorer is installed",
        }

    sandbox_path = Path(sandbox_root)
    trusted_path = Path(trusted_repo)
    source = Path(source_path)

    if not source.exists():
        return {
            "success": False,
            "error": f"Source path does not exist: {source_path}",
        }

    sandbox_path.mkdir(parents=True, exist_ok=True)

    try:
        clone_manager = RepoCloneManager(
            sandbox_root=sandbox_path,
            trusted_repo_root=trusted_path,
        )
        experiment_runner = ExperimentRunner(clone_manager)

        sandbox_info = clone_manager.create_sandbox(str(source))
        sandbox_id = sandbox_info.get("id", "unknown")
        sandbox_dir = sandbox_info.get("path", "")

        result = experiment_runner.run_experiment(
            sandbox_id=sandbox_id,
            name=experiment_name,
            commands=commands,
            timeout=timeout,
        )

        return {
            "success": result.get("status") == "completed",
            "experiment_name": experiment_name,
            "sandbox_id": sandbox_id,
            "sandbox_path": sandbox_dir,
            "status": result.get("status", "unknown"),
            "exit_code": result.get("exit_code"),
            "stdout": result.get("stdout", ""),
            "stderr": result.get("stderr", ""),
            "diff": result.get("diff", ""),
            "diff_hash": result.get("diff_hash", ""),
            "diff_empty": result.get("diff_empty", True),
            "description": description,
            "hypothesis": hypothesis,
            "started_at": result.get("started_at"),
            "ended_at": result.get("ended_at"),
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "experiment_name": experiment_name,
        }


def format_experiment_result(result: dict[str, Any]) -> str:
    """Format experiment result for display."""
    if not result.get("success"):
        error = result.get("error", "Unknown error")
        return f"❌ **Experiment Failed**\n\n```\n{error}\n```"

    lines = [
        f"## {'✅' if result['success'] else '❌'} Experiment: {result.get('experiment_name', 'unknown')}",
        "",
        f"**Status:** {result.get('status', 'unknown')}",
        f"**Exit Code:** {result.get('exit_code', 'N/A')}",
        f"**Sandbox:** `{result.get('sandbox_path', 'N/A')}`",
    ]

    if result.get("description"):
        lines.append(f"**Description:** {result['description']}")

    if result.get("hypothesis"):
        lines.append(f"**Hypothesis:** {result['hypothesis']}")

    lines.append("")

    if result.get("stdout"):
        stdout = result["stdout"]
        if len(stdout) > 2000:
            stdout = stdout[:2000] + "\n... (truncated)"
        lines.extend(
            [
                "### stdout",
                "```",
                stdout,
                "```",
                "",
            ]
        )

    if result.get("stderr"):
        stderr = result["stderr"]
        if len(stderr) > 1000:
            stderr = stderr[:1000] + "\n... (truncated)"
        lines.extend(
            [
                "### stderr",
                "```",
                stderr,
                "```",
                "",
            ]
        )

    if not result.get("diff_empty", True):
        diff = result.get("diff", "")
        lines.extend(
            [
                "### Diff Preview",
                "```diff",
                diff[:3000] if len(diff) > 3000 else diff,
                "```",
                "",
            ]
        )
        lines.append(f"**Diff Hash:** `{result.get('diff_hash', 'N/A')}`")
    else:
        lines.append("_No file changes detected_")

    return "\n".join(lines)


def cleanup_sandbox(sandbox_id: str, sandbox_root: str = "D:/explorer_sandboxes") -> dict[str, Any]:
    """Clean up a specific sandbox."""
    try:
        from odibi.agents.explorer.clone_manager import RepoCloneManager

        clone_manager = RepoCloneManager(
            sandbox_root=Path(sandbox_root),
            trusted_repo_root=Path("D:/odibi"),
        )
        clone_manager.cleanup_sandbox(sandbox_id)
        return {"success": True, "message": f"Cleaned up sandbox {sandbox_id}"}
    except Exception as e:
        return {"success": False, "error": str(e)}

"""Sub-agent task execution tools.

Enables the main agent to spawn focused sub-agents that work in parallel,
similar to how Amp handles complex multi-step tasks.
"""

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

from ..llm_client import LLMClient, LLMConfig

from .file_tools import read_file, list_directory
from .search_tools import grep_search, glob_files, format_search_results


@dataclass
class TaskResult:
    """Result from a sub-agent task."""

    success: bool
    task_description: str
    result: str
    error: Optional[str] = None
    steps_taken: list[str] = field(default_factory=list)


SUB_AGENT_SYSTEM_PROMPT = """You are a focused sub-agent working on a specific task.

You have access to these READ-ONLY tools:
- **read_file(path, start_line?, end_line?)** - Read a file
- **list_directory(path, pattern?, recursive?)** - List directory contents  
- **grep(pattern, path, file_pattern?)** - Search for text in files
- **glob(pattern, path)** - Find files matching a pattern

## Tool Usage Format
```tool
{"tool": "tool_name", "args": {"arg1": "value1"}}
```

## Guidelines
1. Focus ONLY on the task given - don't go beyond scope
2. Be thorough but concise
3. Use tools to gather information
4. Return a clear summary when done
5. Maximum 5 tool calls to complete your task

When you have enough information, provide your final answer WITHOUT any tool calls.
"""


class SubAgent:
    """A focused sub-agent for executing specific tasks."""

    TOOL_PATTERN = re.compile(r"```tool\s*\n({.*?})\s*\n```", re.DOTALL)

    def __init__(self, llm_config: LLMConfig, project_root: str):
        self.client = LLMClient(llm_config)
        self.project_root = project_root

    def execute_tool(self, tool_call: dict) -> str:
        """Execute a tool call."""
        tool_name = tool_call.get("tool", "")
        args = tool_call.get("args", {})

        if tool_name == "read_file":
            result = read_file(
                path=args.get("path", ""),
                start_line=args.get("start_line", 1),
                end_line=args.get("end_line"),
            )
            if result.success:
                return f"**{result.path}** ({result.line_count} lines):\n```\n{result.content[:2000]}\n```"
            return f"Error: {result.error}"

        elif tool_name == "list_directory":
            result = list_directory(
                path=args.get("path", self.project_root),
                pattern=args.get("pattern", "*"),
                recursive=args.get("recursive", False),
            )
            if result.success:
                return f"**{result.path}:**\n```\n{result.content}\n```"
            return f"Error: {result.error}"

        elif tool_name == "grep":
            result = grep_search(
                pattern=args.get("pattern", ""),
                path=args.get("path", self.project_root),
                file_pattern=args.get("file_pattern", "*.py"),
                is_regex=args.get("is_regex", False),
            )
            return format_search_results(result)

        elif tool_name == "glob":
            result = glob_files(
                pattern=args.get("pattern", "**/*"),
                path=args.get("path", self.project_root),
            )
            return format_search_results(result)

        return f"Unknown tool: {tool_name}"

    def run(self, task: str, max_iterations: int = 5) -> TaskResult:
        """Run the sub-agent on a task.

        Args:
            task: The task description.
            max_iterations: Maximum tool calls allowed.

        Returns:
            TaskResult with the outcome.
        """
        messages = [{"role": "user", "content": task}]
        steps = []

        system_prompt = SUB_AGENT_SYSTEM_PROMPT
        system_prompt += f"\n\n**Working Directory:** {self.project_root}"

        try:
            for iteration in range(max_iterations):
                response = self.client.chat(
                    messages=messages,
                    system_prompt=system_prompt,
                    temperature=0.1,
                    max_tokens=2048,
                )
                content = response.get("content", "")

                tool_matches = list(self.TOOL_PATTERN.finditer(content))

                if not tool_matches:
                    return TaskResult(
                        success=True,
                        task_description=task,
                        result=content,
                        steps_taken=steps,
                    )

                messages.append({"role": "assistant", "content": content})

                tool_results = []
                for match in tool_matches:
                    try:
                        tool_call = json.loads(match.group(1))
                        tool_name = tool_call.get("tool", "unknown")
                        steps.append(f"Used {tool_name}")
                        result = self.execute_tool(tool_call)
                        tool_results.append(f"[{tool_name}]: {result}")
                    except json.JSONDecodeError:
                        tool_results.append("Invalid tool format")

                messages.append({
                    "role": "user",
                    "content": "[SYSTEM] Tool results:\n\n" + "\n\n".join(tool_results)
                })

            return TaskResult(
                success=True,
                task_description=task,
                result="Reached maximum iterations. Partial results gathered.",
                steps_taken=steps,
            )

        except Exception as e:
            return TaskResult(
                success=False,
                task_description=task,
                result="",
                error=str(e),
                steps_taken=steps,
            )


def run_task(
    task: str,
    llm_config: LLMConfig,
    project_root: str,
) -> TaskResult:
    """Run a single sub-agent task.

    Args:
        task: Task description for the sub-agent.
        llm_config: LLM configuration.
        project_root: Project root directory.

    Returns:
        TaskResult from the sub-agent.
    """
    agent = SubAgent(llm_config, project_root)
    return agent.run(task)


def run_parallel_tasks(
    tasks: list[str],
    llm_config: LLMConfig,
    project_root: str,
    max_workers: int = 3,
) -> list[TaskResult]:
    """Run multiple sub-agent tasks in parallel.

    Args:
        tasks: List of task descriptions.
        llm_config: LLM configuration.
        project_root: Project root directory.
        max_workers: Maximum parallel workers.

    Returns:
        List of TaskResults in the same order as input tasks.
    """
    results = [None] * len(tasks)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(run_task, task, llm_config, project_root): idx
            for idx, task in enumerate(tasks)
        }

        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                results[idx] = TaskResult(
                    success=False,
                    task_description=tasks[idx],
                    result="",
                    error=str(e),
                )

    return results


def format_task_result(result: TaskResult) -> str:
    """Format a task result for display.

    Args:
        result: TaskResult to format.

    Returns:
        Markdown-formatted result.
    """
    if not result.success:
        return f"❌ **Task Failed:** {result.task_description}\n\nError: {result.error}"

    steps_str = ""
    if result.steps_taken:
        steps_str = f"\n\n**Steps:** {' → '.join(result.steps_taken)}"

    return f"✅ **Task:** {result.task_description}{steps_str}\n\n**Result:**\n{result.result}"


def format_parallel_results(results: list[TaskResult]) -> str:
    """Format multiple task results.

    Args:
        results: List of TaskResults.

    Returns:
        Markdown-formatted results.
    """
    lines = [f"**Completed {len(results)} parallel tasks:**\n"]

    for i, result in enumerate(results, 1):
        lines.append(f"---\n### Task {i}\n{format_task_result(result)}\n")

    return "\n".join(lines)

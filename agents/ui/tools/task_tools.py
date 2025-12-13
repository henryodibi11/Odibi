"""Sub-agent task execution tools.

Enables the main agent to spawn focused sub-agents that work in parallel,
similar to how Amp handles complex multi-step tasks.
Now with live progress callbacks for real-time UI updates.
"""

import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Callable, Optional

from ..llm_client import LLMClient, LLMConfig

from .file_tools import read_file, list_directory
from .search_tools import grep_search, glob_files, format_search_results


@dataclass
class TaskStep:
    """A step taken by a sub-agent."""

    tool_name: str
    status: str = "running"  # running, completed, error
    result: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "tool": self.tool_name,
            "status": self.status,
            "result": self.result[:200] if self.result else None,
        }


@dataclass
class TaskResult:
    """Result from a sub-agent task."""

    success: bool
    task_description: str
    result: str
    error: Optional[str] = None
    steps_taken: list[str] = field(default_factory=list)
    step_details: list[TaskStep] = field(default_factory=list)


@dataclass
class TaskProgress:
    """Progress update from a sub-agent."""

    task_id: str
    task_description: str
    status: str  # starting, tool_call, thinking, completed, error
    message: str
    step_number: int = 0
    tool_name: Optional[str] = None


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
    """A focused sub-agent for executing specific tasks with progress callbacks."""

    TOOL_PATTERN = re.compile(r"```tool\s*\n({.*?})\s*\n```", re.DOTALL)

    def __init__(
        self,
        llm_config: LLMConfig,
        project_root: str,
        task_id: str = "",
        on_progress: Optional[Callable[[TaskProgress], None]] = None,
    ):
        self.client = LLMClient(llm_config)
        self.project_root = project_root
        self.task_id = task_id or f"task_{id(self)}"
        self.on_progress = on_progress
        self._cancelled = False

    def cancel(self):
        """Cancel the sub-agent execution."""
        self._cancelled = True
        self.client.cancel()

    def _emit_progress(
        self,
        status: str,
        message: str,
        step_number: int = 0,
        tool_name: Optional[str] = None,
    ):
        """Emit a progress update."""
        if self.on_progress:
            progress = TaskProgress(
                task_id=self.task_id,
                task_description=self._current_task,
                status=status,
                message=message,
                step_number=step_number,
                tool_name=tool_name,
            )
            self.on_progress(progress)

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

    def run(self, task: str, max_iterations: int = 10) -> TaskResult:
        """Run the sub-agent on a task.

        Args:
            task: The task description.
            max_iterations: Maximum tool calls allowed.

        Returns:
            TaskResult with the outcome.
        """
        self._current_task = task
        messages = [{"role": "user", "content": task}]
        steps = []
        step_details = []

        system_prompt = SUB_AGENT_SYSTEM_PROMPT
        system_prompt += f"\n\n**Working Directory:** {self.project_root}"

        self._emit_progress("starting", f"Starting: {task[:100]}...")

        try:
            for iteration in range(max_iterations):
                if self._cancelled:
                    return TaskResult(
                        success=False,
                        task_description=task,
                        result="",
                        error="Cancelled by user",
                        steps_taken=steps,
                        step_details=step_details,
                    )

                self._emit_progress(
                    "thinking", f"Thinking (step {iteration + 1})...", iteration + 1
                )

                response = self.client.chat(
                    messages=messages,
                    system_prompt=system_prompt,
                    temperature=0.1,
                    max_tokens=2048,
                )
                content = response.get("content", "")

                tool_matches = list(self.TOOL_PATTERN.finditer(content))

                if not tool_matches:
                    self._emit_progress("completed", "Task completed", iteration + 1)
                    return TaskResult(
                        success=True,
                        task_description=task,
                        result=content,
                        steps_taken=steps,
                        step_details=step_details,
                    )

                messages.append({"role": "assistant", "content": content})

                tool_results = []
                for match in tool_matches:
                    try:
                        tool_call = json.loads(match.group(1))
                        tool_name = tool_call.get("tool", "unknown")

                        step = TaskStep(tool_name=tool_name)
                        step_details.append(step)
                        steps.append(f"Used {tool_name}")

                        self._emit_progress(
                            "tool_call",
                            f"Using {tool_name}...",
                            iteration + 1,
                            tool_name,
                        )

                        result = self.execute_tool(tool_call)
                        step.status = "completed"
                        step.result = result
                        tool_results.append(f"[{tool_name}]: {result}")

                    except json.JSONDecodeError:
                        tool_results.append("Invalid tool format")
                        if step_details:
                            step_details[-1].status = "error"

                messages.append(
                    {
                        "role": "user",
                        "content": "[SYSTEM] Tool results:\n\n" + "\n\n".join(tool_results),
                    }
                )

            self._emit_progress("completed", "Max iterations reached", max_iterations)
            return TaskResult(
                success=True,
                task_description=task,
                result="Reached maximum iterations. Partial results gathered.",
                steps_taken=steps,
                step_details=step_details,
            )

        except Exception as e:
            self._emit_progress("error", f"Error: {str(e)}")
            return TaskResult(
                success=False,
                task_description=task,
                result="",
                error=str(e),
                steps_taken=steps,
                step_details=step_details,
            )


class TaskProgressCollector:
    """Collects progress updates from multiple sub-agents."""

    def __init__(self):
        self.updates: list[TaskProgress] = []
        self._lock = threading.Lock()
        self._callbacks: list[Callable[[TaskProgress], None]] = []

    def add_callback(self, callback: Callable[[TaskProgress], None]):
        """Add a callback for progress updates."""
        self._callbacks.append(callback)

    def handle_progress(self, progress: TaskProgress):
        """Handle a progress update from a sub-agent."""
        with self._lock:
            self.updates.append(progress)

        for callback in self._callbacks:
            try:
                callback(progress)
            except Exception:
                pass

    def get_updates(self) -> list[TaskProgress]:
        """Get all collected updates."""
        with self._lock:
            return list(self.updates)

    def clear(self):
        """Clear collected updates."""
        with self._lock:
            self.updates = []

    def format_progress(self) -> str:
        """Format all progress as markdown."""
        if not self.updates:
            return "_No sub-agent activity_"

        lines = []
        current_task = None

        for update in self.updates[-20:]:
            if update.task_id != current_task:
                current_task = update.task_id
                lines.append(f"\n**🤖 {update.task_description[:50]}...**")

            emoji = {
                "starting": "🚀",
                "thinking": "🧠",
                "tool_call": "🔧",
                "completed": "✅",
                "error": "❌",
            }.get(update.status, "⏳")

            lines.append(f"  {emoji} {update.message}")

        return "\n".join(lines)


def run_task(
    task: str,
    llm_config: LLMConfig,
    project_root: str,
    on_progress: Optional[Callable[[TaskProgress], None]] = None,
) -> TaskResult:
    """Run a single sub-agent task.

    Args:
        task: Task description for the sub-agent.
        llm_config: LLM configuration.
        project_root: Project root directory.
        on_progress: Optional callback for progress updates.

    Returns:
        TaskResult from the sub-agent.
    """
    agent = SubAgent(llm_config, project_root, on_progress=on_progress)
    return agent.run(task)


def run_parallel_tasks(
    tasks: list[str],
    llm_config: LLMConfig,
    project_root: str,
    max_workers: int = 3,
    on_progress: Optional[Callable[[TaskProgress], None]] = None,
) -> list[TaskResult]:
    """Run multiple sub-agent tasks in parallel.

    Args:
        tasks: List of task descriptions.
        llm_config: LLM configuration.
        project_root: Project root directory.
        max_workers: Maximum parallel workers.
        on_progress: Optional callback for progress updates.

    Returns:
        List of TaskResults in the same order as input tasks.
    """
    results = [None] * len(tasks)

    collector = TaskProgressCollector()
    if on_progress:
        collector.add_callback(on_progress)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {}

        for idx, task in enumerate(tasks):
            agent = SubAgent(
                llm_config,
                project_root,
                task_id=f"parallel_{idx}",
                on_progress=collector.handle_progress,
            )
            future = executor.submit(agent.run, task)
            future_to_idx[future] = idx

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


def format_task_progress(progress: TaskProgress) -> str:
    """Format a single progress update for display."""
    emoji = {
        "starting": "🚀",
        "thinking": "🧠",
        "tool_call": "🔧",
        "completed": "✅",
        "error": "❌",
    }.get(progress.status, "⏳")

    return f"{emoji} **[{progress.task_id}]** {progress.message}"

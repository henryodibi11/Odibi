"""ImprovementBrain: LLM-powered code improvement within sandboxes.

The brain is responsible for:
1. Analyzing gate failures (tests, lint, golden projects)
2. Proposing fixes using LLM
3. Applying fixes to sandbox files
4. Re-running gates to verify fixes

Uses bounded attempts to prevent infinite loops.
Now with real-time event streaming for transparent UI feedback.
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from odibi.agents.improve.events import (
    BrainEventType,
    EventCallback,
    EventEmitter,
)
from odibi.agents.improve.results import GateCheckResult
from odibi.agents.improve.sandbox import SandboxInfo
from odibi.agents.ui.llm_client import LLMClient, LLMConfig, LLMError

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are an autonomous senior software engineer working in a sandbox to fix code issues.

## OBJECTIVE
Make all promotion gates pass: tests, lint (ruff), and golden project validation.

## CONSTRAINTS
- Prefer the SMALLEST, SAFEST change that fixes the issue
- Do NOT remove or weaken tests
- Keep code style consistent with existing patterns
- Never introduce new dependencies without necessity
- Focus on one issue at a time

## AVAILABLE TOOLS
You have access to file tools to inspect and modify code:
- read_file: Read file contents with line numbers
- write_file: Write/update file contents
- list_directory: List directory contents

**IMPORTANT:** Always use forward slashes (/) in file paths, e.g., `odibi/_test_lint_error.py`

## WORKFLOW
1. Analyze the failure details carefully
2. Use read_file to understand the code context
3. Identify the root cause
4. Use write_file to apply a minimal fix
5. Explain what you fixed and why

When you're done making changes, respond with "DONE" to signal completion.
If you cannot fix the issue after inspecting the code, respond with "CANNOT_FIX: <reason>".
"""


def _build_failure_prompt(
    gate_result: GateCheckResult,
    avoid_issues: list[str],
    campaign_goal: str,
) -> str:
    """Build user prompt with failure details."""
    lines = [
        f"## Campaign Goal\n{campaign_goal}\n",
        "## Gate Failures to Fix\n",
    ]

    if not gate_result.tests_passed and gate_result.test_result:
        test_r = gate_result.test_result
        lines.append(f"### Tests FAILED ({test_r.failed} failed, {test_r.errors} errors)\n")
        if test_r.failed_tests:
            lines.append("Failed tests:\n")
            for t in test_r.failed_tests[:10]:
                lines.append(f"- {t}\n")
        if test_r.output:
            truncated = test_r.output[:3000]
            lines.append(f"\nTest output (truncated):\n```\n{truncated}\n```\n")

    if not gate_result.lint_passed and gate_result.lint_result:
        lint_r = gate_result.lint_result
        lines.append(f"### Lint FAILED ({lint_r.error_count} errors)\n")
        if lint_r.errors:
            lines.append("Lint errors:\n")
            for e in lint_r.errors[:20]:
                # Normalize Windows paths to forward slashes
                e_normalized = e.replace("\\", "/")
                lines.append(f"- {e_normalized}\n")
        if lint_r.output:
            truncated = lint_r.output[:2000].replace("\\", "/")
            lines.append(f"\nLint output:\n```\n{truncated}\n```\n")

    if not gate_result.golden_passed and gate_result.golden_results:
        failed = [g for g in gate_result.golden_results if not g.passed]
        lines.append(f"### Golden Projects FAILED ({len(failed)} failed)\n")
        for g in failed[:5]:
            lines.append(f"- {g.config_name}: {g.error_message or 'Failed'}\n")

    if avoid_issues:
        lines.append("\n## AVOID These Approaches (already tried and failed)\n")
        for issue in avoid_issues[:10]:
            lines.append(f"- {issue}\n")

    lines.append(
        "\n## Instructions\n"
        "Inspect the relevant files, identify the root cause, and apply a minimal fix.\n"
        "Use the tools provided. When done, say 'DONE'.\n"
    )

    return "".join(lines)


@dataclass
class BrainToolDefs:
    """Tool definitions for the improvement brain."""

    @staticmethod
    def get_tools() -> list[dict]:
        """Get tool definitions for LLM function calling."""
        return [
            {
                "type": "function",
                "function": {
                    "name": "read_file",
                    "description": "Read contents of a file in the sandbox",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "path": {
                                "type": "string",
                                "description": "Relative path to the file within the sandbox",
                            },
                            "start_line": {
                                "type": "integer",
                                "description": "Starting line number (1-indexed)",
                                "default": 1,
                            },
                            "end_line": {
                                "type": "integer",
                                "description": "Ending line number (inclusive)",
                            },
                        },
                        "required": ["path"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "write_file",
                    "description": "Write content to a file in the sandbox",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "path": {
                                "type": "string",
                                "description": "Relative path to the file within the sandbox",
                            },
                            "content": {
                                "type": "string",
                                "description": "Complete file content to write",
                            },
                        },
                        "required": ["path", "content"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "list_directory",
                    "description": "List contents of a directory in the sandbox",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "path": {
                                "type": "string",
                                "description": "Relative path to the directory",
                                "default": ".",
                            },
                            "pattern": {
                                "type": "string",
                                "description": "Glob pattern to filter files",
                                "default": "*",
                            },
                        },
                        "required": [],
                    },
                },
            },
        ]


class SandboxFileTools:
    """File operations scoped to a sandbox directory."""

    def __init__(
        self,
        sandbox_path: Path,
        on_event: Optional[Callable[[BrainEventType], None]] = None,
    ):
        self.sandbox_path = sandbox_path.resolve()
        self._on_event = on_event or (lambda *args, **kwargs: None)

    def _resolve_path(self, rel_path: str) -> Path:
        """Resolve a relative path within the sandbox safely."""
        resolved = (self.sandbox_path / rel_path).resolve()
        try:
            resolved.relative_to(self.sandbox_path)
        except ValueError:
            raise ValueError(f"Path escapes sandbox: {rel_path}")
        return resolved

    def read_file(
        self,
        path: str,
        start_line: int = 1,
        end_line: Optional[int] = None,
        max_lines: int = 500,
    ) -> str:
        """Read a file from the sandbox."""
        self._on_event(BrainEventType.FILE_READ, path=path)
        try:
            file_path = self._resolve_path(path)
            if not file_path.exists():
                return f"Error: File not found: {path}"
            if not file_path.is_file():
                return f"Error: Not a file: {path}"

            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()

            total = len(lines)
            start_idx = max(0, start_line - 1)
            end_idx = min(total, end_line or (start_idx + max_lines))

            selected = lines[start_idx:end_idx]
            numbered = "".join(f"{i + start_idx + 1}: {line}" for i, line in enumerate(selected))

            header = f"File: {path} ({total} lines)\n\n"
            return header + numbered

        except Exception as e:
            return f"Error reading {path}: {e}"

    def write_file(self, path: str, content: str) -> str:
        """Write content to a file in the sandbox."""
        try:
            file_path = self._resolve_path(path)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            is_new = not file_path.exists()
            file_path.write_text(content, encoding="utf-8")

            self._on_event(BrainEventType.FILE_WRITE, path=path, is_new=is_new)

            status = "Created" if is_new else "Updated"
            return f"{status} {path} ({len(content)} bytes, {content.count(chr(10)) + 1} lines)"

        except Exception as e:
            return f"Error writing {path}: {e}"

    def list_directory(self, path: str = ".", pattern: str = "*") -> str:
        """List directory contents in the sandbox."""
        self._on_event(BrainEventType.FILE_LIST, path=path)
        try:
            dir_path = self._resolve_path(path)
            if not dir_path.exists():
                return f"Error: Directory not found: {path}"
            if not dir_path.is_dir():
                return f"Error: Not a directory: {path}"

            items = sorted(dir_path.glob(pattern))[:100]
            lines = []
            for item in items:
                rel = item.relative_to(self.sandbox_path)
                suffix = "/" if item.is_dir() else ""
                lines.append(f"{rel}{suffix}")

            return f"Directory: {path}\n\n" + "\n".join(lines)

        except Exception as e:
            return f"Error listing {path}: {e}"

    def execute_tool(self, tool_name: str, args: dict) -> str:
        """Execute a tool by name with given arguments."""
        if tool_name == "read_file":
            return self.read_file(
                path=args.get("path", ""),
                start_line=args.get("start_line", 1),
                end_line=args.get("end_line"),
            )
        elif tool_name == "write_file":
            return self.write_file(
                path=args.get("path", ""),
                content=args.get("content", ""),
            )
        elif tool_name == "list_directory":
            return self.list_directory(
                path=args.get("path", "."),
                pattern=args.get("pattern", "*"),
            )
        else:
            return f"Unknown tool: {tool_name}"


class ImprovementBrain:
    """LLM-powered code improvement within sandboxes.

    Uses an iterative loop:
    1. Analyze gate failures
    2. LLM proposes and applies fixes via tool calls
    3. Re-run gates
    4. Repeat until gates pass or max_attempts reached

    Now emits real-time events for UI streaming feedback.
    """

    def __init__(
        self,
        llm_client: LLMClient,
        max_attempts_per_sandbox: int = 3,
        event_emitter: Optional[EventEmitter] = None,
        on_event: Optional[EventCallback] = None,
        enable_streaming: bool = True,
    ):
        """Initialize the improvement brain.

        Args:
            llm_client: Provider-agnostic LLM client.
            max_attempts_per_sandbox: Maximum fix attempts before giving up.
            event_emitter: Optional EventEmitter for broadcasting events.
            on_event: Optional callback for individual events (simpler API).
            enable_streaming: Whether to use streaming LLM calls.
        """
        self._llm = llm_client
        self._max_attempts = max_attempts_per_sandbox
        self._emitter = event_emitter or EventEmitter()
        self._enable_streaming = enable_streaming

        if on_event:
            self._emitter.subscribe(on_event)

    def _emit(self, event_type: BrainEventType, **data) -> None:
        """Emit an event to all listeners."""
        self._emitter.emit_type(event_type, **data)

    @property
    def emitter(self) -> EventEmitter:
        """Get the event emitter for external subscriptions."""
        return self._emitter

    def improve_sandbox(
        self,
        sandbox: SandboxInfo,
        gate_result: GateCheckResult,
        avoid_issues: list[str],
        campaign_goal: str,
        run_gates: Callable[[SandboxInfo], GateCheckResult],
    ) -> tuple[GateCheckResult, int, list[str]]:
        """Try to fix gate failures using LLM.

        Inner loop:
        1. Build prompt from gate_result failures
        2. Call LLM with file tools
        3. LLM proposes and applies fixes
        4. Re-run gates
        5. Repeat until gates pass or max_attempts reached

        Args:
            sandbox: The sandbox to improve.
            gate_result: Initial gate check result with failures.
            avoid_issues: Known bad approaches to avoid.
            campaign_goal: High-level campaign objective.
            run_gates: Callback to re-run gate checks.

        Returns:
            Tuple of (final_gate_result, attempts_used, files_modified).
        """
        tools = SandboxFileTools(sandbox.sandbox_path, on_event=self._emit)
        tool_defs = BrainToolDefs.get_tools()

        current_result = gate_result
        attempts = 0
        all_files_modified: set[str] = set()

        self._emit(BrainEventType.IMPROVEMENT_START, max_attempts=self._max_attempts)

        for attempt in range(self._max_attempts):
            attempts = attempt + 1
            logger.info(f"Improvement attempt {attempts}/{self._max_attempts}")
            self._emit(
                BrainEventType.ATTEMPT_START,
                attempt=attempts,
                max=self._max_attempts,
            )

            if current_result.all_passed:
                logger.info("All gates passed, no improvement needed")
                self._emit(
                    BrainEventType.IMPROVEMENT_END,
                    attempts=attempts,
                    success=True,
                )
                return current_result, attempts, list(all_files_modified)

            try:
                modified_files = self._run_improvement_loop(
                    sandbox=sandbox,
                    gate_result=current_result,
                    avoid_issues=avoid_issues,
                    campaign_goal=campaign_goal,
                    tools=tools,
                    tool_defs=tool_defs,
                )
                all_files_modified.update(modified_files)

                self._emit(BrainEventType.ATTEMPT_END, files_modified=len(modified_files) > 0)

                if modified_files:
                    logger.info("LLM made changes, re-running gates")
                    self._emit(BrainEventType.GATES_START)
                    current_result = run_gates(sandbox)
                    self._emit(
                        BrainEventType.GATES_END,
                        passed=current_result.all_passed,
                    )

                    if current_result.all_passed:
                        logger.info(f"Gates passed after attempt {attempts}")
                        self._emit(
                            BrainEventType.IMPROVEMENT_END,
                            attempts=attempts,
                            success=True,
                        )
                        return current_result, attempts, list(all_files_modified)
                else:
                    logger.info("LLM made no changes, stopping")
                    break

            except LLMError as e:
                logger.error(f"LLM error during improvement: {e}")
                self._emit(BrainEventType.ATTEMPT_END, error=str(e))
                break
            except Exception as e:
                logger.error(f"Unexpected error during improvement: {e}")
                self._emit(BrainEventType.ATTEMPT_END, error=str(e))
                break

        self._emit(
            BrainEventType.IMPROVEMENT_END,
            attempts=attempts,
            success=current_result.all_passed,
        )
        return current_result, attempts, list(all_files_modified)

    def _run_improvement_loop(
        self,
        sandbox: SandboxInfo,
        gate_result: GateCheckResult,
        avoid_issues: list[str],
        campaign_goal: str,
        tools: SandboxFileTools,
        tool_defs: list[dict],
        max_iterations: int = 20,
    ) -> list[str]:
        """Run the LLM tool-calling loop until done.

        Returns list of file paths that were modified.
        Emits real-time events for UI streaming.
        """
        user_prompt = _build_failure_prompt(gate_result, avoid_issues, campaign_goal)
        messages: list[dict] = [{"role": "user", "content": user_prompt}]

        files_modified: list[str] = []
        accumulated_thinking = ""

        def on_content_chunk(chunk: str):
            nonlocal accumulated_thinking
            accumulated_thinking += chunk
            self._emit(BrainEventType.THINKING_CHUNK, chunk=chunk)

        def on_tool_start(tool_name: str):
            if tool_name:
                self._emit(BrainEventType.TOOL_CALL_START, tool_name=tool_name, args={})

        for iteration in range(max_iterations):
            logger.debug(f"LLM iteration {iteration + 1}/{max_iterations}")
            self._emit(
                BrainEventType.ITERATION_START,
                iteration=iteration + 1,
                max=max_iterations,
            )
            self._emit(BrainEventType.THINKING_START, iteration=iteration + 1)
            accumulated_thinking = ""

            if self._enable_streaming:
                response = self._llm.chat_stream_with_tools(
                    messages=messages,
                    system_prompt=SYSTEM_PROMPT,
                    temperature=0.1,
                    max_tokens=4096,
                    tools=tool_defs,
                    on_content=on_content_chunk,
                    on_tool_call_start=on_tool_start,
                )
            else:
                response = self._llm.chat(
                    messages=messages,
                    system_prompt=SYSTEM_PROMPT,
                    temperature=0.1,
                    max_tokens=4096,
                    tools=tool_defs,
                )

            self._emit(BrainEventType.THINKING_END)

            content = response.content
            tool_calls = response.tool_calls
            logger.debug(
                f"LLM response: content={content[:100] if content else None}... "
                f"tool_calls={len(tool_calls) if tool_calls else 0}"
            )

            self._emit(
                BrainEventType.LLM_RESPONSE,
                content=content[:200] if content else "",
                has_tool_calls=bool(tool_calls),
            )

            if content:
                if "DONE" in content.upper():
                    logger.info("LLM signaled completion")
                    self._emit(BrainEventType.ITERATION_END, iteration=iteration + 1)
                    return files_modified
                if "CANNOT_FIX" in content.upper():
                    logger.info(f"LLM cannot fix: {content}")
                    self._emit(BrainEventType.ITERATION_END, iteration=iteration + 1)
                    return files_modified

            if not tool_calls:
                if content:
                    messages.append({"role": "assistant", "content": content})
                self._emit(BrainEventType.ITERATION_END, iteration=iteration + 1)
                return files_modified

            messages.append(
                {
                    "role": "assistant",
                    "content": content,
                    "tool_calls": tool_calls,
                }
            )

            for tc in tool_calls:
                tool_name = tc["function"]["name"]
                try:
                    args = json.loads(tc["function"]["arguments"])
                except json.JSONDecodeError:
                    args = {}

                self._emit(BrainEventType.TOOL_CALL_START, tool_name=tool_name, args=args)
                logger.debug(f"Tool call: {tool_name}({args})")

                result = tools.execute_tool(tool_name, args)
                logger.debug(f"Tool result: {result[:200]}...")

                success = not result.startswith("Error")
                self._emit(
                    BrainEventType.TOOL_CALL_END,
                    tool_name=tool_name,
                    success=success,
                    result_preview=result[:100],
                )

                if tool_name == "write_file" and not result.startswith("Error"):
                    file_path = args.get("path", "unknown")
                    if file_path not in files_modified:
                        files_modified.append(file_path)

                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc["id"],
                        "content": result,
                    }
                )

        logger.warning(f"Reached max iterations ({max_iterations})")
        return files_modified


def create_improvement_brain(
    model: str = "gpt-4o-mini",
    endpoint: Optional[str] = None,
    api_key: Optional[str] = None,
    max_attempts: int = 3,
    on_event: Optional[EventCallback] = None,
    event_emitter: Optional[EventEmitter] = None,
    enable_streaming: bool = True,
) -> ImprovementBrain:
    """Factory function to create an ImprovementBrain.

    Args:
        model: LLM model name.
        endpoint: API endpoint (uses env vars if None).
        api_key: API key (uses env vars if None).
        max_attempts: Maximum fix attempts per sandbox.
        on_event: Optional callback for brain events (simple API).
        event_emitter: Optional shared event emitter.
        enable_streaming: Whether to use streaming LLM calls.

    Returns:
        Configured ImprovementBrain.
    """
    config = LLMConfig(
        endpoint=endpoint or "",
        api_key=api_key or "",
        model=model,
    )
    client = LLMClient(config)
    return ImprovementBrain(
        client,
        max_attempts,
        event_emitter=event_emitter,
        on_event=on_event,
        enable_streaming=enable_streaming,
    )

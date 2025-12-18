"""Enhanced chat interface with streaming, thinking panel, and activity feed.

This is the upgraded chat component that provides a smoother UX with:
- Token-by-token streaming
- Visible thinking/reasoning
- Activity feed for tool execution
- Collapsible tool results
- File link clickability
- Token/cost display
- Sub-agent progress visibility
"""

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Generator, Optional

import gradio as gr

from ..config import AgentUIConfig
from ..constants import get_tool_emoji
from ..llm_client import LLMClient, LLMConfig
from ..utils import (
    format_token_usage,
    linkify_file_paths,
)
from .streaming_display import format_tool_block
from ..tools.file_tools import (
    format_file_for_display,
    list_directory,
    read_file,
    write_file,
    format_write_result,
)
from ..tools.search_tools import (
    format_search_results,
    format_semantic_results,
    glob_files,
    grep_search,
    semantic_search,
)
from ..tools.shell_tools import (
    format_command_result,
    run_command,
    run_diagnostics,
    run_odibi_pipeline,
    run_pytest,
    run_ruff,
    run_typecheck,
)
from ..tools.web_tools import (
    format_search_results as format_web_results,
    format_web_page,
    read_web_page,
    web_search,
)
from ..tools.diagram_tools import (
    format_diagram,
    render_mermaid,
)
from ..tools.file_tools import undo_edit
from ..tools.git_tools import (
    format_diff,
    format_git_result,
    format_git_status,
    git_diff,
    git_log,
    git_status,
)
from ..tools.task_tools import (
    run_task,
    run_parallel_tasks,
    format_task_result,
    format_parallel_results,
    TaskProgress,
    TaskProgressCollector,
)
from ..tools.code_execution import (
    execute_python,
    execute_sql,
    list_tables,
    describe_table,
    format_execution_result,
)
from ..tools.explorer_tools import (
    run_explorer_experiment,
    format_experiment_result,
)
from ..tools.implement_tools import (
    implement_feature,
    format_implementation_result,
    find_tests,
    format_find_tests_result,
    code_review,
    format_code_review_result,
)
from ..tools.tool_definitions import TOOL_DEFINITIONS, TOOLS_REQUIRING_CONFIRMATION
from .todo_panel import todo_read, todo_write, update_todo_display
from .memories import get_memory_manager, format_memory_list
from .activity_feed import (
    add_activity,
    complete_activity,
    error_activity,
    clear_activity,
    refresh_activity_display,
)
from .thinking_panel import (
    start_thinking,
    update_thinking,
    complete_thinking,
    reset_thinking,
)
from odibi.agents.core.memory import MemoryType


AGENT_CHOICES = [
    ("🤖 Auto (Orchestrator)", "auto"),
    ("🔍 Code Analyst", "code_analyst"),
    ("🔧 Refactor Engineer", "refactor_engineer"),
    ("🧪 Test Architect", "test_architect"),
    ("📝 Documentation", "documentation"),
]


ENHANCED_CHAT_SYSTEM_PROMPT = """
You are a powerful AI coding assistant that works autonomously with any codebase.

You have access to tools for: file operations, code search, shell commands, web research,
task management, diagrams, git, code execution (Databricks), sub-agent spawning, and MEMORY.

## CRITICAL RULES - YOU MUST FOLLOW THESE

### Rule 1: ALWAYS USE TOOLS FOR FACTS
- **NEVER** describe what's in a directory without calling `list_directory` first
- **NEVER** describe file contents without calling `read_file` first
- **NEVER** claim to know what exists without verifying with tools
- **NEVER** make up fake file trees, contents, or search results
- If you don't know something, USE A TOOL to find out. DO NOT GUESS.

### Rule 2: AGENTIC LOOP - NEVER STOP MID-TASK
- You operate in an AGENTIC LOOP. After each tool execution, you receive results and MUST continue.
- **NEVER** say "I'll do X" and then stop - ACTUALLY DO IT with tool calls
- **NEVER** ask "would you like me to continue?" - just CONTINUE
- **NEVER** output partial results - COMPLETE the full task
- **NEVER** output raw JSON like `{"operation": ...}` - always use proper tool calls
- If you plan to call a tool, CALL IT immediately in the same turn

### Rule 3: PROACTIVE EXECUTION
- Use tools IMMEDIATELY without asking permission for read-only operations
- Call multiple tools in sequence if needed to fully answer the question
- After calling a tool and receiving results, CONTINUE REASONING about what to do next

## Memory System - USE PROACTIVELY

You have **remember** and **recall** tools for persistent memory across sessions:

### WHEN TO USE `remember`:
- When the user makes a design **decision** (e.g., "let's use SQLAlchemy")
- When you discover an important **learning** (e.g., "this function requires X")
- When you fix a **bug_fix** (e.g., "race condition fixed by adding lock")
- When user states a **preference** (e.g., "I prefer functional style")
- When identifying a **todo** for later
- After implementing a **feature** worth documenting

### WHEN TO USE `recall`:
- At the START of complex tasks to check for relevant past context
- When the user asks about something you might have discussed before
- When you need to maintain consistency with past decisions

**Memory Types:**
- `decision` - Design choices and rationale
- `learning` - Insights, patterns, discoveries
- `bug_fix` - Bug descriptions and solutions
- `preference` - User's coding style preferences
- `todo` - Tasks for later
- `feature` - Feature implementations
- `context` - Background information

**BE PROACTIVE:** If something seems important enough to remember, SAVE IT without asking!

## Semantic Search - USE FOR DISCOVERY

You have a **search** tool that uses AI embeddings to find code by meaning, not just keywords:

### WHEN TO USE `search` vs `grep`:
- **search** - Finding code by concept/behavior: "authentication logic", "database connections", "error handling"
- **grep** - Finding exact text matches: specific function names, variable names, error messages

### SEARCH FIRST STRATEGY:
1. When exploring unfamiliar code, START with `search` to discover relevant files
2. Use `search` when you don't know exact function/class names
3. After `search` finds relevant chunks, use `read_file` to see full context
4. Fall back to `grep` when you need exact string matches

**Example:** To find how validation works, use `search(query="input validation logic")` NOT `grep(pattern="valid")`.

## Sub-Agent Usage - USE ACTIVELY

You have access to **task** and **parallel_tasks** tools for spawning sub-agents:

- **task** - Spawn a focused sub-agent for a specific task
- **parallel_tasks** - Run multiple sub-agents in parallel for independent tasks

**WHEN TO USE SUB-AGENTS:**
1. When you need to explore multiple files or directories independently
2. When performing repetitive analysis across multiple components
3. When the user's task can be broken into independent subtasks
4. When you want to speed up work by parallelizing
5. When working on complex multi-step implementations

Be proactive about using sub-agents - they make you faster and more thorough!

## Response Format

- Use markdown formatting
- Code blocks with language hints
- Keep responses concise but informative
- When showing directory contents, show the ACTUAL results from tools

## Project Context

You are helping the user with their codebase. If it's an Odibi project (has project.yaml),
you can also help with pipelines, transformers, and connections.

## Odibi Framework Knowledge

You are an expert on the Odibi data engineering framework. Key knowledge:

**Architecture:**
- `odibi/engine/` - Spark, Pandas, Polars engines (must maintain parity)
- `odibi/patterns/` - SCD2, Merge, Aggregation, Dimension, Fact patterns
- `odibi/validation/` - Data quality engine, quarantine, FK validation
- `odibi/transformers/` - SCD, merge, delete detection, advanced transforms
- `odibi/semantics/` - Metrics, dimensions, materialization

**Testing:**
- Run tests: `pytest tests/ -v`
- Run specific: `pytest tests/unit/test_X.py -v`
- Check types: `ruff check .`

**Improvement Workflow:**
1. Understand existing patterns by reading similar code
2. Implement with tests
3. Run `pytest` to verify
4. Run `ruff check .` for linting
5. Maintain engine parity (if Pandas has it, Spark/Polars should too)

**Key Patterns to Follow:**
- Use `get_logging_context()` for structured logging
- Use Pydantic models for config validation
- Support all three engines where possible
- Add tests in `tests/unit/` matching source structure
"""


@dataclass
class ChatState:
    """State for the enhanced chat handler."""

    conversation_history: list[dict] = field(default_factory=list)
    pending_action: Optional[dict] = None
    stop_requested: bool = False
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    session_start: datetime = field(default_factory=datetime.now)
    current_model: str = ""


class EnhancedChatHandler:
    """Enhanced chat handler with streaming and activity tracking."""

    def __init__(self, config: AgentUIConfig):
        self.config = config
        self.state = ChatState()
        self._client: Optional[LLMClient] = None
        self._task_collector = TaskProgressCollector()
        self._conversation_id: Optional[str] = None

    def request_stop(self) -> None:
        """Request the agent loop to stop."""
        self.state.stop_requested = True
        if self._client:
            self._client.cancel()

    def reset_stop(self) -> None:
        """Reset the stop flag."""
        self.state.stop_requested = False

    @property
    def should_stop(self) -> bool:
        """Check if stop was requested."""
        return self.state.stop_requested

    def load_history(self, messages: list[dict[str, str]]) -> None:
        """Load conversation history for LLM context.

        Call this when loading a saved conversation so the LLM
        has context of previous messages.

        Args:
            messages: List of message dicts with 'role' and 'content'.
        """
        self.state.conversation_history = messages.copy() if messages else []

    def _sanitize_conversation_history(self) -> None:
        """Remove corrupted tool_calls that lack corresponding tool responses.

        The OpenAI API requires every assistant message with tool_calls to be
        followed by tool messages for each tool_call_id. This method detects
        and removes any such orphaned tool_calls.
        """
        history = self.state.conversation_history
        if not history:
            return

        tool_call_ids_needed = set()
        tool_call_ids_found = set()

        for msg in history:
            if msg.get("role") == "assistant" and msg.get("tool_calls"):
                for tc in msg["tool_calls"]:
                    tool_call_ids_needed.add(tc.get("id"))
            elif msg.get("role") == "tool":
                tool_call_ids_found.add(msg.get("tool_call_id"))

        orphaned_ids = tool_call_ids_needed - tool_call_ids_found
        if not orphaned_ids:
            return

        cleaned = []
        for msg in history:
            if msg.get("role") == "assistant" and msg.get("tool_calls"):
                has_orphan = any(tc.get("id") in orphaned_ids for tc in msg["tool_calls"])
                if has_orphan:
                    if msg.get("content"):
                        cleaned.append({"role": "assistant", "content": msg["content"]})
                    continue
            cleaned.append(msg)

        self.state.conversation_history = cleaned

    def get_llm_client(self) -> LLMClient:
        """Get the LLM client from current config."""
        llm_config = LLMConfig(
            endpoint=self.config.llm.endpoint,
            api_key=self.config.llm.api_key,
            model=self.config.llm.model,
            api_type=self.config.llm.api_type,
            api_version=self.config.llm.api_version,
        )
        self._client = LLMClient(llm_config)
        self.state.current_model = self.config.llm.model
        return self._client

    def _auto_save_conversation(self, history: list[dict]) -> None:
        """Auto-save conversation to the configured backend.

        Saves after each complete exchange (user message + assistant response).
        Uses the same backend as the memory system (local, ADLS, or Delta).
        """
        if not history or len(history) < 2:
            return

        try:
            from .conversation import get_conversation_store

            # Pass config to use the same backend as memory system
            store = get_conversation_store(self.config)
            project_path = self.config.project.project_root if self.config else None

            if self._conversation_id:
                # Update existing conversation
                conv = store.get(self._conversation_id)
                if conv:
                    conv.messages = history
                    store.save(conv)
            else:
                # Create new conversation
                conv = store.create(
                    messages=history,
                    project_path=project_path,
                )
                self._conversation_id = conv.id
        except Exception as e:
            # Silently fail - don't interrupt chat for save errors
            import logging

            logging.getLogger(__name__).debug("Auto-save failed: %s", e)

    def _on_task_progress(self, progress: TaskProgress):
        """Handle progress updates from sub-agents."""
        add_activity(
            f"[{progress.task_id}] {progress.message}",
            tool_name=progress.tool_name,
        )

    def execute_tool(self, tool_call: dict) -> str:
        """Execute a tool call and return formatted result."""
        tool_name = tool_call.get("tool", "")
        args = tool_call.get("args", {})

        activity_id = add_activity(f"Executing {tool_name}...", tool_name=tool_name)

        try:
            result = self._execute_tool_internal(tool_name, args)
            complete_activity(activity_id)
            return result
        except Exception as e:
            error_activity(activity_id, str(e))
            return f"❌ Error executing {tool_name}: {str(e)}"

    def _execute_tool_internal(self, tool_name: str, args: dict) -> str:
        """Internal tool execution logic."""
        if tool_name == "read_file":
            result = read_file(
                path=args.get("path", ""),
                start_line=args.get("start_line", 1),
                end_line=args.get("end_line"),
            )
            content = format_file_for_display(result)
            return linkify_file_paths(content)

        elif tool_name == "write_file":
            path = args.get("path", "")
            content = args.get("content", "")

            # Log what we're writing for debugging
            content_len = len(content)
            line_count = content.count("\n") + 1 if content else 0
            print(f"[write_file] Path: {path}")
            print(f"[write_file] Content: {content_len} chars, {line_count} lines")
            print(f"[write_file] Args keys: {list(args.keys())}")

            if not content:
                return "❌ **Error:** No content provided to write_file. The content parameter is empty."

            # Check for truncation indicators
            truncation_markers = [
                "// ... rest of",
                "# ... rest of",
                "// ...",
                "# ...",
                "/* ... */",
                "... (remaining",
                "... rest of file",
                "[truncated]",
                "// TODO: add remaining",
            ]
            for marker in truncation_markers:
                if marker.lower() in content.lower():
                    return (
                        f"❌ **Error:** Content appears to be truncated (found '{marker}'). "
                        f"You must provide the COMPLETE file content, not a summary. "
                        f"Read the original file first and include ALL content."
                    )

            result = write_file(path=path, content=content)
            return format_write_result(result, show_diff=True)

        elif tool_name == "list_directory":
            result = list_directory(
                path=args.get("path", "."),
                pattern=args.get("pattern", "*"),
                recursive=args.get("recursive", False),
            )
            if result.success:
                return f"**Directory:** `{result.path}`\n\n```\n{result.content}\n```"
            return f"❌ Error: {result.error}"

        elif tool_name == "grep":
            result = grep_search(
                pattern=args.get("pattern", ""),
                path=args.get("path", "."),
                file_pattern=args.get("file_pattern", "*.py"),
                is_regex=args.get("is_regex", False),
            )
            return linkify_file_paths(format_search_results(result))

        elif tool_name == "glob":
            result = glob_files(
                pattern=args.get("pattern", "**/*"),
                path=args.get("path", "."),
            )
            return linkify_file_paths(format_search_results(result))

        elif tool_name == "search":
            result = semantic_search(
                query=args.get("query", ""),
                project_root=self.config.project.project_root,
                k=args.get("k", 5),
                chunk_type=args.get("chunk_type"),
            )
            return linkify_file_paths(format_semantic_results(result))

        elif tool_name == "run_command":
            result = run_command(
                command=args.get("command", ""),
                working_dir=args.get("working_dir", self.config.project.project_root),
            )
            return format_command_result(result)

        elif tool_name == "pytest":
            result = run_pytest(
                test_path=args.get("test_path"),
                working_dir=args.get("working_dir", self.config.project.project_root),
                verbose=args.get("verbose", True),
                markers=args.get("markers"),
            )
            return format_command_result(result)

        elif tool_name == "ruff":
            result = run_ruff(
                path=args.get("path", "."),
                working_dir=args.get("working_dir", self.config.project.project_root),
                fix=args.get("fix", False),
            )
            return format_command_result(result)

        elif tool_name == "odibi_run":
            result = run_odibi_pipeline(
                pipeline_path=args.get("pipeline_path", ""),
                working_dir=args.get("working_dir", self.config.project.project_root),
                dry_run=args.get("dry_run", True),
                engine=args.get("engine", "pandas"),
            )
            return format_command_result(result)

        elif tool_name == "undo_edit":
            result = undo_edit(path=args.get("path", ""))
            if result.success:
                return f"✅ {result.content}"
            return f"❌ Error: {result.error}"

        elif tool_name == "diagnostics":
            result = run_diagnostics(
                path=args.get("path", "."),
                working_dir=self.config.project.project_root,
                include_ruff=args.get("include_ruff", True),
                include_mypy=args.get("include_mypy", False),
                include_pytest=args.get("include_pytest", False),
            )
            return format_command_result(result)

        elif tool_name == "typecheck":
            result = run_typecheck(
                path=args.get("path", "."),
                working_dir=self.config.project.project_root,
            )
            return format_command_result(result)

        elif tool_name == "web_search":
            result = web_search(
                query=args.get("query", ""),
                max_results=args.get("max_results", 5),
            )
            return format_web_results(result)

        elif tool_name == "read_web_page":
            result = read_web_page(
                url=args.get("url", ""),
            )
            return format_web_page(result)

        elif tool_name == "todo_write":
            todos = args.get("todos", [])
            todo_write(todos)
            return f"✅ Updated {len(todos)} tasks\n\n{update_todo_display()}"

        elif tool_name == "todo_read":
            todos = todo_read()
            return f"**Current Tasks:**\n\n{update_todo_display()}"

        elif tool_name == "mermaid":
            code = args.get("code", "")
            result = render_mermaid(code)
            return format_diagram(result)

        elif tool_name == "git_status":
            result = git_status(
                working_dir=self.config.project.project_root,
            )
            return format_git_status(result)

        elif tool_name == "git_diff":
            result = git_diff(
                working_dir=self.config.project.project_root,
                path=args.get("path"),
                staged=args.get("staged", False),
            )
            return format_diff(result)

        elif tool_name == "git_log":
            result = git_log(
                working_dir=self.config.project.project_root,
                max_count=args.get("max_count", 10),
                path=args.get("path"),
            )
            return format_git_result(result)

        elif tool_name == "task":
            llm_config = LLMConfig(
                endpoint=self.config.llm.endpoint,
                api_key=self.config.llm.api_key,
                model=self.config.llm.model,
                api_type=self.config.llm.api_type,
                api_version=self.config.llm.api_version,
            )
            result = run_task(
                task=args.get("prompt", ""),
                llm_config=llm_config,
                project_root=self.config.project.project_root,
                on_progress=self._on_task_progress,
            )
            return format_task_result(result)

        elif tool_name == "parallel_tasks":
            llm_config = LLMConfig(
                endpoint=self.config.llm.endpoint,
                api_key=self.config.llm.api_key,
                model=self.config.llm.model,
                api_type=self.config.llm.api_type,
                api_version=self.config.llm.api_version,
            )
            tasks = args.get("tasks", [])
            results = run_parallel_tasks(
                tasks=tasks,
                llm_config=llm_config,
                project_root=self.config.project.project_root,
                max_workers=min(3, len(tasks)),
                on_progress=self._on_task_progress,
            )
            return format_parallel_results(results)

        elif tool_name == "execute_python":
            result = execute_python(
                code=args.get("code", ""),
                timeout=args.get("timeout", 30),
            )
            return format_execution_result(result)

        elif tool_name == "sql":
            result = execute_sql(
                query=args.get("query", ""),
                limit=args.get("limit", 100),
                show_schema=args.get("show_schema", False),
            )
            return format_execution_result(result)

        elif tool_name == "list_tables":
            result = list_tables(
                database=args.get("database"),
                pattern=args.get("pattern"),
            )
            return format_execution_result(result)

        elif tool_name == "describe_table":
            result = describe_table(
                table_name=args.get("table_name", ""),
            )
            return format_execution_result(result)

        elif tool_name == "remember":
            try:
                manager = get_memory_manager(self.config)
                memory_type_str = args.get("memory_type", "learning")
                memory_type = MemoryType(memory_type_str)
                tags = args.get("tags", [])
                if isinstance(tags, str):
                    tags = [t.strip() for t in tags.split(",") if t.strip()]

                manager.remember(
                    memory_type=memory_type,
                    summary=args.get("summary", ""),
                    content=args.get("content", args.get("summary", "")),
                    tags=tags,
                    importance=args.get("importance", 0.7),
                )
                from ..constants import get_memory_emoji

                emoji = get_memory_emoji(memory_type_str)
                return f"✅ Memory saved: {emoji} **{memory_type_str.upper()}**\n\n> {args.get('summary', '')}"
            except Exception as e:
                return f"❌ Failed to save memory: {str(e)}"

        elif tool_name == "recall":
            try:
                manager = get_memory_manager(self.config)
                query = args.get("query", "")
                memory_types = args.get("memory_types")
                limit = args.get("limit", 10)

                if memory_types:
                    memory_types = [MemoryType(t) for t in memory_types]

                memories = manager.recall(query, memory_types=memory_types, top_k=limit)
                if not memories:
                    return f"📭 No memories found for: '{query}'"
                return (
                    f"**🧠 Recalled Memories ({len(memories)}):**\n\n{format_memory_list(memories)}"
                )
            except Exception as e:
                return f"❌ Failed to recall memories: {str(e)}"

        elif tool_name == "explorer_experiment":
            result = run_explorer_experiment(
                source_path=args.get("source_path", ""),
                experiment_name=args.get("experiment_name", "experiment"),
                commands=args.get("commands", []),
                description=args.get("description", ""),
                hypothesis=args.get("hypothesis", ""),
                timeout=args.get("timeout", 120),
            )
            return format_experiment_result(result)

        elif tool_name == "implement_feature":
            subtask_default = getattr(self.config.agent, "subtask_max_iterations", 3)
            result = implement_feature(
                description=args.get("description", ""),
                target_files=args.get("target_files", []),
                test_pattern=args.get("test_pattern"),
                max_iterations=args.get("max_iterations", subtask_default),
                project_root=self.config.project.project_root,
            )
            return format_implementation_result(result)

        elif tool_name == "find_tests":
            result = find_tests(
                source_file=args.get("source_file", ""),
                project_root=self.config.project.project_root,
            )
            return format_find_tests_result(result)

        elif tool_name == "code_review":
            result = code_review(
                project_root=self.config.project.project_root,
                path=args.get("path"),
                staged_only=args.get("staged_only", False),
            )
            return format_code_review_result(result)

        return f"❓ Unknown tool: {tool_name}"

    def requires_confirmation(self, tool_name: str, args: dict = None) -> bool:
        """Check if a tool call requires user confirmation.

        Set ODIBI_AUTO_APPROVE=1 to skip all confirmations.
        """
        import os

        if os.environ.get("ODIBI_AUTO_APPROVE", "").lower() in ("1", "true", "yes"):
            return False
        if tool_name in TOOLS_REQUIRING_CONFIRMATION:
            if tool_name == "ruff":
                return args.get("fix", False) if args else False
            return True
        return False

    def _format_token_display(self) -> str:
        """Format the token usage display."""
        return format_token_usage(
            self.state.current_model,
            self.state.total_input_tokens,
            self.state.total_output_tokens,
        )

    def process_message_streaming(
        self,
        message: str,
        history: list[dict],
        agent: str,
        max_iterations: int = 50,
    ) -> Generator[tuple[list[dict], str, str, str, Any, bool], None, None]:
        """Process a message with streaming support.

        Yields:
            Tuple of (history, status, thinking, activity, pending_action, show_actions).
        """
        history = history.copy()
        history.append({"role": "user", "content": message})

        clear_activity()
        reset_thinking()
        self.reset_stop()

        yield history, "🚀 Starting...", "", refresh_activity_display(), None, False

        self.state.conversation_history.append({"role": "user", "content": message})
        add_activity("Received user message")

        # Auto-recall relevant memories for context
        memory_context = ""
        try:
            manager = get_memory_manager(self.config)
            memories = manager.recall(message, top_k=3)
            if memories:
                memory_context = "\n\n## Relevant Memories\n"
                for mem in memories:
                    memory_context += f"- [{mem.memory_type.value}] {mem.summary}\n"
                add_activity(f"Recalled {len(memories)} relevant memories")
        except Exception:
            pass  # Memory recall is optional

        try:
            client = self.get_llm_client()

            system_prompt = ENHANCED_CHAT_SYSTEM_PROMPT
            system_prompt += "\n\n## Accessible Paths"
            system_prompt += f"\n**Working Project:** {self.config.project.project_root}"
            if self.config.project.reference_repos:
                system_prompt += "\n**Reference Repos:**"
                for repo in self.config.project.reference_repos:
                    system_prompt += f"\n- {repo}"

            # Check if semantic search index is available
            try:
                from odibi.agents.ui.components.folder_picker import (
                    get_registered_index_dir,
                    find_existing_index_dir,
                )

                index_dir = get_registered_index_dir(
                    self.config.project.project_root
                ) or find_existing_index_dir(self.config.project.project_root)
                if index_dir:
                    system_prompt += (
                        "\n\n**Semantic Search:** ✅ Index available - "
                        "use `search` tool for concept-based code discovery!"
                    )
                else:
                    system_prompt += (
                        "\n\n**Semantic Search:** ❌ No index - "
                        "use `grep` for text search. Index via folder picker to enable."
                    )
            except Exception:
                pass

            if memory_context:
                system_prompt += memory_context

            for iteration in range(max_iterations):
                if self.should_stop:
                    history.append({"role": "assistant", "content": "⏹️ Stopped by user."})
                    yield history, "", complete_thinking(), refresh_activity_display(), None, False
                    return

                start_time = time.time()
                thinking_text = start_thinking("thinking")
                add_activity(f"Thinking (step {iteration + 1})...")

                yield (
                    history,
                    f"🧠 Thinking (step {iteration + 1})...",
                    thinking_text,
                    refresh_activity_display(),
                    None,
                    False,
                )

                model_lower = self.config.llm.model.lower()
                no_tools_support = model_lower in ("o1-preview", "o1-mini", "o1")

                accumulated_content = ""
                current_assistant_msg = {"role": "assistant", "content": ""}
                history.append(current_assistant_msg)

                def on_content(chunk: str):
                    nonlocal accumulated_content
                    accumulated_content += chunk

                def on_tool_start(name: str):
                    add_activity(f"Preparing to call {name}...", tool_name=name)

                # Sanitize history before each LLM call to remove orphaned tool_calls
                self._sanitize_conversation_history()

                response = client.chat_stream_with_tools(
                    messages=self.state.conversation_history,
                    system_prompt=system_prompt,
                    temperature=0.1,
                    tools=TOOL_DEFINITIONS if not no_tools_support else None,
                    on_content=on_content,
                    on_tool_call_start=on_tool_start,
                )

                elapsed = time.time() - start_time
                thinking_update = update_thinking(f"Completed in {elapsed:.1f}s", append=False)

                if response.usage:
                    self.state.total_input_tokens += response.usage.input_tokens
                    self.state.total_output_tokens += response.usage.output_tokens

                content = response.content
                tool_calls = response.tool_calls

                if content:
                    content = linkify_file_paths(content)
                    history[-1]["content"] = content
                    yield (
                        history,
                        self._format_token_display(),
                        thinking_update,
                        refresh_activity_display(),
                        None,
                        False,
                    )

                if not tool_calls:
                    if content:
                        self.state.conversation_history.append(
                            {"role": "assistant", "content": content}
                        )
                    complete_thinking()
                    complete_activity(add_activity("Response complete"))
                    # Auto-save conversation after each complete exchange
                    self._auto_save_conversation(history)
                    yield (
                        history,
                        self._format_token_display(),
                        complete_thinking(),
                        refresh_activity_display(),
                        None,
                        False,
                    )
                    return

                for tc in tool_calls:
                    tool_name = tc["function"]["name"]
                    tool_id = tc["id"]
                    raw_args = tc["function"].get("arguments", "")

                    try:
                        args = json.loads(raw_args) if raw_args else {}
                    except json.JSONDecodeError as e:
                        # Log the parsing failure
                        print(f"[ERROR] Failed to parse tool args for {tool_name}: {e}")
                        print(f"[ERROR] Raw arguments ({len(raw_args)} chars): {raw_args[:500]}...")
                        args = {}
                        # Add error to history so LLM knows parsing failed
                        error_msg = (
                            f"❌ **Tool call failed:** Could not parse arguments for `{tool_name}`. "
                            f"The JSON was malformed or truncated ({len(raw_args)} chars received). "
                            f"Error: {e}"
                        )
                        history.append({"role": "assistant", "content": error_msg})
                        yield (
                            history,
                            "❌ Tool call parsing failed",
                            thinking_update,
                            refresh_activity_display(),
                            None,
                            False,
                        )
                        continue

                    if self.requires_confirmation(tool_name, args):
                        self.state.pending_action = {
                            "tool": tool_name,
                            "args": args,
                            "tool_call_id": tool_id,
                            "tool_calls": tool_calls,
                            "content": content,
                        }
                        args_json = json.dumps(args, indent=2)
                        action_desc = (
                            f"**Pending Action:** `{tool_name}`\n```json\n{args_json}\n```"
                        )
                        history.append({"role": "assistant", "content": action_desc})
                        yield (
                            history,
                            "⏳ Awaiting confirmation...",
                            thinking_update,
                            refresh_activity_display(),
                            self.state.pending_action,
                            True,
                        )
                        return

                self.state.conversation_history.append(
                    {
                        "role": "assistant",
                        "content": content,
                        "tool_calls": tool_calls,
                    }
                )

                tool_results = []

                for tc in tool_calls:
                    tool_name = tc["function"]["name"]
                    tool_id = tc["id"]
                    emoji = get_tool_emoji(tool_name)

                    try:
                        args = json.loads(tc["function"]["arguments"])
                    except json.JSONDecodeError:
                        args = {}

                    running_block = format_tool_block(
                        tool_name=tool_name,
                        emoji=emoji,
                        status="running",
                        command=args.get("command", args.get("path", args.get("query", ""))),
                    )
                    history.append({"role": "assistant", "content": running_block})
                    live_msg_idx = len(history) - 1

                    yield (
                        history,
                        f"{emoji} Running `{tool_name}`...",
                        thinking_update,
                        refresh_activity_display(),
                        None,
                        False,
                    )

                    tool_start = time.time()
                    result = self.execute_tool({"tool": tool_name, "args": args})
                    tool_elapsed = time.time() - tool_start

                    tool_results.append(
                        {
                            "tool_call_id": tool_id,
                            "tool": tool_name,
                            "result": result,
                        }
                    )

                    completed_block = format_tool_block(
                        tool_name=tool_name,
                        emoji=emoji,
                        status="success",
                        elapsed_seconds=tool_elapsed,
                        exit_code=0,
                        command=args.get("command", args.get("path", args.get("query", ""))),
                        stdout=result,
                    )

                    history[live_msg_idx]["content"] = completed_block

                for tr in tool_results:
                    self.state.conversation_history.append(
                        {
                            "role": "tool",
                            "tool_call_id": tr["tool_call_id"],
                            "content": tr["result"],
                        }
                    )

                yield (
                    history,
                    "🔄 Processing results...",
                    thinking_update,
                    refresh_activity_display(),
                    None,
                    False,
                )

            history.append(
                {
                    "role": "assistant",
                    "content": f"⚠️ Reached maximum iterations ({max_iterations}). Stopping.",
                }
            )
            yield (
                history,
                self._format_token_display(),
                complete_thinking(),
                refresh_activity_display(),
                None,
                False,
            )

        except Exception as e:
            error_msg = f"❌ Error: {str(e)}"
            history.append({"role": "assistant", "content": error_msg})
            error_activity(add_activity("Error occurred"), str(e))
            yield (
                history,
                error_msg,
                complete_thinking(),
                refresh_activity_display(),
                None,
                False,
            )

    def clear_history(self) -> tuple[list, str, str, str]:
        """Clear conversation history."""
        self.state.conversation_history = []
        self.state.pending_action = None
        self.state.total_input_tokens = 0
        self.state.total_output_tokens = 0
        self._conversation_id = None  # Reset for new conversation
        clear_activity()
        reset_thinking()
        return [], "", "", "_No activity yet_"

    def confirm_action(
        self,
        history: list[dict],
        max_iterations: int = 50,
    ) -> Generator[tuple[list[dict], str, Any, bool], None, None]:
        """Execute the pending action and continue the agent loop."""
        if not self.state.pending_action:
            yield history, "", None, False
            return

        try:
            tool_call = self.state.pending_action
            self.state.pending_action = None

            original_tool_calls = tool_call.get("tool_calls", [])
            original_content = tool_call.get("content")

            self.state.conversation_history.append(
                {
                    "role": "assistant",
                    "content": original_content,
                    "tool_calls": original_tool_calls,
                }
            )

            tool_results = []

            for tc in original_tool_calls:
                tc_name = tc["function"]["name"]
                tc_id = tc["id"]
                try:
                    tc_args = json.loads(tc["function"]["arguments"])
                except json.JSONDecodeError:
                    tc_args = {}

                tool_emoji = get_tool_emoji(tc_name)
                yield history, f"{tool_emoji} Executing `{tc_name}`...", None, False

                result = self.execute_tool({"tool": tc_name, "args": tc_args})
                tool_results.append(
                    {
                        "tool_call_id": tc_id,
                        "tool": tc_name,
                        "result": result,
                    }
                )
                history.append(
                    {"role": "assistant", "content": f"**{tool_emoji} {tc_name}:**\n{result}"}
                )

            for tr in tool_results:
                self.state.conversation_history.append(
                    {
                        "role": "tool",
                        "tool_call_id": tr["tool_call_id"],
                        "content": tr["result"],
                    }
                )

            yield history, "🔄 Continuing...", None, False

            client = self.get_llm_client()
            system_prompt = self._get_system_prompt()

            for iteration in range(max_iterations):
                if self.should_stop:
                    history.append({"role": "assistant", "content": "⏹️ Stopped by user."})
                    yield history, "", None, False
                    return

                yield history, f"🤔 Thinking... (step {iteration + 1})", None, False

                response = client.chat(
                    messages=[{"role": "system", "content": system_prompt}]
                    + self.state.conversation_history,
                    tools=TOOL_DEFINITIONS,
                )

                self.state.total_input_tokens += response.usage.input_tokens
                self.state.total_output_tokens += response.usage.output_tokens

                content = response.content
                tool_calls = response.tool_calls

                if content and not tool_calls:
                    self.state.conversation_history.append(
                        {"role": "assistant", "content": content}
                    )
                    history.append({"role": "assistant", "content": linkify_file_paths(content)})
                    yield history, self._format_token_display(), None, False
                    return

                if not tool_calls:
                    yield history, self._format_token_display(), None, False
                    return

                for tc in tool_calls:
                    tool_name = tc["function"]["name"]
                    tool_id = tc["id"]
                    try:
                        args = json.loads(tc["function"]["arguments"])
                    except json.JSONDecodeError:
                        args = {}

                    if self.requires_confirmation(tool_name, args):
                        self.state.pending_action = {
                            "tool": tool_name,
                            "args": args,
                            "tool_call_id": tool_id,
                            "tool_calls": tool_calls,
                            "content": content,
                        }
                        args_json = json.dumps(args, indent=2)
                        action_desc = (
                            f"**Pending Action:** `{tool_name}`\n```json\n{args_json}\n```"
                        )
                        history.append({"role": "assistant", "content": action_desc})
                        yield (
                            history,
                            "⏳ Awaiting confirmation...",
                            self.state.pending_action,
                            True,
                        )
                        return

                self.state.conversation_history.append(
                    {
                        "role": "assistant",
                        "content": content,
                        "tool_calls": tool_calls,
                    }
                )

                for tc in tool_calls:
                    tc_name = tc["function"]["name"]
                    tc_id = tc["id"]
                    try:
                        tc_args = json.loads(tc["function"]["arguments"])
                    except json.JSONDecodeError:
                        tc_args = {}

                    tool_emoji = get_tool_emoji(tc_name)
                    yield history, f"{tool_emoji} Running `{tc_name}`...", None, False

                    result = self.execute_tool({"tool": tc_name, "args": tc_args})
                    history.append(
                        {"role": "assistant", "content": f"**{tool_emoji} {tc_name}:**\n{result}"}
                    )
                    self.state.conversation_history.append(
                        {
                            "role": "tool",
                            "tool_call_id": tc_id,
                            "content": result,
                        }
                    )

            history.append(
                {
                    "role": "assistant",
                    "content": f"⚠️ Reached maximum iterations ({max_iterations}).",
                }
            )
            yield history, self._format_token_display(), None, False

        except Exception as e:
            error_msg = f"❌ Error: {str(e)}"
            history.append({"role": "assistant", "content": error_msg})
            yield history, error_msg, None, False

    def reject_action(self, history: list[dict]) -> tuple[list[dict], str, Any, bool]:
        """Cancel the pending action."""
        self.state.pending_action = None
        history.append({"role": "assistant", "content": "❌ Action cancelled."})
        return history, "", None, False

    def _get_system_prompt(self) -> str:
        """Build system prompt with project context."""
        system_prompt = ENHANCED_CHAT_SYSTEM_PROMPT
        system_prompt += "\n\n## Accessible Paths"
        system_prompt += f"\n**Working Project:** {self.config.project.project_root}"
        return system_prompt


def create_enhanced_chat_interface(
    config: Optional[AgentUIConfig] = None,
) -> tuple[gr.Column, dict[str, Any]]:
    """Create the enhanced chat interface with all new features.

    Returns:
        Tuple of (Gradio Column, dict of component references).
    """
    components: dict[str, Any] = {}

    with gr.Column(scale=2) as chat_column:
        gr.Markdown("## 💬 Chat")

        with gr.Row():
            components["agent_selector"] = gr.Dropdown(
                label="Agent",
                choices=AGENT_CHOICES,
                value="auto",
                scale=2,
            )
            components["stop_btn"] = gr.Button(
                "⏹️ Stop",
                scale=1,
                size="sm",
                variant="stop",
            )
            components["clear_btn"] = gr.Button("🗑️ Clear", scale=1, size="sm")
            components["theme_toggle"] = gr.Button("🌓", scale=0, size="sm", elem_id="theme-toggle")
            components["sound_toggle"] = gr.Button("🔔", scale=0, size="sm", elem_id="sound-toggle")
            components["sound_enabled"] = gr.State(True)

        with gr.Accordion("🧠 Thinking", open=False) as thinking_accordion:
            components["thinking_accordion"] = thinking_accordion
            components["thinking_display"] = gr.Markdown(
                value="",
                elem_classes=["thinking-panel"],
            )

        components["chatbot"] = gr.Chatbot(
            label="Conversation",
            elem_classes=["chatbot"],
            height=500,
        )

        components["status_bar"] = gr.Markdown(
            value="",
            visible=True,
            elem_classes=["status-bar"],
        )

        with gr.Row():
            components["message_input"] = gr.Textbox(
                label="Message",
                placeholder="Ask anything about your codebase...",
                lines=2,
                scale=4,
            )
            components["send_btn"] = gr.Button(
                "➤",
                variant="primary",
                scale=1,
            )

        with gr.Accordion("🔧 Pending Actions", open=False, visible=False) as actions_accordion:
            components["actions_accordion"] = actions_accordion
            components["pending_action"] = gr.JSON(visible=False)
            with gr.Row():
                components["confirm_btn"] = gr.Button("✅ Yes, proceed", variant="primary")
                components["reject_btn"] = gr.Button("❌ No, cancel", variant="secondary")

        with gr.Accordion("📊 Activity", open=True):
            components["activity_display"] = gr.Markdown(
                value="_No activity yet_",
                elem_classes=["activity-feed"],
            )

    return chat_column, components


def setup_enhanced_chat_handlers(
    components: dict[str, Any],
    get_config: Callable,
) -> EnhancedChatHandler:
    """Set up event handlers for the enhanced chat interface."""
    handler = EnhancedChatHandler(get_config())

    def on_send(message: str, history: list[dict], agent: str):
        if not message.strip():
            yield history, "", "", "", None, gr.update(visible=False)
            return

        handler.config = get_config()
        max_iters = getattr(handler.config.agent, "max_iterations", 50)

        for result in handler.process_message_streaming(
            message, history, agent, max_iterations=max_iters
        ):
            updated_history, status, thinking, activity, pending, show_actions = result
            yield (
                updated_history,
                "",
                f"**{status}**" if status else "",
                thinking,
                activity,
                pending,
                gr.update(visible=show_actions),
            )

    components["send_btn"].click(
        fn=on_send,
        inputs=[
            components["message_input"],
            components["chatbot"],
            components["agent_selector"],
        ],
        outputs=[
            components["chatbot"],
            components["message_input"],
            components["status_bar"],
            components["thinking_display"],
            components["activity_display"],
            components["pending_action"],
            components["actions_accordion"],
        ],
    )

    components["message_input"].submit(
        fn=on_send,
        inputs=[
            components["message_input"],
            components["chatbot"],
            components["agent_selector"],
        ],
        outputs=[
            components["chatbot"],
            components["message_input"],
            components["status_bar"],
            components["thinking_display"],
            components["activity_display"],
            components["pending_action"],
            components["actions_accordion"],
        ],
    )

    def on_confirm(history: list[dict]):
        handler.config = get_config()
        for result in handler.confirm_action(history):
            updated_history, status, pending, show_actions = result
            yield (
                updated_history,
                f"**{status}**" if status else "",
                pending,
                gr.update(visible=show_actions),
            )

    def on_reject(history: list[dict]):
        updated, status, pending, visible = handler.reject_action(history)
        return updated, "", pending, gr.update(visible=False)

    components["confirm_btn"].click(
        fn=on_confirm,
        inputs=[components["chatbot"]],
        outputs=[
            components["chatbot"],
            components["status_bar"],
            components["pending_action"],
            components["actions_accordion"],
        ],
    )

    components["reject_btn"].click(
        fn=on_reject,
        inputs=[components["chatbot"]],
        outputs=[
            components["chatbot"],
            components["status_bar"],
            components["pending_action"],
            components["actions_accordion"],
        ],
    )

    def on_clear():
        result = handler.clear_history()
        return result[0], result[1], result[2], result[3]

    components["clear_btn"].click(
        fn=on_clear,
        outputs=[
            components["chatbot"],
            components["message_input"],
            components["thinking_display"],
            components["activity_display"],
        ],
    )

    def on_stop():
        handler.request_stop()
        return "⏹️ Stop requested..."

    components["stop_btn"].click(
        fn=on_stop,
        outputs=[components["status_bar"]],
    )

    def on_theme_toggle():
        return None

    components["theme_toggle"].click(
        fn=on_theme_toggle,
        outputs=None,
        js="() => { toggleTheme(); }",
    )

    def on_sound_toggle(sound_enabled: bool):
        new_state = not sound_enabled
        return gr.update(value="🔔" if new_state else "🔕"), new_state

    components["sound_toggle"].click(
        fn=on_sound_toggle,
        inputs=[components["sound_enabled"]],
        outputs=[components["sound_toggle"], components["sound_enabled"]],
        js="() => { toggleSound(); }",
    )

    return handler

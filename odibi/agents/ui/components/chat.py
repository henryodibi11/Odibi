"""Chat interface component for the Odibi Assistant.

Conversational interface with tool execution and agent routing.
"""

import json
from typing import Any, Generator, Optional

import gradio as gr

from ..config import AgentUIConfig
from ..llm_client import LLMClient, LLMConfig
from ..tools.file_tools import (
    format_file_for_display,
    list_directory,
    read_file,
    write_file,
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
)
from ..tools.code_execution import (
    execute_python,
    execute_sql,
    list_tables,
    describe_table,
    format_execution_result,
)
from ..tools.tool_definitions import TOOL_DEFINITIONS, TOOLS_REQUIRING_CONFIRMATION
from .todo_panel import todo_read, todo_write, update_todo_display
from .memories import get_memory_manager, format_memory_list
from odibi.agents.core.memory import MemoryType

AGENT_CHOICES = [
    ("🤖 Auto (Orchestrator)", "auto"),
    ("🔍 Code Analyst", "code_analyst"),
    ("🔧 Refactor Engineer", "refactor_engineer"),
    ("🧪 Test Architect", "test_architect"),
    ("📝 Documentation", "documentation"),
]


CHAT_SYSTEM_PROMPT = """
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

## Tool Usage Guidelines

1. **list_directory** - ALWAYS use this to see what's in a folder before describing it
2. **read_file** - ALWAYS use this to see file contents before describing them
3. **grep** / **glob** / **search** - Use to find code patterns
4. **todo_write** - Use to plan and track progress on complex tasks
5. **mermaid** - Use to create diagrams when explaining architecture
6. **diagnostics** - Run after making code changes

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

**Example usage:**
- "Analyze all test files" → Use parallel_tasks to analyze each test file simultaneously
- "Find all usages of X across the codebase" → Use task for focused search
- "Review these 3 modules" → Use parallel_tasks with one task per module

Be proactive about using sub-agents - they make you faster and more thorough!

## Response Format

- Use markdown formatting
- Code blocks with language hints
- Keep responses concise but informative
- When showing directory contents, show the ACTUAL results from tools, not fabricated trees

## Project Context

You are helping the user with their codebase. If it's an Odibi project (has project.yaml),
you can also help with pipelines, transformers, and connections.
"""


def create_chat_interface(
    config: Optional[AgentUIConfig] = None,
) -> tuple[gr.Column, dict[str, Any]]:
    """Create the chat interface component.

    Args:
        config: UI configuration.

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

        components["chatbot"] = gr.Chatbot(
            label="Conversation",
            elem_classes=["chatbot"],
        )

        components["status_bar"] = gr.Markdown(
            value="",
            visible=True,
            elem_classes=["status-bar"],
        )

        with gr.Row():
            components["message_input"] = gr.Textbox(
                label="Message",
                placeholder="Ask anything about the Odibi codebase...",
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

    return chat_column, components


class ChatHandler:
    """Handles chat interactions with tool execution using native function calling."""

    def __init__(self, config: AgentUIConfig):
        self.config = config
        self.conversation_history: list[dict[str, str]] = []
        self.pending_action: Optional[dict] = None
        self._stop_requested = False

    def request_stop(self) -> None:
        """Request the agent loop to stop."""
        self._stop_requested = True

    def reset_stop(self) -> None:
        """Reset the stop flag."""
        self._stop_requested = False

    @property
    def should_stop(self) -> bool:
        """Check if stop was requested."""
        return self._stop_requested

    def get_llm_client(self) -> LLMClient:
        """Get the LLM client from current config."""
        llm_config = LLMConfig(
            endpoint=self.config.llm.endpoint,
            api_key=self.config.llm.api_key,
            model=self.config.llm.model,
            api_type=self.config.llm.api_type,
            api_version=self.config.llm.api_version,
        )
        return LLMClient(llm_config)

    def _sanitize_conversation_history(self) -> None:
        """Remove corrupted tool_calls that lack corresponding tool responses.

        The OpenAI API requires every assistant message with tool_calls to be
        followed by tool messages for each tool_call_id. This method detects
        and removes any such orphaned tool_calls.
        """
        if not self.conversation_history:
            return

        tool_call_ids_needed = set()
        tool_call_ids_found = set()

        for msg in self.conversation_history:
            if msg.get("role") == "assistant" and msg.get("tool_calls"):
                for tc in msg["tool_calls"]:
                    tool_call_ids_needed.add(tc.get("id"))
            elif msg.get("role") == "tool":
                tool_call_ids_found.add(msg.get("tool_call_id"))

        orphaned_ids = tool_call_ids_needed - tool_call_ids_found
        if not orphaned_ids:
            return

        cleaned = []
        for msg in self.conversation_history:
            if msg.get("role") == "assistant" and msg.get("tool_calls"):
                has_orphan = any(tc.get("id") in orphaned_ids for tc in msg["tool_calls"])
                if has_orphan:
                    if msg.get("content"):
                        cleaned.append({"role": "assistant", "content": msg["content"]})
                    continue
            cleaned.append(msg)

        self.conversation_history = cleaned

    def _extract_tool_call_from_text(self, content: str) -> Optional[list[dict]]:
        """Extract tool calls from text when model outputs JSON instead of using function calling.

        This handles cases where the model outputs raw JSON like:
        {"id": "list_directory", "params": {"path": "/some/path"}}
        {"operation": "file_operations.ls", "path": "/some/path"}

        Args:
            content: The text content from the model.

        Returns:
            List of tool call dicts compatible with OpenAI format, or None.
        """
        import re
        import uuid

        json_match = re.search(
            r'\{[^{}]*"(?:id|operation|tool|function|name)"[^{}]*(?:\{[^{}]*\})?[^{}]*\}',
            content,
            re.DOTALL,
        )
        if not json_match:
            json_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                return None
        else:
            json_str = json_match.group(0)

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError:
            return None

        tool_name = (
            data.get("id")
            or data.get("name")
            or data.get("operation")
            or data.get("tool")
            or data.get("function")
        )
        if not tool_name:
            return None

        tool_mapping = {
            "file_operations.ls": "list_directory",
            "file_operations.read": "read_file",
            "file_operations.write": "write_file",
            "ls": "list_directory",
            "read": "read_file",
            "write": "write_file",
            "search": "grep",
        }

        tool_name = tool_mapping.get(tool_name, tool_name)

        if "params" in data:
            args = data["params"]
        else:
            args = {
                k: v
                for k, v in data.items()
                if k not in ("id", "name", "operation", "tool", "function")
            }

        return [
            {
                "id": f"call_{uuid.uuid4().hex[:8]}",
                "type": "function",
                "function": {
                    "name": tool_name,
                    "arguments": json.dumps(args),
                },
            }
        ]

    def execute_tool(self, tool_call: dict) -> str:
        """Execute a tool call and return formatted result.

        Args:
            tool_call: Dict with 'tool' and 'args' keys.

        Returns:
            Formatted tool result.
        """
        tool_name = tool_call.get("tool", "")
        args = tool_call.get("args", {})

        if tool_name == "read_file":
            result = read_file(
                path=args.get("path", ""),
                start_line=args.get("start_line", 1),
                end_line=args.get("end_line"),
            )
            return format_file_for_display(result)

        elif tool_name == "write_file":
            result = write_file(
                path=args.get("path", ""),
                content=args.get("content", ""),
            )
            if result.success:
                return f"✅ {result.content}"
            return f"❌ Error: {result.error}"

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
            return format_search_results(result)

        elif tool_name == "glob":
            result = glob_files(
                pattern=args.get("pattern", "**/*"),
                path=args.get("path", "."),
            )
            return format_search_results(result)

        elif tool_name == "search":
            result = semantic_search(
                query=args.get("query", ""),
                project_root=self.config.project.project_root,
                k=args.get("k", 5),
                chunk_type=args.get("chunk_type"),
            )
            return format_semantic_results(result)

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
                emoji = {
                    "decision": "🔵",
                    "learning": "🟢",
                    "bug_fix": "🔴",
                    "preference": "🟣",
                    "todo": "🟡",
                    "feature": "🟠",
                    "context": "⚪",
                }.get(memory_type_str, "📝")
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

        return f"❓ Unknown tool: {tool_name}"

    def requires_confirmation(self, tool_name: str, args: dict = None) -> bool:
        """Check if a tool call requires user confirmation."""
        if tool_name in TOOLS_REQUIRING_CONFIRMATION:
            if tool_name == "ruff":
                return args.get("fix", False) if args else False
            return True
        return False

    def process_message(
        self,
        message: str,
        history: list[dict],
        agent: str,
        max_iterations: int = 50,
    ) -> Generator[tuple[list[dict], str, Any, bool], None, None]:
        """Process a user message with an agentic loop using native function calling.

        Args:
            message: User's message.
            history: Chat history.
            agent: Selected agent role.
            max_iterations: Maximum number of LLM calls to prevent infinite loops.

        Yields:
            Tuple of (updated history, status, pending action, show actions).
        """
        history = history.copy()
        history.append({"role": "user", "content": message})

        yield history, "Thinking...", None, False

        self.conversation_history.append({"role": "user", "content": message})

        try:
            client = self.get_llm_client()

            system_prompt = CHAT_SYSTEM_PROMPT
            system_prompt += "\n\n## Accessible Paths"
            system_prompt += f"\n**Working Project:** {self.config.project.project_root}"
            if self.config.project.reference_repo:
                system_prompt += f"\n**Reference Repo:** {self.config.project.reference_repo}"

            iteration = 0
            self.reset_stop()

            while iteration < max_iterations:
                if self.should_stop:
                    history.append({"role": "assistant", "content": "⏹️ Stopped by user."})
                    yield history, "", None, False
                    return

                iteration += 1
                yield history, f"🤔 Thinking... (step {iteration})", None, False

                self._sanitize_conversation_history()

                model_lower = self.config.llm.model.lower()
                no_tools_support = model_lower in ("o1-preview", "o1-mini", "o1")

                response = client.chat(
                    messages=self.conversation_history,
                    system_prompt=system_prompt,
                    temperature=0.1,
                    tools=TOOL_DEFINITIONS if not no_tools_support else None,
                )

                content = response.get("content")
                tool_calls = response.get("tool_calls")

                if content and not tool_calls:
                    extracted = self._extract_tool_call_from_text(content)
                    if extracted:
                        tool_calls = extracted
                        content = None

                if content:
                    history.append({"role": "assistant", "content": content})

                if not tool_calls:
                    if content:
                        self.conversation_history.append({"role": "assistant", "content": content})
                    yield history, "", None, False
                    return

                for tc in tool_calls:
                    tool_name = tc["function"]["name"]
                    tool_id = tc["id"]

                    try:
                        args = json.loads(tc["function"]["arguments"])
                    except json.JSONDecodeError:
                        args = {}

                    if self.requires_confirmation(tool_name, args):
                        self.pending_action = {
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
                        yield history, "Awaiting confirmation...", self.pending_action, True
                        return

                self.conversation_history.append(
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

                    try:
                        args = json.loads(tc["function"]["arguments"])
                    except json.JSONDecodeError:
                        args = {}

                    tool_emoji = {
                        "read_file": "📖",
                        "write_file": "✏️",
                        "list_directory": "📁",
                        "grep": "🔍",
                        "glob": "🔍",
                        "search": "🧠",
                        "run_command": "⚡",
                        "pytest": "🧪",
                        "ruff": "🔧",
                        "diagnostics": "🩺",
                        "typecheck": "📝",
                        "web_search": "🌐",
                        "read_web_page": "🌐",
                        "todo_write": "📋",
                        "todo_read": "📋",
                        "mermaid": "📊",
                        "git_status": "📦",
                        "git_diff": "📦",
                        "git_log": "📦",
                        "task": "🤖",
                        "parallel_tasks": "🚀",
                        "execute_python": "🐍",
                        "sql": "🗃️",
                        "list_tables": "📋",
                        "describe_table": "📊",
                        "remember": "💾",
                        "recall": "🧠",
                    }.get(tool_name, "🔧")

                    yield history, f"{tool_emoji} Running `{tool_name}`...", None, False

                    result = self.execute_tool({"tool": tool_name, "args": args})
                    tool_results.append(
                        {
                            "tool_call_id": tool_id,
                            "tool": tool_name,
                            "result": result,
                        }
                    )

                    history.append(
                        {"role": "assistant", "content": f"**{tool_emoji} {tool_name}:**\n{result}"}
                    )

                for tr in tool_results:
                    self.conversation_history.append(
                        {
                            "role": "tool",
                            "tool_call_id": tr["tool_call_id"],
                            "content": tr["result"],
                        }
                    )

                yield history, "🔄 Processing results...", None, False

            history.append(
                {
                    "role": "assistant",
                    "content": f"⚠️ Reached maximum iterations ({max_iterations}). Stopping.",
                }
            )
            yield history, "", None, False

        except Exception as e:
            self._sanitize_conversation_history()
            error_msg = f"❌ Error: {str(e)}"
            history.append({"role": "assistant", "content": error_msg})
            yield history, "", None, False

    def confirm_action(
        self,
        history: list[dict],
        system_prompt: str,
        max_iterations: int = 50,
    ):
        """Execute the pending action and continue the agent loop."""
        if not self.pending_action:
            yield history, "", None, False
            return

        try:
            tool_call = self.pending_action
            self.pending_action = None

            original_tool_calls = tool_call.get("tool_calls", [])
            original_content = tool_call.get("content")

            self.conversation_history.append(
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

                tool_emoji = {
                    "read_file": "📖",
                    "write_file": "✏️",
                    "list_directory": "📁",
                    "grep": "🔍",
                    "glob": "🔍",
                    "search": "🧠",
                    "run_command": "⚡",
                    "pytest": "🧪",
                    "ruff": "🔧",
                    "diagnostics": "🩺",
                    "typecheck": "📝",
                    "web_search": "🌐",
                    "read_web_page": "🌐",
                    "todo_write": "📋",
                    "todo_read": "📋",
                    "mermaid": "📊",
                    "git_status": "📦",
                    "git_diff": "📦",
                    "git_log": "📦",
                    "task": "🤖",
                    "parallel_tasks": "🚀",
                    "execute_python": "🐍",
                    "sql": "🗃️",
                    "list_tables": "📋",
                    "describe_table": "📊",
                    "remember": "💾",
                    "recall": "🧠",
                }.get(tc_name, "🔧")

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
                self.conversation_history.append(
                    {
                        "role": "tool",
                        "tool_call_id": tr["tool_call_id"],
                        "content": tr["result"],
                    }
                )

            yield history, "🔄 Continuing...", None, False

            client = self.get_llm_client()

            for iteration in range(max_iterations):
                if self.should_stop:
                    history.append({"role": "assistant", "content": "⏹️ Stopped by user."})
                    yield history, "", None, False
                    return

                yield history, f"🤔 Thinking... (step {iteration + 1})", None, False

                self._sanitize_conversation_history()

                model_lower = self.config.llm.model.lower()
                no_tools_support = model_lower in ("o1-preview", "o1-mini", "o1")

                response = client.chat(
                    messages=self.conversation_history,
                    system_prompt=system_prompt,
                    temperature=0.1,
                    tools=TOOL_DEFINITIONS if not no_tools_support else None,
                )

                content = response.get("content")
                tool_calls = response.get("tool_calls")

                if content and not tool_calls:
                    extracted = self._extract_tool_call_from_text(content)
                    if extracted:
                        tool_calls = extracted
                        content = None

                if content:
                    history.append({"role": "assistant", "content": content})

                if not tool_calls:
                    if content:
                        self.conversation_history.append({"role": "assistant", "content": content})
                    yield history, "", None, False
                    return

                for tc in tool_calls:
                    tc_name = tc["function"]["name"]
                    tc_id = tc["id"]
                    try:
                        tc_args = json.loads(tc["function"]["arguments"])
                    except json.JSONDecodeError:
                        tc_args = {}

                    if self.requires_confirmation(tc_name, tc_args):
                        self.pending_action = {
                            "tool": tc_name,
                            "args": tc_args,
                            "tool_call_id": tc_id,
                            "tool_calls": tool_calls,
                            "content": content,
                        }
                        args_json = json.dumps(tc_args, indent=2)
                        action_desc = f"**Pending Action:** `{tc_name}`\n```json\n{args_json}\n```"
                        history.append({"role": "assistant", "content": action_desc})
                        yield history, "Awaiting confirmation...", self.pending_action, True
                        return

                self.conversation_history.append(
                    {
                        "role": "assistant",
                        "content": content,
                        "tool_calls": tool_calls,
                    }
                )

                tool_results = []

                for tc in tool_calls:
                    tc_name = tc["function"]["name"]
                    tc_id = tc["id"]
                    try:
                        tc_args = json.loads(tc["function"]["arguments"])
                    except json.JSONDecodeError:
                        tc_args = {}

                    tc_emoji = {
                        "read_file": "📖",
                        "write_file": "✏️",
                        "list_directory": "📁",
                        "grep": "🔍",
                        "glob": "🔍",
                        "search": "🧠",
                        "run_command": "⚡",
                        "pytest": "🧪",
                        "ruff": "🔧",
                        "diagnostics": "🩺",
                        "typecheck": "📝",
                        "web_search": "🌐",
                        "read_web_page": "🌐",
                        "todo_write": "📋",
                        "todo_read": "📋",
                        "mermaid": "📊",
                        "git_status": "📦",
                        "git_diff": "📦",
                        "git_log": "📦",
                        "task": "🤖",
                        "parallel_tasks": "🚀",
                        "execute_python": "🐍",
                        "sql": "🗃️",
                        "list_tables": "📋",
                        "describe_table": "📊",
                        "remember": "💾",
                        "recall": "🧠",
                    }.get(tc_name, "🔧")
                    yield history, f"{tc_emoji} Running `{tc_name}`...", None, False

                    tc_result = self.execute_tool({"tool": tc_name, "args": tc_args})
                    tool_results.append(
                        {
                            "tool_call_id": tc_id,
                            "tool": tc_name,
                            "result": tc_result,
                        }
                    )
                    history.append(
                        {"role": "assistant", "content": f"**{tc_emoji} {tc_name}:**\n{tc_result}"}
                    )

                for tr in tool_results:
                    self.conversation_history.append(
                        {
                            "role": "tool",
                            "tool_call_id": tr["tool_call_id"],
                            "content": tr["result"],
                        }
                    )

                yield history, "🔄 Processing...", None, False

        except Exception as e:
            self._sanitize_conversation_history()
            error_msg = f"❌ Error: {str(e)}"
            history.append({"role": "assistant", "content": error_msg})
            yield history, "", None, False

    def reject_action(self, history: list[dict]) -> tuple[list[dict], str, bool]:
        """Cancel the pending action."""
        self.pending_action = None
        history.append({"role": "assistant", "content": "❌ Action cancelled."})
        return history, "", False

    def clear_history(self) -> tuple[list, str]:
        """Clear conversation history."""
        self.conversation_history = []
        self.pending_action = None
        return [], ""


def setup_chat_handlers(
    components: dict[str, Any],
    get_config: callable,
) -> ChatHandler:
    """Set up event handlers for chat interface.

    Args:
        components: Dict of Gradio components.
        get_config: Function to get current config.

    Returns:
        ChatHandler instance.
    """
    handler = ChatHandler(get_config())

    def on_send(message: str, history: list[dict], agent: str):
        if not message.strip():
            yield history, "", "", None, gr.update(visible=False)
            return

        handler.config = get_config()

        for result in handler.process_message(message, history, agent):
            updated_history, status, pending, show_actions = result
            yield (
                updated_history,
                "",
                f"**{status}**" if status else "",
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
            components["pending_action"],
            components["actions_accordion"],
        ],
    )

    def on_confirm(history: list[dict]):
        handler.config = get_config()

        system_prompt = CHAT_SYSTEM_PROMPT
        system_prompt += "\n\n## Accessible Paths"
        system_prompt += f"\n**Working Project:** {handler.config.project.project_root}"

        for result in handler.confirm_action(history, system_prompt):
            updated_history, status, pending, show_actions = result
            yield (
                updated_history,
                f"**{status}**" if status else "",
                pending,
                gr.update(visible=show_actions),
            )

    def on_reject(history: list[dict]):
        updated, status, visible = handler.reject_action(history)
        return updated, "", None, gr.update(visible=False)

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
        return handler.clear_history()

    components["clear_btn"].click(
        fn=on_clear,
        outputs=[components["chatbot"], components["message_input"]],
    )

    def on_stop():
        handler.request_stop()
        return gr.update(visible=False)

    if "stop_btn" in components:
        components["stop_btn"].click(
            fn=on_stop,
            outputs=[components["stop_btn"]],
        )

    return handler

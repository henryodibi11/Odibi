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
    format_tool_result_collapsible,
    linkify_file_paths,
    ConversationBranch,
)
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
from agents.core.memory import MemoryType


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
    branches: ConversationBranch = field(default_factory=ConversationBranch)
    current_model: str = ""


class EnhancedChatHandler:
    """Enhanced chat handler with streaming and activity tracking."""

    def __init__(self, config: AgentUIConfig):
        self.config = config
        self.state = ChatState()
        self._client: Optional[LLMClient] = None
        self._task_collector = TaskProgressCollector()

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
            result = write_file(
                path=args.get("path", ""),
                content=args.get("content", ""),
            )
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

        return f"❓ Unknown tool: {tool_name}"

    def requires_confirmation(self, tool_name: str, args: dict = None) -> bool:
        """Check if a tool call requires user confirmation."""
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

        try:
            client = self.get_llm_client()

            system_prompt = ENHANCED_CHAT_SYSTEM_PROMPT
            system_prompt += "\n\n## Accessible Paths"
            system_prompt += f"\n**Working Project:** {self.config.project.project_root}"
            if self.config.project.reference_repo:
                system_prompt += f"\n**Reference Repo:** {self.config.project.reference_repo}"

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
                    add_activity("Response complete", status="completed")
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

                    yield (
                        history,
                        f"{emoji} Running `{tool_name}`...",
                        thinking_update,
                        refresh_activity_display(),
                        None,
                        False,
                    )

                    result = self.execute_tool({"tool": tool_name, "args": args})
                    tool_results.append(
                        {
                            "tool_call_id": tool_id,
                            "tool": tool_name,
                            "result": result,
                        }
                    )

                    tool_display = format_tool_result_collapsible(tool_name, emoji, result)
                    history.append({"role": "assistant", "content": tool_display})

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
        clear_activity()
        reset_thinking()
        return [], "", "", "_No activity yet_"

    def create_branch(self) -> str:
        """Create a new conversation branch."""
        branch_name = self.state.branches.create_branch()
        self.state.branches.switch_branch(branch_name)
        return f"Created branch: {branch_name}"

    def list_branches(self) -> list[str]:
        """List all conversation branches."""
        return self.state.branches.list_branches()

    def switch_branch(self, branch_name: str) -> str:
        """Switch to a different branch."""
        self.state.branches.switch_branch(branch_name)
        return f"Switched to branch: {branch_name}"


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
            components["theme_toggle"] = gr.Button("🌓", scale=0, size="sm")
            components["sound_toggle"] = gr.Button("🔔", scale=0, size="sm", elem_id="sound-toggle")

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

        with gr.Row():
            components["branch_btn"] = gr.Button("🌿 Branch", size="sm", scale=1)
            components["branch_dropdown"] = gr.Dropdown(
                label="Branch",
                choices=["main"],
                value="main",
                scale=2,
                visible=False,
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

        for result in handler.process_message_streaming(message, history, agent):
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

    def on_branch():
        result = handler.create_branch()
        branches = handler.list_branches()
        return gr.update(choices=branches, visible=True), result

    components["branch_btn"].click(
        fn=on_branch,
        outputs=[components["branch_dropdown"], components["status_bar"]],
    )

    return handler

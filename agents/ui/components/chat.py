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

AGENT_CHOICES = [
    ("🤖 Auto (Orchestrator)", "auto"),
    ("🔍 Code Analyst", "code_analyst"),
    ("🔧 Refactor Engineer", "refactor_engineer"),
    ("🧪 Test Architect", "test_architect"),
    ("📝 Documentation", "documentation"),
]


CHAT_SYSTEM_PROMPT = """
You are a helpful AI coding assistant that can work with any codebase.

You have access to tools for: file operations, code search, shell commands, web research,
task management, diagrams, git, code execution (Databricks), and sub-agent spawning.

## Behavior Guidelines

1. **Be proactive** - Use tools immediately without asking permission for read-only operations
2. **Use todo_write** to plan complex tasks and track progress
3. **Use diagrams** when explaining architecture or flows
4. **Run diagnostics** after making code changes
5. **Ask confirmation ONLY for writes** - write_file, run_command, python, sql

## Agentic Behavior - CRITICAL

You operate in an AGENTIC LOOP. After each tool execution, you receive results and MUST:
1. **Continue working** until the task is FULLY complete
2. **Never ask** "would you like me to continue?" - just CONTINUE
3. **Never give partial results** - COMPLETE the task
4. **Call multiple tools** if needed to fully answer the question

For complex tasks:
1. Use todo_write to plan steps
2. Execute each step, marking todos as you go  
3. Run diagnostics after code changes
4. Summarize what you did when complete

## Response Format

- Use markdown formatting
- Code blocks with language hints
- Keep responses concise but informative

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

        elif tool_name == "python":
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
        max_iterations: int = 25,
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

                model_lower = self.config.llm.model.lower()
                is_reasoning_model = "o1" in model_lower or "o3" in model_lower or "o4" in model_lower

                if is_reasoning_model:
                    yield history, "🧠 Reasoning... (this may take a moment)", None, False
                
                response = client.chat(
                    messages=self.conversation_history,
                    system_prompt=system_prompt,
                    temperature=0.1,
                    tools=TOOL_DEFINITIONS if not is_reasoning_model else None,
                )

                content = response.get("content")
                tool_calls = response.get("tool_calls")

                if content:
                    history.append({"role": "assistant", "content": content})

                if not tool_calls:
                    if content:
                        self.conversation_history.append({"role": "assistant", "content": content})
                    yield history, "", None, False
                    return

                self.conversation_history.append({
                    "role": "assistant",
                    "content": content,
                    "tool_calls": tool_calls,
                })

                tool_results = []

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
                        }
                        args_json = json.dumps(args, indent=2)
                        action_desc = (
                            f"**Pending Action:** `{tool_name}`\n"
                            f"```json\n{args_json}\n```"
                        )
                        history.append({"role": "assistant", "content": action_desc})
                        yield history, "Awaiting confirmation...", self.pending_action, True
                        return

                    tool_emoji = {
                        "read_file": "📖", "write_file": "✏️", "list_directory": "📁",
                        "grep": "🔍", "glob": "🔍", "search": "🧠", "run_command": "⚡",
                        "pytest": "🧪", "ruff": "🔧", "diagnostics": "🩺", "typecheck": "📝",
                        "web_search": "🌐", "read_web_page": "🌐", "todo_write": "📋",
                        "todo_read": "📋", "mermaid": "📊", "git_status": "📦",
                        "git_diff": "📦", "git_log": "📦", "task": "🤖",
                        "parallel_tasks": "🚀", "python": "🐍", "sql": "🗃️",
                        "list_tables": "📋", "describe_table": "📊",
                    }.get(tool_name, "🔧")

                    yield history, f"{tool_emoji} Running `{tool_name}`...", None, False

                    result = self.execute_tool({"tool": tool_name, "args": args})
                    tool_results.append({
                        "tool_call_id": tool_id,
                        "tool": tool_name,
                        "result": result,
                    })

                    history.append({
                        "role": "assistant",
                        "content": f"**{tool_emoji} {tool_name}:**\n{result}"
                    })

                for tr in tool_results:
                    self.conversation_history.append({
                        "role": "tool",
                        "tool_call_id": tr["tool_call_id"],
                        "content": tr["result"],
                    })

                yield history, "🔄 Processing results...", None, False

            history.append(
                {
                    "role": "assistant",
                    "content": f"⚠️ Reached maximum iterations ({max_iterations}). Stopping.",
                }
            )
            yield history, "", None, False

        except Exception as e:
            error_msg = f"❌ Error: {str(e)}"
            history.append({"role": "assistant", "content": error_msg})
            yield history, "", None, False

    def confirm_action(
        self,
        history: list[dict],
        system_prompt: str,
        max_iterations: int = 25,
    ):
        """Execute the pending action and continue the agent loop."""
        if not self.pending_action:
            yield history, "", None, False
            return

        tool_call = self.pending_action
        self.pending_action = None

        tool_name = tool_call["tool"]
        args = tool_call["args"]
        tool_call_id = tool_call.get("tool_call_id", "call_confirmed")

        tool_emoji = {
            "write_file": "✏️", "run_command": "⚡", "python": "🐍",
            "sql": "🗃️", "odibi_run": "🔄",
        }.get(tool_name, "🔧")

        yield history, f"{tool_emoji} Executing `{tool_name}`...", None, False

        result = self.execute_tool({"tool": tool_name, "args": args})
        history.append({"role": "assistant", "content": f"**{tool_emoji} {tool_name}:**\n{result}"})

        self.conversation_history.append({
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": result,
        })

        yield history, "🔄 Continuing...", None, False

        client = self.get_llm_client()

        for iteration in range(max_iterations):
            if self.should_stop:
                history.append({"role": "assistant", "content": "⏹️ Stopped by user."})
                yield history, "", None, False
                return

            yield history, f"🤔 Thinking... (step {iteration + 1})", None, False

            model_lower = self.config.llm.model.lower()
            is_reasoning_model = "o1" in model_lower or "o3" in model_lower or "o4" in model_lower

            if is_reasoning_model:
                yield history, "🧠 Reasoning...", None, False

            response = client.chat(
                messages=self.conversation_history,
                system_prompt=system_prompt,
                temperature=0.1,
                tools=TOOL_DEFINITIONS if not is_reasoning_model else None,
            )

            content = response.get("content")
            tool_calls = response.get("tool_calls")

            if content:
                history.append({"role": "assistant", "content": content})

            if not tool_calls:
                if content:
                    self.conversation_history.append({"role": "assistant", "content": content})
                yield history, "", None, False
                return

            self.conversation_history.append({
                "role": "assistant",
                "content": content,
                "tool_calls": tool_calls,
            })

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
                    }
                    args_json = json.dumps(tc_args, indent=2)
                    action_desc = f"**Pending Action:** `{tc_name}`\n```json\n{args_json}\n```"
                    history.append({"role": "assistant", "content": action_desc})
                    yield history, "Awaiting confirmation...", self.pending_action, True
                    return

                tc_emoji = {"write_file": "✏️", "run_command": "⚡"}.get(tc_name, "🔧")
                yield history, f"{tc_emoji} Running `{tc_name}`...", None, False

                tc_result = self.execute_tool({"tool": tc_name, "args": tc_args})
                history.append({"role": "assistant", "content": f"**{tc_emoji} {tc_name}:**\n{tc_result}"})

                self.conversation_history.append({
                    "role": "tool",
                    "tool_call_id": tc_id,
                    "content": tc_result,
                })

            yield history, "🔄 Processing...", None, False

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

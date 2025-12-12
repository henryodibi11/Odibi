"""Chat interface component for the Odibi Assistant.

Conversational interface with tool execution and agent routing.
"""

import json
import re
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

You have access to the following tools to help users:

## Available Tools

### File Operations
- **read_file(path, start_line?, end_line?)** - Read a file from the codebase
- **write_file(path, content)** - Write content to a file (ALWAYS ask for confirmation first)
- **list_directory(path, pattern?, recursive?)** - List directory contents
- **undo_edit(path)** - Undo the last edit to a file

### Code Search
- **grep(pattern, path, file_pattern?)** - Search for text/regex in files
- **glob(pattern, path)** - Find files matching a glob pattern
- **search(query, k?)** - Semantic search: find code by meaning/concept (uses AI embeddings)

### Shell Commands
- **run_command(command)** - Execute a shell command (ALWAYS ask for confirmation first)
- **pytest(test_path?, verbose?, markers?)** - Run pytest tests
- **ruff(path, fix?)** - Run ruff linter
- **diagnostics(path, include_ruff?, include_mypy?, include_pytest?)** - Run code diagnostics
- **typecheck(path)** - Run mypy type checker

### Web & Research
- **web_search(query, max_results?)** - Search the web for documentation, examples, etc.
- **read_web_page(url)** - Read and extract content from a web page

### Task Management
- **todo_write(todos)** - Update the task list (array of {id, content, status})
- **todo_read()** - Read current task list

### Diagrams
- **mermaid(code)** - Render a Mermaid diagram (flowchart, sequence, etc.)

### Git
- **git_status()** - Show repository status (modified, staged, untracked files)
- **git_diff(path?, staged?)** - Show changes/diff for files
- **git_log(max_count?, path?)** - Show recent commit history

### Odibi
- **odibi_run(pipeline_path, dry_run?, engine?)** - Run an Odibi pipeline

## Tool Usage Format

When you need to use a tool, output it in this exact format:
```tool
{"tool": "tool_name", "args": {"arg1": "value1", "arg2": "value2"}}
```

Examples:
```tool
{"tool": "read_file", "args": {"path": "src/main.py"}}
```

```tool
{"tool": "web_search", "args": {"query": "pandas groupby aggregate examples"}}
```

```tool
{"tool": "todo_write", "args": {"todos": [{"id": "1", "content": "Analyze code", "status": "completed"}, {"id": "2", "content": "Implement fix", "status": "in-progress"}]}}
```

```tool
{"tool": "mermaid", "args": {"code": "flowchart TD\\n    A[Start] --> B[Process]\\n    B --> C[End]"}}
```

## Guidelines

1. **Be helpful and proactive** - Suggest relevant actions
2. **Show your work** - Explain what tools you're using and why
3. **Use todo_write** to plan complex tasks - break them into steps, mark progress
4. **Use diagrams** when explaining architecture or flows
5. **Search the web** when you need documentation or examples
6. **Run diagnostics** after making code changes
7. **Ask for confirmation** before writing/modifying files or running commands
8. **Format code nicely** - Use markdown code blocks with language hints
9. **Summarize tool output** - Don't just dump raw output; explain what it means

## Response Format

- Use markdown formatting
- Code blocks with language: ```python, ```yaml, ```bash
- Keep responses concise but informative
- For long outputs, summarize key points

## Agentic Behavior

You operate in an agentic loop. After each tool execution, you will receive the results
and can decide to:
1. **Continue working** - use more tools to gather info or complete the task
2. **Finish** - provide a final response when the task is complete

Keep working until the user's request is fully addressed. Don't stop after just one tool
call if more work is needed. For complex tasks:
1. First use todo_write to plan the steps
2. Execute each step, marking todos as you go
3. Run diagnostics after code changes
4. Summarize what you did when complete

## Project Context

You are helping the user with their codebase located at the active project path.
You can search, read, and analyze code in that directory.
If the project appears to be an Odibi project (has project.yaml), you can also help with:
- Pipeline configuration and execution
- Transformers: derive, filter, SCD, join, aggregate
- Connections: local, ADLS, Delta Lake
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
                visible=False,
            )
            components["clear_btn"] = gr.Button("🗑️ Clear", scale=1, size="sm")

        components["chatbot"] = gr.Chatbot(
            label="Conversation",
            height=500,
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
    """Handles chat interactions with tool execution."""

    TOOL_PATTERN = re.compile(r"```tool\s*\n({.*?})\s*\n```", re.DOTALL)

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

        return f"❓ Unknown tool: {tool_name}"

    def requires_confirmation(self, tool_call: dict) -> bool:
        """Check if a tool call requires user confirmation."""
        dangerous_tools = {"write_file", "run_command", "odibi_run"}
        tool_name = tool_call.get("tool", "")

        if tool_name in dangerous_tools:
            return True

        if tool_name == "ruff" and tool_call.get("args", {}).get("fix"):
            return True

        return False

    def process_message(
        self,
        message: str,
        history: list[dict],
        agent: str,
        max_iterations: int = 10,
    ) -> Generator[tuple[list[dict], str, Any, bool], None, None]:
        """Process a user message with an agentic loop.

        The agent will automatically continue after tool execution,
        feeding results back to the LLM until no more tools are needed
        or max_iterations is reached.

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
            system_prompt += f"\n\n**Working Project:** {self.config.project.project_root}"
            if self.config.project.odibi_root != self.config.project.project_root:
                system_prompt += f"\n**Odibi Library:** {self.config.project.odibi_root} (available for grep/read when needed)"

            iteration = 0
            self.reset_stop()

            while iteration < max_iterations:
                if self.should_stop:
                    history.append(
                        {"role": "assistant", "content": "⏹️ Stopped by user."}
                    )
                    yield history, "", None, False
                    return

                iteration += 1

                yield history, f"Thinking... (step {iteration})", None, False

                response = client.chat(
                    messages=self.conversation_history,
                    system_prompt=system_prompt,
                    temperature=0.1,
                )

                tool_matches = list(self.TOOL_PATTERN.finditer(response))

                if not tool_matches:
                    history.append({"role": "assistant", "content": response})
                    self.conversation_history.append(
                        {"role": "assistant", "content": response}
                    )
                    yield history, "", None, False
                    return

                tool_results = []

                for match in tool_matches:
                    tool_json = match.group(1)
                    try:
                        tool_call = json.loads(tool_json)

                        if self.requires_confirmation(tool_call):
                            self.pending_action = tool_call
                            response_before = response[: match.start()].strip()
                            if response_before:
                                history.append(
                                    {"role": "assistant", "content": response_before}
                                )

                            args_json = json.dumps(tool_call["args"], indent=2)
                            action_desc = (
                                f"**Pending Action:** `{tool_call['tool']}`\n"
                                f"```json\n{args_json}\n```"
                            )
                            history.append({"role": "assistant", "content": action_desc})

                            self.conversation_history.append(
                                {"role": "assistant", "content": response}
                            )
                            yield history, "Awaiting confirmation...", tool_call, True
                            return

                        else:
                            yield (
                                history,
                                f"Executing {tool_call['tool']}...",
                                None,
                                False,
                            )
                            result = self.execute_tool(tool_call)
                            tool_results.append(
                                {
                                    "tool": tool_call["tool"],
                                    "args": tool_call.get("args", {}),
                                    "result": result,
                                }
                            )

                    except json.JSONDecodeError:
                        tool_results.append(
                            {
                                "tool": "unknown",
                                "args": {},
                                "result": f"❌ Invalid tool format: {tool_json[:100]}...",
                            }
                        )

                response_text = response
                for match in reversed(tool_matches):
                    response_text = (
                        response_text[: match.start()]
                        + "[tool executed]"
                        + response_text[match.end() :]
                    )

                if response_text.strip() and response_text.strip() != "[tool executed]":
                    cleaned = response_text.replace("[tool executed]", "").strip()
                    if cleaned:
                        history.append({"role": "assistant", "content": cleaned})

                self.conversation_history.append(
                    {"role": "assistant", "content": response}
                )

                tool_results_text = "\n\n".join(
                    f"**Tool:** `{r['tool']}`\n{r['result']}" for r in tool_results
                )
                history.append(
                    {"role": "assistant", "content": f"📋 **Results:**\n\n{tool_results_text}"}
                )

                tool_result_msg = "Tool execution results:\n\n" + "\n\n".join(
                    f"[{r['tool']}]: {r['result']}" for r in tool_results
                )
                self.conversation_history.append(
                    {"role": "user", "content": f"[SYSTEM] {tool_result_msg}"}
                )

                yield history, "Continuing...", None, False

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

    def confirm_action(self, history: list[dict]) -> tuple[list[dict], str, bool]:
        """Execute the pending action after confirmation."""
        if not self.pending_action:
            return history, "", False

        tool_call = self.pending_action
        self.pending_action = None

        result = self.execute_tool(tool_call)
        history.append({"role": "assistant", "content": f"**Result:**\n{result}"})

        return history, "", False

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
            yield history, "", None, gr.update(visible=False)
            return

        handler.config = get_config()

        for result in handler.process_message(message, history, agent):
            updated_history, status, pending, show_actions = result
            yield (
                updated_history,
                "",
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
            components["pending_action"],
            components["actions_accordion"],
        ],
    )

    def on_confirm(history: list[dict]):
        updated, status, visible = handler.confirm_action(history)
        return updated, gr.update(visible=False)

    def on_reject(history: list[dict]):
        updated, status, visible = handler.reject_action(history)
        return updated, gr.update(visible=False)

    components["confirm_btn"].click(
        fn=on_confirm,
        inputs=[components["chatbot"]],
        outputs=[components["chatbot"], components["actions_accordion"]],
    )

    components["reject_btn"].click(
        fn=on_reject,
        inputs=[components["chatbot"]],
        outputs=[components["chatbot"], components["actions_accordion"]],
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

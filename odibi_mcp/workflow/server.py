"""Workflow MCP Server.

Provides workflow tools: skills, tasks, formatting, memory, and reasoning/planning.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from pathlib import Path
from datetime import datetime

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WORKSPACE_ROOT = Path(__file__).parent.parent.parent
SKILLS_DIR = Path(__file__).parent / "skills"
TASKS_FILE = WORKSPACE_ROOT / "tasks.json"
MEMORY_FILE = WORKSPACE_ROOT / "memory.json"

server = Server("odibi-workflow")


def _load_json(path: Path) -> dict:
    """Load JSON file, return empty dict if not exists."""
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
    return {}


def _save_json(path: Path, data: dict) -> None:
    """Save data to JSON file."""
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def _generate_task_id() -> str:
    """Generate a unique task ID."""
    return f"task_{int(datetime.utcnow().timestamp() * 1000)}"


async def _call_azure_openai(prompt: str, system_prompt: str = "") -> str:
    """Call Azure OpenAI API for reasoning tasks."""
    try:
        import httpx
    except ImportError:
        return "Error: httpx not installed. Run: pip install httpx"

    api_key = os.environ.get("AZURE_OPENAI_API_KEY")
    endpoint = os.environ.get(
        "AZURE_OPENAI_ENDPOINT", "https://odibi-ai.cognitiveservices.azure.com"
    )
    model = os.environ.get("AZURE_OPENAI_MODEL", "gpt-4.1")

    if not api_key:
        return "Error: AZURE_OPENAI_API_KEY environment variable not set"

    url = f"{endpoint}/openai/deployments/{model}/chat/completions?api-version=2024-02-15-preview"

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                url,
                headers={
                    "api-key": api_key,
                    "Content-Type": "application/json",
                },
                json={
                    "messages": messages,
                    "temperature": 0.7,
                    "max_tokens": 2000,
                },
            )
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]
    except httpx.HTTPStatusError as e:
        return f"API Error: {e.response.status_code} - {e.response.text}"
    except httpx.RequestError as e:
        return f"Request Error: {str(e)}"
    except Exception as e:
        return f"Error: {str(e)}"


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available workflow tools."""
    return [
        # ============ SKILL TOOLS ============
        Tool(
            name="load_skill",
            description="""LOAD A SKILL - Get specialized instructions for a task type.

WHEN TO USE: Before starting complex work. Skills provide step-by-step workflows.
AVAILABLE SKILLS:
- "planning" - How to plan multi-step tasks
- "debugging" - How to debug errors
- "file-editing" - How to safely edit files
- "searching" - How to find files and code
- "code-review" - How to review code
- "odibi-pipeline" - How to build odibi pipelines
- "browsing" - How to use playwright browser automation

ALWAYS load a skill before complex work. Follow its instructions.

Example: load_skill("browsing") → returns browser automation methodology""",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Skill name: planning, debugging, file-editing, searching, code-review, odibi-pipeline",
                    },
                },
                "required": ["name"],
            },
        ),
        Tool(
            name="list_skills",
            description="""LIST AVAILABLE SKILLS - See what skills can be loaded.

Returns list of skill names that can be used with load_skill().""",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        # ============ TASK TOOLS ============
        Tool(
            name="create_task",
            description="""CREATE A TASK - Track work items persistently.

WHEN TO USE: When user requests a multi-step task you need to track.
Tasks persist to tasks.json.

Example: create_task("Implement user authentication", "high")""",
            inputSchema={
                "type": "object",
                "properties": {
                    "description": {
                        "type": "string",
                        "description": "What needs to be done",
                    },
                    "priority": {
                        "type": "string",
                        "description": "Priority level",
                        "enum": ["low", "medium", "high", "critical"],
                        "default": "medium",
                    },
                },
                "required": ["description"],
            },
        ),
        Tool(
            name="list_tasks",
            description="""LIST TASKS - See all tracked tasks.

Filter by status to see pending, in_progress, or completed tasks.

Example: list_tasks("pending") → shows only pending tasks""",
            inputSchema={
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "description": "Filter: pending, in_progress, completed (optional)",
                        "enum": ["pending", "in_progress", "completed"],
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="update_task",
            description="""UPDATE A TASK - Change status or add notes.

Example: update_task("task_123", status="in_progress", notes="Started working on this")""",
            inputSchema={
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "Task ID from create_task",
                    },
                    "status": {
                        "type": "string",
                        "description": "New status",
                        "enum": ["pending", "in_progress", "completed"],
                    },
                    "notes": {
                        "type": "string",
                        "description": "Notes to add",
                    },
                },
                "required": ["task_id"],
            },
        ),
        Tool(
            name="complete_task",
            description="""COMPLETE A TASK - Mark task as done with summary.

Example: complete_task("task_123", "Implemented auth with JWT tokens")""",
            inputSchema={
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "Task ID to complete",
                    },
                    "summary": {
                        "type": "string",
                        "description": "What was accomplished",
                    },
                },
                "required": ["task_id"],
            },
        ),
        # ============ FORMAT TOOLS ============
        Tool(
            name="format_file",
            description="""FORMAT A FILE - Auto-format code after editing.

WHEN TO USE: After making edits to clean up formatting.
FORMATTERS:
- .py → ruff format (or black)
- .js/.ts/.json → prettier
- Other → skipped

Example: format_file("src/main.py") → formats the Python file""",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path to file to format",
                    },
                },
                "required": ["path"],
            },
        ),
        # ============ MEMORY TOOLS ============
        Tool(
            name="remember",
            description="""STORE IN MEMORY - Save key-value for later recall.

WHEN TO USE: Store important context, decisions, or values across sessions.
Persists to memory.json.

Example: remember("project_style", "Use snake_case for functions")""",
            inputSchema={
                "type": "object",
                "properties": {
                    "key": {
                        "type": "string",
                        "description": "Key name to store under",
                    },
                    "value": {
                        "type": "string",
                        "description": "Value to store",
                    },
                },
                "required": ["key", "value"],
            },
        ),
        Tool(
            name="recall",
            description="""RECALL FROM MEMORY - Retrieve a stored value.

Example: recall("project_style") → returns stored value""",
            inputSchema={
                "type": "object",
                "properties": {
                    "key": {
                        "type": "string",
                        "description": "Key to retrieve",
                    },
                },
                "required": ["key"],
            },
        ),
        Tool(
            name="forget",
            description="""FORGET - Delete a stored memory.

Example: forget("old_key") → removes from memory""",
            inputSchema={
                "type": "object",
                "properties": {
                    "key": {
                        "type": "string",
                        "description": "Key to delete",
                    },
                },
                "required": ["key"],
            },
        ),
        Tool(
            name="list_memories",
            description="""LIST ALL MEMORIES - See what's stored.

Returns all keys in memory.json.""",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        # ============ REASONING TOOLS ============
        Tool(
            name="plan_task",
            description="""PLAN A TASK - Get AI to create structured plan. CALL THIS FIRST for complex tasks.

WHEN TO USE: Before ANY task with 3+ steps. 
MANDATORY for: refactoring, new features, debugging, multi-file changes.

The AI reasoning model will return a numbered plan with concrete steps.
Execute the plan without stopping between steps.

Example: plan_task("Add user authentication to the API", "FastAPI backend, need JWT tokens")""",
            inputSchema={
                "type": "object",
                "properties": {
                    "description": {
                        "type": "string",
                        "description": "What needs to be done",
                    },
                    "context": {
                        "type": "string",
                        "description": "Relevant background (tech stack, constraints, related files)",
                    },
                },
                "required": ["description"],
            },
        ),
        Tool(
            name="review_code",
            description="""CODE REVIEW - Get AI analysis of code quality.

WHEN TO USE: Before finalizing changes, after implementing features.
Returns: Issues found, suggestions, security concerns.

Example: review_code("src/auth.py", "security") → security-focused review""",
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to file to review",
                    },
                    "focus": {
                        "type": "string",
                        "description": "Focus area: security, performance, readability, bugs (optional)",
                    },
                },
                "required": ["file_path"],
            },
        ),
        Tool(
            name="debug_issue",
            description="""DEBUG AN ERROR - Get AI help with errors. Use when STUCK.

WHEN TO USE: After an error occurs that you can't immediately fix.
WORKFLOW:
1. Copy the error message
2. Call debug_issue with error and context
3. Apply the suggested fix
4. Retry

DO NOT give up on errors. Call this tool.

Example: debug_issue("KeyError: 'user_id'", "Trying to access user data from response")""",
            inputSchema={
                "type": "object",
                "properties": {
                    "error": {
                        "type": "string",
                        "description": "Error message or full traceback",
                    },
                    "context": {
                        "type": "string",
                        "description": "What you were doing when error occurred",
                    },
                },
                "required": ["error"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls."""

    # Skill Tools
    if name == "load_skill":
        skill_name = arguments["name"]
        skill_path = SKILLS_DIR / f"{skill_name}.md"
        if not skill_path.exists():
            return [TextContent(type="text", text=f"Skill not found: {skill_name}")]
        content = skill_path.read_text(encoding="utf-8")
        return [TextContent(type="text", text=content)]

    elif name == "list_skills":
        skills = []
        if SKILLS_DIR.exists():
            for f in SKILLS_DIR.glob("*.md"):
                skills.append(f.stem)
        return [TextContent(type="text", text=json.dumps({"skills": skills}, indent=2))]

    # Task Tools
    elif name == "create_task":
        tasks = _load_json(TASKS_FILE)
        task_id = _generate_task_id()
        tasks[task_id] = {
            "id": task_id,
            "description": arguments["description"],
            "priority": arguments.get("priority", "medium"),
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "notes": "",
            "summary": "",
        }
        _save_json(TASKS_FILE, tasks)
        return [TextContent(type="text", text=json.dumps({"task_id": task_id}, indent=2))]

    elif name == "list_tasks":
        tasks = _load_json(TASKS_FILE)
        status_filter = arguments.get("status")
        if status_filter:
            tasks = {k: v for k, v in tasks.items() if v.get("status") == status_filter}
        return [TextContent(type="text", text=json.dumps(tasks, indent=2))]

    elif name == "update_task":
        tasks = _load_json(TASKS_FILE)
        task_id = arguments["task_id"]
        if task_id not in tasks:
            return [TextContent(type="text", text=f"Task not found: {task_id}")]
        if "status" in arguments and arguments["status"]:
            tasks[task_id]["status"] = arguments["status"]
        if "notes" in arguments and arguments["notes"]:
            tasks[task_id]["notes"] = arguments["notes"]
        tasks[task_id]["updated_at"] = datetime.utcnow().isoformat()
        _save_json(TASKS_FILE, tasks)
        return [TextContent(type="text", text=json.dumps(tasks[task_id], indent=2))]

    elif name == "complete_task":
        tasks = _load_json(TASKS_FILE)
        task_id = arguments["task_id"]
        if task_id not in tasks:
            return [TextContent(type="text", text=f"Task not found: {task_id}")]
        tasks[task_id]["status"] = "completed"
        tasks[task_id]["completed_at"] = datetime.utcnow().isoformat()
        if "summary" in arguments and arguments["summary"]:
            tasks[task_id]["summary"] = arguments["summary"]
        tasks[task_id]["updated_at"] = datetime.utcnow().isoformat()
        _save_json(TASKS_FILE, tasks)
        return [TextContent(type="text", text=json.dumps(tasks[task_id], indent=2))]

    # Format Tools
    elif name == "format_file":
        file_path = Path(arguments["path"])
        if not file_path.exists():
            return [TextContent(type="text", text=f"File not found: {file_path}")]

        ext = file_path.suffix.lower()
        result = {"file": str(file_path), "formatted": False, "message": ""}

        try:
            if ext == ".py":
                # Try ruff first, then black
                try:
                    subprocess.run(
                        ["ruff", "format", str(file_path)],
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    result["formatted"] = True
                    result["message"] = "Formatted with ruff"
                except (subprocess.CalledProcessError, FileNotFoundError):
                    try:
                        subprocess.run(
                            ["black", str(file_path)],
                            check=True,
                            capture_output=True,
                            text=True,
                        )
                        result["formatted"] = True
                        result["message"] = "Formatted with black"
                    except (subprocess.CalledProcessError, FileNotFoundError) as e:
                        result["message"] = f"No Python formatter available: {e}"

            elif ext in [".js", ".ts", ".jsx", ".tsx", ".json", ".css", ".html", ".md"]:
                try:
                    subprocess.run(
                        ["prettier", "--write", str(file_path)],
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    result["formatted"] = True
                    result["message"] = "Formatted with prettier"
                except (subprocess.CalledProcessError, FileNotFoundError) as e:
                    result["message"] = f"Prettier not available: {e}"

            else:
                result["message"] = f"No formatter configured for {ext} files"

        except Exception as e:
            result["message"] = f"Error formatting file: {e}"

        return [TextContent(type="text", text=json.dumps(result, indent=2))]

    # Memory Tools
    elif name == "remember":
        memory = _load_json(MEMORY_FILE)
        memory[arguments["key"]] = {
            "value": arguments["value"],
            "stored_at": datetime.utcnow().isoformat(),
        }
        _save_json(MEMORY_FILE, memory)
        return [TextContent(type="text", text=f"Stored: {arguments['key']}")]

    elif name == "recall":
        memory = _load_json(MEMORY_FILE)
        key = arguments["key"]
        if key not in memory:
            return [TextContent(type="text", text=f"Key not found: {key}")]
        return [TextContent(type="text", text=json.dumps(memory[key], indent=2))]

    elif name == "forget":
        memory = _load_json(MEMORY_FILE)
        key = arguments["key"]
        if key not in memory:
            return [TextContent(type="text", text=f"Key not found: {key}")]
        del memory[key]
        _save_json(MEMORY_FILE, memory)
        return [TextContent(type="text", text=f"Deleted: {key}")]

    elif name == "list_memories":
        memory = _load_json(MEMORY_FILE)
        keys = list(memory.keys())
        return [TextContent(type="text", text=json.dumps({"keys": keys}, indent=2))]

    # Reasoning Tools
    elif name == "plan_task":
        description = arguments["description"]
        context = arguments.get("context", "")

        system_prompt = """You are a planning assistant. Create structured, actionable plans.
Break down tasks into clear steps with:
- Numbered steps
- Dependencies noted
- Estimated effort where applicable
- Potential risks or blockers
- Success criteria"""

        prompt = f"Create a detailed plan for the following task:\n\n{description}"
        if context:
            prompt += f"\n\nAdditional context:\n{context}"

        result = await _call_azure_openai(prompt, system_prompt)
        return [TextContent(type="text", text=result)]

    elif name == "review_code":
        file_path = Path(arguments["file_path"])
        if not file_path.exists():
            return [TextContent(type="text", text=f"File not found: {file_path}")]

        code = file_path.read_text(encoding="utf-8")
        focus = arguments.get("focus", "general quality")

        system_prompt = """You are an expert code reviewer. Provide constructive, specific feedback.
Focus on:
- Correctness and potential bugs
- Security vulnerabilities
- Performance issues
- Code readability and maintainability
- Best practices

Be specific with line numbers and suggestions."""

        prompt = f"Review the following code with focus on {focus}:\n\nFile: {file_path}\n\n```\n{code}\n```"

        result = await _call_azure_openai(prompt, system_prompt)
        return [TextContent(type="text", text=result)]

    elif name == "debug_issue":
        error = arguments["error"]
        context = arguments.get("context", "")

        system_prompt = """You are a debugging expert. Analyze errors and provide:
1. Root cause analysis
2. Step-by-step debugging approach
3. Potential fixes with code examples
4. Prevention strategies for the future

Be specific and actionable."""

        prompt = f"Debug the following error:\n\n{error}"
        if context:
            prompt += f"\n\nContext:\n{context}"

        result = await _call_azure_openai(prompt, system_prompt)
        return [TextContent(type="text", text=result)]

    return [TextContent(type="text", text=f"Unknown tool: {name}")]


async def main():
    """Run the MCP server."""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())

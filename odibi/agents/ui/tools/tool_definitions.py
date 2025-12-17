from __future__ import annotations

"""Tool definitions for native OpenAI function calling.

Defines all tools as JSON schemas that can be passed to the LLM API.
"""

TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "Read a file from the filesystem. Returns the file contents with line numbers.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute path to the file to read"},
                    "start_line": {
                        "type": "integer",
                        "description": "Starting line number (1-indexed). Default: 1",
                    },
                    "end_line": {
                        "type": "integer",
                        "description": "Ending line number (inclusive). Default: start_line + 500",
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
            "description": "Write content to a file. Creates parent directories if needed.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path to write to"},
                    "content": {"type": "string", "description": "Content to write to the file"},
                },
                "required": ["path", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_directory",
            "description": "List contents of a directory.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Directory path to list"},
                    "pattern": {
                        "type": "string",
                        "description": "Glob pattern to filter. Default: *",
                    },
                    "recursive": {
                        "type": "boolean",
                        "description": "Whether to recurse into subdirectories. Default: false",
                    },
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "grep",
            "description": "Search for a pattern in files using text or regex matching.",
            "parameters": {
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Text or regex pattern to search for",
                    },
                    "path": {"type": "string", "description": "Directory or file to search in"},
                    "file_pattern": {
                        "type": "string",
                        "description": "Glob pattern for files to search. Default: *.py",
                    },
                    "is_regex": {
                        "type": "boolean",
                        "description": "Whether pattern is a regex. Default: false",
                    },
                },
                "required": ["pattern", "path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "glob",
            "description": "Find files matching a glob pattern.",
            "parameters": {
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Glob pattern (e.g., **/*.py, *test*.py)",
                    },
                    "path": {"type": "string", "description": "Base directory to search from"},
                },
                "required": ["pattern", "path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search",
            "description": "Semantic search: find code by meaning/concept using AI embeddings.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language query describing what you're looking for",
                    },
                    "k": {
                        "type": "integer",
                        "description": "Number of results to return. Default: 5",
                    },
                    "chunk_type": {
                        "type": "string",
                        "description": "Filter by type: function, class, module",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_command",
            "description": "Execute a shell command.",
            "parameters": {
                "type": "object",
                "properties": {
                    "command": {"type": "string", "description": "Shell command to execute"},
                    "working_dir": {
                        "type": "string",
                        "description": "Working directory for the command",
                    },
                },
                "required": ["command"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "pytest",
            "description": "Run pytest tests.",
            "parameters": {
                "type": "object",
                "properties": {
                    "test_path": {
                        "type": "string",
                        "description": "Path to test file or directory",
                    },
                    "verbose": {"type": "boolean", "description": "Verbose output. Default: true"},
                    "markers": {"type": "string", "description": "Pytest markers to filter tests"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ruff",
            "description": "Run ruff linter on code.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path to lint. Default: ."},
                    "fix": {"type": "boolean", "description": "Auto-fix issues. Default: false"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "diagnostics",
            "description": "Run code diagnostics (ruff, mypy, pytest).",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path to check. Default: ."},
                    "include_ruff": {
                        "type": "boolean",
                        "description": "Include ruff. Default: true",
                    },
                    "include_mypy": {
                        "type": "boolean",
                        "description": "Include mypy. Default: false",
                    },
                    "include_pytest": {
                        "type": "boolean",
                        "description": "Include pytest. Default: false",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "typecheck",
            "description": "Run mypy type checker.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path to type check. Default: ."}
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "Search the web for documentation, examples, etc.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"},
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum results to return. Default: 5",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_web_page",
            "description": "Read and extract content from a web page.",
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {"type": "string", "description": "URL of the web page to read"}
                },
                "required": ["url"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "todo_write",
            "description": "Update the task list. Use to plan and track progress on complex tasks.",
            "parameters": {
                "type": "object",
                "properties": {
                    "todos": {
                        "type": "array",
                        "description": "Array of todo items",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"},
                                "content": {"type": "string"},
                                "status": {
                                    "type": "string",
                                    "enum": ["todo", "in-progress", "completed"],
                                },
                            },
                            "required": ["id", "content", "status"],
                        },
                    }
                },
                "required": ["todos"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "todo_read",
            "description": "Read the current task list.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "mermaid",
            "description": "Render a Mermaid diagram (flowchart, sequence, etc.).",
            "parameters": {
                "type": "object",
                "properties": {"code": {"type": "string", "description": "Mermaid diagram code"}},
                "required": ["code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "git_status",
            "description": "Show git repository status (modified, staged, untracked files).",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "git_diff",
            "description": "Show git diff for files.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path to diff. Default: all changes"},
                    "staged": {
                        "type": "boolean",
                        "description": "Show staged changes. Default: false",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "git_log",
            "description": "Show recent commit history.",
            "parameters": {
                "type": "object",
                "properties": {
                    "max_count": {
                        "type": "integer",
                        "description": "Maximum commits to show. Default: 10",
                    },
                    "path": {"type": "string", "description": "Filter by path"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "odibi_run",
            "description": "Run an Odibi pipeline.",
            "parameters": {
                "type": "object",
                "properties": {
                    "pipeline_path": {
                        "type": "string",
                        "description": "Path to the pipeline configuration",
                    },
                    "dry_run": {"type": "boolean", "description": "Dry run mode. Default: true"},
                    "engine": {
                        "type": "string",
                        "description": "Engine: pandas or spark. Default: pandas",
                    },
                },
                "required": ["pipeline_path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "task",
            "description": "Spawn a sub-agent to complete a focused task. Use for independent work that can run separately.",
            "parameters": {
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "Task description for the sub-agent",
                    }
                },
                "required": ["prompt"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "parallel_tasks",
            "description": "Run multiple sub-agents in parallel for independent tasks.",
            "parameters": {
                "type": "object",
                "properties": {
                    "tasks": {
                        "type": "array",
                        "description": "Array of task prompts to run in parallel",
                        "items": {"type": "string"},
                    }
                },
                "required": ["tasks"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "execute_python",
            "description": "Execute Python code. In Databricks, has access to spark, pd, np.",
            "parameters": {
                "type": "object",
                "properties": {
                    "code": {"type": "string", "description": "Python code to execute"},
                    "timeout": {
                        "type": "integer",
                        "description": "Timeout in seconds. Default: 30",
                    },
                },
                "required": ["code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "sql",
            "description": "Execute SQL query via Spark.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "SQL query to execute"},
                    "limit": {
                        "type": "integer",
                        "description": "Maximum rows to return. Default: 100",
                    },
                    "show_schema": {
                        "type": "boolean",
                        "description": "Include schema info. Default: false",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_tables",
            "description": "List available tables in the Spark catalog.",
            "parameters": {
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database/schema to list tables from",
                    },
                    "pattern": {"type": "string", "description": "Pattern to filter table names"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "describe_table",
            "description": "Get schema and sample data from a table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Full table name (database.table or just table)",
                    }
                },
                "required": ["table_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "undo_edit",
            "description": "Undo the last edit to a file.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path to the file to restore"}
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "remember",
            "description": "Save an important memory for future reference. Use this to remember decisions, learnings, bug fixes, preferences, or important context that should persist across sessions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "summary": {
                        "type": "string",
                        "description": "Brief one-line summary of what to remember",
                    },
                    "content": {"type": "string", "description": "Full details and context"},
                    "memory_type": {
                        "type": "string",
                        "enum": [
                            "decision",
                            "learning",
                            "bug_fix",
                            "preference",
                            "todo",
                            "feature",
                            "context",
                        ],
                        "description": "Type of memory: decision (design choices), learning (insights), bug_fix (fixes), preference (user preferences), todo (tasks), feature (implementations), context (background info)",
                    },
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Tags for categorizing and searching the memory",
                    },
                    "importance": {
                        "type": "number",
                        "description": "Importance score from 0.0 to 1.0. Default: 0.7",
                    },
                },
                "required": ["summary", "memory_type"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "recall",
            "description": "Search past memories for relevant context. Use this to find previous decisions, learnings, or context.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query to find relevant memories",
                    },
                    "memory_types": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": [
                                "decision",
                                "learning",
                                "bug_fix",
                                "preference",
                                "todo",
                                "feature",
                                "context",
                            ],
                        },
                        "description": "Filter by memory types",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum memories to return. Default: 10",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "explorer_experiment",
            "description": "Run an experiment in an isolated sandbox environment. Creates a clone of the source directory, executes commands, and returns the diff of changes. Safe for testing code changes without affecting the main codebase.",
            "parameters": {
                "type": "object",
                "properties": {
                    "source_path": {
                        "type": "string",
                        "description": "Path to the directory to clone for experimentation (e.g., 'D:/odibi/odibi/validation')",
                    },
                    "experiment_name": {
                        "type": "string",
                        "description": "Short descriptive name for the experiment (e.g., 'exp_data_profiler')",
                    },
                    "commands": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of shell commands to execute in the sandbox",
                    },
                    "description": {
                        "type": "string",
                        "description": "What this experiment attempts to do",
                    },
                    "hypothesis": {
                        "type": "string",
                        "description": "What you expect to happen",
                    },
                    "timeout": {
                        "type": "integer",
                        "description": "Timeout in seconds. Default: 120",
                    },
                },
                "required": ["source_path", "experiment_name", "commands"],
            },
        },
    },
]

TOOLS_REQUIRING_CONFIRMATION = {
    "write_file",
    "run_command",
    "odibi_run",
    "execute_python",
    "sql",
    "ruff",  # only when fix=True
    "explorer_experiment",  # runs commands in sandbox
    "implement_feature",  # writes code and runs tests
}


# Compound tool for implementing features with automatic testing
IMPLEMENT_FEATURE_TOOL = {
    "type": "function",
    "function": {
        "name": "implement_feature",
        "description": "Implement a feature with automatic test verification. Creates/modifies files, runs relevant tests, and iterates until tests pass. Use this for any code changes that should be tested.",
        "parameters": {
            "type": "object",
            "properties": {
                "description": {
                    "type": "string",
                    "description": "What feature/fix to implement",
                },
                "target_files": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Files to create or modify",
                },
                "test_pattern": {
                    "type": "string",
                    "description": "Pytest pattern to run (e.g., 'tests/unit/test_profiler.py' or '-k profiler')",
                },
                "max_iterations": {
                    "type": "integer",
                    "description": "Max fix iterations if tests fail. Default: 3",
                },
            },
            "required": ["description", "target_files"],
        },
    },
}

# Add to main tool list
TOOL_DEFINITIONS.append(IMPLEMENT_FEATURE_TOOL)

# Tool for finding related tests
FIND_TESTS_TOOL = {
    "type": "function",
    "function": {
        "name": "find_tests",
        "description": "Find test files related to a source file. Useful before making changes to know what tests to run.",
        "parameters": {
            "type": "object",
            "properties": {
                "source_file": {
                    "type": "string",
                    "description": "Path to source file (e.g., 'odibi/validation/engine.py')",
                },
            },
            "required": ["source_file"],
        },
    },
}
TOOL_DEFINITIONS.append(FIND_TESTS_TOOL)

# Tool for reviewing code before committing
CODE_REVIEW_TOOL = {
    "type": "function",
    "function": {
        "name": "code_review",
        "description": "Review uncommitted changes in the working directory. Shows diff and suggests improvements.",
        "parameters": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Specific file or directory to review. Default: entire repo",
                },
                "staged_only": {
                    "type": "boolean",
                    "description": "Only review staged changes. Default: false",
                },
            },
        },
    },
}
TOOL_DEFINITIONS.append(CODE_REVIEW_TOOL)


def get_tool_names() -> list[str]:
    """Get list of all tool names."""
    return [t["function"]["name"] for t in TOOL_DEFINITIONS]


def get_tool_by_name(name: str) -> dict | None:
    """Get a tool definition by name."""
    for tool in TOOL_DEFINITIONS:
        if tool["function"]["name"] == name:
            return tool
    return None

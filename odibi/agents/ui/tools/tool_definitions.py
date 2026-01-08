from __future__ import annotations

"""Tool definitions for native OpenAI function calling.

Defines all tools as JSON schemas that can be passed to the LLM API.
Each tool description follows best practices:
- WHEN to use (not just what it does)
- Relationship to other tools
- Common patterns and anti-patterns
"""

TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": """Read a file from the filesystem.

WHEN TO USE:
- To understand existing code before making changes
- To verify file contents after edits
- After search/grep finds relevant files

TIPS:
- For files >500 lines, use start_line/end_line to read sections
- Prefer grep to find specific content in large files
- Read related test files to understand expected behavior

Returns complete file contents with line numbers.""",
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
                        "description": "Ending line number (inclusive). Default: reads to end of file",
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
            "description": """Write content to a file. Creates parent directories if needed.

WHEN TO USE:
- Creating NEW files
- Complete file rewrites where most content changes
- For surgical edits (changing a few lines), prefer edit_file instead

CRITICAL RULES:
- ALWAYS write COMPLETE file content - never use placeholders
- NEVER truncate with '// rest of file...' or similar
- Read the file first if you need to preserve existing content

After writing, run ruff/pytest to verify the change works.""",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path to write to"},
                    "content": {
                        "type": "string",
                        "description": "COMPLETE content to write. Must include ALL code.",
                    },
                },
                "required": ["path", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "edit_file",
            "description": """Make surgical edits to a file by replacing specific text.

WHEN TO USE:
- Changing a few lines in a large file
- Adding imports, methods, or small blocks
- Fixing specific bugs or typos
- Prefer this over write_file when changing <30% of file

HOW IT WORKS:
- Finds exact match of old_str and replaces with new_str
- old_str must be unique in the file (or use replace_all=true)
- Include enough context in old_str to make it unique

EXAMPLE:
  old_str: "def foo():\n    return 1"
  new_str: "def foo():\n    return 2"

If old_str not found, the edit fails - read the file first to get exact text.""",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path to the file to edit"},
                    "old_str": {
                        "type": "string",
                        "description": "Exact text to find and replace. Must match exactly including whitespace.",
                    },
                    "new_str": {
                        "type": "string",
                        "description": "Text to replace old_str with.",
                    },
                    "replace_all": {
                        "type": "boolean",
                        "description": "Replace all occurrences. Default: false (old_str must be unique)",
                    },
                },
                "required": ["path", "old_str", "new_str"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_directory",
            "description": """List contents of a directory.

WHEN TO USE:
- Exploring unfamiliar project structure
- Finding files when you don't know exact names
- Verifying a file exists before reading

TIPS:
- Use pattern to filter (e.g., "*.py", "test_*")
- Use recursive=true sparingly - can return huge lists
- For finding specific files, prefer glob tool instead""",
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
            "description": """Search for exact text or regex patterns in files.

WHEN TO USE:
- Finding specific function/class/variable names
- Locating error messages or strings
- Finding all usages of an import

WHEN TO USE search INSTEAD:
- Looking for concepts ("error handling", "validation logic")
- Don't know exact names to search for

TIPS:
- ALWAYS set file_pattern (e.g., "*.py") to avoid searching binaries
- Use is_regex=true for patterns like "def test_.*"
- Combine with read_file to see full context of matches""",
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
                        "description": "Glob pattern for files to search. ALWAYS SET THIS. Default: *.py",
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
            "description": """Find files matching a glob pattern.

WHEN TO USE:
- Finding all files of a type (e.g., **/*.py)
- Locating test files (e.g., **/test_*.py)
- Finding config files (e.g., **/pyproject.toml)

TIPS:
- Use ** for recursive matching
- Returns paths only, not contents - follow up with read_file
- More efficient than list_directory with recursive=true""",
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
            "description": """Semantic search: find code by meaning using AI embeddings.

WHEN TO USE:
- Exploring unfamiliar codebase ("where is authentication handled?")
- Finding code by concept, not exact names
- Don't know what to grep for

WHEN TO USE grep INSTEAD:
- Know exact function/class/variable names
- Looking for specific strings or error messages

SEARCH STRATEGY:
1. Start BROAD: search("validation logic")
2. Then NARROW: read files found, grep for specifics
3. Try DIFFERENT WORDING if first search misses""",
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
            "description": """Execute a shell command.

WHEN TO USE:
- Running custom scripts
- Git operations not covered by git_* tools
- Installing packages (pip install)
- Any shell operation

PREFER SPECIALIZED TOOLS WHEN AVAILABLE:
- pytest → use pytest tool
- ruff → use ruff tool
- git status/diff/log → use git_* tools

TIPS:
- Set working_dir for commands that depend on cwd
- Long-running commands may timeout
- Check exit code in result for success/failure""",
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
            "description": """Run pytest tests.

WHEN TO USE:
- After making code changes to verify they work
- Before committing to catch regressions
- To understand expected behavior via test cases

TIPS:
- Use test_path to run specific tests (faster feedback)
- Use markers to filter (e.g., markers="-k validation")
- Run related tests BEFORE and AFTER changes to verify fix""",
            "parameters": {
                "type": "object",
                "properties": {
                    "test_path": {
                        "type": "string",
                        "description": "Path to test file or directory. Default: runs all tests",
                    },
                    "verbose": {"type": "boolean", "description": "Verbose output. Default: true"},
                    "markers": {"type": "string", "description": "Pytest markers to filter tests"},
                    "working_dir": {
                        "type": "string",
                        "description": "Working directory to run tests from",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ruff",
            "description": """Run ruff linter on Python code.

WHEN TO USE:
- AFTER making code changes to catch syntax/style errors
- Before committing to ensure code quality
- To find potential bugs and issues

TIPS:
- Run without fix=True first to SEE what will change
- Then run with fix=True to auto-fix safe issues
- Some issues require manual fixes - read the output""",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path to lint. Default: ."},
                    "fix": {
                        "type": "boolean",
                        "description": "Auto-fix issues. Default: false (just report)",
                    },
                    "working_dir": {
                        "type": "string",
                        "description": "Working directory to run from",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "diagnostics",
            "description": """Run multiple code quality checks (ruff, mypy, pytest).

WHEN TO USE:
- Comprehensive check before committing
- When you want all diagnostics at once
- Less useful than individual tools for iterative fixing

For iterative work, prefer individual ruff/pytest/typecheck tools.""",
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
            "description": """Run mypy type checker.

WHEN TO USE:
- After adding type hints to verify correctness
- To find type-related bugs
- When working with typed codebases

Note: Many projects don't use strict typing - check if mypy is configured.""",
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
            "description": """Search the web for documentation, examples, error solutions.

WHEN TO USE:
- Looking up library documentation
- Finding solutions to error messages
- Researching best practices
- When internal codebase search doesn't help

TIPS:
- Be specific: "pydantic v2 field_validator example"
- Include library/framework names
- Follow up with read_web_page to get full content""",
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
            "description": """Read and extract content from a web page.

WHEN TO USE:
- After web_search finds relevant pages
- Reading documentation pages
- Extracting code examples from tutorials

Returns cleaned text content, not raw HTML.""",
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
            "name": "plan",
            "description": """Create an explicit plan before taking action on complex tasks.

WHEN TO USE:
- Multi-step tasks with dependencies
- Tasks touching multiple files
- When you need to think through approach before coding
- Refactoring or architectural changes

HOW TO USE:
1. Describe the goal
2. List steps in order
3. Identify risks/dependencies
4. Execute steps, updating plan as you go

The plan is saved and can be referenced throughout the task.""",
            "parameters": {
                "type": "object",
                "properties": {
                    "goal": {
                        "type": "string",
                        "description": "What you're trying to accomplish",
                    },
                    "steps": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Ordered list of steps to complete the goal",
                    },
                    "risks": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Potential risks or things that could go wrong",
                    },
                    "dependencies": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "What must be true or done before starting",
                    },
                },
                "required": ["goal", "steps"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "todo_write",
            "description": """Update the task list for tracking progress.

WHEN TO USE:
- At START of complex tasks to plan steps
- When breaking down a large task
- To track progress and show user what's done/pending
- Mark items complete AS SOON AS they're done

STATUS VALUES (use exactly):
- "todo" - Not started
- "in-progress" - Currently working on
- "completed" - Done

TIPS:
- Keep items small and specific
- Update frequently - don't batch updates
- User can see this list, so keep it clear""",
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
            "description": """Read the current task list.

WHEN TO USE:
- To check what's been completed
- To review pending items
- When resuming work after a break""",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "mermaid",
            "description": """Render a Mermaid diagram (flowchart, sequence, class, etc.).

WHEN TO USE:
- Explaining architecture or data flow
- Visualizing class relationships
- Showing sequence of operations
- When a picture is worth 1000 words

DIAGRAM TYPES:
- flowchart: Process flows, decision trees
- sequenceDiagram: API calls, interactions
- classDiagram: Class relationships
- erDiagram: Database schemas""",
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
            "description": """Show git repository status (modified, staged, untracked files).

WHEN TO USE:
- Before committing to see what changed
- To check if working directory is clean
- To see which files are staged""",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "git_diff",
            "description": """Show git diff for files.

WHEN TO USE:
- Review changes before committing
- Understand what was modified
- Debug unexpected behavior after changes

TIPS:
- Use staged=true to see what's staged for commit
- Use path to focus on specific files""",
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
            "description": """Show recent commit history.

WHEN TO USE:
- Understanding recent changes to a file
- Finding when a bug was introduced
- Getting context on code evolution""",
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
            "description": """Run an Odibi pipeline.

WHEN TO USE:
- Testing pipeline configurations
- Validating data transformations
- Running data pipelines

TIPS:
- Use dry_run=true first to validate without executing
- Check engine compatibility (pandas vs spark)""",
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
                    "working_dir": {
                        "type": "string",
                        "description": "Working directory for the pipeline",
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
            "description": """Spawn a focused sub-agent to complete an independent task.

WHEN TO USE:
- Task is independent and doesn't need your current context
- Exploring multiple files/directories in parallel
- Repetitive analysis across components
- Work that can run while you continue with other things

WHEN NOT TO USE:
- Task depends on results of other work
- You need tight control over execution
- Simple operations (just do them yourself)

WHAT HAPPENS:
1. Sub-agent gets: the prompt, access to all tools
2. Sub-agent does NOT get: your conversation history
3. You receive: summary of what sub-agent accomplished

TIPS:
- Be SPECIFIC about what you want returned
- Include all necessary context in the prompt
- Tell sub-agent how to verify success""",
            "parameters": {
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "Complete task description with all context needed",
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
            "description": """Run multiple sub-agents in parallel for independent tasks.

WHEN TO USE:
- Multiple independent explorations
- Analyzing several files/modules simultaneously
- Tasks with no dependencies between them

WHEN NOT TO USE:
- Tasks depend on each other's results
- Need to coordinate between tasks
- Order of execution matters

EXAMPLE:
  tasks: [
    "Analyze authentication in src/auth/",
    "Analyze database layer in src/db/",
    "Analyze API routes in src/api/"
  ]

All tasks run simultaneously, results returned together.""",
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
            "description": """Execute Python code directly.

WHEN TO USE:
- Quick calculations or data manipulation
- Testing code snippets before writing to file
- In Databricks: has access to spark, pd, np
- Data exploration and analysis

TIPS:
- Keep code focused and short
- Print results you want to see
- Use for exploration, then write proper code to files""",
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
            "description": """Execute SQL query via Spark.

WHEN TO USE:
- Querying data tables
- Exploring database schemas
- Data analysis and aggregation

Only works in Spark-enabled environments (like Databricks).""",
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
            "description": """List available tables in the Spark catalog.

WHEN TO USE:
- Discovering available data tables
- Finding table names before querying

Only works in Spark-enabled environments.""",
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
            "description": """Get schema and sample data from a table.

WHEN TO USE:
- Understanding table structure before querying
- Seeing column names and types
- Getting sample data

Only works in Spark-enabled environments.""",
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
            "description": """Undo the last edit to a file.

WHEN TO USE:
- Made a mistake in write_file/edit_file
- Want to revert to previous version
- Testing changes and need to rollback

Only undoes the most recent edit - not a full history.""",
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
            "description": """Save an important memory for future sessions.

WHEN TO USE:
- User makes a design DECISION worth remembering
- You discover a LEARNING about the codebase
- A BUG_FIX that might help in the future
- User states a PREFERENCE for code style
- Important CONTEXT that should persist

BE PROACTIVE - if it seems important, save it without asking!

Memory is searchable across sessions via recall tool.""",
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
                        "description": "Type: decision, learning, bug_fix, preference, todo, feature, context",
                    },
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Tags for categorizing and searching",
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
            "description": """Search past memories for relevant context.

WHEN TO USE:
- START of complex tasks - check for relevant past context
- User asks about something discussed before
- Need to maintain consistency with past decisions
- Looking for previously learned patterns

Searches across all saved memories using semantic matching.""",
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
            "description": """Run an experiment in an isolated sandbox environment.

WHEN TO USE:
- Testing risky changes safely
- Experimenting with code without affecting main codebase
- Trying multiple approaches to find best solution

HOW IT WORKS:
1. Creates a clone of source_path
2. Runs commands in the isolated sandbox
3. Returns diff of changes made
4. Original codebase is NOT affected

TIPS:
- Include hypothesis of what you expect
- Use for exploratory work, then apply winning approach to real code""",
            "parameters": {
                "type": "object",
                "properties": {
                    "source_path": {
                        "type": "string",
                        "description": "Path to clone for experimentation",
                    },
                    "experiment_name": {
                        "type": "string",
                        "description": "Short descriptive name (e.g., 'exp_data_profiler')",
                    },
                    "commands": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Shell commands to execute in sandbox",
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
    "run_command",  # arbitrary shell commands
    "sql",  # database writes
    # File operations (write_file, edit_file) now run without confirmation
    # for faster autonomous workflows. Git provides undo safety.
}


# Compound tool for implementing features with automatic testing
IMPLEMENT_FEATURE_TOOL = {
    "type": "function",
    "function": {
        "name": "implement_feature",
        "description": """Implement a feature with automatic test verification.

WHEN TO USE:
- Any code change that should be tested
- Feature implementation with existing tests
- Bug fixes where you want to verify the fix

HOW IT WORKS:
1. Creates/modifies specified files
2. Runs relevant tests
3. If tests fail, iterates to fix (up to max_iterations)
4. Returns final state and test results

More autonomous than manual edit → test → fix cycle.""",
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
                    "description": "Pytest pattern to run (e.g., 'tests/unit/test_profiler.py')",
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
TOOL_DEFINITIONS.append(IMPLEMENT_FEATURE_TOOL)

# Tool for finding related tests
FIND_TESTS_TOOL = {
    "type": "function",
    "function": {
        "name": "find_tests",
        "description": """Find test files related to a source file.

WHEN TO USE:
- Before making changes, to know what tests to run
- To understand how code is tested
- Finding test examples for similar code

Returns paths to related test files based on naming conventions.""",
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
        "description": """Review uncommitted changes and suggest improvements.

WHEN TO USE:
- Before committing changes
- To get a second opinion on code quality
- Finding potential issues or improvements

Shows diff and provides feedback on code quality, style, potential bugs.""",
        "parameters": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Specific file or directory. Default: entire repo",
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

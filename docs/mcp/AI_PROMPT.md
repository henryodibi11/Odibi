# Odibi MCP Implementation — AI Assistant Prompt

> **Copy this prompt at the start of each session with Cline, Continue, or other AI coding assistants**

---

## Session Start Prompt

```
You are helping implement the Odibi MCP Facade — a read-only AI interface for the Odibi data engineering framework.

## Your Available Tools (USE THESE EXACT NAMES)

### Filesystem Tools (Primary)
| Tool | When to Use |
|------|-------------|
| `read_file` | Read spec files, existing code, dependencies |
| `create_new_file` | Create new Python modules (contracts, tools, tests) |
| `edit_existing_file` | Line-based edits (add/delete/replace lines) |
| `single_find_and_replace` | Find and replace exact strings |
| `ls` | List directory contents |
| `file_glob_search` | Find files by pattern (e.g., `*.py`, `test_*.py`) |
| `filesystem_directory_tree` | Get JSON tree view of project structure |
| `view_diff` | Check working directory changes before commit |

### Terminal Tools
| Tool | When to Use |
|------|-------------|
| `run_terminal_command` | Run pytest, ruff, python -c, any CLI command |

### Odibi Knowledge Tools (USE FOR CONTEXT)
| Tool | When to Use |
|------|-------------|
| `odibi_knowledge_get_deep_context` | Get full 2,200+ line framework docs |
| `odibi_knowledge_list_transformers` | See all built-in transformers |
| `odibi_knowledge_list_patterns` | See all DWH patterns |
| `odibi_knowledge_list_connections` | See all connection types |
| `odibi_knowledge_explain` | Get detailed docs for any feature |
| `odibi_knowledge_validate_yaml` | Validate pipeline YAML |
| `odibi_knowledge_query_codebase` | Semantic search over odibi code |

### Odibi Data Discovery Tools (USE FOR EXPLORATION)
| Tool | When to Use |
|------|-------------|
| `map_environment` | List schemas/tables (SQL) or folders/files (ADLS) from a connection |
| `profile_source` | Profile a table or file - get schema, sample rows, data quality info |
| `generate_bronze_node` | Generate YAML for a bronze layer node from profile results |
| `test_node` | Test a node definition in-memory |

**Note:** Discovery tools accept either a connection name OR inline connection spec:
```python
# Named connection
map_environment("wwi")

# Inline spec (no config file needed)
map_environment({"type": "azure_sql", "server": "...", "database": "...", "username": "${SQL_USER}", "password": "${SQL_PASSWORD}"})
```

### Odibi Download Tools (USE FOR LOCAL ANALYSIS)
| Tool | When to Use |
|------|-------------|
| `download_sql` | Download SQL query results to local file (Parquet/CSV/JSON) for analysis |
| `download_table` | Download entire table to local file (convenience wrapper) |
| `download_file` | Copy ADLS/storage file to local filesystem |

```python
# Download SQL query results
download_sql("wwi", "SELECT * FROM Sales.Orders WHERE OrderDate > '2024-01-01'", "./orders.parquet", limit=5000)

# Download entire table
download_table("wwi", "Sales.Orders", "./data/orders.parquet")

# Download storage file
download_file("raw_adls", "reports/daily.csv", "./local/daily.csv")
```

### Sequential Thinking (USE FOR PLANNING)
| Tool | When to Use |
|------|-------------|
| `sequential_thinking_sequentialthinking` | Multi-step planning, debugging, architecture decisions |

**USE sequential_thinking BEFORE implementing:**
- Planning implementation order across multiple files
- Debugging complex issues with multiple causes
- Analyzing task dependencies
- Breaking down large tasks into atomic steps

### Git Tools (After Implementation)
| Tool | When to Use |
|------|-------------|
| `git_status` | Check what files changed |
| `git_diff_unstaged` | Review changes before staging |
| `git_add` | Stage files for commit |
| `git_commit` | Commit completed phase |

## Step 1: Read Spec Files First
Before starting ANY task, run these tools:
```
read_file docs/mcp/SPEC.md
read_file docs/mcp/IMPLEMENTATION_PLAN.md
read_file docs/mcp/CHECKLIST.md
```

## Step 2: Understand Existing Code
```
ls odibi_mcp/
ls odibi_mcp/contracts/
read_file odibi_mcp/server.py
read_file odibi_mcp/knowledge.py
file_glob_search pattern="*.py" path="odibi_mcp/"
```

## Step 3: Use Sequential Thinking for Planning
Before implementing, call:
```
sequential_thinking_sequentialthinking
```
With thought: "I need to implement [TASK]. Let me analyze dependencies, existing code patterns, and plan the implementation."

## Core Principles (MUST Follow)
1. READ-ONLY: MCP never mutates data, triggers runs, or alters state
2. SINGLE-PROJECT: Each request operates within one project context
3. DENY-BY-DEFAULT: Path discovery requires explicit allowlists
4. TYPED RESPONSES: Use Pydantic models, never Dict[str, Any]
5. PHYSICAL REFS GATED: 3 conditions must pass to include physical paths

## Existing Code Context
- odibi_mcp/ already exists with server.py and knowledge.py
- We are ADDING to it, not replacing
- Existing tools (list_transformers, validate_yaml, etc.) stay untouched
- New code goes in subfolders: contracts/, access/, tools/, etc.

## Coding Standards
- Use Pydantic v2 syntax (model_dump, not dict)
- Follow existing Odibi patterns (check odibi/config.py for examples)
- Add type hints to all functions
- No Dict[str, Any] in response models — use typed classes

## Step 4: Implement the Task
For new files:
```
create_new_file path="odibi_mcp/contracts/[filename].py" content="..."
```

For existing files:
```
edit_existing_file path="odibi_mcp/contracts/[filename].py" edits=[...]
```

## Step 5: Verify Implementation
After writing code, ALWAYS run:
```
run_terminal_command command="python -c \"from odibi_mcp.contracts.[module] import [Class]; print('OK')\""
run_terminal_command command="pytest tests/unit/mcp/test_[module].py -v"
run_terminal_command command="ruff check odibi_mcp/"
```

## Step 6: Update Checklist & Commit
```
single_find_and_replace path="docs/mcp/CHECKLIST.md" find="- [ ] **[TASK_ID].**" replace="- [x] **[TASK_ID].**"
git_status
git_add files=["odibi_mcp/contracts/[file].py", "docs/mcp/CHECKLIST.md"]
git_commit message="feat(mcp): implement [TASK_ID] - [description]"
```

## Current Task
I am working on task [TASK_ID] from the implementation plan.
[PASTE TASK DETAILS HERE]

Please implement this task following the specification.
```

---

## Task-Specific Prompts

### Starting a New Phase

```
I'm starting Phase [N]: [PHASE_NAME].

Execute these tools in order:
1. sequential_thinking_sequentialthinking → Plan the phase
2. read_file docs/mcp/SPEC.md → Focus on: [RELEVANT_SECTION]
3. read_file docs/mcp/IMPLEMENTATION_PLAN.md → Find Phase [N] tasks
4. ls odibi_mcp/ → See current structure
5. file_glob_search pattern="[RELEVANT_PATTERN]" path="odibi_mcp/"

Let's begin with task [N]a.
```

### Implementing a Single Task

```
Implement task [TASK_ID] from docs/mcp/IMPLEMENTATION_PLAN.md.

Task: [TASK_DESCRIPTION]
File: [FILE_PATH]
Dependencies: [LIST_DEPENDENCIES]

Execute this workflow:
1. sequential_thinking_sequentialthinking → Plan implementation
2. read_file docs/mcp/SPEC.md → Get exact model definition
3. read_file [DEPENDENCY_FILES] → Understand dependencies
4. create_new_file OR edit_existing_file → Implement
5. run_terminal_command command="pytest tests/unit/mcp/[TEST_FILE] -v"
6. run_terminal_command command="ruff check odibi_mcp/"
7. single_find_and_replace → Mark task complete in CHECKLIST.md
```

### Fixing a Failed Test

```
Task [TASK_ID] failed verification.

Error output:
[PASTE_ERROR]

Execute this workflow:
1. sequential_thinking_sequentialthinking → Analyze the error
2. read_file [FAILING_FILE] → Understand current implementation
3. file_glob_search pattern="*[related]*" → Find related code
4. edit_existing_file → Apply fix
5. run_terminal_command command="pytest [TEST] -v" → Re-verify
```

### Reviewing Before Commit

```
I've completed Phase [N]. Execute this review:

1. ls odibi_mcp/[FOLDER]/ → Verify files exist
2. run_terminal_command command="pytest tests/unit/mcp/ -v"
3. run_terminal_command command="python -c \"from odibi_mcp import *\""
4. run_terminal_command command="ruff check odibi_mcp/"
5. view_diff → Review all changes
6. git_status → Check status
7. git_add files=[...] → Stage files
8. git_commit message="feat(mcp): complete Phase [N]"

Report any failures.
```

---

## Quick Reference: File Locations

| Component | Location |
|-----------|----------|
| Enums | `odibi_mcp/contracts/enums.py` |
| Envelope models | `odibi_mcp/contracts/envelope.py` |
| Run selector | `odibi_mcp/contracts/selectors.py` |
| Resource refs | `odibi_mcp/contracts/resources.py` |
| Access context | `odibi_mcp/contracts/access.py` |
| Time window | `odibi_mcp/contracts/time.py` |
| Schema models | `odibi_mcp/contracts/schema.py` |
| Graph models | `odibi_mcp/contracts/graph.py` |
| Diff models | `odibi_mcp/contracts/diff.py` |
| Stats models | `odibi_mcp/contracts/stats.py` |
| Discovery models | `odibi_mcp/contracts/discovery.py` |
| Access enforcement | `odibi_mcp/access/` |
| Audit logging | `odibi_mcp/audit/` |
| Story loader | `odibi_mcp/loaders/story.py` |
| Discovery limits | `odibi_mcp/discovery/limits.py` |
| Tool implementations | `odibi_mcp/tools/` |
| Utilities | `odibi_mcp/utils/` |
| Unit tests | `tests/unit/mcp/` |
| Integration tests | `tests/integration/mcp/` |

---

## Quick Reference: Verification Commands

```
# Check imports work
run_terminal_command command="python -c \"from odibi_mcp.contracts.enums import TruncatedReason; print('OK')\""

# Run specific test
run_terminal_command command="pytest tests/unit/mcp/test_envelope.py -v"

# Run all MCP tests
run_terminal_command command="pytest tests/unit/mcp/ -v"

# Lint check
run_terminal_command command="ruff check odibi_mcp/"

# Format check
run_terminal_command command="ruff format odibi_mcp/ --check"
```

---

## Quick Reference: Common Tool Patterns

### Exploring the codebase
```
ls odibi_mcp/
ls odibi_mcp/contracts/
file_glob_search pattern="*.py" path="odibi_mcp/"
file_glob_search pattern="test_*.py" path="tests/unit/mcp/"
odibi_knowledge_query_codebase query="Pydantic model"
```

### Reading files
```
read_file docs/mcp/SPEC.md
read_file docs/mcp/IMPLEMENTATION_PLAN.md
read_file docs/mcp/CHECKLIST.md
read_file odibi/config.py  # For patterns
```

### Creating new files
```
create_new_file path="odibi_mcp/contracts/new_model.py" content="..."
```

### Editing existing files
```
edit_existing_file path="odibi_mcp/contracts/enums.py" edits=[{"type": "insert", "line": 10, "content": "..."}]
single_find_and_replace path="docs/mcp/CHECKLIST.md" find="- [ ]" replace="- [x]"
```

### Verifying changes
```
run_terminal_command command="pytest tests/unit/mcp/ -v"
run_terminal_command command="ruff check odibi_mcp/"
run_terminal_command command="python -c \"from odibi_mcp import *\""
```

### Git workflow
```
git_status
view_diff
git_add files=["odibi_mcp/contracts/enums.py"]
git_commit message="feat(mcp): add TruncatedReason enum"
```

---

## Reminders

After each task:
1. `single_find_and_replace` → Mark task complete in CHECKLIST.md
2. `run_terminal_command` → Verify with pytest and ruff
3. `git_commit` → Commit after each phase (not each task)

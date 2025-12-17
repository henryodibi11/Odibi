# Odibi Agents - Complete Usage Guide

A comprehensive guide to using the Odibi AI Agent Suite for code understanding, testing, documentation, and improvement.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Installation](#installation)
3. [Launching the UI](#launching-the-ui)
4. [Multi-Repo Indexing](#multi-repo-indexing)
5. [The Gradio UI](#the-gradio-ui)
6. [Specialized Agents](#specialized-agents)
7. [Memory System](#memory-system)
8. [Cycles & Scheduled Work](#cycles--scheduled-work)
9. [Tools Reference](#tools-reference)
10. [API Reference](#api-reference)
11. [Databricks Usage](#databricks-usage)
12. [Troubleshooting](#troubleshooting)

---

## Quick Start

### Local Usage (No Cloud Required)

```python
import sys
sys.path.insert(0, "d:/odibi")

from odibi.agents.ui import launch

# Index odibi and your project, then launch UI
launch(repos=["d:/odibi", "d:/my_project"])
```

### With LLM Chat (Azure OpenAI)

```python
import os
from odibi.agents import AgentRunner, AzureConfig

config = AzureConfig(
    openai_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    openai_api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    chat_deployment="gpt-4o",
)

runner = AgentRunner(config)
response = runner.ask("How does the NodeExecutor work?")
print(response.content)
```

---

## Installation

### Via pip (when published)

```bash
pip install odibi[agents]
```

### From Source

```bash
cd odibi
pip install -e ".[agents]"
```

### Required Dependencies

| Package | Purpose |
|---------|---------|
| `chromadb>=0.4.0` | Local vector storage |
| `sentence-transformers>=2.2.0` | Local embeddings |
| `gradio>=4.0.0` | Web UI |

### Optional Dependencies

| Package | Purpose |
|---------|---------|
| `openai` | Azure OpenAI / OpenAI chat |
| `tiktoken` | Token counting |
| `requests` | Web tools |

---

## Launching the UI

### Basic Launch

```python
from odibi.agents.ui import launch

# Simple launch
launch()

# With a working project
launch(working_project="d:/my_project")
```

### Multi-Repo Launch

```python
# Index multiple repos into a single searchable index
launch(repos=["d:/odibi", "d:/my_project", "d:/another_repo"])
```

### Launch Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `working_project` | `""` | Root directory of project to work on |
| `repos` | `None` | List of repo paths to index |
| `index_dir` | `{repos[0]}/.odibi/index` | Where to store the vector index |
| `reindex` | `False` | Clear and rebuild the index |
| `share` | `False` | Create public Gradio link |
| `server_port` | Auto | Port for local server |

### Examples

```python
# Force reindex after code changes
launch(repos=["d:/odibi"], reindex=True)

# Custom index location
launch(repos=["d:/odibi", "d:/work"], index_dir="d:/.shared_index")

# Public link for sharing
launch(repos=["d:/odibi"], share=True)

# Specific port
launch(repos=["d:/odibi"], server_port=7860)
```

---

## Multi-Repo Indexing

### How It Works

1. **Parsing**: Each repo is parsed for Python files, extracting:
   - Modules (docstrings, imports, structure)
   - Classes (signatures, bases, methods)
   - Functions/Methods (signatures, docstrings, dependencies)

2. **Embedding**: Code chunks are converted to vectors using `sentence-transformers`

3. **Storage**: Vectors stored in ChromaDB with `repo` metadata for filtering

4. **Search**: Semantic search finds relevant code across all indexed repos

### Programmatic Indexing

```python
from odibi.agents import MultiRepoIndexer

# Index multiple repos
indexer = MultiRepoIndexer(
    repos=["d:/odibi", "d:/my_project"],
    index_dir="d:/.odibi/index",
)
result = indexer.run_indexing(force_recreate=True)

print(f"Indexed {result['total_chunks']} chunks from {result['total_repos']} repos")
print(f"Per-repo: {result['repos']}")
```

### Single Repo Indexing

```python
from odibi.agents import LocalIndexer

indexer = LocalIndexer(
    root="d:/my_project",
    repo_name="my_project",  # Optional, defaults to folder name
)
result = indexer.run_indexing()
```

### Managing the Index

```python
from odibi.agents import ChromaVectorStore

store = ChromaVectorStore(persist_dir="d:/.odibi/index")

# List indexed repos
repos = store.list_repos()
print(repos)  # ['odibi', 'my_project']

# Count chunks per repo
print(store.count())  # Total
print(store.count(repo="odibi"))  # Just odibi

# Delete a repo from index
store.delete_repo("old_project")

# Clear everything
store.delete_all()
```

### Searching the Index

```python
from odibi.agents import LocalEmbedder, ChromaVectorStore

embedder = LocalEmbedder()
store = ChromaVectorStore(persist_dir="d:/.odibi/index")

# Create query embedding
query_vec = embedder.embed_query("How does SCD transformation work?")

# Search all repos
results = store.similarity_search(query_vec, k=10)

# Search specific repo
results = store.similarity_search(query_vec, k=10, filters={"repo": "odibi"})

for r in results:
    print(f"{r['repo']}/{r['file_path']}:{r['line_start']} - {r['name']}")
```

---

## The Gradio UI

### UI Panels Overview

The UI has multiple panels organized into tabs/sections:

#### Left Column

| Panel | Purpose |
|-------|---------|
| **Settings** | Configure LLM endpoint, API key, model |
| **TODO List** | Track tasks and progress |
| **Conversations** | Save/load chat sessions |
| **Memories** | View stored insights and decisions |

#### Right Column (Main Area)

| Panel | Purpose |
|-------|---------|
| **Chat** | Main conversational interface |
| **Cycles** | Run structured improvement cycles |
| **Guided Execution** | Step-by-step pipeline execution |
| **Issue Discovery** | Find and classify codebase issues |
| **Escalation** | Handle complex issues requiring review |
| **Explorer** | Browse and experiment with code |
| **Campaigns** | Run multi-cycle improvement campaigns |

### Settings Panel

Configure your LLM connection:

```
Endpoint: https://your-resource.openai.azure.com/
API Key: your-api-key
Model: gpt-4o
API Type: azure (or openai)
```

**Supported Providers:**

| Provider | Endpoint | API Type |
|----------|----------|----------|
| Azure OpenAI | `https://your-resource.openai.azure.com/` | `azure` |
| OpenAI | `https://api.openai.com/v1` | `openai` |
| Ollama | `http://localhost:11434/v1` | `openai` |
| LM Studio | `http://localhost:1234/v1` | `openai` |

### Chat Interface

The main chat supports:

- **Natural language queries** about your codebase
- **Tool execution** (file read/write, search, shell commands)
- **Streaming responses** with visible thinking
- **Activity feed** showing tool calls and results
- **Token/cost tracking**

**Example Queries:**

```
"How does the NodeExecutor handle errors?"
"Show me all SCD-related transformers"
"What's the dependency graph for pipeline.py?"
"Find where we validate config schemas"
```

### Conversations Panel

- **Save** current chat to continue later
- **Load** previous conversations
- **Branch** from any point to explore alternatives
- **Export** conversations as markdown

### TODO Panel

Track your tasks:

- Add tasks with `/todo add <task>`
- Mark complete with `/todo done <id>`
- View all with `/todo list`

### Memories Panel

View and search stored memories:

- **Decisions** - Design choices and rationale
- **Learnings** - Bug fixes, patterns discovered
- **Preferences** - Your coding style preferences
- **Context** - What you were working on

**Commands:**

```
/remember <text>     - Manually save a memory
/recall <query>      - Search past memories
/memories            - Show recent memories
```

---

## Specialized Agents

### Agent Overview

| Agent | Role | Keywords |
|-------|------|----------|
| **Code Analyst (CAA)** | Understand code | "explain", "how does", "where is", "find" |
| **Refactor Engineer (REA)** | Improve code | "improve", "refactor", "cleanup", "type hint" |
| **Test Architect (TAA)** | Generate tests | "test", "unit test", "pytest", "coverage" |
| **Documentation (DA)** | Create docs | "document", "tutorial", "api doc", "readme" |
| **Orchestrator (ROA)** | Route queries | Auto-routes based on keywords |

### Using Agents Programmatically

```python
from odibi.agents import AgentRunner, AzureConfig
import os

config = AzureConfig(
    openai_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    openai_api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    chat_deployment="gpt-4o",
)

runner = AgentRunner(config)
```

#### Code Analyst (Understanding)

```python
# How does something work?
response = runner.ask("How does the Pipeline class orchestrate node execution?")

# Where is something implemented?
response = runner.ask("Where is the SCD Type 2 logic implemented?")

# Trace dependencies
response = runner.ask("What are all the dependencies of NodeExecutor?")

# Find patterns
response = runner.ask("Show me all uses of the @transform decorator")
```

#### Refactor Engineer (Improvement)

```python
# Suggest improvements
response = runner.ask("Suggest improvements for the node.py module")

# Type hints
response = runner.ask("What type hints should I add to the Pipeline class?")

# Pattern compliance
response = runner.ask("Does config.py follow Pydantic best practices?")

# Code review
response = runner.ask("Review the error handling in NodeExecutor")
```

#### Test Architect (Testing)

```python
# Generate unit tests
response = runner.ask("Generate unit tests for the SCD transformer")

# Integration tests
response = runner.ask("Create integration tests for the Pipeline class")

# Test fixtures
response = runner.ask("What test fixtures do I need for testing NodeConfig?")

# Coverage gaps
response = runner.ask("What tests are missing for the validation module?")
```

#### Documentation Agent

```python
# API documentation
response = runner.ask("Document the public API of the Pipeline class")

# Tutorials
response = runner.ask("Create a tutorial for writing custom transformers")

# YAML schema docs
response = runner.ask("Generate YAML schema documentation for NodeConfig")
```

#### Direct Agent Selection

```python
from odibi.agents import AgentRole

# Force specific agent instead of auto-routing
response = runner.ask(
    "Analyze the config module",
    agent_role=AgentRole.CODE_ANALYST
)

response = runner.ask(
    "Generate tests for merge_transformer",
    agent_role=AgentRole.TEST_ARCHITECT
)
```

### Multi-Agent Workflows

```python
# Full audit combines all agents
audit = runner.audit_component("SCD transformer")
print(audit["analysis"])       # From Code Analyst
print(audit["improvements"])   # From Refactor Engineer
print(audit["tests"])          # From Test Architect
print(audit["documentation"])  # From Documentation Agent
```

---

## Memory System

### What It Remembers

| Type | Purpose | Example |
|------|---------|---------|
| `DECISION` | Design choices | "Use match/case for operation dispatch" |
| `LEARNING` | Bug fixes, insights | "SCD requires sorted input" |
| `PREFERENCE` | Style preferences | "Prefer explicit imports" |
| `TODO` | Future tasks | "Add retry logic to Node" |
| `CONTEXT` | Session context | "Working on validation module" |

### Storing Memories

```python
from odibi.agents.core.memory import MemoryManager, MemoryType

manager = MemoryManager(backend_type="local")

# Manually store a memory
manager.remember(
    content="We decided to use Pydantic for all config validation",
    summary="Use Pydantic for configs",
    memory_type=MemoryType.DECISION,
    tags=["config", "pydantic", "validation"],
    importance=0.9,
)
```

### Retrieving Memories

```python
# Search by query
memories = manager.recall("how do we validate configs?")

# Get recent memories
memories = manager.store.get_recent(days=7, limit=10)

# Filter by type
memories = manager.store.get_by_type(MemoryType.DECISION)

# Filter by tags
memories = manager.store.get_by_tags(["scd", "transformer"])
```

### Memory Storage Backends

| Backend | Location | Best For |
|---------|----------|----------|
| `local` | `.odibi/memories/` JSON files | Development |
| `odibi` | Any odibi connection | Cloud storage |
| `delta` | Delta table | Databricks |

```python
# Local (default)
manager = MemoryManager(backend_type="local")

# ADLS via odibi connection
manager = MemoryManager(
    backend_type="odibi",
    connection=connections["adls"],
    engine=pandas_engine,
    path_prefix="agent/memories",
)

# Delta table (Databricks)
manager = MemoryManager(
    backend_type="delta",
    connection=connections["silver"],
    engine=spark_engine,
    table_path="system.agent_memories",
)
```

---

## Cycles & Scheduled Work

### What is a Cycle?

A **Cycle** is a bounded, auditable unit of work:

1. **Environment Validation** - Check Python, dependencies
2. **Project Selection** - Choose what to work on
3. **User Execution** - Run pipelines like a human
4. **Observation** - Classify failures and pain points
5. **Improvement Proposal** - Suggest code changes
6. **Review** - Approve or reject changes
7. **Regression Checks** - Verify nothing broke
8. **Memory Persistence** - Store learnings
9. **Summary Generation** - Create report
10. **Exit** - Cycle ends

### Running Cycles (UI)

1. Go to **Cycles** tab
2. Select cycle type:
   - **Interactive** - Manual control
   - **Guided** - Step-by-step
   - **Scheduled** - Background work
3. Click **Start Cycle**
4. Follow prompts

### Running Cycles (API)

```python
from odibi.agents import CycleRunner, CycleConfig

config = CycleConfig(
    max_steps=10,
    project_root="d:/odibi/examples",
)

runner = CycleRunner(config)
state = runner.run_cycle()

print(f"Completed: {state.completed_steps}")
print(f"Observations: {state.observations}")
```

### Campaigns (Multiple Cycles)

A **Campaign** runs multiple cycles with goals:

```python
from odibi.agents.improve.campaign import create_campaign_runner
from odibi.agents.improve.config import CampaignConfig

config = CampaignConfig(
    max_cycles=5,
    goal="Improve test coverage for transformers",
    stop_on_regression=True,
)

runner = create_campaign_runner(config)
runner.run()
```

### Issue Discovery

Find and classify codebase issues:

```python
from odibi.agents.core.issue_discovery import IssueDiscoveryEngine

engine = IssueDiscoveryEngine(odibi_root="d:/odibi")
issues = engine.discover_issues()

for issue in issues:
    print(f"{issue.severity}: {issue.title}")
    print(f"  File: {issue.file_path}")
    print(f"  Type: {issue.issue_type}")
```

---

## Tools Reference

### File Tools

| Tool | Description |
|------|-------------|
| `read_file` | Read file contents with line numbers |
| `write_file` | Create or update files |
| `list_directory` | List directory contents |

### Search Tools

| Tool | Description |
|------|-------------|
| `glob` | Find files by pattern |
| `grep` | Search for patterns in files |
| `semantic_search` | RAG-based code search |

### Shell Tools

| Tool | Description |
|------|-------------|
| `run_command` | Execute shell commands |
| `run_pytest` | Run tests with pytest |
| `run_ruff` | Lint with ruff |

### Git Tools

| Tool | Description |
|------|-------------|
| `git_status` | Show repository status |
| `git_diff` | Show changes |
| `git_log` | Show commit history |

### Web Tools

| Tool | Description |
|------|-------------|
| `web_search` | Search the web |
| `read_web_page` | Fetch and parse web pages |

### Diagram Tools

| Tool | Description |
|------|-------------|
| `mermaid` | Generate Mermaid diagrams |

### Task Tools

| Tool | Description |
|------|-------------|
| `todo_write` | Update task list |
| `todo_read` | Read task list |

---

## API Reference

### Core Classes

```python
from odibi.agents import (
    # Configuration
    AzureConfig,

    # Agents
    AgentRunner,
    AgentRunnerConfig,
    AgentRole,
    AgentContext,
    AgentResponse,
    OdibiAgent,

    # Indexing
    LocalIndexer,
    MultiRepoIndexer,
    ChromaVectorStore,
    LocalEmbedder,

    # Cycles
    CycleRunner,
    CycleConfig,
    CycleState,

    # Execution
    ExecutionGateway,
    TaskDefinition,
)
```

### AzureConfig

```python
config = AzureConfig(
    openai_endpoint="https://your-resource.openai.azure.com/",
    openai_api_key="your-key",
    chat_deployment="gpt-4o",  # Your deployment name
    embedding_deployment="text-embedding-3-large",  # Optional
    embedding_dimensions=3072,  # Match your model
)

# Or load from environment
config = AzureConfig.from_env()
```

### AgentRunner

```python
runner = AgentRunner(azure_config)

# Simple query (auto-routed)
response = runner.ask("How does X work?")
print(response.content)
print(response.sources)  # Code references
print(response.confidence)

# Specific agent
response = runner.ask("Generate tests", agent_role=AgentRole.TEST_ARCHITECT)

# With conversation history
response = runner.ask("Explain more", conversation_history=previous_messages)
```

### LocalIndexer

```python
indexer = LocalIndexer(
    root="d:/my_project",
    repo_name="my_project",
    index_dir="d:/.odibi/index",
)

# Run indexing
result = indexer.run_indexing(embedding_batch_size=16)
print(result)  # {'total_chunks': 500, 'uploaded': 500, ...}
```

### MultiRepoIndexer

```python
indexer = MultiRepoIndexer(
    repos=["d:/odibi", "d:/project1", "d:/project2"],
    index_dir="d:/.shared_index",
)

result = indexer.run_indexing(force_recreate=True)
print(result["repos"])  # Per-repo stats
```

### ChromaVectorStore

```python
store = ChromaVectorStore(persist_dir="d:/.odibi/index")

# Add chunks
result = store.add_chunks(chunks_with_embeddings)

# Search
results = store.similarity_search(
    query_embedding=vector,
    k=10,
    filters={"repo": "odibi", "chunk_type": "function"},
)

# Management
repos = store.list_repos()
count = store.count(repo="odibi")
store.delete_repo("old_repo")
store.delete_all()
```

---

## Databricks Usage

### Cluster Setup

Install these packages on your cluster:

| Package | Version |
|---------|---------|
| `gradio` | `>=4.0.0` |
| `chromadb` | `>=0.4.0` |
| `sentence-transformers` | `>=2.2.0` |
| `markdown2` | Latest |

### Notebook Launch

```python
# In Databricks notebook
import sys
sys.path.insert(0, "/Workspace/Users/you/odibi")

from odibi.agents.ui import launch

# Renders inline in notebook
launch(repos=["/Workspace/Users/you/odibi"])
```

### Using DBFS for Index Storage

```python
launch(
    repos=["/Workspace/Users/you/odibi"],
    index_dir="/dbfs/odibi/agent_index",
)
```

### Memory Storage with Delta

```python
from odibi.agents.core.memory import MemoryManager

manager = MemoryManager(
    backend_type="delta",
    connection=spark_connection,
    engine=spark_engine,
    table_path="catalog.schema.agent_memories",
)
```

---

## Troubleshooting

### Common Issues

#### "chromadb not found"

```bash
pip install chromadb>=0.4.0
```

#### "sentence-transformers not found"

```bash
pip install sentence-transformers>=2.2.0
```

#### "Duplicate ID" errors during indexing

```python
# Clear and rebuild the index
launch(repos=["d:/odibi"], reindex=True)
```

#### Slow embedding on first run

The embedding model downloads on first use (~90MB). Pre-load:

```python
from odibi.agents import LocalEmbedder
embedder = LocalEmbedder()
_ = embedder.dimension  # Force load
```

#### LLM connection errors

1. Check your API key is set correctly
2. Verify endpoint URL format
3. Ensure deployment name matches Azure

#### "No relevant code found"

1. Ensure the repo is indexed
2. Try more specific queries
3. Check `store.count()` to verify index has data

### Getting Help

```python
# Check index status
from odibi.agents import ChromaVectorStore
store = ChromaVectorStore(persist_dir="d:/.odibi/index")
print(f"Total chunks: {store.count()}")
print(f"Repos: {store.list_repos()}")

# Test embedding
from odibi.agents import LocalEmbedder
embedder = LocalEmbedder()
vec = embedder.embed_query("test")
print(f"Dimension: {len(vec)}")

# Test search
results = store.similarity_search(vec, k=3)
for r in results:
    print(f"{r['repo']}: {r['name']}")
```

---

## Cheat Sheet

### Quick Commands

```python
# Launch UI with indexing
from odibi.agents.ui import launch
launch(repos=["d:/odibi", "d:/my_project"])

# Force reindex
launch(repos=["d:/odibi"], reindex=True)

# Programmatic search
from odibi.agents import LocalEmbedder, ChromaVectorStore
embedder = LocalEmbedder()
store = ChromaVectorStore(persist_dir="d:/.odibi/index")
results = store.similarity_search(embedder.embed_query("SCD"), k=5)

# Ask agents
from odibi.agents import AgentRunner, AzureConfig
runner = AgentRunner(AzureConfig.from_env())
response = runner.ask("How does X work?")
```

### Chat Commands

```
/todo add <task>      - Add a task
/todo done <id>       - Complete a task
/remember <text>      - Save a memory
/recall <query>       - Search memories
/memories             - Show recent memories
/save                 - Save conversation
/load                 - Load conversation
```

### Environment Variables

```bash
# Azure OpenAI
export AZURE_OPENAI_ENDPOINT="https://your-resource.openai.azure.com/"
export AZURE_OPENAI_API_KEY="your-key"
export AZURE_OPENAI_CHAT_DEPLOYMENT="gpt-4o"

# OpenAI
export OPENAI_API_KEY="sk-..."
```

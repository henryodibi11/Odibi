# Odibi AI Agent Suite

A multi-agent system for understanding, testing, documenting, and improving the Odibi data engineering framework. Powered by Azure OpenAI (GPT-4.1) and Azure AI Search.

## Architecture Overview

```mermaid
flowchart TB
    subgraph User["User Interface"]
        CLI[CLI / Notebook]
    end

    subgraph Orchestrator["RAG Orchestrator Agent"]
        Router[Task Router]
        Memory[Conversation Memory]
        Retriever[Azure AI Search Retriever]
    end

    subgraph Agents["Specialized Agents"]
        CAA[Code Analyst Agent]
        REA[Refactor Engineer Agent]
        TAA[Test Architect Agent]
        DA[Documentation Agent]
    end

    subgraph Azure["Azure AI Services"]
        AzureOAI[Azure OpenAI GPT-4.1]
        Embeddings[text-embedding-3-large]
        Search[Azure AI Search]
    end

    CLI --> Router
    Router --> CAA & REA & TAA & DA
    CAA & REA & TAA & DA --> AzureOAI
    Retriever --> Search
```

## Agents

| Agent | Role | Capabilities |
|-------|------|--------------|
| **Code Analyst (CAA)** | Understanding | Parse modules, extract classes/functions, build dependency graph, identify engine-specific logic |
| **Refactor Engineer (REA)** | Improvement | Suggest cleanups, improve type hints, optimize Pydantic models, validate match/case patterns |
| **Test Architect (TAA)** | Testing | Generate unit/integration tests, create synthetic data, test SCD transformations |
| **Documentation (DA)** | Documentation | Generate API docs, YAML schema docs, tutorials, architecture diagrams |
| **Orchestrator (ROA)** | Coordination | Route tasks, coordinate multi-agent workflows, synthesize outputs |

## Key Features

- **Multi-Agent System** - Specialized agents for analysis, refactoring, testing, docs
- **RAG-Powered** - Retrieves relevant code context from Azure AI Search
- **Long-Term Memory** - Remembers decisions, learnings, and preferences across sessions
- **Flexible Models** - Use any Azure OpenAI model you've deployed
- **Databricks Ready** - REST-only API for enterprise deployment

## Quick Start

### Prerequisites

1. Azure OpenAI resource with:
   - A chat model deployment (e.g., GPT-4o, GPT-4.1, GPT-3.5)
   - An embedding deployment (e.g., text-embedding-3-large)

2. Azure AI Search resource with index created

3. Environment variables:
```bash
export AZURE_OPENAI_API_KEY="your-openai-key"
export AZURE_SEARCH_API_KEY="your-search-key"
```

### Installation

The agents are part of the Odibi package:
```bash
pip install odibi
```

### Index the Codebase

First, index the Odibi codebase into Azure AI Search:

```bash
python -m odibi.agents.pipelines.indexer --odibi-root /path/to/odibi --force-recreate
```

### Interactive CLI

Run the interactive CLI:
```bash
python -m odibi.agents.pipelines.agent_runner
```

Commands:
- `/quit` - Exit
- `/reset` - Reset conversation
- `/agent <role>` - Switch to specific agent
- `/audit <name>` - Full audit of a component

### Python API

```python
import os
from odibi.agents import AgentRunner, AzureConfig

config = AzureConfig(
    openai_api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    search_api_key=os.getenv("AZURE_SEARCH_API_KEY"),
)

runner = AgentRunner(config)

# Ask questions (orchestrator routes automatically)
response = runner.ask("How does the NodeExecutor work?")
print(response.content)

# Use specific agents
from odibi.agents import AgentRole

response = runner.ask(
    "Generate tests for the SCD transformer",
    agent_role=AgentRole.TEST_ARCHITECT
)
```

### Databricks Usage

```python
# In Databricks notebook
from odibi.agents.pipelines.agent_runner import run_databricks_agent

result = run_databricks_agent(
    query="Explain dependency resolution in SparkWorkflowNode",
    agent_role="code_analyst",
    odibi_root="/Workspace/odibi"
)

print(result["content"])
```

## Example Workflows

### 1. Audit a Transformer

```python
audit = runner.audit_transformer("derive_columns")
print(audit["analysis"])      # Code analysis
print(audit["improvements"])  # Refactoring suggestions
print(audit["tests"])         # Generated tests
print(audit["documentation"]) # Documentation
```

### 2. Generate SCD Tests

```python
tests = runner.generate_scd_tests()
print(tests)  # Complete pytest test suite
```

### 3. Document Config Schema

```python
docs = runner.document_config_schema()
print(docs)  # Full YAML schema documentation
```

### 4. Multi-Agent Workflow

```python
workflow = [
    {"query": "Explain existing transformer patterns", "agent": "code_analyst"},
    {"query": "Suggest implementation for 'deduplicate' transformer", "agent": "refactor_engineer"},
    {"query": "Generate tests for the transformer", "agent": "test_architect"},
    {"query": "Document the transformer", "agent": "documentation"},
]

responses = runner.run_workflow(workflow)
```

## Azure Configuration

### Flexible Model Support

The agent suite supports **any Azure OpenAI model** you've deployed. The deployment name is whatever **you named it** when creating the deployment in Azure AI Studio/Foundry.

> **Important:** `chat_deployment` and `embedding_deployment` are your **deployment names**, not model names. If you deployed GPT-4o and named it `"my-gpt4"`, use `"my-gpt4"`.

Configure via environment variables or constructor arguments:

```bash
# Environment variables
export AZURE_OPENAI_ENDPOINT="https://your-resource.openai.azure.com/"
export AZURE_OPENAI_API_KEY="your-key"
export AZURE_OPENAI_CHAT_DEPLOYMENT="gpt-4o"           # or gpt-4.1, gpt-35-turbo, etc.
export AZURE_OPENAI_EMBEDDING_DEPLOYMENT="text-embedding-3-large"  # or ada-002
export AZURE_SEARCH_ENDPOINT="https://your-search.search.windows.net"
export AZURE_SEARCH_API_KEY="your-key"
export AZURE_SEARCH_INDEX="odibi-code"
```

```python
# Python configuration
from odibi.agents import AzureConfig

# Option 1: Load from environment
config = AzureConfig.from_env()

# Option 2: Explicit configuration
config = AzureConfig(
    openai_endpoint="https://my-resource.openai.azure.com/",
    openai_api_key="...",
    chat_deployment="gpt-4o",                    # Any chat model
    embedding_deployment="text-embedding-3-large", # Any embedding model
    embedding_dimensions=3072,                    # Match your model
    search_endpoint="https://my-search.search.windows.net",
    search_api_key="...",
    search_index="my-odibi-index",
)

# Option 3: Override specific settings
config = AzureConfig.from_env()
config.chat_deployment = "gpt-35-turbo"  # Use cheaper model
```

### Supported Models

Use any model you've deployed. Common choices:

| Model Type | Azure Model Name | Dimensions | Notes |
|------------|------------------|------------|-------|
| **Chat** | gpt-4.1 | - | Latest GPT-4 |
| **Chat** | gpt-4o | - | GPT-4 Optimized |
| **Chat** | gpt-4o-mini | - | Fast, cheaper |
| **Chat** | gpt-35-turbo | - | Cost-effective |
| **Embedding** | text-embedding-3-large | 3072 | Best quality |
| **Embedding** | text-embedding-3-small | 1536 | Good balance |
| **Embedding** | text-embedding-ada-002 | 1536 | Legacy |

> Your deployment name can be anything (e.g., `my-chat-model`). The dimensions are auto-detected if your deployment name contains `ada`, `3-large`, or `3-small`. Otherwise, set `embedding_dimensions` explicitly.

### CLI Model Selection

```bash
# Interactive CLI with custom model
python -m odibi.agents.pipelines.agent_runner --chat-model gpt-4o

# Indexing with custom embedding model
python -m odibi.agents.pipelines.indexer --embedding-model text-embedding-ada-002
```

## Long-Term Memory

The agent suite includes a memory system that persists across sessions:

### What It Remembers
- **Decisions** - Design choices and rationale
- **Learnings** - Bug fixes, patterns, insights
- **Preferences** - Your coding style and conventions
- **TODOs** - Things to do later
- **Context** - What you were working on

### Memory Commands (CLI)
```bash
/save              # Save session memories now
/recall <query>    # Search past memories
/remember <text>   # Manually save a memory
/memories          # Show recent memories
```

### Memory API
```python
# Manually remember something
runner.remember(
    content="We decided to use match/case for operation dispatch",
    summary="Use match/case for operations",
    memory_type=MemoryType.DECISION,
    tags=["pattern", "refactoring"],
    importance=0.9,
)

# Recall relevant memories
memories = runner.recall("how do we handle SCD transformations?")

# Auto-save session learnings on exit
runner.save_session_memories()  # Extracts key info via LLM
```

### Memory Storage

Memories can be stored using any Odibi connection:

```python
from odibi.agents.core.memory import MemoryManager

# Local (default)
manager = MemoryManager(backend_type="local")

# ADLS via Odibi connection
manager = MemoryManager(
    backend_type="odibi",
    connection=connections["adls"],
    engine=pandas_engine,
    path_prefix="agent/memories",
)

# Delta table (for Databricks)
manager = MemoryManager(
    backend_type="delta",
    connection=connections["silver"],
    engine=spark_engine,
    table_path="system.agent_memories",
)
```

| Backend | Storage Location | Best For |
|---------|-----------------|----------|
| `local` | JSON files in `.odibi/memories/` | Development, simple usage |
| `odibi` | Any Odibi connection (ADLS, blob, etc.) | Cloud storage, shared access |
| `delta` | Delta table | Databricks, Spark environments |

### Index Schema

The `odibi-code` index stores:

| Field | Type | Description |
|-------|------|-------------|
| `id` | String | Unique chunk identifier |
| `file_path` | String | Relative file path |
| `module_name` | String | Python module name |
| `chunk_type` | String | module, class, function, method |
| `name` | String | Symbol name |
| `content` | String | Source code |
| `docstring` | String | Docstring if present |
| `signature` | String | Function/class signature |
| `parent_class` | String | Parent class for methods |
| `imports` | Collection | Import statements |
| `dependencies` | Collection | Called functions/classes |
| `line_start` | Int32 | Starting line number |
| `line_end` | Int32 | Ending line number |
| `engine_type` | String | pandas, spark, polars, or empty |
| `tags` | Collection | Auto-generated tags |
| `content_vector` | Vector | 3072-dim embedding |

## Gradio UI

A conversational interface for interacting with the agents. Works locally and in Databricks.

### Quick Start

```python
# Local - opens in browser
from odibi.agents.ui import launch
launch()

# Databricks notebook - renders inline
from odibi.agents.ui import launch
launch()

# Custom options
launch(server_port=7860, share=True)
```

### Features

| Feature | Description |
|---------|-------------|
| **Settings Panel** | Configure LLM endpoint, model, API key; memory backend; browse project.yaml connections |
| **Chat Interface** | Conversational AI with tool execution and agent routing |
| **Memory Panel** | View, search, and save memories with type badges |
| **Tool Execution** | Read files, grep/glob search, run pytest/ruff/odibi commands |
| **Confirmation Dialogs** | Approve file writes and shell commands before execution |

### Configuration

Settings are saved to `.odibi/agent_config.yaml`:

```yaml
llm:
  endpoint: https://api.openai.com/v1  # Any OpenAI-compatible endpoint
  model: gpt-4o
  api_type: openai  # or azure
memory:
  backend_type: local  # or odibi, delta
  connection_name: adls_memories
  path_prefix: agent/memories
project:
  odibi_root: d:/odibi
```

### Supported LLM Providers

The UI works with any OpenAI-compatible API:

| Provider | Endpoint | Notes |
|----------|----------|-------|
| **OpenAI** | `https://api.openai.com/v1` | Use `sk-...` API key |
| **Azure OpenAI** | `https://your-resource.openai.azure.com` | Auto-detects Azure |
| **Ollama** | `http://localhost:11434/v1` | Local, no key needed |
| **LM Studio** | `http://localhost:1234/v1` | Local, any key works |
| **vLLM** | `http://localhost:8000/v1` | Self-hosted |
| **Together AI** | `https://api.together.xyz/v1` | Cloud provider |

### Requirements

```bash
pip install odibi[agents]
# or
pip install gradio>=4.0.0 requests>=2.28.0
```

### Running Tests

```bash
cd agents/ui/tests
python run_tests.py -v
```

## File Structure

```
odibi/agents/
├── __init__.py           # Package exports
├── README.md             # This file
├── core/
│   ├── __init__.py
│   ├── agent_base.py     # Base agent class and registry
│   ├── azure_client.py   # Azure REST API clients
│   ├── memory.py         # Memory system
│   ├── memory_backends.py # Storage backends (local, odibi, delta)
│   └── code_parser.py    # Python AST parser
├── prompts/
│   ├── __init__.py
│   ├── code_analyst.py   # CAA implementation
│   ├── refactor_engineer.py  # REA implementation
│   ├── test_architect.py # TAA implementation
│   ├── documentation.py  # DA implementation
│   └── orchestrator.py   # ROA implementation
├── pipelines/
│   ├── __init__.py
│   ├── indexer.py        # Code indexing pipeline
│   └── agent_runner.py   # Agent execution
├── ui/
│   ├── __init__.py       # UI exports (launch, create_app)
│   ├── app.py            # Main Gradio application
│   ├── config.py         # Settings load/save
│   ├── components/
│   │   ├── chat.py       # Chat interface
│   │   ├── settings.py   # Settings panel
│   │   └── memories.py   # Memory viewer
│   └── tools/
│       ├── file_tools.py # Read/write files
│       ├── search_tools.py # Grep/glob search
│       └── shell_tools.py # Command execution
└── examples/
    ├── __init__.py
    └── example_workflows.py  # Example use cases
```

## Design Principles

The agent system follows Odibi's philosophy:

1. **Declarative over imperative** - Agents are configured, not hardcoded
2. **Explicit over implicit** - All routing logic is visible
3. **Composition over inheritance** - Agents compose via orchestrator
4. **REST API only** - No SDK dependencies for Databricks compatibility
5. **Fail-fast** - Clear errors with actionable suggestions

## Constraints

- All code uses REST API (no Azure SDK) for Databricks compatibility
- Uses snake_case naming convention
- Type hints on all functions
- Google-style docstrings
- Pydantic for configuration validation

## Extending the System

### Add a New Agent

1. Create prompt file in `prompts/`:
```python
NEW_AGENT_SYSTEM_PROMPT = """..."""

class NewAgent(OdibiAgent):
    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=NEW_AGENT_SYSTEM_PROMPT,
            role=AgentRole.NEW_ROLE,  # Add to enum
        )

    def process(self, context: AgentContext) -> AgentResponse:
        ...
```

2. Register in orchestrator:
```python
AgentRegistry.register(NewAgent(config))
```

3. Add routing keywords in orchestrator:
```python
ROUTING_KEYWORDS = {
    AgentRole.NEW_ROLE: ["keyword1", "keyword2"],
    ...
}
```

### Custom Workflows

```python
workflow = [
    {"query": "Step 1", "agent": "code_analyst"},
    {"query": "Step 2 based on Step 1", "agent": "refactor_engineer"},
    # Add more steps...
]

responses = runner.run_workflow(workflow)
```

## Troubleshooting

### "No relevant code found"

1. Ensure the codebase is indexed:
   ```bash
   python -m odibi.agents.pipelines.indexer
   ```

2. Check Azure AI Search index exists and has documents

### "Azure OpenAI request failed"

1. Verify API key is set correctly
2. Check deployment names match configuration
3. Ensure quota is available

### "Agent not registered"

1. Ensure `create_agent_suite(config)` is called
2. Check AgentRegistry contains all agents

## License

MIT License - same as Odibi

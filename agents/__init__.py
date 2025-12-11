"""Odibi AI Agent Suite.

A multi-agent system for understanding, testing, documenting, and
improving the Odibi data engineering framework.

## Architecture

The agent suite consists of:

1. **Code Analyst Agent (CAA)** - Understands and maps the codebase
2. **Refactor Engineer Agent (REA)** - Suggests improvements
3. **Test Architect Agent (TAA)** - Generates test suites
4. **Documentation Agent (DA)** - Creates documentation
5. **RAG Orchestrator Agent (ROA)** - Coordinates all agents

## Quick Start (Local - No Cloud Required)

```python
from agents import ensure_index, LocalEmbedder, ChromaVectorStore

# Auto-index the codebase (runs on first use, updates when code changes)
store = ensure_index(odibi_root=".")

# Query the index
embedder = LocalEmbedder()
query_vec = embedder.embed_query("How does the Pipeline class work?")
results = store.similarity_search(query_vec, k=5)
for result in results:
    print(f"{result['name']}: {result['content'][:200]}")
```

## Quick Start (With Azure Chat)

```python
import os
from agents import AgentRunner
from agents.core.azure_client import AzureConfig

# Configure Azure (only chat model needed, local indexing is used)
config = AzureConfig(
    openai_api_key=os.getenv("AZURE_OPENAI_API_KEY"),
)

# Create the agent suite
runner = AgentRunner(config)

# Ask questions
response = runner.ask("How does the NodeExecutor work?")
print(response.content)
```

## Resources

- **Local (default)**: ChromaDB + sentence-transformers (no cloud needed)
- **Azure (optional)**: Azure OpenAI for chat

## Environment Variables (Optional, for Azure chat)

```bash
export AZURE_OPENAI_API_KEY="your-key"
```
"""

from agents.core.agent_base import (
    AgentContext,
    AgentRegistry,
    AgentResponse,
    AgentRole,
    OdibiAgent,
)
from agents.core.azure_client import (
    AzureConfig,
    AzureOpenAIClient,
    AzureSearchClient,
)
from agents.core.chroma_store import ChromaVectorStore
from agents.core.embeddings import BaseEmbedder, LocalEmbedder
from agents.core.index_manager import ensure_index, get_index, needs_reindex
from agents.core.vector_store import BaseVectorStore
from agents.pipelines.agent_runner import AgentRunner, AgentRunnerConfig
from agents.pipelines.indexer import LocalIndexer
from agents.prompts.orchestrator import create_agent_suite

__all__ = [
    "AgentContext",
    "AgentRegistry",
    "AgentResponse",
    "AgentRole",
    "AgentRunner",
    "AgentRunnerConfig",
    "AzureConfig",
    "AzureOpenAIClient",
    "AzureSearchClient",
    "BaseEmbedder",
    "BaseVectorStore",
    "ChromaVectorStore",
    "LocalEmbedder",
    "LocalIndexer",
    "OdibiAgent",
    "create_agent_suite",
    "ensure_index",
    "get_index",
    "needs_reindex",
]

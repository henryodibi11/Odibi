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

## Quick Start

```python
import os
from odibi.agents import create_agent_suite, AgentRunner
from odibi.agents.core.azure_client import AzureConfig

# Configure Azure
config = AzureConfig(
    openai_api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    search_api_key=os.getenv("AZURE_SEARCH_API_KEY"),
)

# Create the agent suite
runner = AgentRunner(config)

# Ask questions
response = runner.ask("How does the NodeExecutor work?")
print(response.content)

# Run specific workflows
audit = runner.audit_transformer("derive_columns")
tests = runner.generate_scd_tests()
docs = runner.document_config_schema()
```

## Azure Resources Required

- Azure OpenAI (GPT-4.1, text-embedding-3-large)
- Azure AI Search (odibi-code index)

## Environment Variables

```bash
export AZURE_OPENAI_API_KEY="your-key"
export AZURE_SEARCH_API_KEY="your-key"
```
"""

from odibi.agents.core.agent_base import (
    AgentContext,
    AgentRegistry,
    AgentResponse,
    AgentRole,
    OdibiAgent,
)
from odibi.agents.core.azure_client import (
    AzureConfig,
    AzureOpenAIClient,
    AzureSearchClient,
)
from odibi.agents.pipelines.agent_runner import AgentRunner, AgentRunnerConfig
from odibi.agents.prompts.orchestrator import create_agent_suite

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
    "OdibiAgent",
    "create_agent_suite",
]

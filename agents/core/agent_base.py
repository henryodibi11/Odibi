"""Base agent class and agent registry for the Odibi AI Agent Suite.

All agents inherit from OdibiAgent and use Azure OpenAI for reasoning
with Azure AI Search for retrieval-augmented generation.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from agents.core.azure_client import (
    AzureConfig,
    AzureOpenAIClient,
    AzureSearchClient,
)


class AgentRole(str, Enum):
    """Enumeration of agent roles in the system."""

    CODE_ANALYST = "code_analyst"
    REFACTOR_ENGINEER = "refactor_engineer"
    TEST_ARCHITECT = "test_architect"
    DOCUMENTATION = "documentation"
    ORCHESTRATOR = "orchestrator"


@dataclass
class AgentContext:
    """Context passed between agents during task execution."""

    query: str
    retrieved_chunks: list[dict] = field(default_factory=list)
    conversation_history: list[dict] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    parent_agent: Optional[str] = None
    task_id: Optional[str] = None
    odibi_root: str = "d:/odibi"


@dataclass
class AgentResponse:
    """Response from an agent."""

    content: str
    agent_role: AgentRole
    sources: list[dict] = field(default_factory=list)
    confidence: float = 1.0
    follow_up_needed: bool = False
    suggested_agents: list[AgentRole] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class OdibiAgent(ABC):
    """Base class for all Odibi AI agents.

    Each agent has:
    - A specific role and system prompt
    - Access to Azure OpenAI for reasoning
    - Access to Azure AI Search for code retrieval
    - The ability to delegate to other agents
    """

    def __init__(
        self,
        config: AzureConfig,
        system_prompt: str,
        role: AgentRole,
    ):
        self.config = config
        self.system_prompt = system_prompt
        self.role = role
        self.openai_client = AzureOpenAIClient(config)
        self.search_client = AzureSearchClient(config)

    @abstractmethod
    def process(self, context: AgentContext) -> AgentResponse:
        """Process a request within the given context.

        Args:
            context: The agent context containing query, history, etc.

        Returns:
            AgentResponse with the result.
        """
        pass

    def retrieve_context(
        self,
        query: str,
        top_k: int = 10,
        filter_expression: Optional[str] = None,
    ) -> list[dict]:
        """Retrieve relevant code chunks from Azure AI Search.

        Args:
            query: The search query.
            top_k: Number of results to retrieve.
            filter_expression: Optional OData filter.

        Returns:
            List of relevant code chunks.
        """
        query_embedding = self.openai_client.create_embeddings([query])[0]

        results = self.search_client.search(
            query=query,
            vector=query_embedding,
            top=top_k,
            filter_expression=filter_expression,
            select=[
                "id",
                "file_path",
                "module_name",
                "chunk_type",
                "name",
                "content",
                "docstring",
                "signature",
                "line_start",
                "line_end",
                "tags",
            ],
            semantic_config="odibi-semantic",
        )

        return results

    def format_context_for_prompt(self, chunks: list[dict]) -> str:
        """Format retrieved chunks into a context string for the LLM.

        Args:
            chunks: List of code chunks from search.

        Returns:
            Formatted context string.
        """
        if not chunks:
            return "No relevant code found."

        context_parts = []
        for i, chunk in enumerate(chunks, 1):
            header = f"### [{i}] {chunk.get('chunk_type', 'unknown').upper()}: {chunk.get('name', 'unknown')}"
            file_info = f"File: {chunk.get('file_path', 'unknown')} (lines {chunk.get('line_start', '?')}-{chunk.get('line_end', '?')})"

            docstring = chunk.get("docstring", "")
            doc_section = f"\nDocstring:\n{docstring}" if docstring else ""

            signature = chunk.get("signature", "")
            sig_section = f"\nSignature: {signature}" if signature else ""

            content = chunk.get("content", "")
            content_section = f"\nCode:\n```python\n{content}\n```"

            context_parts.append(
                f"{header}\n{file_info}{sig_section}{doc_section}{content_section}\n"
            )

        return "\n---\n".join(context_parts)

    def chat(
        self,
        messages: list[dict[str, str]],
        context_chunks: Optional[list[dict]] = None,
        temperature: float = 0.0,
    ) -> str:
        """Send a chat request to Azure OpenAI with optional context.

        Args:
            messages: Conversation messages.
            context_chunks: Optional code chunks to include as context.
            temperature: Sampling temperature.

        Returns:
            Assistant response.
        """
        full_system = self.system_prompt

        if context_chunks:
            context_str = self.format_context_for_prompt(context_chunks)
            full_system += f"\n\n## Retrieved Code Context:\n{context_str}"

        return self.openai_client.chat_completion(
            messages=messages,
            temperature=temperature,
            system_prompt=full_system,
        )


class AgentRegistry:
    """Registry for managing and accessing agents."""

    _agents: dict[AgentRole, OdibiAgent] = {}

    @classmethod
    def register(cls, agent: OdibiAgent) -> None:
        """Register an agent."""
        cls._agents[agent.role] = agent

    @classmethod
    def get(cls, role: AgentRole) -> Optional[OdibiAgent]:
        """Get an agent by role."""
        return cls._agents.get(role)

    @classmethod
    def get_all(cls) -> dict[AgentRole, OdibiAgent]:
        """Get all registered agents."""
        return cls._agents.copy()

    @classmethod
    def clear(cls) -> None:
        """Clear all registered agents."""
        cls._agents.clear()

"""Base agent class and agent registry for the Odibi AI Agent Suite.

All agents inherit from OdibiAgent and use any OpenAI-compatible LLM for reasoning.
Supports: OpenAI, Azure OpenAI, Ollama, LM Studio, vLLM, etc.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Optional

from odibi.agents.core.azure_client import (
    AzureConfig,
    AzureOpenAIClient,
)

if TYPE_CHECKING:
    from odibi.agents.core.chroma_store import ChromaVectorStore
    from odibi.agents.core.embeddings import BaseEmbedder


class AgentRole(str, Enum):
    """Enumeration of agent roles in the system."""

    CODE_ANALYST = "code_analyst"
    REFACTOR_ENGINEER = "refactor_engineer"
    TEST_ARCHITECT = "test_architect"
    DOCUMENTATION = "documentation"
    ORCHESTRATOR = "orchestrator"

    ENVIRONMENT = "environment"
    USER = "user"
    OBSERVER = "observer"
    IMPROVEMENT = "improvement"
    REVIEWER = "reviewer"
    REGRESSION_GUARD = "regression_guard"
    PROJECT = "project"
    CURRICULUM = "curriculum"
    CONVERGENCE = "convergence"
    PRODUCT_OBSERVER = "product_observer"
    UX_FRICTION = "ux_friction"


@dataclass(frozen=True)
class AgentPermissions:
    """Permission flags for agents."""

    can_edit_source: bool = False
    read_only: bool = True
    can_execute_tasks: bool = False
    can_access_private_memories: bool = False
    can_write_memories: bool = False


DEFAULT_PERMISSIONS = AgentPermissions()


ROLE_PERMISSIONS: dict[AgentRole, AgentPermissions] = {
    AgentRole.CODE_ANALYST: AgentPermissions(read_only=True),
    AgentRole.REFACTOR_ENGINEER: AgentPermissions(can_edit_source=True, read_only=False),
    AgentRole.TEST_ARCHITECT: AgentPermissions(
        can_edit_source=True, read_only=False, can_execute_tasks=True
    ),
    AgentRole.DOCUMENTATION: AgentPermissions(can_edit_source=True, read_only=False),
    AgentRole.ORCHESTRATOR: AgentPermissions(read_only=False, can_write_memories=True),
    AgentRole.ENVIRONMENT: AgentPermissions(
        can_edit_source=False, read_only=True, can_execute_tasks=True
    ),
    AgentRole.USER: AgentPermissions(
        can_edit_source=False, read_only=False, can_execute_tasks=True
    ),
    AgentRole.OBSERVER: AgentPermissions(read_only=True),
    AgentRole.IMPROVEMENT: AgentPermissions(can_edit_source=True, read_only=False),
    AgentRole.REVIEWER: AgentPermissions(read_only=True),
    AgentRole.REGRESSION_GUARD: AgentPermissions(
        can_edit_source=False, read_only=True, can_execute_tasks=True
    ),
    AgentRole.PROJECT: AgentPermissions(read_only=True),
    AgentRole.CURRICULUM: AgentPermissions(
        can_access_private_memories=True, can_write_memories=True, read_only=True
    ),
    AgentRole.CONVERGENCE: AgentPermissions(can_write_memories=True, read_only=True),
    AgentRole.PRODUCT_OBSERVER: AgentPermissions(read_only=True),
    AgentRole.UX_FRICTION: AgentPermissions(read_only=True),
}


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
    assistant_mode: Optional[str] = None
    cycle_id: Optional[str] = None
    cycle_step: Optional[str] = None
    event_callback: Optional[Callable] = None

    def emit_progress(self, message: str, detail: str = "") -> None:
        """Emit a progress event if callback is available.

        Use this to provide real-time visibility during long operations.
        """
        if self.event_callback:
            from odibi.agents.core.cycle import CycleEvent

            self.event_callback(
                CycleEvent(
                    event_type="agent_progress",
                    step=self.cycle_step or "",
                    agent_role="",
                    message=message,
                    detail=detail,
                )
            )


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
    - Access to any OpenAI-compatible LLM for reasoning
    - Access to local vector store for code retrieval
    - The ability to delegate to other agents

    Supports multiple LLM providers:
    - OpenAI (api.openai.com)
    - Azure OpenAI
    - Ollama (local)
    - LM Studio (local)
    - Any OpenAI-compatible API
    """

    _global_llm_client = None  # Shared LLM client set from UI config
    _global_vector_store: Optional["ChromaVectorStore"] = None  # Local vector store
    _global_embedder: Optional["BaseEmbedder"] = None  # Embedder for query vectors

    @classmethod
    def set_global_llm_client(cls, client) -> None:
        """Set a global LLM client for all agents to use.

        This allows the UI to configure agents to use the same
        provider-agnostic LLMClient that the chat uses.
        """
        cls._global_llm_client = client

    @classmethod
    def get_global_llm_client(cls):
        """Get the global LLM client if set."""
        return cls._global_llm_client

    @classmethod
    def set_global_vector_store(
        cls, vector_store: "ChromaVectorStore", embedder: "BaseEmbedder"
    ) -> None:
        """Set the global vector store and embedder for all agents.

        Args:
            vector_store: ChromaVectorStore instance for similarity search.
            embedder: Embedder for creating query vectors.
        """
        cls._global_vector_store = vector_store
        cls._global_embedder = embedder

    @classmethod
    def get_global_vector_store(cls) -> Optional["ChromaVectorStore"]:
        """Get the global vector store if set."""
        return cls._global_vector_store

    def __init__(
        self,
        config: Optional[AzureConfig] = None,
        system_prompt: str = "",
        role: AgentRole = AgentRole.CODE_ANALYST,
        permissions: Optional[AgentPermissions] = None,
        llm_client=None,
    ):
        self.config = config
        self.system_prompt = system_prompt
        self.role = role
        self.permissions = permissions or ROLE_PERMISSIONS.get(role, DEFAULT_PERMISSIONS)

        # Use provided client, global client, or fall back to Azure client
        if llm_client is not None:
            self._llm_client = llm_client
            self._use_generic_client = True
        elif OdibiAgent._global_llm_client is not None:
            self._llm_client = OdibiAgent._global_llm_client
            self._use_generic_client = True
        elif config is not None:
            self._llm_client = AzureOpenAIClient(config)
            self._use_generic_client = False
        else:
            self._llm_client = None
            self._use_generic_client = True

        # Legacy compatibility
        self.openai_client = self._llm_client if not self._use_generic_client else None

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
        filters: Optional[dict[str, Any]] = None,
    ) -> list[dict]:
        """Retrieve relevant code chunks from the local vector store.

        Uses the global ChromaVectorStore for similarity search.

        Args:
            query: The search query.
            top_k: Number of results to retrieve.
            filters: Optional metadata filters (e.g., {"chunk_type": "class"}).

        Returns:
            List of relevant code chunks (empty if vector store not configured).
        """
        if OdibiAgent._global_vector_store is None:
            return []

        if OdibiAgent._global_embedder is None:
            return []

        try:
            query_embedding = OdibiAgent._global_embedder.embed_texts([query])[0]

            results = OdibiAgent._global_vector_store.similarity_search(
                query_embedding=query_embedding,
                k=top_k,
                filters=filters,
            )
            return results
        except Exception:
            return []

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
        """Send a chat request to the LLM with optional context.

        Works with any OpenAI-compatible API (OpenAI, Azure, Ollama, etc.)

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

        if self._llm_client is None:
            return "Error: No LLM client configured. Please set up OpenAI or Azure credentials."

        if self._use_generic_client:
            # Generic LLMClient (from UI) - use chat_with_tools or simple chat
            if hasattr(self._llm_client, "chat_with_tools"):
                response = self._llm_client.chat_with_tools(
                    messages=messages,
                    system_prompt=full_system,
                    temperature=temperature,
                    tools=None,
                )
                # Extract string content from response object
                if hasattr(response, "content"):
                    content = response.content
                    return content if isinstance(content, str) else str(content)
                elif isinstance(response, dict):
                    return response.get("content", str(response))
                return str(response)
            elif hasattr(self._llm_client, "chat"):
                result = self._llm_client.chat(
                    messages=messages,
                    system_prompt=full_system,
                    temperature=temperature,
                )
                if isinstance(result, str):
                    return result
                elif hasattr(result, "content"):
                    content = result.content
                    return content if isinstance(content, str) else str(content)
                elif isinstance(result, dict):
                    return result.get("content", str(result))
                return str(result)
            else:
                return "Error: LLM client does not support chat operations."
        else:
            # Azure-specific client
            return self._llm_client.chat_completion(
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

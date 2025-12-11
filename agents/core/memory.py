"""Long-term memory system for the Odibi AI Agent Suite.

Stores and retrieves:
- Conversation summaries
- Design decisions
- Learnings (bugs, fixes, patterns)
- User preferences
- Project context
"""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class MemoryType(str, Enum):
    """Types of memories the system can store."""

    CONVERSATION = "conversation"
    DECISION = "decision"
    LEARNING = "learning"
    PREFERENCE = "preference"
    TODO = "todo"
    BUG_FIX = "bug_fix"
    FEATURE = "feature"
    CONTEXT = "context"


@dataclass
class Memory:
    """A single memory entry."""

    id: str
    memory_type: MemoryType
    content: str
    summary: str
    tags: list[str] = field(default_factory=list)
    source_files: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    importance: float = 0.5
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "id": self.id,
            "memory_type": self.memory_type.value,
            "content": self.content,
            "summary": self.summary,
            "tags": self.tags,
            "source_files": self.source_files,
            "created_at": self.created_at,
            "importance": self.importance,
            "metadata": json.dumps(self.metadata) if self.metadata else "{}",
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Memory":
        """Create from dictionary."""
        metadata = data.get("metadata", "{}")
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        return cls(
            id=data["id"],
            memory_type=MemoryType(data["memory_type"]),
            content=data["content"],
            summary=data["summary"],
            tags=data.get("tags", []),
            source_files=data.get("source_files", []),
            created_at=data.get("created_at", datetime.now().isoformat()),
            importance=data.get("importance", 0.5),
            metadata=metadata,
        )


class MemoryStore:
    """Long-term memory storage with pluggable backends.

    Supports multiple backends:
    1. Local JSON files (default, simple)
    2. Odibi connection (ADLS, Azure Blob, etc.)
    3. Delta table (for Spark/Databricks)

    Example:
        ```python
        # Local storage (default)
        store = MemoryStore()

        # ADLS via Odibi connection
        store = MemoryStore(
            backend_type="odibi",
            connection=connections["adls"],
            engine=pandas_engine,
            path_prefix="agent/memories",
        )

        # Delta table
        store = MemoryStore(
            backend_type="delta",
            connection=connections["silver"],
            engine=spark_engine,
            table_path="system.agent_memories",
        )
        ```
    """

    def __init__(
        self,
        backend_type: str = "local",
        local_path: Optional[str] = None,
        connection: Optional[Any] = None,
        engine: Optional[Any] = None,
        **backend_kwargs,
    ):
        """Initialize memory store.

        Args:
            backend_type: "local", "odibi", or "delta".
            local_path: Path for local storage (if backend_type="local").
            connection: Odibi connection (if backend_type="odibi" or "delta").
            engine: Odibi engine (if backend_type="odibi" or "delta").
            **backend_kwargs: Additional backend-specific arguments.
        """
        from odibi.agents.core.memory_backends import create_memory_backend

        if backend_type == "local":
            self.backend = create_memory_backend(
                "local",
                base_path=local_path or "d:/odibi/.odibi/memories",
            )
        else:
            self.backend = create_memory_backend(
                backend_type,
                connection=connection,
                engine=engine,
                **backend_kwargs,
            )

    def _generate_id(self, content: str, memory_type: str) -> str:
        """Generate unique ID for a memory."""
        timestamp = datetime.now().isoformat()
        key = f"{memory_type}:{timestamp}:{content[:100]}"
        return hashlib.md5(key.encode()).hexdigest()

    def store(self, memory: Memory) -> bool:
        """Store a memory.

        Args:
            memory: The memory to store.

        Returns:
            True if successful.
        """
        return self.backend.save(memory.id, memory.to_dict())

    def search(
        self,
        query: str,
        memory_types: Optional[list[MemoryType]] = None,
        top_k: int = 10,
        min_importance: float = 0.0,
    ) -> list[Memory]:
        """Search memories by keyword.

        Args:
            query: Search query.
            memory_types: Filter by memory types.
            top_k: Number of results.
            min_importance: Minimum importance score.

        Returns:
            List of matching memories.
        """
        results = self.backend.search(query, limit=top_k)

        memories = []
        for data in results:
            memory = Memory.from_dict(data)

            if memory.importance < min_importance:
                continue

            if memory_types and memory.memory_type not in memory_types:
                continue

            memories.append(memory)

        memories.sort(key=lambda m: m.importance, reverse=True)
        return memories

    def get_recent(
        self,
        days: int = 7,
        memory_types: Optional[list[MemoryType]] = None,
        limit: int = 20,
    ) -> list[Memory]:
        """Get recent memories.

        Args:
            days: How many days back to look.
            memory_types: Filter by types.
            limit: Maximum results.

        Returns:
            List of recent memories.
        """
        type_strs = [mt.value for mt in memory_types] if memory_types else None
        results = self.backend.get_recent(days=days, memory_types=type_strs, limit=limit)
        return [Memory.from_dict(data) for data in results]


class MemoryManager:
    """High-level memory management with automatic extraction and retrieval.

    Example:
        ```python
        # Local storage (default)
        manager = MemoryManager()

        # ADLS via Odibi connection
        manager = MemoryManager(
            backend_type="odibi",
            connection=connections["adls"],
            engine=pandas_engine,
            path_prefix="agent/memories",
        )

        # With LLM for auto-extraction
        manager = MemoryManager(
            llm_client=openai_client,  # For extracting insights from conversations
        )
        ```
    """

    EXTRACTION_PROMPT = """
Analyze this conversation and extract important information to remember.

For each piece of information, classify it as one of:
- DECISION: A design or implementation decision made
- LEARNING: Something learned (bug fix, pattern, insight)
- PREFERENCE: User preference or coding style
- TODO: Something to do later
- BUG_FIX: A bug that was fixed and how
- FEATURE: A feature discussed or implemented
- CONTEXT: Important project context

Return JSON array:
[
  {
    "type": "DECISION",
    "summary": "Short summary (1 line)",
    "content": "Full details",
    "importance": 0.8,
    "tags": ["tag1", "tag2"],
    "source_files": ["file1.py", "file2.py"]
  },
  ...
]

Only extract IMPORTANT information worth remembering long-term.
If nothing important, return empty array: []

CONVERSATION:
"""

    def __init__(
        self,
        backend_type: str = "local",
        local_path: Optional[str] = None,
        connection: Optional[Any] = None,
        engine: Optional[Any] = None,
        llm_client: Optional[Any] = None,
        **backend_kwargs,
    ):
        """Initialize memory manager.

        Args:
            backend_type: "local", "odibi", or "delta".
            local_path: Path for local storage.
            connection: Odibi connection (for odibi/delta backends).
            engine: Odibi engine (for odibi/delta backends).
            llm_client: Optional LLM client for auto-extraction (must have chat_completion method).
            **backend_kwargs: Additional backend-specific arguments.
        """
        self.store = MemoryStore(
            backend_type=backend_type,
            local_path=local_path,
            connection=connection,
            engine=engine,
            **backend_kwargs,
        )
        self.llm_client = llm_client

    def extract_and_store(
        self,
        conversation: list[dict[str, str]],
        auto_extract: bool = True,
    ) -> list[Memory]:
        """Extract important information from a conversation and store it.

        Args:
            conversation: List of messages (role, content).
            auto_extract: Use LLM to extract, else store as-is.

        Returns:
            List of memories created.
        """
        if not auto_extract or not self.llm_client:
            summary = self._simple_summary(conversation)
            memory = Memory(
                id=self.store._generate_id(summary, "conversation"),
                memory_type=MemoryType.CONVERSATION,
                content=self._format_conversation(conversation),
                summary=summary,
                importance=0.5,
            )
            self.store.store(memory)
            return [memory]

        conv_text = self._format_conversation(conversation)
        prompt = self.EXTRACTION_PROMPT + conv_text

        try:
            response = self.llm_client.chat_completion(
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
            )

            extracted = json.loads(response)
            memories = []

            for item in extracted:
                memory = Memory(
                    id=self.store._generate_id(item["summary"], item["type"]),
                    memory_type=MemoryType(item["type"].lower()),
                    content=item["content"],
                    summary=item["summary"],
                    tags=item.get("tags", []),
                    source_files=item.get("source_files", []),
                    importance=item.get("importance", 0.5),
                )
                self.store.store(memory)
                memories.append(memory)

            return memories

        except Exception:
            memory = Memory(
                id=self.store._generate_id(conv_text[:100], "conversation"),
                memory_type=MemoryType.CONVERSATION,
                content=conv_text,
                summary=self._simple_summary(conversation),
                importance=0.3,
            )
            self.store.store(memory)
            return [memory]

    def remember(
        self,
        memory_type: MemoryType,
        content: str,
        summary: str,
        tags: Optional[list[str]] = None,
        source_files: Optional[list[str]] = None,
        importance: float = 0.5,
    ) -> Memory:
        """Manually store a memory.

        Args:
            memory_type: Type of memory.
            content: Full content.
            summary: Short summary.
            tags: Optional tags.
            source_files: Related files.
            importance: 0-1 importance score.

        Returns:
            The stored memory.
        """
        memory = Memory(
            id=self.store._generate_id(summary, memory_type.value),
            memory_type=memory_type,
            content=content,
            summary=summary,
            tags=tags or [],
            source_files=source_files or [],
            importance=importance,
        )
        self.store.store(memory)
        return memory

    def recall(
        self,
        query: str,
        memory_types: Optional[list[MemoryType]] = None,
        top_k: int = 5,
    ) -> list[Memory]:
        """Recall relevant memories.

        Args:
            query: What to recall.
            memory_types: Filter by types.
            top_k: Number of results.

        Returns:
            List of relevant memories.
        """
        return self.store.search(query, memory_types, top_k)

    def get_context(self, query: str) -> str:
        """Get formatted context from relevant memories.

        Args:
            query: Current query/task.

        Returns:
            Formatted string of relevant memories.
        """
        memories = self.recall(query, top_k=5)

        if not memories:
            return ""

        parts = ["## Relevant Memories\n"]
        for i, mem in enumerate(memories, 1):
            parts.append(f"### [{i}] {mem.memory_type.value.upper()}: {mem.summary}")
            parts.append(f"*{mem.created_at[:10]}* | Importance: {mem.importance:.1f}")
            if mem.tags:
                parts.append(f"Tags: {', '.join(mem.tags)}")
            parts.append(f"\n{mem.content}\n")

        return "\n".join(parts)

    def _format_conversation(self, conversation: list[dict[str, str]]) -> str:
        """Format conversation for storage/analysis."""
        lines = []
        for msg in conversation:
            role = msg.get("role", "unknown").upper()
            content = msg.get("content", "")
            lines.append(f"[{role}]: {content}")
        return "\n\n".join(lines)

    def _simple_summary(self, conversation: list[dict[str, str]]) -> str:
        """Create simple summary from first user message."""
        for msg in conversation:
            if msg.get("role") == "user":
                content = msg.get("content", "")[:100]
                return f"Conversation about: {content}..."
        return "Conversation"

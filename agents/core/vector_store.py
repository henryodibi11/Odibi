"""Vector store abstractions for the Odibi Agent Suite.

Provides a base interface for vector storage backends.
"""

from abc import ABC, abstractmethod
from typing import Any


class BaseVectorStore(ABC):
    """Abstract base class for vector storage backends."""

    @abstractmethod
    def add_chunks(self, chunks: list[dict[str, Any]]) -> dict[str, int]:
        """Insert or update chunks in the vector store.

        Each chunk should have:
        - id: Unique identifier
        - content: Text content
        - content_vector: Embedding vector
        - Plus any additional metadata fields

        Args:
            chunks: List of chunk dictionaries with embeddings.

        Returns:
            Dictionary with 'succeeded' and 'failed' counts.
        """
        ...

    @abstractmethod
    def similarity_search(
        self,
        query_embedding: list[float],
        k: int = 8,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Search for similar chunks.

        Args:
            query_embedding: Query vector.
            k: Number of results to return.
            filters: Optional metadata filters.

        Returns:
            List of chunk dictionaries with similarity scores.
        """
        ...

    @abstractmethod
    def exists(self) -> bool:
        """Check if the index/collection exists.

        Returns:
            True if the index exists and has data.
        """
        ...

    @abstractmethod
    def delete_all(self) -> bool:
        """Delete all data from the store.

        Returns:
            True if successful.
        """
        ...

    @abstractmethod
    def count(self) -> int:
        """Return the number of documents in the store.

        Returns:
            Document count.
        """
        ...

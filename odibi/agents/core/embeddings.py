"""Embedding abstractions for the Odibi Agent Suite.

Provides a base interface and implementations for:
- Local embeddings using sentence-transformers
- Azure OpenAI embeddings (optional)
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from odibi.agents.core.azure_client import AzureOpenAIClient

logger = logging.getLogger(__name__)


class BaseEmbedder(ABC):
    """Abstract base class for embedding providers."""

    @property
    @abstractmethod
    def dimension(self) -> int:
        """Return the embedding dimension."""
        ...

    @abstractmethod
    def embed_texts(self, texts: list[str], batch_size: int = 16) -> list[list[float]]:
        """Create embeddings for a list of texts.

        Args:
            texts: List of text strings to embed.
            batch_size: Number of texts to process per batch.

        Returns:
            List of embedding vectors (each a list of floats).
        """
        ...

    def embed_query(self, query: str) -> list[float]:
        """Create embedding for a single query.

        Args:
            query: Query text to embed.

        Returns:
            Embedding vector.
        """
        return self.embed_texts([query], batch_size=1)[0]


class LocalEmbedder(BaseEmbedder):
    """Local embeddings using sentence-transformers.

    This is the default embedder that runs entirely locally without
    any cloud dependencies.
    """

    DEFAULT_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

    def __init__(self, model_name: str | None = None):
        """Initialize the local embedder.

        Args:
            model_name: HuggingFace model name for sentence-transformers.
                       Defaults to all-MiniLM-L6-v2 (384 dimensions, fast).
        """
        self.model_name = model_name or self.DEFAULT_MODEL
        self._model = None
        self._dimension = None

    def _load_model(self):
        """Lazy load the model on first use."""
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
            except ImportError:
                raise ImportError(
                    "sentence-transformers is required for local embeddings. "
                    "Install with: pip install sentence-transformers"
                )

            logger.info(f"Loading embedding model: {self.model_name}")
            self._model = SentenceTransformer(self.model_name)
            self._dimension = self._model.get_sentence_embedding_dimension()
            logger.info(f"Loaded model with dimension: {self._dimension}")

    @property
    def dimension(self) -> int:
        """Return the embedding dimension."""
        if self._dimension is None:
            self._load_model()
        return self._dimension

    def embed_texts(self, texts: list[str], batch_size: int = 16) -> list[list[float]]:
        """Create embeddings using sentence-transformers.

        Args:
            texts: List of text strings to embed.
            batch_size: Number of texts to process per batch.

        Returns:
            List of embedding vectors.
        """
        self._load_model()
        logger.info(f"Embedding {len(texts)} texts in batches of {batch_size}...")
        embeddings = self._model.encode(
            texts,
            batch_size=batch_size,
            show_progress_bar=True,
            convert_to_numpy=True,
        )
        logger.info(f"Embedding complete: {len(embeddings)} vectors created")
        return [emb.tolist() for emb in embeddings]


class AzureEmbedder(BaseEmbedder):
    """Azure OpenAI embeddings.

    Use this when you want to use Azure OpenAI for embeddings
    instead of local models.
    """

    def __init__(self, azure_client: "AzureOpenAIClient"):
        """Initialize with an Azure OpenAI client.

        Args:
            azure_client: Configured AzureOpenAIClient instance.
        """
        self._client = azure_client
        self._dimension = azure_client.config.embedding_dimensions

    @property
    def dimension(self) -> int:
        """Return the embedding dimension from Azure config."""
        return self._dimension

    def embed_texts(self, texts: list[str], batch_size: int = 16) -> list[list[float]]:
        """Create embeddings using Azure OpenAI.

        Args:
            texts: List of text strings to embed.
            batch_size: Number of texts to process per batch.

        Returns:
            List of embedding vectors.
        """
        return self._client.create_embeddings(texts, batch_size=batch_size)


def get_default_embedder(
    use_azure: bool = False,
    azure_client: "AzureOpenAIClient | None" = None,
    model_name: str | None = None,
) -> BaseEmbedder:
    """Get the default embedder based on configuration.

    Args:
        use_azure: If True, use Azure OpenAI embeddings.
        azure_client: Required if use_azure is True.
        model_name: Model name for local embedder (ignored if use_azure).

    Returns:
        Configured embedder instance.
    """
    if use_azure:
        if azure_client is None:
            raise ValueError("azure_client is required when use_azure=True")
        return AzureEmbedder(azure_client)
    return LocalEmbedder(model_name=model_name)

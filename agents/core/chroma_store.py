"""ChromaDB vector store implementation.

Local, persistent vector storage using ChromaDB.
No cloud dependencies required.
"""

import logging
from pathlib import Path
from typing import Any

from agents.core.vector_store import BaseVectorStore

logger = logging.getLogger(__name__)


class ChromaVectorStore(BaseVectorStore):
    """ChromaDB-based vector store.

    Provides local, persistent vector storage with metadata filtering.
    """

    DEFAULT_COLLECTION = "odibi-code"

    def __init__(
        self,
        persist_dir: str,
        collection_name: str | None = None,
    ):
        """Initialize ChromaDB vector store.

        Args:
            persist_dir: Directory to persist the database.
            collection_name: Name of the collection (default: odibi-code).
        """
        try:
            import chromadb
        except ImportError:
            raise ImportError(
                "chromadb is required for local vector storage. "
                "Install with: pip install chromadb"
            )

        self.persist_dir = Path(persist_dir)
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        self.collection_name = collection_name or self.DEFAULT_COLLECTION

        logger.info(f"Initializing ChromaDB at {self.persist_dir}")
        try:
            self.client = chromadb.PersistentClient(path=str(self.persist_dir))
            self.collection = self.client.get_or_create_collection(
                name=self.collection_name,
                metadata={"hnsw:space": "cosine"},
            )
        except Exception as e:
            if "metadata" in str(e).lower() or "segment" in str(e).lower():
                logger.warning(f"Corrupted index detected, recreating: {e}")
                import shutil
                shutil.rmtree(self.persist_dir, ignore_errors=True)
                self.persist_dir.mkdir(parents=True, exist_ok=True)
                self.client = chromadb.PersistentClient(path=str(self.persist_dir))
                self.collection = self.client.get_or_create_collection(
                    name=self.collection_name,
                    metadata={"hnsw:space": "cosine"},
                )
            else:
                raise

    def add_chunks(self, chunks: list[dict[str, Any]]) -> dict[str, int]:
        """Insert or update chunks in ChromaDB.

        Args:
            chunks: List of chunk dictionaries with embeddings.

        Returns:
            Dictionary with 'succeeded' and 'failed' counts.
        """
        if not chunks:
            return {"succeeded": 0, "failed": 0}

        ids = []
        embeddings = []
        documents = []
        metadatas = []

        for chunk in chunks:
            ids.append(chunk["id"])
            embeddings.append(chunk["content_vector"])
            documents.append(chunk.get("content", ""))

            metadata = {}
            for key, value in chunk.items():
                if key in ("id", "content", "content_vector"):
                    continue
                if value is None:
                    continue
                if isinstance(value, (str, int, float, bool)):
                    metadata[key] = value
                elif isinstance(value, list) and all(isinstance(v, str) for v in value):
                    metadata[key] = ",".join(value)
            metadatas.append(metadata)

        try:
            self.collection.upsert(
                ids=ids,
                embeddings=embeddings,
                documents=documents,
                metadatas=metadatas,
            )
            logger.info(f"Upserted {len(chunks)} chunks to ChromaDB")
            return {"succeeded": len(chunks), "failed": 0}
        except Exception as e:
            logger.error(f"Failed to upsert chunks: {e}")
            return {"succeeded": 0, "failed": len(chunks)}

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
            filters: Optional metadata filters (e.g., {"chunk_type": "function"}).

        Returns:
            List of chunk dictionaries with similarity scores.
        """
        where = None
        if filters:
            if len(filters) == 1:
                key, value = next(iter(filters.items()))
                where = {key: value}
            else:
                where = {"$and": [{k: v} for k, v in filters.items()]}

        try:
            results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=k,
                where=where,
                include=["documents", "metadatas", "distances"],
            )
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []

        chunks = []
        if results["ids"] and results["ids"][0]:
            for i, doc_id in enumerate(results["ids"][0]):
                chunk = {
                    "id": doc_id,
                    "content": (results["documents"][0][i] if results["documents"] else ""),
                    "score": (1 - results["distances"][0][i] if results["distances"] else 0.0),
                }
                if results["metadatas"] and results["metadatas"][0]:
                    chunk.update(results["metadatas"][0][i] or {})
                chunks.append(chunk)

        return chunks

    def exists(self) -> bool:
        """Check if the collection has data.

        Returns:
            True if the collection exists and has documents.
        """
        return self.count() > 0

    def delete_all(self) -> bool:
        """Delete the collection and recreate it.

        Returns:
            True if successful.
        """
        try:
            self.client.delete_collection(self.collection_name)
            self.collection = self.client.get_or_create_collection(
                name=self.collection_name,
                metadata={"hnsw:space": "cosine"},
            )
            logger.info(f"Deleted and recreated collection: {self.collection_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete collection: {e}")
            return False

    def count(self) -> int:
        """Return the number of documents.

        Returns:
            Document count.
        """
        return self.collection.count()

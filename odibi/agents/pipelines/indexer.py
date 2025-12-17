from __future__ import annotations

"""Code indexing pipeline for the Odibi Agent Suite.

This module provides:
1. LocalIndexer - Local indexing with ChromaDB (default, no cloud needed)
2. MultiRepoIndexer - Index multiple repositories into a single index
3. AzureIndexer - Azure AI Search indexing (optional, for cloud deployment)
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from odibi.agents.core.code_parser import CodeParser
from odibi.agents.core.embeddings import BaseEmbedder, LocalEmbedder

if TYPE_CHECKING:
    from odibi.agents.core.azure_client import AzureConfig
    from odibi.agents.core.vector_store import BaseVectorStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LocalIndexer:
    """Indexes a codebase into a local vector store.

    This is the default indexer that runs entirely locally without
    any cloud dependencies. Uses ChromaDB for storage and
    sentence-transformers for embeddings.
    """

    def __init__(
        self,
        root: str,
        repo_name: str | None = None,
        embedder: BaseEmbedder | None = None,
        vector_store: "BaseVectorStore | None" = None,
        index_dir: str | None = None,
    ):
        """Initialize the local indexer.

        Args:
            root: Path to the repository root.
            repo_name: Name for this repo (defaults to folder name).
            embedder: Embedder to use (default: LocalEmbedder).
            vector_store: Vector store to use (default: ChromaVectorStore).
            index_dir: Directory for the index (default: <root>/.odibi/index).
        """
        self.root = Path(root)
        self.repo_name = repo_name or self.root.name

        self.embedder = embedder or LocalEmbedder()

        if vector_store is None:
            from odibi.agents.core.chroma_store import ChromaVectorStore

            index_dir = index_dir or str(self.root / ".odibi/index")
            self.vector_store = ChromaVectorStore(persist_dir=index_dir)
        else:
            self.vector_store = vector_store

    def parse_codebase(self) -> list[dict]:
        """Parse the codebase into chunks.

        Returns:
            List of chunk dictionaries.
        """
        logger.info(f"Parsing codebase at {self.root} (repo: {self.repo_name})")

        parser = CodeParser(str(self.root), self.repo_name)
        chunks = parser.parse_directory()

        logger.info(f"Parsed {len(chunks)} chunks from {self.repo_name}")
        return [chunk.to_dict() for chunk in chunks]

    def create_embeddings(
        self,
        chunks: list[dict],
        batch_size: int = 16,
    ) -> list[dict]:
        """Create embeddings for all chunks.

        Args:
            chunks: List of chunk dictionaries.
            batch_size: Number of texts per embedding batch.

        Returns:
            List of chunks with embeddings added.
        """
        embedder_name = type(self.embedder).__name__
        logger.info(f"Creating embeddings for {len(chunks)} chunks using {embedder_name}")

        texts = []
        for chunk in chunks:
            text_parts = [
                f"Name: {chunk['name']}",
                f"Type: {chunk['chunk_type']}",
                f"Module: {chunk['module_name']}",
            ]
            if chunk.get("docstring"):
                text_parts.append(f"Docstring: {chunk['docstring']}")
            if chunk.get("signature"):
                text_parts.append(f"Signature: {chunk['signature']}")

            text_parts.append(f"Code:\n{chunk['content'][:2000]}")

            texts.append("\n".join(text_parts))

        embeddings = self.embedder.embed_texts(texts, batch_size=batch_size)

        for chunk, embedding in zip(chunks, embeddings):
            chunk["content_vector"] = embedding

        logger.info(f"Created {len(embeddings)} embeddings")
        return chunks

    def upload_chunks(self, chunks: list[dict]) -> dict[str, int]:
        """Upload chunks to the vector store.

        Args:
            chunks: List of chunks with embeddings.

        Returns:
            Dictionary with upload statistics.
        """
        logger.info(f"Uploading {len(chunks)} chunks to vector store")

        result = self.vector_store.add_chunks(chunks)

        logger.info(f"Upload complete: {result['succeeded']} succeeded, {result['failed']} failed")
        return result

    def run_indexing(
        self,
        embedding_batch_size: int = 16,
    ) -> dict:
        """Run the complete indexing pipeline.

        Args:
            embedding_batch_size: Batch size for embeddings.

        Returns:
            Summary statistics.
        """
        start_time = datetime.now()
        logger.info("Starting local indexing pipeline")

        chunks = self.parse_codebase()

        if not chunks:
            logger.warning("No chunks parsed from codebase")
            return {
                "total_chunks": 0,
                "uploaded": 0,
                "failed": 0,
                "duration_seconds": 0,
                "timestamp": datetime.now().isoformat(),
            }

        chunks_with_embeddings = self.create_embeddings(chunks, batch_size=embedding_batch_size)

        upload_result = self.upload_chunks(chunks_with_embeddings)

        duration = (datetime.now() - start_time).total_seconds()

        summary = {
            "total_chunks": len(chunks),
            "uploaded": upload_result["succeeded"],
            "failed": upload_result["failed"],
            "duration_seconds": round(duration, 2),
            "embedding_model": getattr(self.embedder, "model_name", type(self.embedder).__name__),
            "embedding_dimension": self.embedder.dimension,
            "timestamp": datetime.now().isoformat(),
        }
        if "error" in upload_result:
            summary["upload_error"] = upload_result["error"]

        logger.info(f"Indexing complete: {summary}")
        return summary


class MultiRepoIndexer:
    """Index multiple repositories into a single vector store.

    Example:
        indexer = MultiRepoIndexer(
            repos=["d:/odibi", "d:/my_project"],
            index_dir="d:/.odibi/index",
        )
        indexer.run_indexing()
    """

    def __init__(
        self,
        repos: list[str],
        index_dir: str,
        embedder: BaseEmbedder | None = None,
    ):
        """Initialize the multi-repo indexer.

        Args:
            repos: List of paths to repository roots.
            index_dir: Directory for the shared index.
            embedder: Embedder to use (default: LocalEmbedder).
        """
        self.repos = [Path(r) for r in repos]
        self.index_dir = index_dir
        self.embedder = embedder or LocalEmbedder()

        from odibi.agents.core.chroma_store import ChromaVectorStore

        self.vector_store = ChromaVectorStore(persist_dir=index_dir)

    def run_indexing(
        self,
        embedding_batch_size: int = 16,
        force_recreate: bool = False,
    ) -> dict:
        """Index all repositories.

        Args:
            embedding_batch_size: Batch size for embeddings.
            force_recreate: Clear index before indexing.

        Returns:
            Summary statistics per repo.
        """
        if force_recreate:
            self.vector_store.delete_all()

        results = {}
        for repo_path in self.repos:
            repo_name = repo_path.name
            logger.info(f"Indexing repo: {repo_name} at {repo_path}")

            indexer = LocalIndexer(
                root=str(repo_path),
                repo_name=repo_name,
                embedder=self.embedder,
                vector_store=self.vector_store,
            )
            results[repo_name] = indexer.run_indexing(embedding_batch_size=embedding_batch_size)

        total_chunks = sum(r.get("uploaded", 0) for r in results.values())
        logger.info(
            f"Multi-repo indexing complete: {total_chunks} total chunks from {len(self.repos)} repos"
        )

        return {
            "repos": results,
            "total_chunks": total_chunks,
            "total_repos": len(self.repos),
        }


class AzureIndexer:
    """Indexes the Odibi codebase into Azure AI Search.

    Use this for cloud deployments where you need Azure AI Search
    for vector storage and Azure OpenAI for embeddings.
    """

    def __init__(
        self,
        config: "AzureConfig",
        odibi_root: str = "d:/odibi",
    ):
        """Initialize the Azure indexer.

        Args:
            config: Azure configuration.
            odibi_root: Path to the Odibi repository root.
        """
        from odibi.agents.core.azure_client import (
            AzureOpenAIClient,
            AzureSearchClient,
        )

        self.config = config
        self.odibi_root = Path(odibi_root)
        self.openai_client = AzureOpenAIClient(config)
        self.search_client = AzureSearchClient(config)

    def create_index(self, force_recreate: bool = False) -> bool:
        """Create or update the Azure AI Search index.

        Args:
            force_recreate: If True, delete and recreate the index.

        Returns:
            True if successful.
        """
        from odibi.agents.core.azure_client import get_odibi_index_schema

        logger.info(f"Creating index: {self.config.search_index}")
        logger.info(
            f"Using embedding model: {self.config.embedding_deployment} "
            f"({self.config.embedding_dimensions} dimensions)"
        )

        if force_recreate:
            try:
                self.search_client.delete_index()
                logger.info("Deleted existing index")
            except Exception as e:
                logger.warning(f"Could not delete index: {e}")

        schema = get_odibi_index_schema(config=self.config)
        success = self.search_client.create_index(schema)

        if success:
            logger.info("Index created successfully")
        return success

    def parse_codebase(self) -> list[dict]:
        """Parse the Odibi codebase into chunks.

        Returns:
            List of chunk dictionaries.
        """
        logger.info(f"Parsing codebase at {self.odibi_root}")

        parser = CodeParser(str(self.odibi_root))
        chunks = parser.parse_directory()

        logger.info(f"Parsed {len(chunks)} chunks")
        return [chunk.to_dict() for chunk in chunks]

    def create_embeddings(
        self,
        chunks: list[dict],
        batch_size: int = 16,
    ) -> list[dict]:
        """Create embeddings for all chunks using Azure OpenAI.

        Args:
            chunks: List of chunk dictionaries.
            batch_size: Number of texts per embedding batch.

        Returns:
            List of chunks with embeddings added.
        """
        logger.info(f"Creating embeddings for {len(chunks)} chunks")

        texts = []
        for chunk in chunks:
            text_parts = [
                f"Name: {chunk['name']}",
                f"Type: {chunk['chunk_type']}",
                f"Module: {chunk['module_name']}",
            ]
            if chunk.get("docstring"):
                text_parts.append(f"Docstring: {chunk['docstring']}")
            if chunk.get("signature"):
                text_parts.append(f"Signature: {chunk['signature']}")

            text_parts.append(f"Code:\n{chunk['content'][:2000]}")

            texts.append("\n".join(text_parts))

        embeddings = self.openai_client.create_embeddings(texts, batch_size=batch_size)

        for chunk, embedding in zip(chunks, embeddings):
            chunk["content_vector"] = embedding

        logger.info(f"Created {len(embeddings)} embeddings")
        return chunks

    def upload_chunks(
        self,
        chunks: list[dict],
        batch_size: int = 100,
    ) -> dict[str, int]:
        """Upload chunks to Azure AI Search.

        Args:
            chunks: List of chunks with embeddings.
            batch_size: Number of documents per upload batch.

        Returns:
            Dictionary with upload statistics.
        """
        logger.info(f"Uploading {len(chunks)} chunks to Azure AI Search")

        result = self.search_client.upload_documents(chunks, batch_size=batch_size)

        logger.info(f"Upload complete: {result['succeeded']} succeeded, {result['failed']} failed")
        return result

    def run_full_indexing(
        self,
        force_recreate: bool = False,
        embedding_batch_size: int = 16,
        upload_batch_size: int = 100,
    ) -> dict:
        """Run the complete indexing pipeline.

        Args:
            force_recreate: If True, recreate the index.
            embedding_batch_size: Batch size for embeddings.
            upload_batch_size: Batch size for uploads.

        Returns:
            Summary statistics.
        """
        start_time = datetime.now()
        logger.info("Starting Azure indexing pipeline")

        self.create_index(force_recreate=force_recreate)

        chunks = self.parse_codebase()

        chunks_with_embeddings = self.create_embeddings(chunks, batch_size=embedding_batch_size)

        upload_result = self.upload_chunks(chunks_with_embeddings, batch_size=upload_batch_size)

        duration = (datetime.now() - start_time).total_seconds()

        summary = {
            "total_chunks": len(chunks),
            "uploaded": upload_result["succeeded"],
            "failed": upload_result["failed"],
            "duration_seconds": round(duration, 2),
            "index_name": self.config.search_index,
            "timestamp": datetime.now().isoformat(),
        }

        logger.info(f"Indexing complete: {summary}")
        return summary


# Keep OdibiIndexer as an alias for backward compatibility
OdibiIndexer = AzureIndexer


def run_local_indexing_cli():
    """CLI entry point for local indexing."""
    import argparse

    parser = argparse.ArgumentParser(description="Index Odibi codebase locally (no cloud required)")
    parser.add_argument(
        "--odibi-root",
        default=".",
        help="Path to Odibi repository root (default: current directory)",
    )
    parser.add_argument(
        "--index-dir",
        default=None,
        help="Directory for the index (default: <odibi_root>/.odibi/index)",
    )
    parser.add_argument(
        "--embedding-model",
        default=None,
        help="Sentence-transformers model (default: all-MiniLM-L6-v2)",
    )
    parser.add_argument(
        "--force-recreate",
        action="store_true",
        help="Force recreate the index",
    )
    parser.add_argument(
        "--embedding-batch-size",
        type=int,
        default=16,
        help="Batch size for embedding creation",
    )

    args = parser.parse_args()

    odibi_root = Path(args.odibi_root).resolve()
    if not odibi_root.exists():
        print(f"Error: {odibi_root} does not exist")
        return

    embedder = LocalEmbedder(model_name=args.embedding_model)

    print(f"Odibi root: {odibi_root}")
    print(f"Embedding model: {embedder.model_name}")
    print()

    from odibi.agents.core.chroma_store import ChromaVectorStore

    index_dir = args.index_dir or str(odibi_root / ".odibi/index")
    vector_store = ChromaVectorStore(persist_dir=index_dir)

    if args.force_recreate:
        vector_store.delete_all()

    indexer = LocalIndexer(
        odibi_root=str(odibi_root),
        embedder=embedder,
        vector_store=vector_store,
    )

    summary = indexer.run_indexing(
        embedding_batch_size=args.embedding_batch_size,
    )

    print(json.dumps(summary, indent=2))


def run_azure_indexing_cli():
    """CLI entry point for Azure indexing (legacy)."""
    import argparse

    from odibi.agents.core.azure_client import AzureConfig

    parser = argparse.ArgumentParser(description="Index Odibi codebase to Azure AI Search")
    parser.add_argument(
        "--odibi-root",
        default="d:/odibi",
        help="Path to Odibi repository root",
    )
    parser.add_argument(
        "--force-recreate",
        action="store_true",
        help="Force recreate the search index",
    )
    parser.add_argument(
        "--embedding-batch-size",
        type=int,
        default=16,
        help="Batch size for embedding creation",
    )
    parser.add_argument(
        "--upload-batch-size",
        type=int,
        default=100,
        help="Batch size for document upload",
    )
    parser.add_argument(
        "--chat-model",
        default=None,
        help="Azure OpenAI chat deployment name",
    )
    parser.add_argument(
        "--embedding-model",
        default=None,
        help="Azure OpenAI embedding deployment name",
    )
    parser.add_argument(
        "--index-name",
        default=None,
        help="Azure AI Search index name",
    )

    args = parser.parse_args()

    config = AzureConfig.from_env()

    if args.chat_model:
        config.chat_deployment = args.chat_model
    if args.embedding_model:
        config.embedding_deployment = args.embedding_model
        if "ada" in args.embedding_model.lower() or "3-small" in args.embedding_model.lower():
            config.embedding_dimensions = 1536
        elif "3-large" in args.embedding_model.lower():
            config.embedding_dimensions = 3072
    if args.index_name:
        config.search_index = args.index_name

    errors = config.validate()
    if errors:
        print("Configuration errors:")
        for error in errors:
            print(f"  - {error}")
        print("\nSet environment variables or use CLI arguments.")
        return

    print("Configuration:")
    print(f"  Chat model: {config.chat_deployment}")
    print(f"  Embedding model: {config.embedding_deployment} ({config.embedding_dimensions}d)")
    print(f"  Search index: {config.search_index}")
    print()

    indexer = AzureIndexer(config, odibi_root=args.odibi_root)
    summary = indexer.run_full_indexing(
        force_recreate=args.force_recreate,
        embedding_batch_size=args.embedding_batch_size,
        upload_batch_size=args.upload_batch_size,
    )

    print(json.dumps(summary, indent=2))


def run_cycle_indexing_cli():
    """CLI entry point for manual cycle indexing.

    This indexes validated cycle learnings for semantic retrieval.
    Must be explicitly invoked by the user.

    Usage:
        python -m agents.pipelines.indexer --cycles
        odibi index cycles
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Index completed cycle learnings (manual, explicit invocation)"
    )
    parser.add_argument(
        "--odibi-root",
        default=".",
        help="Path to Odibi repository root (default: current directory)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-index even if already indexed",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show index statistics only, do not index",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear all indexed cycle data",
    )

    args = parser.parse_args()

    odibi_root = Path(args.odibi_root).resolve()
    odibi_dir = odibi_root / ".odibi"

    if not odibi_dir.exists():
        print(f"Error: {odibi_dir} does not exist")
        print("Run this from an Odibi workspace root.")
        return

    from odibi.agents.core.indexing import CycleIndexManager

    manager = CycleIndexManager(str(odibi_dir))

    if args.clear:
        print("Clearing cycle index...")
        if manager.clear_index():
            print("✅ Cycle index cleared")
        else:
            print("❌ Failed to clear index")
        return

    if args.stats:
        print("Cycle Index Statistics:")
        stats = manager.get_index_stats()
        print(f"  Indexed cycles: {stats['indexed_cycle_count']}")
        print(f"  Vector documents: {stats['vector_document_count']}")
        if stats["indexed_cycle_ids"]:
            print("  Indexed cycle IDs:")
            for cycle_id in stats["indexed_cycle_ids"][:10]:
                print(f"    - {cycle_id}")
            if len(stats["indexed_cycle_ids"]) > 10:
                print(f"    ... and {len(stats['indexed_cycle_ids']) - 10} more")
        return

    print("Starting manual cycle indexing...")
    print(f"Odibi root: {odibi_root}")
    print()

    result = manager.index_completed_cycles(force=args.force)

    print("\nIndexing Results:")
    print(f"  ✅ Indexed: {len(result.indexed)}")
    for cycle_id in result.indexed:
        print(f"      - {cycle_id}")

    if result.skipped_already_indexed:
        print(f"  ⏭️  Skipped (already indexed): {len(result.skipped_already_indexed)}")

    if result.skipped_incomplete:
        print(f"  ⏭️  Skipped (incomplete): {len(result.skipped_incomplete)}")

    if result.skipped_failed:
        print(f"  ⏭️  Skipped (failed): {len(result.skipped_failed)}")

    if result.errors:
        print(f"  ❌ Errors: {len(result.errors)}")
        for error in result.errors:
            print(f"      - {error}")

    print(f"\nTotal processed: {result.total_processed}")


# Default CLI uses local indexing
run_indexing_from_cli = run_local_indexing_cli

if __name__ == "__main__":
    run_local_indexing_cli()

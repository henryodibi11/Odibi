"""Code indexing pipeline for Azure AI Search.

This module handles:
1. Parsing the Odibi codebase
2. Creating embeddings via Azure OpenAI
3. Uploading to Azure AI Search
"""

import json
import logging
from datetime import datetime
from pathlib import Path

from odibi.agents.core.azure_client import (
    AzureConfig,
    AzureOpenAIClient,
    AzureSearchClient,
    get_odibi_index_schema,
)
from odibi.agents.core.code_parser import OdibiCodeParser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OdibiIndexer:
    """Indexes the Odibi codebase into Azure AI Search."""

    def __init__(
        self,
        config: AzureConfig,
        odibi_root: str = "d:/odibi",
    ):
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
        logger.info(f"Creating index: {self.config.search_index}")
        logger.info(
            f"Using embedding model: {self.config.embedding_deployment} ({self.config.embedding_dimensions} dimensions)"
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

        parser = OdibiCodeParser(str(self.odibi_root))
        chunks = parser.parse_directory()

        logger.info(f"Parsed {len(chunks)} chunks")
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
        logger.info("Starting full indexing pipeline")

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

    def incremental_update(self, file_paths: list[str]) -> dict:
        """Update index for specific files only.

        Args:
            file_paths: List of file paths to reindex.

        Returns:
            Update statistics.
        """
        logger.info(f"Incremental update for {len(file_paths)} files")

        parser = OdibiCodeParser(str(self.odibi_root))
        all_chunks = []

        for file_path in file_paths:
            full_path = self.odibi_root / file_path
            if full_path.exists():
                chunks = parser.parse_file(full_path)
                all_chunks.extend([c.to_dict() for c in chunks])

        if not all_chunks:
            return {"updated": 0, "message": "No chunks found"}

        chunks_with_embeddings = self.create_embeddings(all_chunks)
        result = self.upload_chunks(chunks_with_embeddings)

        return {
            "files_processed": len(file_paths),
            "chunks_updated": result["succeeded"],
            "failed": result["failed"],
        }


def run_indexing_from_cli():
    """CLI entry point for indexing."""
    import argparse

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
        help="Azure OpenAI chat deployment name (e.g., gpt-4.1, gpt-4o, gpt-35-turbo)",
    )
    parser.add_argument(
        "--embedding-model",
        default=None,
        help="Azure OpenAI embedding deployment name (e.g., text-embedding-3-large, text-embedding-ada-002)",
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

    indexer = OdibiIndexer(config, odibi_root=args.odibi_root)
    summary = indexer.run_full_indexing(
        force_recreate=args.force_recreate,
        embedding_batch_size=args.embedding_batch_size,
        upload_batch_size=args.upload_batch_size,
    )

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    run_indexing_from_cli()

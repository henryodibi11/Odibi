"""Azure OpenAI and Azure AI Search REST API clients.

All interactions use REST API (no SDK) for Databricks compatibility.
"""

import time
from dataclasses import dataclass
from typing import Any, Optional

import requests


@dataclass
class AzureConfig:
    """Azure resource configuration.

    Supports any Azure OpenAI model deployment. Configure via:
    - Constructor arguments
    - Environment variables
    - .env file

    Example:
        ```python
        # Use GPT-4.1
        config = AzureConfig(chat_deployment="gpt-4.1")

        # Use GPT-4o
        config = AzureConfig(chat_deployment="gpt-4o")

        # Use GPT-3.5-turbo for cost savings
        config = AzureConfig(chat_deployment="gpt-35-turbo")

        # Custom endpoint and model
        config = AzureConfig(
            openai_endpoint="https://my-resource.openai.azure.com/",
            chat_deployment="my-gpt4-deployment",
            embedding_deployment="my-ada-deployment",
        )
        ```
    """

    openai_endpoint: str = ""
    openai_api_key: str = ""
    openai_api_version: str = "2024-12-01-preview"
    chat_deployment: str = ""
    embedding_deployment: str = ""
    embedding_dimensions: int = 3072

    search_endpoint: str = ""
    search_api_key: str = ""
    search_index: str = "odibi-code"
    search_api_version: str = "2023-11-01"

    def __post_init__(self):
        """Load defaults from environment variables if not provided."""
        import os

        if not self.openai_endpoint:
            self.openai_endpoint = os.getenv(
                "AZURE_OPENAI_ENDPOINT",
                os.getenv("AZURE_OPENAI_BASE", "https://na-mfg.openai.azure.com/"),
            )

        if not self.openai_api_key:
            self.openai_api_key = os.getenv("AZURE_OPENAI_API_KEY", "")

        if not self.chat_deployment:
            self.chat_deployment = os.getenv(
                "AZURE_OPENAI_CHAT_DEPLOYMENT", os.getenv("AZURE_OPENAI_MODEL", "gpt-4.1")
            )

        if not self.embedding_deployment:
            self.embedding_deployment = os.getenv(
                "AZURE_OPENAI_EMBEDDING_DEPLOYMENT",
                os.getenv("AZURE_OPENAI_EMBEDDING_MODEL", "text-embedding-3-large"),
            )

        if not self.search_endpoint:
            self.search_endpoint = os.getenv(
                "AZURE_SEARCH_ENDPOINT", "https://goatbisearch.search.windows.net"
            )

        if not self.search_api_key:
            self.search_api_key = os.getenv("AZURE_SEARCH_API_KEY", "")

        if not self.search_index:
            self.search_index = os.getenv("AZURE_SEARCH_INDEX", "odibi-code")

        embedding_dims = os.getenv("AZURE_OPENAI_EMBEDDING_DIMENSIONS")
        if embedding_dims:
            self.embedding_dimensions = int(embedding_dims)
        elif "ada" in self.embedding_deployment.lower():
            self.embedding_dimensions = 1536
        elif "3-large" in self.embedding_deployment.lower():
            self.embedding_dimensions = 3072
        elif "3-small" in self.embedding_deployment.lower():
            self.embedding_dimensions = 1536

    @classmethod
    def from_env(cls) -> "AzureConfig":
        """Create config from environment variables.

        Environment variables:
            AZURE_OPENAI_ENDPOINT: Azure OpenAI endpoint URL
            AZURE_OPENAI_API_KEY: Azure OpenAI API key
            AZURE_OPENAI_CHAT_DEPLOYMENT: Chat model deployment name
            AZURE_OPENAI_EMBEDDING_DEPLOYMENT: Embedding model deployment name
            AZURE_OPENAI_EMBEDDING_DIMENSIONS: Embedding vector dimensions
            AZURE_SEARCH_ENDPOINT: Azure AI Search endpoint URL
            AZURE_SEARCH_API_KEY: Azure AI Search API key
            AZURE_SEARCH_INDEX: Search index name

        Returns:
            AzureConfig instance.
        """
        return cls()

    def validate(
        self,
        require_chat: bool = True,
        require_embeddings: bool = False,
        require_search: bool = False,
    ) -> list[str]:
        """Validate the configuration.

        Args:
            require_chat: Require chat model configuration (default: True).
            require_embeddings: Require embedding configuration (default: False).
            require_search: Require Azure AI Search configuration (default: False).

        Returns:
            List of validation error messages (empty if valid).
        """
        errors = []

        if require_chat:
            if not self.openai_api_key:
                errors.append("AZURE_OPENAI_API_KEY is required for chat")

            if not self.openai_endpoint:
                errors.append("AZURE_OPENAI_ENDPOINT is required for chat")

            if not self.chat_deployment:
                errors.append("AZURE_OPENAI_CHAT_DEPLOYMENT is required for chat")

        if require_embeddings:
            if not self.openai_api_key:
                errors.append("AZURE_OPENAI_API_KEY is required for embeddings")

            if not self.openai_endpoint:
                errors.append("AZURE_OPENAI_ENDPOINT is required for embeddings")

            if not self.embedding_deployment:
                errors.append("AZURE_OPENAI_EMBEDDING_DEPLOYMENT is required for embeddings")

        if require_search:
            if not self.search_api_key:
                errors.append("AZURE_SEARCH_API_KEY is required for Azure AI Search")

            if not self.search_endpoint:
                errors.append("AZURE_SEARCH_ENDPOINT is required for Azure AI Search")

        return errors


class AzureOpenAIClient:
    """REST-based Azure OpenAI client for Databricks compatibility."""

    def __init__(self, config: AzureConfig):
        self.config = config
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Content-Type": "application/json",
                "api-key": config.openai_api_key,
            }
        )

    def chat_completion(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0.0,
        max_tokens: int = 4096,
        system_prompt: Optional[str] = None,
    ) -> str:
        """Send chat completion request to Azure OpenAI.

        Args:
            messages: List of message dictionaries with 'role' and 'content'.
            temperature: Sampling temperature (0.0 = deterministic).
            max_tokens: Maximum tokens in response.
            system_prompt: Optional system message to prepend.

        Returns:
            Assistant's response text.

        Raises:
            RuntimeError: If API call fails.
        """
        url = (
            f"{self.config.openai_endpoint.rstrip('/')}"
            f"/openai/deployments/{self.config.chat_deployment}"
            f"/chat/completions?api-version={self.config.openai_api_version}"
        )

        full_messages = []
        if system_prompt:
            full_messages.append({"role": "system", "content": system_prompt})
        full_messages.extend(messages)

        model_lower = self.config.chat_deployment.lower()
        uses_completion_tokens = any(x in model_lower for x in ("o1", "o3", "o4", "gpt-5"))

        payload = {
            "messages": full_messages,
        }

        if uses_completion_tokens:
            payload["max_completion_tokens"] = max_tokens
        else:
            payload["temperature"] = temperature
            payload["max_tokens"] = max_tokens

        response = self._session.post(url, json=payload)

        if response.status_code != 200:
            raise RuntimeError(
                f"Azure OpenAI chat completion failed: {response.status_code} - {response.text}"
            )

        result = response.json()
        return result["choices"][0]["message"]["content"]

    def create_embeddings(
        self,
        texts: list[str],
        batch_size: int = 16,
    ) -> list[list[float]]:
        """Create embeddings for a list of texts.

        Args:
            texts: List of text strings to embed.
            batch_size: Number of texts per API call.

        Returns:
            List of embedding vectors (list of floats).

        Raises:
            RuntimeError: If API call fails.
        """
        url = (
            f"{self.config.openai_endpoint.rstrip('/')}"
            f"/openai/deployments/{self.config.embedding_deployment}"
            f"/embeddings?api-version={self.config.openai_api_version}"
        )

        all_embeddings = []

        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            payload = {"input": batch}

            response = self._session.post(url, json=payload)

            if response.status_code != 200:
                raise RuntimeError(
                    f"Azure OpenAI embeddings failed: {response.status_code} - {response.text}"
                )

            result = response.json()
            batch_embeddings = [item["embedding"] for item in result["data"]]
            all_embeddings.extend(batch_embeddings)

            if i + batch_size < len(texts):
                time.sleep(0.1)

        return all_embeddings


class AzureSearchClient:
    """REST-based Azure AI Search client for Databricks compatibility."""

    def __init__(self, config: AzureConfig):
        self.config = config
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Content-Type": "application/json",
                "api-key": config.search_api_key,
            }
        )

    def search(
        self,
        query: str,
        vector: Optional[list[float]] = None,
        top: int = 10,
        filter_expression: Optional[str] = None,
        select: Optional[list[str]] = None,
        semantic_config: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Perform hybrid search (keyword + vector) on Azure AI Search.

        Args:
            query: Text query for keyword search.
            vector: Optional embedding vector for vector search.
            top: Number of results to return.
            filter_expression: OData filter expression.
            select: List of fields to return.
            semantic_config: Semantic configuration name for reranking.

        Returns:
            List of search result documents.

        Raises:
            RuntimeError: If search fails.
        """
        url = (
            f"{self.config.search_endpoint.rstrip('/')}"
            f"/indexes/{self.config.search_index}"
            f"/docs/search?api-version={self.config.search_api_version}"
        )

        payload: dict[str, Any] = {
            "search": query,
            "top": top,
            "queryType": "semantic" if semantic_config else "simple",
        }

        if vector:
            payload["vectorQueries"] = [
                {
                    "kind": "vector",
                    "vector": vector,
                    "fields": "content_vector",
                    "k": top,
                }
            ]

        if filter_expression:
            payload["filter"] = filter_expression

        if select:
            payload["select"] = ",".join(select)

        if semantic_config:
            payload["semanticConfiguration"] = semantic_config

        response = self._session.post(url, json=payload)

        if response.status_code != 200:
            raise RuntimeError(f"Azure AI Search failed: {response.status_code} - {response.text}")

        result = response.json()
        return result.get("value", [])

    def upload_documents(
        self,
        documents: list[dict[str, Any]],
        batch_size: int = 100,
    ) -> dict[str, int]:
        """Upload documents to the search index.

        Args:
            documents: List of documents to upload. Each must have an 'id' field.
            batch_size: Number of documents per upload batch.

        Returns:
            Dictionary with 'succeeded' and 'failed' counts.

        Raises:
            RuntimeError: If upload fails.
        """
        url = (
            f"{self.config.search_endpoint.rstrip('/')}"
            f"/indexes/{self.config.search_index}"
            f"/docs/index?api-version={self.config.search_api_version}"
        )

        succeeded = 0
        failed = 0

        for i in range(0, len(documents), batch_size):
            batch = documents[i : i + batch_size]
            payload = {"value": [{"@search.action": "mergeOrUpload", **doc} for doc in batch]}

            response = self._session.post(url, json=payload)

            if response.status_code not in (200, 207):
                raise RuntimeError(
                    f"Azure AI Search upload failed: {response.status_code} - {response.text}"
                )

            result = response.json()
            for item in result.get("value", []):
                if item.get("status"):
                    succeeded += 1
                else:
                    failed += 1

            if i + batch_size < len(documents):
                time.sleep(0.1)

        return {"succeeded": succeeded, "failed": failed}

    def create_index(self, index_schema: dict[str, Any]) -> bool:
        """Create or update a search index.

        Args:
            index_schema: Full index definition including name, fields, etc.

        Returns:
            True if successful.

        Raises:
            RuntimeError: If index creation fails.
        """
        index_name = index_schema.get("name", self.config.search_index)
        url = (
            f"{self.config.search_endpoint.rstrip('/')}"
            f"/indexes/{index_name}"
            f"?api-version={self.config.search_api_version}"
        )

        response = self._session.put(url, json=index_schema)

        if response.status_code not in (200, 201):
            raise RuntimeError(
                f"Azure AI Search index creation failed: {response.status_code} - {response.text}"
            )

        return True

    def delete_index(self, index_name: Optional[str] = None) -> bool:
        """Delete a search index.

        Args:
            index_name: Name of the index to delete (uses config default if None).

        Returns:
            True if successful.

        Raises:
            RuntimeError: If deletion fails.
        """
        name = index_name or self.config.search_index
        url = (
            f"{self.config.search_endpoint.rstrip('/')}"
            f"/indexes/{name}"
            f"?api-version={self.config.search_api_version}"
        )

        response = self._session.delete(url)

        if response.status_code not in (204, 404):
            raise RuntimeError(
                f"Azure AI Search index deletion failed: {response.status_code} - {response.text}"
            )

        return True


def get_odibi_index_schema(
    config: Optional[AzureConfig] = None,
    index_name: Optional[str] = None,
    embedding_dimensions: int = 3072,
) -> dict[str, Any]:
    """Get the Azure AI Search index schema for Odibi code.

    Args:
        config: Optional AzureConfig to use for embedding settings.
        index_name: Optional custom index name.
        embedding_dimensions: Vector dimensions (default: 3072 for text-embedding-3-large).
            Use 1536 for text-embedding-ada-002 or text-embedding-3-small.

    Returns:
        Index schema dictionary for the search index.

    Example:
        ```python
        # Default schema (3072 dimensions)
        schema = get_odibi_index_schema()

        # For ada-002 embeddings (1536 dimensions)
        schema = get_odibi_index_schema(embedding_dimensions=1536)

        # Use config settings
        config = AzureConfig(embedding_deployment="text-embedding-ada-002")
        schema = get_odibi_index_schema(config=config)
        ```
    """
    if config:
        embedding_dimensions = config.embedding_dimensions
        actual_index_name = index_name or config.search_index
        openai_endpoint = config.openai_endpoint
        embedding_deployment = config.embedding_deployment
    else:
        actual_index_name = index_name or "odibi-code"
        openai_endpoint = "https://na-mfg.openai.azure.com/"
        embedding_deployment = "text-embedding-3-large"

    return {
        "name": actual_index_name,
        "fields": [
            {"name": "id", "type": "Edm.String", "key": True, "filterable": True},
            {"name": "file_path", "type": "Edm.String", "filterable": True, "searchable": True},
            {"name": "module_name", "type": "Edm.String", "filterable": True, "searchable": True},
            {"name": "chunk_type", "type": "Edm.String", "filterable": True},
            {"name": "name", "type": "Edm.String", "searchable": True, "filterable": True},
            {"name": "content", "type": "Edm.String", "searchable": True},
            {"name": "docstring", "type": "Edm.String", "searchable": True},
            {"name": "signature", "type": "Edm.String", "searchable": True},
            {"name": "parent_class", "type": "Edm.String", "filterable": True, "searchable": True},
            {"name": "imports", "type": "Collection(Edm.String)", "filterable": True},
            {"name": "dependencies", "type": "Collection(Edm.String)", "filterable": True},
            {"name": "line_start", "type": "Edm.Int32", "filterable": True},
            {"name": "line_end", "type": "Edm.Int32", "filterable": True},
            {"name": "engine_type", "type": "Edm.String", "filterable": True},
            {"name": "tags", "type": "Collection(Edm.String)", "filterable": True},
            {
                "name": "content_vector",
                "type": "Collection(Edm.Single)",
                "searchable": True,
                "dimensions": embedding_dimensions,
                "vectorSearchProfile": "default-profile",
            },
        ],
        "vectorSearch": {
            "profiles": [
                {
                    "name": "default-profile",
                    "algorithm": "default-algorithm",
                    "vectorizer": "default-vectorizer",
                }
            ],
            "algorithms": [
                {
                    "name": "default-algorithm",
                    "kind": "hnsw",
                    "hnswParameters": {
                        "m": 4,
                        "efConstruction": 400,
                        "efSearch": 500,
                        "metric": "cosine",
                    },
                }
            ],
            "vectorizers": [
                {
                    "name": "default-vectorizer",
                    "kind": "azureOpenAI",
                    "azureOpenAIParameters": {
                        "resourceUri": openai_endpoint,
                        "deploymentId": embedding_deployment,
                        "modelName": embedding_deployment,
                    },
                }
            ],
        },
        "semantic": {
            "configurations": [
                {
                    "name": "odibi-semantic",
                    "prioritizedFields": {
                        "contentFields": [{"fieldName": "content"}],
                        "titleField": {"fieldName": "name"},
                        "keywordsFields": [{"fieldName": "docstring"}],
                    },
                }
            ]
        },
    }

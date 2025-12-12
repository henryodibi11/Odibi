"""Provider-agnostic LLM client using OpenAI-compatible API.

Works with any provider that supports the OpenAI chat completions format:
- OpenAI
- Azure OpenAI
- Ollama
- LM Studio
- vLLM
- LocalAI
- Together AI
- Anyscale
- etc.

All you need is: endpoint, api_key, model name.
"""

import json
from dataclasses import dataclass
from typing import Optional

import requests


@dataclass
class LLMConfig:
    """LLM provider configuration.

    Works with any OpenAI-compatible API endpoint.

    Example configurations:

        # OpenAI
        LLMConfig(
            endpoint="https://api.openai.com/v1",
            api_key="sk-...",
            model="gpt-4o",
        )

        # Azure OpenAI
        LLMConfig(
            endpoint="https://your-resource.openai.azure.com",
            api_key="...",
            model="gpt-4o",  # deployment name
            api_type="azure",
            api_version="2024-12-01-preview",
        )

        # Ollama (local)
        LLMConfig(
            endpoint="http://localhost:11434/v1",
            api_key="ollama",  # any non-empty string
            model="llama3.2",
        )

        # LM Studio (local)
        LLMConfig(
            endpoint="http://localhost:1234/v1",
            api_key="lm-studio",
            model="local-model",
        )
    """

    endpoint: str = ""
    api_key: str = ""
    model: str = "gpt-4o"
    api_type: str = "openai"
    api_version: str = "2024-12-01-preview"

    def __post_init__(self):
        import os

        if not self.endpoint:
            self.endpoint = os.getenv(
                "LLM_ENDPOINT",
                os.getenv(
                    "OPENAI_API_BASE",
                    os.getenv("AZURE_OPENAI_ENDPOINT", "https://api.openai.com/v1"),
                ),
            )

        if not self.api_key:
            self.api_key = os.getenv(
                "LLM_API_KEY",
                os.getenv("OPENAI_API_KEY", os.getenv("AZURE_OPENAI_API_KEY", "")),
            )

        if not self.model:
            self.model = os.getenv(
                "LLM_MODEL",
                os.getenv("OPENAI_MODEL", os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")),
            )

        if "azure" in self.endpoint.lower() and self.api_type == "openai":
            self.api_type = "azure"


class LLMClient:
    """Provider-agnostic LLM client.

    Uses the OpenAI chat completions API format, which is supported by
    most LLM providers.
    """

    def __init__(self, config: LLMConfig):
        """Initialize the LLM client.

        Args:
            config: LLM configuration with endpoint, key, and model.
        """
        self.config = config
        self._session = requests.Session()

    def _get_headers(self) -> dict[str, str]:
        """Get request headers based on API type."""
        headers = {"Content-Type": "application/json"}

        if self.config.api_type == "azure":
            headers["api-key"] = self.config.api_key
        else:
            headers["Authorization"] = f"Bearer {self.config.api_key}"

        return headers

    def _get_url(self) -> str:
        """Get the chat completions URL based on API type."""
        endpoint = self.config.endpoint.rstrip("/")

        if self.config.api_type == "azure":
            return (
                f"{endpoint}/openai/deployments/{self.config.model}"
                f"/chat/completions?api-version={self.config.api_version}"
            )
        else:
            if not endpoint.endswith("/v1"):
                endpoint = f"{endpoint}/v1"
            return f"{endpoint}/chat/completions"

    def chat(
        self,
        messages: list[dict[str, str]],
        system_prompt: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: int = 4096,
    ) -> str:
        """Send a chat completion request.

        Args:
            messages: List of message dicts with 'role' and 'content'.
            system_prompt: Optional system message to prepend.
            temperature: Sampling temperature (0.0 = deterministic).
            max_tokens: Maximum tokens in response.

        Returns:
            The assistant's response text.

        Raises:
            LLMError: If the API call fails.
        """
        url = self._get_url()
        headers = self._get_headers()

        full_messages = []
        if system_prompt:
            full_messages.append({"role": "system", "content": system_prompt})
        full_messages.extend(messages)

        payload = {
            "messages": full_messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        if self.config.api_type != "azure":
            payload["model"] = self.config.model

        try:
            response = self._session.post(url, headers=headers, json=payload, timeout=120)

            if response.status_code != 200:
                raise LLMError(f"LLM API error: {response.status_code} - {response.text[:500]}")

            result = response.json()
            return result["choices"][0]["message"]["content"]

        except requests.exceptions.Timeout:
            raise LLMError("LLM API request timed out")
        except requests.exceptions.RequestException as e:
            raise LLMError(f"LLM API connection error: {e}")
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            raise LLMError(f"Invalid LLM API response: {e}")

    def chat_stream(
        self,
        messages: list[dict[str, str]],
        system_prompt: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: int = 4096,
    ):
        """Send a streaming chat completion request.

        Args:
            messages: List of message dicts with 'role' and 'content'.
            system_prompt: Optional system message to prepend.
            temperature: Sampling temperature (0.0 = deterministic).
            max_tokens: Maximum tokens in response.

        Yields:
            Chunks of the assistant's response text.

        Raises:
            LLMError: If the API call fails.
        """
        url = self._get_url()
        headers = self._get_headers()

        full_messages = []
        if system_prompt:
            full_messages.append({"role": "system", "content": system_prompt})
        full_messages.extend(messages)

        payload = {
            "messages": full_messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": True,
        }

        if self.config.api_type != "azure":
            payload["model"] = self.config.model

        try:
            response = self._session.post(
                url, headers=headers, json=payload, timeout=120, stream=True
            )

            if response.status_code != 200:
                raise LLMError(f"LLM API error: {response.status_code} - {response.text[:500]}")

            for line in response.iter_lines():
                if line:
                    line = line.decode("utf-8")
                    if line.startswith("data: "):
                        data = line[6:]
                        if data == "[DONE]":
                            break
                        try:
                            chunk = json.loads(data)
                            delta = chunk.get("choices", [{}])[0].get("delta", {})
                            content = delta.get("content", "")
                            if content:
                                yield content
                        except json.JSONDecodeError:
                            continue

        except requests.exceptions.Timeout:
            raise LLMError("LLM API request timed out")
        except requests.exceptions.RequestException as e:
            raise LLMError(f"LLM API connection error: {e}")


class LLMError(Exception):
    """Error from LLM API call."""

    pass


def create_client(config: LLMConfig) -> LLMClient:
    """Create an LLM client from configuration.

    Args:
        config: LLM configuration.

    Returns:
        LLMClient instance.
    """
    return LLMClient(config)

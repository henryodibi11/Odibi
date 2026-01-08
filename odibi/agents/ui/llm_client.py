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
import time
import threading
from dataclasses import dataclass, field
from typing import Callable, Generator, Optional

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
            api_key="not-needed",  # any non-empty string
            model="llama3.2",
        )

        # LM Studio (local)
        LLMConfig(
            endpoint="http://localhost:1234/v1",
            api_key="not-needed",
            model="local-model",
        )
    """

    endpoint: str = ""
    api_key: str = ""
    model: str = "gpt-4o-mini"
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


@dataclass
class TokenUsage:
    """Token usage information from a completion."""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

    @property
    def input_tokens(self) -> int:
        return self.prompt_tokens

    @property
    def output_tokens(self) -> int:
        return self.completion_tokens


@dataclass
class ChatResponse:
    """Enhanced response from a chat completion."""

    content: Optional[str] = None
    tool_calls: Optional[list] = None
    usage: TokenUsage = field(default_factory=TokenUsage)
    finish_reason: str = ""
    model: str = ""


class LLMClient:
    """Provider-agnostic LLM client with streaming and retry support.

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
        self._cancel_event = threading.Event()

    def cancel(self) -> None:
        """Cancel any ongoing request."""
        self._cancel_event.set()

    def reset_cancel(self) -> None:
        """Reset the cancel flag."""
        self._cancel_event.clear()

    @property
    def is_cancelled(self) -> bool:
        """Check if cancellation was requested."""
        return self._cancel_event.is_set()

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

    def _retry_with_backoff(
        self,
        func: Callable,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
    ):
        """Execute a function with exponential backoff retry.

        Args:
            func: Function to execute.
            max_retries: Maximum number of retries.
            base_delay: Initial delay in seconds.
            max_delay: Maximum delay between retries.

        Returns:
            Result of the function.

        Raises:
            The last exception if all retries fail.
        """
        last_exception = None

        for attempt in range(max_retries + 1):
            if self.is_cancelled:
                raise LLMError("Request cancelled")

            try:
                return func()
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                last_exception = e
                if attempt < max_retries:
                    delay = min(base_delay * (2**attempt), max_delay)
                    time.sleep(delay)
            except LLMError as e:
                if "rate limit" in str(e).lower() or "429" in str(e):
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (2**attempt), max_delay)
                        time.sleep(delay)
                else:
                    raise

        raise LLMError(f"Max retries exceeded: {last_exception}")

    def chat(
        self,
        messages: list[dict[str, str]],
        system_prompt: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: int = 16384,
        tools: Optional[list[dict]] = None,
    ) -> ChatResponse:
        """Send a chat completion request with optional function calling.

        Args:
            messages: List of message dicts with 'role' and 'content'.
            system_prompt: Optional system message to prepend.
            temperature: Sampling temperature (0.0 = deterministic).
            max_tokens: Maximum tokens in response.
            tools: Optional list of tool definitions for function calling.

        Returns:
            ChatResponse with content, tool_calls, and usage.

        Raises:
            LLMError: If the API call fails.
        """
        self.reset_cancel()

        def make_request():
            url = self._get_url()
            headers = self._get_headers()

            full_messages = []
            if system_prompt:
                full_messages.append({"role": "system", "content": system_prompt})
            full_messages.extend(messages)

            model_lower = self.config.model.lower()
            uses_completion_tokens = any(x in model_lower for x in ("o1", "o3", "o4", "gpt-5"))
            no_tools_support = model_lower in ("o1-preview", "o1-mini", "o1")

            payload = {
                "messages": full_messages,
            }

            if uses_completion_tokens:
                payload["max_completion_tokens"] = max_tokens
            else:
                payload["temperature"] = temperature
                payload["max_tokens"] = max_tokens

            if tools and not no_tools_support:
                payload["tools"] = tools
                payload["tool_choice"] = "auto"

            if self.config.api_type != "azure":
                payload["model"] = self.config.model

            response = self._session.post(url, headers=headers, json=payload, timeout=180)

            if response.status_code == 429:
                raise LLMError(f"Rate limit exceeded: {response.text[:200]}")

            if response.status_code != 200:
                raise LLMError(f"LLM API error: {response.status_code} - {response.text[:500]}")

            result = response.json()
            message = result["choices"][0]["message"]

            usage_data = result.get("usage", {})
            usage = TokenUsage(
                prompt_tokens=usage_data.get("prompt_tokens", 0),
                completion_tokens=usage_data.get("completion_tokens", 0),
                total_tokens=usage_data.get("total_tokens", 0),
            )

            return ChatResponse(
                content=message.get("content"),
                tool_calls=message.get("tool_calls"),
                usage=usage,
                finish_reason=result["choices"][0].get("finish_reason", ""),
                model=result.get("model", self.config.model),
            )

        return self._retry_with_backoff(make_request)

    def chat_stream(
        self,
        messages: list[dict[str, str]],
        system_prompt: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: int = 16384,
    ) -> Generator[str, None, None]:
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
        self.reset_cancel()
        url = self._get_url()
        headers = self._get_headers()

        full_messages = []
        if system_prompt:
            full_messages.append({"role": "system", "content": system_prompt})
        full_messages.extend(messages)

        model_lower = self.config.model.lower()
        uses_completion_tokens = any(x in model_lower for x in ("o1", "o3", "o4", "gpt-5"))

        payload = {
            "messages": full_messages,
            "stream": True,
        }

        if uses_completion_tokens:
            payload["max_completion_tokens"] = max_tokens
        else:
            payload["temperature"] = temperature
            payload["max_tokens"] = max_tokens

        if self.config.api_type != "azure":
            payload["model"] = self.config.model

        try:
            response = self._session.post(
                url, headers=headers, json=payload, timeout=120, stream=True
            )

            if response.status_code != 200:
                raise LLMError(f"LLM API error: {response.status_code} - {response.text[:500]}")

            for line in response.iter_lines():
                if self.is_cancelled:
                    response.close()
                    return

                if line:
                    line = line.decode("utf-8")
                    if line.startswith("data: "):
                        data = line[6:]
                        if data == "[DONE]":
                            break
                        try:
                            chunk = json.loads(data)
                            choices = chunk.get("choices", [])
                            if choices:
                                delta = choices[0].get("delta", {})
                                content = delta.get("content", "")
                                if content:
                                    yield content
                        except (json.JSONDecodeError, IndexError, KeyError):
                            continue

        except requests.exceptions.Timeout:
            raise LLMError("LLM API request timed out")
        except requests.exceptions.RequestException as e:
            raise LLMError(f"LLM API connection error: {e}")

    def chat_stream_with_tools(
        self,
        messages: list[dict[str, str]],
        system_prompt: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: int = 16384,
        tools: Optional[list[dict]] = None,
        on_content: Optional[Callable[[str], None]] = None,
        on_tool_call_start: Optional[Callable[[str], None]] = None,
        on_thinking: Optional[Callable[[str], None]] = None,
    ) -> ChatResponse:
        """Stream a chat completion with tool support.

        This method streams content tokens for display while also handling
        tool calls. It provides callbacks for real-time UI updates.

        Args:
            messages: List of message dicts.
            system_prompt: Optional system message.
            temperature: Sampling temperature.
            max_tokens: Maximum tokens.
            tools: Tool definitions for function calling.
            on_content: Callback for each content chunk.
            on_tool_call_start: Callback when tool call detection starts.
            on_thinking: Callback for thinking/reasoning tokens.

        Returns:
            ChatResponse with full content and tool calls.
        """
        self.reset_cancel()
        url = self._get_url()
        headers = self._get_headers()

        full_messages = []
        if system_prompt:
            full_messages.append({"role": "system", "content": system_prompt})
        full_messages.extend(messages)

        model_lower = self.config.model.lower()
        uses_completion_tokens = any(x in model_lower for x in ("o1", "o3", "o4", "gpt-5"))
        no_tools_support = model_lower in ("o1-preview", "o1-mini", "o1")

        payload = {
            "messages": full_messages,
            "stream": True,
        }

        if uses_completion_tokens:
            payload["max_completion_tokens"] = max_tokens
        else:
            payload["temperature"] = temperature
            payload["max_tokens"] = max_tokens

        if tools and not no_tools_support:
            payload["tools"] = tools
            payload["tool_choice"] = "auto"

        if self.config.api_type != "azure":
            payload["model"] = self.config.model

        accumulated_content = ""
        tool_calls = []
        finish_reason = ""

        try:
            response = self._session.post(
                url, headers=headers, json=payload, timeout=180, stream=True
            )

            if response.status_code != 200:
                raise LLMError(f"LLM API error: {response.status_code} - {response.text[:500]}")

            for line in response.iter_lines():
                if self.is_cancelled:
                    response.close()
                    break

                if not line:
                    continue

                line = line.decode("utf-8")
                if not line.startswith("data: "):
                    continue

                data = line[6:]
                if data == "[DONE]":
                    break

                try:
                    chunk = json.loads(data)
                    choices = chunk.get("choices", [])
                    if not choices:
                        continue

                    choice = choices[0]
                    delta = choice.get("delta", {})
                    finish_reason = choice.get("finish_reason", "") or finish_reason

                    content = delta.get("content", "")
                    if content:
                        accumulated_content += content
                        if on_content:
                            on_content(content)

                    tool_call_deltas = delta.get("tool_calls", [])
                    for tc_delta in tool_call_deltas:
                        tc_index = tc_delta.get("index", 0)

                        while len(tool_calls) <= tc_index:
                            tool_calls.append(
                                {
                                    "id": "",
                                    "type": "function",
                                    "function": {"name": "", "arguments": ""},
                                }
                            )

                        tc = tool_calls[tc_index]

                        if "id" in tc_delta:
                            tc["id"] = tc_delta["id"]
                            if on_tool_call_start:
                                on_tool_call_start(tc_delta.get("function", {}).get("name", ""))

                        if "function" in tc_delta:
                            func = tc_delta["function"]
                            if "name" in func:
                                tc["function"]["name"] = func["name"]
                            if "arguments" in func:
                                tc["function"]["arguments"] += func["arguments"]

                except (json.JSONDecodeError, IndexError, KeyError):
                    continue

        except requests.exceptions.Timeout:
            raise LLMError("LLM API request timed out")
        except requests.exceptions.RequestException as e:
            raise LLMError(f"LLM API connection error: {e}")

        valid_tool_calls = [tc for tc in tool_calls if tc["id"] and tc["function"]["name"]]

        return ChatResponse(
            content=accumulated_content if accumulated_content else None,
            tool_calls=valid_tool_calls if valid_tool_calls else None,
            finish_reason=finish_reason,
            model=self.config.model,
        )


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

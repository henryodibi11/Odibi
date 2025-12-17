"""Tests for the enhanced LLM client.

Tests for:
- Token usage tracking
- Retry with exponential backoff
- Cancel support
- Streaming with tools
"""

import pytest
from unittest.mock import patch, MagicMock

from odibi.agents.ui.llm_client import (
    LLMConfig,
    LLMClient,
    LLMError,
    TokenUsage,
    ChatResponse,
    create_client,
)


class TestLLMConfig:
    """Tests for LLM configuration."""

    def test_default_values(self):
        """Test default configuration values."""
        config = LLMConfig()
        assert config.api_type == "openai"
        assert config.api_version == "2024-12-01-preview"

    def test_azure_auto_detection(self):
        """Test Azure is auto-detected from endpoint."""
        config = LLMConfig(
            endpoint="https://my-resource.openai.azure.com",
            api_key="test-key",
            model="gpt-4o",
        )
        assert config.api_type == "azure"

    def test_explicit_api_type(self):
        """Test explicit API type is preserved."""
        config = LLMConfig(
            endpoint="https://custom.endpoint.com",
            api_key="test-key",
            model="gpt-4o",
            api_type="openai",
        )
        assert config.api_type == "openai"


class TestTokenUsage:
    """Tests for token usage tracking."""

    def test_token_usage_properties(self):
        """Test TokenUsage properties."""
        usage = TokenUsage(
            prompt_tokens=100,
            completion_tokens=50,
            total_tokens=150,
        )
        assert usage.input_tokens == 100
        assert usage.output_tokens == 50
        assert usage.total_tokens == 150

    def test_token_usage_defaults(self):
        """Test TokenUsage default values."""
        usage = TokenUsage()
        assert usage.prompt_tokens == 0
        assert usage.completion_tokens == 0
        assert usage.total_tokens == 0


class TestChatResponse:
    """Tests for chat response dataclass."""

    def test_chat_response_with_content(self):
        """Test ChatResponse with content."""
        response = ChatResponse(
            content="Hello!",
            tool_calls=None,
            usage=TokenUsage(100, 50, 150),
        )
        assert response.content == "Hello!"
        assert response.tool_calls is None
        assert response.usage.total_tokens == 150

    def test_chat_response_with_tool_calls(self):
        """Test ChatResponse with tool calls."""
        response = ChatResponse(
            content=None,
            tool_calls=[{"id": "call_1", "function": {"name": "test"}}],
        )
        assert response.content is None
        assert len(response.tool_calls) == 1


class TestLLMClient:
    """Tests for LLM client."""

    def test_create_client(self):
        """Test client creation."""
        config = LLMConfig(
            endpoint="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o",
        )
        client = create_client(config)
        assert isinstance(client, LLMClient)
        assert client.config == config

    def test_get_headers_openai(self):
        """Test OpenAI headers."""
        config = LLMConfig(
            endpoint="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o",
            api_type="openai",
        )
        client = LLMClient(config)
        headers = client._get_headers()
        assert "Authorization" in headers
        assert headers["Authorization"] == "Bearer test-key"

    def test_get_headers_azure(self):
        """Test Azure headers."""
        config = LLMConfig(
            endpoint="https://my.openai.azure.com",
            api_key="test-key",
            model="gpt-4o",
            api_type="azure",
        )
        client = LLMClient(config)
        headers = client._get_headers()
        assert "api-key" in headers
        assert headers["api-key"] == "test-key"

    def test_get_url_openai(self):
        """Test OpenAI URL construction."""
        config = LLMConfig(
            endpoint="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o",
            api_type="openai",
        )
        client = LLMClient(config)
        url = client._get_url()
        assert "chat/completions" in url
        assert "api.openai.com" in url

    def test_get_url_azure(self):
        """Test Azure URL construction."""
        config = LLMConfig(
            endpoint="https://my.openai.azure.com",
            api_key="test-key",
            model="gpt-4o-deployment",
            api_type="azure",
            api_version="2024-12-01-preview",
        )
        client = LLMClient(config)
        url = client._get_url()
        assert "gpt-4o-deployment" in url
        assert "api-version" in url
        assert "chat/completions" in url

    def test_cancel_sets_flag(self):
        """Test cancel sets the cancel event."""
        config = LLMConfig(
            endpoint="https://api.openai.com/v1",
            api_key="test-key",
        )
        client = LLMClient(config)

        assert not client.is_cancelled
        client.cancel()
        assert client.is_cancelled

        client.reset_cancel()
        assert not client.is_cancelled

    @patch("agents.ui.llm_client.requests.Session")
    def test_retry_with_backoff(self, mock_session_class):
        """Test retry logic with exponential backoff."""
        import requests

        config = LLMConfig(
            endpoint="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o",
        )
        client = LLMClient(config)

        call_count = 0

        def side_effect():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise requests.exceptions.Timeout("Temporary error")
            return "success"

        result = client._retry_with_backoff(
            side_effect,
            max_retries=3,
            base_delay=0.01,
        )

        assert result == "success"
        assert call_count == 3

    @patch("agents.ui.llm_client.requests.Session")
    def test_retry_max_exceeded(self, mock_session_class):
        """Test retry raises after max retries."""
        import requests

        config = LLMConfig(
            endpoint="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o",
        )
        client = LLMClient(config)

        def always_fail():
            raise requests.exceptions.ConnectionError("Always fails")

        with pytest.raises(LLMError) as exc_info:
            client._retry_with_backoff(
                always_fail,
                max_retries=2,
                base_delay=0.01,
            )

        assert "Max retries exceeded" in str(exc_info.value)


class TestLLMClientIntegration:
    """Integration-style tests for LLM client (mocked HTTP)."""

    @patch("agents.ui.llm_client.requests.Session")
    def test_chat_success(self, mock_session_class):
        """Test successful chat completion."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": "Hello!",
                        "tool_calls": None,
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15,
            },
            "model": "gpt-4o",
        }
        mock_session.post.return_value = mock_response

        config = LLMConfig(
            endpoint="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o",
        )
        client = LLMClient(config)

        result = client.chat(
            messages=[{"role": "user", "content": "Hi"}],
        )

        assert result["content"] == "Hello!"
        assert result["usage"].total_tokens == 15

    @patch("agents.ui.llm_client.requests.Session")
    def test_chat_with_tool_calls(self, mock_session_class):
        """Test chat completion with tool calls."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": None,
                        "tool_calls": [
                            {
                                "id": "call_123",
                                "type": "function",
                                "function": {
                                    "name": "read_file",
                                    "arguments": '{"path": "/test.py"}',
                                },
                            }
                        ],
                    },
                    "finish_reason": "tool_calls",
                }
            ],
            "usage": {
                "prompt_tokens": 20,
                "completion_tokens": 10,
                "total_tokens": 30,
            },
        }
        mock_session.post.return_value = mock_response

        config = LLMConfig(
            endpoint="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o",
        )
        client = LLMClient(config)

        result = client.chat(
            messages=[{"role": "user", "content": "Read test.py"}],
            tools=[{"type": "function", "function": {"name": "read_file"}}],
        )

        assert result["content"] is None
        assert len(result["tool_calls"]) == 1
        assert result["tool_calls"][0]["function"]["name"] == "read_file"

    @patch("agents.ui.llm_client.requests.Session")
    def test_chat_error_handling(self, mock_session_class):
        """Test chat error handling."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_session.post.return_value = mock_response

        config = LLMConfig(
            endpoint="https://api.openai.com/v1",
            api_key="test-key",
            model="gpt-4o",
        )
        client = LLMClient(config)

        with pytest.raises(LLMError) as exc_info:
            client.chat(
                messages=[{"role": "user", "content": "Hi"}],
            )

        assert "500" in str(exc_info.value)

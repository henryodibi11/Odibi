"""Unit tests for HttpConnection."""

import base64

import pytest

from odibi.connections.http import HttpConnection


class TestHttpConnectionInit:
    """Tests for HttpConnection initialization."""

    def test_init_basic(self):
        """Should create connection with base URL."""
        conn = HttpConnection(base_url="https://api.example.com", validate=False)
        assert conn.base_url == "https://api.example.com/"

    def test_init_adds_trailing_slash(self):
        """Should add trailing slash to base URL."""
        conn = HttpConnection(base_url="https://api.example.com", validate=False)
        assert conn.base_url.endswith("/")

    def test_init_preserves_trailing_slash(self):
        """Should not duplicate trailing slash."""
        conn = HttpConnection(base_url="https://api.example.com/", validate=False)
        assert conn.base_url == "https://api.example.com/"

    def test_init_empty_headers_default(self):
        """Should default to empty headers."""
        conn = HttpConnection(base_url="https://api.example.com", validate=False)
        assert conn.headers == {}

    def test_init_custom_headers(self):
        """Should accept custom headers."""
        headers = {"X-Custom": "value", "Accept": "application/json"}
        conn = HttpConnection(
            base_url="https://api.example.com",
            headers=headers,
            validate=False,
        )
        assert conn.headers["X-Custom"] == "value"
        assert conn.headers["Accept"] == "application/json"


class TestHttpConnectionAuth:
    """Tests for HttpConnection authentication."""

    def test_auth_bearer_token(self):
        """Should set Bearer token authorization."""
        conn = HttpConnection(
            base_url="https://api.example.com",
            auth={"token": "my-secret-token"},
            validate=False,
        )
        assert conn.headers["Authorization"] == "Bearer my-secret-token"

    def test_auth_basic(self):
        """Should set Basic authorization."""
        conn = HttpConnection(
            base_url="https://api.example.com",
            auth={"username": "user", "password": "pass"},
            validate=False,
        )

        expected_creds = base64.b64encode(b"user:pass").decode()
        assert conn.headers["Authorization"] == f"Basic {expected_creds}"

    def test_auth_api_key_default_header(self):
        """Should set API key with default header name."""
        conn = HttpConnection(
            base_url="https://api.example.com",
            auth={"api_key": "secret-key"},
            validate=False,
        )
        assert conn.headers["X-API-Key"] == "secret-key"

    def test_auth_api_key_custom_header(self):
        """Should set API key with custom header name."""
        conn = HttpConnection(
            base_url="https://api.example.com",
            auth={"api_key": "secret-key", "header_name": "X-Custom-Auth"},
            validate=False,
        )
        assert conn.headers["X-Custom-Auth"] == "secret-key"
        assert "X-API-Key" not in conn.headers

    def test_auth_preserves_existing_headers(self):
        """Should preserve existing headers when adding auth."""
        conn = HttpConnection(
            base_url="https://api.example.com",
            headers={"Accept": "application/json"},
            auth={"token": "my-token"},
            validate=False,
        )
        assert conn.headers["Accept"] == "application/json"
        assert conn.headers["Authorization"] == "Bearer my-token"


class TestHttpConnectionValidate:
    """Tests for HttpConnection validate method."""

    def test_validate_success(self):
        """Should pass validation with valid base_url."""
        conn = HttpConnection(base_url="https://api.example.com", validate=False)
        conn.validate()

    def test_validate_fails_empty_url(self):
        """Should fail validation with empty base_url."""
        conn = HttpConnection.__new__(HttpConnection)
        conn.base_url = ""
        conn.headers = {}

        with pytest.raises(ValueError, match="base_url"):
            conn.validate()


class TestHttpConnectionGetPath:
    """Tests for HttpConnection get_path method."""

    def test_get_path_simple(self):
        """Should join base URL with path."""
        conn = HttpConnection(base_url="https://api.example.com", validate=False)
        result = conn.get_path("v1/users")

        assert result == "https://api.example.com/v1/users"

    def test_get_path_strips_leading_slash(self):
        """Should handle leading slash in path."""
        conn = HttpConnection(base_url="https://api.example.com", validate=False)
        result = conn.get_path("/v1/users")

        assert result == "https://api.example.com/v1/users"

    def test_get_path_absolute_url_passthrough(self):
        """Should pass through absolute URLs unchanged."""
        conn = HttpConnection(base_url="https://api.example.com", validate=False)

        result_http = conn.get_path("http://other.example.com/data")
        result_https = conn.get_path("https://other.example.com/data")

        assert result_http == "http://other.example.com/data"
        assert result_https == "https://other.example.com/data"

    def test_get_path_with_query_params(self):
        """Should preserve query parameters."""
        conn = HttpConnection(base_url="https://api.example.com", validate=False)
        result = conn.get_path("v1/users?page=1&limit=10")

        assert result == "https://api.example.com/v1/users?page=1&limit=10"


class TestHttpConnectionStorageOptions:
    """Tests for HttpConnection pandas_storage_options method."""

    def test_storage_options_returns_headers(self):
        """Should return headers as storage options."""
        headers = {"Authorization": "Bearer token", "Accept": "application/json"}
        conn = HttpConnection(
            base_url="https://api.example.com",
            headers=headers,
            validate=False,
        )

        options = conn.pandas_storage_options()

        assert options == headers

    def test_storage_options_includes_auth_headers(self):
        """Should include auth headers in storage options."""
        conn = HttpConnection(
            base_url="https://api.example.com",
            auth={"token": "my-token"},
            validate=False,
        )

        options = conn.pandas_storage_options()

        assert "Authorization" in options
        assert options["Authorization"] == "Bearer my-token"

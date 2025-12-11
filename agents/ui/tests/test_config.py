"""Tests for UI configuration management."""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    AgentUIConfig,
    LLMConfig,
    MemoryConfig,
    ProjectConfig,
    get_config_path,
    load_config,
    load_odibi_connections,
    save_config,
)


class TestLLMConfig:
    """Tests for LLMConfig."""

    def test_default_values(self):
        """Test default LLM config values."""
        config = LLMConfig()
        assert config.model == "gpt-4o"
        assert config.api_type == "openai"

    def test_custom_values(self):
        """Test custom LLM config values."""
        config = LLMConfig(
            endpoint="https://api.openai.com/v1",
            model="gpt-4o-mini",
            api_key="test-key",
        )
        assert config.endpoint == "https://api.openai.com/v1"
        assert config.model == "gpt-4o-mini"
        assert config.api_key == "test-key"

    def test_env_fallback(self, monkeypatch):
        """Test environment variable fallback."""
        monkeypatch.setenv("LLM_ENDPOINT", "https://env.openai.com/v1")
        monkeypatch.setenv("LLM_API_KEY", "env-key")

        config = LLMConfig()
        assert config.endpoint == "https://env.openai.com/v1"
        assert config.api_key == "env-key"

    def test_azure_auto_detection(self):
        """Test automatic Azure API type detection."""
        config = LLMConfig(endpoint="https://myresource.openai.azure.com")
        assert config.api_type == "azure"


class TestMemoryConfig:
    """Tests for MemoryConfig."""

    def test_default_values(self):
        """Test default memory config values."""
        config = MemoryConfig()
        assert config.backend_type == "local"
        assert config.connection_name is None
        assert config.path_prefix == "agent/memories"

    def test_odibi_backend(self):
        """Test odibi backend config."""
        config = MemoryConfig(
            backend_type="odibi",
            connection_name="adls_memories",
            path_prefix="custom/path",
        )
        assert config.backend_type == "odibi"
        assert config.connection_name == "adls_memories"


class TestAgentUIConfig:
    """Tests for AgentUIConfig."""

    def test_default_config(self):
        """Test default UI config."""
        config = AgentUIConfig()
        assert isinstance(config.llm, LLMConfig)
        assert isinstance(config.memory, MemoryConfig)
        assert isinstance(config.project, ProjectConfig)
        assert config.default_agent == "auto"

    def test_to_dict(self):
        """Test config serialization to dict."""
        config = AgentUIConfig(
            llm=LLMConfig(model="gpt-4o"),
            default_agent="code_analyst",
        )
        data = config.to_dict()

        assert data["llm"]["model"] == "gpt-4o"
        assert data["default_agent"] == "code_analyst"
        assert "memory" in data
        assert "project" in data

    def test_from_dict(self):
        """Test config deserialization from dict."""
        data = {
            "llm": {
                "endpoint": "https://api.openai.com/v1",
                "model": "gpt-3.5-turbo",
            },
            "memory": {
                "backend_type": "delta",
            },
            "default_agent": "test_architect",
        }
        config = AgentUIConfig.from_dict(data)

        assert config.llm.endpoint == "https://api.openai.com/v1"
        assert config.llm.model == "gpt-3.5-turbo"
        assert config.memory.backend_type == "delta"
        assert config.default_agent == "test_architect"

    def test_roundtrip(self):
        """Test config roundtrip (to_dict -> from_dict)."""
        original = AgentUIConfig(
            llm=LLMConfig(
                endpoint="https://api.openai.com/v1",
                model="gpt-4o-mini",
            ),
            memory=MemoryConfig(
                backend_type="odibi",
                connection_name="test_conn",
            ),
            default_agent="documentation",
        )

        data = original.to_dict()
        restored = AgentUIConfig.from_dict(data)

        assert restored.llm.endpoint == original.llm.endpoint
        assert restored.llm.model == original.llm.model
        assert restored.memory.backend_type == original.memory.backend_type
        assert restored.memory.connection_name == original.memory.connection_name
        assert restored.default_agent == original.default_agent


class TestConfigPersistence:
    """Tests for config save/load."""

    def test_get_config_path(self):
        """Test config path generation."""
        path = get_config_path("d:/test/project")
        assert path == Path("d:/test/project/.odibi/agent_config.yaml")

    def test_load_missing_config(self):
        """Test loading config when file doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = load_config(tmpdir)
            assert isinstance(config, AgentUIConfig)
            assert config.project.odibi_root == tmpdir

    def test_save_and_load_config(self):
        """Test saving and loading config."""
        with tempfile.TemporaryDirectory() as tmpdir:
            original = AgentUIConfig(
                llm=LLMConfig(model="gpt-4o"),
                project=ProjectConfig(odibi_root=tmpdir),
            )

            success = save_config(original)
            assert success

            config_path = get_config_path(tmpdir)
            assert config_path.exists()

            loaded = load_config(tmpdir)
            assert loaded.llm.model == "gpt-4o"

    def test_api_key_masked_in_file(self):
        """Test that API key is masked when saved."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = AgentUIConfig(
                llm=LLMConfig(api_key="super-secret-key"),
                project=ProjectConfig(odibi_root=tmpdir),
            )

            save_config(config)

            config_path = get_config_path(tmpdir)
            content = config_path.read_text()
            assert "super-secret-key" not in content
            assert "***" in content


class TestLoadOdibiConnections:
    """Tests for loading Odibi connections."""

    def test_missing_file(self):
        """Test loading from non-existent file."""
        connections = load_odibi_connections("/nonexistent/project.yaml")
        assert connections == []

    def test_empty_connections(self):
        """Test loading from file with no connections."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "project.yaml"
            path.write_text("name: test-project\n")

            connections = load_odibi_connections(str(path))
            assert connections == []

    def test_valid_connections(self):
        """Test loading valid connections."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "project.yaml"
            path.write_text(
                """
name: test-project
connections:
  local:
    type: local
    base_path: ./data
  adls:
    type: azure_blob
    account_name: myaccount
  delta:
    type: delta
    path: /mnt/delta
"""
            )

            connections = load_odibi_connections(str(path))
            assert "local" in connections
            assert "adls" in connections
            assert "delta" in connections

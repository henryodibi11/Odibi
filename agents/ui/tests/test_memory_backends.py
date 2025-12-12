"""Tests for memory backend integration with chat tools."""

import pytest
from unittest.mock import MagicMock
import pandas as pd

from agents.core.memory import MemoryManager, MemoryType
from agents.core.memory_backends import (
    LocalFileBackend,
    OdibiConnectionBackend,
)


class TestLocalFileBackend:
    """Test local file backend."""

    def test_save_and_load(self, tmp_path):
        """Test saving and loading a memory."""
        backend = LocalFileBackend(base_path=str(tmp_path / "memories"))

        data = {
            "id": "test-123",
            "memory_type": "decision",
            "summary": "Use SQLAlchemy",
            "content": "We decided to use SQLAlchemy for the database layer.",
            "tags": ["database", "orm"],
            "created_at": "2024-01-01T00:00:00",
            "importance": 0.8,
        }

        assert backend.save("test-123", data)
        loaded = backend.load("test-123")

        assert loaded is not None
        assert loaded["summary"] == "Use SQLAlchemy"
        assert loaded["memory_type"] == "decision"

    def test_search(self, tmp_path):
        """Test searching memories."""
        backend = LocalFileBackend(base_path=str(tmp_path / "memories"))

        backend.save(
            "mem-1",
            {
                "id": "mem-1",
                "summary": "Database choice",
                "content": "Using SQLAlchemy for ORM",
                "tags": ["database"],
                "memory_type": "decision",
            },
        )
        backend.save(
            "mem-2",
            {
                "id": "mem-2",
                "summary": "API framework",
                "content": "Using FastAPI for REST",
                "tags": ["api"],
                "memory_type": "decision",
            },
        )

        results = backend.search("database")
        assert len(results) == 1
        assert results[0]["summary"] == "Database choice"


class TestOdibiConnectionBackend:
    """Test ADLS/Odibi connection backend."""

    def test_save_calls_engine_write(self):
        """Verify save() calls engine.write() with correct params."""
        mock_connection = MagicMock()
        mock_connection.name = "adls_test"

        mock_engine = MagicMock()
        mock_engine.read.return_value = pd.DataFrame()  # Empty index

        backend = OdibiConnectionBackend(
            connection=mock_connection,
            engine=mock_engine,
            path_prefix="agent/memories",
        )

        data = {
            "id": "test-456",
            "memory_type": "learning",
            "summary": "Test learning",
            "content": "This is a test",
            "created_at": "2024-01-01T00:00:00",
        }

        result = backend.save("test-456", data)

        assert result is True
        assert mock_engine.write.call_count >= 1

        # Check the memory file was written
        write_calls = mock_engine.write.call_args_list
        memory_write = write_calls[0]

        assert memory_write.kwargs["connection"] == mock_connection
        assert memory_write.kwargs["format"] == "json"
        assert "agent/memories/test-456.json" in memory_write.kwargs["path"]

    def test_load_calls_engine_read(self):
        """Verify load() calls engine.read() correctly."""
        mock_connection = MagicMock()
        mock_engine = MagicMock()

        test_data = {
            "id": "test-789",
            "memory_type": "decision",
            "summary": "Test decision",
            "content": "Content here",
        }
        mock_engine.read.return_value = pd.DataFrame([test_data])

        backend = OdibiConnectionBackend(
            connection=mock_connection,
            engine=mock_engine,
            path_prefix="memories",
        )

        result = backend.load("test-789")

        assert result is not None
        assert result["summary"] == "Test decision"
        mock_engine.read.assert_called()


class TestMemoryManagerIntegration:
    """Test MemoryManager with different backends."""

    def test_remember_uses_backend(self, tmp_path):
        """Test that remember() stores to the configured backend."""
        manager = MemoryManager(
            backend_type="local",
            local_path=str(tmp_path / "memories"),
        )

        memory = manager.remember(
            memory_type=MemoryType.DECISION,
            summary="Use pytest for testing",
            content="We decided to use pytest as our testing framework.",
            tags=["testing", "pytest"],
            importance=0.8,
        )

        assert memory.summary == "Use pytest for testing"

        # Verify it was actually saved
        recalled = manager.recall("pytest")
        assert len(recalled) == 1
        assert recalled[0].summary == "Use pytest for testing"

    def test_odibi_backend_integration(self):
        """Test MemoryManager with mocked Odibi backend."""
        mock_connection = MagicMock()
        mock_engine = MagicMock()
        mock_engine.read.return_value = pd.DataFrame()

        manager = MemoryManager(
            backend_type="odibi",
            connection=mock_connection,
            engine=mock_engine,
            path_prefix="agent/memories",
        )

        manager.remember(
            memory_type=MemoryType.LEARNING,
            summary="ADLS works!",
            content="Verified that memories are saved to ADLS.",
            importance=0.9,
        )

        # Verify engine.write was called (meaning it tried to save to ADLS)
        assert mock_engine.write.call_count >= 1


class TestChatMemoryTools:
    """Test the chat's remember/recall tool integration."""

    def test_remember_tool_saves_to_backend(self, tmp_path):
        """Simulate what happens when chat calls remember tool."""
        # Directly use MemoryManager like chat.py does (avoids gradio import)
        manager = MemoryManager(
            backend_type="local",
            local_path=str(tmp_path / ".odibi" / "memories"),
        )

        # Simulate tool call (same as chat.py execute_tool for "remember")
        manager.remember(
            memory_type=MemoryType.DECISION,
            summary="Test from chat",
            content="Memory saved via chat tool",
            tags=["test", "chat"],
            importance=0.7,
        )

        # Verify it persisted
        memories = manager.store.get_recent(days=1)
        assert len(memories) >= 1
        assert any(m.summary == "Test from chat" for m in memories)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

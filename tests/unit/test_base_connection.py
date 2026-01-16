"""Unit tests for BaseConnection abstract class."""

import pytest

from odibi.connections.base import BaseConnection


class TestBaseConnection:
    """Tests for BaseConnection abstract class."""

    def test_cannot_instantiate_directly(self):
        """Should not allow direct instantiation of BaseConnection."""
        with pytest.raises(TypeError):
            BaseConnection()

    def test_subclass_must_implement_get_path(self):
        """Should require subclass to implement get_path."""

        class IncompleteConnection(BaseConnection):
            def validate(self):
                pass

        with pytest.raises(TypeError):
            IncompleteConnection()

    def test_subclass_must_implement_validate(self):
        """Should require subclass to implement validate."""

        class IncompleteConnection(BaseConnection):
            def get_path(self, relative_path):
                return relative_path

        with pytest.raises(TypeError):
            IncompleteConnection()

    def test_complete_subclass_can_be_instantiated(self):
        """Should allow instantiation of complete subclass."""

        class CompleteConnection(BaseConnection):
            def get_path(self, relative_path):
                return f"/base/{relative_path}"

            def validate(self):
                pass

        conn = CompleteConnection()
        assert conn.get_path("test") == "/base/test"
        conn.validate()

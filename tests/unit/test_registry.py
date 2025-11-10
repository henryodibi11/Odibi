"""Tests for transformation registry."""

import pytest
from odibi.transformations import transformation, get_registry


class TestTransformationRegistry:
    """Tests for TransformationRegistry class."""

    def setup_method(self):
        """Clear registry before each test."""
        get_registry().clear()

    def test_register_transformation(self):
        """Should register transformation successfully."""

        @transformation("test_transform")
        def test_transform(df):
            """Test transformation."""
            return df

        registry = get_registry()
        assert registry.get("test_transform") == test_transform

    def test_register_requires_docstring(self):
        """Should reject transformations without docstring."""
        with pytest.raises(ValueError, match="must have a docstring"):

            @transformation("no_docstring")
            def no_docstring(df):
                return df

    def test_register_duplicate_name_fails(self):
        """Should reject duplicate transformation names."""

        @transformation("duplicate")
        def first(df):
            """First function."""
            return df

        with pytest.raises(ValueError, match="already registered"):

            @transformation("duplicate")
            def second(df):
                """Second function."""
                return df

    def test_get_nonexistent_returns_none(self):
        """Should return None for unregistered transformation."""
        registry = get_registry()
        assert registry.get("nonexistent") is None

    def test_metadata_stored(self):
        """Should store metadata with transformation."""

        @transformation("with_metadata", version="2.0.0", category="test", tags=["example"])
        def with_metadata(df):
            """Test function with metadata."""
            return df

        registry = get_registry()
        metadata = registry.get_metadata("with_metadata")

        assert metadata["version"] == "2.0.0"
        assert metadata["category"] == "test"
        assert "example" in metadata["tags"]
        assert "Test function" in metadata["docstring"]

    def test_list_all_transformations(self):
        """Should list all registered transformations."""

        @transformation("transform1")
        def transform1(df):
            """First transformation."""
            return df

        @transformation("transform2")
        def transform2(df):
            """Second transformation."""
            return df

        registry = get_registry()
        all_transforms = registry.list_all()

        assert len(all_transforms) == 2
        assert "transform1" in all_transforms
        assert "transform2" in all_transforms

    def test_unregister_transformation(self):
        """Should unregister transformation."""

        @transformation("to_unregister")
        def to_unregister(df):
            """Will be unregistered."""
            return df

        registry = get_registry()
        assert registry.get("to_unregister") is not None

        success = registry.unregister("to_unregister")
        assert success is True
        assert registry.get("to_unregister") is None

    def test_unregister_nonexistent_returns_false(self):
        """Should return False when unregistering nonexistent transformation."""
        registry = get_registry()
        success = registry.unregister("nonexistent")
        assert success is False

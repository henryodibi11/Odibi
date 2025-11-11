"""Tests for story theme system."""

import pytest

from odibi.story.themes import (
    StoryTheme,
    get_theme,
    list_themes,
    DEFAULT_THEME,
    CORPORATE_THEME,
    DARK_THEME,
    MINIMAL_THEME,
)


class TestStoryTheme:
    """Tests for StoryTheme class."""

    def test_create_default_theme(self):
        """Should create theme with default values."""
        theme = StoryTheme(name="test")

        assert theme.name == "test"
        assert theme.primary_color == "#0066cc"
        assert theme.success_color == "#28a745"
        assert theme.error_color == "#dc3545"

    def test_create_custom_theme(self):
        """Should create theme with custom values."""
        theme = StoryTheme(
            name="custom",
            primary_color="#ff0000",
            font_family="Comic Sans MS",
            logo_url="https://example.com/logo.png",
        )

        assert theme.name == "custom"
        assert theme.primary_color == "#ff0000"
        assert theme.font_family == "Comic Sans MS"
        assert theme.logo_url == "https://example.com/logo.png"

    def test_to_css_vars(self):
        """Should convert theme to CSS variables."""
        theme = StoryTheme(
            name="test",
            primary_color="#123456",
            font_family="Arial",
        )

        css_vars = theme.to_css_vars()

        assert css_vars["--primary-color"] == "#123456"
        assert css_vars["--font-family"] == "Arial"
        assert "--success-color" in css_vars
        assert "--error-color" in css_vars

    def test_to_css_string(self):
        """Should generate CSS string."""
        theme = StoryTheme(
            name="test",
            primary_color="#000000",
        )

        css = theme.to_css_string()

        assert ":root {" in css
        assert "--primary-color: #000000;" in css
        assert "}" in css

    def test_to_css_string_with_custom_css(self):
        """Should include custom CSS."""
        theme = StoryTheme(name="test", custom_css="body { background: red; }")

        css = theme.to_css_string()

        assert "body { background: red; }" in css

    def test_from_dict(self):
        """Should create theme from dictionary."""
        data = {
            "name": "from_dict",
            "primary_color": "#abcdef",
            "font_family": "Georgia",
        }

        theme = StoryTheme.from_dict(data)

        assert theme.name == "from_dict"
        assert theme.primary_color == "#abcdef"
        assert theme.font_family == "Georgia"

    def test_from_yaml(self, tmp_path):
        """Should load theme from YAML file."""
        theme_data = """
name: yaml_theme
primary_color: "#112233"
success_color: "#00ff00"
font_family: Verdana
company_name: Test Company
        """

        theme_file = tmp_path / "theme.yaml"
        theme_file.write_text(theme_data)

        theme = StoryTheme.from_yaml(str(theme_file))

        assert theme.name == "yaml_theme"
        assert theme.primary_color == "#112233"
        assert theme.success_color == "#00ff00"
        assert theme.company_name == "Test Company"


class TestBuiltInThemes:
    """Tests for built-in themes."""

    def test_default_theme(self):
        """Should have default theme."""
        assert DEFAULT_THEME.name == "default"
        assert DEFAULT_THEME.primary_color == "#0066cc"

    def test_corporate_theme(self):
        """Should have corporate theme."""
        assert CORPORATE_THEME.name == "corporate"
        assert CORPORATE_THEME.primary_color == "#003366"
        assert "Georgia" in CORPORATE_THEME.heading_font

    def test_dark_theme(self):
        """Should have dark theme."""
        assert DARK_THEME.name == "dark"
        assert DARK_THEME.bg_color == "#1e1e1e"
        assert DARK_THEME.text_color == "#e0e0e0"
        assert DARK_THEME.custom_css is not None

    def test_minimal_theme(self):
        """Should have minimal theme."""
        assert MINIMAL_THEME.name == "minimal"
        assert MINIMAL_THEME.primary_color == "#000000"
        assert MINIMAL_THEME.max_width == "900px"


class TestGetTheme:
    """Tests for get_theme function."""

    def test_get_builtin_theme(self):
        """Should get built-in theme by name."""
        theme = get_theme("default")

        assert theme.name == "default"
        assert isinstance(theme, StoryTheme)

    def test_get_builtin_theme_case_insensitive(self):
        """Should be case-insensitive."""
        theme = get_theme("CORPORATE")

        assert theme.name == "corporate"

    def test_get_theme_from_file(self, tmp_path):
        """Should load theme from file path."""
        theme_data = """
name: file_theme
primary_color: "#ffffff"
        """

        theme_file = tmp_path / "custom.yaml"
        theme_file.write_text(theme_data)

        theme = get_theme(str(theme_file))

        assert theme.name == "file_theme"
        assert theme.primary_color == "#ffffff"

    def test_get_nonexistent_theme(self):
        """Should raise error for nonexistent theme."""
        with pytest.raises(ValueError, match="Theme 'nonexistent' not found"):
            get_theme("nonexistent")


class TestListThemes:
    """Tests for list_themes function."""

    def test_list_all_themes(self):
        """Should list all built-in themes."""
        themes = list_themes()

        assert "default" in themes
        assert "corporate" in themes
        assert "dark" in themes
        assert "minimal" in themes
        assert len(themes) >= 4

    def test_list_themes_returns_copy(self):
        """Should return a copy, not the original dict."""
        themes1 = list_themes()
        themes2 = list_themes()

        # Modify one
        themes1["test"] = StoryTheme(name="test")

        # Other should be unchanged
        assert "test" not in themes2

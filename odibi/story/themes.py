"""
Theme System
============

Customizable themes for story rendering with branding support.
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass
import yaml
from pathlib import Path


@dataclass
class StoryTheme:
    """
    Story theme configuration.

    Defines colors, typography, branding, and layout options for
    rendered stories (HTML).
    """

    name: str

    # Colors
    primary_color: str = "#0066cc"
    success_color: str = "#28a745"
    error_color: str = "#dc3545"
    warning_color: str = "#ffc107"
    bg_color: str = "#ffffff"
    text_color: str = "#333333"
    border_color: str = "#dddddd"
    code_bg: str = "#f5f5f5"

    # Typography
    font_family: str = (
        "system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"
    )
    heading_font: str = "inherit"
    code_font: str = "Consolas, Monaco, 'Courier New', monospace"
    font_size: str = "16px"

    # Branding
    logo_url: Optional[str] = None
    company_name: Optional[str] = None
    footer_text: Optional[str] = None

    # Layout
    max_width: str = "1200px"
    sidebar: bool = False

    # Custom CSS
    custom_css: Optional[str] = None

    def to_css_vars(self) -> Dict[str, str]:
        """
        Convert theme to CSS variables.

        Returns:
            Dictionary of CSS variable names and values
        """
        return {
            "--primary-color": self.primary_color,
            "--success-color": self.success_color,
            "--error-color": self.error_color,
            "--warning-color": self.warning_color,
            "--bg-color": self.bg_color,
            "--text-color": self.text_color,
            "--border-color": self.border_color,
            "--code-bg": self.code_bg,
            "--font-family": self.font_family,
            "--heading-font": self.heading_font,
            "--code-font": self.code_font,
            "--font-size": self.font_size,
            "--max-width": self.max_width,
        }

    def to_css_string(self) -> str:
        """
        Generate CSS string from theme.

        Returns:
            CSS string with :root variables
        """
        lines = [":root {"]
        for var_name, var_value in self.to_css_vars().items():
            lines.append(f"    {var_name}: {var_value};")
        lines.append("}")

        if self.custom_css:
            lines.append("")
            lines.append(self.custom_css)

        return "\n".join(lines)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StoryTheme":
        """
        Create theme from dictionary.

        Args:
            data: Theme configuration dictionary

        Returns:
            StoryTheme instance
        """
        return cls(**data)

    @classmethod
    def from_yaml(cls, path: str) -> "StoryTheme":
        """
        Load theme from YAML file.

        Args:
            path: Path to YAML theme file

        Returns:
            StoryTheme instance
        """
        with open(path, "r") as f:
            data = yaml.safe_load(f)

        return cls.from_dict(data)


# Built-in Themes
# ===============

DEFAULT_THEME = StoryTheme(
    name="default",
)

CORPORATE_THEME = StoryTheme(
    name="corporate",
    primary_color="#003366",
    success_color="#2e7d32",
    error_color="#c62828",
    font_family="Arial, Helvetica, sans-serif",
    heading_font="Georgia, 'Times New Roman', serif",
    font_size="15px",
)

DARK_THEME = StoryTheme(
    name="dark",
    primary_color="#00bfff",
    success_color="#4caf50",
    error_color="#f44336",
    warning_color="#ffb300",
    bg_color="#1e1e1e",
    text_color="#e0e0e0",
    border_color="#444444",
    code_bg="#2d2d2d",
    custom_css="""
    body { background: #121212; }
    .container { background: #1e1e1e; }
    .node-header { background: #2d2d2d; }
    .summary { background: #2d2d2d; }
    """,
)

MINIMAL_THEME = StoryTheme(
    name="minimal",
    primary_color="#000000",
    success_color="#006600",
    error_color="#cc0000",
    warning_color="#ff9900",
    font_family="'Helvetica Neue', Helvetica, Arial, sans-serif",
    heading_font="'Helvetica Neue', Helvetica, Arial, sans-serif",
    font_size="14px",
    max_width="900px",
)

# Theme registry
BUILTIN_THEMES = {
    "default": DEFAULT_THEME,
    "corporate": CORPORATE_THEME,
    "dark": DARK_THEME,
    "minimal": MINIMAL_THEME,
}


def get_theme(name: str) -> StoryTheme:
    """
    Get theme by name.

    Args:
        name: Theme name or path to YAML theme file

    Returns:
        StoryTheme instance

    Raises:
        ValueError: If theme not found
    """
    # Check if it's a file path
    if Path(name).exists():
        return StoryTheme.from_yaml(name)

    # Check built-in themes
    if name.lower() in BUILTIN_THEMES:
        return BUILTIN_THEMES[name.lower()]

    raise ValueError(
        f"Theme '{name}' not found. " f"Available themes: {', '.join(BUILTIN_THEMES.keys())}"
    )


def list_themes() -> Dict[str, StoryTheme]:
    """
    List all available built-in themes.

    Returns:
        Dictionary of theme name -> StoryTheme
    """
    return BUILTIN_THEMES.copy()

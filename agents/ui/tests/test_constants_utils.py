"""Tests for constants and utilities that don't require Gradio.

Tests for:
- Token/cost estimation
- Conversation branching
- File link utilities
- Diff generation
- Elapsed time formatting
"""

from agents.ui.constants import (
    get_tool_emoji,
    get_memory_emoji,
    TOOL_EMOJI,
    MEMORY_TYPE_EMOJI,
    estimate_cost,
)
from agents.ui.utils import (
    create_file_link,
    create_diff,
    format_diff_markdown,
    format_token_usage,
    format_collapsible,
    truncate_for_display,
    ConversationBranch,
    format_elapsed_time,
)


class TestConstants:
    """Tests for constants module."""

    def test_get_tool_emoji_known_tool(self):
        """Test getting emoji for known tools."""
        assert get_tool_emoji("read_file") == "📖"
        assert get_tool_emoji("write_file") == "✏️"
        assert get_tool_emoji("grep") == "🔍"
        assert get_tool_emoji("pytest") == "🧪"

    def test_get_tool_emoji_unknown_tool(self):
        """Test fallback for unknown tools."""
        assert get_tool_emoji("unknown_tool") == "🔧"

    def test_get_memory_emoji_known_type(self):
        """Test getting emoji for known memory types."""
        assert get_memory_emoji("decision") == "🔵"
        assert get_memory_emoji("learning") == "🟢"
        assert get_memory_emoji("bug_fix") == "🔴"

    def test_get_memory_emoji_unknown_type(self):
        """Test fallback for unknown memory types."""
        assert get_memory_emoji("unknown_type") == "📝"

    def test_estimate_cost_known_model(self):
        """Test cost estimation for known models."""
        cost = estimate_cost("gpt-4o", 1000, 500)
        assert cost > 0
        assert isinstance(cost, float)

    def test_estimate_cost_unknown_model(self):
        """Test cost estimation uses default for unknown models."""
        cost = estimate_cost("unknown-model", 1000, 500)
        assert cost > 0

    def test_all_tool_emojis_exist(self):
        """Test that all tool emojis are valid strings."""
        for tool, emoji in TOOL_EMOJI.items():
            assert isinstance(emoji, str)
            assert len(emoji) > 0

    def test_all_memory_emojis_exist(self):
        """Test that all memory type emojis are valid strings."""
        for mem_type, emoji in MEMORY_TYPE_EMOJI.items():
            assert isinstance(emoji, str)
            assert len(emoji) > 0


class TestUtils:
    """Tests for utility functions."""

    def test_create_file_link_basic(self):
        """Test basic file link creation."""
        link = create_file_link("/path/to/file.py")
        assert "file.py" in link
        assert "file://" in link

    def test_create_file_link_with_line(self):
        """Test file link with line number."""
        link = create_file_link("/path/to/file.py", line=42)
        assert "#L42" in link
        assert ":42" in link

    def test_create_file_link_with_range(self):
        """Test file link with line range."""
        link = create_file_link("/path/to/file.py", line=10, end_line=20)
        assert "#L10-L20" in link

    def test_create_diff(self):
        """Test diff creation."""
        old = "line1\nline2\nline3\n"
        new = "line1\nmodified\nline3\n"
        diff = create_diff(old, new, "test.py")
        assert "-line2" in diff
        assert "+modified" in diff

    def test_format_diff_markdown(self):
        """Test markdown diff formatting."""
        diff = "-old\n+new"
        result = format_diff_markdown(diff)
        assert "```diff" in result
        assert diff in result

    def test_format_token_usage(self):
        """Test token usage formatting."""
        result = format_token_usage("gpt-4o", 1000, 500, show_cost=True)
        assert "1,000" in result
        assert "500" in result
        assert "$" in result

    def test_format_token_usage_no_cost(self):
        """Test token usage without cost."""
        result = format_token_usage("gpt-4o", 1000, 500, show_cost=False)
        assert "$" not in result

    def test_format_collapsible(self):
        """Test collapsible section creation."""
        result = format_collapsible("Title", "Content", collapsed=True)
        assert "<details>" in result
        assert "<summary>Title</summary>" in result
        assert "Content" in result

    def test_format_collapsible_open(self):
        """Test collapsible section that starts open."""
        result = format_collapsible("Title", "Content", collapsed=False)
        assert "<details open>" in result

    def test_truncate_for_display(self):
        """Test text truncation."""
        long_text = "a" * 1000
        result = truncate_for_display(long_text, max_length=100)
        assert len(result) <= 100
        assert result.endswith("...")

    def test_truncate_short_text(self):
        """Test that short text is not truncated."""
        short_text = "hello"
        result = truncate_for_display(short_text, max_length=100)
        assert result == short_text

    def test_format_elapsed_time_seconds(self):
        """Test elapsed time formatting for seconds."""
        assert "5.0s" in format_elapsed_time(5.0)

    def test_format_elapsed_time_minutes(self):
        """Test elapsed time formatting for minutes."""
        result = format_elapsed_time(125)
        assert "2m" in result

    def test_format_elapsed_time_hours(self):
        """Test elapsed time formatting for hours."""
        result = format_elapsed_time(3700)
        assert "1h" in result


class TestConversationBranch:
    """Tests for conversation branching."""

    def test_initial_state(self):
        """Test initial branch state."""
        branch = ConversationBranch()
        assert branch.current_branch == "main"
        assert "main" in branch.list_branches()

    def test_add_message(self):
        """Test adding messages to a branch."""
        branch = ConversationBranch()
        branch.add_message({"role": "user", "content": "hello"})
        history = branch.get_history()
        assert len(history) == 1
        assert history[0]["content"] == "hello"

    def test_create_branch(self):
        """Test creating a new branch."""
        branch = ConversationBranch()
        branch.add_message({"role": "user", "content": "msg1"})
        branch.add_message({"role": "assistant", "content": "msg2"})

        new_branch = branch.create_branch(from_message_index=1)
        assert new_branch in branch.list_branches()
        assert len(branch.branches[new_branch]) == 1

    def test_switch_branch(self):
        """Test switching branches."""
        branch = ConversationBranch()
        new_branch = branch.create_branch()
        branch.switch_branch(new_branch)
        assert branch.current_branch == new_branch

    def test_delete_branch(self):
        """Test deleting a branch."""
        branch = ConversationBranch()
        new_branch = branch.create_branch()
        branch.delete_branch(new_branch)
        assert new_branch not in branch.list_branches()

    def test_cannot_delete_main(self):
        """Test that main branch cannot be deleted."""
        branch = ConversationBranch()
        branch.delete_branch("main")
        assert "main" in branch.list_branches()

    def test_switch_to_nonexistent_branch(self):
        """Test switching to nonexistent branch does nothing."""
        branch = ConversationBranch()
        branch.switch_branch("nonexistent")
        assert branch.current_branch == "main"

    def test_get_history_for_specific_branch(self):
        """Test getting history for a specific branch."""
        branch = ConversationBranch()
        branch.add_message({"role": "user", "content": "main_msg"})

        new_branch = branch.create_branch()
        branch.switch_branch(new_branch)
        branch.add_message({"role": "user", "content": "branch_msg"})

        main_history = branch.get_history("main")
        branch_history = branch.get_history(new_branch)

        assert len(main_history) == 1
        assert len(branch_history) == 2


class TestDiffGeneration:
    """Tests for diff generation utilities."""

    def test_create_diff_additions(self):
        """Test diff shows additions."""
        old = "line1\n"
        new = "line1\nline2\n"
        diff = create_diff(old, new, "test.py")
        assert "+line2" in diff

    def test_create_diff_deletions(self):
        """Test diff shows deletions."""
        old = "line1\nline2\n"
        new = "line1\n"
        diff = create_diff(old, new, "test.py")
        assert "-line2" in diff

    def test_create_diff_no_changes(self):
        """Test diff with identical content."""
        content = "line1\nline2\n"
        diff = create_diff(content, content, "test.py")
        assert diff == ""  # unified_diff returns empty for identical content

    def test_create_diff_header(self):
        """Test diff includes file header."""
        old = "old\n"
        new = "new\n"
        diff = create_diff(old, new, "myfile.py")
        assert "a/myfile.py" in diff
        assert "b/myfile.py" in diff

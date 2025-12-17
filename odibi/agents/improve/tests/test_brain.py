"""Tests for ImprovementBrain - LLM-powered code improvement.

Tests verify:
- Brain attempts fixes on gate failures
- Brain stops after max attempts
- Brain uses avoid_issues list
- Brain returns passing result on success
- SandboxFileTools path safety
"""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from odibi.agents.improve.brain import (
    ImprovementBrain,
    SandboxFileTools,
    BrainToolDefs,
    _build_failure_prompt,
    create_improvement_brain,
)
from odibi.agents.improve.results import (
    GateCheckResult,
    TestResult,
    LintResult,
)
from odibi.agents.improve.sandbox import SandboxInfo


@pytest.fixture
def temp_sandbox():
    """Create a temporary sandbox directory."""
    temp = tempfile.mkdtemp(prefix="brain_test_")
    sandbox_path = Path(temp)

    (sandbox_path / "odibi").mkdir()
    (sandbox_path / "odibi" / "__init__.py").write_text("# odibi package")
    (sandbox_path / "odibi" / "core.py").write_text(
        "def broken_function():\n    return None  # Bug here\n"
    )
    (sandbox_path / "tests").mkdir()
    (sandbox_path / "tests" / "test_core.py").write_text("def test_example():\n    assert True\n")

    yield sandbox_path
    shutil.rmtree(temp, ignore_errors=True)


@pytest.fixture
def sandbox_info(temp_sandbox: Path) -> SandboxInfo:
    """Create a SandboxInfo for testing."""
    return SandboxInfo(
        sandbox_id="test_cycle_001",
        sandbox_path=temp_sandbox,
        created_at="2025-12-16T00:00:00",
        is_active=True,
    )


@pytest.fixture
def failing_gate_result() -> GateCheckResult:
    """Create a failing GateCheckResult for testing."""
    return GateCheckResult(
        tests_passed=False,
        lint_passed=True,
        validate_passed=True,
        golden_passed=True,
        all_passed=False,
        test_result=TestResult(
            passed=5,
            failed=1,
            skipped=0,
            errors=0,
            duration=2.5,
            exit_code=1,
            output="FAILED test_core.py::test_broken - AssertionError",
            failed_tests=["test_core.py::test_broken"],
        ),
        failure_reasons=["Tests failed: 1 failed, 0 errors"],
    )


@pytest.fixture
def passing_gate_result() -> GateCheckResult:
    """Create a passing GateCheckResult for testing."""
    return GateCheckResult(
        tests_passed=True,
        lint_passed=True,
        validate_passed=True,
        golden_passed=True,
        all_passed=True,
    )


class TestSandboxFileTools:
    """Tests for SandboxFileTools - file operations within sandbox."""

    def test_read_file_success(self, temp_sandbox: Path):
        """Test reading a file within the sandbox."""
        tools = SandboxFileTools(temp_sandbox)

        result = tools.read_file("odibi/__init__.py")

        assert "odibi package" in result
        assert "File: odibi/__init__.py" in result

    def test_read_file_not_found(self, temp_sandbox: Path):
        """Test reading a nonexistent file."""
        tools = SandboxFileTools(temp_sandbox)

        result = tools.read_file("nonexistent.py")

        assert "Error" in result
        assert "not found" in result.lower()

    def test_read_file_path_escape_blocked(self, temp_sandbox: Path):
        """Test that path traversal is blocked."""
        tools = SandboxFileTools(temp_sandbox)

        result = tools.read_file("../../../etc/passwd")

        assert "Error" in result
        assert "escapes" in result.lower() or "error" in result.lower()

    def test_write_file_success(self, temp_sandbox: Path):
        """Test writing a file within the sandbox."""
        tools = SandboxFileTools(temp_sandbox)

        result = tools.write_file("new_file.py", "# New file content\n")

        assert "Created" in result or "Updated" in result
        assert (temp_sandbox / "new_file.py").exists()
        assert (temp_sandbox / "new_file.py").read_text() == "# New file content\n"

    def test_write_file_update_existing(self, temp_sandbox: Path):
        """Test updating an existing file."""
        tools = SandboxFileTools(temp_sandbox)

        new_content = "def fixed_function():\n    return True\n"
        result = tools.write_file("odibi/core.py", new_content)

        assert "Updated" in result
        assert (temp_sandbox / "odibi" / "core.py").read_text() == new_content

    def test_write_file_path_escape_blocked(self, temp_sandbox: Path):
        """Test that write path traversal is blocked."""
        tools = SandboxFileTools(temp_sandbox)

        result = tools.write_file("../../evil.py", "evil content")

        assert "Error" in result
        assert not Path(temp_sandbox.parent / "evil.py").exists()

    def test_list_directory_success(self, temp_sandbox: Path):
        """Test listing a directory."""
        tools = SandboxFileTools(temp_sandbox)

        result = tools.list_directory("odibi")

        assert "__init__.py" in result
        assert "core.py" in result

    def test_list_directory_not_found(self, temp_sandbox: Path):
        """Test listing a nonexistent directory."""
        tools = SandboxFileTools(temp_sandbox)

        result = tools.list_directory("nonexistent")

        assert "Error" in result

    def test_execute_tool_dispatch(self, temp_sandbox: Path):
        """Test tool execution dispatcher."""
        tools = SandboxFileTools(temp_sandbox)

        read_result = tools.execute_tool("read_file", {"path": "odibi/__init__.py"})
        assert "odibi package" in read_result

        list_result = tools.execute_tool("list_directory", {"path": "."})
        assert "odibi/" in list_result

        unknown_result = tools.execute_tool("unknown_tool", {})
        assert "Unknown tool" in unknown_result


class TestBrainToolDefs:
    """Tests for BrainToolDefs - tool definitions."""

    def test_get_tools_returns_list(self):
        """Test that get_tools returns a list of tool definitions."""
        tools = BrainToolDefs.get_tools()

        assert isinstance(tools, list)
        assert len(tools) == 3

    def test_tool_definitions_have_required_fields(self):
        """Test that tool definitions have required OpenAI fields."""
        tools = BrainToolDefs.get_tools()

        for tool in tools:
            assert tool["type"] == "function"
            assert "function" in tool
            assert "name" in tool["function"]
            assert "description" in tool["function"]
            assert "parameters" in tool["function"]

    def test_tool_names(self):
        """Test that expected tools are defined."""
        tools = BrainToolDefs.get_tools()
        names = [t["function"]["name"] for t in tools]

        assert "read_file" in names
        assert "write_file" in names
        assert "list_directory" in names


class TestBuildFailurePrompt:
    """Tests for _build_failure_prompt function."""

    def test_includes_campaign_goal(self, failing_gate_result):
        """Test that prompt includes campaign goal."""
        prompt = _build_failure_prompt(
            gate_result=failing_gate_result,
            avoid_issues=[],
            campaign_goal="Fix all bugs",
        )

        assert "Fix all bugs" in prompt
        assert "Campaign Goal" in prompt

    def test_includes_test_failures(self, failing_gate_result):
        """Test that prompt includes test failure details."""
        prompt = _build_failure_prompt(
            gate_result=failing_gate_result,
            avoid_issues=[],
            campaign_goal="Fix bugs",
        )

        assert "Tests FAILED" in prompt
        assert "test_core.py::test_broken" in prompt

    def test_includes_avoid_issues(self, failing_gate_result):
        """Test that prompt includes issues to avoid."""
        prompt = _build_failure_prompt(
            gate_result=failing_gate_result,
            avoid_issues=["bad_approach_1", "bad_approach_2"],
            campaign_goal="Fix bugs",
        )

        assert "AVOID" in prompt
        assert "bad_approach_1" in prompt
        assert "bad_approach_2" in prompt

    def test_includes_lint_failures(self):
        """Test that prompt includes lint failure details."""
        gate_result = GateCheckResult(
            tests_passed=True,
            lint_passed=False,
            validate_passed=True,
            golden_passed=True,
            all_passed=False,
            lint_result=LintResult(
                error_count=3,
                warning_count=1,
                fixable_count=2,
                exit_code=1,
                output="E501: line too long",
                duration=0.5,
                errors=["odibi/core.py:10: E501 line too long"],
            ),
            failure_reasons=["Lint errors: 3"],
        )

        prompt = _build_failure_prompt(
            gate_result=gate_result,
            avoid_issues=[],
            campaign_goal="Fix lint",
        )

        assert "Lint FAILED" in prompt
        assert "3 errors" in prompt


class TestImprovementBrain:
    """Tests for ImprovementBrain class."""

    def test_brain_attempts_fix_on_failure(
        self,
        sandbox_info: SandboxInfo,
        failing_gate_result: GateCheckResult,
        passing_gate_result: GateCheckResult,
    ):
        """Test that brain attempts to fix gate failures."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = "DONE"
        mock_response.tool_calls = None
        mock_client.chat.return_value = mock_response
        mock_client.chat_stream_with_tools.return_value = mock_response

        brain = ImprovementBrain(mock_client, max_attempts_per_sandbox=3)

        call_count = [0]

        def mock_run_gates(sandbox):
            call_count[0] += 1
            if call_count[0] >= 2:
                return passing_gate_result
            return failing_gate_result

        result, attempts, files_modified = brain.improve_sandbox(
            sandbox=sandbox_info,
            gate_result=failing_gate_result,
            avoid_issues=[],
            campaign_goal="Fix bugs",
            run_gates=mock_run_gates,
        )

        assert mock_client.chat_stream_with_tools.called or mock_client.chat.called

    def test_brain_stops_after_max_attempts(
        self,
        sandbox_info: SandboxInfo,
        failing_gate_result: GateCheckResult,
    ):
        """Test that brain stops after max_attempts."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = None
        mock_response.tool_calls = [
            {
                "id": "call_1",
                "function": {
                    "name": "read_file",
                    "arguments": '{"path": "odibi/core.py"}',
                },
            }
        ]
        mock_client.chat.return_value = mock_response
        mock_client.chat_stream_with_tools.return_value = mock_response

        brain = ImprovementBrain(mock_client, max_attempts_per_sandbox=2)

        def always_fail(sandbox):
            return failing_gate_result

        result, attempts, files_modified = brain.improve_sandbox(
            sandbox=sandbox_info,
            gate_result=failing_gate_result,
            avoid_issues=[],
            campaign_goal="Fix bugs",
            run_gates=always_fail,
        )

        assert attempts <= 2
        assert not result.all_passed

    def test_brain_uses_avoid_issues(
        self,
        sandbox_info: SandboxInfo,
        failing_gate_result: GateCheckResult,
    ):
        """Test that brain includes avoid_issues in prompts."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = "DONE"
        mock_response.tool_calls = None
        mock_client.chat.return_value = mock_response
        mock_client.chat_stream_with_tools.return_value = mock_response

        brain = ImprovementBrain(mock_client, max_attempts_per_sandbox=1)

        avoid = ["dont_do_this", "also_avoid_this"]

        brain.improve_sandbox(
            sandbox=sandbox_info,
            gate_result=failing_gate_result,
            avoid_issues=avoid,
            campaign_goal="Fix bugs",
            run_gates=lambda s: failing_gate_result,
        )

        call_args = mock_client.chat_stream_with_tools.call_args
        messages = call_args.kwargs.get("messages", call_args.args[0] if call_args.args else [])

        user_message = next((m["content"] for m in messages if m.get("role") == "user"), "")
        assert "dont_do_this" in user_message
        assert "also_avoid_this" in user_message

    def test_brain_returns_passing_result_on_success(
        self,
        sandbox_info: SandboxInfo,
        failing_gate_result: GateCheckResult,
        passing_gate_result: GateCheckResult,
    ):
        """Test that brain returns passing result when gates pass."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = None
        mock_response.tool_calls = [
            {
                "id": "call_1",
                "function": {
                    "name": "write_file",
                    "arguments": '{"path": "fix.py", "content": "fixed"}',
                },
            }
        ]
        mock_client.chat.side_effect = [
            mock_response,
            MagicMock(content="DONE", tool_calls=None),
        ]
        mock_client.chat_stream_with_tools.side_effect = [
            mock_response,
            MagicMock(content="DONE", tool_calls=None),
        ]

        brain = ImprovementBrain(mock_client, max_attempts_per_sandbox=3)

        call_count = [0]

        def fix_on_second_call(sandbox):
            call_count[0] += 1
            return passing_gate_result if call_count[0] >= 1 else failing_gate_result

        result, attempts, files_modified = brain.improve_sandbox(
            sandbox=sandbox_info,
            gate_result=failing_gate_result,
            avoid_issues=[],
            campaign_goal="Fix bugs",
            run_gates=fix_on_second_call,
        )

        assert result.all_passed
        assert attempts >= 1

    def test_brain_skips_if_already_passing(
        self,
        sandbox_info: SandboxInfo,
        passing_gate_result: GateCheckResult,
    ):
        """Test that brain skips if gates already pass."""
        mock_client = MagicMock()
        brain = ImprovementBrain(mock_client, max_attempts_per_sandbox=3)

        result, attempts, files_modified = brain.improve_sandbox(
            sandbox=sandbox_info,
            gate_result=passing_gate_result,
            avoid_issues=[],
            campaign_goal="Fix bugs",
            run_gates=lambda s: passing_gate_result,
        )

        assert result.all_passed
        assert attempts == 1
        assert not mock_client.chat.called and not mock_client.chat_stream_with_tools.called


class TestCreateImprovementBrain:
    """Tests for create_improvement_brain factory."""

    def test_create_with_defaults(self):
        """Test creating brain with default settings."""
        with patch("agents.improve.brain.LLMClient") as mock_client_class:
            brain = create_improvement_brain()

            mock_client_class.assert_called_once()
            assert brain._max_attempts == 3

    def test_create_with_custom_settings(self):
        """Test creating brain with custom settings."""
        with patch("agents.improve.brain.LLMClient") as mock_client_class:
            brain = create_improvement_brain(
                model="gpt-4o",
                max_attempts=5,
            )

            call_args = mock_client_class.call_args
            config = call_args.args[0] if call_args.args else call_args.kwargs.get("config")
            assert config.model == "gpt-4o"
            assert brain._max_attempts == 5

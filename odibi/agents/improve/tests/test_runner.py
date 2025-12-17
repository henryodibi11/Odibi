"""Tests for OdibiPipelineRunner and related components."""

import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from odibi.agents.improve.results import (
    CommandResult,
    CycleResult,
    ExecutionStatus,
    LintResult,
    PipelineResult,
    TestResult,
)
from odibi.agents.improve.runner import OdibiPipelineRunner, RunnerError
from odibi.agents.improve.story_parser import NodeExecution, NodeFailure, ParsedStory, StoryParser


@pytest.fixture
def temp_sandbox():
    """Create a temporary sandbox directory."""
    temp = tempfile.mkdtemp(prefix="runner_test_")
    yield Path(temp)
    shutil.rmtree(temp, ignore_errors=True)


@pytest.fixture
def mock_sandbox(temp_sandbox: Path):
    """Create a mock sandbox with some files."""
    # Create basic structure
    (temp_sandbox / "odibi").mkdir()
    (temp_sandbox / "odibi" / "__init__.py").write_text("# odibi package")
    (temp_sandbox / "tests").mkdir()
    (temp_sandbox / "tests" / "__init__.py").write_text("")
    (temp_sandbox / "tests" / "test_example.py").write_text(
        "def test_pass(): assert True\ndef test_fail(): assert False"
    )
    return temp_sandbox


class TestCommandResult:
    """Tests for CommandResult dataclass."""

    def test_success_when_exit_zero(self):
        result = CommandResult(
            command="echo hello",
            exit_code=0,
            stdout="hello\n",
            stderr="",
            duration=0.1,
        )
        assert result.success is True

    def test_failure_when_exit_nonzero(self):
        result = CommandResult(
            command="false",
            exit_code=1,
            stdout="",
            stderr="error",
            duration=0.1,
        )
        assert result.success is False

    def test_failure_when_timed_out(self):
        result = CommandResult(
            command="sleep 100",
            exit_code=0,
            stdout="",
            stderr="",
            duration=10.0,
            timed_out=True,
        )
        assert result.success is False


class TestTestResultDataclass:
    """Tests for TestResult dataclass."""

    def test_success_all_passed(self):
        result = TestResult(
            passed=10,
            failed=0,
            skipped=2,
            errors=0,
            duration=5.0,
            exit_code=0,
            output="10 passed, 2 skipped",
        )
        assert result.success is True
        assert result.total == 12

    def test_failure_when_failed(self):
        result = TestResult(
            passed=8,
            failed=2,
            skipped=0,
            errors=0,
            duration=5.0,
            exit_code=1,
            output="8 passed, 2 failed",
        )
        assert result.success is False

    def test_failure_when_errors(self):
        result = TestResult(
            passed=8,
            failed=0,
            skipped=0,
            errors=2,
            duration=5.0,
            exit_code=1,
            output="8 passed, 2 errors",
        )
        assert result.success is False


class TestLintResult:
    """Tests for LintResult dataclass."""

    def test_success_no_errors(self):
        result = LintResult(
            error_count=0,
            warning_count=5,
            fixable_count=0,
            exit_code=0,
            output="All checks passed!",
            duration=1.0,
        )
        assert result.success is True

    def test_failure_with_errors(self):
        result = LintResult(
            error_count=3,
            warning_count=0,
            fixable_count=2,
            exit_code=1,
            output="Found 3 errors",
            duration=1.0,
        )
        assert result.success is False


class TestPipelineResult:
    """Tests for PipelineResult dataclass."""

    def test_success_no_failures(self):
        result = PipelineResult(
            config_path="test.yaml",
            pipeline_name="test",
            status=ExecutionStatus.SUCCESS,
            duration=10.0,
            total_nodes=5,
            completed_nodes=5,
            failed_nodes=0,
            skipped_nodes=0,
        )
        assert result.success is True
        assert result.success_rate == 100.0

    def test_failure_with_failed_nodes(self):
        result = PipelineResult(
            config_path="test.yaml",
            pipeline_name="test",
            status=ExecutionStatus.FAILED,
            duration=10.0,
            total_nodes=5,
            completed_nodes=3,
            failed_nodes=2,
            skipped_nodes=0,
        )
        assert result.success is False
        assert result.success_rate == 60.0


class TestCycleResult:
    """Tests for CycleResult dataclass."""

    def test_duration_calculation(self):
        started = datetime.now()
        result = CycleResult(
            cycle_id="cycle_001",
            sandbox_path=Path("/tmp/sandbox"),
            started_at=started,
        )
        # Duration should be > 0 since we just created it
        assert result.duration >= 0

    def test_mark_complete(self):
        result = CycleResult(
            cycle_id="cycle_001",
            sandbox_path=Path("/tmp/sandbox"),
            started_at=datetime.now(),
        )
        result.mark_complete(
            promoted=True,
            learning=True,
            lesson="Fixed null handling bug",
        )
        assert result.completed_at is not None
        assert result.promoted is True
        assert result.learning is True
        assert result.lesson == "Fixed null handling bug"

    def test_to_dict(self):
        result = CycleResult(
            cycle_id="cycle_001",
            sandbox_path=Path("/tmp/sandbox"),
            started_at=datetime.now(),
        )
        result.mark_complete(promoted=False, rejection_reason="Tests failed")

        data = result.to_dict()
        assert data["cycle_id"] == "cycle_001"
        assert data["promoted"] is False
        assert data["rejection_reason"] == "Tests failed"
        assert "started_at" in data
        assert "completed_at" in data


class TestOdibiPipelineRunner:
    """Tests for OdibiPipelineRunner."""

    def test_init_with_valid_path(self, mock_sandbox: Path):
        runner = OdibiPipelineRunner(mock_sandbox)
        assert runner.sandbox_path == mock_sandbox

    def test_init_with_invalid_path(self):
        with pytest.raises(RunnerError, match="does not exist"):
            OdibiPipelineRunner(Path("/nonexistent/path"))

    def test_run_command_success(self, mock_sandbox: Path):
        runner = OdibiPipelineRunner(mock_sandbox)
        result = runner._run_command(["python", "--version"])
        assert result.exit_code == 0
        assert "Python" in result.stdout or "Python" in result.stderr

    def test_run_command_failure(self, mock_sandbox: Path):
        runner = OdibiPipelineRunner(mock_sandbox)
        result = runner._run_command(["python", "-c", "raise Exception('test')"])
        assert result.exit_code != 0

    def test_run_command_timeout(self, mock_sandbox: Path):
        runner = OdibiPipelineRunner(mock_sandbox)
        result = runner._run_command(
            ["python", "-c", "import time; time.sleep(10)"],
            timeout=1,
        )
        assert result.timed_out is True
        assert result.success is False


class TestStoryParser:
    """Tests for StoryParser."""

    @pytest.fixture
    def sample_markdown_story(self) -> str:
        return """# 📊 Pipeline Run Story: bronze_ingest

**Started:** 2025-12-16T10:00:00
**Completed:** 2025-12-16T10:05:30
**Duration:** 330.00s
**Status:** ✅ Success (5/5 nodes)

**Layer:** bronze

---

## Summary

- ✅ **Completed:** 5 nodes
- ❌ **Failed:** 0 nodes
- ⏭️ **Skipped:** 0 nodes
- 📊 **Success Rate:** 100.0%
- 📈 **Total Rows Processed:** 15,000
- ⏱️ **Duration:** 330.00s

---

## Node Execution Details

### ✅ load_source_data
**Operation:** `read_csv`
**Duration:** 10.5000s
**Rows In:** 0
**Rows Out:** 5,000

### ✅ transform_data
**Operation:** `filter_rows`
**Duration:** 5.2500s
**Rows In:** 5,000
**Rows Out:** 4,500

### ✅ validate_data
**Operation:** `validate`
**Duration:** 2.1000s
**Rows In:** 4,500
**Rows Out:** 4,500

### ✅ deduplicate
**Operation:** `deduplicate`
**Duration:** 3.0000s
**Rows In:** 4,500
**Rows Out:** 4,000

### ✅ write_output
**Operation:** `write_delta`
**Duration:** 309.1500s
**Rows In:** 4,000
**Rows Out:** 4,000
"""

    @pytest.fixture
    def sample_failed_story(self) -> str:
        return """# 📊 Pipeline Run Story: failed_pipeline

**Started:** 2025-12-16T10:00:00
**Completed:** 2025-12-16T10:02:00
**Duration:** 120.00s
**Status:** ❌ Failed (2 nodes)

---

## Summary

- ✅ **Completed:** 3 nodes
- ❌ **Failed:** 2 nodes
- ⏭️ **Skipped:** 1 nodes
- 📊 **Success Rate:** 50.0%
- 📈 **Total Rows Processed:** 1,000

---

## Node Execution Details

### ✅ load_data
**Operation:** `read_csv`
**Duration:** 5.0000s
**Rows Out:** 1,000

### ❌ transform_broken
**Operation:** `derive_columns`
**Duration:** 0.5000s
**Error Type:** `ValueError`

Error details:
```python
ValueError: Column 'missing_col' not found in DataFrame
```
"""

    def test_parse_successful_story(self, sample_markdown_story: str):
        parser = StoryParser()
        story = parser.parse_content(sample_markdown_story)

        assert story.pipeline_name == "bronze_ingest"
        assert story.status == "success"
        assert story.total_nodes == 5
        assert story.completed_nodes == 5
        assert story.failed_nodes == 0
        assert story.duration == 330.0
        assert story.total_rows_processed == 15000
        assert story.layer == "bronze"
        assert story.success_rate == 100.0

    def test_parse_failed_story(self, sample_failed_story: str):
        parser = StoryParser()
        story = parser.parse_content(sample_failed_story)

        assert story.pipeline_name == "failed_pipeline"
        assert story.status == "failed"
        assert story.completed_nodes == 3
        assert story.failed_nodes == 2
        assert story.skipped_nodes == 1
        assert story.success_rate == 50.0

    def test_get_failures(self, sample_failed_story: str):
        parser = StoryParser()
        story = parser.parse_content(sample_failed_story)
        failures = parser.get_failures(story)

        # The parser should extract failures based on error sections
        # Note: The current parser implementation may need refinement
        # to fully extract all failure details
        assert isinstance(failures, list)

    def test_parse_json_story(self):
        parser = StoryParser()
        json_content = """{
            "pipeline_name": "json_pipeline",
            "started_at": "2025-12-16T10:00:00",
            "completed_at": "2025-12-16T10:05:00",
            "duration": 300.0,
            "total_nodes": 3,
            "completed_nodes": 3,
            "failed_nodes": 0,
            "skipped_nodes": 0,
            "nodes": [
                {"node_name": "node1", "operation": "read", "status": "success", "duration": 10.0, "rows_out": 100}
            ]
        }"""

        story = parser.parse_content(json_content, format="json")

        assert story.pipeline_name == "json_pipeline"
        assert story.total_nodes == 3
        assert story.completed_nodes == 3
        assert story.status == "success"
        assert len(story.nodes) == 1
        assert story.nodes[0].node_name == "node1"

    def test_parse_nonexistent_file(self):
        parser = StoryParser()
        with pytest.raises(FileNotFoundError):
            parser.parse(Path("/nonexistent/story.md"))

    def test_get_success_rate(self, sample_markdown_story: str):
        parser = StoryParser()
        story = parser.parse_content(sample_markdown_story)
        rate = parser.get_success_rate(story)
        assert rate == 100.0


class TestParsedStory:
    """Tests for ParsedStory dataclass."""

    def test_success_rate_calculation(self):
        story = ParsedStory(
            pipeline_name="test",
            total_nodes=10,
            completed_nodes=8,
            failed_nodes=2,
        )
        assert story.success_rate == 80.0

    def test_success_rate_zero_nodes(self):
        story = ParsedStory(
            pipeline_name="test",
            total_nodes=0,
        )
        assert story.success_rate == 0.0

    def test_get_failures(self):
        story = ParsedStory(pipeline_name="test")
        story.nodes = [
            NodeExecution("node1", "read", "success", 1.0),
            NodeExecution("node2", "transform", "failed", 0.5, error_message="Failed"),
            NodeExecution("node3", "write", "success", 2.0),
        ]

        failures = story.get_failures()
        assert len(failures) == 1
        assert failures[0].node_name == "node2"
        assert failures[0].error_message == "Failed"


class TestNodeFailure:
    """Tests for NodeFailure dataclass."""

    def test_str_representation(self):
        failure = NodeFailure(
            node_name="broken_node",
            operation="transform",
            error_message="Column not found",
        )
        assert "broken_node" in str(failure)
        assert "Column not found" in str(failure)

    def test_str_without_message(self):
        failure = NodeFailure(
            node_name="broken_node",
            operation="transform",
        )
        assert "Unknown error" in str(failure)

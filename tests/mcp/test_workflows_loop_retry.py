"""Tests for loop and retry functionality in workflow engine."""

import pytest
from odibi_mcp.tools.workflows import Engine


class TestLoopStep:
    """Test loop step type."""

    def test_loop_basic_success(self):
        """Loop executes body until condition is met."""
        workflow = {
            "steps": [
                {"type": "set", "values": {"results.done": False}},
                {
                    "type": "loop",
                    "label": "increment_loop",
                    "max_attempts": 5,
                    "until": {"path": "results.done", "equals": True},
                    "body": [
                        {
                            "type": "call",
                            "tool": "list_patterns",
                            "args": {},
                            "assign": "results.patterns",
                        },
                        # Counter is 1 on 1st iteration, 2 on 2nd, etc. Exit on 3rd iteration.
                        {
                            "type": "branch",
                            "cases": [
                                {
                                    "when": {
                                        "path": "context.loop_attempts.increment_loop",
                                        "equals": 3,
                                    },
                                    "goto": "set_done",
                                }
                            ],
                        },
                        {"type": "log", "message": "Continue looping"},
                        {"type": "goto", "label": "end_body"},
                        {"type": "set", "label": "set_done", "values": {"results.done": True}},
                        {"type": "log", "label": "end_body", "message": "Iteration complete"},
                    ],
                },
                {"type": "log", "message": "Loop complete"},
            ]
        }

        engine = Engine(workflow, {})
        result = engine.run()

        assert result["status"] == "COMPLETED"
        assert engine.state["results"]["done"] is True
        assert engine.state["context"]["loop_attempts"]["increment_loop"] == 3

        # Check loop events
        loop_events = [
            e for e in engine.events if e.get("type") in ["loop_iteration", "loop_complete"]
        ]
        assert len(loop_events) > 0

    def test_loop_exhaustion(self):
        """Loop exhausts max_attempts before condition met."""
        workflow = {
            "steps": [
                {"type": "set", "values": {"results.never_true": False}},
                {
                    "type": "loop",
                    "label": "limited_loop",
                    "max_attempts": 2,
                    "until": {"path": "results.never_true", "equals": True},  # Never becomes true
                    "body": [
                        {
                            "type": "call",
                            "tool": "list_patterns",
                            "args": {},
                            "assign": "results.temp",
                        },
                        {"type": "log", "message": "Loop body executed"},
                    ],
                },
                {"type": "log", "message": "After loop"},
            ]
        }

        engine = Engine(workflow, {})
        result = engine.run()

        assert result["status"] == "COMPLETED"
        assert engine.state["context"]["loop_attempts"]["limited_loop"] == 2

        # Check exhaustion event
        exhausted = [e for e in engine.events if e.get("type") == "loop_exhausted"]
        assert len(exhausted) == 1

    def test_loop_with_on_exhausted(self):
        """Loop jumps to on_exhausted label when max_attempts reached."""
        workflow = {
            "steps": [
                {"type": "set", "values": {"results.never_met": False, "results.exhausted": False}},
                {
                    "type": "loop",
                    "label": "failing_loop",
                    "max_attempts": 2,
                    "until": {"path": "results.never_met", "equals": True},  # Never becomes true
                    "body": [
                        {
                            "type": "call",
                            "tool": "list_patterns",
                            "args": {},
                            "assign": "results.temp",
                        }
                    ],
                    "on_exhausted": "handle_failure",
                },
                {"type": "log", "message": "Success path"},
                {"type": "goto", "label": "end"},
                {"type": "set", "label": "handle_failure", "values": {"results.exhausted": True}},
                {"type": "log", "label": "end", "message": "Done"},
            ]
        }

        engine = Engine(workflow, {})
        result = engine.run()

        assert result["status"] == "COMPLETED"
        assert engine.state["results"]["exhausted"] is True

    def test_loop_condition_already_met(self):
        """Loop doesn't execute if condition already true."""
        workflow = {
            "steps": [
                {"type": "set", "values": {"results.valid": True, "results.iterations": 0}},
                {
                    "type": "loop",
                    "label": "skip_loop",
                    "max_attempts": 5,
                    "until": {"path": "results.valid", "equals": True},
                    "body": [{"type": "set", "values": {"results.iterations": 1}}],
                },
            ]
        }

        engine = Engine(workflow, {})
        result = engine.run()

        assert result["status"] == "COMPLETED"
        assert engine.state["results"]["iterations"] == 0  # Body never executed

    def test_loop_with_complex_body(self):
        """Loop with branching and conditional logic in body."""
        workflow = {
            "steps": [
                {"type": "set", "values": {"results.done": False, "results.skipped": False}},
                {
                    "type": "loop",
                    "label": "complex_loop",
                    "max_attempts": 10,
                    "until": {"path": "results.done", "equals": True},
                    "body": [
                        {
                            "type": "call",
                            "tool": "list_patterns",
                            "args": {},
                            "assign": "results.temp",
                        },
                        {
                            "type": "branch",
                            "cases": [
                                {
                                    "when": {
                                        "path": "context.loop_attempts.complex_loop",
                                        "equals": 2,
                                    },
                                    "goto": "skip_rest",
                                }
                            ],
                        },
                        {"type": "log", "message": "After branch"},
                        {"type": "log", "label": "skip_rest", "message": "Continue"},
                        {
                            "type": "branch",
                            "cases": [
                                {
                                    "when": {
                                        "path": "context.loop_attempts.complex_loop",
                                        "equals": 2,
                                    },
                                    "goto": "mark_skipped",
                                }
                            ],
                        },
                        {"type": "goto", "label": "check_done"},
                        {
                            "type": "set",
                            "label": "mark_skipped",
                            "values": {"results.skipped": True},
                        },
                        {
                            "type": "branch",
                            "label": "check_done",
                            "cases": [
                                {
                                    "when": {
                                        "path": "context.loop_attempts.complex_loop",
                                        "equals": 3,
                                    },
                                    "goto": "mark_done",
                                }
                            ],
                        },
                        {"type": "goto", "label": "end_iteration"},
                        {"type": "set", "label": "mark_done", "values": {"results.done": True}},
                        {"type": "log", "label": "end_iteration", "message": "End iteration"},
                    ],
                },
            ]
        }

        engine = Engine(workflow, {})
        result = engine.run()

        assert result["status"] == "COMPLETED"
        assert engine.state["results"]["done"] is True
        assert engine.state["context"]["loop_attempts"]["complex_loop"] == 3


class TestRetryLogic:
    """Test retry logic on call steps."""

    def test_call_no_retry_success(self):
        """Call succeeds on first attempt without retry config."""
        workflow = {
            "steps": [
                {"type": "call", "tool": "list_patterns", "args": {}, "assign": "results.patterns"}
            ]
        }

        engine = Engine(workflow, {})
        result = engine.run()

        assert result["status"] == "COMPLETED"
        assert "patterns" in engine.state["results"]

        # Check only one tool_call event
        tool_calls = [e for e in engine.events if e.get("type") == "tool_call"]
        assert len(tool_calls) == 1

    def test_call_retry_on_exception(self):
        """Call retries on exception when retry_on_exception is true."""
        workflow = {
            "steps": [
                {
                    "type": "call",
                    "tool": "nonexistent_tool",
                    "args": {},
                    "retry": {"max_attempts": 3, "backoff": 0.1, "retry_on_exception": True},
                    "assign": "results.output",
                }
            ]
        }

        engine = Engine(workflow, {})
        result = engine.run()

        assert result["status"] == "COMPLETED"

        # Check multiple retry attempts
        retry_events = [e for e in engine.events if e.get("type") and "retry" in e.get("type")]
        assert len(retry_events) > 0

        # Check tool was called 3 times
        tool_calls = [e for e in engine.events if e.get("type") == "tool_call"]
        assert len(tool_calls) == 3

    def test_call_on_error_goto(self):
        """Call jumps to on_error label on failure."""
        workflow = {
            "steps": [
                {
                    "type": "call",
                    "tool": "nonexistent_tool",
                    "args": {},
                    "assign": "results.output",
                    "on_error": {"goto": "handle_error"},
                },
                {"type": "log", "message": "Success path"},
                {"type": "goto", "label": "end"},
                {"type": "set", "label": "handle_error", "values": {"results.error_handled": True}},
                {"type": "log", "label": "end", "message": "Done"},
            ]
        }

        engine = Engine(workflow, {})
        result = engine.run()

        assert result["status"] == "COMPLETED"
        assert engine.state["results"]["error_handled"] is True

    def test_call_retry_backoff(self):
        """Call respects backoff timing between retries."""
        import time

        workflow = {
            "steps": [
                {
                    "type": "call",
                    "tool": "nonexistent_tool",
                    "args": {},
                    "retry": {"max_attempts": 2, "backoff": 0.1, "retry_on_exception": True},
                    "assign": "results.output",
                }
            ]
        }

        engine = Engine(workflow, {})
        start = time.time()
        result = engine.run()
        elapsed = time.time() - start

        assert result["status"] == "COMPLETED"

        # Should have waited at least backoff time (0.1s for second attempt)
        assert elapsed >= 0.1

        # Check retry_wait event
        wait_events = [e for e in engine.events if e.get("type") == "retry_wait"]
        assert len(wait_events) == 1  # One wait between attempts


class TestLoopRetryIntegration:
    """Test loop and retry working together."""

    def test_loop_with_retry_calls(self):
        """Loop body contains calls with retry logic."""
        workflow = {
            "steps": [
                {"type": "set", "values": {"results.attempts": 0}},
                {
                    "type": "loop",
                    "label": "validation_loop",
                    "max_attempts": 3,
                    "until": {"path": "results.valid", "equals": True},
                    "body": [
                        {
                            "type": "call",
                            "tool": "list_patterns",
                            "args": {},
                            "retry": {
                                "max_attempts": 2,
                                "backoff": 0.05,
                                "retry_on_exception": True,
                            },
                            "assign": "results.patterns",
                        },
                        {
                            "type": "branch",
                            "cases": [
                                {"when": {"path": "results.attempts", "equals": 0}, "goto": "set_1"}
                            ],
                        },
                        {"type": "set", "values": {"results.attempts": 2}},
                        {"type": "goto", "label": "mark_valid"},
                        {"type": "set", "label": "set_1", "values": {"results.attempts": 1}},
                        {"type": "log", "label": "mark_valid", "message": "Valid"},
                        {"type": "set", "values": {"results.valid": True}},
                    ],
                },
            ]
        }

        engine = Engine(workflow, {})
        result = engine.run()

        assert result["status"] == "COMPLETED"
        assert engine.state["results"]["valid"] is True
        assert engine.state["results"]["attempts"] >= 1

    def test_nested_error_handling(self):
        """Loop with retry and on_error handling."""
        workflow = {
            "steps": [
                {"type": "set", "values": {"results.loop_count": 0, "results.errors": 0}},
                {
                    "type": "loop",
                    "label": "error_loop",
                    "max_attempts": 2,
                    "until": {"path": "results.success", "equals": True},
                    "body": [
                        {
                            "type": "branch",
                            "cases": [
                                {
                                    "when": {"path": "results.loop_count", "equals": 0},
                                    "goto": "set_loop_1",
                                }
                            ],
                        },
                        {"type": "set", "values": {"results.loop_count": 2}},
                        {"type": "goto", "label": "continue_loop"},
                        {"type": "set", "label": "set_loop_1", "values": {"results.loop_count": 1}},
                        {"type": "log", "label": "continue_loop", "message": "Loop iteration"},
                        {
                            "type": "call",
                            "tool": "bad_tool",
                            "args": {},
                            "retry": {"max_attempts": 1, "retry_on_exception": False},
                            "on_error": {"goto": "log_error"},
                        },
                        {"type": "goto", "label": "after_error"},
                        {
                            "type": "branch",
                            "label": "log_error",
                            "cases": [
                                {
                                    "when": {"path": "results.errors", "equals": 0},
                                    "goto": "set_err_1",
                                }
                            ],
                        },
                        {"type": "set", "values": {"results.errors": 2}},
                        {"type": "goto", "label": "after_error"},
                        {"type": "set", "label": "set_err_1", "values": {"results.errors": 1}},
                        {"type": "log", "label": "after_error", "message": "Continuing"},
                        {"type": "set", "values": {"results.success": True}},
                    ],
                    "on_exhausted": "final_cleanup",
                },
                {"type": "log", "label": "final_cleanup", "message": "Cleanup"},
            ]
        }

        engine = Engine(workflow, {})
        result = engine.run()

        assert result["status"] == "COMPLETED"
        assert engine.state["results"]["errors"] >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

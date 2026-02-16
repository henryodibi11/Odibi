"""Unit tests for ODIBI custom exceptions."""

import pytest

from odibi.exceptions import (
    OdibiException,
    ConfigValidationError,
    ConnectionError,
    DependencyError,
    ExecutionContext,
    NodeExecutionError,
    TransformError,
    ValidationError,
    GateFailedError,
)


class TestInheritance:
    def test_odibi_exception_inherits_from_exception(self):
        assert issubclass(OdibiException, Exception)

    @pytest.mark.parametrize(
        "exc_class",
        [
            ConfigValidationError,
            ConnectionError,
            DependencyError,
            NodeExecutionError,
            TransformError,
            ValidationError,
            GateFailedError,
        ],
    )
    def test_all_exceptions_inherit_from_odibi_exception(self, exc_class):
        assert issubclass(exc_class, OdibiException)

    def test_odibi_exception_can_be_raised_and_caught(self):
        with pytest.raises(OdibiException):
            raise OdibiException("test")

    def test_transform_error_can_be_raised_with_message(self):
        with pytest.raises(TransformError, match="something broke"):
            raise TransformError("something broke")


class TestConfigValidationError:
    def test_message_only(self):
        err = ConfigValidationError("missing key 'name'")
        assert err.message == "missing key 'name'"
        assert err.file is None
        assert err.line is None
        assert "Configuration validation error" in str(err)
        assert "missing key 'name'" in str(err)

    def test_with_file(self):
        err = ConfigValidationError("bad value", file="pipeline.yaml")
        assert err.file == "pipeline.yaml"
        assert "File: pipeline.yaml" in str(err)

    def test_with_file_and_line(self):
        err = ConfigValidationError("bad value", file="pipeline.yaml", line=42)
        assert err.line == 42
        assert "Line: 42" in str(err)
        assert "File: pipeline.yaml" in str(err)

    def test_line_without_file(self):
        err = ConfigValidationError("bad value", line=10)
        assert "File:" not in str(err)
        assert "Line: 10" in str(err)

    def test_is_catchable_as_odibi_exception(self):
        with pytest.raises(OdibiException):
            raise ConfigValidationError("oops")


class TestConnectionError:
    def test_basic(self):
        err = ConnectionError("my_db", "timeout")
        assert err.connection_name == "my_db"
        assert err.reason == "timeout"
        assert err.suggestions == []
        assert "[X] Connection validation failed: my_db" in str(err)
        assert "Reason: timeout" in str(err)

    def test_with_suggestions(self):
        suggestions = ["Check credentials", "Verify network"]
        err = ConnectionError("my_db", "auth failed", suggestions=suggestions)
        assert err.suggestions == suggestions
        msg = str(err)
        assert "Suggestions:" in msg
        assert "1. Check credentials" in msg
        assert "2. Verify network" in msg

    def test_without_suggestions_no_suggestions_section(self):
        err = ConnectionError("x", "y")
        assert "Suggestions:" not in str(err)

    def test_none_suggestions_defaults_to_empty_list(self):
        err = ConnectionError("x", "y", suggestions=None)
        assert err.suggestions == []


class TestDependencyError:
    def test_message_only(self):
        err = DependencyError("missing node A")
        assert err.message == "missing node A"
        assert err.cycle is None
        assert "[X] Dependency error: missing node A" in str(err)
        assert "Cycle detected" not in str(err)

    def test_with_cycle(self):
        cycle = ["A", "B", "C", "A"]
        err = DependencyError("circular dependency", cycle=cycle)
        assert err.cycle == cycle
        msg = str(err)
        assert "Cycle detected: A -> B -> C -> A" in msg

    def test_empty_cycle_not_shown(self):
        err = DependencyError("issue", cycle=[])
        assert "Cycle detected" not in str(err)


class TestExecutionContext:
    def test_minimal(self):
        ctx = ExecutionContext(node_name="my_node")
        assert ctx.node_name == "my_node"
        assert ctx.config_file is None
        assert ctx.config_line is None
        assert ctx.step_index is None
        assert ctx.total_steps is None
        assert ctx.input_schema == []
        assert ctx.input_shape is None
        assert ctx.previous_steps == []

    def test_full(self):
        ctx = ExecutionContext(
            node_name="n1",
            config_file="p.yaml",
            config_line=5,
            step_index=2,
            total_steps=4,
            input_schema=["col_a", "col_b"],
            input_shape=(100, 2),
            previous_steps=["step1", "step2"],
        )
        assert ctx.config_file == "p.yaml"
        assert ctx.config_line == 5
        assert ctx.step_index == 2
        assert ctx.total_steps == 4
        assert ctx.input_schema == ["col_a", "col_b"]
        assert ctx.input_shape == (100, 2)
        assert ctx.previous_steps == ["step1", "step2"]

    def test_none_lists_default_to_empty(self):
        ctx = ExecutionContext("x", input_schema=None, previous_steps=None)
        assert ctx.input_schema == []
        assert ctx.previous_steps == []


class TestNodeExecutionError:
    @pytest.fixture
    def minimal_context(self):
        return ExecutionContext(node_name="test_node")

    @pytest.fixture
    def full_context(self):
        return ExecutionContext(
            node_name="load_sales",
            config_file="sales.yaml",
            config_line=15,
            step_index=1,
            total_steps=3,
            input_schema=["id", "amount", "date"],
            input_shape=(500, 3),
            previous_steps=["read_csv", "filter_nulls"],
        )

    def test_minimal(self, minimal_context):
        err = NodeExecutionError("something failed", minimal_context)
        msg = str(err)
        assert "[X] Node execution failed: test_node" in msg
        assert "Error: something failed" in msg
        assert err.original_error is None
        assert err.suggestions == []
        assert err.story_path is None

    def test_with_all_context(self, full_context):
        err = NodeExecutionError(
            "column missing",
            full_context,
            suggestions=["Check schema", "Add column"],
            story_path="stories/sales.md",
        )
        msg = str(err)
        assert "Location: sales.yaml:15" in msg
        assert "Step: 2 of 3" in msg
        assert "Available columns: ['id', 'amount', 'date']" in msg
        assert "Input shape: (500, 3)" in msg
        assert "read_csv" in msg
        assert "filter_nulls" in msg
        assert "1. Check schema" in msg
        assert "2. Add column" in msg
        assert "Story: stories/sales.md" in msg

    def test_with_original_error_non_py4j(self, minimal_context):
        original = ValueError("bad value")
        err = NodeExecutionError("wrap", minimal_context, original_error=original)
        msg = str(err)
        assert "Error: wrap" in msg
        assert "Type: ValueError" in msg

    def test_location_without_line(self):
        ctx = ExecutionContext(node_name="n", config_file="f.yaml")
        err = NodeExecutionError("x", ctx)
        msg = str(err)
        assert "Location: f.yaml" in msg
        assert ":None" not in msg

    def test_step_index_zero(self):
        ctx = ExecutionContext(node_name="n", step_index=0, total_steps=5)
        err = NodeExecutionError("x", ctx)
        assert "Step: 1 of 5" in str(err)

    def test_no_suggestions_section_when_empty(self, minimal_context):
        err = NodeExecutionError("x", minimal_context)
        assert "Suggestions:" not in str(err)

    def test_no_story_section_when_none(self, minimal_context):
        err = NodeExecutionError("x", minimal_context)
        assert "Story:" not in str(err)

    def test_no_previous_steps_section_when_empty(self, minimal_context):
        err = NodeExecutionError("x", minimal_context)
        assert "Previous steps:" not in str(err)


class TestCleanPy4JError:
    @pytest.fixture
    def error_instance(self):
        ctx = ExecutionContext(node_name="n")
        return NodeExecutionError("msg", ctx)

    def test_regular_exception_passthrough(self, error_instance):
        err = ValueError("plain error")
        clean_msg, clean_type = error_instance._clean_spark_error(err)
        assert clean_msg == "plain error"
        assert clean_type == "ValueError"

    def test_analysis_exception_pattern(self, error_instance):
        class Py4JJavaError(Exception):
            pass

        err = Py4JJavaError(
            "An error occurred while calling o46.save.\n"
            ": org.apache.spark.sql.AnalysisException: Column 'x' does not exist"
        )
        clean_msg, clean_type = error_instance._clean_spark_error(err)
        assert "Column 'x' does not exist" in clean_msg
        assert clean_type == "AnalysisException"

    def test_parse_exception_pattern(self, error_instance):
        class Py4JJavaError(Exception):
            pass

        err = Py4JJavaError(
            "An error occurred\n: org.apache.spark.sql.catalyst.parser.ParseException: syntax error"
        )
        clean_msg, clean_type = error_instance._clean_spark_error(err)
        assert "syntax error" in clean_msg
        assert clean_type == "ParseException"

    def test_file_not_found_pattern(self, error_instance):
        class Py4JJavaError(Exception):
            pass

        err = Py4JJavaError("An error occurred\n: java.io.FileNotFoundException: /data/missing.csv")
        clean_msg, clean_type = error_instance._clean_spark_error(err)
        assert "/data/missing.csv" in clean_msg
        assert clean_type == "FileNotFoundException"

    def test_generic_exception_pattern(self, error_instance):
        class Py4JJavaError(Exception):
            pass

        err = Py4JJavaError("SomeException: bad stuff happened")
        clean_msg, clean_type = error_instance._clean_spark_error(err)
        assert "bad stuff happened" in clean_msg
        assert clean_type == "SomeException"

    def test_py4j_no_matching_pattern_uses_java_line_fallback(self, error_instance):
        class Py4JJavaError(Exception):
            pass

        err = Py4JJavaError(
            "An error occurred while calling o46.save.\n"
            ": java.lang.NullPointerException: some detail\n"
            "  at org.apache.hadoop.Something"
        )
        clean_msg, clean_type = error_instance._clean_spark_error(err)
        assert clean_type == "NullPointerException"

    def test_py4j_completely_unparseable(self, error_instance):
        class Py4JJavaError(Exception):
            pass

        err = Py4JJavaError("totally unparseable garbage with no patterns at all")
        clean_msg, clean_type = error_instance._clean_spark_error(err)
        assert clean_type == "Py4JJavaError"
        assert "unparseable" in clean_msg

    def test_java_fallback_pattern(self, error_instance):
        class Py4JJavaError(Exception):
            pass

        err = Py4JJavaError(
            "An error occurred while calling o46.save.\n"
            ": java.lang.IllegalArgumentException: Path must not be null\n"
            "  at org.apache.hadoop..."
        )
        clean_msg, clean_type = error_instance._clean_spark_error(err)
        assert "Path must not be null" in clean_msg

    def test_cleaned_error_used_in_format(self):
        class Py4JJavaError(Exception):
            pass

        original = Py4JJavaError(
            "An error occurred\n: org.apache.spark.sql.AnalysisException: Table not found"
        )
        ctx = ExecutionContext(node_name="n")
        err = NodeExecutionError("fallback msg", ctx, original_error=original)
        msg = str(err)
        assert "Table not found" in msg
        assert "AnalysisException" in msg

    def test_uncleaned_error_falls_back_to_message(self):
        original = ValueError("short")
        ctx = ExecutionContext(node_name="n")
        err = NodeExecutionError("fallback msg", ctx, original_error=original)
        msg = str(err)
        assert "Error: fallback msg" in msg
        assert "Type: ValueError" in msg


class TestValidationError:
    def test_single_failure(self):
        err = ValidationError("node_a", ["null check failed"])
        assert err.node_name == "node_a"
        assert err.failures == ["null check failed"]
        msg = str(err)
        assert "[X] Validation failed for node: node_a" in msg
        assert "* null check failed" in msg

    def test_multiple_failures(self):
        failures = ["null check", "range check", "type check"]
        err = ValidationError("node_b", failures)
        msg = str(err)
        for f in failures:
            assert f"* {f}" in msg

    def test_empty_failures(self):
        err = ValidationError("node_c", [])
        assert "Failures:" in str(err)


class TestGateFailedError:
    def test_basic(self):
        err = GateFailedError(
            node_name="quality_node",
            pass_rate=0.85,
            required_rate=0.95,
            failed_rows=150,
            total_rows=1000,
        )
        assert err.node_name == "quality_node"
        assert err.pass_rate == 0.85
        assert err.required_rate == 0.95
        assert err.failed_rows == 150
        assert err.total_rows == 1000
        assert err.failure_reasons == []
        msg = str(err)
        assert "[X] Quality gate failed for node: quality_node" in msg
        assert "85.0%" in msg
        assert "95.0%" in msg
        assert "150" in msg
        assert "1,000" in msg

    def test_with_failure_reasons(self):
        reasons = ["too many nulls", "out of range values"]
        err = GateFailedError(
            node_name="n",
            pass_rate=0.50,
            required_rate=0.99,
            failed_rows=500,
            total_rows=1000,
            failure_reasons=reasons,
        )
        msg = str(err)
        assert "Reasons:" in msg
        assert "* too many nulls" in msg
        assert "* out of range values" in msg

    def test_without_failure_reasons_no_reasons_section(self):
        err = GateFailedError("n", 0.9, 0.95, 10, 100)
        assert "Reasons:" not in str(err)

    def test_none_failure_reasons_defaults_to_empty(self):
        err = GateFailedError("n", 0.9, 0.95, 10, 100, failure_reasons=None)
        assert err.failure_reasons == []

    def test_zero_pass_rate(self):
        err = GateFailedError("n", 0.0, 0.95, 1000, 1000)
        assert "0.0%" in str(err)

    def test_large_numbers_formatted(self):
        err = GateFailedError("n", 0.5, 0.99, 1000000, 2000000)
        assert "1,000,000" in str(err)
        assert "2,000,000" in str(err)

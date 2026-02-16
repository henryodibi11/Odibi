"""Unit tests for odibi/exceptions.py."""

import pytest

from odibi.exceptions import (
    ConfigValidationError,
    ConnectionError,
    DependencyError,
    ExecutionContext,
    GateFailedError,
    NodeExecutionError,
    OdibiException,
    TransformError,
    ValidationError,
)


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


class TestOdibiException:
    """Test base OdibiException class."""

    def test_can_be_instantiated_with_message(self):
        """Test that OdibiException can be instantiated with a message."""
        exc = OdibiException("Test error message")
        assert str(exc) == "Test error message"

    def test_inherits_from_exception(self):
        """Test that OdibiException inherits from Exception."""
        exc = OdibiException("Test")
        assert isinstance(exc, Exception)

    def test_can_be_raised_and_caught(self):
        """Test that OdibiException can be raised and caught."""
        with pytest.raises(OdibiException) as exc_info:
            raise OdibiException("Test error")
        assert "Test error" in str(exc_info.value)

    def test_can_be_caught_as_exception(self):
        """Test that OdibiException can be caught by parent Exception type."""
        with pytest.raises(Exception) as exc_info:
            raise OdibiException("Test error")
        assert isinstance(exc_info.value, OdibiException)


class TestConfigValidationError:
    """Test ConfigValidationError class."""

    def test_can_be_instantiated_with_message(self):
        """Test basic instantiation with message."""
        exc = ConfigValidationError("Invalid config")
        assert exc.message == "Invalid config"
        assert "Invalid config" in str(exc)

    def test_inherits_from_odibi_exception(self):
        """Test inheritance from OdibiException."""
        exc = ConfigValidationError("Test")
        assert isinstance(exc, OdibiException)
        assert isinstance(exc, Exception)

    def test_can_be_caught_by_parent_type(self):
        """Test that ConfigValidationError can be caught by OdibiException."""
        with pytest.raises(OdibiException) as exc_info:
            raise ConfigValidationError("Config error")
        assert isinstance(exc_info.value, ConfigValidationError)

    def test_stores_file_and_line_attributes(self):
        """Test that file and line attributes are stored."""
        exc = ConfigValidationError("Error", file="config.yaml", line=42)
        assert exc.file == "config.yaml"
        assert exc.line == 42

    def test_formats_error_with_file_location(self):
        """Test error formatting includes file location."""
        exc = ConfigValidationError("Missing field", file="pipeline.yaml", line=10)
        error_str = str(exc)
        assert "pipeline.yaml" in error_str
        assert "10" in error_str
        assert "Missing field" in error_str

    def test_formats_error_without_optional_fields(self):
        """Test error formatting works without file/line."""
        exc = ConfigValidationError("Basic error")
        error_str = str(exc)
        assert "Configuration validation error" in error_str
        assert "Basic error" in error_str


class TestConnectionError:
    """Test ConnectionError class."""

    def test_can_be_instantiated_with_required_fields(self):
        """Test basic instantiation."""
        exc = ConnectionError("my_db", "Invalid credentials")
        assert exc.connection_name == "my_db"
        assert exc.reason == "Invalid credentials"

    def test_inherits_from_odibi_exception(self):
        """Test inheritance chain."""
        exc = ConnectionError("conn", "reason")
        assert isinstance(exc, OdibiException)
        assert isinstance(exc, Exception)

    def test_can_be_caught_by_parent_type(self):
        """Test catching by parent type."""
        with pytest.raises(OdibiException) as exc_info:
            raise ConnectionError("db", "Failed to connect")
        assert isinstance(exc_info.value, ConnectionError)

    def test_stores_suggestions_attribute(self):
        """Test suggestions are stored."""
        suggestions = ["Check credentials", "Verify network"]
        exc = ConnectionError("db", "timeout", suggestions=suggestions)
        assert exc.suggestions == suggestions

    def test_formats_error_with_suggestions(self):
        """Test error formatting includes suggestions."""
        exc = ConnectionError(
            "azure_db",
            "Authentication failed",
            suggestions=["Check connection string", "Verify permissions"],
        )
        error_str = str(exc)
        assert "azure_db" in error_str
        assert "Authentication failed" in error_str
        assert "Check connection string" in error_str
        assert "Verify permissions" in error_str

    def test_formats_error_without_suggestions(self):
        """Test error formatting works without suggestions."""
        exc = ConnectionError("local_db", "Not found")
        error_str = str(exc)
        assert "local_db" in error_str
        assert "Not found" in error_str


class TestDependencyError:
    """Test DependencyError class."""

    def test_can_be_instantiated_with_message(self):
        """Test basic instantiation."""
        exc = DependencyError("Circular dependency detected")
        assert exc.message == "Circular dependency detected"

    def test_inherits_from_odibi_exception(self):
        """Test inheritance chain."""
        exc = DependencyError("Test")
        assert isinstance(exc, OdibiException)
        assert isinstance(exc, Exception)

    def test_can_be_caught_by_parent_type(self):
        """Test catching by parent type."""
        with pytest.raises(OdibiException) as exc_info:
            raise DependencyError("Missing node")
        assert isinstance(exc_info.value, DependencyError)

    def test_stores_cycle_attribute(self):
        """Test cycle is stored."""
        cycle = ["A", "B", "C", "A"]
        exc = DependencyError("Cycle found", cycle=cycle)
        assert exc.cycle == cycle

    def test_formats_error_with_cycle(self):
        """Test error formatting includes cycle visualization."""
        exc = DependencyError("Circular dependency", cycle=["node_a", "node_b", "node_a"])
        error_str = str(exc)
        assert "Circular dependency" in error_str
        assert "node_a -> node_b -> node_a" in error_str

    def test_formats_error_without_cycle(self):
        """Test error formatting works without cycle."""
        exc = DependencyError("Missing dependency")
        error_str = str(exc)
        assert "Missing dependency" in error_str


class TestExecutionContext:
    """Test ExecutionContext class."""

    def test_can_be_instantiated_with_minimal_fields(self):
        """Test basic instantiation."""
        context = ExecutionContext("my_node")
        assert context.node_name == "my_node"

    def test_stores_all_optional_attributes(self):
        """Test all attributes are stored correctly."""
        context = ExecutionContext(
            node_name="transform_node",
            config_file="pipeline.yaml",
            config_line=25,
            step_index=3,
            total_steps=10,
            input_schema=["col1", "col2", "col3"],
            input_shape=(100, 3),
            previous_steps=["load_data", "clean_data"],
        )
        assert context.node_name == "transform_node"
        assert context.config_file == "pipeline.yaml"
        assert context.config_line == 25
        assert context.step_index == 3
        assert context.total_steps == 10
        assert context.input_schema == ["col1", "col2", "col3"]
        assert context.input_shape == (100, 3)
        assert context.previous_steps == ["load_data", "clean_data"]

    def test_defaults_to_empty_lists(self):
        """Test default values for list attributes."""
        context = ExecutionContext("node")
        assert context.input_schema == []
        assert context.previous_steps == []


class TestNodeExecutionError:
    """Test NodeExecutionError class."""

    def test_can_be_instantiated_with_required_fields(self):
        """Test basic instantiation."""
        context = ExecutionContext("node")
        exc = NodeExecutionError("Execution failed", context)
        assert exc.message == "Execution failed"
        assert exc.context == context

    def test_inherits_from_odibi_exception(self):
        """Test inheritance chain."""
        context = ExecutionContext("node")
        exc = NodeExecutionError("Error", context)
        assert isinstance(exc, OdibiException)
        assert isinstance(exc, Exception)

    def test_can_be_caught_by_parent_type(self):
        """Test catching by parent type."""
        context = ExecutionContext("node")
        with pytest.raises(OdibiException) as exc_info:
            raise NodeExecutionError("Failed", context)
        assert isinstance(exc_info.value, NodeExecutionError)

    def test_stores_original_error(self):
        """Test original error is stored."""
        context = ExecutionContext("node")
        original = ValueError("Original error")
        exc = NodeExecutionError("Wrapped error", context, original_error=original)
        assert exc.original_error is original

    def test_stores_suggestions_and_story_path(self):
        """Test suggestions and story_path are stored."""
        context = ExecutionContext("node")
        suggestions = ["Check input data", "Review transformation"]
        exc = NodeExecutionError(
            "Error",
            context,
            suggestions=suggestions,
            story_path="stories/etl_pipeline.yaml",
        )
        assert exc.suggestions == suggestions
        assert exc.story_path == "stories/etl_pipeline.yaml"

    def test_formats_error_with_full_context(self):
        """Test error formatting with complete context."""
        context = ExecutionContext(
            node_name="aggregate_sales",
            config_file="pipeline.yaml",
            config_line=50,
            step_index=2,
            total_steps=5,
            input_schema=["date", "amount", "region"],
            input_shape=(1000, 3),
            previous_steps=["load_raw", "clean_data"],
        )
        exc = NodeExecutionError(
            "Aggregation failed",
            context,
            suggestions=["Check column names", "Verify data types"],
            story_path="stories/sales.yaml",
        )
        error_str = str(exc)
        assert "aggregate_sales" in error_str
        assert "pipeline.yaml" in error_str
        assert "50" in error_str
        assert "Step: 3 of 5" in error_str
        assert "Aggregation failed" in error_str
        assert "date" in error_str
        assert "(1000, 3)" in error_str
        assert "load_raw" in error_str
        assert "Check column names" in error_str
        assert "stories/sales.yaml" in error_str

    def test_formats_error_with_original_exception(self):
        """Test error formatting includes original exception type."""
        context = ExecutionContext("node")
        original = KeyError("missing_column")
        exc = NodeExecutionError("Column not found", context, original_error=original)
        error_str = str(exc)
        assert "KeyError" in error_str or "missing_column" in error_str

    def test_clean_spark_error_with_analysis_exception(self):
        """Test Spark error cleaning with AnalysisException."""
        context = ExecutionContext("node")

        # Simulate a Py4J error with AnalysisException
        class MockPy4JJavaError(Exception):
            """Mock Py4J Java error."""

            pass

        # Create a mock error with typical Spark stack trace
        spark_error = MockPy4JJavaError(
            "An error occurred while calling o46.save.\n"
            ": org.apache.spark.sql.AnalysisException: Column 'invalid_col' does not exist"
        )
        spark_error.__class__.__name__ = "Py4JJavaError"

        exc = NodeExecutionError("Query failed", context, original_error=spark_error)
        clean_msg, clean_type = exc._clean_spark_error(spark_error)

        # Should extract the clean message
        assert "does not exist" in clean_msg or "AnalysisException" in str(exc)

    def test_clean_spark_error_with_generic_error(self):
        """Test error cleaning with non-Spark error."""
        context = ExecutionContext("node")
        exc = NodeExecutionError("Error", context)

        generic_error = ValueError("Simple error message")
        clean_msg, clean_type = exc._clean_spark_error(generic_error)

        assert clean_msg == "Simple error message"
        assert clean_type == "ValueError"


class TestTransformError:
    """Test TransformError class."""

    def test_can_be_instantiated_with_message(self):
        """Test basic instantiation."""
        exc = TransformError("Transform failed")
        assert str(exc) == "Transform failed"

    def test_inherits_from_odibi_exception(self):
        """Test inheritance chain."""
        exc = TransformError("Test")
        assert isinstance(exc, OdibiException)
        assert isinstance(exc, Exception)

    def test_can_be_caught_by_parent_type(self):
        """Test catching by parent type."""
        with pytest.raises(OdibiException) as exc_info:
            raise TransformError("Failed")
        assert isinstance(exc_info.value, TransformError)


class TestValidationError:
    """Test ValidationError class."""

    def test_can_be_instantiated_with_required_fields(self):
        """Test basic instantiation."""
        failures = ["Field X is null", "Field Y out of range"]
        exc = ValidationError("data_node", failures)
        assert exc.node_name == "data_node"
        assert exc.failures == failures

    def test_inherits_from_odibi_exception(self):
        """Test inheritance chain."""
        exc = ValidationError("node", [])
        assert isinstance(exc, OdibiException)
        assert isinstance(exc, Exception)

    def test_can_be_caught_by_parent_type(self):
        """Test catching by parent type."""
        with pytest.raises(OdibiException) as exc_info:
            raise ValidationError("node", ["error"])
        assert isinstance(exc_info.value, ValidationError)

    def test_formats_error_with_failures(self):
        """Test error formatting includes all failures."""
        exc = ValidationError(
            "customer_data",
            [
                "email field contains invalid format",
                "age field is negative",
                "name field is empty",
            ],
        )
        error_str = str(exc)
        assert "customer_data" in error_str
        assert "email field contains invalid format" in error_str
        assert "age field is negative" in error_str
        assert "name field is empty" in error_str

    def test_formats_error_with_empty_failures(self):
        """Test error formatting with empty failures list."""
        exc = ValidationError("node", [])
        error_str = str(exc)
        assert "node" in error_str


class TestGateFailedError:
    """Test GateFailedError class."""

    def test_can_be_instantiated_with_required_fields(self):
        """Test basic instantiation."""
        exc = GateFailedError("quality_node", 0.85, 0.95, 150, 1000)
        assert exc.node_name == "quality_node"
        assert exc.pass_rate == 0.85
        assert exc.required_rate == 0.95
        assert exc.failed_rows == 150
        assert exc.total_rows == 1000

    def test_inherits_from_odibi_exception(self):
        """Test inheritance chain."""
        exc = GateFailedError("node", 0.5, 0.9, 10, 100)
        assert isinstance(exc, OdibiException)
        assert isinstance(exc, Exception)

    def test_can_be_caught_by_parent_type(self):
        """Test catching by parent type."""
        with pytest.raises(OdibiException) as exc_info:
            raise GateFailedError("node", 0.5, 0.9, 10, 100)
        assert isinstance(exc_info.value, GateFailedError)

    def test_stores_failure_reasons(self):
        """Test failure reasons are stored."""
        reasons = ["Null values in primary key", "Duplicate records found"]
        exc = GateFailedError("node", 0.8, 0.95, 200, 1000, failure_reasons=reasons)
        assert exc.failure_reasons == reasons

    def test_formats_error_with_metrics(self):
        """Test error formatting includes pass rate metrics."""
        exc = GateFailedError("validation_node", 0.875, 0.95, 125, 1000)
        error_str = str(exc)
        assert "validation_node" in error_str
        assert "87.5%" in error_str or "88%" in error_str  # Pass rate
        assert "95" in error_str  # Required rate
        assert "125" in error_str  # Failed rows
        assert "1,000" in error_str or "1000" in error_str  # Total rows

    def test_formats_error_with_failure_reasons(self):
        """Test error formatting includes failure reasons."""
        exc = GateFailedError(
            "data_quality",
            0.7,
            0.9,
            300,
            1000,
            failure_reasons=["Missing required fields", "Invalid date formats"],
        )
        error_str = str(exc)
        assert "Missing required fields" in error_str
        assert "Invalid date formats" in error_str

    def test_formats_error_without_failure_reasons(self):
        """Test error formatting works without failure reasons."""
        exc = GateFailedError("node", 0.8, 0.95, 20, 100)
        error_str = str(exc)
        assert "node" in error_str


class TestExceptionInheritance:
    """Test exception inheritance and catching behavior."""

    def test_all_custom_exceptions_inherit_from_odibi_exception(self):
        """Test that all custom exceptions inherit from OdibiException."""
        exceptions = [
            ConfigValidationError("test"),
            ConnectionError("conn", "reason"),
            DependencyError("test"),
            NodeExecutionError("test", ExecutionContext("node")),
            TransformError("test"),
            ValidationError("node", []),
            GateFailedError("node", 0.5, 0.9, 10, 100),
        ]
        for exc in exceptions:
            assert isinstance(exc, OdibiException)
            assert isinstance(exc, Exception)

    def test_can_catch_all_with_odibi_exception(self):
        """Test that all custom exceptions can be caught by OdibiException."""
        exceptions = [
            ConfigValidationError("test"),
            ConnectionError("conn", "reason"),
            DependencyError("test"),
            TransformError("test"),
            ValidationError("node", []),
            GateFailedError("node", 0.5, 0.9, 10, 100),
        ]
        for exc in exceptions:
            with pytest.raises(OdibiException):
                raise exc

    def test_specific_exception_type_preserved(self):
        """Test that specific exception types are preserved when catching."""
        with pytest.raises(OdibiException) as exc_info:
            raise ConfigValidationError("Config error")
        assert type(exc_info.value) is ConfigValidationError

        with pytest.raises(OdibiException) as exc_info:
            raise ValidationError("node", ["error"])
        assert type(exc_info.value) is ValidationError

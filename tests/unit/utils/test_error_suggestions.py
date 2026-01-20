"""Tests for centralized error suggestions engine."""

from odibi.utils.error_suggestions import (
    ErrorContext,
    ErrorPattern,
    get_suggestions,
    get_suggestions_for_connection,
    get_suggestions_for_transform,
    format_error_with_suggestions,
)


class TestErrorPattern:
    """Test ErrorPattern matching."""

    def test_simple_pattern_match(self):
        pattern = ErrorPattern(
            name="test",
            patterns=["not found", "missing"],
            suggestions=["Check the thing"],
        )
        ctx = ErrorContext()
        assert pattern.matches("Column not found", ctx)
        assert pattern.matches("File is missing", ctx)
        assert not pattern.matches("Success", ctx)

    def test_case_insensitive_matching(self):
        pattern = ErrorPattern(
            name="test",
            patterns=["ODBC Driver"],
            suggestions=["Install driver"],
        )
        ctx = ErrorContext()
        assert pattern.matches("odbc driver not found", ctx)
        assert pattern.matches("ODBC DRIVER error", ctx)


class TestGetSuggestions:
    """Test the main get_suggestions function."""

    def test_odbc_driver_error(self):
        error = Exception("ODBC Driver 18 for SQL Server not found")
        suggestions = get_suggestions(error)
        assert len(suggestions) > 0
        assert any("ODBC" in s or "driver" in s.lower() for s in suggestions)

    def test_login_failed_error(self):
        error = Exception("Login failed for user 'test_user'")
        ctx = ErrorContext(connection_type="azure_sql", auth_mode="sql")
        suggestions = get_suggestions(error, ctx)
        assert len(suggestions) > 0
        assert any("password" in s.lower() or "username" in s.lower() for s in suggestions)

    def test_firewall_error(self):
        error = Exception("Cannot connect: firewall rule blocking")
        suggestions = get_suggestions(error)
        assert any("firewall" in s.lower() or "IP" in s for s in suggestions)

    def test_column_not_found_with_available_columns(self):
        error = Exception("KeyError: 'CustomerID'")
        ctx = ErrorContext(
            available_columns=["customer_id", "CustomerName", "Phone"],
            node_name="test_node",
        )
        suggestions = get_suggestions(error, ctx)
        assert len(suggestions) > 0
        assert any("customer_id" in s.lower() for s in suggestions)

    def test_import_error_suggestion(self):
        error = ModuleNotFoundError("No module named 'polars'")
        suggestions = get_suggestions(error)
        assert any("pip install" in s.lower() for s in suggestions)

    def test_delta_not_found(self):
        error = Exception("Delta table not found at path /data/table")
        ctx = ErrorContext(path="/data/table")
        suggestions = get_suggestions(error, ctx)
        assert len(suggestions) > 0
        assert any("delta" in s.lower() or "path" in s.lower() for s in suggestions)

    def test_validation_failed(self):
        error = Exception("Validation failed: pass rate 85%")
        suggestions = get_suggestions(error)
        assert any("validation" in s.lower() or "quarantine" in s.lower() for s in suggestions)

    def test_env_var_missing(self):
        error = Exception("Environment variable ${SQL_PASSWORD} not set")
        suggestions = get_suggestions(error)
        assert any("environment" in s.lower() or ".env" in s.lower() for s in suggestions)

    def test_no_duplicate_suggestions(self):
        error = Exception("Column 'id' not found in DataFrame")
        suggestions = get_suggestions(error)
        assert len(suggestions) == len(set(suggestions))


class TestConvenienceFunctions:
    """Test convenience wrapper functions."""

    def test_get_suggestions_for_connection(self):
        error = Exception("Login failed")
        suggestions = get_suggestions_for_connection(
            error=error,
            connection_name="my_db",
            connection_type="azure_sql",
            auth_mode="sql",
        )
        assert len(suggestions) > 0

    def test_get_suggestions_for_transform(self):
        error = Exception("Column 'amount' not found")
        suggestions = get_suggestions_for_transform(
            error=error,
            node_name="process_sales",
            transformer_name="aggregate",
            engine="pandas",
            available_columns=["quantity", "price", "total"],
        )
        assert len(suggestions) > 0
        assert any("quantity" in s or "price" in s or "total" in s for s in suggestions)

    def test_format_error_with_suggestions(self):
        error = Exception("ODBC Driver not installed")
        formatted = format_error_with_suggestions(error, max_suggestions=3)
        assert "ODBC Driver not installed" in formatted
        assert "Suggestions:" in formatted
        assert "1." in formatted


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_error_message(self):
        error = Exception("")
        suggestions = get_suggestions(error)
        assert isinstance(suggestions, list)

    def test_none_context(self):
        error = Exception("Some error")
        suggestions = get_suggestions(error, None)
        assert isinstance(suggestions, list)

    def test_unknown_error_type(self):
        error = Exception("Completely unique error xyz123")
        suggestions = get_suggestions(error)
        assert isinstance(suggestions, list)

    def test_available_columns_shown(self):
        error = Exception("KeyError: 'missing_col'")
        ctx = ErrorContext(available_columns=["col1", "col2", "col3"])
        suggestions = get_suggestions(error, ctx)
        assert any("col1" in s or "Available columns" in s for s in suggestions)

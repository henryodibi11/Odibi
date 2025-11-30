import json
import re
import sys
from io import StringIO

from odibi.utils.logging import StructuredLogger


def strip_ansi(text):
    """Remove ANSI escape codes from text."""
    ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
    return ansi_escape.sub("", text)


class TestStructuredLogger:
    def test_standard_logging(self):
        """Test standard human-readable logging."""
        # Capture stdout
        captured = StringIO()
        original_stdout = sys.stdout
        sys.stdout = captured

        try:
            # Force re-init of logging config for this test
            import logging

            # Reset logging
            root = logging.getLogger()
            for handler in root.handlers[:]:
                root.removeHandler(handler)

            logger = StructuredLogger(structured=False)
            logger.info("Test message", extra="value")

            output = strip_ansi(captured.getvalue())
            assert "Test message" in output
            assert "extra=value" in output
            assert "{" not in output  # Should not be JSON

        finally:
            sys.stdout = original_stdout

    def test_structured_logging(self):
        """Test JSON structured logging."""
        captured = StringIO()
        original_stdout = sys.stdout
        sys.stdout = captured

        try:
            logger = StructuredLogger(structured=True)
            logger.info("Test message", extra="value")

            output = captured.getvalue().strip()

            # Should be valid JSON
            data = json.loads(output)
            assert data["message"] == "Test message"
            assert data["level"] == "INFO"
            assert data["extra"] == "value"
            assert "timestamp" in data

        finally:
            sys.stdout = original_stdout

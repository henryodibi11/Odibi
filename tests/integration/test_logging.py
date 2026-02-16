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
    def test_standard_logging(self, caplog):
        """Test standard human-readable logging."""
        import logging

        logger = StructuredLogger(structured=False)
        logger.logger.propagate = True

        with caplog.at_level(logging.INFO, logger="odibi"):
            logger.info("Test message", extra="value")

        assert any("Test message" in r.message for r in caplog.records)
        assert any("extra=value" in r.message for r in caplog.records)

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

import json
import logging
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

    def test_configure_logging_preserves_logger_registry_aliases_and_levels(self):
        import odibi.connections.factory as factory_module
        import odibi.connections.http as http_module
        import odibi.utils as utils_module
        import odibi.utils.logging as logging_module

        logger = logging_module.logger
        secrets = logger._secrets
        original_secrets = set(secrets)
        original_structured = logger.structured
        original_level = logger.level
        stdlib_loggers = {
            name: logging.getLogger(name)
            for name in [
                "odibi",
                "py4j",
                "azure",
                "azure.core.pipeline.policies.http_logging_policy",
                "adlfs",
                "urllib3",
                "fsspec",
            ]
        }
        original_stdlib_levels = {name: item.level for name, item in stdlib_loggers.items()}
        sentinel = "logging-lifecycle-sentinel"

        try:
            factory_module.logger.register_secret(sentinel)
            logging_module.configure_logging(structured=True, level="DEBUG")

            assert logging_module.logger is logger
            assert factory_module.logger is logger
            assert http_module.logger is logger
            assert utils_module.logger is logger
            assert logger._secrets is secrets
            assert sentinel in secrets
            assert logger.structured is True
            assert logger.level == logging.DEBUG
            assert logging.getLogger("odibi").level == logging.DEBUG
            for name in [
                "py4j",
                "azure",
                "azure.core.pipeline.policies.http_logging_policy",
                "adlfs",
                "urllib3",
                "fsspec",
            ]:
                assert logging.getLogger(name).level == logging.WARNING
        finally:
            secrets.clear()
            secrets.update(original_secrets)
            logger._configure(original_structured, logging.getLevelName(original_level))
            for name, stdlib_logger in stdlib_loggers.items():
                stdlib_logger.setLevel(original_stdlib_levels[name])

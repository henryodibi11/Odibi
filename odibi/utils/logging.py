"""Structured logging for Odibi framework.

This module provides the core logging infrastructure:
- StructuredLogger: Base logger with JSON/human-readable output
- LoggingContext: Context-aware logging wrapper (imported from logging_context)
- Secret redaction for sensitive data

For enhanced observability features, use:
    from odibi.utils.logging_context import LoggingContext, OperationType
"""

import codecs
import json
import logging
import sys
from datetime import datetime


try:
    from rich.console import Console
    from rich.logging import RichHandler

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class StructuredLogger:
    """Logger that supports both human-readable and JSON output with secret redaction.

    This is the base logging class for Odibi. For context-aware logging with
    automatic pipeline/node tracking, use LoggingContext instead.

    Example:
        >>> logger = StructuredLogger(structured=True, level="DEBUG")
        >>> logger.info("Processing started", pipeline="daily_etl", rows=1000)
    """

    def __init__(self, structured: bool = False, level: str = "INFO"):
        """Initialize structured logger.

        Args:
            structured: If True, output JSON logs; otherwise human-readable
            level: Log level (DEBUG, INFO, WARNING, ERROR)
        """
        self.structured = structured
        self.level = getattr(logging, level.upper(), logging.INFO)
        self._secrets: set = set()
        self._initialized = False

        if (
            sys.platform == "win32"
            and sys.stdout
            and sys.stdout.encoding
            and sys.stdout.encoding.lower() != "utf-8"
        ):
            try:
                sys.stdout.reconfigure(encoding="utf-8")
            except AttributeError:
                sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

        self._setup_handlers()

    def _setup_handlers(self) -> None:
        """Set up logging handlers."""
        if self._initialized:
            return

        if not self.structured and RICH_AVAILABLE:
            logging.basicConfig(
                level=self.level,
                format="%(message)s",
                datefmt="[%X]",
                handlers=[
                    RichHandler(
                        rich_tracebacks=True,
                        markup=True,
                        show_path=False,
                        console=(
                            Console(force_terminal=True, legacy_windows=False)
                            if sys.platform == "win32"
                            else None
                        ),
                    )
                ],
            )
        else:
            logging.basicConfig(level=self.level, format="%(message)s", stream=sys.stdout)

        self.logger = logging.getLogger("odibi")
        self.logger.setLevel(self.level)

        third_party_level = max(self.level, logging.WARNING)
        for logger_name in [
            "py4j",
            "azure",
            "azure.core.pipeline.policies.http_logging_policy",
            "adlfs",
            "urllib3",
            "fsspec",
        ]:
            logging.getLogger(logger_name).setLevel(third_party_level)

        self._initialized = True

    def register_secret(self, secret: str) -> None:
        """Register a secret string to be redacted from logs.

        Args:
            secret: Secret value to redact (passwords, keys, tokens)
        """
        if secret and isinstance(secret, str) and len(secret.strip()) > 0:
            self._secrets.add(secret)

    def _redact(self, text: str) -> str:
        """Redact registered secrets from text.

        Args:
            text: Text to redact

        Returns:
            Text with secrets replaced by [REDACTED]
        """
        if not text or not self._secrets:
            return text

        for secret in self._secrets:
            if secret in text:
                text = text.replace(secret, "[REDACTED]")
        return text

    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        self._log("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        self._log("WARNING", message, **kwargs)

    def error(self, message: str, **kwargs) -> None:
        """Log error message."""
        self._log("ERROR", message, **kwargs)

    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        self._log("DEBUG", message, **kwargs)

    def _log(self, level: str, message: str, **kwargs) -> None:
        """Internal log method with redaction and formatting.

        Args:
            level: Log level
            message: Log message
            **kwargs: Additional context to include
        """
        level_val = getattr(logging, level, logging.INFO)
        if level_val < self.level:
            return

        message = self._redact(str(message))

        redacted_kwargs = {}
        for k, v in kwargs.items():
            if isinstance(v, str):
                redacted_kwargs[k] = self._redact(v)
            elif v is None:
                continue
            else:
                redacted_kwargs[k] = v

        if self.structured:
            log_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": level,
                "message": message,
                **redacted_kwargs,
            }
            print(json.dumps(log_entry, default=str))
        else:
            context_str = ""
            if redacted_kwargs:
                context_items = [f"{k}={v}" for k, v in redacted_kwargs.items()]
                context_str = f" ({', '.join(context_items)})"

            formatted_msg = f"{message}{context_str}"

            if level == "INFO":
                self.logger.info(formatted_msg)
            elif level == "WARNING":
                self.logger.warning(f"[WARN] {formatted_msg}")
            elif level == "ERROR":
                self.logger.error(f"[ERROR] {formatted_msg}")
            elif level == "DEBUG":
                self.logger.debug(f"[DEBUG] {formatted_msg}")


# Global instance to be initialized
logger = StructuredLogger()


def configure_logging(structured: bool, level: str):
    """Configure the global logger."""
    global logger
    logger = StructuredLogger(structured=structured, level=level)

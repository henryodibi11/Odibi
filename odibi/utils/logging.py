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
    """Logger that supports both human-readable and JSON output with secret redaction."""

    def __init__(self, structured: bool = False, level: str = "INFO"):
        self.structured = structured
        self.level = getattr(logging, level.upper(), logging.INFO)
        self._secrets = set()

        # Fix Windows Unicode handling
        if sys.platform == "win32" and sys.stdout.encoding.lower() != "utf-8":
            try:
                sys.stdout.reconfigure(encoding="utf-8")
            except AttributeError:
                # Python < 3.7 doesn't have reconfigure, use codecs wrapper
                sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

        # Set up logging
        if not self.structured and RICH_AVAILABLE:
            # Rich logging (safe for Windows emoji)
            # Force console to use UTF-8 or fallback safely
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
            # Standard logging (JSON or fallback)
            logging.basicConfig(level=self.level, format="%(message)s", stream=sys.stdout)

        self.logger = logging.getLogger("odibi")
        self.logger.setLevel(self.level)

        # Third-party loggers respect user's level, but never MORE verbose than WARNING
        # (Azure SDK is extremely noisy at INFO/DEBUG)
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

    def register_secret(self, secret: str):
        """Register a secret string to be redacted from logs."""
        if secret and isinstance(secret, str) and len(secret.strip()) > 0:
            self._secrets.add(secret)

    def _redact(self, text: str) -> str:
        """Redact registered secrets from text."""
        if not text or not self._secrets:
            return text

        for secret in self._secrets:
            if secret in text:
                text = text.replace(secret, "[REDACTED]")
        return text

    def info(self, message: str, **kwargs):
        self._log("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs):
        self._log("WARNING", message, **kwargs)

    def error(self, message: str, **kwargs):
        self._log("ERROR", message, **kwargs)

    def debug(self, message: str, **kwargs):
        self._log("DEBUG", message, **kwargs)

    def _log(self, level: str, message: str, **kwargs):
        # Check level
        level_val = getattr(logging, level, logging.INFO)
        if level_val < self.level:
            return

        # Redact message
        message = self._redact(str(message))

        # Redact kwargs values
        redacted_kwargs = {}
        for k, v in kwargs.items():
            if isinstance(v, str):
                redacted_kwargs[k] = self._redact(v)
            else:
                redacted_kwargs[k] = v

        if self.structured:
            # JSON Output
            log_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": level,
                "message": message,
                **redacted_kwargs,
            }
            print(json.dumps(log_entry))
        else:
            # Standard Output
            # Format extra context if present
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

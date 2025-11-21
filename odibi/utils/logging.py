import json
import sys
import logging
from datetime import datetime


class StructuredLogger:
    """Logger that supports both human-readable and JSON output with secret redaction."""

    def __init__(self, structured: bool = False, level: str = "INFO"):
        self.structured = structured
        self.level = getattr(logging, level.upper(), logging.INFO)
        self._secrets = set()

        # Set up standard logging for when not structured
        logging.basicConfig(level=self.level, format="%(message)s", stream=sys.stdout)
        self.logger = logging.getLogger("odibi")

        # Suppress noisy Py4J logging
        logging.getLogger("py4j").setLevel(logging.WARNING)

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

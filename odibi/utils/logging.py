import json
import sys
import logging
from datetime import datetime


class StructuredLogger:
    """Logger that supports both human-readable and JSON output."""

    def __init__(self, structured: bool = False, level: str = "INFO"):
        self.structured = structured
        self.level = getattr(logging, level.upper(), logging.INFO)

        # Set up standard logging for when not structured
        logging.basicConfig(level=self.level, format="%(message)s", stream=sys.stdout)
        self.logger = logging.getLogger("odibi")

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

        if self.structured:
            # JSON Output
            log_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": level,
                "message": message,
                **kwargs,
            }
            print(json.dumps(log_entry))
        else:
            # Standard Output
            # Format extra context if present
            context_str = ""
            if kwargs:
                context_items = [f"{k}={v}" for k, v in kwargs.items()]
                context_str = f" ({', '.join(context_items)})"

            formatted_msg = f"{message}{context_str}"

            if level == "INFO":
                self.logger.info(formatted_msg)
            elif level == "WARNING":
                self.logger.warning(f"⚠️ {formatted_msg}")
            elif level == "ERROR":
                self.logger.error(f"❌ {formatted_msg}")
            elif level == "DEBUG":
                self.logger.debug(f"🐛 {formatted_msg}")


# Global instance to be initialized
logger = StructuredLogger()


def configure_logging(structured: bool, level: str):
    """Configure the global logger."""
    global logger
    logger = StructuredLogger(structured=structured, level=level)

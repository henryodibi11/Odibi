"""Custom exceptions for ODIBI framework."""

from typing import List, Optional, Tuple


class OdibiException(Exception):
    """Base exception for all ODIBI errors."""

    pass


class ConfigValidationError(OdibiException):
    """Configuration validation failed."""

    def __init__(self, message: str, file: Optional[str] = None, line: Optional[int] = None):
        self.message = message
        self.file = file
        self.line = line
        super().__init__(self._format_error())

    def _format_error(self) -> str:
        """Format error message with location info."""
        parts = ["Configuration validation error"]
        if self.file:
            parts.append(f"\n  File: {self.file}")
        if self.line:
            parts.append(f"\n  Line: {self.line}")
        parts.append(f"\n  Error: {self.message}")
        return "".join(parts)


class ConnectionError(OdibiException):
    """Connection failed or invalid."""

    def __init__(self, connection_name: str, reason: str, suggestions: Optional[List[str]] = None):
        self.connection_name = connection_name
        self.reason = reason
        self.suggestions = suggestions or []
        super().__init__(self._format_error())

    def _format_error(self) -> str:
        """Format connection error with suggestions."""
        parts = [
            f"[X] Connection validation failed: {self.connection_name}",
            f"\n  Reason: {self.reason}",
        ]

        if self.suggestions:
            parts.append("\n\n  Suggestions:")
            for i, suggestion in enumerate(self.suggestions, 1):
                parts.append(f"\n    {i}. {suggestion}")

        return "".join(parts)


class DependencyError(OdibiException):
    """Dependency graph error (cycles, missing nodes, etc.)."""

    def __init__(self, message: str, cycle: Optional[List[str]] = None):
        self.message = message
        self.cycle = cycle
        super().__init__(self._format_error())

    def _format_error(self) -> str:
        """Format dependency error."""
        parts = [f"[X] Dependency error: {self.message}"]

        if self.cycle:
            parts.append("\n  Cycle detected: " + " -> ".join(self.cycle))

        return "".join(parts)


class ExecutionContext:
    """Runtime context for error reporting."""

    def __init__(
        self,
        node_name: str,
        config_file: Optional[str] = None,
        config_line: Optional[int] = None,
        step_index: Optional[int] = None,
        total_steps: Optional[int] = None,
        input_schema: Optional[List[str]] = None,
        input_shape: Optional[tuple] = None,
        previous_steps: Optional[List[str]] = None,
    ):
        self.node_name = node_name
        self.config_file = config_file
        self.config_line = config_line
        self.step_index = step_index
        self.total_steps = total_steps
        self.input_schema = input_schema or []
        self.input_shape = input_shape
        self.previous_steps = previous_steps or []


class NodeExecutionError(OdibiException):
    """Node execution failed."""

    def __init__(
        self,
        message: str,
        context: ExecutionContext,
        original_error: Optional[Exception] = None,
        suggestions: Optional[List[str]] = None,
        story_path: Optional[str] = None,
    ):
        self.message = message
        self.context = context
        self.original_error = original_error
        self.suggestions = suggestions or []
        self.story_path = story_path
        super().__init__(self._format_error())

    def _clean_spark_error(self, error: Exception) -> Tuple[str, str]:
        """Extract clean error message from Spark exception.

        Args:
            error: Original exception

        Returns:
            (Clean Message, Error Type)
        """
        error_type = type(error).__name__
        msg = str(error)

        # Check for Py4J Java Error
        if "Py4JJavaError" in error_type:
            # The message usually contains the full Java stack trace
            # We want to find the actual exception message, usually after the first line
            # or specific patterns like "AnalysisException: ..."

            # Try to find the Java Exception class name
            import re

            # Match patterns like "org.apache.spark.sql.AnalysisException: ..."
            # or just "AnalysisException: ..." at start of line

            # Common Spark Exceptions to look for
            patterns = [
                r"org\.apache\.spark\.sql\.AnalysisException: (.*)",
                r"org\.apache\.spark\.sql\.catalyst\.parser\.ParseException: (.*)",
                r"java\.io\.FileNotFoundException: (.*)",
                r"java\.lang\.IllegalArgumentException: (.*)",
                r"org\.apache\.hadoop\.mapred\.InvalidInputException: (.*)",
                # Catch-all for simple class names
                r"([a-zA-Z0-9]+Exception): (.*)",
            ]

            for pattern in patterns:
                match = re.search(pattern, msg)
                if match:
                    # Found a clean message
                    clean_msg = (
                        match.group(1)
                        if len(match.groups()) == 1
                        else f"{match.group(1)}: {match.group(2)}"
                    )
                    # If it's the generic catch-all, simplify the type
                    if len(match.groups()) == 2:
                        error_type = match.group(1).split(".")[-1]
                        clean_msg = match.group(2)
                    else:
                        # Try to guess type from the pattern we matched?
                        # For named patterns, we know the type
                        if "AnalysisException" in pattern:
                            error_type = "AnalysisException"
                        elif "ParseException" in pattern:
                            error_type = "ParseException"
                        elif "FileNotFoundException" in pattern:
                            error_type = "FileNotFoundException"

                    return clean_msg.strip(), error_type

            # Fallback: If we can't parse it, take the first few lines before the stack trace
            # Py4J errors usually start with:
            # "An error occurred while calling o46.save.\n: java.lang.IllegalArgumentException: ..."
            lines = msg.split("\n")
            for line in lines:
                if line.strip().startswith(": java.") or line.strip().startswith(": org."):
                    # Found the java exception line
                    parts = line.split(":", 2)  # : java.lang.Exception: Message
                    if len(parts) >= 3:
                        error_type = parts[1].split(".")[-1].strip()
                        return parts[2].strip(), error_type

        return msg, error_type

    def _format_error(self) -> str:
        """Generate rich error message with context."""
        parts = [f"[X] Node execution failed: {self.context.node_name}"]

        # Location info
        if self.context.config_file:
            parts.append(f"\n  Location: {self.context.config_file}")
            if self.context.config_line:
                parts.append(f":{self.context.config_line}")

        # Step info
        if self.context.step_index is not None and self.context.total_steps:
            parts.append(f"\n  Step: {self.context.step_index + 1} of {self.context.total_steps}")

        # Error message
        # CLEAN THE ERROR HERE
        if self.original_error:
            clean_msg, clean_type = self._clean_spark_error(self.original_error)
            # If we successfully cleaned it (message is shorter than original), use it
            if len(clean_msg) < len(str(self.original_error)):
                parts.append(f"\n\n  Error: {clean_msg}")
                parts.append(f"\n  Type: {clean_type}")
            else:
                parts.append(f"\n\n  Error: {self.message}")
                parts.append(f"\n  Type: {type(self.original_error).__name__}")
        else:
            parts.append(f"\n\n  Error: {self.message}")

        # Context information
        if self.context.input_schema:
            parts.append(f"\n\n  Available columns: {self.context.input_schema}")

        if self.context.input_shape:
            parts.append(f"\n  Input shape: {self.context.input_shape}")

        if self.context.previous_steps:
            parts.append("\n\n  Previous steps:")
            for step in self.context.previous_steps:
                parts.append(f"\n    - {step}")

        # Suggestions
        if self.suggestions:
            parts.append("\n\n  Suggestions:")
            for i, suggestion in enumerate(self.suggestions, 1):
                parts.append(f"\n    {i}. {suggestion}")

        # Story reference
        if self.story_path:
            parts.append(f"\n\n  Story: {self.story_path}")

        return "".join(parts)


class TransformError(OdibiException):
    """Transform step failed."""

    pass


class ValidationError(OdibiException):
    """Data validation failed."""

    def __init__(self, node_name: str, failures: List[str]):
        self.node_name = node_name
        self.failures = failures
        super().__init__(self._format_error())

    def _format_error(self) -> str:
        """Format validation error."""
        parts = [f"[X] Validation failed for node: {self.node_name}"]
        parts.append("\n\n  Failures:")
        for failure in self.failures:
            parts.append(f"\n    * {failure}")
        return "".join(parts)

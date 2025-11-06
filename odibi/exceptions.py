"""Custom exceptions for ODIBI framework."""

from typing import Optional, List, Any


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
            f"✗ Connection validation failed: {self.connection_name}",
            f"\n  Reason: {self.reason}"
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
        parts = [f"✗ Dependency error: {self.message}"]
        
        if self.cycle:
            parts.append("\n  Cycle detected: " + " → ".join(self.cycle))
        
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
    
    def _format_error(self) -> str:
        """Generate rich error message with context."""
        parts = [f"✗ Node execution failed: {self.context.node_name}"]
        
        # Location info
        if self.context.config_file:
            parts.append(f"\n  Location: {self.context.config_file}")
            if self.context.config_line:
                parts.append(f":{self.context.config_line}")
        
        # Step info
        if self.context.step_index is not None and self.context.total_steps:
            parts.append(
                f"\n  Step: {self.context.step_index + 1} of {self.context.total_steps}"
            )
        
        # Error message
        parts.append(f"\n\n  Error: {self.message}")
        
        # Original error details
        if self.original_error:
            parts.append(f"\n  Type: {type(self.original_error).__name__}")
        
        # Context information
        if self.context.input_schema:
            parts.append(f"\n\n  Available columns: {self.context.input_schema}")
        
        if self.context.input_shape:
            parts.append(f"\n  Input shape: {self.context.input_shape}")
        
        if self.context.previous_steps:
            parts.append("\n\n  Previous steps:")
            for step in self.context.previous_steps:
                parts.append(f"\n    ✓ {step}")
        
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
        parts = [f"✗ Validation failed for node: {self.node_name}"]
        parts.append("\n\n  Failures:")
        for failure in self.failures:
            parts.append(f"\n    • {failure}")
        return "".join(parts)

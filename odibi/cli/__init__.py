"""
Command-line interface for Odibi.

Available commands:
- run: Execute a pipeline from YAML config
- validate: Validate YAML config without execution
"""
from odibi.cli.main import main

__all__ = ['main']

"""Validate command implementation."""

import argparse

from odibi.cli.main import cmd_validate


def validate_command(args):
    """Delegate the legacy argument shape to the active safe validator."""
    return cmd_validate(
        argparse.Namespace(
            file=args.config,
            env=getattr(args, "env", None),
            format=getattr(args, "format", "auto"),
        )
    )

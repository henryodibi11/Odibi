"""Main CLI entry point."""

import sys
import argparse
from odibi.cli.run import run_command
from odibi.cli.validate import validate_command
from odibi.cli.story import add_story_parser, story_command


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Odibi Data Pipeline Framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  odibi run config.yaml                    Run a pipeline
  odibi validate config.yaml               Validate configuration
  odibi story generate config.yaml        Generate documentation
  odibi story diff run1.json run2.json    Compare two runs
  odibi story list                         List story files
        """,
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # odibi run
    run_parser = subparsers.add_parser("run", help="Execute pipeline")
    run_parser.add_argument("config", help="Path to YAML config file")
    run_parser.add_argument(
        "--env", default="development", help="Environment (development/production)"
    )

    # odibi validate
    validate_parser = subparsers.add_parser("validate", help="Validate config")
    validate_parser.add_argument("config", help="Path to YAML config file")

    # odibi story
    add_story_parser(subparsers)

    args = parser.parse_args()

    if args.command == "run":
        return run_command(args)
    elif args.command == "validate":
        return validate_command(args)
    elif args.command == "story":
        return story_command(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())

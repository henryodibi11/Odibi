"""Main CLI entry point."""

import argparse
import sys

from odibi.cli.doctor import add_doctor_parser, doctor_command
from odibi.cli.export import add_export_parser, export_command
from odibi.cli.graph import graph_command
from odibi.cli.init_pipeline import add_init_parser, init_pipeline_command
from odibi.cli.run import run_command
from odibi.cli.secrets import add_secrets_parser, secrets_command
from odibi.cli.story import add_story_parser, story_command
from odibi.cli.test import test_command
from odibi.cli.ui import add_ui_parser, ui_command
from odibi.cli.validate import validate_command
from odibi.introspect import generate_docs
from odibi.utils.telemetry import setup_telemetry


def main():
    """Main CLI entry point."""
    # Configure telemetry early
    setup_telemetry()

    parser = argparse.ArgumentParser(
        description="Odibi Data Pipeline Framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  odibi run config.yaml                    Run a pipeline
  odibi validate config.yaml               Validate configuration
  odibi graph config.yaml                  Visualize dependencies
  odibi story generate config.yaml        Generate documentation
  odibi story diff run1.json run2.json    Compare two runs
  odibi story list                         List story files
        """,
    )

    # Global arguments
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging verbosity (default: INFO)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # odibi run
    run_parser = subparsers.add_parser("run", help="Execute pipeline")
    run_parser.add_argument("config", help="Path to YAML config file")
    run_parser.add_argument(
        "--env", default="development", help="Environment (development/production)"
    )
    run_parser.add_argument(
        "--dry-run", action="store_true", help="Simulate execution without running operations"
    )
    run_parser.add_argument(
        "--resume", action="store_true", help="Resume from last failure (skip successful nodes)"
    )
    run_parser.add_argument(
        "--parallel", action="store_true", help="Run independent nodes in parallel"
    )
    run_parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of worker threads for parallel execution (default: 4)",
    )
    run_parser.add_argument(
        "--on-error",
        choices=["fail_fast", "fail_later", "ignore"],
        help="Override error handling strategy",
    )

    # odibi deploy
    deploy_parser = subparsers.add_parser("deploy", help="Deploy definitions to System Catalog")
    deploy_parser.add_argument("config", help="Path to YAML config file")
    deploy_parser.add_argument(
        "--env", default="development", help="Environment (development/production)"
    )

    # odibi validate
    validate_parser = subparsers.add_parser("validate", help="Validate config")
    validate_parser.add_argument("config", help="Path to YAML config file")

    # odibi test
    test_parser = subparsers.add_parser("test", help="Run unit tests for transformations")
    test_parser.add_argument(
        "path", nargs="?", default="tests", help="Path to tests directory or file (default: tests)"
    )
    test_parser.add_argument("--snapshot", action="store_true", help="Update snapshots for tests")

    # odibi docs
    subparsers.add_parser("docs", help="Generate API documentation")

    # odibi graph
    graph_parser = subparsers.add_parser("graph", help="Visualize dependency graph")
    graph_parser.add_argument("config", help="Path to YAML config file")
    graph_parser.add_argument("--pipeline", help="Pipeline name (optional)")
    graph_parser.add_argument(
        "--format",
        choices=["ascii", "dot", "mermaid"],
        default="ascii",
        help="Output format (default: ascii)",
    )
    graph_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    # odibi story
    add_story_parser(subparsers)

    # odibi secrets
    add_secrets_parser(subparsers)

    # odibi init-pipeline (create/init)
    add_init_parser(subparsers)

    # odibi doctor
    add_doctor_parser(subparsers)

    # odibi ui
    add_ui_parser(subparsers)

    # odibi export
    add_export_parser(subparsers)

    args = parser.parse_args()

    # Configure logging
    import logging

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.command == "run":
        return run_command(args)
    elif args.command == "deploy":
        from odibi.cli.deploy import deploy_command

        return deploy_command(args)
    elif args.command == "docs":
        generate_docs()
        return 0
    elif args.command == "validate":
        return validate_command(args)
    elif args.command == "test":
        return test_command(args)
    elif args.command == "graph":
        return graph_command(args)
    elif args.command == "story":
        return story_command(args)
    elif args.command == "secrets":
        return secrets_command(args)
    elif args.command in ["init-pipeline", "create", "init", "generate-project"]:
        return init_pipeline_command(args)
    elif args.command == "doctor":
        return doctor_command(args)
    elif args.command == "ui":
        return ui_command(args)
    elif args.command == "export":
        return export_command(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())

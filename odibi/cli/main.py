"""Main CLI entry point."""

import sys
import argparse
from odibi.cli.run import run_command
from odibi.cli.validate import validate_command
from odibi.cli.doctor import doctor_command
from odibi.cli.story import add_story_parser, story_command
from odibi.cli.graph import graph_command
from odibi.cli.secrets import add_secrets_parser, secrets_command
from odibi.cli.generate_project import add_generate_project_parser, generate_project_command
from odibi.cli.stress import add_stress_parser, run_stress_test
from odibi.cli.schema import schema_command
from odibi.cli.ide import add_ide_parser, init_vscode_command
from odibi.cli.create import add_create_parser, create_command
from odibi.cli.init_pipeline import add_init_pipeline_parser, init_pipeline_command
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
  odibi doctor config.yaml                 Diagnose configuration issues
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

    # odibi validate
    validate_parser = subparsers.add_parser("validate", help="Validate config")
    validate_parser.add_argument("config", help="Path to YAML config file")

    # odibi doctor
    doctor_parser = subparsers.add_parser("doctor", help="Diagnose config issues")
    doctor_parser.add_argument("config", help="Path to YAML config file")

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

    # odibi generate-project
    add_generate_project_parser(subparsers)

    # odibi stress
    add_stress_parser(subparsers)

    # odibi schema
    subparsers.add_parser("schema", help="Generate JSON schema for VS Code")

    # odibi init-vscode
    add_ide_parser(subparsers)

    # odibi create
    add_create_parser(subparsers)

    # odibi init-pipeline
    add_init_pipeline_parser(subparsers)

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
    elif args.command == "validate":
        return validate_command(args)
    elif args.command == "doctor":
        return doctor_command(args)
    elif args.command == "graph":
        return graph_command(args)
    elif args.command == "story":
        return story_command(args)
    elif args.command == "secrets":
        return secrets_command(args)
    elif args.command == "generate-project":
        return generate_project_command(args)
    elif args.command == "stress":
        return run_stress_test(args)
    elif args.command == "schema":
        return schema_command(args)
    elif args.command == "init-vscode":
        return init_vscode_command(args)
    elif args.command == "create":
        return create_command(args)
    elif args.command == "init-pipeline":
        return init_pipeline_command(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())

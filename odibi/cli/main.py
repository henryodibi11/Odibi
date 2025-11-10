"""Main CLI entry point."""
import sys
import argparse
from odibi.cli.run import run_command
from odibi.cli.validate import validate_command


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description='Odibi Data Pipeline Framework')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # odibi run
    run_parser = subparsers.add_parser('run', help='Execute pipeline')
    run_parser.add_argument('config', help='Path to YAML config file')
    run_parser.add_argument('--env', default='development', help='Environment (development/production)')
    
    # odibi validate
    validate_parser = subparsers.add_parser('validate', help='Validate config')
    validate_parser.add_argument('config', help='Path to YAML config file')
    
    args = parser.parse_args()
    
    if args.command == 'run':
        return run_command(args)
    elif args.command == 'validate':
        return validate_command(args)
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())

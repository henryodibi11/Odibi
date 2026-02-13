"""
Story CLI Commands
==================

Commands for generating and managing pipeline documentation stories.
"""

import argparse
from pathlib import Path

import yaml

from odibi.config import ProjectConfig
from odibi.story import DocStoryGenerator


def story_command(args) -> int:
    """
    Handle story subcommands.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 for success, 1 for error)
    """
    if args.story_command == "generate":
        return generate_command(args)
    elif args.story_command == "diff":
        return diff_command(args)
    elif args.story_command == "list":
        return list_command(args)
    elif args.story_command == "last":
        return last_command(args)
    elif args.story_command == "show":
        return show_command(args)
    else:
        print(f"Unknown story command: {args.story_command}")
        return 1


def generate_command(args) -> int:
    """
    Generate documentation story from pipeline config.

    Args:
        args: Parsed arguments with config, output, format, validate, etc.

    Returns:
        Exit code
    """
    try:
        # Load configuration
        print(f"📖 Loading configuration from {args.config}...")

        with open(args.config, "r") as f:
            config_data = yaml.safe_load(f)

        config = ProjectConfig(**config_data)

        # Get the pipeline config (assume first pipeline if not specified)
        if config.pipelines:
            pipeline_config = config.pipelines[0]
        else:
            print("❌ No pipelines found in configuration")
            return 1

        # Create doc story generator
        print("📝 Generating documentation story...")
        generator = DocStoryGenerator(
            pipeline_config=pipeline_config,
            project_config=config if hasattr(config, "project") else None,
        )

        # Determine output path
        if args.output:
            output_path = args.output
        else:
            # Auto-generate output filename
            format_ext = {"html": ".html", "markdown": ".md", "json": ".json"}.get(
                args.format.lower(), ".html"
            )
            output_path = f"docs/{pipeline_config.pipeline}_documentation{format_ext}"

        # Load theme if HTML format
        theme = None
        if args.format.lower() == "html" and args.theme:
            from odibi.story.themes import get_theme

            try:
                theme = get_theme(args.theme)
                print(f"🎨 Using theme: {theme.name}")
            except ValueError as e:
                print(f"⚠️  Theme warning: {e}, using default theme")

        # Generate story
        result_path = generator.generate(
            output_path=output_path,
            format=args.format,
            validate=not args.no_validate,
            include_flow_diagram=not args.no_diagram,
            theme=theme,
        )

        print(f"✅ Documentation generated: {result_path}")
        print(f"📄 Format: {args.format.upper()}")

        if args.format.lower() == "html":
            print(f"🌐 Open in browser: file://{Path(result_path).absolute()}")

        return 0

    except FileNotFoundError as e:
        print(f"❌ Configuration file not found: {e}")
        return 1
    except ValueError as e:
        print(f"❌ Validation error: {e}")
        return 1
    except Exception as e:
        print(f"❌ Error generating documentation: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1


def diff_command(args) -> int:
    """
    Compare two pipeline run stories.

    Args:
        args: Parsed arguments with story1, story2 paths

    Returns:
        Exit code
    """
    try:
        import json

        print("📊 Comparing stories...")
        print(f"  Story 1: {args.story1}")
        print(f"  Story 2: {args.story2}")

        # Load story metadata from JSON files
        with open(args.story1, "r") as f:
            story1_data = json.load(f)

        with open(args.story2, "r") as f:
            story2_data = json.load(f)

        # Compare basic metrics
        print("\n📈 Comparison Results:")
        print("=" * 60)

        # Pipeline info
        print(f"\nPipeline: {story1_data.get('pipeline_name', 'Unknown')}")

        # Execution times
        print("\n⏱️  Execution Time:")
        print(f"  Story 1: {story1_data.get('duration', 0):.2f}s")
        print(f"  Story 2: {story2_data.get('duration', 0):.2f}s")

        time_diff = story2_data.get("duration", 0) - story1_data.get("duration", 0)
        if time_diff > 0:
            print(f"  Difference: +{time_diff:.2f}s (slower)")
        elif time_diff < 0:
            print(f"  Difference: {time_diff:.2f}s (faster)")
        else:
            print("  Difference: No change")

        # Success rate
        print("\n✅ Success Rate:")
        print(f"  Story 1: {story1_data.get('success_rate', 0):.1f}%")
        print(f"  Story 2: {story2_data.get('success_rate', 0):.1f}%")

        # Row counts
        print("\n📊 Rows Processed:")
        print(f"  Story 1: {story1_data.get('total_rows_processed', 0):,}")
        print(f"  Story 2: {story2_data.get('total_rows_processed', 0):,}")

        row_diff = story2_data.get("total_rows_processed", 0) - story1_data.get(
            "total_rows_processed", 0
        )
        if row_diff != 0:
            print(f"  Difference: {row_diff:+,} rows")

        # Node-level differences
        if args.detailed:
            print("\n🔍 Node-Level Details:")
            print("-" * 60)

            story1_nodes = {n["node_name"]: n for n in story1_data.get("nodes", [])}
            story2_nodes = {n["node_name"]: n for n in story2_data.get("nodes", [])}

            all_nodes = set(story1_nodes.keys()) | set(story2_nodes.keys())

            for node_name in sorted(all_nodes):
                node1 = story1_nodes.get(node_name, {})
                node2 = story2_nodes.get(node_name, {})

                print(f"\n  {node_name}:")

                if node1 and node2:
                    # Compare durations
                    dur1 = node1.get("duration", 0)
                    dur2 = node2.get("duration", 0)
                    dur_diff = dur2 - dur1
                    print(f"    Duration: {dur1:.3f}s → {dur2:.3f}s ({dur_diff:+.3f}s)")

                    # Compare row counts
                    rows1 = node1.get("rows_out", 0) or 0
                    rows2 = node2.get("rows_out", 0) or 0
                    if rows1 or rows2:
                        row_diff = rows2 - rows1
                        print(f"    Rows: {rows1:,} → {rows2:,} ({row_diff:+,})")

                    # Status changes
                    status1 = node1.get("status", "unknown")
                    status2 = node2.get("status", "unknown")
                    if status1 != status2:
                        print(f"    ⚠️  Status changed: {status1} → {status2}")

                elif node1:
                    print("    ❌ Removed in Story 2")
                elif node2:
                    print("    ➕ Added in Story 2")

        print("\n" + "=" * 60)
        return 0

    except FileNotFoundError as e:
        print(f"❌ Story file not found: {e}")
        return 1
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in story file: {e}")
        return 1
    except Exception as e:
        print(f"❌ Error comparing stories: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1


def list_command(args) -> int:
    """
    List available story files.

    Args:
        args: Parsed arguments with directory path

    Returns:
        Exit code
    """
    try:
        from datetime import datetime

        story_dir = Path(args.directory)

        if not story_dir.exists():
            print(f"❌ Directory not found: {story_dir}")
            return 1

        # Find story files (JSON, HTML, MD)
        story_files = []
        for ext in ["*.json", "*.html", "*.md"]:
            story_files.extend(story_dir.glob(ext))

        if not story_files:
            print(f"ℹ️  No story files found in {story_dir}")
            return 0

        # Sort by modification time (newest first)
        story_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

        print(f"\n📚 Stories in {story_dir}:")
        print("=" * 80)

        for story_file in story_files[: args.limit]:
            # Get file metadata
            stat = story_file.stat()
            size = stat.st_size
            modified = datetime.fromtimestamp(stat.st_mtime)

            # Format size
            if size < 1024:
                size_str = f"{size}B"
            elif size < 1024 * 1024:
                size_str = f"{size / 1024:.1f}KB"
            else:
                size_str = f"{size / 1024 / 1024:.1f}MB"

            print(f"\n  📄 {story_file.name}")
            print(f"     Modified: {modified.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"     Size: {size_str}")
            print(f"     Path: {story_file}")

        if len(story_files) > args.limit:
            print(f"\n  ... and {len(story_files) - args.limit} more")
            print("  (Use --limit to show more)")

        print()
        return 0

    except Exception as e:
        print(f"❌ Error listing stories: {e}")
        return 1


def last_command(args) -> int:
    """
    Show the most recent story file.

    Args:
        args: Parsed arguments with optional --node filter

    Returns:
        Exit code
    """
    import json
    import webbrowser

    # Common story directories to search (with recursive glob)
    search_patterns = [
        Path("stories/runs"),
        Path("data/gold/stories"),
        Path("data/stories"),
        Path("stories"),
        Path("."),
    ]

    # Find the most recent story file
    latest_story = None
    latest_time = 0

    for base_dir in search_patterns:
        if not base_dir.exists():
            continue

        # Use recursive glob to find stories in subdirectories
        for ext in ["**/*.html", "**/*.json", "*.html", "*.json"]:
            for story_file in base_dir.glob(ext):
                if story_file.is_file():
                    mtime = story_file.stat().st_mtime
                    if mtime > latest_time:
                        latest_time = mtime
                        latest_story = story_file

    if not latest_story:
        print("❌ No story files found")
        print("   Run a pipeline first: odibi run odibi.yaml")
        return 1

    print(f"📖 Latest story: {latest_story}")

    # If --node is specified, filter and display node info
    if hasattr(args, "node") and args.node:
        if latest_story.suffix == ".json":
            with open(latest_story, "r") as f:
                story_data = json.load(f)

            # Find the node
            nodes = story_data.get("nodes", [])
            node_info = None
            for node in nodes:
                if node.get("node_name") == args.node:
                    node_info = node
                    break

            if node_info:
                print(f"\n🔍 Node: {args.node}")
                print("=" * 60)
                print(f"   Status: {node_info.get('status', 'unknown')}")
                print(f"   Duration: {node_info.get('duration', 0):.3f}s")
                print(f"   Rows In: {node_info.get('rows_in', 'N/A')}")
                print(f"   Rows Out: {node_info.get('rows_out', 'N/A')}")

                if node_info.get("error"):
                    print(f"\n❌ Error: {node_info.get('error')}")

                if node_info.get("source_path"):
                    print(f"\n   Source: {node_info.get('source_path')}")
                if node_info.get("target_path"):
                    print(f"   Target: {node_info.get('target_path')}")
            else:
                print(f"❌ Node '{args.node}' not found in story")
                print(f"   Available nodes: {[n.get('node_name') for n in nodes]}")
                return 1
        else:
            print("   (Node filtering only works with JSON stories)")
    else:
        # Open the story in browser if HTML, or print path
        if latest_story.suffix == ".html":
            abs_path = latest_story.absolute()
            print("🌐 Opening in browser...")
            webbrowser.open(f"file://{abs_path}")
        else:
            print(f"   View: odibi story show {latest_story}")

    return 0


def show_command(args) -> int:
    """
    Show a specific story file.

    Args:
        args: Parsed arguments with story path

    Returns:
        Exit code
    """
    import json
    import webbrowser

    story_path = Path(args.path)

    if not story_path.exists():
        print(f"❌ Story not found: {story_path}")
        return 1

    print(f"📖 Story: {story_path}")

    if story_path.suffix == ".html":
        abs_path = story_path.absolute()
        print("🌐 Opening in browser...")
        webbrowser.open(f"file://{abs_path}")
    elif story_path.suffix == ".json":
        with open(story_path, "r") as f:
            story_data = json.load(f)

        print(f"\n📊 Pipeline: {story_data.get('pipeline_name', 'Unknown')}")
        print(f"   Duration: {story_data.get('duration', 0):.2f}s")
        print(f"   Status: {'✅ Success' if story_data.get('success') else '❌ Failed'}")
        print(f"   Nodes: {len(story_data.get('nodes', []))}")

        if story_data.get("nodes"):
            print("\n   Node Summary:")
            for node in story_data["nodes"]:
                status_icon = "✅" if node.get("status") == "success" else "❌"
                print(f"     {status_icon} {node.get('node_name')}: {node.get('duration', 0):.3f}s")
    else:
        print("   (Use a text editor to view this file)")

    return 0


def add_story_parser(subparsers) -> argparse.ArgumentParser:
    """
    Add story subcommand parser.

    Args:
        subparsers: Argparse subparsers object

    Returns:
        Story parser
    """
    story_parser = subparsers.add_parser(
        "story", help="Generate and manage pipeline documentation stories"
    )

    story_subparsers = story_parser.add_subparsers(dest="story_command", help="Story commands")

    # odibi story generate
    generate_parser = story_subparsers.add_parser(
        "generate", help="Generate documentation story from pipeline config"
    )
    generate_parser.add_argument("config", help="Path to pipeline YAML config file")
    generate_parser.add_argument(
        "-o", "--output", help="Output file path (auto-generated if not specified)"
    )
    generate_parser.add_argument(
        "-f",
        "--format",
        choices=["html", "markdown", "md", "json"],
        default="html",
        help="Output format (default: html)",
    )
    generate_parser.add_argument(
        "--no-validate", action="store_true", help="Skip explanation quality validation"
    )
    generate_parser.add_argument(
        "--no-diagram", action="store_true", help="Exclude flow diagram from documentation"
    )
    generate_parser.add_argument(
        "-t",
        "--theme",
        default="default",
        help="Theme name or path to custom theme YAML (default: default, options: corporate, dark, minimal)",
    )
    generate_parser.add_argument(
        "-v", "--verbose", action="store_true", help="Verbose output with stack traces"
    )

    # odibi story diff
    diff_parser = story_subparsers.add_parser("diff", help="Compare two pipeline run stories")
    diff_parser.add_argument("story1", help="Path to first story JSON file")
    diff_parser.add_argument("story2", help="Path to second story JSON file")
    diff_parser.add_argument(
        "-d", "--detailed", action="store_true", help="Show detailed node-level comparison"
    )
    diff_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    # odibi story list
    list_parser = story_subparsers.add_parser("list", help="List available story files")
    list_parser.add_argument(
        "-d",
        "--directory",
        default="stories/runs",
        help="Directory to search for stories (default: stories/runs)",
    )
    list_parser.add_argument(
        "-l",
        "--limit",
        type=int,
        default=10,
        help="Maximum number of stories to show (default: 10)",
    )

    # odibi story last
    last_parser = story_subparsers.add_parser(
        "last", help="View the most recent story (opens HTML in browser)"
    )
    last_parser.add_argument(
        "--node",
        help="Filter to show details for a specific node",
    )

    # odibi story show
    show_parser = story_subparsers.add_parser("show", help="View a specific story file")
    show_parser.add_argument("path", help="Path to story file (JSON or HTML)")

    return story_parser

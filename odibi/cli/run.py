"""Run command implementation."""

from pathlib import Path

from odibi.pipeline import PipelineManager
from odibi.utils.extensions import load_extensions
from odibi.utils.logging import logger


def run_command(args):
    """Execute pipeline from config file."""
    try:
        config_path = Path(args.config).resolve()
        project_root = config_path.parent

        # Change CWD to config directory to resolve relative paths consistently
        import os

        original_cwd = os.getcwd()
        os.chdir(project_root)
        logger.debug(f"Changed working directory to: {project_root}")

        # Load .env file from project directory if it exists
        env_file = project_root / ".env"
        if env_file.exists():
            try:
                from dotenv import load_dotenv

                load_dotenv(env_file)
                logger.debug(f"Loaded environment from: {env_file}")
            except ImportError:
                logger.warning("python-dotenv not installed, skipping .env file")

        try:
            # Load extensions from config dir (which is now CWD)
            load_extensions(project_root)

            manager = PipelineManager.from_yaml(config_path.name, env=args.env)
            results = manager.run(
                pipelines=getattr(args, "pipeline_name", None),
                dry_run=args.dry_run,
                resume_from_failure=args.resume,
                parallel=args.parallel,
                max_workers=args.workers,
                on_error=args.on_error,
                tag=getattr(args, "tag", None),
                node=getattr(args, "node_name", None),
            )
        finally:
            # Restore CWD
            os.chdir(original_cwd)

        # Check results for failures
        failed = False
        all_results = results.values() if isinstance(results, dict) else [results]

        for result in all_results:
            if result.failed:
                failed = True

                # Print debug summary with next steps
                debug_output = result.debug_summary()
                if debug_output:
                    print(debug_output)

                # Also log individual errors for detail
                for node_name in result.failed:
                    node_res = result.node_results.get(node_name)
                    if node_res and node_res.error:
                        # Unbury Suggestions
                        error_obj = node_res.error
                        suggestions = getattr(error_obj, "suggestions", [])

                        if not suggestions and hasattr(error_obj, "original_error"):
                            suggestions = getattr(error_obj.original_error, "suggestions", [])

                        if suggestions:
                            logger.info("💡 Suggestions:")
                            for suggestion in suggestions:
                                logger.info(f"   - {suggestion}")
            else:
                # Success - still show story path
                if result.story_path:
                    print(f"\n✅ Pipeline '{result.pipeline_name}' completed successfully")
                    print(f"   Duration: {result.duration:.2f}s")
                    print(f"   Story: {result.story_path}")
                    print(f"   View:  odibi story show {result.story_path}")

        if failed:
            return 1
        else:
            return 0

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return 1
